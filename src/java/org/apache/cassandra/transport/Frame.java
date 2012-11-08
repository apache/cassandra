
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport;

import java.io.IOException;
import java.util.EnumSet;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.frame.*;

import org.apache.cassandra.utils.ByteBufferUtil;

public class Frame
{
    public final Header header;
    public final ChannelBuffer body;
    public final Connection connection;

    /**
     * On-wire frame.
     * Frames are defined as:
     *
     *   0         8        16        24        32
     *   +---------+---------+---------+---------+
     *   | version |  flags  | stream  | opcode  |
     *   +---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     */
    private Frame(Header header, ChannelBuffer body, Connection connection)
    {
        this.header = header;
        this.body = body;
        this.connection = connection;
    }

    public static Frame create(ChannelBuffer fullFrame, Connection connection)
    {
        assert fullFrame.readableBytes() >= Header.LENGTH : String.format("Frame too short (%d bytes = %s)",
                                                                          fullFrame.readableBytes(),
                                                                          ByteBufferUtil.bytesToHex(fullFrame.toByteBuffer()));

        int version = fullFrame.readByte();
        int flags = fullFrame.readByte();
        int streamId = fullFrame.readByte();
        int opcode = fullFrame.readByte();
        int length = fullFrame.readInt();
        assert length == fullFrame.readableBytes();

        // version first byte is the "direction" of the frame (request or response)
        Message.Direction direction = Message.Direction.extractFromVersion(version);
        version = version & 0x7F;

        Header header = new Header(version, flags, streamId, Message.Type.fromOpcode(opcode, direction));
        return new Frame(header, fullFrame, connection);
    }

    public static Frame create(Message.Type type, int streamId, EnumSet<Header.Flag> flags, ChannelBuffer body, Connection connection)
    {
        Header header = new Header(Header.CURRENT_VERSION, flags, streamId, type);
        return new Frame(header, body, connection);
    }

    public static class Header
    {
        public static final int LENGTH = 8;
        public static final int CURRENT_VERSION = 1;

        public final int version;
        public final EnumSet<Flag> flags;
        public final int streamId;
        public final Message.Type type;

        private Header(int version, int flags, int streamId, Message.Type type)
        {
            this(version, Flag.deserialize(flags), streamId, type);
        }

        private Header(int version, EnumSet<Flag> flags, int streamId, Message.Type type)
        {
            this.version = version;
            this.flags = flags;
            this.streamId = streamId;
            this.type = type;
        }

        public static enum Flag
        {
            // The order of that enum matters!!
            COMPRESSED,
            TRACING;

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                Flag[] values = Flag.values();
                for (int n = 0; n < 8; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(values[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }
    }

    public Frame with(ChannelBuffer newBody)
    {
        return new Frame(header, newBody, connection);
    }

    public static class Decoder extends LengthFieldBasedFrameDecoder
    {
        private static final int MAX_FRAME_LENTH = 256 * 1024 * 1024; // 256 MB
        private final Connection connection;

        public Decoder(Connection.Tracker tracker, Connection.Factory factory)
        {
            super(MAX_FRAME_LENTH, 4, 4, 0, 0, true);
            this.connection = factory.newConnection(tracker);
        }

        @Override
        public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception
        {
            connection.registerChannel(e.getChannel());
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
        throws Exception
        {
            try
            {
                // We must at least validate that the frame version is something we support/know and it doesn't hurt to
                // check the opcode is not garbage. And we should do that indenpently of what is the the bytes corresponding
                // to the frame length are, i.e. we shouldn't wait for super.decode() to return non-null.
                if (buffer.readableBytes() == 0)
                    return null;

                int firstByte = buffer.getByte(0);
                Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
                int version = firstByte & 0x7F;
                // We really only support the current version so far
                if (version != Header.CURRENT_VERSION)
                    throw new ProtocolException("Invalid or unsupported protocol version: " + version);

                // Validate the opcode
                if (buffer.readableBytes() >= 4)
                    Message.Type.fromOpcode(buffer.getByte(3), direction);

                ChannelBuffer frame = (ChannelBuffer) super.decode(ctx, channel, buffer);
                if (frame == null)
                {
                    return null;
                }
                return Frame.create(frame, connection);
            }
            catch (CorruptedFrameException e)
            {
                throw new ProtocolException(e.getMessage());
            }
            catch (TooLongFrameException e)
            {
                throw new ProtocolException(e.getMessage());
            }
        }
    }

    public static class Encoder extends OneToOneEncoder
    {
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
        throws IOException
        {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;

            ChannelBuffer header = ChannelBuffers.buffer(Frame.Header.LENGTH);
            Message.Type type = frame.header.type;
            header.writeByte(type.direction.addToVersion(frame.header.version));
            header.writeByte(Header.Flag.serialize(frame.header.flags));
            header.writeByte(frame.header.streamId);
            header.writeByte(type.opcode);
            header.writeInt(frame.body.readableBytes());

            return ChannelBuffers.wrappedBuffer(header, frame.body);
        }
    }

    public static class Decompressor extends OneToOneDecoder
    {
        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg)
        throws IOException
        {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;

            if (!frame.header.flags.contains(Header.Flag.COMPRESSED))
                return frame;

            FrameCompressor compressor = frame.connection.getCompressor();
            if (compressor == null)
                return frame;

            return compressor.decompress(frame);
        }
    }

    public static class Compressor extends OneToOneEncoder
    {
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
        throws IOException
        {
            assert msg instanceof Frame : "Expecting frame, got " + msg;

            Frame frame = (Frame)msg;

            // Never compress STARTUP messages
            if (frame.header.type == Message.Type.STARTUP || frame.connection == null)
                return frame;

            FrameCompressor compressor = frame.connection.getCompressor();
            if (compressor == null)
                return frame;

            frame.header.flags.add(Header.Flag.COMPRESSED);
            return compressor.compress(frame);

        }
    }
}
