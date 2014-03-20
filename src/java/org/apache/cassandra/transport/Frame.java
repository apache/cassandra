
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
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.messages.ErrorMessage;

public class Frame
{
    public final Header header;
    public final ByteBuf body;

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
    private Frame(Header header, ByteBuf body)
    {
        this.header = header;
        this.body = body;
    }

    public static Frame create(Message.Type type, int streamId, int version, EnumSet<Header.Flag> flags, ByteBuf body)
    {
        Header header = new Header(version, flags, streamId, type);
        return new Frame(header, body);
    }

    public static class Header
    {
        public static final int LENGTH = 8;

        public static final int BODY_LENGTH_OFFSET = 4;
        public static final int BODY_LENGTH_SIZE = 4;

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

    public Frame with(ByteBuf newBody)
    {
        return new Frame(header, newBody);
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        private static final int MAX_FRAME_LENGTH = DatabaseDescriptor.getNativeTransportMaxFrameSize();

        private boolean discardingTooLongFrame;
        private long tooLongFrameLength;
        private long bytesToDiscard;
        private int tooLongStreamId;

        private final Connection.Factory factory;

        public Decoder(Connection.Factory factory)
        {
            this.factory = factory;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> results)
        throws Exception
        {
            if (discardingTooLongFrame)
            {
                bytesToDiscard = discard(buffer, bytesToDiscard);
                // If we have discarded everything, throw the exception
                if (bytesToDiscard <= 0)
                    fail();
                return;
            }

            // Wait until we have read at least the header
            if (buffer.readableBytes() < Header.LENGTH)
                return;

            int idx = buffer.readerIndex();

            int firstByte = buffer.getByte(idx);
            Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
            int version = firstByte & 0x7F;

            if (version > Server.CURRENT_VERSION)
                throw new ProtocolException("Invalid or unsupported protocol version: " + version);

            int flags = buffer.getByte(idx + 1);
            int streamId = buffer.getByte(idx + 2);

            // This throws a protocol exceptions if the opcode is unknown
            Message.Type type = Message.Type.fromOpcode(buffer.getByte(idx + 3), direction);

            long bodyLength = buffer.getUnsignedInt(idx + Header.BODY_LENGTH_OFFSET);

            if (bodyLength < 0)
            {
                buffer.skipBytes(Header.LENGTH);
                throw new ProtocolException("Invalid frame body length: " + bodyLength);
            }

            long frameLength = bodyLength + Header.LENGTH;
            if (frameLength > MAX_FRAME_LENGTH)
            {
                // Enter the discard mode and discard everything received so far.
                discardingTooLongFrame = true;
                tooLongStreamId = streamId;
                tooLongFrameLength = frameLength;
                bytesToDiscard = discard(buffer, frameLength);
                if (bytesToDiscard <= 0)
                    fail();
                return;
            }

            // never overflows because it's less than the max frame length
            int frameLengthInt = (int) frameLength;
            if (buffer.readableBytes() < frameLengthInt)
                return;

            // extract body
            // TODO: do we need unpooled?
            ByteBuf body = Unpooled.copiedBuffer(buffer.duplicate().slice(idx + Header.LENGTH, (int) bodyLength));
            buffer.readerIndex(idx + frameLengthInt);

            Connection connection = ctx.channel().attr(Connection.attributeKey).get();
            if (connection == null)
            {
                // First message seen on this channel, attach the connection object
                connection = factory.newConnection(ctx.channel(), version);
                ctx.channel().attr(Connection.attributeKey).set(connection);
            }
            else if (connection.getVersion() != version)
            {
                throw new ProtocolException(String.format("Invalid message version. Got %d but previous messages on this connection had version %d", version, connection.getVersion()));
            }

            results.add(new Frame(new Header(version, flags, streamId, type), body));
        }

        private void fail()
        {
            // Reset to the initial state and throw the exception
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0;
            discardingTooLongFrame = false;
            String msg = String.format("Request is too big: length %d exceeds maximum allowed length %d.", tooLongFrameLength,  MAX_FRAME_LENGTH);
            throw ErrorMessage.wrap(new InvalidRequestException(msg), tooLongStreamId);
        }
    }

    // How much remains to be discarded
    private static long discard(ByteBuf buffer, long remainingToDiscard)
    {
        int availableToDiscard = (int) Math.min(remainingToDiscard, buffer.readableBytes());
        buffer.skipBytes(availableToDiscard);
        return remainingToDiscard - availableToDiscard;
    }

    @ChannelHandler.Sharable
    public static class Encoder extends MessageToMessageEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, List results)
        throws IOException
        {
            ByteBuf header = Unpooled.buffer(Frame.Header.LENGTH);
            Message.Type type = frame.header.type;
            header.writeByte(type.direction.addToVersion(frame.header.version));
            header.writeByte(Header.Flag.serialize(frame.header.flags));
            header.writeByte(frame.header.streamId);
            header.writeByte(type.opcode);
            header.writeInt(frame.body.readableBytes());

            results.add(Unpooled.wrappedBuffer(header, frame.body));
        }
    }

    @ChannelHandler.Sharable
    public static class Decompressor extends MessageToMessageDecoder<Frame>
    {
        public void decode(ChannelHandlerContext ctx, Frame frame, List results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            if (!frame.header.flags.contains(Header.Flag.COMPRESSED) || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameCompressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(frame);
                return;
            }

            results.add(compressor.decompress(frame));
        }
    }

    @ChannelHandler.Sharable
    public static class Compressor extends MessageToMessageEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, List results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            // Never compress STARTUP messages
            if (frame.header.type == Message.Type.STARTUP || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameCompressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(frame);
                return;
            }

            frame.header.flags.add(Header.Flag.COMPRESSED);
            results.add(compressor.compress(frame));
        }
    }
}
