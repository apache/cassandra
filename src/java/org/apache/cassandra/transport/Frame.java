
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

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.Attribute;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;
import org.apache.cassandra.transport.frame.FrameBodyTransformer;
import org.apache.cassandra.transport.messages.ErrorMessage;

public class Frame
{
    public static final byte PROTOCOL_VERSION_MASK = 0x7f;

    public final Header header;
    public final ByteBuf body;

    /**
     * An on-wire frame consists of a header and a body.
     *
     * The header is defined the following way in native protocol version 3 and later:
     *
     *   0         8        16        24        32         40
     *   +---------+---------+---------+---------+---------+
     *   | version |  flags  |      stream       | opcode  |
     *   +---------+---------+---------+---------+---------+
     *   |                length                 |
     *   +---------+---------+---------+---------+
     */
    private Frame(Header header, ByteBuf body)
    {
        this.header = header;
        this.body = body;
    }

    public void retain()
    {
        body.retain();
    }

    public boolean release()
    {
        return body.release();
    }

    public static Frame create(Message.Type type, int streamId, ProtocolVersion version, EnumSet<Header.Flag> flags, ByteBuf body)
    {
        Header header = new Header(version, flags, streamId, type, body.readableBytes());
        return new Frame(header, body);
    }

    public static class Header
    {
        // 9 bytes in protocol version 3 and later
        public static final int LENGTH = 9;

        public static final int BODY_LENGTH_SIZE = 4;

        public final ProtocolVersion version;
        public final EnumSet<Flag> flags;
        public final int streamId;
        public final Message.Type type;
        public final long bodySizeInBytes;

        private Header(ProtocolVersion version, EnumSet<Flag> flags, int streamId, Message.Type type, long bodySizeInBytes)
        {
            this.version = version;
            this.flags = flags;
            this.streamId = streamId;
            this.type = type;
            this.bodySizeInBytes = bodySizeInBytes;
        }

        public enum Flag
        {
            // The order of that enum matters!!
            COMPRESSED,
            TRACING,
            CUSTOM_PAYLOAD,
            WARNING,
            USE_BETA,
            CHECKSUMMED;

            private static final Flag[] ALL_VALUES = values();

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                for (int n = 0; n < ALL_VALUES.length; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(ALL_VALUES[n]);
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

        @VisibleForTesting
        Frame decodeFrame(ByteBuf buffer)
        throws Exception
        {
            if (discardingTooLongFrame)
            {
                bytesToDiscard = discard(buffer, bytesToDiscard);
                // If we have discarded everything, throw the exception
                if (bytesToDiscard <= 0)
                    fail();
                return null;
            }

            int readableBytes = buffer.readableBytes();
            if (readableBytes == 0)
                return null;

            int idx = buffer.readerIndex();

            // Check the first byte for the protocol version before we wait for a complete header.  Protocol versions
            // 1 and 2 use a shorter header, so we may never have a complete header's worth of bytes.
            int firstByte = buffer.getByte(idx++);
            Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
            int versionNum = firstByte & PROTOCOL_VERSION_MASK;
            ProtocolVersion version = ProtocolVersion.decode(versionNum, DatabaseDescriptor.getNativeTransportAllowOlderProtocols());

            // Wait until we have the complete header
            if (readableBytes < Header.LENGTH)
                return null;

            int flags = buffer.getByte(idx++);
            EnumSet<Header.Flag> decodedFlags = Header.Flag.deserialize(flags);

            if (version.isBeta() && !decodedFlags.contains(Header.Flag.USE_BETA))
                throw new ProtocolException(String.format("Beta version of the protocol used (%s), but USE_BETA flag is unset", version),
                                            version);

            int streamId = buffer.getShort(idx);
            idx += 2;

            // This throws a protocol exceptions if the opcode is unknown
            Message.Type type;
            try
            {
                type = Message.Type.fromOpcode(buffer.getByte(idx++), direction);
            }
            catch (ProtocolException e)
            {
                throw ErrorMessage.wrap(e, streamId);
            }

            long bodyLength = buffer.getUnsignedInt(idx);
            idx += Header.BODY_LENGTH_SIZE;

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
                return null;
            }

            if (buffer.readableBytes() < frameLength)
                return null;

            ClientRequestSizeMetrics.totalBytesRead.inc(frameLength);
            ClientRequestSizeMetrics.bytesRecievedPerFrame.update(frameLength);

            // extract body
            ByteBuf body = buffer.slice(idx, (int) bodyLength);
            body.retain();

            idx += bodyLength;
            buffer.readerIndex(idx);

            return new Frame(new Header(version, decodedFlags, streamId, type, bodyLength), body);
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> results)
        throws Exception
        {
            Frame frame = decodeFrame(buffer);
            if (frame == null) return;

            Attribute<Connection> attrConn = ctx.channel().attr(Connection.attributeKey);
            Connection connection = attrConn.get();
            if (connection == null)
            {
                // First message seen on this channel, attach the connection object
                connection = factory.newConnection(ctx.channel(), frame.header.version);
                attrConn.set(connection);
            }
            else if (connection.getVersion() != frame.header.version)
            {
                throw ErrorMessage.wrap(
                        new ProtocolException(String.format(
                                "Invalid message version. Got %s but previous messages on this connection had version %s",
                                frame.header.version, connection.getVersion())),
                        frame.header.streamId);
            }
            results.add(frame);
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
        public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            ByteBuf header = CBUtil.allocator.buffer(Header.LENGTH);

            Message.Type type = frame.header.type;
            header.writeByte(type.direction.addToVersion(frame.header.version.asInt()));
            header.writeByte(Header.Flag.serialize(frame.header.flags));

            // Continue to support writing pre-v3 headers so that we can give proper error messages to drivers that
            // connect with the v1/v2 protocol. See CASSANDRA-11464.
            if (frame.header.version.isGreaterOrEqualTo(ProtocolVersion.V3))
                header.writeShort(frame.header.streamId);
            else
                header.writeByte(frame.header.streamId);

            header.writeByte(type.opcode);
            header.writeInt(frame.body.readableBytes());

            int messageSize = header.readableBytes() + frame.body.readableBytes();
            ClientRequestSizeMetrics.totalBytesWritten.inc(messageSize);
            ClientRequestSizeMetrics.bytesTransmittedPerFrame.update(messageSize);

            results.add(header);
            results.add(frame.body);
        }
    }

    @ChannelHandler.Sharable
    public static class InboundBodyTransformer extends MessageToMessageDecoder<Frame>
    {
        public void decode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            if ((!frame.header.flags.contains(Header.Flag.COMPRESSED) && !frame.header.flags.contains(Header.Flag.CHECKSUMMED)) || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameBodyTransformer transformer = connection.getTransformer();
            if (transformer == null)
            {
                results.add(frame);
                return;
            }

            try
            {
                results.add(frame.with(transformer.transformInbound(frame.body, frame.header.flags)));
            }
            finally
            {
                // release the old frame
                frame.release();
            }
        }
    }

    @ChannelHandler.Sharable
    public static class OutboundBodyTransformer extends MessageToMessageEncoder<Frame>
    {
        public void encode(ChannelHandlerContext ctx, Frame frame, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            // Never transform STARTUP messages
            if (frame.header.type == Message.Type.STARTUP || connection == null)
            {
                results.add(frame);
                return;
            }

            FrameBodyTransformer transformer = connection.getTransformer();
            if (transformer == null)
            {
                results.add(frame);
                return;
            }

            try
            {
                results.add(frame.with(transformer.transformOutbound(frame.body)));
                frame.header.flags.addAll(transformer.getOutboundHeaderFlags());
            }
            finally
            {
                // release the old frame
                frame.release();
            }
        }
    }
}
