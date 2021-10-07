
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
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.metrics.ClientMessageSizeMetrics;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Envelope
{
    public static final byte PROTOCOL_VERSION_MASK = 0x7f;

    public final Header header;
    public final ByteBuf body;

    /**
     * An on-wire message envelope consists of a header and a body.
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
    public Envelope(Header header, ByteBuf body)
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

    @VisibleForTesting
    public Envelope clone()
    {
        return new Envelope(header, Unpooled.wrappedBuffer(ByteBufferUtil.clone(body.nioBuffer())));
    }

    public static Envelope create(Message.Type type, int streamId, ProtocolVersion version, EnumSet<Header.Flag> flags, ByteBuf body)
    {
        Header header = new Header(version, flags, streamId, type, body.readableBytes());
        return new Envelope(header, body);
    }

    // used by V4 and earlier in Encoder.encode
    public ByteBuf encodeHeader()
    {
        ByteBuf buf = CBUtil.allocator.buffer(Header.LENGTH);

        Message.Type type = header.type;
        buf.writeByte(type.direction.addToVersion(header.version.asInt()));
        buf.writeByte(Header.Flag.serialize(header.flags));

        // Continue to support writing pre-v3 headers so that we can give proper error messages to drivers that
        // connect with the v1/v2 protocol. See CASSANDRA-11464.
        if (header.version.isGreaterOrEqualTo(ProtocolVersion.V3))
            buf.writeShort(header.streamId);
        else
            buf.writeByte(header.streamId);

        buf.writeByte(type.opcode);
        buf.writeInt(body.readableBytes());
        return buf;
    }

    // Used by V5 and later
    public void encodeHeaderInto(ByteBuffer buf)
    {
        buf.put((byte) header.type.direction.addToVersion(header.version.asInt()));
        buf.put((byte) Envelope.Header.Flag.serialize(header.flags));

        if (header.version.isGreaterOrEqualTo(ProtocolVersion.V3))
            buf.putShort((short) header.streamId);
        else
            buf.put((byte) header.streamId);

        buf.put((byte) header.type.opcode);
        buf.putInt(body.readableBytes());
    }

    // Used by V5 and later
    public void encodeInto(ByteBuffer buf)
    {
        encodeHeaderInto(buf);
        buf.put(body.nioBuffer());
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
            USE_BETA;

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

    public Envelope with(ByteBuf newBody)
    {
        return new Envelope(header, newBody);
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        private static final int MAX_TOTAL_LENGTH = DatabaseDescriptor.getNativeTransportMaxFrameSize();

        private boolean discardingTooLongMessage;
        private long tooLongTotalLength;
        private long bytesToDiscard;
        private int tooLongStreamId;

        /**
         * Used by protocol V5 and later to extract a CQL message header from the buffer containing
         * it, without modifying the position of the underlying buffer. This essentially mirrors the
         * pre-V5 code in {@link Decoder#decode(ByteBuf)}, with three differences:
         * <ul>
         *  <li>The input is a ByteBuffer rather than a ByteBuf</li>
         *  <li>This cannot return null, as V5 always deals with entire CQL messages. Coalescing of bytes
         *  off the wire happens at the layer below, in {@link org.apache.cassandra.net.FrameDecoder}</li>
         *  <li>This method never throws {@link ProtocolException}. Instead, a subclass of
         *  {@link HeaderExtractionResult} is returned which may provide either a {@link Header} or a
         *  {@link ProtocolException},depending on the result of its {@link HeaderExtractionResult#isSuccess()}
         *  method.</li>
         *</ul>
         *
         * @param buffer ByteBuffer containing the message envelope
         * @return The result of attempting to extract a header from the input buffer.
         */
        HeaderExtractionResult extractHeader(ByteBuffer buffer)
        {
            Preconditions.checkArgument(buffer.remaining() >= Header.LENGTH,
                                        "Undersized buffer supplied. Expected %s, actual %s",
                                        Header.LENGTH,
                                        buffer.remaining());
            int idx = buffer.position();
            int firstByte = buffer.get(idx++);
            int versionNum = firstByte & PROTOCOL_VERSION_MASK;
            int flags = buffer.get(idx++);
            int streamId = buffer.getShort(idx);
            idx += 2;
            int opcode = buffer.get(idx++);
            long bodyLength = buffer.getInt(idx);

            // if a negative length is read, return error but report length as 0 so we don't attempt to skip
            if (bodyLength < 0)
                return new HeaderExtractionResult.Error(new ProtocolException("Invalid value for envelope header body length field: " + bodyLength),
                                                        streamId, bodyLength);

            Message.Direction direction = Message.Direction.extractFromVersion(firstByte);
            Message.Type type;
            ProtocolVersion version;
            EnumSet<Header.Flag> decodedFlags;
            try
            {
                // This throws a protocol exception if the version number is unsupported,
                // the opcode is unknown or invalid flags are set for the version
                version = ProtocolVersion.decode(versionNum, DatabaseDescriptor.getNativeTransportAllowOlderProtocols());
                decodedFlags = decodeFlags(version, flags);
                type = Message.Type.fromOpcode(opcode, direction);
                return new HeaderExtractionResult.Success(new Header(version, decodedFlags, streamId, type, bodyLength));
            }
            catch (ProtocolException e)
            {
                // Including the streamId and bodyLength is a best effort to allow the caller
                // to send a meaningful response to the client and continue processing the
                // rest of the frame. It's possible that these are bogus and may have contributed
                // to the ProtocolException. If so, the upstream CQLMessageHandler should run into
                // further errors and once it breaches its threshold for consecutive errors, it will
                // cause the channel to be closed.
                return new HeaderExtractionResult.Error(e, streamId, bodyLength);
            }
        }

        public static abstract class HeaderExtractionResult
        {
            enum Outcome { SUCCESS, ERROR };

            private final Outcome outcome;
            private final int streamId;
            private final long bodyLength;
            private HeaderExtractionResult(Outcome outcome, int streamId, long bodyLength)
            {
                this.outcome = outcome;
                this.streamId = streamId;
                this.bodyLength = bodyLength;
            }

            boolean isSuccess()
            {
                return outcome == Outcome.SUCCESS;
            }

            int streamId()
            {
                return streamId;
            }

            long bodyLength()
            {
                return bodyLength;
            }

            Header header()
            {
                throw new IllegalStateException(String.format("Unable to provide header from extraction result : %s", outcome));
            };

            ProtocolException error()
            {
                throw new IllegalStateException(String.format("Unable to provide error from extraction result : %s", outcome));
            }

            private static class Success extends HeaderExtractionResult
            {
                private final Header header;
                Success(Header header)
                {
                    super(Outcome.SUCCESS, header.streamId, header.bodySizeInBytes);
                    this.header = header;
                }

                @Override
                Header header()
                {
                    return header;
                }
            }

            private static class Error extends HeaderExtractionResult
            {
                private final ProtocolException error;
                private Error(ProtocolException error, int streamId, long bodyLength)
                {
                    super(Outcome.ERROR, streamId, bodyLength);
                    this.error = error;
                }

                @Override
                ProtocolException error()
                {
                    return error;
                }
            }
        }

        @VisibleForTesting
        Envelope decode(ByteBuf buffer)
        {
            if (discardingTooLongMessage)
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

            ProtocolVersion version;
            
            try
            {
                version = ProtocolVersion.decode(versionNum, DatabaseDescriptor.getNativeTransportAllowOlderProtocols());
            }
            catch (ProtocolException e)
            {
                // Skip the remaining useless bytes. Otherwise the channel closing logic may try to decode again. 
                buffer.skipBytes(readableBytes);
                throw e;
            }

            // Wait until we have the complete header
            if (readableBytes < Header.LENGTH)
                return null;

            int flags = buffer.getByte(idx++);
            EnumSet<Header.Flag> decodedFlags = decodeFlags(version, flags);

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

            long totalLength = bodyLength + Header.LENGTH;
            if (totalLength > MAX_TOTAL_LENGTH)
            {
                // Enter the discard mode and discard everything received so far.
                discardingTooLongMessage = true;
                tooLongStreamId = streamId;
                tooLongTotalLength = totalLength;
                bytesToDiscard = discard(buffer, totalLength);
                if (bytesToDiscard <= 0)
                    fail();
                return null;
            }

            if (buffer.readableBytes() < totalLength)
                return null;

            ClientMessageSizeMetrics.bytesReceived.inc(totalLength);
            ClientMessageSizeMetrics.bytesReceivedPerRequest.update(totalLength);

            // extract body
            ByteBuf body = buffer.slice(idx, (int) bodyLength);
            body.retain();

            idx += bodyLength;
            buffer.readerIndex(idx);

            return new Envelope(new Header(version, decodedFlags, streamId, type, bodyLength), body);
        }

        private EnumSet<Header.Flag> decodeFlags(ProtocolVersion version, int flags)
        {
            EnumSet<Header.Flag> decodedFlags = Header.Flag.deserialize(flags);

            if (version.isBeta() && !decodedFlags.contains(Header.Flag.USE_BETA))
                throw new ProtocolException(String.format("Beta version of the protocol used (%s), but USE_BETA flag is unset", version),
                                            version);
            return decodedFlags;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> results)
        {
            Envelope envelope = decode(buffer);
            if (envelope == null)
                return;

            results.add(envelope);
        }

        private void fail()
        {
            // Reset to the initial state and throw the exception
            long tooLongTotalLength = this.tooLongTotalLength;
            this.tooLongTotalLength = 0;
            discardingTooLongMessage = false;
            String msg = String.format("Request is too big: length %d exceeds maximum allowed length %d.", tooLongTotalLength, MAX_TOTAL_LENGTH);
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
    public static class Encoder extends MessageToMessageEncoder<Envelope>
    {
        public static final Encoder instance = new Envelope.Encoder();
        private Encoder(){}

        public void encode(ChannelHandlerContext ctx, Envelope source, List<Object> results)
        {
            ByteBuf serializedHeader = source.encodeHeader();
            int messageSize = serializedHeader.readableBytes() + source.body.readableBytes();
            ClientMessageSizeMetrics.bytesSent.inc(messageSize);
            ClientMessageSizeMetrics.bytesSentPerResponse.update(messageSize);

            results.add(serializedHeader);
            results.add(source.body);
        }
    }

    @ChannelHandler.Sharable
    public static class Decompressor extends MessageToMessageDecoder<Envelope>
    {
        public static Decompressor instance = new Envelope.Decompressor();
        private Decompressor(){}

        public void decode(ChannelHandlerContext ctx, Envelope source, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            if (!source.header.flags.contains(Header.Flag.COMPRESSED) || connection == null)
            {
                results.add(source);
                return;
            }

            org.apache.cassandra.transport.Compressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(source);
                return;
            }

            results.add(compressor.decompress(source));
        }
    }

    @ChannelHandler.Sharable
    public static class Compressor extends MessageToMessageEncoder<Envelope>
    {
        public static Compressor instance = new Compressor();
        private Compressor(){}

        public void encode(ChannelHandlerContext ctx, Envelope source, List<Object> results)
        throws IOException
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();

            // Never compress STARTUP messages
            if (source.header.type == Message.Type.STARTUP || connection == null)
            {
                results.add(source);
                return;
            }

            org.apache.cassandra.transport.Compressor compressor = connection.getCompressor();
            if (compressor == null)
            {
                results.add(source);
                return;
            }
            source.header.flags.add(Header.Flag.COMPRESSED);
            results.add(compressor.compress(source));
        }
    }
}
