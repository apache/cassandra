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
package org.apache.cassandra.net;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.Message.validateLegacyProtocolMagic;
import static org.apache.cassandra.net.Crc.*;
import static org.apache.cassandra.net.OutboundConnectionSettings.*;

/**
 * Messages for the handshake phase of the internode protocol.
 *
 * The modern handshake is composed of 2 messages: Initiate and Accept
 * <p>
 * The legacy handshake is composed of 3 messages, the first being sent by the initiator of the connection. The other
 * side then answer with the 2nd message. At that point, if a version mismatch is detected by the connection initiator,
 * it will simply disconnect and reconnect with a more appropriate version. But if the version is acceptable, the connection
 * initiator sends the third message of the protocol, after which it considers the connection ready.
 */
@VisibleForTesting
public class HandshakeProtocol
{
    static final long TIMEOUT_MILLIS = 3 * DatabaseDescriptor.getRpcTimeout(MILLISECONDS);

    /**
     * The initial message sent when a node creates a new connection to a remote peer. This message contains:
     *   1) the {@link Message#PROTOCOL_MAGIC} number (4 bytes).
     *   2) the connection flags (4 bytes), which encodes:
     *      - the version the initiator thinks should be used for the connection (in practice, either the initiator
     *        version if it's the first time we connect to that remote since startup, or the last version known for that
     *        peer otherwise).
     *      - the "mode" of the connection: whether it is for streaming or for messaging.
     *      - whether compression should be used or not (if it is, compression is enabled _after_ the last message of the
     *        handshake has been sent).
     *   3) the connection initiator's broadcast address
     *   4) a CRC protecting the message from corruption
     * <p>
     * More precisely, connection flags:
     * <pre>
     * {@code
     *                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |C C C M C      |    REQUEST    |      MIN      |      MAX      |
     * |A A M O R      |    VERSION    |   SUPPORTED   |   SUPPORTED   |
     * |T T P D C      |  (DEPRECATED) |    VERSION    |    VERSION    |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * }
     * </pre>
     * CAT - QOS category, 2 bits: SMALL, LARGE, URGENT, or LEGACY (unset)
     * CMP - compression enabled bit
     * MOD - connection mode; if the bit is on, the connection is for streaming; if the bit is off, it is for inter-node messaging.
     * CRC - crc enabled bit
     * VERSION - {@link org.apache.cassandra.net.MessagingService#current_version}
     */
    static class Initiate
    {
        /** Contains the PROTOCOL_MAGIC (int) and the flags (int). */
        private static final int MIN_LENGTH = 8;
        private static final int MAX_LENGTH = 12 + InetAddressAndPort.Serializer.MAXIMUM_SIZE;

        @Deprecated // this is ignored by post40 nodes, i.e. if maxMessagingVersion is set
        final int requestMessagingVersion;
        // the messagingVersion bounds the sender will accept to initiate a connection;
        // if the remote peer supports any, the newest supported version will be selected; otherwise the nearest supported version
        final AcceptVersions acceptVersions;
        final ConnectionType type;
        final Framing framing;
        final InetAddressAndPort from;

        Initiate(int requestMessagingVersion, AcceptVersions acceptVersions, ConnectionType type, Framing framing, InetAddressAndPort from)
        {
            this.requestMessagingVersion = requestMessagingVersion;
            this.acceptVersions = acceptVersions;
            this.type = type;
            this.framing = framing;
            this.from = from;
        }

        @VisibleForTesting
        int encodeFlags()
        {
            int flags = 0;
            if (type.isMessaging())
                flags |= type.twoBitID();
            if (type.isStreaming())
                flags |= 1 << 3;

            // framing id is split over 2nd and 4th bits, for backwards compatibility
            flags |= ((framing.id & 1) << 2) | ((framing.id & 2) << 3);
            flags |= (requestMessagingVersion << 8);

            if (requestMessagingVersion < VERSION_40 || acceptVersions.max < VERSION_40)
                return flags; // for testing, permit serializing as though we are pre40

            flags |= (acceptVersions.min << 16);
            flags |= (acceptVersions.max << 24);
            return flags;
        }

        ByteBuf encode()
        {
            ByteBuffer buffer = BufferPools.forNetworking().get(MAX_LENGTH, BufferType.OFF_HEAP);
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(buffer))
            {
                out.writeInt(Message.PROTOCOL_MAGIC);
                out.writeInt(encodeFlags());

                if (requestMessagingVersion >= VERSION_40 && acceptVersions.max >= VERSION_40)
                {
                    inetAddressAndPortSerializer.serialize(from, out, requestMessagingVersion);
                    out.writeInt(computeCrc32(buffer, 0, buffer.position()));
                }
                buffer.flip();
                return GlobalBufferPoolAllocator.wrap(buffer);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        static Initiate maybeDecode(ByteBuf buf) throws IOException
        {
            if (buf.readableBytes() < MIN_LENGTH)
                return null;

            ByteBuffer nio = buf.nioBuffer();
            int start = nio.position();
            try (DataInputBuffer in = new DataInputBuffer(nio, false))
            {
                validateLegacyProtocolMagic(in.readInt());
                int flags = in.readInt();

                int requestedMessagingVersion = getBits(flags, 8, 8);
                int minMessagingVersion = getBits(flags, 16, 8);
                int maxMessagingVersion = getBits(flags, 24, 8);
                int framingBits = getBits(flags, 2, 1) | (getBits(flags, 4, 1) << 1);
                Framing framing = Framing.forId(framingBits);

                boolean isStream = getBits(flags, 3, 1) == 1;

                ConnectionType type = isStream
                                    ? ConnectionType.STREAMING
                                    : ConnectionType.fromId(getBits(flags, 0, 2));

                InetAddressAndPort from = null;

                if (requestedMessagingVersion >= VERSION_40 && maxMessagingVersion >= MessagingService.VERSION_40)
                {
                    from = inetAddressAndPortSerializer.deserialize(in, requestedMessagingVersion);

                    int computed = computeCrc32(nio, start, nio.position());
                    int read = in.readInt();
                    if (read != computed)
                        throw new InvalidCrc(read, computed);
                }

                buf.skipBytes(nio.position() - start);
                return new Initiate(requestedMessagingVersion,
                                    minMessagingVersion == 0 && maxMessagingVersion == 0
                                        ? null : new AcceptVersions(minMessagingVersion, maxMessagingVersion),
                                    type, framing, from);

            }
            catch (EOFException e)
            {
                return null;
            }
        }

        @VisibleForTesting
        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Initiate))
                return false;

            Initiate that = (Initiate)other;
            return    this.type == that.type
                   && this.framing == that.framing
                   && this.requestMessagingVersion == that.requestMessagingVersion
                   && Objects.equals(this.acceptVersions, that.acceptVersions);
        }

        @Override
        public String toString()
        {
            return String.format("Initiate(request: %d, min: %d, max: %d, type: %s, framing: %b, from: %s)",
                                 requestMessagingVersion,
                                 acceptVersions == null ? requestMessagingVersion : acceptVersions.min,
                                 acceptVersions == null ? requestMessagingVersion : acceptVersions.max,
                                 type, framing, from);
        }
    }


    /**
     * The second message of the handshake, sent by the node receiving the {@link Initiate} back to the
     * connection initiator.
     *
     * This message contains
     *   1) the messaging version of the peer sending this message
     *   2) the negotiated messaging version if one could be accepted by both peers,
     *      or if not the closest version that this peer could support to the ones requested
     *   3) a CRC protectingn the integrity of the message
     *
     * Note that the pre40 equivalent of this message contains ONLY the messaging version of the peer.
     */
    static class Accept
    {
        /** The messaging version sent by the receiving peer (int). */
        private static final int MAX_LENGTH = 12;

        final int useMessagingVersion;
        final int maxMessagingVersion;

        Accept(int useMessagingVersion, int maxMessagingVersion)
        {
            this.useMessagingVersion = useMessagingVersion;
            this.maxMessagingVersion = maxMessagingVersion;
        }

        ByteBuf encode(ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.directBuffer(MAX_LENGTH);
            buffer.clear();
            buffer.writeInt(maxMessagingVersion);
            buffer.writeInt(useMessagingVersion);
            buffer.writeInt(computeCrc32(buffer, 0, 8));
            return buffer;
        }

        /**
         * Respond to pre40 nodes only with our current messagingVersion
         */
        static ByteBuf respondPre40(int messagingVersion, ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.directBuffer(4);
            buffer.clear();
            buffer.writeInt(messagingVersion);
            return buffer;
        }

        static Accept maybeDecode(ByteBuf in, int handshakeMessagingVersion) throws InvalidCrc
        {
            int readerIndex = in.readerIndex();
            if (in.readableBytes() < 4)
                return null;
            int maxMessagingVersion = in.readInt();
            int useMessagingVersion = 0;

            // if the other node is pre-4.0, it will respond only with its maxMessagingVersion
            if (maxMessagingVersion < VERSION_40 || handshakeMessagingVersion < VERSION_40)
                return new Accept(useMessagingVersion, maxMessagingVersion);

            if (in.readableBytes() < 8)
            {
                in.readerIndex(readerIndex);
                return null;
            }
            useMessagingVersion = in.readInt();

            // verify crc
            int computed = computeCrc32(in, readerIndex, readerIndex + 8);
            int read = in.readInt();
            if (read != computed)
                throw new InvalidCrc(read, computed);

            return new Accept(useMessagingVersion, maxMessagingVersion);
        }

        @VisibleForTesting
        @Override
        public boolean equals(Object other)
        {
            return other instanceof Accept
                   && this.useMessagingVersion == ((Accept) other).useMessagingVersion
                   && this.maxMessagingVersion == ((Accept) other).maxMessagingVersion;
        }

        @Override
        public String toString()
        {
            return String.format("Accept(use: %d, max: %d)", useMessagingVersion, maxMessagingVersion);
        }
    }

    /**
     * The third message of the handshake, sent by pre40 nodes on reception of {@link Accept}.
     * This message contains:
     *   1) The connection initiator's {@link org.apache.cassandra.net.MessagingService#current_version} (4 bytes).
     *      This indicates the max messaging version supported by this node.
     *   2) The connection initiator's broadcast address as encoded by {@link InetAddressAndPort.Serializer}.
     *      This can be either 7 bytes for an IPv4 address, or 19 bytes for an IPv6 one, post40.
     *      This can be either 5 bytes for an IPv4 address, or 17 bytes for an IPv6 one, pre40.
     * <p>
     * This message concludes the legacy handshake protocol.
     */
    static class ConfirmOutboundPre40
    {
        private static final int MAX_LENGTH = 4 + InetAddressAndPort.Serializer.MAXIMUM_SIZE;

        final int maxMessagingVersion;
        final InetAddressAndPort from;

        ConfirmOutboundPre40(int maxMessagingVersion, InetAddressAndPort from)
        {
            this.maxMessagingVersion = maxMessagingVersion;
            this.from = from;
        }

        ByteBuf encode()
        {
            ByteBuffer buffer = BufferPools.forNetworking().get(MAX_LENGTH, BufferType.OFF_HEAP);
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(buffer))
            {
                out.writeInt(maxMessagingVersion);
                // pre-4.0 nodes should only receive the address, never port, and it's ok to hardcode VERSION_30
                inetAddressAndPortSerializer.serialize(from, out, VERSION_30);
                buffer.flip();
                return GlobalBufferPoolAllocator.wrap(buffer);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @SuppressWarnings("resource")
        static ConfirmOutboundPre40 maybeDecode(ByteBuf in)
        {
            ByteBuffer nio = in.nioBuffer();
            int start = nio.position();
            DataInputPlus input = new DataInputBuffer(nio, false);
            try
            {
                int version = input.readInt();
                InetAddressAndPort address = inetAddressAndPortSerializer.deserialize(input, version);
                in.skipBytes(nio.position() - start);
                return new ConfirmOutboundPre40(version, address);
            }
            catch (EOFException e)
            {
                // makes the assumption we didn't have enough bytes to deserialize an IPv6 address,
                // as we only check the MIN_LENGTH of the buf.
                return null;
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @VisibleForTesting
        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof ConfirmOutboundPre40))
                return false;

            ConfirmOutboundPre40 that = (ConfirmOutboundPre40) other;
            return this.maxMessagingVersion == that.maxMessagingVersion
                   && Objects.equals(this.from, that.from);
        }

        @Override
        public String toString()
        {
            return String.format("ConfirmOutboundPre40(maxMessagingVersion: %d; address: %s)", maxMessagingVersion, from);
        }
    }

    private static int getBits(int packed, int start, int count)
    {
        return (packed >>> start) & ~(-1 << count);
    }

}
