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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.tracing.Tracing.TraceType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClockTranslation;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.instance;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;
import static org.apache.cassandra.utils.vint.VIntCoding.computeUnsignedVIntSize;
import static org.apache.cassandra.utils.vint.VIntCoding.getUnsignedVInt;
import static org.apache.cassandra.utils.vint.VIntCoding.skipUnsignedVInt;

/**
 * Immutable main unit of internode communication - what used to be {@code MessageIn} and {@code MessageOut} fused
 * in one class.
 *
 * @param <T> The type of the message payload.
 */
public class Message<T>
{
    public final Header header;
    public final T payload;

    private Message(Header header, T payload)
    {
        this.header = header;
        this.payload = payload;
    }

    /** Sender of the message. */
    public InetAddressAndPort from()
    {
        return header.from;
    }

    /** Whether the message has crossed the node boundary, that is whether it originated from another node. */
    public boolean isCrossNode()
    {
        return !from().equals(FBUtilities.getBroadcastAddressAndPort());
    }

    /**
     * id of the request/message. In 4.0+ can be shared between multiple messages of the same logical request,
     * whilst in versions above a new id would be allocated for each message sent.
     */
    public long id()
    {
        return header.id;
    }

    public Verb verb()
    {
        return header.verb;
    }

    boolean isFailureResponse()
    {
        return verb() == Verb.FAILURE_RSP;
    }

    /**
     * Creation time of the message. If cross-node timeouts are enabled ({@link DatabaseDescriptor#hasCrossNodeTimeout()},
     * {@code deserialize()} will use the marshalled value, otherwise will use current time on the deserializing machine.
     */
    public long createdAtNanos()
    {
        return header.createdAtNanos;
    }

    public long expiresAtNanos()
    {
        return header.expiresAtNanos;
    }

    /** For how long the message has lived. */
    public long elapsedSinceCreated(TimeUnit units)
    {
        return units.convert(approxTime.now() - createdAtNanos(), NANOSECONDS);
    }

    public long creationTimeMillis()
    {
        return approxTime.translate().toMillisSinceEpoch(createdAtNanos());
    }

    /** Whether a failure response should be returned upon failure */
    boolean callBackOnFailure()
    {
        return header.callBackOnFailure();
    }

    /** See CASSANDRA-14145 */
    public boolean trackRepairedData()
    {
        return header.trackRepairedData();
    }

    /** Used for cross-DC write optimisation - pick one node in the DC and have it relay the write to its local peers */
    @Nullable
    public ForwardingInfo forwardTo()
    {
        return header.forwardTo();
    }

    /** The originator of the request - used when forwarding and will differ from {@link #from()} */
    @Nullable
    public InetAddressAndPort respondTo()
    {
        return header.respondTo();
    }

    @Nullable
    public UUID traceSession()
    {
        return header.traceSession();
    }

    @Nullable
    public TraceType traceType()
    {
        return header.traceType();
    }

    /*
     * request/response convenience
     */

    /**
     * Make a request {@link Message} with supplied verb and payload. Will fill in remaining fields
     * automatically.
     *
     * If you know that you will need to set some params or flags - prefer using variants of {@code out()}
     * that allow providing them at point of message constructions, rather than allocating new messages
     * with those added flags and params. See {@code outWithFlag()}, {@code outWithFlags()}, and {@code outWithParam()}
     * family.
     */
    public static <T> Message<T> out(Verb verb, T payload)
    {
        assert !verb.isResponse();

        return outWithParam(nextId(), verb, payload, null, null);
    }

    public static <T> Message<T> out(Verb verb, T payload, long expiresAtNanos)
    {
        return outWithParam(nextId(), verb, expiresAtNanos, payload, 0, null, null);
    }

    public static <T> Message<T> outWithFlag(Verb verb, T payload, MessageFlag flag)
    {
        assert !verb.isResponse();
        return outWithParam(nextId(), verb, 0, payload, flag.addTo(0), null, null);
    }

    public static <T> Message<T> outWithFlags(Verb verb, T payload, MessageFlag flag1, MessageFlag flag2)
    {
        assert !verb.isResponse();
        return outWithParam(nextId(), verb, 0, payload, flag2.addTo(flag1.addTo(0)), null, null);
    }

    static <T> Message<T> outWithParam(long id, Verb verb, T payload, ParamType paramType, Object paramValue)
    {
        return outWithParam(id, verb, 0, payload, paramType, paramValue);
    }

    private static <T> Message<T> outWithParam(long id, Verb verb, long expiresAtNanos, T payload, ParamType paramType, Object paramValue)
    {
        return outWithParam(id, verb, expiresAtNanos, payload, 0, paramType, paramValue);
    }

    private static <T> Message<T> outWithParam(long id, Verb verb, long expiresAtNanos, T payload, int flags, ParamType paramType, Object paramValue)
    {
        if (payload == null)
            throw new IllegalArgumentException();

        InetAddressAndPort from = FBUtilities.getBroadcastAddressAndPort();
        long createdAtNanos = approxTime.now();
        if (expiresAtNanos == 0)
            expiresAtNanos = verb.expiresAtNanos(createdAtNanos);

        return new Message<>(new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, buildParams(paramType, paramValue)), payload);
    }

    public static <T> Message<T> internalResponse(Verb verb, T payload)
    {
        assert verb.isResponse();
        return outWithParam(0, verb, payload, null, null);
    }

    /** Builds a response Message with provided payload, and all the right fields inferred from request Message */
    public <T> Message<T> responseWith(T payload)
    {
        return outWithParam(id(), verb().responseVerb, expiresAtNanos(), payload, null, null);
    }

    /** Builds a response Message with no payload, and all the right fields inferred from request Message */
    public Message<NoPayload> emptyResponse()
    {
        return responseWith(NoPayload.noPayload);
    }

    /** Builds a failure response Message with an explicit reason, and fields inferred from request Message */
    public Message<RequestFailureReason> failureResponse(RequestFailureReason reason)
    {
        return failureResponse(id(), expiresAtNanos(), reason);
    }

    static Message<RequestFailureReason> failureResponse(long id, long expiresAtNanos, RequestFailureReason reason)
    {
        return outWithParam(id, Verb.FAILURE_RSP, expiresAtNanos, reason, null, null);
    }

    Message<T> withCallBackOnFailure()
    {
        return new Message<>(header.withFlag(MessageFlag.CALL_BACK_ON_FAILURE), payload);
    }

    public Message<T> withForwardTo(ForwardingInfo peers)
    {
        return new Message<>(header.withParam(ParamType.FORWARD_TO, peers), payload);
    }

    private static final EnumMap<ParamType, Object> NO_PARAMS = new EnumMap<>(ParamType.class);

    private static Map<ParamType, Object> buildParams(ParamType type, Object value)
    {
        Map<ParamType, Object> params = NO_PARAMS;
        if (Tracing.isTracing())
            params = Tracing.instance.addTraceHeaders(new EnumMap<>(ParamType.class));

        if (type != null)
        {
            if (params.isEmpty())
                params = new EnumMap<>(ParamType.class);
            params.put(type, value);
        }

        return params;
    }

    private static Map<ParamType, Object> addParam(Map<ParamType, Object> params, ParamType type, Object value)
    {
        if (type == null)
            return params;

        params = new EnumMap<>(params);
        params.put(type, value);
        return params;
    }

    /*
     * id generation
     */

    private static final long NO_ID = 0L; // this is a valid ID for pre40 nodes

    private static final AtomicInteger nextId = new AtomicInteger(0);

    private static long nextId()
    {
        long id;
        do
        {
            id = nextId.incrementAndGet();
        }
        while (id == NO_ID);

        return id;
    }

    /**
     * WARNING: this is inaccurate for messages from pre40 nodes, which can use 0 as an id (but will do so rarely)
     */
    @VisibleForTesting
    boolean hasId()
    {
        return id() != NO_ID;
    }

    /** we preface every message with this number so the recipient can validate the sender is sane */
    static final int PROTOCOL_MAGIC = 0xCA552DFA;

    static void validateLegacyProtocolMagic(int magic) throws InvalidLegacyProtocolMagic
    {
        if (magic != PROTOCOL_MAGIC)
            throw new InvalidLegacyProtocolMagic(magic);
    }

    public static final class InvalidLegacyProtocolMagic extends IOException
    {
        public final int read;
        private InvalidLegacyProtocolMagic(int read)
        {
            super(String.format("Read %d, Expected %d", read, PROTOCOL_MAGIC));
            this.read = read;
        }
    }

    public String toString()
    {
        return "(from:" + from() + ", type:" + verb().stage + " verb:" + verb() + ')';
    }

    /**
     * Split into a separate object to allow partial message deserialization without wasting work and allocation
     * afterwards, if the entire message is necessary and available.
     */
    public static class Header
    {
        public final long id;
        public final Verb verb;
        public final InetAddressAndPort from;
        public final long createdAtNanos;
        public final long expiresAtNanos;
        private final int flags;
        private final Map<ParamType, Object> params;

        private Header(long id, Verb verb, InetAddressAndPort from, long createdAtNanos, long expiresAtNanos, int flags, Map<ParamType, Object> params)
        {
            this.id = id;
            this.verb = verb;
            this.from = from;
            this.createdAtNanos = createdAtNanos;
            this.expiresAtNanos = expiresAtNanos;
            this.flags = flags;
            this.params = params;
        }

        Header withFlag(MessageFlag flag)
        {
            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flag.addTo(flags), params);
        }

        Header withParam(ParamType type, Object value)
        {
            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, addParam(params, type, value));
        }

        boolean callBackOnFailure()
        {
            return MessageFlag.CALL_BACK_ON_FAILURE.isIn(flags);
        }

        boolean trackRepairedData()
        {
            return MessageFlag.TRACK_REPAIRED_DATA.isIn(flags);
        }

        @Nullable
        ForwardingInfo forwardTo()
        {
            return (ForwardingInfo) params.get(ParamType.FORWARD_TO);
        }

        @Nullable
        InetAddressAndPort respondTo()
        {
            return (InetAddressAndPort) params.get(ParamType.RESPOND_TO);
        }

        @Nullable
        public UUID traceSession()
        {
            return (UUID) params.get(ParamType.TRACE_SESSION);
        }

        @Nullable
        public TraceType traceType()
        {
            return (TraceType) params.getOrDefault(ParamType.TRACE_TYPE, TraceType.QUERY);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class Builder<T>
    {
        private Verb verb;
        private InetAddressAndPort from;
        private T payload;
        private int flags = 0;
        private final Map<ParamType, Object> params = new EnumMap<>(ParamType.class);
        private long createdAtNanos;
        private long expiresAtNanos;
        private long id;

        private boolean hasId;

        private Builder()
        {
        }

        public Builder<T> from(InetAddressAndPort from)
        {
            this.from = from;
            return this;
        }

        public Builder<T> withPayload(T payload)
        {
            this.payload = payload;
            return this;
        }

        public Builder<T> withFlag(MessageFlag flag)
        {
            flags = flag.addTo(flags);
            return this;
        }

        public Builder<T> withFlags(int flags)
        {
            this.flags = flags;
            return this;
        }

        public Builder<T> withParam(ParamType type, Object value)
        {
            params.put(type, value);
            return this;
        }

        /**
         * A shortcut to add tracing params.
         * Effectively, it is the same as calling {@link #withParam(ParamType, Object)} with tracing params
         * If there is already tracing params, calling this method overrides any existing ones.
         */
        public Builder<T> withTracingParams()
        {
            if (Tracing.isTracing())
                Tracing.instance.addTraceHeaders(params);
            return this;
        }

        public Builder<T> withoutParam(ParamType type)
        {
            params.remove(type);
            return this;
        }

        public Builder<T> withParams(Map<ParamType, Object> params)
        {
            this.params.putAll(params);
            return this;
        }

        public Builder<T> ofVerb(Verb verb)
        {
            this.verb = verb;
            if (expiresAtNanos == 0 && verb != null && createdAtNanos != 0)
                expiresAtNanos = verb.expiresAtNanos(createdAtNanos);
            if (!this.verb.isResponse() && from == null) // default to sending from self if we're a request verb
                from = FBUtilities.getBroadcastAddressAndPort();
            return this;
        }

        public Builder<T> withCreatedAt(long createdAtNanos)
        {
            this.createdAtNanos = createdAtNanos;
            if (expiresAtNanos == 0 && verb != null)
                expiresAtNanos = verb.expiresAtNanos(createdAtNanos);
            return this;
        }

        public Builder<T> withExpiresAt(long expiresAtNanos)
        {
            this.expiresAtNanos = expiresAtNanos;
            return this;
        }

        public Builder<T> withId(long id)
        {
            this.id = id;
            hasId = true;
            return this;
        }

        public Message<T> build()
        {
            if (verb == null)
                throw new IllegalArgumentException();
            if (from == null)
                throw new IllegalArgumentException();
            if (payload == null)
                throw new IllegalArgumentException();

            return new Message<>(new Header(hasId ? id : nextId(), verb, from, createdAtNanos, expiresAtNanos, flags, params), payload);
        }
    }

    public static <T> Builder<T> builder(Message<T> message)
    {
        return new Builder<T>().from(message.from())
                               .withId(message.id())
                               .ofVerb(message.verb())
                               .withCreatedAt(message.createdAtNanos())
                               .withExpiresAt(message.expiresAtNanos())
                               .withFlags(message.header.flags)
                               .withParams(message.header.params)
                               .withPayload(message.payload);
    }

    public static <T> Builder<T> builder(Verb verb, T payload)
    {
        return new Builder<T>().ofVerb(verb)
                               .withCreatedAt(approxTime.now())
                               .withPayload(payload);
    }

    public static final Serializer serializer = new Serializer();

    /**
     * Each message contains a header with several fixed fields, an optional key-value params section, and then
     * the message payload itself. Below is a visualization of the layout.
     *
     *  The params are prefixed by the count of key-value pairs; this value is encoded as unsigned vint.
     *  An individual param has an unsvint id (more specifically, a {@link ParamType}), and a byte array value.
     *  The param value is prefixed with it's length, encoded as an unsigned vint, followed by by the value's bytes.
     *
     * Legacy Notes (see {@link Serializer#serialize(Message, DataOutputPlus, int)} for complete details):
     * - pre 4.0, the IP address was sent along in the header, before the verb. The IP address may be either IPv4 (4 bytes) or IPv6 (16 bytes)
     * - pre-4.0, the verb was encoded as a 4-byte integer; in 4.0 and up it is an unsigned vint
     * - pre-4.0, the payloadSize was encoded as a 4-byte integer; in 4.0 and up it is an unsigned vint
     * - pre-4.0, the count of param key-value pairs was encoded as a 4-byte integer; in 4.0 and up it is an unsigned vint
     * - pre-4.0, param names were encoded as strings; in 4.0 they are encoded as enum id vints
     * - pre-4.0, expiry time wasn't encoded at all; in 4.0 it's an unsigned vint
     * - pre-4.0, message id was an int; in 4.0 and up it's an unsigned vint
     * - pre-4.0, messages included PROTOCOL MAGIC BYTES; post-4.0, we rely on frame CRCs instead
     * - pre-4.0, messages would serialize boolean params as dummy ONE_BYTEs; post-4.0 we have a dedicated 'flags' vint
     *
     * <pre>
     * {@code
     *            1 1 1 1 1 2 2 2 2 2 3
     *  0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Message ID (vint)             |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Creation timestamp (int)      |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Expiry (vint)                 |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Verb (vint)                   |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Flags (vint)                  |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Param count (vint)            |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                               /
     * /           Params              /
     * /                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Payload size (vint)           |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                               /
     * /           Payload             /
     * /                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * }
     * </pre>
     */
    public static final class Serializer
    {
        private static final int CREATION_TIME_SIZE = 4;

        private Serializer()
        {
        }

        public <T> void serialize(Message<T> message, DataOutputPlus out, int version) throws IOException
        {
            if (version >= VERSION_40)
                serializePost40(message, out, version);
            else
                serializePre40(message, out, version);
        }

        public <T> Message<T> deserialize(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            return version >= VERSION_40 ? deserializePost40(in, peer, version) : deserializePre40(in, version);
        }

        /**
         * A partial variant of deserialize, taking in a previously deserialized {@link Header} as an argument.
         *
         * Skip deserializing the {@link Header} from the input stream in favour of using the provided header.
         */
        public <T> Message<T> deserialize(DataInputPlus in, Header header, int version) throws IOException
        {
            return version >= VERSION_40 ? deserializePost40(in, header, version) : deserializePre40(in, header, version);
        }

        private <T> int serializedSize(Message<T> message, int version)
        {
            return version >= VERSION_40 ? serializedSizePost40(message, version) : serializedSizePre40(message, version);
        }

        /**
         * Size of the next message in the stream. Returns -1 if there aren't sufficient bytes read yet to determine size.
         */
        int inferMessageSize(ByteBuffer buf, int index, int limit, int version) throws InvalidLegacyProtocolMagic
        {
            int size = version >= VERSION_40 ? inferMessageSizePost40(buf, index, limit) : inferMessageSizePre40(buf, index, limit);
            if (size > DatabaseDescriptor.getInternodeMaxMessageSizeInBytes())
                throw new OversizedMessageException(size);
            return size;
        }

        /**
         * Partially deserialize the message - by only extracting the header and leaving the payload alone.
         *
         * To get the rest of the message without repeating the work done here, use {@link #deserialize(DataInputPlus, Header, int)}
         * method.
         *
         * It's assumed that the provided buffer contains all the bytes necessary to deserialize the header fully.
         */
        Header extractHeader(ByteBuffer buf, InetAddressAndPort from, long currentTimeNanos, int version) throws IOException
        {
            return version >= VERSION_40
                 ? extractHeaderPost40(buf, from, currentTimeNanos, version)
                 : extractHeaderPre40(buf, currentTimeNanos, version);
        }

        private static long getExpiresAtNanos(long createdAtNanos, long currentTimeNanos, long expirationPeriodNanos)
        {
            if (!DatabaseDescriptor.hasCrossNodeTimeout() || createdAtNanos > currentTimeNanos)
                createdAtNanos = currentTimeNanos;
            return createdAtNanos + expirationPeriodNanos;
        }

        /*
         * 4.0 ser/deser
         */

        private void serializeHeaderPost40(Header header, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(header.id);
            // int cast cuts off the high-order half of the timestamp, which we can assume remains
            // the same between now and when the recipient reconstructs it.
            out.writeInt((int) approxTime.translate().toMillisSinceEpoch(header.createdAtNanos));
            out.writeUnsignedVInt(NANOSECONDS.toMillis(header.expiresAtNanos - header.createdAtNanos));
            out.writeUnsignedVInt(header.verb.id);
            out.writeUnsignedVInt(header.flags);
            serializeParams(header.params, out, version);
        }

        private Header deserializeHeaderPost40(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            long id = in.readUnsignedVInt();
            long currentTimeNanos = approxTime.now();
            MonotonicClockTranslation timeSnapshot = approxTime.translate();
            long creationTimeNanos = calculateCreationTimeNanos(in.readInt(), timeSnapshot, currentTimeNanos);
            long expiresAtNanos = getExpiresAtNanos(creationTimeNanos, currentTimeNanos, TimeUnit.MILLISECONDS.toNanos(in.readUnsignedVInt()));
            Verb verb = Verb.fromId(Ints.checkedCast(in.readUnsignedVInt()));
            int flags = Ints.checkedCast(in.readUnsignedVInt());
            Map<ParamType, Object> params = deserializeParams(in, version);
            return new Header(id, verb, peer, creationTimeNanos, expiresAtNanos, flags, params);
        }

        private void skipHeaderPost40(DataInputPlus in) throws IOException
        {
            skipUnsignedVInt(in); // id
            in.skipBytesFully(4); // createdAt
            skipUnsignedVInt(in); // expiresIn
            skipUnsignedVInt(in); // verb
            skipUnsignedVInt(in); // flags
            skipParamsPost40(in); // params
        }

        private int serializedHeaderSizePost40(Header header, int version)
        {
            long size = 0;
            size += sizeofUnsignedVInt(header.id);
            size += CREATION_TIME_SIZE;
            size += sizeofUnsignedVInt(1 + NANOSECONDS.toMillis(header.expiresAtNanos - header.createdAtNanos));
            size += sizeofUnsignedVInt(header.verb.id);
            size += sizeofUnsignedVInt(header.flags);
            size += serializedParamsSize(header.params, version);
            return Ints.checkedCast(size);
        }

        private Header extractHeaderPost40(ByteBuffer buf, InetAddressAndPort from, long currentTimeNanos, int version) throws IOException
        {
            MonotonicClockTranslation timeSnapshot = approxTime.translate();

            int index = buf.position();

            long id = getUnsignedVInt(buf, index);
            index += computeUnsignedVIntSize(id);

            int createdAtMillis = buf.getInt(index);
            index += sizeof(createdAtMillis);

            long expiresInMillis = getUnsignedVInt(buf, index);
            index += computeUnsignedVIntSize(expiresInMillis);

            Verb verb = Verb.fromId(Ints.checkedCast(getUnsignedVInt(buf, index)));
            index += computeUnsignedVIntSize(verb.id);

            int flags = Ints.checkedCast(getUnsignedVInt(buf, index));
            index += computeUnsignedVIntSize(flags);

            Map<ParamType, Object> params = extractParams(buf, index, version);

            long createdAtNanos = calculateCreationTimeNanos(createdAtMillis, timeSnapshot, currentTimeNanos);
            long expiresAtNanos = getExpiresAtNanos(createdAtNanos, currentTimeNanos, TimeUnit.MILLISECONDS.toNanos(expiresInMillis));

            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, params);
        }

        private <T> void serializePost40(Message<T> message, DataOutputPlus out, int version) throws IOException
        {
            serializeHeaderPost40(message.header, out, version);
            out.writeUnsignedVInt(message.payloadSize(version));
            message.verb().serializer().serialize(message.payload, out, version);
        }

        private <T> Message<T> deserializePost40(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            Header header = deserializeHeaderPost40(in, peer, version);
            skipUnsignedVInt(in); // payload size, not needed by payload deserializer
            T payload = (T) header.verb.serializer().deserialize(in, version);
            return new Message<>(header, payload);
        }

        private <T> Message<T> deserializePost40(DataInputPlus in, Header header, int version) throws IOException
        {
            skipHeaderPost40(in);
            skipUnsignedVInt(in); // payload size, not needed by payload deserializer
            T payload = (T) header.verb.serializer().deserialize(in, version);
            return new Message<>(header, payload);
        }

        private <T> int serializedSizePost40(Message<T> message, int version)
        {
            long size = 0;
            size += serializedHeaderSizePost40(message.header, version);
            int payloadSize = message.payloadSize(version);
            size += sizeofUnsignedVInt(payloadSize) + payloadSize;
            return Ints.checkedCast(size);
        }

        private int inferMessageSizePost40(ByteBuffer buf, int readerIndex, int readerLimit)
        {
            int index = readerIndex;

            int idSize = computeUnsignedVIntSize(buf, index, readerLimit);
            if (idSize < 0)
                return -1; // not enough bytes to read id
            index += idSize;

            index += CREATION_TIME_SIZE;
            if (index > readerLimit)
                return -1;

            int expirationSize = computeUnsignedVIntSize(buf, index, readerLimit);
            if (expirationSize < 0)
                return -1;
            index += expirationSize;

            int verbIdSize = computeUnsignedVIntSize(buf, index, readerLimit);
            if (verbIdSize < 0)
                return -1;
            index += verbIdSize;

            int flagsSize = computeUnsignedVIntSize(buf, index, readerLimit);
            if (flagsSize < 0)
                return -1;
            index += flagsSize;

            int paramsSize = extractParamsSizePost40(buf, index, readerLimit);
            if (paramsSize < 0)
                return -1;
            index += paramsSize;

            long payloadSize = getUnsignedVInt(buf, index, readerLimit);
            if (payloadSize < 0)
                return -1;
            index += computeUnsignedVIntSize(payloadSize) + payloadSize;

            return index - readerIndex;
        }

        /*
         * legacy ser/deser
         */

        private void serializeHeaderPre40(Header header, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(PROTOCOL_MAGIC);
            out.writeInt(Ints.checkedCast(header.id));
            // int cast cuts off the high-order half of the timestamp, which we can assume remains
            // the same between now and when the recipient reconstructs it.
            out.writeInt((int) approxTime.translate().toMillisSinceEpoch(header.createdAtNanos));
            inetAddressAndPortSerializer.serialize(header.from, out, version);
            out.writeInt(header.verb.toPre40Verb().id);
            serializeParams(addFlagsToLegacyParams(header.params, header.flags), out, version);
        }

        private Header deserializeHeaderPre40(DataInputPlus in, int version) throws IOException
        {
            validateLegacyProtocolMagic(in.readInt());
            int id = in.readInt();
            long currentTimeNanos = approxTime.now();
            MonotonicClockTranslation timeSnapshot = approxTime.translate();
            long creationTimeNanos = calculateCreationTimeNanos(in.readInt(), timeSnapshot, currentTimeNanos);
            InetAddressAndPort from = inetAddressAndPortSerializer.deserialize(in, version);
            Verb verb = Verb.fromId(in.readInt());
            Map<ParamType, Object> params = deserializeParams(in, version);
            int flags = removeFlagsFromLegacyParams(params);
            return new Header(id, verb, from, creationTimeNanos, verb.expiresAtNanos(creationTimeNanos), flags, params);
        }

        private static final int PRE_40_MESSAGE_PREFIX_SIZE = 12; // protocol magic + id + createdAt

        private void skipHeaderPre40(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(PRE_40_MESSAGE_PREFIX_SIZE); // magic, id, createdAt
            in.skipBytesFully(in.readByte());              // from
            in.skipBytesFully(4);                          // verb
            skipParamsPre40(in);                           // params
        }

        private int serializedHeaderSizePre40(Header header, int version)
        {
            long size = 0;
            size += PRE_40_MESSAGE_PREFIX_SIZE;
            size += inetAddressAndPortSerializer.serializedSize(header.from, version);
            size += sizeof(header.verb.id);
            size += serializedParamsSize(addFlagsToLegacyParams(header.params, header.flags), version);
            return Ints.checkedCast(size);
        }

        private Header extractHeaderPre40(ByteBuffer buf, long currentTimeNanos, int version) throws IOException
        {
            MonotonicClockTranslation timeSnapshot = approxTime.translate();

            int index = buf.position();

            index += 4; // protocol magic

            long id = buf.getInt(index);
            index += 4;

            int createdAtMillis = buf.getInt(index);
            index += 4;

            InetAddressAndPort from = inetAddressAndPortSerializer.extract(buf, index);
            index += 1 + buf.get(index);

            Verb verb = Verb.fromId(buf.getInt(index));
            index += 4;

            Map<ParamType, Object> params = extractParams(buf, index, version);
            int flags = removeFlagsFromLegacyParams(params);

            long createdAtNanos = calculateCreationTimeNanos(createdAtMillis, timeSnapshot, currentTimeNanos);
            long expiresAtNanos = verb.expiresAtNanos(createdAtNanos);

            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, params);
        }

        private <T> void serializePre40(Message<T> message, DataOutputPlus out, int version) throws IOException
        {
            if (message.isFailureResponse())
                message = toPre40FailureResponse(message);

            serializeHeaderPre40(message.header, out, version);

            if (message.payload != null && message.payload != NoPayload.noPayload)
            {
                int payloadSize = message.payloadSize(version);
                out.writeInt(payloadSize);
                message.verb().serializer().serialize(message.payload, out, version);
            }
            else
            {
                out.writeInt(0);
            }
        }

        private <T> Message<T> deserializePre40(DataInputPlus in, int version) throws IOException
        {
            Header header = deserializeHeaderPre40(in, version);
            return deserializePre40(in, header, false, version);
        }

        private <T> Message<T> deserializePre40(DataInputPlus in, Header header, int version) throws IOException
        {
            return deserializePre40(in, header, true, version);
        }

        private <T> Message<T> deserializePre40(DataInputPlus in, Header header, boolean skipHeader, int version) throws IOException
        {
            if (skipHeader)
                skipHeaderPre40(in);

            IVersionedAsymmetricSerializer<?, T> payloadSerializer = header.verb.serializer();
            if (null == payloadSerializer)
                payloadSerializer = instance().callbacks.responseSerializer(header.id, header.from);
            int payloadSize = in.readInt();
            T payload = deserializePayloadPre40(in, version, payloadSerializer, payloadSize);

            Message<T> message = new Message<>(header, payload);

            return header.params.containsKey(ParamType.FAILURE_RESPONSE)
                 ? (Message<T>) toPost40FailureResponse(message)
                 : message;
        }

        private <T> T deserializePayloadPre40(DataInputPlus in, int version, IVersionedAsymmetricSerializer<?, T> serializer, int payloadSize) throws IOException
        {
            if (payloadSize == 0 || serializer == null)
            {
                // if there's no deserializer for the verb, skip the payload bytes to leave
                // the stream in a clean state (for the next message)
                in.skipBytesFully(payloadSize);
                return null;
            }

            return serializer.deserialize(in, version);
        }

        private <T> int serializedSizePre40(Message<T> message, int version)
        {
            if (message.isFailureResponse())
                message = toPre40FailureResponse(message);

            long size = 0;
            size += serializedHeaderSizePre40(message.header, version);
            int payloadSize = message.payloadSize(version);
            size += sizeof(payloadSize);
            size += payloadSize;
            return Ints.checkedCast(size);
        }

        private int inferMessageSizePre40(ByteBuffer buf, int readerIndex, int readerLimit) throws InvalidLegacyProtocolMagic
        {
            int index = readerIndex;
            // protocol magic
            index += 4;
            if (index > readerLimit)
                return -1;
            validateLegacyProtocolMagic(buf.getInt(index - 4));

            // rest of prefix
            index += PRE_40_MESSAGE_PREFIX_SIZE - 4;
            // ip address
            index += 1;
            if (index > readerLimit)
                return -1;
            index += buf.get(index - 1);
            // verb
            index += 4;
            if (index > readerLimit)
                return -1;

            int paramsSize = extractParamsSizePre40(buf, index, readerLimit);
            if (paramsSize < 0)
                return -1;
            index += paramsSize;

            // payload
            index += 4;

            if (index > readerLimit)
                return -1;
            index += buf.getInt(index - 4);

            return index - readerIndex;
        }

        private Message toPre40FailureResponse(Message post40)
        {
            Map<ParamType, Object> params = new EnumMap<>(ParamType.class);
            params.putAll(post40.header.params);

            params.put(ParamType.FAILURE_RESPONSE, LegacyFlag.instance);
            params.put(ParamType.FAILURE_REASON, post40.payload);

            Header header = new Header(post40.id(), post40.verb().toPre40Verb(), post40.from(), post40.createdAtNanos(), post40.expiresAtNanos(), 0, params);
            return new Message<>(header, NoPayload.noPayload);
        }

        private Message<RequestFailureReason> toPost40FailureResponse(Message<?> pre40)
        {
            Map<ParamType, Object> params = new EnumMap<>(ParamType.class);
            params.putAll(pre40.header.params);

            params.remove(ParamType.FAILURE_RESPONSE);

            RequestFailureReason reason = (RequestFailureReason) params.remove(ParamType.FAILURE_REASON);
            if (null == reason)
                reason = RequestFailureReason.UNKNOWN;

            Header header = new Header(pre40.id(), Verb.FAILURE_RSP, pre40.from(), pre40.createdAtNanos(), pre40.expiresAtNanos(), pre40.header.flags, params);
            return new Message<>(header, reason);
        }

        /*
         * created at + cross-node
         */

        private static final long TIMESTAMP_WRAPAROUND_GRACE_PERIOD_START  = 0xFFFFFFFFL - MINUTES.toMillis(15L);
        private static final long TIMESTAMP_WRAPAROUND_GRACE_PERIOD_END    =               MINUTES.toMillis(15L);

        private static long calculateCreationTimeNanos(int messageTimestampMillis, MonotonicClockTranslation timeSnapshot, long currentTimeNanos)
        {
            long currentTimeMillis = timeSnapshot.toMillisSinceEpoch(currentTimeNanos);
            // Reconstruct the message construction time sent by the remote host (we sent only the lower 4 bytes, assuming the
            // higher 4 bytes wouldn't change between the sender and receiver)
            long highBits = currentTimeMillis & 0xFFFFFFFF00000000L;

            long sentLowBits = messageTimestampMillis & 0x00000000FFFFFFFFL;
            long currentLowBits =   currentTimeMillis & 0x00000000FFFFFFFFL;

            // if our sent bits occur within a grace period of a wrap around event,
            // and our current bits are no more than the same grace period after a wrap around event,
            // assume a wrap around has occurred, and deduct one highBit
            if (      sentLowBits > TIMESTAMP_WRAPAROUND_GRACE_PERIOD_START
                      && currentLowBits < TIMESTAMP_WRAPAROUND_GRACE_PERIOD_END)
            {
                highBits -= 0x0000000100000000L;
            }

            long sentTimeMillis = (highBits | sentLowBits);
            return timeSnapshot.fromMillisSinceEpoch(sentTimeMillis);
        }

        /*
         * param ser/deser
         */

        private Map<ParamType, Object> addFlagsToLegacyParams(Map<ParamType, Object> params, int flags)
        {
            if (flags == 0)
                return params;

            Map<ParamType, Object> extended = new EnumMap<>(ParamType.class);
            extended.putAll(params);

            if (MessageFlag.CALL_BACK_ON_FAILURE.isIn(flags))
                extended.put(ParamType.FAILURE_CALLBACK, LegacyFlag.instance);

            if (MessageFlag.TRACK_REPAIRED_DATA.isIn(flags))
                extended.put(ParamType.TRACK_REPAIRED_DATA, LegacyFlag.instance);

            return extended;
        }

        private int removeFlagsFromLegacyParams(Map<ParamType, Object> params)
        {
            int flags = 0;

            if (null != params.remove(ParamType.FAILURE_CALLBACK))
                flags = MessageFlag.CALL_BACK_ON_FAILURE.addTo(flags);

            if (null != params.remove(ParamType.TRACK_REPAIRED_DATA))
                flags = MessageFlag.TRACK_REPAIRED_DATA.addTo(flags);

            return flags;
        }

        private void serializeParams(Map<ParamType, Object> params, DataOutputPlus out, int version) throws IOException
        {
            if (version >= VERSION_40)
                out.writeUnsignedVInt(params.size());
            else
                out.writeInt(params.size());

            for (Map.Entry<ParamType, Object> kv : params.entrySet())
            {
                ParamType type = kv.getKey();
                if (version >= VERSION_40)
                    out.writeUnsignedVInt(type.id);
                else
                    out.writeUTF(type.legacyAlias);

                IVersionedSerializer serializer = type.serializer;
                Object value = kv.getValue();

                int length = Ints.checkedCast(serializer.serializedSize(value, version));
                if (version >= VERSION_40)
                    out.writeUnsignedVInt(length);
                else
                    out.writeInt(length);

                serializer.serialize(value, out, version);
            }
        }

        private Map<ParamType, Object> deserializeParams(DataInputPlus in, int version) throws IOException
        {
            int count = version >= VERSION_40 ? Ints.checkedCast(in.readUnsignedVInt()) : in.readInt();

            if (count == 0)
                return NO_PARAMS;

            Map<ParamType, Object> params = new EnumMap<>(ParamType.class);

            for (int i = 0; i < count; i++)
            {
                ParamType type = version >= VERSION_40
                    ? ParamType.lookUpById(Ints.checkedCast(in.readUnsignedVInt()))
                    : ParamType.lookUpByAlias(in.readUTF());

                int length = version >= VERSION_40
                    ? Ints.checkedCast(in.readUnsignedVInt())
                    : in.readInt();

                if (null != type)
                    params.put(type, type.serializer.deserialize(in, version));
                else
                    in.skipBytesFully(length); // forward compatibiliy with minor version changes
            }

            return params;
        }

        /*
         * Extract post-4.0 params map from a ByteBuffer without modifying it.
         */
        private Map<ParamType, Object> extractParams(ByteBuffer buf, int readerIndex, int version) throws IOException
        {
            long count = version >= VERSION_40 ? getUnsignedVInt(buf, readerIndex) : buf.getInt(readerIndex);

            if (count == 0)
                return NO_PARAMS;

            final int position = buf.position();
            buf.position(readerIndex);

            try (DataInputBuffer in = new DataInputBuffer(buf, false))
            {
                return deserializeParams(in, version);
            }
            finally
            {
                buf.position(position);
            }
        }

        private void skipParamsPost40(DataInputPlus in) throws IOException
        {
            int count = Ints.checkedCast(in.readUnsignedVInt());

            for (int i = 0; i < count; i++)
            {
                skipUnsignedVInt(in);
                in.skipBytesFully(Ints.checkedCast(in.readUnsignedVInt()));
            }
        }

        private void skipParamsPre40(DataInputPlus in) throws IOException
        {
            int count = in.readInt();

            for (int i = 0; i < count; i++)
            {
                in.skipBytesFully(in.readShort());
                in.skipBytesFully(in.readInt());
            }
        }

        private long serializedParamsSize(Map<ParamType, Object> params, int version)
        {
            long size = version >= VERSION_40
                      ? computeUnsignedVIntSize(params.size())
                      : sizeof(params.size());

            for (Map.Entry<ParamType, Object> kv : params.entrySet())
            {
                ParamType type = kv.getKey();
                Object value = kv.getValue();

                long valueLength = type.serializer.serializedSize(value, version);

                if (version >= VERSION_40)
                    size += sizeofUnsignedVInt(type.id) + sizeofUnsignedVInt(valueLength);
                else
                    size += sizeof(type.legacyAlias) + 4;

                size += valueLength;
            }

            return size;
        }

        private int extractParamsSizePost40(ByteBuffer buf, int readerIndex, int readerLimit)
        {
            int index = readerIndex;

            long paramsCount = getUnsignedVInt(buf, index, readerLimit);
            if (paramsCount < 0)
                return -1;
            index += computeUnsignedVIntSize(paramsCount);

            for (int i = 0; i < paramsCount; i++)
            {
                long type = getUnsignedVInt(buf, index, readerLimit);
                if (type < 0)
                    return -1;
                index += computeUnsignedVIntSize(type);

                long length = getUnsignedVInt(buf, index, readerLimit);
                if (length < 0)
                    return -1;
                index += computeUnsignedVIntSize(length) + length;
            }

            return index - readerIndex;
        }

        private int extractParamsSizePre40(ByteBuffer buf, int readerIndex, int readerLimit)
        {
            int index = readerIndex;

            index += 4;
            if (index > readerLimit)
                return -1;
            int paramsCount = buf.getInt(index - 4);

            for (int i = 0; i < paramsCount; i++)
            {
                // try to read length and skip to the end of the param name
                index += 2;

                if (index > readerLimit)
                    return -1;
                index += buf.getShort(index - 2);
                // try to read length and skip to the end of the param value
                index += 4;
                if (index > readerLimit)
                    return -1;
                index += buf.getInt(index - 4);
            }

            return index - readerIndex;
        }

        private <T> int payloadSize(Message<T> message, int version)
        {
            long payloadSize = message.payload != null && message.payload != NoPayload.noPayload
                             ? message.verb().serializer().serializedSize(message.payload, version)
                             : 0;
            return Ints.checkedCast(payloadSize);
        }
    }

    private int serializedSize30;
    private int serializedSize3014;
    private int serializedSize40;

    /**
     * Serialized size of the entire message, for the provided messaging version. Caches the calculated value.
     */
    public int serializedSize(int version)
    {
        switch (version)
        {
            case VERSION_30:
                if (serializedSize30 == 0)
                    serializedSize30 = serializer.serializedSize(this, VERSION_30);
                return serializedSize30;
            case VERSION_3014:
                if (serializedSize3014 == 0)
                    serializedSize3014 = serializer.serializedSize(this, VERSION_3014);
                return serializedSize3014;
            case VERSION_40:
                if (serializedSize40 == 0)
                    serializedSize40 = serializer.serializedSize(this, VERSION_40);
                return serializedSize40;
            default:
                throw new IllegalStateException();
        }
    }

    private int payloadSize30   = -1;
    private int payloadSize3014 = -1;
    private int payloadSize40   = -1;

    private int payloadSize(int version)
    {
        switch (version)
        {
            case VERSION_30:
                if (payloadSize30 < 0)
                    payloadSize30 = serializer.payloadSize(this, VERSION_30);
                return payloadSize30;
            case VERSION_3014:
                if (payloadSize3014 < 0)
                    payloadSize3014 = serializer.payloadSize(this, VERSION_3014);
                return payloadSize3014;
            case VERSION_40:
                if (payloadSize40 < 0)
                    payloadSize40 = serializer.payloadSize(this, VERSION_40);
                return payloadSize40;
            default:
                throw new IllegalStateException();
        }
    }

    static class OversizedMessageException extends RuntimeException
    {
        OversizedMessageException(int size)
        {
            super("Message of size " + size + " bytes exceeds allowed maximum of " + DatabaseDescriptor.getInternodeMaxMessageSizeInBytes() + " bytes");
        }
    }
}
