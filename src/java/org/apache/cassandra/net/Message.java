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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.VERSION_50;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.vint.VIntCoding.*;

/**
 * Immutable main unit of internode communication - what used to be {@code MessageIn} and {@code MessageOut} fused
 * in one class.
 *
 * @param <T> The type of the message payload.
 */
public class Message<T>
{
    private static final Logger logger = LoggerFactory.getLogger(Message.class);
    private static final NoSpamLogger noSpam1m = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public final Header header;
    public final T payload;

    Message(Header header, T payload)
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
        return !from().equals(getBroadcastAddressAndPort());
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

    public boolean isFailureResponse()
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

    public boolean trackWarnings()
    {
        return header.trackWarnings();
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
    public TimeUUID traceSession()
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

    public static <T> Message<T> synthetic(InetAddressAndPort from, Verb verb, T payload)
    {
        return new Message<>(new Header(-1, verb, from, -1, -1, 0, NO_PARAMS), payload);
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

    @VisibleForTesting
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
        return withParam(getBroadcastAddressAndPort(), id, verb, expiresAtNanos, payload, flags, paramType, paramValue);
    }

    private static <T> Message<T> withParam(InetAddressAndPort from, long id, Verb verb, long expiresAtNanos, T payload, int flags, ParamType paramType, Object paramValue)
    {
        if (payload == null)
            throw new IllegalArgumentException();

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

    /**
     * Used by the {@code MultiRangeReadCommand} to split multi-range responses from a replica
     * into single-range responses.
     */
    public static <T> Message<T> remoteResponse(InetAddressAndPort from, Verb verb, T payload)
    {
        assert verb.isResponse();
        long createdAtNanos = approxTime.now();
        long expiresAtNanos = verb.expiresAtNanos(createdAtNanos);
        return new Message<>(new Header(0, verb, from, createdAtNanos, expiresAtNanos, 0, NO_PARAMS), payload);
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

    public <V> Message<V> withPayload(V newPayload)
    {
        return new Message<>(header, newPayload);
    }

    Message<T> withCallBackOnFailure()
    {
        return new Message<>(header.withFlag(MessageFlag.CALL_BACK_ON_FAILURE), payload);
    }

    public Message<T> withForwardTo(ForwardingInfo peers)
    {
        return new Message<>(header.withParam(ParamType.FORWARD_TO, peers), payload);
    }

    public Message<T> withFrom(InetAddressAndPort from)
    {
        return new Message<>(header.withFrom(from), payload);
    }

    public Message<T> withFlag(MessageFlag flag)
    {
        return new Message<>(header.withFlag(flag), payload);
    }

    public Message<T> withParam(ParamType type, Object value)
    {
        return new Message<>(header.withParam(type, value), payload);
    }

    public Message<T> withParams(Map<ParamType, Object> values)
    {
        if (values == null || values.isEmpty())
            return this;
        return new Message<>(header.withParams(values), payload);
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

    private static Map<ParamType, Object> addParams(Map<ParamType, Object> params, Map<ParamType, Object> values)
    {
        if (values == null || values.isEmpty())
            return params;

        params = new EnumMap<>(params);
        params.putAll(values);
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
            this.expiresAtNanos = expiresAtNanos;
            this.createdAtNanos = createdAtNanos;
            this.flags = flags;
            this.params = params;
        }

        Header withFrom(InetAddressAndPort from)
        {
            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, params);
        }

        Header withFlag(MessageFlag flag)
        {
            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flag.addTo(flags), params);
        }

        Header withParam(ParamType type, Object value)
        {
            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, addParam(params, type, value));
        }

        Header withParams(Map<ParamType, Object> values)
        {
            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, addParams(params, values));
        }

        boolean callBackOnFailure()
        {
            return MessageFlag.CALL_BACK_ON_FAILURE.isIn(flags);
        }

        boolean trackRepairedData()
        {
            return MessageFlag.TRACK_REPAIRED_DATA.isIn(flags);
        }

        boolean trackWarnings()
        {
            return MessageFlag.TRACK_WARNINGS.isIn(flags);
        }

        @Nullable
        ForwardingInfo forwardTo()
        {
            return (ForwardingInfo) params.get(ParamType.FORWARD_TO);
        }

        @Nullable
        InetAddressAndPort respondTo()
        {
            InetAddressAndPort respondTo = (InetAddressAndPort) params.get(ParamType.RESPOND_TO);
            if (respondTo == null) respondTo = from;
            return respondTo;
        }

        @Nullable
        public TimeUUID traceSession()
        {
            return (TimeUUID) params.get(ParamType.TRACE_SESSION);
        }

        @Nullable
        public TraceType traceType()
        {
            return (TraceType) params.getOrDefault(ParamType.TRACE_TYPE, TraceType.QUERY);
        }

        public Map<ParamType, Object> params()
        {
            return Collections.unmodifiableMap(params);
        }

        @Nullable
        public Map<String,byte[]> customParams()
        {
            return (Map<String,byte[]>) params.get(ParamType.CUSTOM_MAP);
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

        public Builder<T> withCustomParam(String name, byte[] value)
        {
            Map<String,byte[]> customParams  = (Map<String,byte[]>)
                    params.computeIfAbsent(ParamType.CUSTOM_MAP, (t) -> new HashMap<String,byte[]>());

            customParams.put(name, value);
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
                from = getBroadcastAddressAndPort();
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
            serializeHeader(message.header, out, version);
            out.writeUnsignedVInt32(message.payloadSize(version));
            message.verb().serializer().serialize(message.payload, out, version);
        }

        public <T> Message<T> deserialize(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            Header header = deserializeHeader(in, peer, version);
            skipUnsignedVInt(in); // payload size, not needed by payload deserializer
            T payload = (T) header.verb.serializer().deserialize(in, version);
            return new Message<>(header, payload);
        }

        /**
         * A partial variant of deserialize, taking in a previously deserialized {@link Header} as an argument.
         *
         * Skip deserializing the {@link Header} from the input stream in favour of using the provided header.
         */
        public <T> Message<T> deserialize(DataInputPlus in, Header header, int version) throws IOException
        {
            skipHeader(in);
            skipUnsignedVInt(in); // payload size, not needed by payload deserializer
            T payload = (T) header.verb.serializer().deserialize(in, version);
            return new Message<>(header, payload);
        }

        private <T> int serializedSize(Message<T> message, int version)
        {
            long size = 0;
            size += serializedHeader(message.header, version);
            int payloadSize = message.payloadSize(version);
            size += sizeofUnsignedVInt(payloadSize) + payloadSize;
            return Ints.checkedCast(size);
        }

        /**
         * Size of the next message in the stream. Returns -1 if there aren't sufficient bytes read yet to determine size.
         */
        int inferMessageSize(ByteBuffer buf, int readerIndex, int readerLimit)
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

            int paramsSize = extractParamsSize(buf, index, readerLimit);
            if (paramsSize < 0)
                return -1;
            index += paramsSize;

            long payloadSize = getUnsignedVInt(buf, index, readerLimit);
            if (payloadSize < 0)
                return -1;
            index += computeUnsignedVIntSize(payloadSize) + payloadSize;

            int size = index - readerIndex;

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
            MonotonicClockTranslation timeSnapshot = approxTime.translate();

            int index = buf.position();

            long id = getUnsignedVInt(buf, index);
            index += computeUnsignedVIntSize(id);

            int createdAtMillis = buf.getInt(index);
            index += sizeof(createdAtMillis);

            long expiresInMillis = getUnsignedVInt(buf, index);
            index += computeUnsignedVIntSize(expiresInMillis);

            Verb verb = Verb.fromId(getUnsignedVInt32(buf, index));
            index += computeUnsignedVIntSize(verb.id);

            int flags = getUnsignedVInt32(buf, index);
            index += computeUnsignedVIntSize(flags);

            Map<ParamType, Object> params = extractParams(buf, index, version);

            long createdAtNanos = calculateCreationTimeNanos(createdAtMillis, timeSnapshot, currentTimeNanos);
            long expiresAtNanos = getExpiresAtNanos(createdAtNanos, currentTimeNanos, TimeUnit.MILLISECONDS.toNanos(expiresInMillis));

            return new Header(id, verb, from, createdAtNanos, expiresAtNanos, flags, params);
        }

        private static long getExpiresAtNanos(long createdAtNanos, long currentTimeNanos, long expirationPeriodNanos)
        {
            if (!DatabaseDescriptor.hasCrossNodeTimeout() || createdAtNanos > currentTimeNanos)
                createdAtNanos = currentTimeNanos;
            return createdAtNanos + expirationPeriodNanos;
        }

        private void serializeHeader(Header header, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(header.id);
            // int cast cuts off the high-order half of the timestamp, which we can assume remains
            // the same between now and when the recipient reconstructs it.
            out.writeInt((int) approxTime.translate().toMillisSinceEpoch(header.createdAtNanos));
            out.writeUnsignedVInt(NANOSECONDS.toMillis(header.expiresAtNanos - header.createdAtNanos));
            out.writeUnsignedVInt32(header.verb.id);
            out.writeUnsignedVInt32(header.flags);
            serializeParams(header.params, out, version);
        }

        private Header deserializeHeader(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            long id = in.readUnsignedVInt();
            long currentTimeNanos = approxTime.now();
            MonotonicClockTranslation timeSnapshot = approxTime.translate();
            long creationTimeNanos = calculateCreationTimeNanos(in.readInt(), timeSnapshot, currentTimeNanos);
            long expiresAtNanos = getExpiresAtNanos(creationTimeNanos, currentTimeNanos, TimeUnit.MILLISECONDS.toNanos(in.readUnsignedVInt()));
            Verb verb = Verb.fromId(in.readUnsignedVInt32());
            int flags = in.readUnsignedVInt32();
            Map<ParamType, Object> params = deserializeParams(in, version);
            return new Header(id, verb, peer, creationTimeNanos, expiresAtNanos, flags, params);
        }

        private void skipHeader(DataInputPlus in) throws IOException
        {
            skipUnsignedVInt(in); // id
            in.skipBytesFully(4); // createdAt
            skipUnsignedVInt(in); // expiresIn
            skipUnsignedVInt(in); // verb
            skipUnsignedVInt(in); // flags
            skipParams(in); // params
        }

        private int serializedHeader(Header header, int version)
        {
            long size = 0;
            size += sizeofUnsignedVInt(header.id);
            size += CREATION_TIME_SIZE;
            size += sizeofUnsignedVInt(NANOSECONDS.toMillis(header.expiresAtNanos - header.createdAtNanos));
            size += sizeofUnsignedVInt(header.verb.id);
            size += sizeofUnsignedVInt(header.flags);
            size += serializedParamsSize(header.params, version);
            return Ints.checkedCast(size);
        }

        /*
         * created at + cross-node
         */

        private static final long TIMESTAMP_WRAPAROUND_GRACE_PERIOD_START  = 0xFFFFFFFFL - MINUTES.toMillis(15L);
        private static final long TIMESTAMP_WRAPAROUND_GRACE_PERIOD_END    =               MINUTES.toMillis(15L);

        @VisibleForTesting
        static long calculateCreationTimeNanos(int messageTimestampMillis, MonotonicClockTranslation timeSnapshot, long currentTimeNanos)
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
            // if the message timestamp wrapped, but we still haven't, add one highBit
            else if (sentLowBits < TIMESTAMP_WRAPAROUND_GRACE_PERIOD_END
                     && currentLowBits > TIMESTAMP_WRAPAROUND_GRACE_PERIOD_START)
            {
                highBits += 0x0000000100000000L;
            }

            long sentTimeMillis = (highBits | sentLowBits);

            if (Math.abs(currentTimeMillis - sentTimeMillis) > MINUTES.toMillis(15))
            {
                noSpam1m.warn("Bad timestamp {} generated, overriding with currentTimeMillis = {}", sentTimeMillis, currentTimeMillis);
                sentTimeMillis = currentTimeMillis;
            }

            return timeSnapshot.fromMillisSinceEpoch(sentTimeMillis);
        }

        /*
         * param ser/deser
         */

        private void serializeParams(Map<ParamType, Object> params, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(params.size());

            for (Map.Entry<ParamType, Object> kv : params.entrySet())
            {
                ParamType type = kv.getKey();
                out.writeUnsignedVInt32(type.id);

                IVersionedSerializer serializer = type.serializer;
                Object value = kv.getValue();

                int length = Ints.checkedCast(serializer.serializedSize(value, version));
                out.writeUnsignedVInt32(length);

                serializer.serialize(value, out, version);
            }
        }

        private Map<ParamType, Object> deserializeParams(DataInputPlus in, int version) throws IOException
        {
            int count = in.readUnsignedVInt32();

            if (count == 0)
                return NO_PARAMS;

            Map<ParamType, Object> params = new EnumMap<>(ParamType.class);

            for (int i = 0; i < count; i++)
            {
                ParamType type = ParamType.lookUpById(in.readUnsignedVInt32());

                int length = in.readUnsignedVInt32();

                if (null != type)
                {
                    params.put(type, type.serializer.deserialize(in, version));
                }
                else
                {
                    in.skipBytesFully(length); // forward compatibiliy with minor version changes
                }
            }

            return params;
        }

        private Map<ParamType, Object> extractParams(ByteBuffer buf, int readerIndex, int version) throws IOException
        {
            long count = getUnsignedVInt(buf, readerIndex);

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

        private void skipParams(DataInputPlus in) throws IOException
        {
            int count = in.readUnsignedVInt32();

            for (int i = 0; i < count; i++)
            {
                skipUnsignedVInt(in);
                in.skipBytesFully(in.readUnsignedVInt32());
            }
        }

        private long serializedParamsSize(Map<ParamType, Object> params, int version)
        {
            long size = computeUnsignedVIntSize(params.size());

            for (Map.Entry<ParamType, Object> kv : params.entrySet())
            {
                ParamType type = kv.getKey();
                Object value = kv.getValue();

                long valueLength = type.serializer.serializedSize(value, version);

                size += sizeofUnsignedVInt(type.id) + sizeofUnsignedVInt(valueLength);

                size += valueLength;
            }

            return size;
        }

        private int extractParamsSize(ByteBuffer buf, int readerIndex, int readerLimit)
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

        private <T> int payloadSize(Message<T> message, int version)
        {
            long payloadSize = message.payload != null && message.payload != NoPayload.noPayload
                             ? message.getPayloadSerializer().serializedSize(message.payload, version)
                             : 0;
            return Ints.checkedCast(payloadSize);
        }
    }

    private IVersionedAsymmetricSerializer<T, ?> getPayloadSerializer()
    {
        return verb().serializer();
    }

    private int serializedSize40;
    private int serializedSize50;

    /**
     * Serialized size of the entire message, for the provided messaging version. Caches the calculated value.
     */
    public int serializedSize(int version)
    {
        switch (version)
        {
            case VERSION_40:
                if (serializedSize40 == 0)
                    serializedSize40 = serializer.serializedSize(this, VERSION_40);
                return serializedSize40;
            case VERSION_50:
                if (serializedSize50 == 0)
                    serializedSize50 = serializer.serializedSize(this, VERSION_50);
                return serializedSize50;
            default:
                throw new IllegalStateException("Unkown serialization version " + version);
        }
    }

    private int payloadSize40   = -1;
    private int payloadSize50   = -1;

    private int payloadSize(int version)
    {
        switch (version)
        {
            case VERSION_40:
                if (payloadSize40 < 0)
                    payloadSize40 = serializer.payloadSize(this, VERSION_40);
                return payloadSize40;
            case VERSION_50:
                if (payloadSize50 < 0)
                    payloadSize50 = serializer.payloadSize(this, VERSION_50);
                return payloadSize50;

            default:
                throw new IllegalStateException("Unkown serialization version " + version);
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
