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
package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SerializerSupport;
import accord.messages.AbstractEpochRequest;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.BeginRecovery;
import accord.messages.Commit;
import accord.messages.LocalRequest;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.PreAccept;
import accord.messages.Propagate;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.concurrent.ManyToOneConcurrentLinkedQueue;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.interop.AccordInteropApply;
import org.apache.cassandra.service.accord.interop.AccordInteropCommit;
import org.apache.cassandra.service.accord.serializers.AcceptSerializers;
import org.apache.cassandra.service.accord.serializers.ApplySerializers;
import org.apache.cassandra.service.accord.serializers.BeginInvalidationSerializers;
import org.apache.cassandra.service.accord.serializers.CommitSerializers;
import org.apache.cassandra.service.accord.serializers.EnumSerializer;
import org.apache.cassandra.service.accord.serializers.FetchSerializers;
import org.apache.cassandra.service.accord.serializers.InformDurableSerializers;
import org.apache.cassandra.service.accord.serializers.InformOfTxnIdSerializers;
import org.apache.cassandra.service.accord.serializers.PreacceptSerializers;
import org.apache.cassandra.service.accord.serializers.RecoverySerializers;
import org.apache.cassandra.service.accord.serializers.SetDurableSerializers;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.messages.MessageType.ACCEPT_INVALIDATE_REQ;
import static accord.messages.MessageType.ACCEPT_REQ;
import static accord.messages.MessageType.APPLY_MAXIMAL_REQ;
import static accord.messages.MessageType.APPLY_MINIMAL_REQ;
import static accord.messages.MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
import static accord.messages.MessageType.BEGIN_INVALIDATE_REQ;
import static accord.messages.MessageType.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.COMMIT_INVALIDATE_REQ;
import static accord.messages.MessageType.COMMIT_MAXIMAL_REQ;
import static accord.messages.MessageType.COMMIT_SLOW_PATH_REQ;
import static accord.messages.MessageType.INFORM_DURABLE_REQ;
import static accord.messages.MessageType.INFORM_OF_TXN_REQ;
import static accord.messages.MessageType.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.PROPAGATE_APPLY_MSG;
import static accord.messages.MessageType.PROPAGATE_OTHER_MSG;
import static accord.messages.MessageType.PROPAGATE_PRE_ACCEPT_MSG;
import static accord.messages.MessageType.PROPAGATE_STABLE_MSG;
import static accord.messages.MessageType.SET_GLOBALLY_DURABLE_REQ;
import static accord.messages.MessageType.SET_SHARD_DURABLE_REQ;
import static accord.messages.MessageType.STABLE_FAST_PATH_REQ;
import static accord.messages.MessageType.STABLE_MAXIMAL_REQ;
import static accord.messages.MessageType.STABLE_SLOW_PATH_REQ;
import static org.apache.cassandra.db.TypeSizes.BYTE_SIZE;
import static org.apache.cassandra.db.TypeSizes.INT_SIZE;
import static org.apache.cassandra.db.TypeSizes.LONG_SIZE;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_APPLY_MAXIMAL_REQ;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_APPLY_MINIMAL_REQ;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_COMMIT_MAXIMAL_REQ;
import static org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType.INTEROP_COMMIT_MINIMAL_REQ;
import static org.apache.cassandra.service.accord.serializers.ReadDataSerializers.applyThenWaitUntilApplied;

public class AccordJournal implements IJournal, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);

    private static final boolean LOG_MESSAGE_PROVIDER = false;

    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    private static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[22]);

    public final Journal<Key, Object> journal;
    private final AccordEndpointMapper endpointMapper;

    private final DelayedRequestProcessor delayedRequestProcessor = new DelayedRequestProcessor();

    Node node;

    enum Status { INITIALIZED, STARTING, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    @VisibleForTesting
    public AccordJournal(AccordEndpointMapper endpointMapper, Params params)
    {
        File directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
        this.journal = new Journal<>("AccordJournal", directory, params, Key.SUPPORT, RECORD_SERIALIZER);
        this.endpointMapper = endpointMapper;
    }

    public AccordJournal start(Node node)
    {
        Invariants.checkState(status == Status.INITIALIZED);
        this.node = node;
        status = Status.STARTING;
        journal.start();
        this.delayedRequestProcessor.start();
        status = Status.STARTED;
        return this;
    }

    @Override
    public boolean isTerminated()
    {
        return status == Status.TERMINATED;
    }

    @Override
    public void shutdown()
    {
        Invariants.checkState(status == Status.STARTED);
        this.delayedRequestProcessor.interrupt();
        status = Status.TERMINATING;
        journal.shutdown();
        status = Status.TERMINATED;
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, Arrays.asList(journal));
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    /**
     * Accord protocol messages originating from remote nodes.
     */
    public void processRemoteRequest(Request request, ResponseContext context)
    {
        Type type = Type.fromMessageType(request.type());
        Key key = new Key(type.txnId(request), type);
        journal.asyncWrite(key, request, SENTINEL_HOSTS);
        RemoteRequestContext requestContext = RemoteRequestContext.forLive(request, context);
        if (node.topology().hasEpoch(request.waitForEpoch()))
            requestContext.process(node, endpointMapper);
        else
            delayedRequestProcessor.delay(requestContext);
    }

    /**
     * Accord protocol messages originating from local node, e.g. Propagate.
     */
    @SuppressWarnings("rawtypes, unchecked")
    public <R> void processLocalRequest(LocalRequest request, BiConsumer<? super R, Throwable> callback)
    {
        Type type = Type.fromMessageType(request.type());
        Key key = new Key(type.txnId(request), type);
        journal.asyncWrite(key, request, SENTINEL_HOSTS);
        LocalRequestContext requestContext = LocalRequestContext.create(request, callback);
        if (node.topology().hasEpoch(request.waitForEpoch()))
            request.process(node, requestContext.callback);
        else
            delayedRequestProcessor.delay(requestContext);
    }

    @VisibleForTesting
    @Override
    public void appendMessageBlocking(Message message)
    {
        Type type = Type.fromMessageType(message.type());
        Key key = new Key(type.txnId(message), type);
        journal.blockingWrite(key, message, SENTINEL_HOSTS);
    }

    @Override
    public boolean appendCommand(int commandStoreId, TxnId txnId, SavedCommand.SavedDiff outcome, Runnable runnable)
    {
        Key key = new Key(txnId, Type.SAVED_COMMAND, commandStoreId);
        // This is not super optimal, but for a sanity check we mostly care about the fact that the latest written
        // state represents actuality.
        if (CassandraRelevantProperties.DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED.getBoolean())
        {
            journal.blockingWrite(key, outcome, SENTINEL_HOSTS);
            return false;
        }
        else
        {
            RecordPointer pointer = journal.asyncWrite(key, outcome, SENTINEL_HOSTS);
            journal.awaitFlush(pointer, runnable);
            return true;
        }
    }

    @VisibleForTesting
    public <M extends Message> M readMessage(TxnId txnId, MessageType messageType, Class<M> clazz)
    {
        for (Type type : Type.synonymousTypesFromMessageType(messageType))
        {
            M message = clazz.cast(journal.readFirst(new Key(txnId, type)));
            if (null != message) return message;
        }
        return null;
    }

    <M extends Message> M readMessage(TxnId txnId, MessageType messageType, Class<M> clazz, Predicate<Object> condition)
    {
        for (Type type : Type.synonymousTypesFromMessageType(messageType))
        {
            M message = clazz.cast(journal.readFirstMatching(new Key(txnId, type), condition));
            if (null != message) return message;
        }

        return null;
    }

    /*
     * Context necessary to process log records
     */
    static abstract class RequestContext implements ReplyContext
    {
        final long waitForEpoch;

        RequestContext(long waitForEpoch)
        {
            this.waitForEpoch = waitForEpoch;
        }

        public abstract void process(Node node, AccordEndpointMapper endpointMapper);
    }

    private static class LocalRequestContext<T> extends RequestContext
    {
        private final BiConsumer<T, Throwable> callback;
        private final LocalRequest<T> request;

        LocalRequestContext(long waitForEpoch, LocalRequest<T> request, BiConsumer<T, Throwable> callback)
        {
            super(waitForEpoch);
            this.callback = callback;
            this.request = request;
        }

        public void process(Node node, AccordEndpointMapper endpointMapper)
        {
            request.process(node, callback);
        }

        static <R> LocalRequestContext<R> create(LocalRequest<R> request, BiConsumer<R, Throwable> callback)
        {
            return new LocalRequestContext<>(request.waitForEpoch(), request, callback);
        }
    }

    /**
     * Barebones response context not holding a reference to the entire message
     */
    private abstract static class RemoteRequestContext extends RequestContext implements ResponseContext
    {
        private final Request request;

        RemoteRequestContext(long waitForEpoch, Request request)
        {
            super(waitForEpoch);
            this.request = request;
        }

        static LiveRemoteRequestContext forLive(Request request, ResponseContext context)
        {
            return new LiveRemoteRequestContext(request, context.id(), context.from(), context.verb(), context.expiresAtNanos());
        }

        @Override
        public void process(Node node, AccordEndpointMapper endpointMapper)
        {
            this.request.process(node, endpointMapper.mappedId(from()), this);
        }

        @Override public abstract long id();
        @Override public abstract InetAddressAndPort from();
        @Override public abstract Verb verb();
        @Override public abstract long expiresAtNanos();
    }

    private static class LiveRemoteRequestContext extends RemoteRequestContext
    {
        private final long id;
        private final InetAddressAndPort from;
        private final Verb verb;
        private final long expiresAtNanos;

        LiveRemoteRequestContext(Request request, long id, InetAddressAndPort from, Verb verb, long expiresAtNanos)
        {
            super(request.waitForEpoch(), request);
            this.id = id;
            this.from = from;
            this.verb = verb;
            this.expiresAtNanos = expiresAtNanos;
        }


        @Override
        public long id()
        {
            return id;
        }
        @Override
        public InetAddressAndPort from()
        {
            return from;
        }
        @Override
        public Verb verb()
        {
            return verb;
        }
        @Override
        public long expiresAtNanos()
        {
            return expiresAtNanos;
        }
    }

    /*
     * Records ser/de in the Journal
     */

    public static class Key
    {
        final Timestamp timestamp;
        final Type type;
        final int commandStoreId;

        Key(Timestamp timestamp, Type type)
        {
            this(timestamp, type, -1);
        }

        Key(Timestamp timestamp, Type type, int commandStoreId)
        {
            if (timestamp == null) throw new NullPointerException("Null timestamp for type " + type);
            this.timestamp = timestamp;
            this.type = type;
            this.commandStoreId = commandStoreId;
        }

        /**
         * Support for (de)serializing and comparing record keys.
         * <p>
         * Implements its own serialization and comparison for {@link Timestamp} to satisty
         * {@link KeySupport} contract - puts hybrid logical clock ahead of epoch
         * when ordering timestamps. This is done for more precise elimination of candidate
         * segments by min/max record key in segment.
         */
        static final KeySupport<Key> SUPPORT = new KeySupport<>()
        {
            private static final int HLC_OFFSET             = 0;
            private static final int EPOCH_AND_FLAGS_OFFSET = HLC_OFFSET             + LONG_SIZE;
            private static final int NODE_OFFSET            = EPOCH_AND_FLAGS_OFFSET + LONG_SIZE;
            private static final int TYPE_OFFSET            = NODE_OFFSET            + INT_SIZE;
            private static final int CS_ID_OFFSET           = TYPE_OFFSET            + BYTE_SIZE;

            @Override
            public int serializedSize(int userVersion)
            {
                return LONG_SIZE  // timestamp.hlc()
                     + 6          // timestamp.epoch()
                     + 2          // timestamp.flags()
                     + INT_SIZE   // timestamp.node
                     + BYTE_SIZE  // type
                     + BYTE_SIZE; // commandStoreId
            }

            @Override
            public void serialize(Key key, DataOutputPlus out, int userVersion) throws IOException
            {
                serializeTimestamp(key.timestamp, out);
                out.writeByte(key.type.id);
                out.writeByte(key.commandStoreId);
            }

            private void serialize(Key key, byte[] out)
            {
                serializeTimestamp(key.timestamp, out);
                out[20] = (byte) (key.type.id & 0xFF);
                out[21] = (byte) (key.commandStoreId & 0xFF);
            }

            @Override
            public Key deserialize(DataInputPlus in, int userVersion) throws IOException
            {
                Timestamp timestamp = deserializeTimestamp(in);
                int type = in.readByte();
                int commandStoreId = in.readByte();
                return new Key(timestamp, Type.fromId(type), commandStoreId);
            }

            @Override
            public Key deserialize(ByteBuffer buffer, int position, int userVersion)
            {
                Timestamp timestamp = deserializeTimestamp(buffer, position);
                int type = buffer.get(position + TYPE_OFFSET);
                int commandStoreId = buffer.get(position + CS_ID_OFFSET);
                return new Key(timestamp, Type.fromId(type), commandStoreId);
            }

            private void serializeTimestamp(Timestamp timestamp, DataOutputPlus out) throws IOException
            {
                out.writeLong(timestamp.hlc());
                out.writeLong(epochAndFlags(timestamp));
                out.writeInt(timestamp.node.id);
            }

            private Timestamp deserializeTimestamp(DataInputPlus in) throws IOException
            {
                long hlc = in.readLong();
                long epochAndFlags = in.readLong();
                int nodeId = in.readInt();
                return Timestamp.fromValues(epoch(epochAndFlags), hlc, flags(epochAndFlags), new Id(nodeId));
            }


            private void serializeTimestamp(Timestamp timestamp, byte[] out)
            {
                ByteArrayUtil.putLong(out, 0, timestamp.hlc());
                ByteArrayUtil.putLong(out, 8, epochAndFlags(timestamp));
                ByteArrayUtil.putInt(out, 16, timestamp.node.id);
            }

            private Timestamp deserializeTimestamp(ByteBuffer buffer, int position)
            {
                long hlc = buffer.getLong(position + HLC_OFFSET);
                long epochAndFlags = buffer.getLong(position + EPOCH_AND_FLAGS_OFFSET);
                int nodeId = buffer.getInt(position + NODE_OFFSET);
                return Timestamp.fromValues(epoch(epochAndFlags), hlc, flags(epochAndFlags), new Id(nodeId));
            }

            @Override
            public void updateChecksum(Checksum crc, Key key, int userVersion)
            {
                byte[] out = keyCRCBytes.get();
                serialize(key, out);
                crc.update(out, 0, out.length);
            }

            @Override
            public int compareWithKeyAt(Key k, ByteBuffer buffer, int position, int userVersion)
            {
                int cmp = compareWithTimestampAt(k.timestamp, buffer, position);
                if (cmp != 0) return cmp;

                byte type = buffer.get(position + TYPE_OFFSET);
                cmp = Byte.compare((byte) k.type.id, type);
                if (cmp != 0) return cmp;
                byte commandStoreId = buffer.get(position + CS_ID_OFFSET);
                cmp = Byte.compare((byte) k.commandStoreId, commandStoreId);
                return cmp;
            }

            private int compareWithTimestampAt(Timestamp timestamp, ByteBuffer buffer, int position)
            {
                long hlc = buffer.getLong(position + HLC_OFFSET);
                int cmp = Long.compareUnsigned(timestamp.hlc(), hlc);
                if (cmp != 0) return cmp;

                long epochAndFlags = buffer.getLong(position + EPOCH_AND_FLAGS_OFFSET);
                cmp = Long.compareUnsigned(epochAndFlags(timestamp), epochAndFlags);
                if (cmp != 0) return cmp;

                int nodeId = buffer.getInt(position + NODE_OFFSET);
                cmp = Integer.compareUnsigned(timestamp.node.id, nodeId);
                return cmp;
            }

            @Override
            public int compare(Key k1, Key k2)
            {
                int cmp = compare(k1.timestamp, k2.timestamp);
                if (cmp == 0) cmp = Byte.compare((byte) k1.type.id, (byte) k2.type.id);
                if (cmp == 0) cmp = Byte.compare((byte) k1.commandStoreId, (byte) k2.commandStoreId);
                return cmp;
            }

            private int compare(Timestamp timestamp1, Timestamp timestamp2)
            {
                int cmp = Long.compareUnsigned(timestamp1.hlc(), timestamp2.hlc());
                if (cmp == 0) cmp = Long.compareUnsigned(epochAndFlags(timestamp1), epochAndFlags(timestamp2));
                if (cmp == 0) cmp = Integer.compareUnsigned(timestamp1.node.id, timestamp2.node.id);
                return cmp;
            }

            private long epochAndFlags(Timestamp timestamp)
            {
                return (timestamp.epoch() << 16) | (long) timestamp.flags();
            }

            private long epoch(long epochAndFlags)
            {
                return epochAndFlags >>> 16;
            }

            private int flags(long epochAndFlags)
            {
                return (int) (epochAndFlags & ((1 << 16) - 1));
            }
        };

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;
            return (other instanceof Key) && equals((Key) other);
        }

        boolean equals(Key other)
        {
            return this.type == other.type && this.timestamp.equals(other.timestamp);
        }

        @Override
        public int hashCode()
        {
            return type.hashCode() + 31 * timestamp.hashCode();
        }

        @Override
        public String toString()
        {
            return "Key{" + timestamp + ", " + type + '}';
        }
    }

    private static final ValueSerializer<Key, Object> RECORD_SERIALIZER = new ValueSerializer<>()
    {
        @Override
        public int serializedSize(Key key, Object record, int userVersion)
        {
            return Ints.checkedCast(key.type.serializedSize(key, record, userVersion));
        }

        @Override
        public void serialize(Key key, Object record, DataOutputPlus out, int userVersion) throws IOException
        {
            key.type.serialize(key, record, out, userVersion);
        }

        @Override
        public Object deserialize(Key key, DataInputPlus in, int userVersion) throws IOException
        {
            return key.type.deserialize(key, in, userVersion);
        }
    };

    /* Adapts vanilla message serializers to journal-expected signatures; converts user version to MS version */
    static final class MessageSerializer implements ValueSerializer<Key, Object>
    {
        final IVersionedSerializer<Message> wrapped;

        private MessageSerializer(IVersionedSerializer<Message> wrapped)
        {
            this.wrapped = wrapped;
        }

        static MessageSerializer wrap(IVersionedSerializer<Message> wrapped)
        {
            return new MessageSerializer(wrapped);
        }

        @Override
        public int serializedSize(Key key, Object message, int userVersion)
        {
            return Ints.checkedCast(wrapped.serializedSize((Message) message, msVersion(userVersion)));
        }

        @Override
        public void serialize(Key key, Object message, DataOutputPlus out, int userVersion) throws IOException
        {
            wrapped.serialize((Message) message, out, msVersion(userVersion));
        }

        @Override
        public Object deserialize(Key key, DataInputPlus in, int userVersion) throws IOException
        {
            return wrapped.deserialize(in, msVersion(userVersion));
        }
    }

    @FunctionalInterface
    interface TxnIdProvider
    {
        TxnId txnId(Message message);
    }

    private static final TxnIdProvider EPOCH = msg -> ((AbstractEpochRequest<?>) msg).txnId;
    private static final TxnIdProvider TXN   = msg -> ((TxnRequest<?>) msg).txnId;
    private static final TxnIdProvider LOCAL = msg -> ((LocalRequest<?>) msg).primaryTxnId();
    private static final TxnIdProvider INVL  = msg -> ((Commit.Invalidate) msg).primaryTxnId();

    /**
     * Accord Message type - consequently the kind of persisted record.
     * <p>
     * Note: {@link EnumSerializer} is intentionally not being reused here, for two reasons:
     *  1. This is an internal enum, fully under our control, not part of an external library
     *  2. It's persisted in the record key, so has the additional constraint of being fixed size and
     *     shouldn't be using varint encoding
     */
    public enum Type implements ValueSerializer<Key, Object>
    {
        /* Auxiliary journal records */
        SAVED_COMMAND                 (1, SavedCommand.serializer),

        /* Accord protocol requests */
        PRE_ACCEPT                    (64, PRE_ACCEPT_REQ,                    PreacceptSerializers.request, TXN  ),
        ACCEPT                        (65, ACCEPT_REQ,                        AcceptSerializers.request,    TXN  ),
        ACCEPT_INVALIDATE             (66, ACCEPT_INVALIDATE_REQ,             AcceptSerializers.invalidate, EPOCH),
        COMMIT_SLOW_PATH              (67, COMMIT_SLOW_PATH_REQ,              CommitSerializers.request,    TXN  ),
        COMMIT_MAXIMAL                (68, COMMIT_MAXIMAL_REQ,                CommitSerializers.request,    TXN  ),
        STABLE_FAST_PATH              (87, STABLE_FAST_PATH_REQ,              CommitSerializers.request,    TXN  ),
        STABLE_SLOW_PATH              (88, STABLE_SLOW_PATH_REQ,              CommitSerializers.request,    TXN  ),
        STABLE_MAXIMAL                (89, STABLE_MAXIMAL_REQ,                CommitSerializers.request,    TXN  ),
        COMMIT_INVALIDATE             (69, COMMIT_INVALIDATE_REQ,             CommitSerializers.invalidate, INVL ),
        APPLY_MINIMAL                 (70, APPLY_MINIMAL_REQ,                 ApplySerializers.request,     TXN  ),
        APPLY_MAXIMAL                 (71, APPLY_MAXIMAL_REQ,                 ApplySerializers.request,     TXN  ),
        APPLY_THEN_WAIT_UNTIL_APPLIED (72, APPLY_THEN_WAIT_UNTIL_APPLIED_REQ, applyThenWaitUntilApplied,    EPOCH),

        BEGIN_RECOVER                 (73, BEGIN_RECOVER_REQ,        RecoverySerializers.request,           TXN  ),
        BEGIN_INVALIDATE              (74, BEGIN_INVALIDATE_REQ,     BeginInvalidationSerializers.request,  EPOCH),
        INFORM_OF_TXN                 (75, INFORM_OF_TXN_REQ,        InformOfTxnIdSerializers.request,      EPOCH),
        INFORM_DURABLE                (76, INFORM_DURABLE_REQ,       InformDurableSerializers.request,      TXN  ),
        SET_SHARD_DURABLE             (77, SET_SHARD_DURABLE_REQ,    SetDurableSerializers.shardDurable,    EPOCH),
        SET_GLOBALLY_DURABLE          (78, SET_GLOBALLY_DURABLE_REQ, SetDurableSerializers.globallyDurable, EPOCH),

        /* Accord local messages */
        PROPAGATE_PRE_ACCEPT          (79, PROPAGATE_PRE_ACCEPT_MSG, FetchSerializers.propagate, LOCAL),
        PROPAGATE_STABLE              (80, PROPAGATE_STABLE_MSG,     FetchSerializers.propagate, LOCAL),
        PROPAGATE_APPLY               (81, PROPAGATE_APPLY_MSG,      FetchSerializers.propagate, LOCAL),
        PROPAGATE_OTHER               (82, PROPAGATE_OTHER_MSG,      FetchSerializers.propagate, LOCAL),

        /* C* interop messages */
        INTEROP_COMMIT                (83, INTEROP_COMMIT_MINIMAL_REQ,  STABLE_FAST_PATH_REQ, AccordInteropCommit.serializer, TXN),
        INTEROP_COMMIT_MAXIMAL        (84, INTEROP_COMMIT_MAXIMAL_REQ, STABLE_MAXIMAL_REQ,   AccordInteropCommit.serializer, TXN),
        INTEROP_APPLY_MINIMAL         (85, INTEROP_APPLY_MINIMAL_REQ,  APPLY_MINIMAL_REQ,    AccordInteropApply.serializer,  TXN),
        INTEROP_APPLY_MAXIMAL         (86, INTEROP_APPLY_MAXIMAL_REQ,  APPLY_MAXIMAL_REQ,    AccordInteropApply.serializer,  TXN),
        ;

        final int id;

        /**
         * An incoming message of a given type from Accord's perspective might have multiple
         * concrete implementations some of which are supplied by the Cassandra integration.
         * The incoming type specifies the handling for writing out a message to the journal.
         */
        final MessageType incomingType;

        /**
         * The outgoing type is the type that will be returned to Accord; must be a subclass of the incoming type.
         * <p>
         * This type will always be from accord.messages.MessageType and never from the extended types in the integration.
         */
        final MessageType outgoingType;

        final TxnIdProvider txnIdProvider;
        final ValueSerializer<Key, Object> serializer;

        Type(int id,  ValueSerializer<Key, Object> serializer)
        {
            this(id, null, null, serializer, null);
        }


        Type(int id, MessageType incomingType, MessageType outgoingType, IVersionedSerializer<?> serializer, TxnIdProvider txnIdProvider)
        {
            //noinspection unchecked
            this(id, incomingType, outgoingType, MessageSerializer.wrap((IVersionedSerializer<Message>) serializer), txnIdProvider);
        }

        Type(int id, MessageType type, IVersionedSerializer<?> serializer, TxnIdProvider txnIdProvider)
        {
            //noinspection unchecked
            this(id, type, type, MessageSerializer.wrap((IVersionedSerializer<Message>) serializer), txnIdProvider);
        }

        Type(int id, MessageType incomingType, MessageType outgoingType, ValueSerializer<Key, ?> serializer, TxnIdProvider txnIdProvider)
        {
            if (id < 0)
                throw new IllegalArgumentException("Negative Type id " + id);
            if (id > Byte.MAX_VALUE)
                throw new IllegalArgumentException("Type id doesn't fit in a single byte: " + id);

            this.id = id;
            this.incomingType = incomingType;
            this.outgoingType = outgoingType;
            //noinspection unchecked
            this.serializer = (ValueSerializer<Key, Object>) serializer;
            this.txnIdProvider = txnIdProvider;
        }

        private static final Type[] idToTypeMapping;
        private static final Map<MessageType, Type> msgTypeToTypeMap;

        private static final ListMultimap<MessageType, Type> msgTypeToSynonymousTypesMap;

        static
        {
            Type[] types = values();

            int maxId = -1;
            for (Type type : types)
                maxId = Math.max(type.id, maxId);

            Type[] idToType = new Type[maxId + 1];
            for (Type type : types)
            {
                if (null != idToType[type.id])
                    throw new IllegalStateException("Duplicate Type id " + type.id);
                idToType[type.id] = type;
            }
            idToTypeMapping = idToType;

            Map<MessageType, Type> msgTypeToType = new HashMap<>();
            for (Type type : types)
            {
                if (null != type.incomingType && null != msgTypeToType.put(type.incomingType, type))
                    throw new IllegalStateException("Duplicate MessageType " + type.incomingType);
            }
            msgTypeToTypeMap = ImmutableMap.copyOf(msgTypeToType);

            Multimap<MessageType, Type> msgTypeToSynonymousTypes = ArrayListMultimap.create();
            for (Type type : types)
            {
                if (null != type.outgoingType)
                {
                    Type incomingType = msgTypeToTypeMap.get(type.incomingType);
                    if (msgTypeToSynonymousTypes.get(type.outgoingType).contains(incomingType))
                        throw new IllegalStateException("Duplicate synonymous Type " + type.incomingType);
                    msgTypeToSynonymousTypes.put(type.outgoingType, incomingType);
                }
            }
            msgTypeToSynonymousTypesMap = ImmutableListMultimap.copyOf(msgTypeToSynonymousTypes);

            //TODO (now): enable as this shows we are currently missing a message
//            IllegalStateException e = null;
//            for (MessageType t : MessageType.values)
//            {
//                if (!t.hasSideEffects()) continue;
//                Type matches = msgTypeToTypeMap.get(t);
//                if (matches == null)
//                {
//                    IllegalStateException ise = new IllegalStateException("Missing MessageType " + t);
//                    if (e == null) e = ise;
//                    else e.addSuppressed(ise);
//                }
//            }
//            if (e != null)
//                throw e;
        }

        static Type fromId(int id)
        {
            if (id < 0 || id >= idToTypeMapping.length)
                throw new IllegalArgumentException("Out or range Type id " + id);
            Type type = idToTypeMapping[id];
            if (null == type)
                throw new IllegalArgumentException("Unknown Type id " + id);
            return type;
        }

        static List<Type> synonymousTypesFromMessageType(MessageType msgType)
        {
            List<Type> synonymousTypes = msgTypeToSynonymousTypesMap.get(msgType);
            if (synonymousTypes.isEmpty())
                throw new IllegalArgumentException("Unsupported MessageType " + msgType);
            return synonymousTypes;
        }

        static Type fromMessageType(MessageType msgType)
        {
            Type type = msgTypeToTypeMap.get(msgType);
            if (null == type)
                throw new IllegalArgumentException("Unsupported MessageType " + msgType);
            return type;
        }

        @Override
        public int serializedSize(Key key, Object record, int userVersion)
        {
            return serializer.serializedSize(key, record, userVersion);
        }

        @Override
        public void serialize(Key key, Object record, DataOutputPlus out, int userVersion) throws IOException
        {
            serializer.serialize(key, record, out, userVersion);
        }

        @Override
        public Object deserialize(Key key, DataInputPlus in, int userVersion) throws IOException
        {
            return serializer.deserialize(key, in, userVersion);
        }

        TxnId txnId(Message message)
        {
            return txnIdProvider.txnId(message);
        }
    }

    static
    {
        // make noise early if we forget to update our version mappings
        Invariants.checkState(MessagingService.current_version == MessagingService.VERSION_51, "Expected current version to be %d but given %d", MessagingService.VERSION_51, MessagingService.current_version);
    }

    private static int msVersion(int version)
    {
        switch (version)
        {
            default: throw new IllegalArgumentException();
            case 1: return MessagingService.VERSION_51;
        }
    }

    /*
     * Message provider implementation
     */
    @Override
    public SerializerSupport.MessageProvider makeMessageProvider(TxnId txnId)
    {
        return LOG_MESSAGE_PROVIDER ? new LoggingMessageProvider(txnId, new MessageProvider(txnId)) : new MessageProvider(txnId);
    }

    private  final class MessageProvider implements SerializerSupport.MessageProvider
    {
        final TxnId txnId;

        private MessageProvider(TxnId txnId)
        {
            this.txnId = txnId;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public Set<MessageType> test(Set<MessageType> messages)
        {
            Set<Key> keys = new ObjectHashSet<>(messages.size() + 1, 0.9f);
            for (MessageType message : messages)
                for (Type synonymousType : Type.synonymousTypesFromMessageType(message))
                    keys.add(new Key(txnId, synonymousType, -1));
            Set<Key> presentKeys = journal.test(keys);
            Set<MessageType> presentMessages = new ObjectHashSet<>(presentKeys.size() + 1, 0.9f);
            for (Key key : presentKeys)
                presentMessages.add(key.type.outgoingType);
            return presentMessages;
        }

        @Override
        public Set<MessageType> all()
        {
            Set<Type> types = EnumSet.allOf(Type.class);
            Set<Key> keys = new ObjectHashSet<>(types.size() + 1, 0.9f);
            for (Type type : types)
                keys.add(new Key(txnId, type, -1));
            Set<Key> presentKeys = journal.test(keys);
            Set<MessageType> presentMessages = new ObjectHashSet<>(presentKeys.size() + 1, 0.9f);
            for (Key key : presentKeys)
                presentMessages.add(key.type.outgoingType);
            return presentMessages;
        }

        @Override
        public PreAccept preAccept()
        {
            return readMessage(txnId, PRE_ACCEPT_REQ, PreAccept.class);
        }

        @Override
        public BeginRecovery beginRecover()
        {
            return readMessage(txnId, BEGIN_RECOVER_REQ, BeginRecovery.class);
        }

        @Override
        public Propagate propagatePreAccept()
        {
            return readMessage(txnId, PROPAGATE_PRE_ACCEPT_MSG, Propagate.class);
        }

        @Override
        public Accept accept(Ballot ballot)
        {
            return readMessage(txnId, ACCEPT_REQ, Accept.class, (accept) -> ((Accept) accept).ballot.equals(ballot));
        }

        @Override
        public Commit commitSlowPath()
        {
            return readMessage(txnId, COMMIT_SLOW_PATH_REQ, Commit.class);
        }

        @Override
        public Commit commitMaximal()
        {
            return readMessage(txnId, COMMIT_MAXIMAL_REQ, Commit.class);
        }

        @Override
        public Commit stableFastPath()
        {
            return readMessage(txnId, STABLE_FAST_PATH_REQ, Commit.class);
        }

        @Override
        public Commit stableSlowPath()
        {
            return readMessage(txnId, STABLE_SLOW_PATH_REQ, Commit.class);
        }

        @Override
        public Commit stableMaximal()
        {
            return readMessage(txnId, STABLE_MAXIMAL_REQ, Commit.class);
        }

        @Override
        public Propagate propagateStable()
        {
            return readMessage(txnId, PROPAGATE_STABLE_MSG, Propagate.class);
        }

        @Override
        public Apply applyMinimal()
        {
            return readMessage(txnId, APPLY_MINIMAL_REQ, Apply.class);
        }

        @Override
        public Apply applyMaximal()
        {
            return readMessage(txnId, APPLY_MAXIMAL_REQ, Apply.class);
        }

        @Override
        public Propagate propagateApply()
        {
            return readMessage(txnId, PROPAGATE_APPLY_MSG, Propagate.class);
        }

        @Override
        public Propagate propagateOther()
        {
            return readMessage(txnId, PROPAGATE_OTHER_MSG, Propagate.class);
        }

        @Override
        public ApplyThenWaitUntilApplied applyThenWaitUntilApplied()
        {
            return readMessage(txnId, APPLY_THEN_WAIT_UNTIL_APPLIED_REQ, ApplyThenWaitUntilApplied.class);
        }
    }

    private final class LoggingMessageProvider implements SerializerSupport.MessageProvider
    {
        private final TxnId txnId;
        private final MessageProvider provider;

        LoggingMessageProvider(TxnId txnId, MessageProvider provider)
        {
            this.txnId = txnId;
            this.provider = provider;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public Set<MessageType> test(Set<MessageType> messages)
        {
            logger.debug("Checking {} messages for {}", messages, txnId);
            Set<MessageType> confirmed = provider.test(messages);
            logger.debug("Confirmed {} messages for {}", confirmed, txnId);
            return confirmed;
        }

        @Override
        public Set<MessageType> all()
        {
            logger.debug("Checking all messages for {}", txnId);
            Set<MessageType> confirmed = provider.all();
            logger.debug("Confirmed {} messages for {}", confirmed, txnId);
            return confirmed;
        }

        @Override
        public PreAccept preAccept()
        {
            logger.debug("Fetching {} message for {}", PRE_ACCEPT_REQ, txnId);
            PreAccept preAccept = provider.preAccept();
            logger.debug("Fetched {} message for {}: {}", PRE_ACCEPT_REQ, txnId, preAccept);
            return preAccept;
        }

        @Override
        public BeginRecovery beginRecover()
        {
            logger.debug("Fetching {} message for {}", BEGIN_RECOVER_REQ, txnId);
            BeginRecovery beginRecover = provider.beginRecover();
            logger.debug("Fetched {} message for {}: {}", BEGIN_RECOVER_REQ, txnId, beginRecover);
            return beginRecover;
        }

        @Override
        public Propagate propagatePreAccept()
        {
            logger.debug("Fetching {} message for {}", PROPAGATE_PRE_ACCEPT_MSG, txnId);
            Propagate propagate = provider.propagatePreAccept();
            logger.debug("Fetched {} message for {}: {}", PROPAGATE_PRE_ACCEPT_MSG, txnId, propagate);
            return propagate;
        }

        @Override
        public Accept accept(Ballot ballot)
        {
            logger.debug("Fetching {} message (with accepted: {}) for {}", ACCEPT_REQ, ballot, txnId);
            Accept accept = provider.accept(ballot);
            logger.debug("Fetched {} message (with accepted: {}) for {}: {}", ACCEPT_REQ, ballot, txnId, accept);
            return accept;
        }

        @Override
        public Commit commitSlowPath()
        {
            logger.debug("Fetching {} message for {}", COMMIT_SLOW_PATH_REQ, txnId);
            Commit commit = provider.commitSlowPath();
            logger.debug("Fetched {} message for {}: {}", COMMIT_SLOW_PATH_REQ, txnId, commit);
            return commit;
        }

        @Override
        public Commit commitMaximal()
        {
            logger.debug("Fetching {} message for {}", COMMIT_MAXIMAL_REQ, txnId);
            Commit commit = provider.commitMaximal();
            logger.debug("Fetched {} message for {}: {}", COMMIT_MAXIMAL_REQ, txnId, commit);
            return commit;
        }

        @Override
        public Commit stableFastPath()
        {
            logger.debug("Fetching {} message for {}", STABLE_FAST_PATH_REQ, txnId);
            Commit commit = provider.stableFastPath();
            logger.debug("Fetched {} message for {}: {}", STABLE_FAST_PATH_REQ, txnId, commit);
            return commit;
        }

        @Override
        public Commit stableSlowPath()
        {
            logger.debug("Fetching {} message for {}", STABLE_SLOW_PATH_REQ, txnId);
            Commit commit = provider.stableSlowPath();
            logger.debug("Fetched {} message for {}: {}", STABLE_SLOW_PATH_REQ, txnId, commit);
            return commit;
        }

        @Override
        public Commit stableMaximal()
        {
            logger.debug("Fetching {} message for {}", STABLE_MAXIMAL_REQ, txnId);
            Commit commit = provider.stableMaximal();
            logger.debug("Fetched {} message for {}: {}", STABLE_MAXIMAL_REQ, txnId, commit);
            return commit;
        }

        @Override
        public Propagate propagateStable()
        {
            logger.debug("Fetching {} message for {}", PROPAGATE_STABLE_MSG, txnId);
            Propagate propagate = provider.propagateStable();
            logger.debug("Fetched {} message for {}: {}", PROPAGATE_STABLE_MSG, txnId, propagate);
            return propagate;
        }

        @Override
        public Apply applyMinimal()
        {
            logger.debug("Fetching {} message for {}", APPLY_MINIMAL_REQ, txnId);
            Apply apply = provider.applyMinimal();
            logger.debug("Fetched {} message for {}: {}", APPLY_MINIMAL_REQ, txnId, apply);
            return apply;
        }

        @Override
        public Apply applyMaximal()
        {
            logger.debug("Fetching {} message for {}", APPLY_MAXIMAL_REQ, txnId);
            Apply apply = provider.applyMaximal();
            logger.debug("Fetched {} message for {}: {}", APPLY_MAXIMAL_REQ, txnId, apply);
            return apply;
        }

        @Override
        public Propagate propagateApply()
        {
            logger.debug("Fetching {} message for {}", PROPAGATE_APPLY_MSG, txnId);
            Propagate propagate = provider.propagateApply();
            logger.debug("Fetched {} message for {}: {}", PROPAGATE_APPLY_MSG, txnId, propagate);
            return propagate;
        }

        @Override
        public Propagate propagateOther()
        {
            logger.debug("Fetching {} message for {}", PROPAGATE_OTHER_MSG, txnId);
            Propagate propagate = provider.propagateOther();
            logger.debug("Fetched {} message for {}: {}", PROPAGATE_OTHER_MSG, txnId, propagate);
            return propagate;
        }

        @Override
        public ApplyThenWaitUntilApplied applyThenWaitUntilApplied()
        {
            logger.debug("Fetching {} message for {}", APPLY_THEN_WAIT_UNTIL_APPLIED_REQ, txnId);
            ApplyThenWaitUntilApplied apply = provider.applyThenWaitUntilApplied();
            logger.debug("Fetched {} message for {}: {}", APPLY_THEN_WAIT_UNTIL_APPLIED_REQ, txnId, apply);
            return apply;
        }
    }

    /*
     * Methods that exist purely for testing
     */

    @Override
    public List<SavedCommand.LoadedDiff> loadAll(int storeId, TxnId txnId)
    {
        return (List<SavedCommand.LoadedDiff>)(List<?>) journal.readAll(new Key(txnId, Type.SAVED_COMMAND, storeId));
    }

    /*
     * Handling topology changes / epoch shift
     */

    private final class DelayedRequestProcessor extends Thread
    {
        private final ManyToOneConcurrentLinkedQueue<RequestContext> delayedRequests = new ManyToOneConcurrentLinkedQueue<>();
        private final LongArrayList waitForEpochs = new LongArrayList();
        private final Long2ObjectHashMap<List<RequestContext>> byEpoch = new Long2ObjectHashMap<>();
        private final AtomicReference<Condition> signal = new AtomicReference<>(Condition.newOneTimeCondition());

        private void delay(RequestContext requestContext)
        {
            delayedRequests.add(requestContext);
            runOnce();
        }

        private void runOnce()
        {
            signal.get().signal();
        }

        public void run()
        {
            while (true)
            {
                try
                {
                    Condition signal = Condition.newOneTimeCondition();
                    this.signal.set(signal);
                    // First, poll delayed requests, put them into by epoch
                    while (!delayedRequests.isEmpty())
                    {
                        RequestContext context = delayedRequests.poll();
                        long waitForEpoch = context.waitForEpoch;

                        List<RequestContext> l = byEpoch.computeIfAbsent(waitForEpoch, (ignore) -> new ArrayList<>());
                        if (l.isEmpty())
                            waitForEpochs.pushLong(waitForEpoch);
                        l.add(context);
                        node.withEpoch(waitForEpoch, this::runOnce);
                    }

                    // Next, process all delayed epochs
                    for (int i = 0; i < waitForEpochs.size(); i++)
                    {
                        long epoch = waitForEpochs.getLong(i);
                        if (node.topology().hasEpoch(epoch))
                        {
                            List<RequestContext> requests = byEpoch.remove(epoch);
                            assert requests != null : String.format("%s %s (%d)", byEpoch, waitForEpochs, epoch);
                            for (RequestContext request : requests)
                            {
                                try
                                {
                                    request.process(node, endpointMapper);
                                }
                                catch (Throwable t)
                                {
                                    logger.error(String.format("Caught an exception while processing a delayed request %s", request), t);
                                }
                            }
                        }
                    }

                    waitForEpochs.removeIfLong(epoch -> !byEpoch.containsKey(epoch));

                    signal.await();
                }
                catch (InterruptedException e)
                {
                    logger.info("Delayed request processor thread interrupted. Shutting down.");
                    return;
                }
                catch (Throwable t)
                {
                    logger.error("Caught an exception in delayed processor", t);
                }
            }
        }
    }
}