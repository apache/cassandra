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
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node.Id;
import accord.local.SerializerSupport;
import accord.messages.AbstractEpochRequest;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.BeginRecovery;
import accord.messages.Commit;
import accord.messages.LocalMessage;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.PreAccept;
import accord.messages.Propagate;
import accord.messages.TxnRequest;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.AsyncWriteCallback;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.net.MessagingService;
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

import static accord.messages.MessageType.*;
import static org.apache.cassandra.db.TypeSizes.BYTE_SIZE;
import static org.apache.cassandra.db.TypeSizes.INT_SIZE;
import static org.apache.cassandra.db.TypeSizes.LONG_SIZE;

public class AccordJournal implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordJournal.class);

    private static final boolean LOG_MESSAGE_PROVIDER = false;

    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    private static final ThreadLocal<byte[]> keyCRCBytes = ThreadLocal.withInitial(() -> new byte[21]);

    static final Params PARAMS = new Params()
    {
        @Override
        public int segmentSize()
        {
            return 32 << 20;
        }

        @Override
        public FailurePolicy failurePolicy()
        {
            return FailurePolicy.STOP;
        }

        @Override
        public FlushMode flushMode()
        {
            return FlushMode.BATCH;
        }

        @Override
        public int flushPeriod()
        {
            return 1000;
        }

        @Override
        public int periodicFlushLagBlock()
        {
            return 1500;
        }

        @Override
        public int userVersion()
        {
            /*
             * NOTE: when accord journal version gets bumped, expose it via yaml.
             * This way operators can force previous version on upgrade, temporarily,
             * to allow easier downgrades if something goes wrong.
             */
            return 1;
        }
    };

    final File directory;
    final Journal<Key, Object> journal;

    enum Status { INITIALIZED, STARTING, STARTED, TERMINATING, TERMINATED }
    private volatile Status status = Status.INITIALIZED;

    @VisibleForTesting
    public AccordJournal()
    {
        directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
        journal = new Journal<>("AccordJournal", directory, PARAMS, Key.SUPPORT, RECORD_SERIALIZER);
    }

    public AccordJournal start()
    {
        Invariants.checkState(status == Status.INITIALIZED);
        status = Status.STARTING;
        journal.start();
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
        // TODO (expected)
        return true;
    }

    void appendAuxiliaryRecord(AuxiliaryRecord record)
    {
        Key key = new Key(record.timestamp, record.type());
        journal.write(key, record, SENTINEL_HOSTS);
    }

    public void appendMessage(Message message, Executor executor, AsyncWriteCallback callback)
    {
        Type type = Type.fromMessageType(message.type());
        Key key = new Key(type.txnId(message), type);
        journal.asyncWrite(key, message, SENTINEL_HOSTS, executor, callback);
    }

    @VisibleForTesting
    public void appendMessageBlocking(Message message)
    {
        Type type = Type.fromMessageType(message.type());
        Key key = new Key(type.txnId(message), type);
        journal.write(key, message, SENTINEL_HOSTS);
    }

    @VisibleForTesting
    public <M extends Message> M readMessage(TxnId txnId, Type type, Class<M> clazz)
    {
        return clazz.cast(journal.readFirst(new Key(txnId, type)));
    }

    private <M extends Message> M readMessage(TxnId txnId, Type type, Class<M> clazz, Predicate<Object> condition)
    {
        return clazz.cast(journal.readFirstMatching(new Key(txnId, type), condition));
    }

    static class Key
    {
        final Timestamp timestamp;
        final Type type;

        Key(Timestamp timestamp, Type type)
        {
            this.timestamp = timestamp;
            this.type = type;
        }

        /**
         * Support for (de)serializing and comparing record keys.
         * <p>
         * Implements its own serialization and comparison for {@link Timestamp} to satisty
         * {@link KeySupport} contract - puts hybrid logical clock ahead of epoch
         * when ordering timestamps. This is done for more precise elimination of candidate
         * segments by min/max record key in segment.
         */
        static final KeySupport<Key> SUPPORT = new KeySupport<Key>()
        {
            private static final int HLC_OFFSET             = 0;
            private static final int EPOCH_AND_FLAGS_OFFSET = HLC_OFFSET             + LONG_SIZE;
            private static final int NODE_OFFSET            = EPOCH_AND_FLAGS_OFFSET + LONG_SIZE;
            private static final int TYPE_OFFSET            = NODE_OFFSET            + INT_SIZE;

            @Override
            public int serializedSize(int userVersion)
            {
                return LONG_SIZE  // timestamp.hlc()
                     + 6          // timestamp.epoch()
                     + 2          // timestamp.flags()
                     + INT_SIZE   // timestamp.node
                     + BYTE_SIZE; // type
            }

            @Override
            public void serialize(Key key, DataOutputPlus out, int userVersion) throws IOException
            {
                serializeTimestamp(key.timestamp, out);
                out.writeByte(key.type.id);
            }

            private void serializeTimestamp(Timestamp timestamp, DataOutputPlus out) throws IOException
            {
                out.writeLong(timestamp.hlc());
                out.writeLong(epochAndFlags(timestamp));
                out.writeInt(timestamp.node.id);
            }

            private void serialize(Key key, byte[] out)
            {
                serializeTimestamp(key.timestamp, out);
                out[20] = (byte) (key.type.id & 0xFF);
            }

            private void serializeTimestamp(Timestamp timestamp, byte[] out)
            {
                ByteArrayUtil.putLong(out, 0, timestamp.hlc());
                ByteArrayUtil.putLong(out, 8, epochAndFlags(timestamp));
                ByteArrayUtil.putInt(out, 16, timestamp.node.id);
            }

            @Override
            public Key deserialize(DataInputPlus in, int userVersion) throws IOException
            {
                Timestamp timestamp = deserializeTimestamp(in);
                int type = in.readByte();
                return new Key(timestamp, Type.fromId(type));
            }

            private Timestamp deserializeTimestamp(DataInputPlus in) throws IOException
            {
                long hlc = in.readLong();
                long epochAndFlags = in.readLong();
                int nodeId = in.readInt();
                return Timestamp.fromValues(epoch(epochAndFlags), hlc, flags(epochAndFlags), new Id(nodeId));
            }

            @Override
            public Key deserialize(ByteBuffer buffer, int position, int userVersion)
            {
                Timestamp timestamp = deserializeTimestamp(buffer, position);
                int type = buffer.get(position + TYPE_OFFSET);
                return new Key(timestamp, Type.fromId(type));
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

    static final ValueSerializer<Key, Object> RECORD_SERIALIZER = new ValueSerializer<Key, Object>()
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
    private static final TxnIdProvider LOCAL = msg -> ((LocalMessage) msg).primaryTxnId();
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
        REPLAY               (0,                            ReplayRecord.SERIALIZER),

        /* Accord protocol requests */
        PRE_ACCEPT           (64, PRE_ACCEPT_REQ,           PreacceptSerializers.request,          TXN  ),
        ACCEPT               (65, ACCEPT_REQ,               AcceptSerializers.request,             TXN  ),
        ACCEPT_INVALIDATE    (66, ACCEPT_INVALIDATE_REQ,    AcceptSerializers.invalidate,          EPOCH),
        COMMIT_MINIMAL       (67, COMMIT_MINIMAL_REQ,       CommitSerializers.request,             TXN  ),
        COMMIT_MAXIMAL       (68, COMMIT_MAXIMAL_REQ,       CommitSerializers.request,             TXN  ),
        COMMIT_INVALIDATE    (69, COMMIT_INVALIDATE_REQ,    CommitSerializers.invalidate,          INVL),
        APPLY_MINIMAL        (70, APPLY_MINIMAL_REQ,        ApplySerializers.request,              TXN  ),
        APPLY_MAXIMAL        (71, APPLY_MAXIMAL_REQ,        ApplySerializers.request,              TXN  ),
        BEGIN_RECOVER        (72, BEGIN_RECOVER_REQ,        RecoverySerializers.request,           TXN  ),
        BEGIN_INVALIDATE     (73, BEGIN_INVALIDATE_REQ,     BeginInvalidationSerializers.request,  EPOCH),
        INFORM_OF_TXN        (74, INFORM_OF_TXN_REQ,        InformOfTxnIdSerializers.request,      EPOCH),
        INFORM_DURABLE       (75, INFORM_DURABLE_REQ,       InformDurableSerializers.request,      TXN  ),
        SET_SHARD_DURABLE    (76, SET_SHARD_DURABLE_REQ,    SetDurableSerializers.shardDurable,    EPOCH),
        SET_GLOBALLY_DURABLE (77, SET_GLOBALLY_DURABLE_REQ, SetDurableSerializers.globallyDurable, EPOCH),

        /* Accord local messages */
        PROPAGATE_PRE_ACCEPT (78, PROPAGATE_PRE_ACCEPT_MSG, FetchSerializers.propagate,            LOCAL),
        PROPAGATE_COMMIT     (79, PROPAGATE_COMMIT_MSG,     FetchSerializers.propagate,            LOCAL),
        PROPAGATE_APPLY      (80, PROPAGATE_APPLY_MSG,      FetchSerializers.propagate,            LOCAL),
        PROPAGATE_OTHER      (81, PROPAGATE_OTHER_MSG,      FetchSerializers.propagate,            LOCAL),
        ;

        final int id;
        final MessageType type;
        final TxnIdProvider txnIdProvider;
        final ValueSerializer<Key, Object> serializer;

        Type(int id, ValueSerializer<Key, ? extends AuxiliaryRecord> serializer)
        {
            this(id, null, serializer, null);
        }

        Type(int id, MessageType type, IVersionedSerializer<?> serializer, TxnIdProvider txnIdProvider)
        {
            //noinspection unchecked
            this(id, type, MessageSerializer.wrap((IVersionedSerializer<Message>) serializer), txnIdProvider);
        }

        Type(int id, MessageType type, ValueSerializer<Key, ?> serializer, TxnIdProvider txnIdProvider)
        {
            if (id < 0)
                throw new IllegalArgumentException("Negative Type id " + id);
            if (id > Byte.MAX_VALUE)
                throw new IllegalArgumentException("Type id doesn't fit in a single byte: " + id);

            this.id = id;
            this.type = type;
            //noinspection unchecked
            this.serializer = (ValueSerializer<Key, Object>) serializer;
            this.txnIdProvider = txnIdProvider;
        }

        private static final Type[] idToTypeMapping;
        private static final Map<MessageType, Type> msgTypeToTypeMap;

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

            EnumMap<MessageType, Type> msgTypeToType = new EnumMap<>(MessageType.class);
            for (Type type : types)
            {
                if (null != type.type && null != msgTypeToType.put(type.type, type))
                    throw new IllegalStateException("Duplicate MessageType " + type.type);
            }
            msgTypeToTypeMap = msgTypeToType;
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
        Invariants.checkState(MessagingService.current_version == MessagingService.VERSION_50, "Expected current version to be %d but given %d", MessagingService.VERSION_50, MessagingService.current_version);
    }

    private static int msVersion(int version)
    {
        switch (version)
        {
            default: throw new IllegalArgumentException();
            case 1: return MessagingService.VERSION_50;
        }
    }

    static abstract class AuxiliaryRecord
    {
        final Timestamp timestamp;

        AuxiliaryRecord(Timestamp timestamp)
        {
            this.timestamp = timestamp;
        }

        abstract Type type();
    }

    /*
     * Placeholder for future record.
     */
    static final class ReplayRecord extends AuxiliaryRecord
    {
        ReplayRecord(Timestamp timestamp)
        {
            super(timestamp);
        }

        @Override
        Type type()
        {
            return Type.REPLAY;
        }

        static final ValueSerializer<Key, ReplayRecord> SERIALIZER = new ValueSerializer<Key, ReplayRecord>()
        {
            @Override
            public int serializedSize(Key key, ReplayRecord record, int userVersion)
            {
                return 0;
            }

            @Override
            public void serialize(Key key, ReplayRecord record, DataOutputPlus out, int userVersion)
            {
            }

            @Override
            public ReplayRecord deserialize(Key key, DataInputPlus in, int userVersion)
            {
                return new ReplayRecord(key.timestamp);
            }
        };
    }

    SerializerSupport.MessageProvider makeMessageProvider(TxnId txnId)
    {
        return LOG_MESSAGE_PROVIDER ? new LoggingMessageProvider(txnId, new MessageProvider(txnId)) : new MessageProvider(txnId);
    }

    final class MessageProvider implements SerializerSupport.MessageProvider
    {
        final TxnId txnId;

        private MessageProvider(TxnId txnId)
        {
            this.txnId = txnId;
        }

        @Override
        public Set<MessageType> test(Set<MessageType> messages)
        {
            Set<Key> keys = new ObjectHashSet<>(messages.size() + 1, 0.9f);
            for (MessageType message : messages)
                keys.add(new Key(txnId, Type.fromMessageType(message)));
            Set<Key> presentKeys = journal.test(keys);
            EnumSet<MessageType> presentMessages = EnumSet.noneOf(MessageType.class);
            for (Key key : presentKeys)
                presentMessages.add(key.type.type);
            return presentMessages;
        }

        @Override
        public PreAccept preAccept()
        {
            return readMessage(txnId, Type.PRE_ACCEPT, PreAccept.class);
        }

        @Override
        public BeginRecovery beginRecover()
        {
            return readMessage(txnId, Type.BEGIN_RECOVER, BeginRecovery.class);
        }

        @Override
        public Propagate propagatePreAccept()
        {
            return readMessage(txnId, Type.PROPAGATE_PRE_ACCEPT, Propagate.class);
        }

        @Override
        public Accept accept(Ballot ballot)
        {
            return readMessage(txnId, Type.ACCEPT, Accept.class, (accept) -> ((Accept) accept).ballot.equals(ballot));
        }

        @Override
        public Commit commitMinimal()
        {
            return readMessage(txnId, Type.COMMIT_MINIMAL, Commit.class);
        }

        @Override
        public Commit commitMaximal()
        {
            return readMessage(txnId, Type.COMMIT_MAXIMAL, Commit.class);
        }

        @Override
        public Propagate propagateCommit()
        {
            return readMessage(txnId, Type.PROPAGATE_COMMIT, Propagate.class);
        }

        @Override
        public Apply applyMinimal()
        {
            return readMessage(txnId, Type.APPLY_MINIMAL, Apply.class);
        }

        @Override
        public Apply applyMaximal()
        {
            return readMessage(txnId, Type.APPLY_MAXIMAL, Apply.class);
        }

        @Override
        public Propagate propagateApply()
        {
            return readMessage(txnId, Type.PROPAGATE_APPLY, Propagate.class);
        }
    }

    final class LoggingMessageProvider implements SerializerSupport.MessageProvider
    {
        private final TxnId txnId;
        private final MessageProvider provider;

        LoggingMessageProvider(TxnId txnId, MessageProvider provider)
        {
            this.txnId = txnId;
            this.provider = provider;
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
        public Commit commitMinimal()
        {
            logger.debug("Fetching {} message for {}", COMMIT_MINIMAL_REQ, txnId);
            Commit commit = provider.commitMinimal();
            logger.debug("Fetched {} message for {}: {}", COMMIT_MINIMAL_REQ, txnId, commit);
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
        public Propagate propagateCommit()
        {
            logger.debug("Fetching {} message for {}", PROPAGATE_COMMIT_MSG, txnId);
            Propagate propagate = provider.propagateCommit();
            logger.debug("Fetched {} message for {}: {}", PROPAGATE_COMMIT_MSG, txnId, propagate);
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
    }
}
