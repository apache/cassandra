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
import java.util.*;
import java.util.concurrent.Executor;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.PreAccept;
import accord.messages.TxnRequest;
import accord.primitives.*;
import accord.utils.Invariants;
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
import org.apache.cassandra.service.accord.serializers.CommitSerializers;
import org.apache.cassandra.service.accord.serializers.EnumSerializer;
import org.apache.cassandra.service.accord.serializers.PreacceptSerializers;

import static org.apache.cassandra.db.TypeSizes.BYTE_SIZE;
import static org.apache.cassandra.db.TypeSizes.INT_SIZE;
import static org.apache.cassandra.db.TypeSizes.LONG_SIZE;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumLong;

public class AccordJournal
{
    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

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
            return FlushMode.GROUP;
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
    final Journal<Key, TxnRequest<?>> journal;

    @VisibleForTesting
    public AccordJournal()
    {
        directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
        journal = new Journal<>("AccordJournal", directory, PARAMS, Key.SUPPORT, MESSAGE_SERIALIZER);
    }

    public AccordJournal start()
    {
        // journal.start(); TODO: re-enable
        return this;
    }

    public void shutdown()
    {
        // journal.shutdown(); TODO: re-enable
    }

    boolean mustMakeDurable(PreLoadContext context)
    {
        return false;
        // return context instanceof TxnRequest && Type.mustMakeDurable((TxnRequest<?>) context); TODO: re-enable
    }

    public void append(PreLoadContext context, Executor executor, AsyncWriteCallback callback)
    {
        append((TxnRequest<?>) context, executor, callback);
    }

    public void append(TxnRequest<?> message, Executor executor, AsyncWriteCallback callback)
    {
        Key key = new Key(message.txnId, Type.fromMsgType(message.type()));
        journal.asyncWrite(key, message, SENTINEL_HOSTS, executor, callback);
    }

    public TxnRequest<?> read(TxnId txnId, Type type)
    {
        Key key = new Key(txnId, type);
        return journal.read(key);
    }

    PreAccept readPreAccept(TxnId txnId)
    {
        return (PreAccept) read(txnId, Type.PREACCEPT_REQ);
    }

    Accept readAccept(TxnId txnId)
    {
        return (Accept) read(txnId, Type.ACCEPT_REQ);
    }

    Commit readCommit(TxnId txnId)
    {
        return (Commit) read(txnId, Type.COMMIT_REQ);
    }

    Apply readApply(TxnId txnId)
    {
        return (Apply) read(txnId, Type.APPLY_REQ);
    }

    static class Key
    {
        final TxnId txnId;
        final Type type;

        Key(TxnId txnId, Type type)
        {
            this.txnId = txnId;
            this.type = type;
        }

        /**
         * Support for (de)serializing and comparing record keys.
         * <p>
         * Implements its own serialization and comparison for {@link TxnId} to satisty
         * {@link KeySupport} contract - puts hybrid logical clock ahead of epoch
         * when ordering txn ids. This is done for more precise elimination of candidate
         * segments by min/max record key in segment.
         */
        static final KeySupport<Key> SUPPORT = new KeySupport<Key>()
        {
            private static final int HLC_OFFSET             = 0;
            private static final int EPOCH_AND_FLAGS_OFFSET = HLC_OFFSET             + LONG_SIZE;
            private static final int NODE_OFFSET            = EPOCH_AND_FLAGS_OFFSET + LONG_SIZE;
            private static final int TYPE_OFFSET            = NODE_OFFSET            + INT_SIZE;

            @Override
            public int serializedSize(int version)
            {
                return LONG_SIZE  // txnId.hlc()
                     + 6          // txnId.epoch()
                     + 2          // txnId.flags()
                     + INT_SIZE   // txnId.node
                     + BYTE_SIZE; // type
            }

            @Override
            public void serialize(Key key, DataOutputPlus out, int version) throws IOException
            {
                serializeTxnId(key.txnId, out);
                out.writeByte(key.type.id);
            }

            private void serializeTxnId(TxnId txnId, DataOutputPlus out) throws IOException
            {
                out.writeLong(txnId.hlc());
                out.writeLong(epochAndFlags(txnId));
                out.writeInt(txnId.node.id);
            }

            @Override
            public Key deserialize(DataInputPlus in, int version) throws IOException
            {
                TxnId txnId = deserializeTxnId(in);
                int type = in.readByte();
                return new Key(txnId, Type.fromId(type));
            }

            private TxnId deserializeTxnId(DataInputPlus in) throws IOException
            {
                long hlc = in.readLong();
                long epochAndFlags = in.readLong();
                int nodeId = in.readInt();
                return TxnId.fromValues(epoch(epochAndFlags), hlc, flags(epochAndFlags), new Id(nodeId));
            }

            @Override
            public Key deserialize(ByteBuffer buffer, int position, int version)
            {
                TxnId txnId = deserializeTxnId(buffer, position);
                int type = buffer.get(position + TYPE_OFFSET);
                return new Key(txnId, Type.fromId(type));
            }

            private TxnId deserializeTxnId(ByteBuffer buffer, int position)
            {
                long hlc = buffer.getLong(position + HLC_OFFSET);
                long epochAndFlags = buffer.getLong(position + EPOCH_AND_FLAGS_OFFSET);
                int nodeId = buffer.getInt(position + NODE_OFFSET);
                return TxnId.fromValues(epoch(epochAndFlags), hlc, flags(epochAndFlags), new Id(nodeId));
            }

            @Override
            public void updateChecksum(Checksum crc, Key key, int version)
            {
                updateChecksum(crc, key.txnId);
                crc.update(key.type.id & 0xFF);
            }

            private void updateChecksum(Checksum crc, TxnId txnId)
            {
                updateChecksumLong(crc, txnId.hlc());
                updateChecksumLong(crc, epochAndFlags(txnId));
                updateChecksumInt(crc, txnId.node.id);
            }

            @Override
            public int compareWithKeyAt(Key k, ByteBuffer buffer, int position, int version)
            {
                int cmp = compareWithTxnIdAt(k.txnId, buffer, position);
                if (cmp != 0) return cmp;

                byte type = buffer.get(position + TYPE_OFFSET);
                cmp = Byte.compare((byte) k.type.id, type);
                return cmp;
            }

            private int compareWithTxnIdAt(TxnId txnId, ByteBuffer buffer, int position)
            {
                long hlc = buffer.getLong(position + HLC_OFFSET);
                int cmp = Long.compareUnsigned(txnId.hlc(), hlc);
                if (cmp != 0) return cmp;

                long epochAndFlags = buffer.getLong(position + EPOCH_AND_FLAGS_OFFSET);
                cmp = Long.compareUnsigned(epochAndFlags(txnId), epochAndFlags);
                if (cmp != 0) return cmp;

                int nodeId = buffer.getInt(position + NODE_OFFSET);
                cmp = Integer.compareUnsigned(txnId.node.id, nodeId);
                return cmp;
            }

            @Override
            public int compare(Key k1, Key k2)
            {
                int cmp = compare(k1.txnId, k2.txnId);
                if (cmp == 0) cmp = Byte.compare((byte) k1.type.id, (byte) k2.type.id);
                return cmp;
            }

            private int compare(TxnId txnId1, TxnId txnId2)
            {
                int cmp = Long.compareUnsigned(txnId1.hlc(), txnId2.hlc());
                if (cmp == 0) cmp = Long.compareUnsigned(epochAndFlags(txnId1), epochAndFlags(txnId2));
                if (cmp == 0) cmp = Integer.compareUnsigned(txnId1.node.id, txnId2.node.id);
                return cmp;
            }

            private long epochAndFlags(TxnId txnId)
            {
                return (txnId.epoch() << 16) | (long) txnId.flags();
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
            return this.type == other.type && this.txnId.equals(other.txnId);
        }

        @Override
        public int hashCode()
        {
            return type.hashCode() + 31 * txnId.hashCode();
        }

        @Override
        public String toString()
        {
            return "Key{" + txnId + ", " + type + '}';
        }
    }

    static final ValueSerializer<Key, TxnRequest<?>> MESSAGE_SERIALIZER = new ValueSerializer<Key, TxnRequest<?>>()
    {
        @Override
        public int serializedSize(TxnRequest<?> message, int version)
        {
            return Ints.checkedCast(Type.ofMessage(message).serializedSize(message, version));
        }

        @Override
        public void serialize(TxnRequest<?> message, DataOutputPlus out, int version) throws IOException
        {
            Type.ofMessage(message).serialize(message, out, version);
        }

        @Override
        public TxnRequest<?> deserialize(Key key, DataInputPlus in, int version) throws IOException
        {
            return key.type.deserialize(in, version);
        }
    };

    /**
     * Accord Message type - consequently the kind of persisted record.
     * <p>
     * Note: {@link EnumSerializer} is intentionally not being reused here, for two reasons:
     *  1. This is an internal enum, fully under our control, not part of an external library
     *  2. It's persisted in the record key, so has the additional constraint of being fixed size and
     *     shouldn't be using varint encoding
     */
    public enum Type implements IVersionedSerializer<TxnRequest<?>>
    {
        PREACCEPT_REQ (0, MessageType.PRE_ACCEPT_REQ, PreacceptSerializers.request),
        ACCEPT_REQ    (1, MessageType.ACCEPT_REQ,     AcceptSerializers.request   ),
        COMMIT_REQ    (2, MessageType.COMMIT_REQ,     CommitSerializers.request   ),
        APPLY_REQ     (3, MessageType.APPLY_REQ,      ApplySerializers.request    );

        final int id;
        final MessageType msgType;
        final IVersionedSerializer<TxnRequest<?>> serializer;

        Type(int id, MessageType msgType, IVersionedSerializer<? extends TxnRequest<?>> serializer)
        {
            if (id < 0)
                throw new IllegalArgumentException("Negative Type id " + id);
            if (id > Byte.MAX_VALUE)
                throw new IllegalArgumentException("Type id doesn't fit in a single byte: " + id);

            this.id = id;
            this.msgType = msgType;

            //noinspection unchecked
            this.serializer = (IVersionedSerializer<TxnRequest<?>>) serializer;
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
                if (null != msgTypeToType.put(type.msgType, type))
                    throw new IllegalStateException("Duplicate MessageType " + type.msgType);
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

        static Type fromMsgType(MessageType msgType)
        {
            Type type = msgTypeToTypeMap.get(msgType);
            if (null == type)
                throw new IllegalArgumentException("Unsupported MessageType " + msgType);
            return type;
        }

        static Type ofMessage(TxnRequest<?> request)
        {
            return fromMsgType(request.type());
        }

        static boolean mustMakeDurable(TxnRequest<?> message)
        {
            return message.type().hasSideEffects;
        }

        @Override
        public void serialize(TxnRequest<?> request, DataOutputPlus out, int version) throws IOException
        {
            serializer.serialize(request, out, msVersion(version));
        }

        @Override
        public TxnRequest<?> deserialize(DataInputPlus in, int version) throws IOException
        {
            return serializer.deserialize(in, msVersion(version));
        }

        @Override
        public long serializedSize(TxnRequest<?> request, int version)
        {
            return serializer.serializedSize(request, msVersion(version));
        }

        static
        {
            // make noise early if we forget to update our version mappings
            Invariants.checkState(MessagingService.current_version == MessagingService.VERSION_50);
        }

        private static int msVersion(int version)
        {
            switch (version)
            {
                default: throw new IllegalArgumentException();
                case 1: return MessagingService.VERSION_50;
            }
        }
    }
}
