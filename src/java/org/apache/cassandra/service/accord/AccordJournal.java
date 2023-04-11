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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.ValueSerializer;
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

/*
 *  TODO: expose more journal params via Config
 */
class AccordJournal
{
    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    final File directory;
    final Journal<Key, TxnRequest<?>> journal;

    AccordJournal()
    {
        directory = new File(DatabaseDescriptor.getAccordJournalDirectory());
        journal = new Journal<>("AccordJournal", directory, Params.DEFAULT, Key.SUPPORT, MESSAGE_SERIALIZER);
    }

    AccordJournal start()
    {
        journal.start();
        return this;
    }

    void shutdown()
    {
        journal.shutdown();
    }

    boolean mustAppend(PreLoadContext context)
    {
        return context instanceof TxnRequest && Type.mustAppend((TxnRequest<?>) context);
    }

    void append(int storeId, PreLoadContext context, Executor executor, Runnable onDurable)
    {
        append(storeId, (TxnRequest<?>) context, executor, onDurable);
    }

    void append(int storeId, TxnRequest<?> message, Executor executor, Runnable onDurable)
    {
        Key key = new Key(message.txnId, Type.fromMsgType(message.type()), storeId);
        journal.asyncWrite(key, message, SENTINEL_HOSTS, executor, onDurable);
    }

    TxnRequest<?> read(int storeId, TxnId txnId, Type type)
    {
        Key key = new Key(txnId, type, storeId);
        return journal.read(key);
    }

    PreAccept readPreAccept(int storeId, TxnId txnId)
    {
        return (PreAccept) read(storeId, txnId, Type.PREACCEPT_REQ);
    }

    Accept readAccept(int storeId, TxnId txnId)
    {
        return (Accept) read(storeId, txnId, Type.ACCEPT_REQ);
    }

    Commit readCommit(int storeId, TxnId txnId)
    {
        return (Commit) read(storeId, txnId, Type.COMMIT_REQ);
    }

    Apply readApply(int storeId, TxnId txnId)
    {
        return (Apply) read(storeId, txnId, Type.APPLY_REQ);
    }

    private static class Key
    {
        final TxnId txnId;
        final Type type;
        final int storeId;

        Key(TxnId txnId, Type type, int storeId)
        {
            this.txnId = txnId;
            this.type = type;
            this.storeId = storeId;
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
            private static final int STORE_ID_OFFSET        = TYPE_OFFSET            + BYTE_SIZE;

            @Override
            public int serializedSize(int version)
            {
                return LONG_SIZE // txnId.hlc()
                     + 6         // txnId.epoch()
                     + 2         // txnId.flags()
                     + INT_SIZE  // txnId.node
                     + BYTE_SIZE // type
                     + INT_SIZE; // storeId
            }

            @Override
            public void serialize(Key key, DataOutputPlus out, int version) throws IOException
            {
                serializeTxnId(key.txnId, out);
                out.writeByte(key.type.id);
                out.writeInt(key.storeId);
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
                int storeId = in.readInt();
                return new Key(txnId, Type.fromId(type), storeId);
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
                int storeId = buffer.getInt(position + STORE_ID_OFFSET);
                return new Key(txnId, Type.fromId(type), storeId);
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
                updateChecksumInt(crc, key.storeId);
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
                if (cmp != 0) return cmp;

                int storeId = buffer.getInt(position + STORE_ID_OFFSET);
                cmp = Integer.compareUnsigned(k.storeId, storeId);
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
                if (cmp == 0) cmp = Integer.compareUnsigned(k1.storeId, k2.storeId);
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
    }

    static final ValueSerializer<Key, TxnRequest<?>> MESSAGE_SERIALIZER = new ValueSerializer<Key, TxnRequest<?>>()
    {
        @Override
        public int serializedSize(TxnRequest<?> message, int version)
        {
            return Ints.checkedCast(Type.serializer(message).serializedSize(message, version));
        }

        @Override
        public void serialize(TxnRequest<?> message, DataOutputPlus out, int version) throws IOException
        {
            Type.serializer(message).serialize(message, out, version);
        }

        @Override
        public TxnRequest<?> deserialize(Key key, DataInputPlus in, int userVersion) throws IOException
        {
            return key.type.serializer.deserialize(in, userVersion);
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
    private enum Type
    {
        PREACCEPT_REQ (0, MessageType.PREACCEPT_REQ, PreacceptSerializers.request),
        ACCEPT_REQ    (1, MessageType.ACCEPT_REQ,    AcceptSerializers.request   ),
        COMMIT_REQ    (2, MessageType.COMMIT_REQ,    CommitSerializers.request   ),
        APPLY_REQ     (3, MessageType.APPLY_REQ,     ApplySerializers.request    );

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

        static boolean mustAppend(TxnRequest<?> message)
        {
            return msgTypeToTypeMap.containsKey(message.type());
        }

        static IVersionedSerializer<TxnRequest<?>> serializer(TxnRequest<?> msgType)
        {
            return fromMsgType(msgType.type()).serializer;
        }
    }
}
