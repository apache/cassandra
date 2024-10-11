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
import java.util.Objects;
import java.util.zip.Checksum;

import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.BootstrapBeganAtSerializer;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.CommandDiffSerializer;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.DurableBeforeSerializer;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.FlyweightSerializer;
import org.apache.cassandra.service.accord.AccordJournalValueSerializers.RedundantBeforeSerializer;
import org.apache.cassandra.utils.ByteArrayUtil;

import static org.apache.cassandra.db.TypeSizes.BYTE_SIZE;
import static org.apache.cassandra.db.TypeSizes.INT_SIZE;
import static org.apache.cassandra.db.TypeSizes.LONG_SIZE;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.RangesForEpochSerializer;
import static org.apache.cassandra.service.accord.AccordJournalValueSerializers.SafeToReadSerializer;

public final class JournalKey
{
    public final Type type;
    public final TxnId id;
    public final int commandStoreId;

    public JournalKey(TxnId id, Type type, int commandStoreId)
    {
        Invariants.nonNull(type);
        Invariants.nonNull(id);
        this.type = type;
        this.id = id;
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
    public static final JournalKeySupport SUPPORT = new JournalKeySupport();

    public static final class JournalKeySupport implements KeySupport<JournalKey>
    {
        private static final int CS_ID_OFFSET = 0;
        private static final int TYPE_OFFSET = INT_SIZE;
        private static final int MSB_OFFSET = TYPE_OFFSET + BYTE_SIZE;
        private static final int LSB_OFFSET = MSB_OFFSET + LONG_SIZE;
        private static final int NODE_OFFSET = LSB_OFFSET + LONG_SIZE;
        // TODO (required): revisit commandStoreId - this can go arbitrarily high so may want to use vint
        public static final int TOTAL_SIZE = NODE_OFFSET + INT_SIZE;

        @Override
        public int serializedSize(int userVersion)
        {
            return TOTAL_SIZE;
        }

        @Override
        public void serialize(JournalKey key, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeInt(key.commandStoreId);
            out.writeByte(key.type.id);
            serializeTxnId(key.id, out);
        }

        private void serialize(JournalKey key, byte[] out)
        {
            ByteArrayUtil.putInt(out, CS_ID_OFFSET, key.commandStoreId);
            out[TYPE_OFFSET] = (byte) (key.type.id & 0xFF);
            serializeTxnId(key.id, out);
        }

        @Override
        public JournalKey deserialize(DataInputPlus in, int userVersion) throws IOException
        {
            int commandStoreId = in.readInt();
            int type = in.readByte();
            TxnId txnId = deserializeTxnId(in);
            return new JournalKey(txnId, Type.fromId(type), commandStoreId);
        }

        @Override
        public JournalKey deserialize(ByteBuffer buffer, int position, int userVersion)
        {
            int commandStoreId = buffer.getInt(position + CS_ID_OFFSET);
            int type = buffer.get(position + TYPE_OFFSET);
            TxnId txnId = deserializeTxnId(buffer, position);
            return new JournalKey(txnId, Type.fromId(type), commandStoreId);
        }

        private void serializeTxnId(TxnId txnId, DataOutputPlus out) throws IOException
        {
            out.writeLong(txnId.msb);
            out.writeLong(txnId.lsb);
            out.writeInt(txnId.node.id);
        }

        private TxnId deserializeTxnId(DataInputPlus in) throws IOException
        {
            long msb = in.readLong();
            long lsb = in.readLong();
            int nodeId = in.readInt();
            return TxnId.fromBits(msb, lsb, new Id(nodeId));
        }

        private void serializeTxnId(TxnId txnId, byte[] out)
        {
            ByteArrayUtil.putLong(out, MSB_OFFSET, txnId.msb);
            ByteArrayUtil.putLong(out, LSB_OFFSET, txnId.lsb);
            ByteArrayUtil.putInt(out, NODE_OFFSET, txnId.node.id);
        }

        private TxnId deserializeTxnId(ByteBuffer buffer, int position)
        {
            long msb = buffer.getLong(position + MSB_OFFSET);
            long lsb = buffer.getLong(position + LSB_OFFSET);
            int nodeId = buffer.getInt(position + NODE_OFFSET);
            return TxnId.fromBits(msb, lsb, new Id(nodeId));
        }

        @Override
        public void updateChecksum(Checksum crc, JournalKey key, int userVersion)
        {
            byte[] out = AccordJournal.keyCRCBytes.get();
            serialize(key, out);
            crc.update(out, 0, out.length);
        }

        @Override
        public int compareWithKeyAt(JournalKey k, ByteBuffer buffer, int position, int userVersion)
        {
            int commandStoreId = buffer.getInt(position + CS_ID_OFFSET);
            int cmp = Integer.compare(k.commandStoreId, commandStoreId);
            if (cmp != 0) return cmp;

            byte type = buffer.get(position + TYPE_OFFSET);
            cmp = Byte.compare((byte) k.type.id, type);
            if (cmp != 0) return cmp;

            cmp = compareWithTxnIdAt(k.id, buffer, position);
            return cmp;
        }

        private int compareWithTxnIdAt(TxnId txnId, ByteBuffer buffer, int position)
        {
            long msb = buffer.getLong(position + MSB_OFFSET);
            int cmp = Timestamp.compareMsb(txnId.msb, msb);
            if (cmp != 0) return cmp;

            long lsb = buffer.getLong(position + LSB_OFFSET);
            cmp = Timestamp.compareLsb(txnId.lsb, lsb);
            if (cmp != 0) return cmp;

            int nodeId = buffer.getInt(position + NODE_OFFSET);
            cmp = Integer.compare(txnId.node.id, nodeId);
            return cmp;
        }

        @Override
        public int compare(JournalKey k1, JournalKey k2)
        {
            int cmp = Integer.compare(k1.commandStoreId, k2.commandStoreId);
            if (cmp == 0) cmp = Byte.compare((byte) k1.type.id, (byte) k2.type.id);
            if (cmp == 0) cmp = k1.id.compareTo(k2.id);
            return cmp;
        }
    };

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        return (other instanceof JournalKey) && equals((JournalKey) other);
    }

    boolean equals(JournalKey other)
    {
        return this.id.equals(other.id) &&
               this.type == other.type &&
               this.commandStoreId == other.commandStoreId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, type, commandStoreId);
    }

    public String toString()
    {
        return "Key{" +
               "id=" + id +
               "type=" + type +
               ", commandStoreId=" + commandStoreId +
               '}';
    }

    public enum Type
    {
        COMMAND_DIFF                 (0, new CommandDiffSerializer()),
        REDUNDANT_BEFORE             (1, new RedundantBeforeSerializer()),
        DURABLE_BEFORE               (2, new DurableBeforeSerializer()),
        SAFE_TO_READ                 (3, new SafeToReadSerializer()),
        BOOTSTRAP_BEGAN_AT           (4, new BootstrapBeganAtSerializer()),
        RANGES_FOR_EPOCH             (5, new RangesForEpochSerializer()),
        ;

        public final int id;
        public final FlyweightSerializer<?, ?> serializer;

        Type(int id, FlyweightSerializer<?, ?> serializer)
        {
            this.id = id;
            this.serializer = serializer;
        }

        private static final Type[] idToTypeMapping;

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
    }
}
