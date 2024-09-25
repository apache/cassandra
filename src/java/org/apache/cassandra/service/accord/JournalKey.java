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

import accord.local.Node;
import accord.primitives.Timestamp;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.utils.ByteArrayUtil;

import static org.apache.cassandra.db.TypeSizes.INT_SIZE;
import static org.apache.cassandra.db.TypeSizes.LONG_SIZE;
import static org.apache.cassandra.db.TypeSizes.SHORT_SIZE;

public final class JournalKey
{
    final AccordJournal.Type type;
    public final Timestamp timestamp;
    // TODO: command store id _before_ timestamp
    public final int commandStoreId;

    JournalKey(Timestamp timestamp)
    {
        this(timestamp, -1);
    }

    JournalKey(Timestamp timestamp, int commandStoreId)
    {
        if (timestamp == null) throw new NullPointerException("Null timestamp");
        this.timestamp = timestamp;
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
    public static final KeySupport<JournalKey> SUPPORT = new KeySupport<>()
    {
        private static final int HLC_OFFSET = 0;
        private static final int EPOCH_AND_FLAGS_OFFSET = HLC_OFFSET + LONG_SIZE;
        private static final int NODE_OFFSET = EPOCH_AND_FLAGS_OFFSET + LONG_SIZE;
        private static final int CS_ID_OFFSET = NODE_OFFSET + INT_SIZE;

        @Override
        public int serializedSize(int userVersion)
        {
            return LONG_SIZE   // timestamp.hlc()
                   + 6           // timestamp.epoch()
                   + 2           // timestamp.flags()
                   + INT_SIZE    // timestamp.node
                   + SHORT_SIZE; // commandStoreId
        }

        @Override
        public void serialize(JournalKey key, DataOutputPlus out, int userVersion) throws IOException
        {
            serializeTimestamp(key.timestamp, out);
            out.writeShort(key.commandStoreId);
        }

        private void serialize(JournalKey key, byte[] out)
        {
            serializeTimestamp(key.timestamp, out);
            ByteArrayUtil.putShort(out, 20, (short) key.commandStoreId);
        }

        @Override
        public JournalKey deserialize(DataInputPlus in, int userVersion) throws IOException
        {
            Timestamp timestamp = deserializeTimestamp(in);
            int commandStoreId = in.readShort();
            return new JournalKey(timestamp, commandStoreId);
        }

        @Override
        public JournalKey deserialize(ByteBuffer buffer, int position, int userVersion)
        {
            Timestamp timestamp = deserializeTimestamp(buffer, position);
            int commandStoreId = buffer.getShort(position + CS_ID_OFFSET);
            return new JournalKey(timestamp, commandStoreId);
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
            return Timestamp.fromValues(epoch(epochAndFlags), hlc, flags(epochAndFlags), new Node.Id(nodeId));
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
            return Timestamp.fromValues(epoch(epochAndFlags), hlc, flags(epochAndFlags), new Node.Id(nodeId));
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
            int cmp = compareWithTimestampAt(k.timestamp, buffer, position);
            if (cmp != 0) return cmp;

            short commandStoreId = buffer.getShort(position + CS_ID_OFFSET);
            cmp = Short.compare((byte) k.commandStoreId, commandStoreId);
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
        public int compare(JournalKey k1, JournalKey k2)
        {
            int cmp = compare(k1.timestamp, k2.timestamp);
            if (cmp == 0) cmp = Short.compare((short) k1.commandStoreId, (short) k2.commandStoreId);
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
        return (other instanceof JournalKey) && equals((JournalKey) other);
    }

    boolean equals(JournalKey other)
    {
        return this.timestamp.equals(other.timestamp) &&
               this.commandStoreId == other.commandStoreId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timestamp, commandStoreId);
    }

    public String toString()
    {
        return "Key{" +
               "timestamp=" + timestamp +
               ", commandStoreId=" + commandStoreId +
               '}';
    }

    public enum Type implements ValueSerializer<JournalKey, Object>
    {
        COMMAND_DIFF                 (1, new NoOpSerializer()),
        ;

        final int id;
        final ValueSerializer<JournalKey, Object> serializer;

        Type(int id, ValueSerializer<JournalKey, Object> serializer)
        {
            this.id = id;
            this.serializer = serializer;
        }
        // TODO: merger
    }

    private static class NoOpSerializer implements ValueSerializer<JournalKey, Object>
    {
            public int serializedSize(JournalKey key, Object value, int userVersion)
            {
                throw new UnsupportedOperationException();
            }

            public void serialize(JournalKey key, Object value, DataOutputPlus out, int userVersion)
            {
                throw new UnsupportedOperationException();
            }

            public Object deserialize(JournalKey key, DataInputPlus in, int userVersion)
            {
                throw new UnsupportedOperationException();
            }
        }

}
