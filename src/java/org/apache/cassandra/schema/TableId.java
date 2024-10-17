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
package org.apache.cassandra.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.agrona.collections.Hashing;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * The unique identifier of a table.
 * <p>
 * This is essentially a UUID, but we wrap it as it's used quite a bit in the code and having a nicely named class make
 * the code more readable.
 */
public final class TableId implements Comparable<TableId>
{
    public static final long MAGIC = 1956074401491665062L;
    public static final long EMPTY_SIZE = ObjectSizes.measureDeep(new UUID(0, 0));

    private static final ConcurrentHashMap<TableId, TableId> internCache = new ConcurrentHashMap<>();

    final long msb, lsb;

    private TableId(UUID id)
    {
        this(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    private TableId(long msb, long lsb)
    {
        this.msb = msb;
        this.lsb = lsb;
    }

    public static TableId fromUUID(UUID id)
    {
        return new TableId(id);
    }

    public static TableId fromRaw(long msb, long lsb)
    {
        return new TableId(msb, lsb);
    }

    // TODO: should we be using UUID.randomUUID()?
    public static TableId generate()
    {
        return new TableId(nextTimeUUID().asUUID());
    }

    public static TableId fromString(String idString)
    {
        return new TableId(UUID.fromString(idString));
    }

    public static TableId get(ClusterMetadata prev)
    {
        int i = 0;
        while (true)
        {
            TableId tableId = TableId.fromLong(prev.epoch.getEpoch() + i);
            if (!tableIdExists(prev, tableId))
                return tableId;
            i++;
        }
    }

    private static boolean tableIdExists(ClusterMetadata metadata, TableId tableId)
    {
        return metadata.schema.getKeyspaces().stream().anyMatch(ks -> ks.tables.containsTable(tableId));
    }

    @Nullable
    public static Pair<String, TableId> tableNameAndIdFromFilename(String filename)
    {
        int dash = filename.lastIndexOf('-');
        if (dash <= 0 || dash != filename.length() - 32 - 1)
            return null;

        TableId id = fromHexString(filename.substring(dash + 1));
        String tableName = filename.substring(0, dash);

        return Pair.create(tableName, id);
    }

    private static TableId fromHexString(String nonDashUUID)
    {
        ByteBuffer bytes = ByteBufferUtil.hexToBytes(nonDashUUID);
        long msb = bytes.getLong(0);
        long lsb = bytes.getLong(8);
        return fromUUID(new UUID(msb, lsb));
    }

    public static TableId fromLong(long start)
    {
        return TableId.fromUUID(new UUID(MAGIC, start));
    }

    /**
     * Creates the UUID of a system table.
     *
     * This is deterministically based on the table name as system tables are hardcoded and initialized independently
     * on each node (they don't go through a CREATE), but we still want them to have the same ID everywhere.
     *
     * We shouldn't use this for any other table.
     */
    public static TableId forSystemTable(String keyspace, String table)
    {
        assert SchemaConstants.isSystemKeyspace(keyspace) : String.format("Table %s.%s is not a system table; only keyspaces allowed are %s", keyspace, table, SchemaConstants.getSystemKeyspaces());
        return unsafeDeterministic(keyspace, table);
    }

    public static TableId unsafeDeterministic(String keyspace, String table)
    {
        return new TableId(UUID.nameUUIDFromBytes(ArrayUtils.addAll(keyspace.getBytes(UTF_8), table.getBytes(UTF_8))));
    }

    public String toHexString()
    {
        return ByteBufferUtil.bytesToHex(ByteBuffer.wrap(UUIDGen.decompose(msb, lsb)));
    }

    public UUID asUUID()
    {
        return new UUID(msb, lsb);
    }

    @Override
    public int hashCode()
    {
        return Hashing.hash(msb ^ lsb);
    }

    @Override
    public final boolean equals(Object o)
    {
        if (o == this | o == null) return o == this;
        if (o.getClass() != TableId.class) return false;
        TableId that = (TableId) o;
        return this.msb == that.msb && this.lsb == that.lsb;
    }

    @Override
    public String toString()
    {
        return new UUID(msb, lsb).toString();
    }

    public void serialize(DataOutput out) throws IOException
    {
        out.writeLong(msb);
        out.writeLong(lsb);
    }

    public <V> int serialize(V dst, ValueAccessor<V> accessor, int offset)
    {
        int position = offset;
        position += accessor.putLong(dst, position, msb);
        position += accessor.putLong(dst, position, lsb);
        return position - offset;
    }

    public final int serializedSize()
    {
        return 16;
    }

    public static int staticSerializedSize()
    {
        return 16;
    }

    public static TableId deserialize(DataInput in) throws IOException
    {
        return new TableId(in.readLong(), in.readLong());
    }

    public static <V> TableId deserialize(V src, ValueAccessor<V> accessor, int offset)
    {
        return new TableId(accessor.getLong(src, offset), accessor.getLong(src, offset + TypeSizes.LONG_SIZE));
    }

    public TableId intern()
    {
        TableId interned = internCache.putIfAbsent(this, this);
        return interned == null ? this : interned;
    }

    @Override
    public int compareTo(TableId that)
    {
        int c = Long.compare(this.msb, that.msb);
        return c != 0 ? c : Long.compare(this.lsb, that.lsb);
    }

    public static final IVersionedSerializer<TableId> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TableId t, DataOutputPlus out, int version) throws IOException
        {
            t.serialize(out);
        }

        @Override
        public TableId deserialize(DataInputPlus in, int version) throws IOException
        {
            return TableId.deserialize(in);
        }

        @Override
        public long serializedSize(TableId t, int version)
        {
            return t.serializedSize();
        }
    };

    public static final MetadataSerializer<TableId> metadataSerializer = new MetadataSerializer<TableId>()
    {
        @Override
        public void serialize(TableId t, DataOutputPlus out, Version version) throws IOException
        {
            t.serialize(out);
        }

        @Override
        public TableId deserialize(DataInputPlus in, Version version) throws IOException
        {
            return TableId.deserialize(in);
        }

        @Override
        public long serializedSize(TableId t, Version version)
        {
            return t.serializedSize();
        }
    };

    public static void scheduleCachePruning()
    {
        ScheduledExecutors.scheduledFastTasks.scheduleSelfRecurring(internCache::clear, 1, TimeUnit.HOURS);
    }
}
