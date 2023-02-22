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
import java.util.UUID;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * The unique identifier of a table.
 * <p>
 * This is essentially a UUID, but we wrap it as it's used quite a bit in the code and having a nicely named class make
 * the code more readable.
 */
public class TableId implements Comparable<TableId>
{
    // TODO: should this be a TimeUUID?
    private final UUID id;

    private TableId(UUID id)
    {
        this.id = id;
    }

    public static TableId fromUUID(UUID id)
    {
        return new TableId(id);
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
        return ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(id));
    }

    public UUID asUUID()
    {
        return id;
    }

    @Override
    public int compareTo(TableId that)
    {
        return this.id.compareTo(that.id);
    }

    @Override
    public final int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public final boolean equals(Object o)
    {
        return this == o || (o instanceof TableId && this.id.equals(((TableId) o).id));
    }

    @Override
    public String toString()
    {
        return id.toString();
    }

    public void serialize(DataOutput out) throws IOException
    {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }

    public <V> int serialize(V dst, ValueAccessor<V> accessor, int offset)
    {
        int position = offset;
        position += accessor.putLong(dst, position, id.getMostSignificantBits());
        position += accessor.putLong(dst, position, id.getLeastSignificantBits());
        return position - offset;
    }

    public final int serializedSize()
    {
        return 16;
    }

    public static TableId deserialize(DataInput in) throws IOException
    {
        return new TableId(new UUID(in.readLong(), in.readLong()));
    }

    public static <V> TableId deserialize(V src, ValueAccessor<V> accessor, int offset) throws IOException
    {
        return new TableId(new UUID(accessor.getLong(src, offset), accessor.getLong(src, offset + TypeSizes.LONG_SIZE)));
    }

    public static final IVersionedSerializer<TableId> serializer = new IVersionedSerializer<TableId>()
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
}
