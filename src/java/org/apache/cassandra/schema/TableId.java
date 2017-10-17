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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * The unique identifier of a table.
 * <p>
 * This is essentially a UUID, but we wrap it as it's used quite a bit in the code and having a nicely name class make
 * the code more readable.
 */
public class TableId
{
    private final UUID id;

    private TableId(UUID id)
    {
        this.id = id;
    }

    public static TableId fromUUID(UUID id)
    {
        return new TableId(id);
    }

    public static TableId generate()
    {
        return new TableId(UUIDGen.getTimeUUID());
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
        assert SchemaConstants.isLocalSystemKeyspace(keyspace) || SchemaConstants.isReplicatedSystemKeyspace(keyspace);
        return new TableId(UUID.nameUUIDFromBytes(ArrayUtils.addAll(keyspace.getBytes(), table.getBytes())));
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

    public int serializedSize()
    {
        return 16;
    }

    public static TableId deserialize(DataInput in) throws IOException
    {
        return new TableId(new UUID(in.readLong(), in.readLong()));
    }
}
