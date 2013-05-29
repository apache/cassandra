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
package org.apache.cassandra.repair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import com.google.common.base.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * RepairJobDesc is used from various repair processes to distinguish one RepairJob to another.
 *
 * @since 2.0
 */
public class RepairJobDesc
{
    public static final IVersionedSerializer<RepairJobDesc> serializer = new RepairJobDescSerializer();

    /** RepairSession id */
    public final UUID sessionId;
    public final String keyspace;
    public final String columnFamily;
    /** repairing range  */
    public final Range<Token> range;

    public RepairJobDesc(UUID sessionId, String keyspace, String columnFamily, Range<Token> range)
    {
        this.sessionId = sessionId;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.range = range;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("[repair #");
        sb.append(sessionId);
        sb.append(" on ");
        sb.append(keyspace).append("/").append(columnFamily);
        sb.append(", ").append(range);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepairJobDesc that = (RepairJobDesc) o;

        if (!columnFamily.equals(that.columnFamily)) return false;
        if (!keyspace.equals(that.keyspace)) return false;
        if (range != null ? !range.equals(that.range) : that.range != null) return false;
        if (!sessionId.equals(that.sessionId)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sessionId, keyspace, columnFamily, range);
    }

    private static class RepairJobDescSerializer implements IVersionedSerializer<RepairJobDesc>
    {
        public void serialize(RepairJobDesc desc, DataOutput out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(desc.sessionId, out, version);
            out.writeUTF(desc.keyspace);
            out.writeUTF(desc.columnFamily);
            AbstractBounds.serializer.serialize(desc.range, out, version);
        }

        public RepairJobDesc deserialize(DataInput in, int version) throws IOException
        {
            UUID sessionId = UUIDSerializer.serializer.deserialize(in, version);
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            Range<Token> range = (Range<Token>)AbstractBounds.serializer.deserialize(in, version);
            return new RepairJobDesc(sessionId, keyspace, columnFamily, range);
        }

        public long serializedSize(RepairJobDesc desc, int version)
        {
            int size = 0;
            size += UUIDSerializer.serializer.serializedSize(desc.sessionId, version);
            size += TypeSizes.NATIVE.sizeof(desc.keyspace);
            size += TypeSizes.NATIVE.sizeof(desc.columnFamily);
            size += AbstractBounds.serializer.serializedSize(desc.range, version);
            return size;
        }
    }
}
