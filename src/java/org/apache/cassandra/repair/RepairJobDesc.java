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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.ByteBufferUtil.getArray;

/**
 * RepairJobDesc is used from various repair processes to distinguish one RepairJob to another.
 *
 * @since 2.0
 */
public class RepairJobDesc
{
    public static final IVersionedSerializer<RepairJobDesc> serializer = new RepairJobDescSerializer();

    public final TimeUUID parentSessionId;
    /** RepairSession id */
    public final TimeUUID sessionId;
    public final String keyspace;
    public final String columnFamily;
    /** repairing range  */
    public final Collection<Range<Token>> ranges;

    public RepairJobDesc(TimeUUID parentSessionId, TimeUUID sessionId, String keyspace, String columnFamily, Collection<Range<Token>> ranges)
    {
        this.parentSessionId = parentSessionId;
        this.sessionId = sessionId;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.ranges = ranges;
    }

    public UUID determanisticId()
    {
        byte[] bytes = getArray(bytes(parentSessionId));
        bytes = ArrayUtils.addAll(bytes, getArray(bytes(sessionId)));
        bytes = ArrayUtils.addAll(bytes, keyspace.getBytes(StandardCharsets.UTF_8));
        bytes = ArrayUtils.addAll(bytes, columnFamily.getBytes(StandardCharsets.UTF_8));
        bytes = ArrayUtils.addAll(bytes, ranges.toString().getBytes(StandardCharsets.UTF_8));
        return UUID.nameUUIDFromBytes(bytes);
    }

    @Override
    public String toString()
    {
        return "[repair #" + sessionId + " on " + keyspace + "/" + columnFamily + ", " + ranges + "]";
    }

    public String toString(PreviewKind previewKind)
    {
        return '[' + previewKind.logPrefix() + " #" + sessionId + " on " + keyspace + "/" + columnFamily + ", " + ranges + "]";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepairJobDesc that = (RepairJobDesc) o;

        if (!Objects.equals(parentSessionId, that.parentSessionId)) return false;
        if (!sessionId.equals(that.sessionId)) return false;
        if (!keyspace.equals(that.keyspace)) return false;
        if (!columnFamily.equals(that.columnFamily)) return false;
        if (ranges != null ? that.ranges == null || (ranges.size() != that.ranges.size()) || (ranges.size() == that.ranges.size() && !ranges.containsAll(that.ranges)) : that.ranges != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parentSessionId, sessionId, keyspace, columnFamily, ranges);
    }

    private static class RepairJobDescSerializer implements IVersionedSerializer<RepairJobDesc>
    {
        public void serialize(RepairJobDesc desc, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(desc.parentSessionId != null);
            if (desc.parentSessionId != null)
                desc.parentSessionId.serialize(out);

            desc.sessionId.serialize(out);
            out.writeUTF(desc.keyspace);
            out.writeUTF(desc.columnFamily);
            IPartitioner.validate(desc.ranges);
            out.writeInt(desc.ranges.size());
            for (Range<Token> rt : desc.ranges)
                AbstractBounds.tokenSerializer.serialize(rt, out, version);
        }

        public RepairJobDesc deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID parentSessionId = null;
            if (in.readBoolean())
                parentSessionId = TimeUUID.deserialize(in);
            TimeUUID sessionId = TimeUUID.deserialize(in);
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            int nRanges = in.readInt();
            Collection<Range<Token>> ranges = new ArrayList<>(nRanges);
            Range<Token> range;

            for (int i = 0; i < nRanges; i++)
            {
                range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in,
                        IPartitioner.global(), version);
                ranges.add(range);
            }

            return new RepairJobDesc(parentSessionId, sessionId, keyspace, columnFamily, ranges);
        }

        public long serializedSize(RepairJobDesc desc, int version)
        {
            int size = TypeSizes.sizeof(desc.parentSessionId != null);
            if (desc.parentSessionId != null)
                size += TimeUUID.sizeInBytes();
            size += TimeUUID.sizeInBytes();
            size += TypeSizes.sizeof(desc.keyspace);
            size += TypeSizes.sizeof(desc.columnFamily);
            size += TypeSizes.sizeof(desc.ranges.size());
            for (Range<Token> rt : desc.ranges)
            {
                size += AbstractBounds.tokenSerializer.serializedSize(rt, version);
            }
            return size;
        }
    }
}
