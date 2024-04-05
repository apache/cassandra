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

package org.apache.cassandra.tcm.ownership;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class Truncations implements MetadataValue<Truncations>
{
    public static final Truncations EMPTY = new Truncations(Epoch.EMPTY, ImmutableMap.of());

    public static final Serializer serializer = new Serializer();

    private final Epoch lastModified;
    // TODO this might be just on TableId -> Long if it turns out that TruncationRecord object is not necessary
    private final ImmutableMap<TableId, TruncationRecord> truncations;

    public Truncations(Epoch lastModified, ImmutableMap<TableId, TruncationRecord> truncations)
    {
        this.lastModified = lastModified;
        this.truncations = truncations;
    }

    @Override
    public Truncations withLastModified(Epoch epoch)
    {
        return new Truncations(epoch, truncations);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public ImmutableMap<TableId, TruncationRecord> diff(Truncations next)
    {
        Map<TableId, TruncationRecord> diff = new HashMap<>();
        for (Map.Entry<TableId, TruncationRecord> entry : next.truncations.entrySet())
        {
            TableId maybeNewKey = entry.getKey();
            TruncationRecord maybeNewRecord = entry.getValue();
            TruncationRecord truncationRecord = truncations.get(maybeNewKey);
            if (truncationRecord == null || maybeNewRecord.truncationTimestamp > truncationRecord.truncationTimestamp)
            {
                diff.put(maybeNewKey, maybeNewRecord);
            }
        }
        return ImmutableMap.copyOf(diff);
    }

    public Truncations withTruncation(TableId tableId, TruncationRecord truncationRecord)
    {
        Map<TableId, TruncationRecord> map = new HashMap<>(truncations);
        // overwrite what is there with new record
        // TODO - should we check that what we are going to put has timestamp strictly bigger than what is already there?
        map.put(tableId, truncationRecord);
        return new Truncations(lastModified, ImmutableMap.copyOf(map));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Truncations that = (Truncations) o;
        return Objects.equals(lastModified, that.lastModified) && Objects.equals(truncations, that.truncations);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, truncations);
    }

    @Override
    public String toString()
    {
        return "Truncations{" +
               "lastModified=" + lastModified +
               ", truncations=" + truncations +
               '}';
    }

    public static class TruncationRecord
    {
        public final long truncationTimestamp;

        public TruncationRecord(long truncationTimestamp)
        {
            this.truncationTimestamp = truncationTimestamp;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TruncationRecord that = (TruncationRecord) o;
            return truncationTimestamp == that.truncationTimestamp;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(truncationTimestamp);
        }

        @Override
        public String toString()
        {
            return "TruncationRecord{" +
                   "truncationTimestamp=" + truncationTimestamp +
                   '}';
        }
    }

    public static final class Serializer implements MetadataSerializer<Truncations>
    {

        @Override
        public void serialize(Truncations t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            out.writeInt(t.truncations.size());
            for (Map.Entry<TableId, TruncationRecord> entry : t.truncations.entrySet())
            {
                entry.getKey().serialize(out);
                out.writeLong(entry.getValue().truncationTimestamp);
            }
        }

        @Override
        public Truncations deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            int size = in.readInt();
            if (size == 0) return new Truncations(lastModified, ImmutableMap.of());
            ImmutableMap.Builder<TableId, TruncationRecord> result = ImmutableMap.builder();
            for (int i = 0; i < size; i++)
            {
                TableId tableId = TableId.deserialize(in);
                TruncationRecord truncationRecord = new TruncationRecord(in.readLong());
                result.put(tableId, truncationRecord);
            }
            return new Truncations(lastModified, result.build());
        }

        @Override
        public long serializedSize(Truncations t, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.lastModified, version);
            size += TypeSizes.sizeof(t.truncations.size());
            for (Map.Entry<TableId, TruncationRecord> entry : t.truncations.entrySet())
            {
                size += 16; // TableId.serializedSize(), not sure why it is not static
                size += TypeSizes.sizeof(entry.getValue().truncationTimestamp);
            }
            return size;
        }
    }
}
