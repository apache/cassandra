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

package org.apache.cassandra.service.accord.db;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import accord.api.Data;
import accord.api.Result;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordKey;

public class AccordData implements Data, Result
{
    private final NavigableMap<AccordKey, FilteredPartition> partitions = new TreeMap<>(AccordKey::compare);

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordData data = (AccordData) o;
        return partitions.equals(data.partitions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitions);
    }

    @Override
    public String toString()
    {
        return "AccordData{" + partitions + '}';
    }

    public AccordData() {}

    public AccordData(FilteredPartition partition)
    {
        put(partition);
    }

    void put(AccordKey key, FilteredPartition partition)
    {
        // TODO: support multiple partitions (ie: read commands) per partition
        Preconditions.checkArgument(!partitions.containsKey(key) || partitions.get(key).equals(partition));
        partitions.put(key, partition);
    }

    void put(FilteredPartition partition)
    {
        put(AccordKey.of(partition), partition);
    }

    FilteredPartition get(AccordKey key)
    {
        return partitions.get(key);
    }

    @Override
    public Data merge(Data data)
    {
        AccordData that = (AccordData) data;
        AccordData merged = new AccordData();
        this.partitions.forEach(merged::put);
        that.partitions.forEach(merged::put);
        return merged;
    }

    public static Data merge(Data left, Data right)
    {
        if (left == null)
            return right;
        if (right == null)
            return right;

        return left.merge(right);
    }

    public void forEach(Consumer<FilteredPartition> consumer)
    {
        partitions.values().forEach(consumer);
    }

    private static final IVersionedSerializer<FilteredPartition> partitionSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(FilteredPartition partition, DataOutputPlus out, int version) throws IOException
        {
            partition.metadata().id.serialize(out);
            TableMetadata metadata = Schema.instance.getTableMetadata(partition.metadata().id);
            try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
            {
                UnfilteredRowIteratorSerializer.serializer.serialize(iterator, ColumnFilter.all(metadata), out, version, partition.rowCount());
            }
        }

        @Override
        public FilteredPartition deserialize(DataInputPlus in, int version) throws IOException
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(TableId.deserialize(in));
            Preconditions.checkState(metadata != null);
            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, ColumnFilter.all(metadata), DeserializationHelper.Flag.FROM_REMOTE))
            {
                return new FilteredPartition(UnfilteredRowIterators.filter(partition, 0));
            }
        }

        @Override
        public long serializedSize(FilteredPartition partition, int version)
        {
            long size = TableId.serializedSize();
            TableMetadata metadata = Schema.instance.getTableMetadata(partition.metadata().id);
            Preconditions.checkState(metadata != null);
            try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
            {
                return size + UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, ColumnFilter.all(metadata), version, partition.rowCount());
            }
        }
    };

    public static final IVersionedSerializer<AccordData> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AccordData data, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(data.partitions.size());
            for (FilteredPartition partition : data.partitions.values())
                partitionSerializer.serialize(partition, out, version);
        }

        @Override
        public AccordData deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            AccordData data = new AccordData();
            for (int i=0; i<size; i++)
            {
                FilteredPartition partition = partitionSerializer.deserialize(in, version);
                data.put(AccordKey.of(partition), partition);
            }
            return data;
        }

        @Override
        public long serializedSize(AccordData data, int version)
        {
            long size = TypeSizes.sizeof(data.partitions.size());
            for (FilteredPartition partition : data.partitions.values())
                size += partitionSerializer.serializedSize(partition, version);
            return size;
        }
    };
}
