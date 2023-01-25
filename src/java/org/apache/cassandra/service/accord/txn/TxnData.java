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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import accord.api.Data;
import accord.api.Result;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;

public class TxnData implements Data, Result, Iterable<FilteredPartition>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnData());

    private final Map<TxnDataName, FilteredPartition> data;

    public TxnData(Map<TxnDataName, FilteredPartition> data)
    {
        this.data = data;
    }

    public TxnData()
    {
        this(new HashMap<>());
    }

    public void put(TxnDataName name, FilteredPartition partition)
    {
        data.put(name, partition);
    }

    public FilteredPartition get(TxnDataName name)
    {
        return data.get(name);
    }

    public Set<Map.Entry<TxnDataName, FilteredPartition>> entrySet()
    {
        return data.entrySet();
    }

    @Override
    public Data merge(Data data)
    {
        TxnData that = (TxnData) data;
        TxnData merged = new TxnData();
        this.data.forEach(merged::put);
        that.data.forEach(merged::put);
        return merged;
    }

    public static Data merge(Data left, Data right)
    {
        if (left == null)
            return right;
        if (right == null)
            return null;

        return left.merge(right);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (Map.Entry<TxnDataName, FilteredPartition> entry : data.entrySet())
        {
            size += entry.getKey().estimatedSizeOnHeap();
            
            for (Row row : entry.getValue())
                size += row.unsharedHeapSize();
            
            // TODO: Include the other parts of FilteredPartition after we rebase to pull in BTreePartitionData?
        }
        return size;
    }

    @Override
    public Iterator<FilteredPartition> iterator()
    {
        return data.values().iterator();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnData that = (TxnData) o;
        return data.equals(that.data);
    }

    private static final IVersionedSerializer<FilteredPartition> partitionSerializer = new IVersionedSerializer<FilteredPartition>()
    {
        @Override
        public void serialize(FilteredPartition partition, DataOutputPlus out, int version) throws IOException
        {
            partition.metadata().id.serialize(out);
            try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
            {
                UnfilteredRowIteratorSerializer.serializer.serialize(iterator, ColumnFilter.all(partition.metadata()), out, version, partition.rowCount());
            }
        }

        @Override
        public FilteredPartition deserialize(DataInputPlus in, int version) throws IOException
        {
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, ColumnFilter.all(metadata), DeserializationHelper.Flag.FROM_REMOTE))
            {
                return new FilteredPartition(UnfilteredRowIterators.filter(partition, 0));
            }
        }

        @Override
        public long serializedSize(FilteredPartition partition, int version)
        {
            TableId tableId = partition.metadata().id;
            long size = tableId.serializedSize();
            try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
            {
                return size + UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, ColumnFilter.all(partition.metadata()), version, partition.rowCount());
            }
        }
    };

    public static final IVersionedSerializer<TxnData> serializer = new IVersionedSerializer<TxnData>()
    {
        @Override
        public void serialize(TxnData data, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(data.data.size());
            for (Map.Entry<TxnDataName, FilteredPartition> entry : data.data.entrySet())
            {
                TxnDataName.serializer.serialize(entry.getKey(), out, version);
                partitionSerializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public TxnData deserialize(DataInputPlus in, int version) throws IOException
        {
            Map<TxnDataName, FilteredPartition> data = new HashMap<>();
            long size = in.readUnsignedVInt();
            for (int i=0; i<size; i++)
            {
                TxnDataName name = TxnDataName.serializer.deserialize(in, version);
                FilteredPartition partition = partitionSerializer.deserialize(in, version);
                data.put(name, partition);
            }
            return new TxnData(data);
        }

        @Override
        public long serializedSize(TxnData data, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(data.data.size());
            for (Map.Entry<TxnDataName, FilteredPartition> entry : data.data.entrySet())
            {
                size += TxnDataName.serializer.serializedSize(entry.getKey(), version);
                size += partitionSerializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };
}
