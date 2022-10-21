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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;

import accord.api.Data;
import accord.api.Result;
import accord.primitives.Keys;
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
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ObjectSizes;

public class AccordData extends AbstractKeyIndexed<FilteredPartition> implements Data, Result, Iterable<FilteredPartition>
{
    private static final long EMPTY_SIZE = ObjectSizes.measureDeep(new AccordData(Collections.emptyList()));

    private static PartitionKey getKey(FilteredPartition partition)
    {
        return new PartitionKey(partition.metadata().id, partition.partitionKey());
    }

    public AccordData(FilteredPartition partition)
    {
        this(Keys.of(PartitionKey.of(partition)), new ByteBuffer[] { serialize(partition, partitionSerializer) });
    }

    public AccordData(List<FilteredPartition> items)
    {
        super(items, AccordData::getKey);
    }

    public AccordData(Keys keys, ByteBuffer[] serialized)
    {
        super(keys, serialized);
    }

    void serialize(FilteredPartition partition, DataOutputPlus out, int version) throws IOException
    {
        partitionSerializer.serialize(partition, out, version);
    }

    @Override
    FilteredPartition deserialize(DataInputPlus in, int version) throws IOException
    {
        return partitionSerializer.deserialize(in, version);
    }

    @Override
    long serializedSize(FilteredPartition partition, int version)
    {
        return partitionSerializer.serializedSize(partition, version);
    }

    @Override
    long emptySizeOnHeap()
    {
        return EMPTY_SIZE;
    }

    FilteredPartition get(PartitionKey key)
    {
        return getDeserialized(key);
    }

    @Override
    public Iterator<FilteredPartition> iterator()
    {
        return Arrays.stream(serialized).map(this::deserialize).iterator();
    }

    @Override
    public Data merge(Data data)
    {
        return super.merge((AccordData) data, AccordData::new);
    }

    public static Data merge(Data left, Data right)
    {
        if (left == null)
            return right;
        if (right == null)
            return right;

        return left.merge(right);
    }

    private static final IVersionedSerializer<FilteredPartition> partitionSerializer = new IVersionedSerializer<FilteredPartition>()
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

    public static final IVersionedSerializer<AccordData> serializer = new Serializer<>(AccordData::new);
}
