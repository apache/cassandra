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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.RandomAccess;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.utils.ObjectSizes.sizeOfReferenceArray;

public class TxnDataRangeValue extends ArrayList<FilteredPartition> implements TxnDataValue, RandomAccess
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnDataRangeValue(0)) - sizeOfReferenceArray(0);

    public TxnDataRangeValue() {}

    public TxnDataRangeValue(int size)
    {
        super(size);
    }

    @Override
    public Kind kind()
    {
        return Kind.range;
    }

    @Override
    public TxnDataRangeValue merge(TxnDataValue other)
    {
        if (isEmpty())
            return (TxnDataRangeValue)other;

        TxnDataRangeValue otherRange = (TxnDataRangeValue)other;
        if (otherRange.isEmpty())
            return this;

        TableId tableId = tableId();
        for (FilteredPartition partition : otherRange)
            checkState(partition.metadata().id.equals(tableId), "All values should be for the same table");

        addAll(((TxnDataRangeValue)other));
        return this;
    }

    Supplier<PartitionIterator> toPartitionIterator(boolean reversed)
    {
        // Sorting isn't preserved when merging TxnDataRangeValues together so sort here
        sort();
        return () -> PartitionIterators.concat(Lists.transform(this, v -> PartitionIterators.singletonIterator(v.rowIterator(reversed))));
    }

    private void sort()
    {
        sort(Comparator.comparing(FilteredPartition::partitionKey));
    }

    private @Nullable TableId tableId()
    {
        if (isEmpty())
            return null;
        return get(0).metadata().id;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE + sizeOfReferenceArray(size());
        for (FilteredPartition partition : this)
        {
            Row staticRow = partition.staticRow();
            if (staticRow != null)
                size += staticRow.unsharedHeapSize();
            for (Row row : partition)
                size += row.unsharedHeapSize();
        }

        // TODO: Include the other parts of FilteredPartition after we rebase to pull in BTreePartitionData?
        return size;
    }

    public static final TxnDataValueSerializer<TxnDataRangeValue> serializer = new TxnDataValueSerializer<TxnDataRangeValue>()
    {
        @Override
        public void serialize(TxnDataRangeValue value, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(value.size());

            if (value.isEmpty())
                return;

            TableId.serializer.serialize(value.tableId(), out, version);
            for (FilteredPartition partition : value)
            {
                try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                {
                    UnfilteredRowIteratorSerializer.serializer.serialize(iterator, ColumnFilter.all(partition.metadata()), out, version, partition.rowCount());
                }
            }
        }

        @Override
        public TxnDataRangeValue deserialize(DataInputPlus in, int version) throws IOException
        {
            int numPartitions = in.readUnsignedVInt32();
            TxnDataRangeValue value = new TxnDataRangeValue(numPartitions);
            if (numPartitions == 0)
                return value;
            // TODO (required): This needs to be updated for schema change to use the correct cluster metadata
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            for (int i = 0; i < numPartitions; i++)
            {
                try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, ColumnFilter.all(metadata), DeserializationHelper.Flag.FROM_REMOTE))
                {
                    value.add(new FilteredPartition(UnfilteredRowIterators.filter(partition, 0)));
                }
            }
            return value;
        }

        @Override
        public long serializedSize(TxnDataRangeValue value, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(value.size());
            if (value.size() == 0)
                return size;
            size += TableId.serializer.serializedSize(value.tableId(), version);
            for (FilteredPartition partition : value)
            {
                try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
                {
                    size += UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, ColumnFilter.all(partition.metadata()), version, partition.rowCount());
                }
            }
            return size;
        }
    };
}
