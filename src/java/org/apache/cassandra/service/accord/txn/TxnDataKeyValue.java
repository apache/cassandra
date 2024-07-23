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

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public class TxnDataKeyValue extends FilteredPartition implements TxnDataValue
{
    public TxnDataKeyValue(RowIterator rows)
    {
        super(rows);
    }

    @Override
    public TxnDataValue.Kind kind()
    {
        return Kind.key;
    }

    @Override
    public TxnDataValue merge(TxnDataValue other)
    {
        // TODO (review): In Accord, for keys, these should always be identical and really there shouldn't be duplicates
        // so we could return either
        return this;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = 0;
        Row staticRow = staticRow();
        if (staticRow != null)
            size += staticRow.unsharedHeapSize();
        for (Row row : this)
            size += row.unsharedHeapSize();
        // TODO: Include the other parts of FilteredPartition after we rebase to pull in BTreePartitionData?
        return size;
    }

    public static final TxnDataValueSerializer<TxnDataKeyValue> serializer = new TxnDataValueSerializer<TxnDataKeyValue>()
    {
        @Override
        public void serialize(TxnDataKeyValue value, DataOutputPlus out, int version) throws IOException
        {
            value.metadata().id.serialize(out);
            try (UnfilteredRowIterator iterator = value.unfilteredIterator())
            {
                UnfilteredRowIteratorSerializer.serializer.serialize(iterator, ColumnFilter.all(value.metadata()), out, version, value.rowCount());
            }
        }

        @Override
        public TxnDataKeyValue deserialize(DataInputPlus in, int version) throws IOException
        {
            // TODO (required): This needs to use the correct cluster metadata for schema change
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, ColumnFilter.all(metadata), DeserializationHelper.Flag.FROM_REMOTE))
            {
                return new TxnDataKeyValue(UnfilteredRowIterators.filter(partition, 0));
            }
        }

        @Override
        public long serializedSize(TxnDataKeyValue value, int version)
        {
            TableId tableId = value.metadata().id;
            long size = tableId.serializedSize();
            try (UnfilteredRowIterator iterator = value.unfilteredIterator())
            {
                return size + UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, ColumnFilter.all(value.metadata()), version, value.rowCount());
            }
        }
    };
}
