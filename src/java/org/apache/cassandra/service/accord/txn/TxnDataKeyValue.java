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

import accord.primitives.Ranges;
import accord.utils.Invariants;
import org.apache.cassandra.db.partitions.FilteredPartition;
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
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.Version;

import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.apache.cassandra.db.rows.DeserializationHelper.Flag.FROM_REMOTE;

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
    public TxnDataValue merge(TxnDataValue that)
    {
        Invariants.require(this.equals(that));
        return this;
    }

    @Override
    public TxnDataValue without(Ranges ranges)
    {
        return ranges.contains(new TokenKey(metadata().id, partitionKey().getToken())) ? null : this;
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

    public static final TxnDataValueSerializer<TxnDataKeyValue> serializer = new TxnDataValueSerializer<>()
    {
        @Override
        public void serialize(TxnDataKeyValue value, DataOutputPlus out, Version version) throws IOException
        {
            value.metadata().id.serializeCompact(out);
            try (UnfilteredRowIterator iterator = value.unfilteredIterator())
            {
                UnfilteredRowIteratorSerializer.serializer.serialize(iterator, out, version.messageVersion(), value.rowCount(), STABLE, null);
            }
        }

        @Override
        public TxnDataKeyValue deserialize(DataInputPlus in, Version version) throws IOException
        {
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserializeCompact(in));
            UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(metadata, in, version.messageVersion(), FROM_REMOTE, STABLE, null);
            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version.messageVersion(), metadata, FROM_REMOTE, header))
            {
                return new TxnDataKeyValue(UnfilteredRowIterators.filter(partition, 0));
            }
        }

        @Override
        public long serializedSize(TxnDataKeyValue value, Version version)
        {
            TableId tableId = value.metadata().id;
            long size = tableId.serializedCompactSize();
            try (UnfilteredRowIterator iterator = value.unfilteredIterator())
            {
                return size + UnfilteredRowIteratorSerializer.serializer.serializedSize(iterator, version.messageVersion(), value.rowCount(), STABLE, null);
            }
        }
    };
}
