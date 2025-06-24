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

package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;

public class SimplePartition extends AbstractBTreePartition
{
    static
    {
        DatabaseDescriptor.clientInitialization(false); // if the user setup DD respect w/e was done
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }
    public static final int DEFAULT_TIMESTAMP = 42;
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    private final OpOrder writeOrder = new OpOrder();
    private final AtomicBTreePartition delegate;

    public SimplePartition(TableMetadata metadata, DecoratedKey partitionKey)
    {
        super(partitionKey);
        delegate = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), partitionKey, POOL.newAllocator(metadata.toString()));
    }

    @Override
    protected BTreePartitionData holder()
    {
        return delegate.holder();
    }

    @Override
    protected boolean canHaveShadowedData()
    {
        return false;
    }

    @Override
    public TableMetadata metadata()
    {
        return delegate.metadata();
    }

    public SimplePartition clear()
    {
        delegate.unsafeSetHolder(BTreePartitionData.EMPTY);
        return this;
    }

    public SimplePartition add(Row row)
    {
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(metadata(), partitionKey, row);
        try (OpOrder.Group group = writeOrder.start())
        {
            delegate.addAll(update, HeapCloner.instance, group, UpdateTransaction.NO_OP);
        }
        return this;
    }

    public RowBuilder add(Clustering<?> ck)
    {
        return new RowBuilder(ck);
    }

    public SimplePartition addEmpty(Clustering<?> ck)
    {
        return add(ck).build();
    }

    public SimplePartition addEmptyAndLive(Clustering<?> ck)
    {
        return addEmptyAndLive(ck, DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP);
    }

    public SimplePartition addEmptyAndLive(Clustering<?> ck, long timestamp, long nowInSec)
    {
        return add(ck).liveness(timestamp, nowInSec).build();
    }

    public RowIterator filtered()
    {
        return FilteredRows.filter(unfilteredIterator(), DEFAULT_TIMESTAMP);
    }

    public class RowBuilder
    {
        private final Row.Builder builder = BTreeRow.unsortedBuilder();
        private long timestamp = DEFAULT_TIMESTAMP;

        public RowBuilder(Clustering<?> ck)
        {
            builder.newRow(ck);
        }

        public RowBuilder timestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        public RowBuilder liveness(long timestamp, long nowInSec)
        {
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
            return this;
        }

        public RowBuilder add(ColumnMetadata column, ByteBuffer value)
        {
            if (column.type.unwrap().isMultiCell())
                throw new IllegalArgumentException("Unable to add a single value to a multi cell column " + column);
            builder.addCell(BufferCell.live(column, timestamp, value));
            return this;
        }

        public RowBuilder addComplex(ColumnMetadata column, List<ByteBuffer> values)
        {
            if (!column.type.unwrap().isMultiCell())
                throw new IllegalArgumentException("Unable to add multiple values to a regular column " + column);
            builder.addComplexDeletion(column, DeletionTime.build(timestamp - 1, timestamp - 1));
            // map needs to be specially handled as its key/value
            if (column.type.unwrap() instanceof MapType)
            {
                for (int i = 0; i < values.size(); i = i + 2)
                {
                    ByteBuffer key = values.get(i);
                    ByteBuffer value = values.get(i + 1);
                    builder.addCell(BufferCell.live(column, timestamp, value, CellPath.create(key)));
                }
            }
            else
            {
                for (int i = 0; i < values.size(); i++)
                    builder.addCell(complexCell(column, i, values.get(i), timestamp));
            }
            return this;
        }

        private Cell<?> complexCell(ColumnMetadata column, int idx, ByteBuffer value, long timestamp)
        {
            var type = column.type.unwrap();
            if (type.isCollection())
            {
                CollectionType<?> ct = (CollectionType<?>) type;
                switch (ct.kind)
                {
                    case SET:
                    {
                        // this isn't correct... the value isn't actually known... so only support map with key/value matching...
                        return BufferCell.live(column, timestamp, value, CellPath.create(value));
                    }
                    case LIST:
                    {
                        // this isn't actually correct, as the cellpath is based off time, but a counter is used to keep things deterministic
                        CellPath path = CellPath.create(ByteBuffer.wrap(TimeUUID.Generator.atUnixMillisAsBytes(idx)));
                        return BufferCell.live(column, timestamp, value, path);
                    }
                    case MAP:
                        throw new UnsupportedOperationException("Map isn't supported due to API being single element rather than multi-element");
                    default:
                        throw new UnsupportedOperationException(ct.kind.name());
                }
            }
            else if (type.isUDT())
            {
                UserType ut = (UserType) type;
                CellPath path = ut.cellPathForField(ut.fieldName(idx));
                return BufferCell.live(column, timestamp, value, path);
            }

            throw new UnsupportedOperationException(type.toString());
        }

        public SimplePartition build()
        {
            SimplePartition.this.add(builder.build());
            return SimplePartition.this;
        }
    }
}
