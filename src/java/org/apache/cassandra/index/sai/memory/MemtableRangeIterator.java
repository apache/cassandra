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

package org.apache.cassandra.index.sai.memory;

import java.io.IOException;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Iterates over primary keys in a memtable
 */
public class MemtableRangeIterator extends RangeIterator
{
    private final Memtable memtable;
    private final PrimaryKey.Factory pkFactory;
    private final AbstractBounds<PartitionPosition> keyRange;
    private final ColumnFilter columns;
    private UnfilteredPartitionIterator partitionIterator;
    private UnfilteredRowIterator rowIterator;

    public MemtableRangeIterator(Memtable memtable,
                                 PrimaryKey.Factory pkFactory,
                                 AbstractBounds<PartitionPosition> keyRange)
    {
        super(minKey(memtable, pkFactory),
              maxKey(memtable, pkFactory),
              memtable.getOperations());

        TableMetadata metadata = memtable.metadata();
        this.memtable = memtable;
        this.pkFactory = pkFactory;
        this.keyRange = keyRange;
        this.columns = ColumnFilter.selectionBuilder()
                                           .addAll(metadata.partitionKeyColumns())
                                           .addAll(metadata.clusteringColumns())
                                           .addAll(metadata.regularColumns())
                                           .build();

        DataRange dataRange = new DataRange(keyRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
        this.partitionIterator = memtable.makePartitionIterator(columns, dataRange);
        this.rowIterator = null;
    }

    private static PrimaryKey minKey(Memtable memtable, PrimaryKey.Factory factory)
    {
        DecoratedKey pk = memtable.minPartitionKey();
        return pk != null ? factory.createPartitionKeyOnly(pk) : null;
    }

    private static PrimaryKey maxKey(Memtable memtable, PrimaryKey.Factory factory)
    {
        DecoratedKey pk = memtable.maxPartitionKey();
        return pk != null ? factory.createPartitionKeyOnly(pk) : null;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        PartitionPosition start = nextKey.partitionKey() != null
                                  ? nextKey.partitionKey()
                                  : nextKey.token().minKeyBound();
        if (!keyRange.right.isMinimum() && start.compareTo(keyRange.right) > 0)
        {
            partitionIterator = EmptyIterators.unfilteredPartition(memtable.metadata());
            rowIterator = null;
            return;
        }

        AbstractBounds<PartitionPosition> partitionBounds = AbstractBounds.bounds(start, true, keyRange.right, true);
        DataRange dataRange = new DataRange(partitionBounds, new ClusteringIndexSliceFilter(Slices.ALL, false));
        FileUtils.closeQuietly(partitionIterator);
        partitionIterator = memtable.makePartitionIterator(columns, dataRange);
        if (partitionIterator.hasNext())
        {
            this.rowIterator = partitionIterator.next();
            if (!nextKey.hasEmptyClustering() && rowIterator.partitionKey().equals(nextKey.partitionKey()))
            {
                Slice slice = Slice.make(nextKey.clustering(), Clustering.EMPTY);
                Slices slices = Slices.with(memtable.metadata().comparator, slice);
                FileUtils.closeQuietly(rowIterator);
                rowIterator = memtable.getPartition(nextKey.partitionKey()).unfilteredIterator(columns, slices, false);
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(partitionIterator, rowIterator);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        while (hasNextRow(rowIterator) || partitionIterator.hasNext())
        {
            if (!hasNextRow(rowIterator))
            {
                FileUtils.closeQuietly(rowIterator);
                rowIterator = partitionIterator.next();
                continue;
            }

            Unfiltered unfiltered = rowIterator.next();
            if (unfiltered.isRow())
            {
                Row row = (Row) unfiltered;
                return pkFactory.create(rowIterator.partitionKey(), row.clustering());
            }
        }
        return endOfData();
    }

    private static boolean hasNextRow(UnfilteredRowIterator rowIterator)
    {
        return rowIterator != null && rowIterator.hasNext();
    }
}
