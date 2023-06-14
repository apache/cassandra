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
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Iterates over primary keys in a memtable
 */
public class MemtableKeyRangeIterator extends KeyRangeIterator
{
    private final Memtable memtable;
    private final PrimaryKey.Factory pkFactory;
    private final AbstractBounds<PartitionPosition> keyRange;
    private final ColumnFilter columns;
    private UnfilteredPartitionIterator partitionIterator;
    private UnfilteredRowIterator rowIterator;


    private MemtableKeyRangeIterator(Memtable memtable,
                                     PrimaryKey.Factory pkFactory,
                                     AbstractBounds<PartitionPosition> keyRange)
    {
        super(pkFactory.createTokenOnly(keyRange.left.getToken()),
              pkFactory.createTokenOnly(maxToken(keyRange, memtable.metadata().partitioner)),
              memtable.operationCount());

        TableMetadata metadata = memtable.metadata();
        this.memtable = memtable;
        this.pkFactory = pkFactory;
        this.keyRange = keyRange;
        this.columns = ColumnFilter.selectionBuilder()
                                           .addAll(metadata.partitionKeyColumns())
                                           .addAll(metadata.clusteringColumns())
                                           .build();

        DataRange dataRange = new DataRange(keyRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
        this.partitionIterator = memtable.partitionIterator(columns, dataRange, null);
        this.rowIterator = null;
    }

    private static Token maxToken(AbstractBounds<PartitionPosition> keyRange, IPartitioner partitioner)
    {
        return keyRange.right.getToken().isMinimum() ? partitioner.getMaximumToken() : keyRange.right.getToken();
    }

    public static MemtableKeyRangeIterator create(Memtable memtable, AbstractBounds<PartitionPosition> keyRange)
    {
        PrimaryKey.Factory pkFactory = new PrimaryKey.Factory(memtable.metadata().comparator);
        return new MemtableKeyRangeIterator(memtable, pkFactory, keyRange);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        AbstractBounds<PartitionPosition> keyRange = AbstractBounds.bounds(nextKey.partitionKey(),
                                                                true,
                                                                           this.keyRange.right,
                                                                           this.keyRange.inclusiveRight());
        DataRange dataRange = new DataRange(keyRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
        this.partitionIterator = memtable.partitionIterator(columns, dataRange, null);
        if (partitionIterator.hasNext())
        {
            this.rowIterator = partitionIterator.next();
            if (!nextKey.hasEmptyClustering() && rowIterator.partitionKey().equals(nextKey.partitionKey()))
            {
                Slice slice = Slice.make(nextKey.clustering(), Clustering.EMPTY);
                Slices slices = Slices.with(this.memtable.metadata().comparator, slice);
                this.rowIterator = memtable.rowIterator(nextKey.partitionKey(), slices, this.columns, false, null);
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        partitionIterator.close();
        if (rowIterator != null)
            rowIterator.close();
    }

    @Override
    protected PrimaryKey computeNext()
    {
        while (hasNextRow(rowIterator) || partitionIterator.hasNext())
        {
            if (!hasNextRow(rowIterator))
            {
                rowIterator = partitionIterator.next();
                continue;
            }

            Unfiltered unfiltered = rowIterator.next();
            if (!unfiltered.isRow())
                continue;

            Row row = (Row) unfiltered;
            return pkFactory.create(rowIterator.partitionKey(), row.clustering());
        }
        return endOfData();
    }

    private static boolean hasNextRow(UnfilteredRowIterator rowIterator)
    {
        return rowIterator != null && rowIterator.hasNext();
    }
}
