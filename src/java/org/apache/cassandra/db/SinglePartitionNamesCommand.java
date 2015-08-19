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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Sets;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.memory.HeapAllocator;

/**
 * General interface for storage-engine read queries.
 */
public class SinglePartitionNamesCommand extends SinglePartitionReadCommand<ClusteringIndexNamesFilter>
{
    private int oldestUnrepairedDeletionTime = Integer.MAX_VALUE;
    protected SinglePartitionNamesCommand(boolean isDigest,
                                          int digestVersion,
                                          boolean isForThrift,
                                          CFMetaData metadata,
                                          int nowInSec,
                                          ColumnFilter columnFilter,
                                          RowFilter rowFilter,
                                          DataLimits limits,
                                          DecoratedKey partitionKey,
                                          ClusteringIndexNamesFilter clusteringIndexFilter)
    {
        super(isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    public SinglePartitionNamesCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       ClusteringIndexNamesFilter clusteringIndexFilter)
    {
        this(false, 0, false, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    public SinglePartitionNamesCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       ByteBuffer key,
                                       ClusteringIndexNamesFilter clusteringIndexFilter)
    {
        this(metadata, nowInSec, columnFilter, rowFilter, limits, metadata.decorateKey(key), clusteringIndexFilter);
    }

    public SinglePartitionNamesCommand copy()
    {
        return new SinglePartitionNamesCommand(isDigestQuery(), digestVersion(), isForThrift(), metadata(), nowInSec(), columnFilter(), rowFilter(), limits(), partitionKey(), clusteringIndexFilter());
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedDeletionTime;
    }

    protected UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));

        ImmutableBTreePartition result = null;
        ClusteringIndexNamesFilter filter = clusteringIndexFilter();

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            Partition partition = memtable.getPartition(partitionKey());
            if (partition == null)
                continue;

            try (UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(columnFilter(), partition))
            {
                if (iter.isEmpty())
                    continue;

                UnfilteredRowIterator clonedFilter = copyOnHeap
                                                   ? UnfilteredRowIterators.cloningIterator(iter, HeapAllocator.instance)
                                                   : iter;
                result = add(isForThrift() ? ThriftResultsMerger.maybeWrap(clonedFilter, nowInSec()) : clonedFilter, result, false);
            }
        }

        /* add the SSTables on disk */
        Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
        int sstablesIterated = 0;

        // read sorted sstables
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a partition tombstone with a timestamp greater
            // than the most recent update to this sstable, we're done, since the rest of the sstables
            // will also be older
            if (result != null && sstable.getMaxTimestamp() < result.partitionLevelDeletion().markedForDeleteAt())
                break;

            long currentMaxTs = sstable.getMaxTimestamp();
            filter = reduceFilter(filter, result, currentMaxTs);
            if (filter == null)
                break;

            Tracing.trace("Merging data from sstable {}", sstable.descriptor.generation);
            sstable.incrementReadCount();
            try (UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), columnFilter(), filter.isReversed(), isForThrift())))
            {
                if (iter.isEmpty())
                    continue;

                sstablesIterated++;
                result = add(isForThrift() ? ThriftResultsMerger.maybeWrap(iter, nowInSec()) : iter, result, sstable.isRepaired());
            }
        }

        cfs.metric.updateSSTableIterated(sstablesIterated);

        if (result == null || result.isEmpty())
            return UnfilteredRowIterators.emptyIterator(metadata(), partitionKey(), false);

        DecoratedKey key = result.partitionKey();
        cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);

        // "hoist up" the requested data into a more recent sstable
        if (sstablesIterated > cfs.getMinimumCompactionThreshold()
            && !cfs.isAutoCompactionDisabled()
            && cfs.getCompactionStrategyManager().shouldDefragment())
        {
            // !!WARNING!!   if we stop copying our data to a heap-managed object,
            //               we will need to track the lifetime of this mutation as well
            Tracing.trace("Defragmenting requested data");

            try (UnfilteredRowIterator iter = result.unfilteredIterator(columnFilter(), Slices.ALL, false))
            {
                final Mutation mutation = new Mutation(PartitionUpdate.fromIterator(iter));
                StageManager.getStage(Stage.MUTATION).execute(new Runnable()
                {
                    public void run()
                    {
                        // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                        Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false);
                    }
                });
            }
        }

        return result.unfilteredIterator(columnFilter(), Slices.ALL, clusteringIndexFilter().isReversed());
    }

    private ImmutableBTreePartition add(UnfilteredRowIterator iter, ImmutableBTreePartition result, boolean isRepaired)
    {
        if (!isRepaired)
            oldestUnrepairedDeletionTime = Math.min(oldestUnrepairedDeletionTime, iter.stats().minLocalDeletionTime);

        int maxRows = Math.max(clusteringIndexFilter().requestedRows().size(), 1);
        if (result == null)
            return ImmutableBTreePartition.create(iter, maxRows);

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(columnFilter(), Slices.ALL, clusteringIndexFilter().isReversed())), nowInSec()))
        {
            return ImmutableBTreePartition.create(merged, maxRows);
        }
    }

    private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, Partition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        SearchIterator<Clustering, Row> searchIter = result.searchIterator(columnFilter(), false);

        PartitionColumns columns = columnFilter().fetchedColumns();
        NavigableSet<Clustering> clusterings = filter.requestedRows();

        // We want to remove rows for which we have values for all requested columns. We have to deal with both static and regular rows.
        // TODO: we could also remove a selected column if we've found values for every requested row but we'll leave
        // that for later.

        boolean removeStatic = false;
        if (!columns.statics.isEmpty())
        {
            Row staticRow = searchIter.next(Clustering.STATIC_CLUSTERING);
            removeStatic = staticRow != null && canRemoveRow(staticRow, columns.statics, sstableTimestamp);
        }

        NavigableSet<Clustering> toRemove = null;
        for (Clustering clustering : clusterings)
        {
            if (!searchIter.hasNext())
                break;

            Row row = searchIter.next(clustering);
            if (row == null || !canRemoveRow(row, columns.regulars, sstableTimestamp))
                continue;

            if (toRemove == null)
                toRemove = new TreeSet<>(result.metadata().comparator);
            toRemove.add(clustering);
        }

        if (!removeStatic && toRemove == null)
            return filter;

        // Check if we have everything we need
        boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
        boolean hasNoMoreClusterings = clusterings.isEmpty() || (toRemove != null && toRemove.size() == clusterings.size());
        if (hasNoMoreStatic && hasNoMoreClusterings)
            return null;

        if (toRemove != null)
        {
            BTreeSet.Builder<Clustering> newClusterings = BTreeSet.builder(result.metadata().comparator);
            newClusterings.addAll(Sets.difference(clusterings, toRemove));
            clusterings = newClusterings.build();
        }
        return new ClusteringIndexNamesFilter(clusterings, filter.isReversed());
    }

    private boolean canRemoveRow(Row row, Columns requestedColumns, long sstableTimestamp)
    {
        // We can remove a row if it has data that is more recent that the next sstable to consider for the data that the query
        // cares about. And the data we care about is 1) the row timestamp (since every query cares if the row exists or not)
        // and 2) the requested columns.
        if (row.primaryKeyLivenessInfo().isEmpty() || row.primaryKeyLivenessInfo().timestamp() <= sstableTimestamp)
            return false;

        for (ColumnDefinition column : requestedColumns)
        {
            // We can never be sure we have all of a collection, so never remove rows in that case.
            if (column.type.isCollection())
                return false;

            Cell cell = row.getCell(column);
            if (cell == null || cell.timestamp() <= sstableTimestamp)
                return false;
        }
        return true;
    }
}
