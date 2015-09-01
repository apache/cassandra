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

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.HeapAllocator;

/**
 * General interface for storage-engine read queries.
 */
public class SinglePartitionSliceCommand extends SinglePartitionReadCommand<ClusteringIndexSliceFilter>
{
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    public SinglePartitionSliceCommand(boolean isDigest,
                                       int digestVersion,
                                       boolean isForThrift,
                                       CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       ClusteringIndexSliceFilter clusteringIndexFilter)
    {
        super(isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    public SinglePartitionSliceCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       ClusteringIndexSliceFilter clusteringIndexFilter)
    {
        this(false, 0, false, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    /**
     * Creates a new single partition slice command for the provided single slice.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slice the slice of rows to query.
     *
     * @return a newly created read command that queries {@code slice} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(CFMetaData metadata, int nowInSec, DecoratedKey key, Slice slice)
    {
        return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(CFMetaData metadata, int nowInSec, DecoratedKey key, Slices slices)
    {
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, false);
        return new SinglePartitionSliceCommand(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(CFMetaData metadata, int nowInSec, ByteBuffer key, Slices slices)
    {
        return create(metadata, nowInSec, metadata.decorateKey(key), slices);
    }

    public SinglePartitionSliceCommand copy()
    {
        return new SinglePartitionSliceCommand(isDigestQuery(), digestVersion(), isForThrift(), metadata(), nowInSec(), columnFilter(), rowFilter(), limits(), partitionKey(), clusteringIndexFilter());
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    protected UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));

        List<UnfilteredRowIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());
        ClusteringIndexSliceFilter filter = clusteringIndexFilter();

        try
        {
            for (Memtable memtable : view.memtables)
            {
                Partition partition = memtable.getPartition(partitionKey());
                if (partition == null)
                    continue;

                @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(columnFilter(), partition);
                @SuppressWarnings("resource") // same as above
                UnfilteredRowIterator maybeCopied = copyOnHeap ? UnfilteredRowIterators.cloningIterator(iter, HeapAllocator.instance) : iter;
                oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, partition.stats().minLocalDeletionTime);
                iterators.add(isForThrift() ? ThriftResultsMerger.maybeWrap(maybeCopied, nowInSec()) : maybeCopied);
            }
            /*
             * We can't eliminate full sstables based on the timestamp of what we've already read like
             * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
             * we've read. We still rely on the sstable ordering by maxTimestamp since if
             *   maxTimestamp_s1 > maxTimestamp_s0,
             * we're guaranteed that s1 cannot have a row tombstone such that
             *   timestamp(tombstone) > maxTimestamp_s0
             * since we necessarily have
             *   timestamp(tombstone) <= maxTimestamp_s1
             * In other words, iterating in maxTimestamp order allow to do our mostRecentPartitionTombstone elimination
             * in one pass, and minimize the number of sstables for which we read a partition tombstone.
             */
            int sstablesIterated = 0;
            Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
            List<SSTableReader> skippedSSTables = null;
            long mostRecentPartitionTombstone = Long.MIN_VALUE;
            long minTimestamp = Long.MAX_VALUE;
            int nonIntersectingSSTables = 0;

            for (SSTableReader sstable : view.sstables)
            {
                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());
                // if we've already seen a partition tombstone with a timestamp greater
                // than the most recent update to this sstable, we can skip it
                if (sstable.getMaxTimestamp() < mostRecentPartitionTombstone)
                    break;

                if (!filter.shouldInclude(sstable))
                {
                    nonIntersectingSSTables++;
                    // sstable contains no tombstone if maxLocalDeletionTime == Integer.MAX_VALUE, so we can safely skip those entirely
                    if (sstable.getSSTableMetadata().maxLocalDeletionTime != Integer.MAX_VALUE)
                    {
                        if (skippedSSTables == null)
                            skippedSSTables = new ArrayList<>();
                        skippedSSTables.add(sstable);
                    }
                    continue;
                }

                sstable.incrementReadCount();
                @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), columnFilter(), filter.isReversed(), isForThrift()));
                if (!sstable.isRepaired())
                    oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());

                iterators.add(isForThrift() ? ThriftResultsMerger.maybeWrap(iter, nowInSec()) : iter);
                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone, iter.partitionLevelDeletion().markedForDeleteAt());
                sstablesIterated++;
            }

            int includedDueToTombstones = 0;
            // Check for partition tombstones in the skipped sstables
            if (skippedSSTables != null)
            {
                for (SSTableReader sstable : skippedSSTables)
                {
                    if (sstable.getMaxTimestamp() <= minTimestamp)
                        continue;

                    sstable.incrementReadCount();
                    @SuppressWarnings("resource") // 'iter' is either closed right away, or added to iterators which is close on exception, or through the closing of the final merged iterator
                    UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), columnFilter(), filter.isReversed(), isForThrift()));
                    if (iter.partitionLevelDeletion().markedForDeleteAt() > minTimestamp)
                    {
                        iterators.add(iter);
                        if (!sstable.isRepaired())
                            oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
                        includedDueToTombstones++;
                        sstablesIterated++;
                    }
                    else
                    {
                        iter.close();
                    }
                }
            }
            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                              nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones);

            cfs.metric.updateSSTableIterated(sstablesIterated);

            if (iterators.isEmpty())
                return UnfilteredRowIterators.emptyIterator(cfs.metadata, partitionKey(), filter.isReversed());

            Tracing.trace("Merging data from memtables and {} sstables", sstablesIterated);

            @SuppressWarnings("resource") //  Closed through the closing of the result of that method.
            UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators, nowInSec());
            if (!merged.isEmpty())
            {
                DecoratedKey key = merged.partitionKey();
                cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
            }

            return merged;
        }
        catch (RuntimeException | Error e)
        {
            try
            {
                FBUtilities.closeAll(iterators);
            }
            catch (Exception suppressed)
            {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }
}
