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
package org.apache.cassandra.db.compaction;

import java.util.UUID;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.metrics.CompactionMetrics;

/**
 * Merge multiple iterators over the content of sstable into a "compacted" iterator.
 * <p>
 * On top of the actual merging the source iterators, this class:
 * <ul>
 *   <li>purge gc-able tombstones if possible (see PurgeIterator below).</li>
 *   <li>update 2ndary indexes if necessary (as we don't read-before-write on index updates, index entries are
 *       not deleted on deletion of the base table data, which is ok because we'll fix index inconsistency
 *       on reads. This however mean that potentially obsolete index entries could be kept a long time for
 *       data that is not read often, so compaction "pro-actively" fix such index entries. This is mainly
 *       an optimization).</li>
 *   <li>invalidate cached partitions that are empty post-compaction. This avoids keeping partitions with
 *       only purgable tombstones in the row cache.</li>
 *   <li>keep tracks of the compaction progress.</li>
 * </ul>
 */
public class CompactionIterator extends CompactionInfo.Holder implements UnfilteredPartitionIterator
{
    private static final long UNFILTERED_TO_UPDATE_PROGRESS = 100;

    private final OperationType type;
    private final CompactionController controller;
    private final List<ISSTableScanner> scanners;
    private final int nowInSec;
    private final UUID compactionId;

    private final long totalBytes;
    private long bytesRead;

    /*
     * counters for merged rows.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    private final long[] mergeCounters;

    private final UnfilteredPartitionIterator compacted;
    private final CompactionMetrics metrics;

    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, int nowInSec, UUID compactionId)
    {
        this(type, scanners, controller, nowInSec, compactionId, null);
    }

    @SuppressWarnings("resource") // We make sure to close mergedIterator in close() and CompactionIterator is itself an AutoCloseable
    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, int nowInSec, UUID compactionId, CompactionMetrics metrics)
    {
        this.controller = controller;
        this.type = type;
        this.scanners = scanners;
        this.nowInSec = nowInSec;
        this.compactionId = compactionId;
        this.bytesRead = 0;

        long bytes = 0;
        for (ISSTableScanner scanner : scanners)
            bytes += scanner.getLengthInBytes();
        this.totalBytes = bytes;
        this.mergeCounters = new long[scanners.size()];
        this.metrics = metrics;

        if (metrics != null)
            metrics.beginCompaction(this);

        this.compacted = scanners.isEmpty()
                       ? UnfilteredPartitionIterators.empty(controller.cfs.metadata)
                       : new PurgeIterator(UnfilteredPartitionIterators.merge(scanners, nowInSec, listener()), controller);
    }

    public boolean isForThrift()
    {
        return false;
    }

    public CFMetaData metadata()
    {
        return controller.cfs.metadata;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.cfs.metadata,
                                  type,
                                  bytesRead,
                                  totalBytes,
                                  compactionId);
    }

    private void updateCounterFor(int rows)
    {
        assert rows > 0 && rows - 1 < mergeCounters.length;
        mergeCounters[rows - 1] += 1;
    }

    public long[] getMergedRowCounts()
    {
        return mergeCounters;
    }

    private UnfilteredPartitionIterators.MergeListener listener()
    {
        return new UnfilteredPartitionIterators.MergeListener()
        {
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                int merged = 0;
                for (UnfilteredRowIterator iter : versions)
                {
                    if (iter != null)
                        merged++;
                }

                assert merged > 0;

                CompactionIterator.this.updateCounterFor(merged);

                if (type != OperationType.COMPACTION || !controller.cfs.indexManager.hasIndexes())
                    return null;

                // If we have a 2ndary index, we must update it with deleted/shadowed cells.
                // TODO: this should probably be done asynchronously and batched.
                final SecondaryIndexManager.Updater indexer = controller.cfs.indexManager.gcUpdaterFor(partitionKey, nowInSec);
                final RowDiffListener diffListener = new RowDiffListener()
                {
                    public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                    {
                    }

                    public void onDeletion(int i, Clustering clustering, DeletionTime merged, DeletionTime original)
                    {
                    }

                    public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                    {
                    }

                    public void onCell(int i, Clustering clustering, Cell merged, Cell original)
                    {
                        if (original != null && (merged == null || !merged.isLive(nowInSec)))
                            indexer.remove(clustering, original);
                    }
                };

                return new UnfilteredRowIterators.MergeListener()
                {
                    public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                    {
                    }

                    public void onMergedRows(Row merged, Columns columns, Row[] versions)
                    {
                        Rows.diff(merged, columns, versions, diffListener);
                    }

                    public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker mergedMarker, RangeTombstoneMarker[] versions)
                    {
                    }

                    public void close()
                    {
                    }
                };
            }

            public void close()
            {
            }
        };
    }

    private void updateBytesRead()
    {
        long n = 0;
        for (ISSTableScanner scanner : scanners)
            n += scanner.getCurrentPosition();
        bytesRead = n;
    }

    public boolean hasNext()
    {
        return compacted.hasNext();
    }

    public UnfilteredRowIterator next()
    {
        return compacted.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        try
        {
            compacted.close();
        }
        finally
        {
            if (metrics != null)
                metrics.finishCompaction(this);
        }
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    private class PurgeIterator extends PurgingPartitionIterator
    {
        private final CompactionController controller;

        private DecoratedKey currentKey;
        private long maxPurgeableTimestamp;
        private boolean hasCalculatedMaxPurgeableTimestamp;

        private long compactedUnfiltered;

        private PurgeIterator(UnfilteredPartitionIterator toPurge, CompactionController controller)
        {
            super(toPurge, controller.gcBefore, controller.compactingRepaired() ? Integer.MIN_VALUE : Integer.MAX_VALUE, controller.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones());
            this.controller = controller;
        }

        @Override
        protected void onEmptyPartitionPostPurge(DecoratedKey key)
        {
            if (type == OperationType.COMPACTION)
                controller.cfs.invalidateCachedPartition(key);
        }

        @Override
        protected void onNewPartition(DecoratedKey key)
        {
            currentKey = key;
            hasCalculatedMaxPurgeableTimestamp = false;
        }

        @Override
        protected void updateProgress()
        {
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                updateBytesRead();
        }

        /*
         * Tombstones with a localDeletionTime before this can be purged. This is the minimum timestamp for any sstable
         * containing `currentKey` outside of the set of sstables involved in this compaction. This is computed lazily
         * on demand as we only need this if there is tombstones and this a bit expensive (see #8914).
         */
        protected long getMaxPurgeableTimestamp()
        {
            if (!hasCalculatedMaxPurgeableTimestamp)
            {
                hasCalculatedMaxPurgeableTimestamp = true;
                maxPurgeableTimestamp = controller.maxPurgeableTimestamp(currentKey);
            }
            return maxPurgeableTimestamp;
        }
    }
}
