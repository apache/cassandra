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

import java.util.*;
import java.util.function.LongPredicate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

import org.apache.cassandra.db.transform.DuplicateRowChecker;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.transactions.CompactionTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.utils.Throwables;

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
public class CompactionIterator implements UnfilteredPartitionIterator
{
    private static final long UNFILTERED_TO_UPDATE_PROGRESS = 100;

    private final OperationType type;
    private final AbstractCompactionController controller;
    private final List<ISSTableScanner> scanners;
    private final ImmutableSet<SSTableReader> sstables;
    private final int nowInSec;
    private final UUID compactionId;

    private final long totalBytes;
    private volatile long[] bytesReadByLevel;

    /**
     * Merged frequency counters for partitions and rows (AKA histograms).
     * The array index represents the number of sstables containing the row or partition minus one. So index 0 contains
     * the number of rows or partitions coming from a single sstable (therefore copied rather than merged), index 1 contains
     * the number of rows or partitions coming from two sstables and so forth.
     */
    private final long[] mergedPartitionsHistogram;
    private final long[] mergedRowsHistogram;

    private final UnfilteredPartitionIterator compacted;
    private final TableOperation op;

    @SuppressWarnings("resource") // We make sure to close mergedIterator in close() and CompactionIterator is itself an AutoCloseable
    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, AbstractCompactionController controller, int nowInSec, UUID compactionId)
    {
        this.controller = controller;
        this.type = type;
        this.scanners = scanners;
        this.nowInSec = nowInSec;
        this.compactionId = compactionId;
        this.bytesReadByLevel = new long[LeveledGenerations.MAX_LEVEL_COUNT];

        long bytes = 0;
        for (ISSTableScanner scanner : scanners)
            bytes += scanner.getLengthInBytes();
        this.totalBytes = bytes;
        this.mergedPartitionsHistogram = new long[scanners.size()];
        this.mergedRowsHistogram = new long[scanners.size()];
        // note that we leak `this` from the constructor when calling beginCompaction below, this means we have to get the sstables before
        // calling that to avoid a NPE.
        sstables = scanners.stream().map(ISSTableScanner::getBackingSSTables).flatMap(Collection::stream).collect(ImmutableSet.toImmutableSet());
        op = createOperation();

        UnfilteredPartitionIterator merged = scanners.isEmpty()
                                           ? EmptyIterators.unfilteredPartition(controller.cfs.metadata())
                                           : UnfilteredPartitionIterators.merge(scanners, listener());
        merged = Transformation.apply(merged, new GarbageSkipper(controller));
        merged = Transformation.apply(merged, new Purger(controller, nowInSec));
        merged = DuplicateRowChecker.duringCompaction(merged, type);
        compacted = Transformation.apply(merged, new AbortableUnfilteredPartitionTransformation(op));
    }

    protected TableOperation createOperation()
    {
        return new AbstractTableOperation() {

            @Override
            public OperationProgress getProgress()
            {
                return new AbstractTableOperation.OperationProgress(controller.cfs.metadata(), type, bytesRead(), totalBytes, compactionId, sstables);
            }

            @Override
            public boolean isGlobal()
            {
                return false;
            }
        };
    }

    /**
     * @return A {@link TableOperation} backed by this iterator. This operation can be observed for progress
     * and for interrupting provided that it is registered with a {@link TableOperationObserver}, normally the
     * metrics in the compaction manager. The caller is responsible for registering the operation and checking
     * {@link TableOperation#isStopRequested()}.
     */
    public TableOperation getOperation()
    {
        return op;
    }

    public TableMetadata metadata()
    {
        return controller.cfs.metadata();
    }

    long bytesRead()
    {
        long[] bytesReadByLevel = this.bytesReadByLevel;
        return Arrays.stream(bytesReadByLevel).reduce(Long::sum).orElse(0L);
    }

    long bytesRead(int level)
    {
        return level >= 0 && level < bytesReadByLevel.length ? bytesReadByLevel[level] : 0;
    }

    long totalBytes()
    {
        return totalBytes;
    }

    long totalSourcePartitions()
    {
        return Arrays.stream(mergedPartitionsHistogram).reduce(0L, Long::sum);
    }

    long totalSourceRows()
    {
        return Arrays.stream(mergedRowsHistogram).reduce(0L, Long::sum);
    }

    long[] mergedPartitionsHistogram()
    {
        return mergedPartitionsHistogram;
    }

    long[] mergedRowsHistogram()
    {
        return mergedRowsHistogram;
    }

    public boolean isGlobal()
    {
        return false;
    }

    private UnfilteredPartitionIterators.MergeListener listener()
    {
        return new UnfilteredPartitionIterators.MergeListener()
        {
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                int numVersions = 0;
                for (int i=0, isize=versions.size(); i<isize; i++)
                {
                    @SuppressWarnings("resource")
                    UnfilteredRowIterator iter = versions.get(i);
                    if (iter != null)
                        numVersions++;
                }

                mergedPartitionsHistogram[numVersions - 1] += 1;

                final CompactionTransaction indexTransaction = getIndexTransaction(partitionKey,versions);

                return new UnfilteredRowIterators.MergeListener()
                {
                    public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                    {
                    }

                    public Row onMergedRows(Row merged, Row[] versions)
                    {
                        int numVersions = 0;
                        for (Row v : versions)
                        {
                            if (v != null)
                                numVersions++;
                        }

                        assert numVersions > 0 && numVersions - 1 < mergedRowsHistogram.length;
                        mergedRowsHistogram[numVersions - 1] += 1;

                        if (indexTransaction != null)
                        {
                            indexTransaction.start();
                            indexTransaction.onRowMerge(merged, versions);
                            indexTransaction.commit();
                        }

                        return merged;
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

    private CompactionTransaction getIndexTransaction(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
    {
        if (type != OperationType.COMPACTION || !controller.cfs.indexManager.handles(IndexTransaction.Type.COMPACTION))
            return null;

        Columns statics = Columns.NONE;
        Columns regulars = Columns.NONE;
        for (int i=0, isize=versions.size(); i<isize; i++)
        {
            @SuppressWarnings("resource")
            UnfilteredRowIterator iter = versions.get(i);
            if (iter != null)
            {
                statics = statics.mergeTo(iter.columns().statics);
                regulars = regulars.mergeTo(iter.columns().regulars);
            }
        }
        final RegularAndStaticColumns regularAndStaticColumns = new RegularAndStaticColumns(statics, regulars);
        // If we have a 2ndary index, we must update it with deleted/shadowed cells.
        // we can reuse a single CleanupTransaction for the duration of a partition.
        // Currently, it doesn't do any batching of row updates, so every merge event
        // for a single partition results in a fresh cycle of:
        // * Get new Indexer instances
        // * Indexer::start
        // * Indexer::onRowMerge (for every row being merged by the compaction)
        // * Indexer::commit
        // A new OpOrder.Group is opened in an ARM block wrapping the commits
        // TODO: this should probably be done asynchronously and batched.
        return controller.cfs.indexManager.newCompactionTransaction(partitionKey, regularAndStaticColumns, versions.size(), nowInSec);
    }

    private void updateBytesRead()
    {
        long[] bytesReadByLevel = new long[this.bytesReadByLevel.length];
        for (ISSTableScanner scanner : scanners)
        {
            int level = scanner.level();
            long n = scanner.getCurrentPosition();

            if (level >= 0 && level < bytesReadByLevel.length)
                bytesReadByLevel[level] += n;
        }
        this.bytesReadByLevel = bytesReadByLevel;
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
        updateBytesRead();

        Throwables.maybeFail(Throwables.close(null, compacted));
    }

    public String toString()
    {
        return String.format("%s: %s, (%d/%d)", type, metadata(), bytesRead(), totalBytes());
    }

    private class Purger extends PurgeFunction
    {
        private final AbstractCompactionController controller;

        private DecoratedKey currentKey;
        private LongPredicate purgeEvaluator;

        private long compactedUnfiltered;

        private Purger(AbstractCompactionController controller, int nowInSec)
        {
            super(nowInSec, controller.gcBefore, controller.compactingRepaired() ? Integer.MAX_VALUE : Integer.MIN_VALUE,
                  controller.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                  controller.cfs.metadata.get().enforceStrictLiveness());
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
            purgeEvaluator = null;
        }

        @Override
        protected void updateProgress()
        {
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                updateBytesRead();
        }

        /*
         * Evaluates whether a tombstone with the given deletion timestamp can be purged. This is the minimum
         * timestamp for any sstable containing `currentKey` outside of the set of sstables involved in this compaction.
         * This is computed lazily on demand as we only need this if there is tombstones and this a bit expensive
         * (see #8914).
         */
        protected LongPredicate getPurgeEvaluator()
        {
            if (purgeEvaluator == null)
            {
                purgeEvaluator = controller.getPurgeEvaluator(currentKey);
            }
            return purgeEvaluator;
        }
    }

    /**
     * Unfiltered row iterator that removes deleted data as provided by a "tombstone source" for the partition.
     * The result produced by this iterator is such that when merged with tombSource it produces the same output
     * as the merge of dataSource and tombSource.
     */
    private static class GarbageSkippingUnfilteredRowIterator extends WrappingUnfilteredRowIterator
    {
        final UnfilteredRowIterator tombSource;
        final DeletionTime partitionLevelDeletion;
        final Row staticRow;
        final ColumnFilter cf;
        final TableMetadata metadata;
        final boolean cellLevelGC;

        DeletionTime tombOpenDeletionTime = DeletionTime.LIVE;
        DeletionTime dataOpenDeletionTime = DeletionTime.LIVE;
        DeletionTime openDeletionTime = DeletionTime.LIVE;
        DeletionTime partitionDeletionTime;
        DeletionTime activeDeletionTime;
        Unfiltered tombNext = null;
        Unfiltered dataNext = null;
        Unfiltered next = null;

        /**
         * Construct an iterator that filters out data shadowed by the provided "tombstone source".
         *
         * @param dataSource The input row. The result is a filtered version of this.
         * @param tombSource Tombstone source, i.e. iterator used to identify deleted data in the input row.
         * @param cellLevelGC If false, the iterator will only look at row-level deletion times and tombstones.
         *                    If true, deleted or overwritten cells within a surviving row will also be removed.
         */
        protected GarbageSkippingUnfilteredRowIterator(UnfilteredRowIterator dataSource, UnfilteredRowIterator tombSource, boolean cellLevelGC)
        {
            super(dataSource);
            this.tombSource = tombSource;
            this.cellLevelGC = cellLevelGC;
            metadata = dataSource.metadata();
            cf = ColumnFilter.all(metadata);

            activeDeletionTime = partitionDeletionTime = tombSource.partitionLevelDeletion();

            // Only preserve partition level deletion if not shadowed. (Note: Shadowing deletion must not be copied.)
            this.partitionLevelDeletion = dataSource.partitionLevelDeletion().supersedes(tombSource.partitionLevelDeletion()) ?
                    dataSource.partitionLevelDeletion() :
                    DeletionTime.LIVE;

            Row dataStaticRow = garbageFilterRow(dataSource.staticRow(), tombSource.staticRow());
            this.staticRow = dataStaticRow != null ? dataStaticRow : Rows.EMPTY_STATIC_ROW;

            tombNext = advance(tombSource);
            dataNext = advance(dataSource);
        }

        private static Unfiltered advance(UnfilteredRowIterator source)
        {
            return source.hasNext() ? source.next() : null;
        }

        @Override
        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public void close()
        {
            super.close();
            tombSource.close();
        }

        @Override
        public Row staticRow()
        {
            return staticRow;
        }

        @Override
        public boolean hasNext()
        {
            // Produce the next element. This may consume multiple elements from both inputs until we find something
            // from dataSource that is still live. We track the currently open deletion in both sources, as well as the
            // one we have last issued to the output. The tombOpenDeletionTime is used to filter out content; the others
            // to decide whether or not a tombstone is superseded, and to be able to surface (the rest of) a deletion
            // range from the input when a suppressing deletion ends.
            while (next == null && dataNext != null)
            {
                int cmp = tombNext == null ? -1 : metadata.comparator.compare(dataNext, tombNext);
                if (cmp < 0)
                {
                    if (dataNext.isRow())
                        next = ((Row) dataNext).filter(cf, activeDeletionTime, false, metadata);
                    else
                        next = processDataMarker();
                }
                else if (cmp == 0)
                {
                    if (dataNext.isRow())
                    {
                        next = garbageFilterRow((Row) dataNext, (Row) tombNext);
                    }
                    else
                    {
                        tombOpenDeletionTime = updateOpenDeletionTime(tombOpenDeletionTime, tombNext);
                        activeDeletionTime = Ordering.natural().max(partitionDeletionTime,
                                                                    tombOpenDeletionTime);
                        next = processDataMarker();
                    }
                }
                else // (cmp > 0)
                {
                    if (tombNext.isRangeTombstoneMarker())
                    {
                        tombOpenDeletionTime = updateOpenDeletionTime(tombOpenDeletionTime, tombNext);
                        activeDeletionTime = Ordering.natural().max(partitionDeletionTime,
                                                                    tombOpenDeletionTime);
                        boolean supersededBefore = openDeletionTime.isLive();
                        boolean supersededAfter = !dataOpenDeletionTime.supersedes(activeDeletionTime);
                        // If a range open was not issued because it was superseded and the deletion isn't superseded any more, we need to open it now.
                        if (supersededBefore && !supersededAfter)
                            next = new RangeTombstoneBoundMarker(((RangeTombstoneMarker) tombNext).closeBound(false).invert(), dataOpenDeletionTime);
                        // If the deletion begins to be superseded, we don't close the range yet. This can save us a close/open pair if it ends after the superseding range.
                    }
                }

                if (next instanceof RangeTombstoneMarker)
                    openDeletionTime = updateOpenDeletionTime(openDeletionTime, next);

                if (cmp <= 0)
                    dataNext = advance(wrapped);
                if (cmp >= 0)
                    tombNext = advance(tombSource);
            }
            return next != null;
        }

        protected Row garbageFilterRow(Row dataRow, Row tombRow)
        {
            if (cellLevelGC)
            {
                return Rows.removeShadowedCells(dataRow, tombRow, activeDeletionTime);
            }
            else
            {
                DeletionTime deletion = Ordering.natural().max(tombRow.deletion().time(),
                                                               activeDeletionTime);
                return dataRow.filter(cf, deletion, false, metadata);
            }
        }

        /**
         * Decide how to act on a tombstone marker from the input iterator. We can decide what to issue depending on
         * whether or not the ranges before and after the marker are superseded/live -- if none are, we can reuse the
         * marker; if both are, the marker can be ignored; otherwise we issue a corresponding start/end marker.
         */
        private RangeTombstoneMarker processDataMarker()
        {
            dataOpenDeletionTime = updateOpenDeletionTime(dataOpenDeletionTime, dataNext);
            boolean supersededBefore = openDeletionTime.isLive();
            boolean supersededAfter = !dataOpenDeletionTime.supersedes(activeDeletionTime);
            RangeTombstoneMarker marker = (RangeTombstoneMarker) dataNext;
            if (!supersededBefore)
                if (!supersededAfter)
                    return marker;
                else
                    return new RangeTombstoneBoundMarker(marker.closeBound(false), marker.closeDeletionTime(false));
            else
                if (!supersededAfter)
                    return new RangeTombstoneBoundMarker(marker.openBound(false), marker.openDeletionTime(false));
                else
                    return null;
        }

        @Override
        public Unfiltered next()
        {
            if (!hasNext())
                throw new IllegalStateException();

            Unfiltered v = next;
            next = null;
            return v;
        }

        private DeletionTime updateOpenDeletionTime(DeletionTime openDeletionTime, Unfiltered next)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) next;
            assert openDeletionTime.isLive() == !marker.isClose(false);
            assert openDeletionTime.isLive() || openDeletionTime.equals(marker.closeDeletionTime(false));
            return marker.isOpen(false) ? marker.openDeletionTime(false) : DeletionTime.LIVE;
        }
    }

    /**
     * Partition transformation applying GarbageSkippingUnfilteredRowIterator, obtaining tombstone sources for each
     * partition using the controller's shadowSources method.
     */
    private static class GarbageSkipper extends Transformation<UnfilteredRowIterator>
    {
        final AbstractCompactionController controller;
        final boolean cellLevelGC;

        private GarbageSkipper(AbstractCompactionController controller)
        {
            this.controller = controller;
            cellLevelGC = controller.tombstoneOption == TombstoneOption.CELL;
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            Iterable<UnfilteredRowIterator> sources = controller.shadowSources(partition.partitionKey(), !cellLevelGC);
            if (sources == null)
                return partition;
            List<UnfilteredRowIterator> iters = new ArrayList<>();
            for (UnfilteredRowIterator iter : sources)
            {
                if (!iter.isEmpty())
                    iters.add(iter);
                else
                    iter.close();
            }
            if (iters.isEmpty())
                return partition;

            return new GarbageSkippingUnfilteredRowIterator(partition, UnfilteredRowIterators.merge(iters), cellLevelGC);
        }
    }

    private static class AbortableUnfilteredPartitionTransformation extends Transformation<UnfilteredRowIterator>
    {
        private final AbortableUnfilteredRowTransformation abortableIter;
        private final TableOperation op;

        private AbortableUnfilteredPartitionTransformation(TableOperation op)
        {
            this.op = op;
            this.abortableIter = new AbortableUnfilteredRowTransformation(op);
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            if (op.isStopRequested())
                throw new CompactionInterruptedException(op.getProgress());
            return Transformation.apply(partition, abortableIter);
        }
    }

    private static class AbortableUnfilteredRowTransformation extends Transformation
    {
        private final TableOperation op;

        private AbortableUnfilteredRowTransformation(TableOperation op)
        {
            this.op = op;
        }

        public Row applyToRow(Row row)
        {
            if (op.isStopRequested())
                throw new CompactionInterruptedException(op.getProgress());
            return row;
        }
    }
}
