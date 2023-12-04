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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import accord.local.Commands;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.DuplicateRowChecker;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.CompactionTransaction;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandRows;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandsColumns;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandsForKeyAccessor;
import org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyRows;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.service.paxos.uncommitted.PaxosRows;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

import static accord.local.Commands.Cleanup.TRUNCATE_WITH_OUTCOME;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.config.Config.PaxosStatePurging.legacy;
import static org.apache.cassandra.config.DatabaseDescriptor.paxosStatePurging;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandRows.maybeDropTruncatedCommandColumns;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandRows.truncatedApply;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyColumns.last_executed_micros;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyColumns.last_executed_timestamp;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyColumns.last_write_timestamp;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyColumns.max_timestamp;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyRows.truncateTimestampsForKeyRow;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeDurabilityOrNull;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeRouteOrNull;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeSaveStatusOrNull;
import static org.apache.cassandra.service.accord.AccordKeyspace.deserializeTimestampOrNull;

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
    private final AbstractCompactionController controller;
    private final List<ISSTableScanner> scanners;
    private final ImmutableSet<SSTableReader> sstables;
    private final long nowInSec;
    private final TimeUUID compactionId;
    private final long totalBytes;
    private long bytesRead;
    private long totalSourceCQLRows;

    // Keep targetDirectory for compactions, needed for `nodetool compactionstats`
    private volatile String targetDirectory;

    /*
     * counters for merged rows.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    private final long[] mergeCounters;

    private final UnfilteredPartitionIterator compacted;
    private final ActiveCompactionsTracker activeCompactions;

    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, AbstractCompactionController controller, long nowInSec, TimeUUID compactionId)
    {
        this(type, scanners, controller, nowInSec, compactionId, ActiveCompactionsTracker.NOOP, null, AccordService::instance);
    }

    public CompactionIterator(OperationType type,
                              List<ISSTableScanner> scanners,
                              AbstractCompactionController controller,
                              long nowInSec,
                              TimeUUID compactionId,
                              ActiveCompactionsTracker activeCompactions,
                              TopPartitionTracker.Collector topPartitionCollector)
    {
        this(type, scanners, controller, nowInSec, compactionId, activeCompactions, topPartitionCollector,
             AccordService::instance);
    }

    @SuppressWarnings("resource") // We make sure to close mergedIterator in close() and CompactionIterator is itself an AutoCloseable
    public CompactionIterator(OperationType type,
                              List<ISSTableScanner> scanners,
                              AbstractCompactionController controller,
                              long nowInSec,
                              TimeUUID compactionId,
                              ActiveCompactionsTracker activeCompactions,
                              TopPartitionTracker.Collector topPartitionCollector,
                              @Nonnull Supplier<IAccordService> accordService)
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
        // note that we leak `this` from the constructor when calling beginCompaction below, this means we have to get the sstables before
        // calling that to avoid a NPE.
        sstables = scanners.stream().map(ISSTableScanner::getBackingSSTables).flatMap(Collection::stream).collect(ImmutableSet.toImmutableSet());
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        this.activeCompactions.beginCompaction(this); // note that CompactionTask also calls this, but CT only creates CompactionIterator with a NOOP ActiveCompactions

        UnfilteredPartitionIterator merged = scanners.isEmpty()
                                           ? EmptyIterators.unfilteredPartition(controller.cfs.metadata())
                                           : UnfilteredPartitionIterators.merge(scanners, listener());
        if (topPartitionCollector != null) // need to count tombstones before they are purged
            merged = Transformation.apply(merged, new TopPartitionTracker.TombstoneCounter(topPartitionCollector, nowInSec));
        merged = Transformation.apply(merged, new GarbageSkipper(controller));
        Transformation<UnfilteredRowIterator> purger = isPaxos(controller.cfs) && paxosStatePurging() != legacy
                                                       ? new PaxosPurger()
                                                       : isAccordCommands(controller.cfs)
                                                         ? new AccordCommandsPurger(accordService)
                                                         : isAccordDepsCommandsForKey(controller.cfs)
                                                           ? new AccordCommandsForKeyPurger(AccordKeyspace.DepsCommandsForKeysAccessor, accordService)
                                                           : isAccordAllCommandsForKey(controller.cfs)
                                                             ? new AccordCommandsForKeyPurger(AccordKeyspace.AllCommandsForKeysAccessor, accordService)
                                                             : new Purger(controller, nowInSec);
        merged = Transformation.apply(merged, purger);
        merged = DuplicateRowChecker.duringCompaction(merged, type);
        compacted = Transformation.apply(merged, new AbortableUnfilteredPartitionTransformation(this));
    }

    public TableMetadata metadata()
    {
        return controller.cfs.metadata();
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.cfs.metadata(),
                                  type,
                                  bytesRead,
                                  totalBytes,
                                  compactionId,
                                  sstables,
                                  targetDirectory);
    }

    public boolean isGlobal()
    {
        return false;
    }

    public void setTargetDirectory(final String targetDirectory)
    {
        this.targetDirectory = targetDirectory;
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

    public long getTotalSourceCQLRows()
    {
        return totalSourceCQLRows;
    }

    private UnfilteredPartitionIterators.MergeListener listener()
    {
        return new UnfilteredPartitionIterators.MergeListener()
        {
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                int merged = 0;
                for (int i=0, isize=versions.size(); i<isize; i++)
                {
                    @SuppressWarnings("resource")
                    UnfilteredRowIterator iter = versions.get(i);
                    if (iter != null)
                        merged++;
                }

                assert merged > 0;

                CompactionIterator.this.updateCounterFor(merged);

                if ( (type != OperationType.COMPACTION && type != OperationType.MAJOR_COMPACTION) 
                    || !controller.cfs.indexManager.handles(IndexTransaction.Type.COMPACTION) ) 
                {
                    return null;
                }
                
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
                final CompactionTransaction indexTransaction =
                    controller.cfs.indexManager.newCompactionTransaction(partitionKey,
                                                                         regularAndStaticColumns,
                                                                         versions.size(),
                                                                         nowInSec);

                return new UnfilteredRowIterators.MergeListener()
                {
                    public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                    {
                    }

                    public Row onMergedRows(Row merged, Row[] versions)
                    {
                        indexTransaction.start();
                        indexTransaction.onRowMerge(merged, versions);
                        indexTransaction.commit();
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

    private void updateBytesRead()
    {
        long n = 0;
        for (ISSTableScanner scanner : scanners)
            n += scanner.getCurrentPosition();
        bytesRead = n;
    }

    public long getBytesRead()
    {
        return bytesRead;
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
            activeCompactions.finishCompaction(this);
        }
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    private class Purger extends PurgeFunction
    {
        private final AbstractCompactionController controller;

        private DecoratedKey currentKey;
        private LongPredicate purgeEvaluator;

        private long compactedUnfiltered;

        private Purger(AbstractCompactionController controller, long nowInSec)
        {
            super(nowInSec, controller.gcBefore, controller.compactingRepaired() ? Long.MAX_VALUE : Integer.MIN_VALUE,
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
            totalSourceCQLRows++;
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                updateBytesRead();
        }

        /*
         * Called at the beginning of each new partition
         * Return true if the current partitionKey ignores the gc_grace_seconds during compaction.
         * Note that this method should be called after the onNewPartition because it depends on the currentKey
         * which is set in the onNewPartition
         */
        @Override
        protected boolean shouldIgnoreGcGrace()
        {
            return controller.cfs.shouldIgnoreGcGraceForKey(currentKey);
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
    private static class GarbageSkippingUnfilteredRowIterator implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;

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
            this.wrapped = dataSource;
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

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
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
            wrapped.close();
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

    private abstract class AbstractPurger extends Transformation<UnfilteredRowIterator>
    {
        int compactedUnfiltered;

        protected void onEmptyPartitionPostPurge(DecoratedKey key)
        {
            if (type == OperationType.COMPACTION)
                controller.cfs.invalidateCachedPartition(key);
        }

        protected void updateProgress()
        {
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                updateBytesRead();
        }

        @Override
        @SuppressWarnings("resource")
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            beginPartition(partition);
            UnfilteredRowIterator purged = Transformation.apply(partition, this);
            if (purged.isEmpty())
            {
                onEmptyPartitionPostPurge(purged.partitionKey());
                purged.close();
                return null;
            }

            return purged;
        }

        protected abstract void beginPartition(UnfilteredRowIterator partition);
    }

    private class PaxosPurger extends AbstractPurger
    {
        private final long paxosPurgeGraceMicros = DatabaseDescriptor.getPaxosPurgeGrace(MICROSECONDS);
        private final Map<TableId, PaxosRepairHistory.Searcher> tableIdToHistory = new HashMap<>();

        private Token token;

        @Override
        protected void beginPartition(UnfilteredRowIterator partition)
        {
            this.token = partition.partitionKey().getToken();
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            TableId tableId = PaxosRows.getTableId(row);

            switch (paxosStatePurging())
            {
                default: throw new AssertionError();
                case legacy:
                case gc_grace:
                {
                    TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
                    return row.purgeDataOlderThan(TimeUnit.SECONDS.toMicros(nowInSec - (metadata == null ? (3 * 3600) : metadata.params.gcGraceSeconds)), false);
                }
                case repaired:
                {
                    PaxosRepairHistory.Searcher history = tableIdToHistory.computeIfAbsent(tableId, find -> {
                        TableMetadata metadata = Schema.instance.getTableMetadata(find);
                        if (metadata == null)
                            return null;
                        return Keyspace.openAndGetStore(metadata).getPaxosRepairHistory().searcher();
                    });

                    return history == null ? row :
                           row.purgeDataOlderThan(history.ballotForToken(token).unixMicros() - paxosPurgeGraceMicros, false);
                }
            }
        }
    }

    class AccordCommandsPurger extends AbstractPurger
    {
        final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        final DurableBefore durableBefore;

        int storeId;
        TxnId txnId;

        AccordCommandsPurger(Supplier<IAccordService> accordService)
        {
            Pair<Int2ObjectHashMap<RedundantBefore>, DurableBefore> redundantBeforesAndDurableBefore = accordService.get().getRedundantBeforesAndDurableBefore();
            this.redundantBefores = redundantBeforesAndDurableBefore.left;
            this.durableBefore = redundantBeforesAndDurableBefore.right;
        }

        protected void beginPartition(UnfilteredRowIterator partition)
        {
            ByteBuffer[] partitionKeyComponents = CommandRows.splitPartitionKey(partition.partitionKey());
            storeId = CommandRows.getStoreId(partitionKeyComponents);
            txnId = CommandRows.getTxnId(partitionKeyComponents);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            RedundantBefore redundantBefore = redundantBefores.get(storeId);
            // TODO (expected): if the store has been retired, this should return null
            if (redundantBefore == null)
                return row;

            // When commands end up being sliced by compaction we need this to discard tombstones and slices
            // without enough information to run the rest of the cleanup logic
            if (durableBefore.isUniversal(txnId))
                return null;

            Cell durabilityCell = row.getCell(CommandsColumns.durability);
            Durability durability = deserializeDurabilityOrNull(durabilityCell);
            Cell executeAtCell = row.getCell(CommandsColumns.execute_at);
            Timestamp executeAt = deserializeTimestampOrNull(executeAtCell);
            Cell routeCell = row.getCell(CommandsColumns.route);
            Route<?> route = deserializeRouteOrNull(routeCell);
            Cell statusCell = row.getCell(CommandsColumns.status);
            SaveStatus saveStatus = deserializeSaveStatusOrNull(statusCell);

            // With a sliced row we might not have enough columns to determine what to do so output the
            // the row unmodified and we will try again later once it merges with the rest of the command state
            // or is dropped by `durableBefore.min(txnId) == Universal`
            if (executeAt == null || durability == null || saveStatus == null || route == null)
                return row;

            Commands.Cleanup cleanup = Commands.shouldCleanup(txnId, saveStatus.status,
                                                              durability, executeAt, route,
                                                              redundantBefore, durableBefore);
            switch (cleanup)
            {
                default: throw new AssertionError(String.format("Unexpected cleanup task: %s", cleanup));
                case ERASE:
                    // Emit a tombstone so if this is slicing the command and making it not possible to determine if it
                    // can be truncated later it can still be dropped via the tombstone.
                    // Eventually the tombstone can be dropped by `durableBefore.min(txnId) == Universal`
                    // We can still encounter sliced command state just because compaction inputs are random
                    return BTreeRow.emptyDeletedRow(row.clustering(), new Row.Deletion(DeletionTime.build(row.primaryKeyLivenessInfo().timestamp(), nowInSec), false));

                case TRUNCATE_WITH_OUTCOME:
                case TRUNCATE:
                    if (saveStatus.compareTo(cleanup.appliesIfNot) >= 0)
                        return maybeDropTruncatedCommandColumns(row, durabilityCell, executeAtCell, routeCell, statusCell);
                    return truncatedApply(cleanup.appliesIfNot,
                                          row, nowInSec, durability, durabilityCell, executeAtCell, routeCell, cleanup == TRUNCATE_WITH_OUTCOME);

                case NO:
                    return row;
            }
        }




        @Override
        protected Row applyToStatic(Row row)
        {
            checkState(row.isStatic() && row.isEmpty());
            return row;
        }
    }

    class AccordTimestampsForKeyPurger extends AbstractPurger
    {
        final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        int storeId;
        PartitionKey partitionKey;

        AccordTimestampsForKeyPurger(Supplier<IAccordService> accordService)
        {
            this.redundantBefores = accordService.get().getRedundantBeforesAndDurableBefore().left;
        }

        protected void beginPartition(UnfilteredRowIterator partition)
        {
            ByteBuffer[] partitionKeyComponents = TimestampsForKeyRows.splitPartitionKey(partition.partitionKey());
            storeId = TimestampsForKeyRows.getStoreId(partitionKeyComponents);
            partitionKey = TimestampsForKeyRows.getKey(partitionKeyComponents);
        }
        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            RedundantBefore redundantBefore = redundantBefores.get(storeId);
            // TODO (expected): if the store has been retired, this should return null
            if (redundantBefore == null)
                return row;

            RedundantBefore.Entry redundantBeforeEntry = redundantBefore.get(partitionKey.toUnseekable());
            if (redundantBeforeEntry == null)
                return row;

            TxnId redundantBeforeTxnId = redundantBeforeEntry.shardRedundantBefore();

            Cell lastExecuteMicrosCell = row.getCell(last_executed_micros);
            Long last_execute_micros = null;
            if (lastExecuteMicrosCell != null && !lastExecuteMicrosCell.accessor().isEmpty(lastExecuteMicrosCell.value()))
                last_execute_micros = lastExecuteMicrosCell.accessor().getLong(lastExecuteMicrosCell.value(), 0);
            if (last_execute_micros != null && last_execute_micros < redundantBeforeTxnId.hlc())
            {
                lastExecuteMicrosCell = null;
            }

            Cell lastExecuteCell = row.getCell(last_executed_timestamp);
            Timestamp last_execute = deserializeTimestampOrNull(lastExecuteCell);
            if (last_execute != null && last_execute.compareTo(redundantBeforeTxnId) < 0)
            {
                lastExecuteCell = null;
            }

            Cell lastWriteCell = row.getCell(last_write_timestamp);
            Timestamp last_write = deserializeTimestampOrNull(lastWriteCell);
            if (last_write != null && last_write.compareTo(redundantBeforeTxnId) < 0)
            {
                lastWriteCell = null;
            }

            Cell<?> maxTimestampCell = row.getCell(max_timestamp);
            Timestamp max_timestamp = deserializeTimestampOrNull(maxTimestampCell);
            if (max_timestamp != null && max_timestamp.compareTo(redundantBeforeTxnId) < 0)
            {
                maxTimestampCell = null;
            }

            // No need to emit a tombstone as earlier versions of the row will also be nulled out
            // when compacted later or loaded into a commands for key
            if (lastExecuteMicrosCell == null &&
                lastExecuteCell == null &&
                lastWriteCell == null &&
                maxTimestampCell == null)
                return null;

            return truncateTimestampsForKeyRow(nowInSec, row, lastExecuteMicrosCell, lastExecuteCell, lastWriteCell, maxTimestampCell);
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            checkState(row.isStatic() && row.isEmpty());
            return row;
        }
    }

    class AccordCommandsForKeyPurger extends AbstractPurger
    {
        final CommandsForKeyAccessor accessor;
        final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        int storeId;
        PartitionKey partitionKey;

        AccordCommandsForKeyPurger(CommandsForKeyAccessor accessor, Supplier<IAccordService> accordService)
        {
            this.accessor = accessor;
            this.redundantBefores = accordService.get().getRedundantBeforesAndDurableBefore().left;
        }

        protected void beginPartition(UnfilteredRowIterator partition)
        {
            ByteBuffer[] partitionKeyComponents = accessor.splitPartitionKey(partition.partitionKey());
            storeId = accessor.getStoreId(partitionKeyComponents);
            partitionKey = accessor.getKey(partitionKeyComponents);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            updateProgress();

            RedundantBefore redundantBefore = redundantBefores.get(storeId);
            // TODO (expected): if the store has been retired, this should return null
            if (redundantBefore == null)
                return row;

            RedundantBefore.Entry redundantBeforeEntry = redundantBefore.get(partitionKey.toUnseekable());
            if (redundantBeforeEntry == null)
                return row;

            TxnId redundantBeforeTxnId = redundantBeforeEntry.shardRedundantBefore();
            Timestamp timestamp = accessor.getTimestamp(row);
            if (timestamp != null && timestamp.compareTo(redundantBeforeTxnId) < 0)
                return null;

            return row;
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            checkState(row.isStatic() && row.isEmpty());
            return row;
        }
    }

    private static class AbortableUnfilteredPartitionTransformation extends Transformation<UnfilteredRowIterator>
    {
        private final AbortableUnfilteredRowTransformation abortableIter;

        private AbortableUnfilteredPartitionTransformation(CompactionIterator iter)
        {
            this.abortableIter = new AbortableUnfilteredRowTransformation(iter);
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            if (abortableIter.iter.isStopRequested())
                throw new CompactionInterruptedException(abortableIter.iter.getCompactionInfo());
            return Transformation.apply(partition, abortableIter);
        }
    }

    private static class AbortableUnfilteredRowTransformation extends Transformation<UnfilteredRowIterator>
    {
        private final CompactionIterator iter;

        private AbortableUnfilteredRowTransformation(CompactionIterator iter)
        {
            this.iter = iter;
        }

        public Row applyToRow(Row row)
        {
            if (iter.isStopRequested())
                throw new CompactionInterruptedException(iter.getCompactionInfo());
            return row;
        }
    }

    private static boolean isPaxos(ColumnFamilyStore cfs)
    {
        return cfs.name.equals(SystemKeyspace.PAXOS) && cfs.getKeyspaceName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);
    }

    private static boolean isAccordCommands(ColumnFamilyStore cfs)
    {
        return cfs.name.equals(AccordKeyspace.COMMANDS) && cfs.keyspace.getName().equals(SchemaConstants.ACCORD_KEYSPACE_NAME);
    }

    private static boolean isAccordCommandsForKey(ColumnFamilyStore cfs, String name)
    {
        return cfs.name.equals(name) && cfs.keyspace.getName().equals(SchemaConstants.ACCORD_KEYSPACE_NAME);
    }

    private static boolean isAccordDepsCommandsForKey(ColumnFamilyStore cfs)
    {
        return isAccordCommandsForKey(cfs, AccordKeyspace.DEPS_COMMANDS_FOR_KEY);
    }

    private static boolean isAccordAllCommandsForKey(ColumnFamilyStore cfs)
    {
        return isAccordCommandsForKey(cfs, AccordKeyspace.ALL_COMMANDS_FOR_KEY);
    }
}