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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Locks;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.service.StorageService;
import static org.apache.cassandra.db.index.SecondaryIndexManager.Updater;

/**
 * A thread-safe and atomic Partition implementation.
 *
 * Operations (in particular addAll) on this implementation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all rows have
 * been added.
 */
public class AtomicBTreePartition implements Partition
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicBTreePartition.class);

    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(CFMetaData.createFake("keyspace", "table"),
                                                                                       StorageService.getPartitioner().decorateKey(ByteBuffer.allocate(1)),
                                                                                       null));

    // Reserved values for wasteTracker field. These values must not be consecutive (see avoidReservedValues)
    private static final int TRACKER_NEVER_WASTED = 0;
    private static final int TRACKER_PESSIMISTIC_LOCKING = Integer.MAX_VALUE;

    // The granularity with which we track wasted allocation/work; we round up
    private static final int ALLOCATION_GRANULARITY_BYTES = 1024;
    // The number of bytes we have to waste in excess of our acceptable realtime rate of waste (defined below)
    private static final long EXCESS_WASTE_BYTES = 10 * 1024 * 1024L;
    private static final int EXCESS_WASTE_OFFSET = (int) (EXCESS_WASTE_BYTES / ALLOCATION_GRANULARITY_BYTES);
    // Note this is a shift, because dividing a long time and then picking the low 32 bits doesn't give correct rollover behavior
    private static final int CLOCK_SHIFT = 17;
    // CLOCK_GRANULARITY = 1^9ns >> CLOCK_SHIFT == 132us == (1/7.63)ms

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63Kb/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    private static final AtomicIntegerFieldUpdater<AtomicBTreePartition> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicBTreePartition.class, "wasteTracker");

    private static final Holder EMPTY = new Holder(BTree.empty(), DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, RowStats.NO_STATS);

    private final CFMetaData metadata;
    private final DecoratedKey partitionKey;
    private final MemtableAllocator allocator;

    private volatile Holder ref;

    private static final AtomicReferenceFieldUpdater<AtomicBTreePartition, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartition.class, Holder.class, "ref");

    public AtomicBTreePartition(CFMetaData metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
        this.allocator = allocator;
        this.ref = EMPTY;
    }

    public boolean isEmpty()
    {
        return ref.deletionInfo.isLive() && BTree.isEmpty(ref.tree) && ref.staticRow == null;
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return ref.deletionInfo.getPartitionDeletion();
    }

    public PartitionColumns columns()
    {
        // We don't really know which columns will be part of the update, so assume it's all of them
        return metadata.partitionColumns();
    }

    public boolean hasRows()
    {
        return !BTree.isEmpty(ref.tree);
    }

    public RowStats stats()
    {
        return ref.stats;
    }

    public Row getRow(Clustering clustering)
    {
        Row row = searchIterator(ColumnFilter.selection(columns()), false).next(clustering);
        // Note that for statics, this will never return null, this will return an empty row. However,
        // it's more consistent for this method to return null if we don't really have a static row.
        return row == null || (clustering == Clustering.STATIC_CLUSTERING && row.isEmpty()) ? null : row;
    }

    private Row staticRow(Holder current, ColumnFilter columns, boolean setActiveDeletionToRow)
    {
        DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
        if (columns.fetchedColumns().statics.isEmpty() || (current.staticRow.isEmpty() && partitionDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;

        Row row = current.staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, metadata);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    public SearchIterator<Clustering, Row> searchIterator(final ColumnFilter columns, final boolean reversed)
    {
        // TODO: we could optimize comparison for "NativeRow" à la #6755
        final Holder current = ref;
        return new SearchIterator<Clustering, Row>()
        {
            private final SearchIterator<Clustering, Row> rawIter = new BTreeSearchIterator<>(current.tree, metadata.comparator, !reversed);
            private final DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();

            public boolean hasNext()
            {
                return rawIter.hasNext();
            }

            public Row next(Clustering clustering)
            {
                if (clustering == Clustering.STATIC_CLUSTERING)
                    return staticRow(current, columns, true);

                Row row = rawIter.next(clustering);
                RangeTombstone rt = current.deletionInfo.rangeCovering(clustering);

                // A search iterator only return a row, so it doesn't allow to directly account for deletion that should apply to to row
                // (the partition deletion or the deletion of a range tombstone that covers it). So if needs be, reuse the row deletion
                // to carry the proper deletion on the row.
                DeletionTime activeDeletion = partitionDeletion;
                if (rt != null && rt.deletionTime().supersedes(activeDeletion))
                    activeDeletion = rt.deletionTime();

                if (row == null)
                    return activeDeletion.isLive() ? null : ArrayBackedRow.emptyDeletedRow(clustering, activeDeletion);

                return row.filter(columns, activeDeletion, true, metadata);
            }
        };
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.all(metadata()), Slices.ALL, false);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        if (slices.size() == 0)
        {
            Holder current = ref;
            DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
            if (selection.fetchedColumns().statics.isEmpty() && partitionDeletion.isLive())
                return UnfilteredRowIterators.emptyIterator(metadata, partitionKey, reversed);

            return new AbstractUnfilteredRowIterator(metadata,
                                                     partitionKey,
                                                     partitionDeletion,
                                                     selection.fetchedColumns(),
                                                     staticRow(current, selection, false),
                                                     reversed,
                                                     current.stats)
            {
                protected Unfiltered computeNext()
                {
                    return endOfData();
                }
            };
        }

        Holder current = ref;
        Row staticRow = staticRow(current, selection, false);
        return slices.size() == 1
             ? sliceIterator(selection, slices.get(0), reversed, current, staticRow)
             : new SlicesIterator(metadata, partitionKey, selection, slices, reversed, current, staticRow);
    }

    private UnfilteredRowIterator sliceIterator(ColumnFilter selection, Slice slice, boolean reversed, Holder current, Row staticRow)
    {
        Slice.Bound start = slice.start() == Slice.Bound.BOTTOM ? null : slice.start();
        Slice.Bound end = slice.end() == Slice.Bound.TOP ? null : slice.end();
        Iterator<Row> rowIter = BTree.slice(current.tree, metadata.comparator, start, true, end, true, !reversed);

        return new RowAndDeletionMergeIterator(metadata,
                                               partitionKey,
                                               current.deletionInfo.getPartitionDeletion(),
                                               selection,
                                               staticRow,
                                               reversed,
                                               current.stats,
                                               rowIter,
                                               current.deletionInfo.rangeIterator(slice, reversed),
                                               true);
    }

    public class SlicesIterator extends AbstractUnfilteredRowIterator
    {
        private final Holder current;
        private final ColumnFilter selection;
        private final Slices slices;

        private int idx;
        private Iterator<Unfiltered> currentSlice;

        private SlicesIterator(CFMetaData metadata,
                               DecoratedKey key,
                               ColumnFilter selection,
                               Slices slices,
                               boolean isReversed,
                               Holder holder,
                               Row staticRow)
        {
            super(metadata, key, holder.deletionInfo.getPartitionDeletion(), selection.fetchedColumns(), staticRow, isReversed, holder.stats);
            this.current = holder;
            this.selection = selection;
            this.slices = slices;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if (idx >= slices.size())
                        return endOfData();

                    int sliceIdx = isReverseOrder ? slices.size() - idx - 1 : idx;
                    currentSlice = sliceIterator(selection, slices.get(sliceIdx), isReverseOrder, current, Rows.EMPTY_STATIC_ROW);
                    idx++;
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }
    }

    /**
     * Adds a given update to this in-memtable partition.
     *
     * @return an array containing first the difference in size seen after merging the updates, and second the minimum
     * time detla between updates.
     */
    public long[] addAllWithSizeDelta(final PartitionUpdate update, OpOrder.Group writeOp, Updater indexer)
    {
        RowUpdater updater = new RowUpdater(this, allocator, writeOp, indexer);
        DeletionInfo inputDeletionInfoCopy = null;

        boolean monitorOwned = false;
        try
        {
            if (usePessimisticLocking())
            {
                Locks.monitorEnterUnsafe(this);
                monitorOwned = true;
            }
            while (true)
            {
                Holder current = ref;
                updater.ref = current;
                updater.reset();

                DeletionInfo deletionInfo;
                if (update.deletionInfo().mayModify(current.deletionInfo))
                {
                    if (inputDeletionInfoCopy == null)
                        inputDeletionInfoCopy = update.deletionInfo().copy(HeapAllocator.instance);

                    deletionInfo = current.deletionInfo.mutableCopy().add(inputDeletionInfoCopy);
                    updater.allocated(deletionInfo.unsharedHeapSize() - current.deletionInfo.unsharedHeapSize());
                }
                else
                {
                    deletionInfo = current.deletionInfo;
                }

                Row newStatic = update.staticRow();
                Row staticRow = newStatic.isEmpty()
                              ? current.staticRow
                              : (current.staticRow.isEmpty() ? updater.apply(newStatic) : updater.apply(current.staticRow, newStatic));
                Object[] tree = BTree.update(current.tree, update.metadata().comparator, update, update.rowCount(), updater);
                RowStats newStats = current.stats.mergeWith(update.stats());

                if (tree != null && refUpdater.compareAndSet(this, current, new Holder(tree, deletionInfo, staticRow, newStats)))
                {
                    indexer.updateRowLevelIndexes();
                    updater.finish();
                    return new long[]{ updater.dataSize, updater.colUpdateTimeDelta };
                }
                else if (!monitorOwned)
                {
                    boolean shouldLock = usePessimisticLocking();
                    if (!shouldLock)
                    {
                        shouldLock = updateWastedAllocationTracker(updater.heapSize);
                    }
                    if (shouldLock)
                    {
                        Locks.monitorEnterUnsafe(this);
                        monitorOwned = true;
                    }
                }
            }
        }
        finally
        {
            if (monitorOwned)
                Locks.monitorExitUnsafe(this);
        }

    }

    public boolean usePessimisticLocking()
    {
        return wasteTracker == TRACKER_PESSIMISTIC_LOCKING;
    }

    /**
     * Update the wasted allocation tracker state based on newly wasted allocation information
     *
     * @param wastedBytes the number of bytes wasted by this thread
     * @return true if the caller should now proceed with pessimistic locking because the waste limit has been reached
     */
    private boolean updateWastedAllocationTracker(long wastedBytes)
    {
        // Early check for huge allocation that exceeds the limit
        if (wastedBytes < EXCESS_WASTE_BYTES)
        {
            // We round up to ensure work < granularity are still accounted for
            int wastedAllocation = ((int) (wastedBytes + ALLOCATION_GRANULARITY_BYTES - 1)) / ALLOCATION_GRANULARITY_BYTES;

            int oldTrackerValue;
            while (TRACKER_PESSIMISTIC_LOCKING != (oldTrackerValue = wasteTracker))
            {
                // Note this time value has an arbitrary offset, but is a constant rate 32 bit counter (that may wrap)
                int time = (int) (System.nanoTime() >>> CLOCK_SHIFT);
                int delta = oldTrackerValue - time;
                if (oldTrackerValue == TRACKER_NEVER_WASTED || delta >= 0 || delta < -EXCESS_WASTE_OFFSET)
                    delta = -EXCESS_WASTE_OFFSET;
                delta += wastedAllocation;
                if (delta >= 0)
                    break;
                if (wasteTrackerUpdater.compareAndSet(this, oldTrackerValue, avoidReservedValues(time + delta)))
                    return false;
            }
        }
        // We have definitely reached our waste limit so set the state if it isn't already
        wasteTrackerUpdater.set(this, TRACKER_PESSIMISTIC_LOCKING);
        // And tell the caller to proceed with pessimistic locking
        return true;
    }

    private static int avoidReservedValues(int wasteTracker)
    {
        if (wasteTracker == TRACKER_NEVER_WASTED || wasteTracker == TRACKER_PESSIMISTIC_LOCKING)
            return wasteTracker + 1;
        return wasteTracker;
    }

    private static final class Holder
    {
        final DeletionInfo deletionInfo;
        // the btree of rows
        final Object[] tree;
        final Row staticRow;
        final RowStats stats;

        Holder(Object[] tree, DeletionInfo deletionInfo, Row staticRow, RowStats stats)
        {
            this.tree = tree;
            this.deletionInfo = deletionInfo;
            this.staticRow = staticRow;
            this.stats = stats;
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(this.tree, info, this.staticRow, this.stats);
        }
    }

    // the function we provide to the btree utilities to perform any column replacements
    private static final class RowUpdater implements UpdateFunction<Row, Row>
    {
        final AtomicBTreePartition updating;
        final MemtableAllocator allocator;
        final OpOrder.Group writeOp;
        final Updater indexer;
        final int nowInSec;
        Holder ref;
        Row.Builder regularBuilder;
        long dataSize;
        long heapSize;
        long colUpdateTimeDelta = Long.MAX_VALUE;
        final MemtableAllocator.DataReclaimer reclaimer;
        List<Row> inserted; // TODO: replace with walk of aborted BTree


        private RowUpdater(AtomicBTreePartition updating, MemtableAllocator allocator, OpOrder.Group writeOp, Updater indexer)
        {
            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.nowInSec = FBUtilities.nowInSeconds();
            this.reclaimer = allocator.reclaimer();
        }

        private Row.Builder builder(Clustering clustering)
        {
            boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
            // We know we only insert/update one static per PartitionUpdate, so no point in saving the builder
            if (isStatic)
                return allocator.rowBuilder(updating.metadata(), writeOp, true);

            if (regularBuilder == null)
                regularBuilder = allocator.rowBuilder(updating.metadata(), writeOp, false);
            return regularBuilder;
        }

        public Row apply(Row insert)
        {
            Row data = Rows.copy(insert, builder(insert.clustering())).build();
            insertIntoIndexes(data);

            this.dataSize += data.dataSize();
            this.heapSize += data.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(data);
            return data;
        }

        public Row apply(Row existing, Row update)
        {
            Columns mergedColumns = existing.columns().mergeTo(update.columns());

            Row.Builder builder = builder(existing.clustering());
            colUpdateTimeDelta = Math.min(colUpdateTimeDelta, Rows.merge(existing, update, mergedColumns, builder, nowInSec, indexer));

            Row reconciled = builder.build();

            dataSize += reconciled.dataSize() - existing.dataSize();
            heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(reconciled);
            discard(existing);

            return reconciled;
        }

        private void insertIntoIndexes(Row toInsert)
        {
            if (indexer == SecondaryIndexManager.nullUpdater)
                return;

            maybeIndexPrimaryKeyColumns(toInsert);
            Clustering clustering = toInsert.clustering();
            for (Cell cell : toInsert.cells())
                indexer.insert(clustering, cell);
        }

        private void maybeIndexPrimaryKeyColumns(Row row)
        {
            // We want to update a primary key index with the most up to date info contains in that inserted row (if only for
            // backward compatibility). Note that if there is an index but not a partition key or clustering column one, we've
            // wasting this work. We might be able to avoid that if row indexing was pushed in the index updater.
            long timestamp = row.primaryKeyLivenessInfo().timestamp();
            int ttl = row.primaryKeyLivenessInfo().ttl();

            for (Cell cell : row.cells())
            {
                long cellTimestamp = cell.timestamp();
                if (cell.isLive(nowInSec))
                {
                    if (cellTimestamp > timestamp)
                    {
                        timestamp = cellTimestamp;
                        ttl = cell.ttl();
                    }
                }
            }

            indexer.maybeIndex(row.clustering(), timestamp, ttl, row.deletion());
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            if (inserted != null)
            {
                for (Row row : inserted)
                    abort(row);
                inserted.clear();
            }
            reclaimer.cancel();
        }

        protected void abort(Row abort)
        {
            reclaimer.reclaimImmediately(abort);
        }

        protected void discard(Row discard)
        {
            reclaimer.reclaim(discard);
        }

        public boolean abortEarly()
        {
            return updating.ref != ref;
        }

        public void allocated(long heapSize)
        {
            this.heapSize += heapSize;
        }

        protected void finish()
        {
            allocator.onHeap().adjust(heapSize, writeOp);
            reclaimer.commit();
        }
    }
}
