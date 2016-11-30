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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.Locks;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * A thread-safe and atomic Partition implementation.
 *
 * Operations (in particular addAll) on this implementation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all rows have
 * been added.
 */
public class AtomicBTreePartition extends AbstractBTreePartition
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(CFMetaData.createFake("keyspace", "table"),
                                                                                       DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.allocate(1)),
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

    private static final AtomicIntegerFieldUpdater<AtomicBTreePartition> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicBTreePartition.class, "wasteTracker");
    private static final AtomicReferenceFieldUpdater<AtomicBTreePartition, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartition.class, Holder.class, "ref");

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63Kb/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    private final MemtableAllocator allocator;
    private volatile Holder ref;

    public AtomicBTreePartition(CFMetaData metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        // involved in potential bug? partition columns may be a subset if we alter columns while it's in memtable
        super(metadata, partitionKey);
        this.allocator = allocator;
        this.ref = EMPTY;
    }

    protected Holder holder()
    {
        return ref;
    }

    protected boolean canHaveShadowedData()
    {
        return true;
    }

    /**
     * Adds a given update to this in-memtable partition.
     *
     * @return an array containing first the difference in size seen after merging the updates, and second the minimum
     * time detla between updates.
     */
    public long[] addAllWithSizeDelta(final PartitionUpdate update, OpOrder.Group writeOp, UpdateTransaction indexer)
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

            indexer.start();

            while (true)
            {
                Holder current = ref;
                updater.ref = current;
                updater.reset();

                if (!update.deletionInfo().getPartitionDeletion().isLive())
                    indexer.onPartitionDeletion(update.deletionInfo().getPartitionDeletion());

                if (update.deletionInfo().hasRanges())
                    update.deletionInfo().rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);

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

                PartitionColumns columns = update.columns().mergeTo(current.columns);
                Row newStatic = update.staticRow();
                Row staticRow = newStatic.isEmpty()
                              ? current.staticRow
                              : (current.staticRow.isEmpty() ? updater.apply(newStatic) : updater.apply(current.staticRow, newStatic));
                Object[] tree = BTree.update(current.tree, update.metadata().comparator, update, update.rowCount(), updater);
                EncodingStats newStats = current.stats.mergeWith(update.stats());

                if (tree != null && refUpdater.compareAndSet(this, current, new Holder(columns, tree, deletionInfo, staticRow, newStats)))
                {
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
            indexer.commit();
            if (monitorOwned)
                Locks.monitorExitUnsafe(this);
        }
    }

    @Override
    public DeletionInfo deletionInfo()
    {
        return allocator.ensureOnHeap().applyToDeletionInfo(super.deletionInfo());
    }

    @Override
    public Row staticRow()
    {
        return allocator.ensureOnHeap().applyToStatic(super.staticRow());
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return allocator.ensureOnHeap().applyToPartitionKey(super.partitionKey());
    }

    @Override
    public Row getRow(Clustering clustering)
    {
        return allocator.ensureOnHeap().applyToRow(super.getRow(clustering));
    }

    @Override
    public Row lastRow()
    {
        return allocator.ensureOnHeap().applyToRow(super.lastRow());
    }

    @Override
    public SearchIterator<Clustering, Row> searchIterator(ColumnFilter columns, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.searchIterator(columns, reversed));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(selection, slices, reversed));
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(Holder current, ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
    }

    @Override
    public Iterator<Row> iterator()
    {
        return allocator.ensureOnHeap().applyToPartition(super.iterator());
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

    // the function we provide to the btree utilities to perform any column replacements
    private static final class RowUpdater implements UpdateFunction<Row, Row>
    {
        final AtomicBTreePartition updating;
        final MemtableAllocator allocator;
        final OpOrder.Group writeOp;
        final UpdateTransaction indexer;
        final int nowInSec;
        Holder ref;
        Row.Builder regularBuilder;
        long dataSize;
        long heapSize;
        long colUpdateTimeDelta = Long.MAX_VALUE;
        List<Row> inserted; // TODO: replace with walk of aborted BTree

        private RowUpdater(AtomicBTreePartition updating, MemtableAllocator allocator, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.nowInSec = FBUtilities.nowInSeconds();
        }

        private Row.Builder builder(Clustering clustering)
        {
            boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
            // We know we only insert/update one static per PartitionUpdate, so no point in saving the builder
            if (isStatic)
                return allocator.rowBuilder(writeOp);

            if (regularBuilder == null)
                regularBuilder = allocator.rowBuilder(writeOp);
            return regularBuilder;
        }

        public Row apply(Row insert)
        {
            Row data = Rows.copy(insert, builder(insert.clustering())).build();
            indexer.onInserted(insert);

            this.dataSize += data.dataSize();
            this.heapSize += data.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(data);
            return data;
        }

        public Row apply(Row existing, Row update)
        {
            Row.Builder builder = builder(existing.clustering());
            colUpdateTimeDelta = Math.min(colUpdateTimeDelta, Rows.merge(existing, update, builder, nowInSec));

            Row reconciled = builder.build();

            indexer.onUpdated(existing, reconciled);

            dataSize += reconciled.dataSize() - existing.dataSize();
            heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(reconciled);

            return reconciled;
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            if (inserted != null)
                inserted.clear();
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
        }
    }
}
