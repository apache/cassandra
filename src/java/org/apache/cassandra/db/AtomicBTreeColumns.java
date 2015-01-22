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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.Locks;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.NativePool;

import static org.apache.cassandra.db.index.SecondaryIndexManager.Updater;

/**
 * A thread-safe and atomic ISortedColumns implementation.
 * Operations (in particular addAll) on this implemenation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all columns have
 * been added.
 * <p/>
 * WARNING: removing element through getSortedColumns().iterator() is *not* supported
 */
public class AtomicBTreeColumns extends ColumnFamily
{
    static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreeColumns(CFMetaData.IndexCf, null))
            + ObjectSizes.measure(new Holder(null, null));

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

    private static final AtomicIntegerFieldUpdater<AtomicBTreeColumns> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicBTreeColumns.class, "wasteTracker");

    private static final Function<Cell, CellName> NAME = new Function<Cell, CellName>()
    {
        public CellName apply(Cell column)
        {
            return column.name();
        }
    };

    public static final Factory<AtomicBTreeColumns> factory = new Factory<AtomicBTreeColumns>()
    {
        public AtomicBTreeColumns create(CFMetaData metadata, boolean insertReversed, int initialCapacity)
        {
            if (insertReversed)
                throw new IllegalArgumentException();
            return new AtomicBTreeColumns(metadata);
        }
    };

    private static final DeletionInfo LIVE = DeletionInfo.live();
    // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
    // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
    private static final Holder EMPTY = new Holder(BTree.empty(), LIVE);

    private volatile Holder ref;

    private static final AtomicReferenceFieldUpdater<AtomicBTreeColumns, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreeColumns.class, Holder.class, "ref");

    private AtomicBTreeColumns(CFMetaData metadata)
    {
        this(metadata, EMPTY);
    }

    private AtomicBTreeColumns(CFMetaData metadata, Holder holder)
    {
        super(metadata);
        this.ref = holder;
    }

    public Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new AtomicBTreeColumns(metadata, ref);
    }

    public DeletionInfo deletionInfo()
    {
        return ref.deletionInfo;
    }

    public void delete(DeletionTime delTime)
    {
        delete(new DeletionInfo(delTime));
    }

    protected void delete(RangeTombstone tombstone)
    {
        delete(new DeletionInfo(tombstone, getComparator()));
    }

    public void delete(DeletionInfo info)
    {
        if (info.isLive())
            return;

        // Keeping deletion info for max markedForDeleteAt value
        while (true)
        {
            Holder current = ref;
            DeletionInfo curDelInfo = current.deletionInfo;
            DeletionInfo newDelInfo = info.mayModify(curDelInfo) ? curDelInfo.copy().add(info) : curDelInfo;
            if (refUpdater.compareAndSet(this, current, current.with(newDelInfo)))
                break;
        }
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        ref = ref.with(newInfo);
    }

    public void purgeTombstones(int gcBefore)
    {
        while (true)
        {
            Holder current = ref;
            if (!current.deletionInfo.hasPurgeableTombstones(gcBefore))
                break;

            DeletionInfo purgedInfo = current.deletionInfo.copy();
            purgedInfo.purge(gcBefore);
            if (refUpdater.compareAndSet(this, current, current.with(purgedInfo)))
                break;
        }
    }

    /**
     * This is only called by Memtable.resolve, so only AtomicBTreeColumns needs to implement it.
     *
     * @return the difference in size seen after merging the given columns
     */
    public Pair<Long, Long> addAllWithSizeDelta(final ColumnFamily cm, MemtableAllocator allocator, OpOrder.Group writeOp, Updater indexer)
    {
        ColumnUpdater updater = new ColumnUpdater(this, cm.metadata, allocator, writeOp, indexer);
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
                if (cm.deletionInfo().mayModify(current.deletionInfo))
                {
                    if (inputDeletionInfoCopy == null)
                        inputDeletionInfoCopy = cm.deletionInfo().copy(HeapAllocator.instance);

                    deletionInfo = current.deletionInfo.copy().add(inputDeletionInfoCopy);
                    updater.allocated(deletionInfo.unsharedHeapSize() - current.deletionInfo.unsharedHeapSize());
                }
                else
                {
                    deletionInfo = current.deletionInfo;
                }

                Object[] tree = BTree.update(current.tree, metadata.comparator.columnComparator(Memtable.MEMORY_POOL instanceof NativePool), cm, cm.getColumnCount(), true, updater);

                if (tree != null && refUpdater.compareAndSet(this, current, new Holder(tree, deletionInfo)))
                {
                    indexer.updateRowLevelIndexes();
                    updater.finish();
                    return Pair.create(updater.dataSize, updater.colUpdateTimeDelta);
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

    boolean usePessimisticLocking()
    {
        return wasteTracker == TRACKER_PESSIMISTIC_LOCKING;
    }

    /**
     * Update the wasted allocation tracker state based on newly wasted allocation information
     *
     * @param wastedBytes the number of bytes wasted by this thread
     * @return true if the caller should now proceed with pessimistic locking because the waste limit has been reached
     */
    private boolean updateWastedAllocationTracker(long wastedBytes) {
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

    // no particular reason not to implement these next methods, we just haven't needed them yet

    public void addColumn(Cell column)
    {
        throw new UnsupportedOperationException();
    }

    public void maybeAppendColumn(Cell cell, DeletionInfo.InOrderTester tester, int gcBefore)
    {
        throw new UnsupportedOperationException();
    }

    public void addAll(ColumnFamily cf)
    {
        throw new UnsupportedOperationException();
    }

    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    public Cell getColumn(CellName name)
    {
        return (Cell) BTree.find(ref.tree, asymmetricComparator(), name);
    }

    private Comparator<Object> asymmetricComparator()
    {
        return metadata.comparator.asymmetricColumnComparator(Memtable.MEMORY_POOL instanceof NativePool);
    }

    public Iterable<CellName> getColumnNames()
    {
        return collection(false, NAME);
    }

    public Collection<Cell> getSortedColumns()
    {
        return collection(true, Functions.<Cell>identity());
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        return collection(false, Functions.<Cell>identity());
    }

    private <V> Collection<V> collection(final boolean forwards, final Function<Cell, V> f)
    {
        final Holder ref = this.ref;
        return new AbstractCollection<V>()
        {
            public Iterator<V> iterator()
            {
                return Iterators.transform(BTree.<Cell>slice(ref.tree, forwards), f);
            }

            public int size()
            {
                return BTree.slice(ref.tree, true).count();
            }
        };
    }

    public int getColumnCount()
    {
        return BTree.slice(ref.tree, true).count();
    }

    public boolean hasColumns()
    {
        return !BTree.isEmpty(ref.tree);
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        return slices.length == 1
             ? slice(ref.tree, asymmetricComparator(), slices[0].start, slices[0].finish, true)
             : new SliceIterator(ref.tree, asymmetricComparator(), true, slices);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        return slices.length == 1
             ? slice(ref.tree, asymmetricComparator(), slices[0].finish, slices[0].start, false)
             : new SliceIterator(ref.tree, asymmetricComparator(), false, slices);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    public BatchRemoveIterator<Cell> batchRemoveIterator()
    {
        throw new UnsupportedOperationException();
    }

    private static final class Holder
    {
        final DeletionInfo deletionInfo;
        // the btree of columns
        final Object[] tree;

        Holder(Object[] tree, DeletionInfo deletionInfo)
        {
            this.tree = tree;
            this.deletionInfo = deletionInfo;
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(this.tree, info);
        }
    }

    // the function we provide to the btree utilities to perform any column replacements
    private static final class ColumnUpdater implements UpdateFunction<Cell>
    {
        final AtomicBTreeColumns updating;
        final CFMetaData metadata;
        final MemtableAllocator allocator;
        final OpOrder.Group writeOp;
        final Updater indexer;
        Holder ref;
        long dataSize;
        long heapSize;
        long colUpdateTimeDelta = Long.MAX_VALUE;
        final MemtableAllocator.DataReclaimer reclaimer;
        List<Cell> inserted; // TODO: replace with walk of aborted BTree

        private ColumnUpdater(AtomicBTreeColumns updating, CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group writeOp, Updater indexer)
        {
            this.updating = updating;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.indexer = indexer;
            this.metadata = metadata;
            this.reclaimer = allocator.reclaimer();
        }

        public Cell apply(Cell insert)
        {
            indexer.insert(insert);
            insert = insert.localCopy(metadata, allocator, writeOp);
            this.dataSize += insert.cellDataSize();
            this.heapSize += insert.unsharedHeapSizeExcludingData();
            if (inserted == null)
                inserted = new ArrayList<>();
            inserted.add(insert);
            return insert;
        }

        public Cell apply(Cell existing, Cell update)
        {
            Cell reconciled = existing.reconcile(update);
            indexer.update(existing, reconciled);
            if (existing != reconciled)
            {
                reconciled = reconciled.localCopy(metadata, allocator, writeOp);
                dataSize += reconciled.cellDataSize() - existing.cellDataSize();
                heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
                if (inserted == null)
                    inserted = new ArrayList<>();
                inserted.add(reconciled);
                discard(existing);
                //Getting the minimum delta for an update containing multiple columns
                colUpdateTimeDelta =  Math.min(Math.abs(existing.timestamp()  - update.timestamp()), colUpdateTimeDelta);
            }
            return reconciled;
        }

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            if (inserted != null)
            {
                for (Cell cell : inserted)
                    abort(cell);
                inserted.clear();
            }
            reclaimer.cancel();
        }

        protected void abort(Cell abort)
        {
            reclaimer.reclaimImmediately(abort);
        }

        protected void discard(Cell discard)
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
            allocator.onHeap().allocate(heapSize, writeOp);
            reclaimer.commit();
        }
    }

    private static class SliceIterator extends AbstractIterator<Cell>
    {
        private final Object[] btree;
        private final boolean forwards;
        private final Comparator<Object> comparator;
        private final ColumnSlice[] slices;

        private int idx = 0;
        private Iterator<Cell> currentSlice;

        SliceIterator(Object[] btree, Comparator<Object> comparator, boolean forwards, ColumnSlice[] slices)
        {
            this.btree = btree;
            this.comparator = comparator;
            this.slices = slices;
            this.forwards = forwards;
        }

        protected Cell computeNext()
        {
            while (currentSlice != null || idx < slices.length)
            {
                if (currentSlice == null)
                {
                    ColumnSlice slice = slices[idx++];
                    if (forwards)
                        currentSlice = slice(btree, comparator, slice.start, slice.finish, true);
                    else
                        currentSlice = slice(btree, comparator, slice.finish, slice.start, false);
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }

            return endOfData();
        }
    }

    private static Iterator<Cell> slice(Object[] btree, Comparator<Object> comparator, Composite start, Composite finish, boolean forwards)
    {
        return BTree.slice(btree,
                           comparator,
                           start.isEmpty() ? null : start,
                           true,
                           finish.isEmpty() ? null : finish,
                           true,
                           forwards);
    }
}
