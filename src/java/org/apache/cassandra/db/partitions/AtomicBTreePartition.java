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
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A thread-safe and atomic Partition implementation.
 *
 * Operations (in particular addAll) on this implementation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all rows have
 * been added.
 */
public final class AtomicBTreePartition extends AbstractBTreePartition
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition(null,
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
    private static final AtomicReferenceFieldUpdater<AtomicBTreePartition, BTreePartitionData> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartition.class, BTreePartitionData.class, "ref");

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63KiB/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    @Unmetered
    private final MemtableAllocator allocator;

    private volatile BTreePartitionData ref;

    @Unmetered
    private final TableMetadataRef metadata;

    public AtomicBTreePartition(TableMetadataRef metadata, DecoratedKey partitionKey, MemtableAllocator allocator)
    {
        // involved in potential bug? partition columns may be a subset if we alter columns while it's in memtable
        super(partitionKey);
        this.metadata = metadata;
        this.allocator = allocator;
        this.ref = BTreePartitionData.EMPTY;
    }

    protected BTreePartitionData holder()
    {
        return ref;
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    protected boolean canHaveShadowedData()
    {
        return true;
    }

    /**
     * Adds a given update to this in-memtable partition.
     *
     * @return an array containing first the difference in size seen after merging the updates, and second the minimum
     * time delta between updates.
     */
    public BTreePartitionUpdater addAll(final PartitionUpdate update, Cloner cloner, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        return new Updater(allocator, cloner, writeOp, indexer).addAll(update);
    }

    @VisibleForTesting
    public void unsafeSetHolder(BTreePartitionData holder)
    {
        ref = holder;
    }

    @VisibleForTesting
    public BTreePartitionData unsafeGetHolder()
    {
        return ref;
    }

    class Updater extends BTreePartitionUpdater
    {
        BTreePartitionData current;

        public Updater(MemtableAllocator allocator, Cloner cloner, OpOrder.Group writeOp, UpdateTransaction indexer)
        {
            super(allocator, cloner, writeOp, indexer);
        }

        Updater addAll(final PartitionUpdate update)
        {
            try
            {
                boolean shouldLock = shouldLock(writeOp);
                indexer.start();

                while (true)
                {
                    if (shouldLock)
                    {
                        synchronized (this)
                        {
                            if (tryUpdateData(update))
                                return this;
                        }
                    }
                    else
                    {
                        if (tryUpdateData(update))
                            return this;

                        shouldLock = shouldLock(heapSize, writeOp);
                    }
                }
            }
            finally
            {
                indexer.commit();
                reportAllocatedMemory();
            }
        }

        private boolean tryUpdateData(PartitionUpdate update)
        {
            current = ref;
            this.dataSize = 0;
            this.heapSize = 0;
            BTreePartitionData result = makeMergedPartition(current, update);
            return refUpdater.compareAndSet(AtomicBTreePartition.this, current, result);
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
    public Row getRow(Clustering<?> clustering)
    {
        return allocator.ensureOnHeap().applyToRow(super.getRow(clustering));
    }

    @Override
    public Row lastRow()
    {
        return allocator.ensureOnHeap().applyToRow(super.lastRow());
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(BTreePartitionData current, ColumnFilter selection, Slices slices, boolean reversed)
    {
        return allocator.ensureOnHeap().applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
    }

    @Override
    public Iterator<Row> iterator()
    {
        return allocator.ensureOnHeap().applyToPartition(super.iterator());
    }

    private boolean shouldLock(OpOrder.Group writeOp)
    {
        if (!useLock())
            return false;

        return lockIfOldest(writeOp);
    }

    private boolean shouldLock(long addWaste, OpOrder.Group writeOp)
    {
        if (!updateWastedAllocationTracker(addWaste))
            return false;

        return lockIfOldest(writeOp);
    }

    private boolean lockIfOldest(OpOrder.Group writeOp)
    {
        if (!writeOp.isOldestLiveGroup())
        {
            Thread.yield();
            return writeOp.isOldestLiveGroup();
        }

        return true;
    }

    public boolean useLock()
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
                int time = (int) (nanoTime() >>> CLOCK_SHIFT);
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
}
