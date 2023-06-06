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

package org.apache.cassandra.db.memtable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Promise;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableCleaner;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;
import org.github.jamm.Unmetered;

/**
 * A memtable that uses memory tracked and maybe allocated via a MemtableAllocator from a MemtablePool.
 * Provides methods of memory tracking and triggering flushes when the relevant limits are reached.
 */
public abstract class AbstractAllocatorMemtable extends AbstractMemtableWithCommitlog
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractAllocatorMemtable.class);

    public static final MemtablePool MEMORY_POOL = AbstractAllocatorMemtable.createMemtableAllocatorPool();

    @Unmetered
    protected final Owner owner;
    @Unmetered  // total pool size should not be included in memtable's deep size
    protected final MemtableAllocator allocator;

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    @Unmetered
    protected final ClusteringComparator initialComparator;
    // As above, used to determine if the memtable needs to be flushed on schema change.
    @Unmetered
    public final Factory initialFactory;

    private final long creationNano = Clock.Global.nanoTime();

    @VisibleForTesting
    static MemtablePool createMemtableAllocatorPool()
    {
        Config.MemtableAllocationType allocationType = DatabaseDescriptor.getMemtableAllocationType();
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMiB() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMiB() << 20;
        float memtableCleanupThreshold = DatabaseDescriptor.getMemtableCleanupThreshold();
        MemtableCleaner cleaner = AbstractAllocatorMemtable::flushLargestMemtable;
        return createMemtableAllocatorPoolInternal(allocationType, heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
    }

    @VisibleForTesting
    public static MemtablePool createMemtableAllocatorPoolInternal(Config.MemtableAllocationType allocationType,
                                                                   long heapLimit, long offHeapLimit,
                                                                   float memtableCleanupThreshold, MemtableCleaner cleaner)
    {
        switch (allocationType)
        {
        case unslabbed_heap_buffers_logged:
            return new HeapPool.Logged(heapLimit, memtableCleanupThreshold, cleaner);
        case unslabbed_heap_buffers:
            logger.debug("Memtables allocating with on-heap buffers");
            return new HeapPool(heapLimit, memtableCleanupThreshold, cleaner);
        case heap_buffers:
            logger.debug("Memtables allocating with on-heap slabs");
            return new SlabPool(heapLimit, 0, memtableCleanupThreshold, cleaner);
        case offheap_buffers:
            logger.debug("Memtables allocating with off-heap buffers");
            return new SlabPool(heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
        case offheap_objects:
            logger.debug("Memtables allocating with off-heap objects");
            return new NativePool(heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
        default:
            throw new AssertionError();
        }
    }

    // only to be used by init(), to setup the very first memtable for the cfs
    public AbstractAllocatorMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(metadataRef, commitLogLowerBound);
        this.allocator = MEMORY_POOL.newAllocator(metadataRef.toString());
        this.initialComparator = metadata.get().comparator;
        this.initialFactory = metadata().params.memtable.factory();
        this.owner = owner;
        scheduleFlush();
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    @Override
    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        switch (reason)
        {
        case SCHEMA_CHANGE:
            return initialComparator != metadata().comparator // If the CF comparator has changed, because our partitions reference the old one
                   || !initialFactory.equals(metadata().params.memtable.factory()); // If a different type of memtable is requested
        case OWNED_RANGES_CHANGE:
            return false; // by default we don't use the local ranges, thus this has no effect
        default:
            return true;
        }
    }

    public void metadataUpdated()
    {
        // We decided not to swap out this memtable, but if the flush period has changed we must schedule it for the
        // new expiration time.
        scheduleFlush();
    }

    public void localRangesUpdated()
    {
        // nothing to be done by default
    }

    public void performSnapshot(String snapshotName)
    {
        throw new AssertionError("performSnapshot must be implemented if shouldSwitch(SNAPSHOT) can return false.");
    }

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        super.switchOut(writeBarrier, commitLogUpperBound);
        allocator.setDiscarding();
    }

    public void discard()
    {
        super.discard();
        allocator.setDiscarded();
    }

    public String toString()
    {
        MemoryUsage usage = Memtable.getMemoryUsage(this);
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %s)",
                             metadata.get().name,
                             hashCode(),
                             FBUtilities.prettyPrintMemory(getLiveDataSize()),
                             operationCount(),
                             usage);
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage stats)
    {
        stats.ownershipRatioOnHeap += getAllocator().onHeap().ownershipRatio();
        stats.ownershipRatioOffHeap += getAllocator().offHeap().ownershipRatio();
        stats.ownsOnHeap += getAllocator().onHeap().owns();
        stats.ownsOffHeap += getAllocator().offHeap().owns();
    }

    public void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        getAllocator().onHeap().allocate(additionalSpace, opGroup);
    }

    public void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        getAllocator().offHeap().allocate(additionalSpace, opGroup);
    }

    void scheduleFlush()
    {
        int period = metadata().params.memtableFlushPeriodInMs;
        if (period > 0)
            scheduleFlush(owner, period);
    }

    private static void scheduleFlush(Owner owner, int period)
    {
        logger.trace("scheduling flush in {} ms", period);
        WrappedRunnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow()
            {
                Memtable current = owner.getCurrentMemtable();
                if (current instanceof AbstractAllocatorMemtable)
                    ((AbstractAllocatorMemtable) current).flushIfPeriodExpired();
            }
        };
        ScheduledExecutors.scheduledTasks.scheduleSelfRecurring(runnable, period, TimeUnit.MILLISECONDS);
    }

    private void flushIfPeriodExpired()
    {
        int period = metadata().params.memtableFlushPeriodInMs;
        if (period > 0 && (Clock.Global.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period)))
        {
            if (isClean())
            {
                // if we're still clean, instead of swapping just reschedule a flush for later
                scheduleFlush(owner, period);
            }
            else
            {
                // we'll be rescheduled by the constructor of the Memtable.
                owner.signalFlushRequired(AbstractAllocatorMemtable.this,
                                          ColumnFamilyStore.FlushReason.MEMTABLE_PERIOD_EXPIRED);
            }
        }
    }

    /**
     * Finds the largest memtable, as a percentage of *either* on- or off-heap memory limits, and immediately
     * queues it for flushing. If the memtable selected is flushed before this completes, no work is done.
     */
    public static Future<Boolean> flushLargestMemtable()
    {
        float largestRatio = 0f;
        AbstractAllocatorMemtable largestMemtable = null;
        Memtable.MemoryUsage largestUsage = null;
        float liveOnHeap = 0, liveOffHeap = 0;
        // we take a reference to the current main memtable for the CF prior to snapping its ownership ratios
        // to ensure we have some ordering guarantee for performing the switchMemtableIf(), i.e. we will only
        // swap if the memtables we are measuring here haven't already been swapped by the time we try to swap them
        for (Memtable currentMemtable : ColumnFamilyStore.activeMemtables())
        {
            if (!(currentMemtable instanceof AbstractAllocatorMemtable))
                continue;
            AbstractAllocatorMemtable current = (AbstractAllocatorMemtable) currentMemtable;

            // find the total ownership ratio for the memtable and all SecondaryIndexes owned by this CF,
            // both on- and off-heap, and select the largest of the two ratios to weight this CF
            MemoryUsage usage = Memtable.newMemoryUsage();
            current.addMemoryUsageTo(usage);

            for (Memtable indexMemtable : current.owner.getIndexMemtables())
                if (indexMemtable instanceof AbstractAllocatorMemtable)
                    indexMemtable.addMemoryUsageTo(usage);

            float ratio = Math.max(usage.ownershipRatioOnHeap, usage.ownershipRatioOffHeap);
            if (ratio > largestRatio)
            {
                largestMemtable = current;
                largestUsage = usage;
                largestRatio = ratio;
            }

            liveOnHeap += usage.ownershipRatioOnHeap;
            liveOffHeap += usage.ownershipRatioOffHeap;
        }

        Promise<Boolean> returnFuture = new AsyncPromise<>();

        if (largestMemtable != null)
        {
            float usedOnHeap = MEMORY_POOL.onHeap.usedRatio();
            float usedOffHeap = MEMORY_POOL.offHeap.usedRatio();
            float flushingOnHeap = MEMORY_POOL.onHeap.reclaimingRatio();
            float flushingOffHeap = MEMORY_POOL.offHeap.reclaimingRatio();
            logger.info("Flushing largest {} to free up room. Used total: {}, live: {}, flushing: {}, this: {}",
                        largestMemtable.owner, ratio(usedOnHeap, usedOffHeap), ratio(liveOnHeap, liveOffHeap),
                        ratio(flushingOnHeap, flushingOffHeap), ratio(largestUsage.ownershipRatioOnHeap, largestUsage.ownershipRatioOffHeap));

            Future<CommitLogPosition> flushFuture = largestMemtable.owner.signalFlushRequired(largestMemtable, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
            flushFuture.addListener(() -> {
                try
                {
                    flushFuture.get();
                    returnFuture.trySuccess(true);
                }
                catch (Throwable t)
                {
                    returnFuture.tryFailure(t);
                }
            }, ImmediateExecutor.INSTANCE);
        }
        else
        {
            logger.debug("Flushing of largest memtable, not done, no memtable found");

            returnFuture.trySuccess(false);
        }

        return returnFuture;
    }

    private static String ratio(float onHeap, float offHeap)
    {
        return String.format("%.2f/%.2f", onHeap, offHeap);
    }
}
