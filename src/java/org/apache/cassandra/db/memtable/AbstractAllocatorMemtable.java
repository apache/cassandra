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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableCleaner;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;

/**
 * A memtable that uses memory tracked and maybe allocated via a MemtableAllocator from a MemtablePool.
 * Provides methods of memory tracking and triggering flushes when the relevant limits are reached.
 */
public abstract class AbstractAllocatorMemtable extends AbstractMemtableWithCommitlog
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractAllocatorMemtable.class);

    public static final MemtablePool MEMORY_POOL = AbstractAllocatorMemtable.createMemtableAllocatorPool();

    protected final Owner owner;
    protected final MemtableAllocator allocator;

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    private final long creationNano = System.nanoTime();

    private static MemtablePool createMemtableAllocatorPool()
    {
        Config.MemtableAllocationType allocationType = DatabaseDescriptor.getMemtableAllocationType();
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
        float memtableCleanupThreshold = DatabaseDescriptor.getMemtableCleanupThreshold();
        MemtableCleaner cleaner = AbstractAllocatorMemtable::flushLargestMemtable;
        return createMemtableAllocatorPool(allocationType, heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
    }

    @VisibleForTesting
    public static MemtablePool createMemtableAllocatorPool(Config.MemtableAllocationType allocationType, 
                                                           long heapLimit, long offHeapLimit, 
                                                           float memtableCleanupThreshold, MemtableCleaner cleaner)
    {
        switch (allocationType)
        {
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
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = metadata.get().comparator;
        this.owner = owner;
        scheduleFlush();
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        switch (reason)
        {
        case SCHEMA_CHANGE:
            return initialComparator != metadata().comparator // If the CF comparator has changed, because our partitions reference the old one
                   || metadata().params.memtable.factory != factory(); // If a different type of memtable is requested
        default:
            return true;
        }
    }

    public void metadataUpdated()
    {
        scheduleFlush();
    }

    public void performSnapshot(String snapshotName)
    {
        // unless shouldSwitch(SNAPSHOT) returns false, this cannot be called.
        throw new AssertionError();
    }

    protected abstract Factory factory();

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
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             metadata.get().name,
                             hashCode(),
                             FBUtilities.prettyPrintMemory(liveDataSize.get()),
                             currentOperations,
                             100 * allocator.onHeap().ownershipRatio(),
                             100 * allocator.offHeap().ownershipRatio());
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1024L * 1024 * 1024 * 1024 * 1024);
    }

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
        ScheduledExecutors.scheduledTasks.schedule(runnable, period, TimeUnit.MILLISECONDS);
    }

    private void flushIfPeriodExpired()
    {
        int period = metadata().params.memtableFlushPeriodInMs;
        if (period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period)))
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
    public static CompletableFuture<Boolean> flushLargestMemtable()
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

        CompletableFuture<Boolean> returnFuture = new CompletableFuture<>();

        if (largestMemtable != null)
        {
            float usedOnHeap = MEMORY_POOL.onHeap.usedRatio();
            float usedOffHeap = MEMORY_POOL.offHeap.usedRatio();
            float flushingOnHeap = MEMORY_POOL.onHeap.reclaimingRatio();
            float flushingOffHeap = MEMORY_POOL.offHeap.reclaimingRatio();
            logger.debug("Flushing largest {} to free up room. Used total: {}, live: {}, flushing: {}, this: {}",
                         largestMemtable.owner, ratio(usedOnHeap, usedOffHeap), ratio(liveOnHeap, liveOffHeap),
                         ratio(flushingOnHeap, flushingOffHeap), ratio(largestUsage.ownershipRatioOnHeap, largestUsage.ownershipRatioOffHeap));

            ListenableFuture<CommitLogPosition> flushFuture = largestMemtable.owner.signalFlushRequired(largestMemtable, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
            flushFuture.addListener(() -> {
                try
                {
                    flushFuture.get();
                    returnFuture.complete(true);
                }
                catch (Throwable t)
                {
                    returnFuture.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        }
        else
        {
            logger.debug("Flushing of largest memtable, not done, no memtable found");

            returnFuture.complete(false);
        }

        return returnFuture;
    }

    private static String ratio(float onHeap, float offHeap)
    {
        return String.format("%.2f/%.2f", onHeap, offHeap);
    }
}
