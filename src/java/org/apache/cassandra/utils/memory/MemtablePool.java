/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.utils.concurrent.WaitQueue;


/**
 * Represents an amount of memory used for a given purpose, that can be allocated to specific tasks through
 * child MemtableAllocator objects.
 */
public abstract class MemtablePool
{
    final MemtableCleanerThread<?> cleaner;

    // the total memory used by this pool
    public final SubPool onHeap;
    public final SubPool offHeap;

    public final Timer blockedOnAllocating;

    final WaitQueue hasRoom = new WaitQueue();

    MemtablePool(long maxOnHeapMemory, long maxOffHeapMemory, float cleanThreshold, Runnable cleaner)
    {
        this.onHeap = getSubPool(maxOnHeapMemory, cleanThreshold);
        this.offHeap = getSubPool(maxOffHeapMemory, cleanThreshold);
        this.cleaner = getCleaner(cleaner);
        blockedOnAllocating = CassandraMetricsRegistry.Metrics.timer(new DefaultNameFactory("MemtablePool")
                                                                         .createMetricName("BlockedOnAllocation"));
        if (this.cleaner != null)
            this.cleaner.start();
    }

    SubPool getSubPool(long limit, float cleanThreshold)
    {
        return new SubPool(limit, cleanThreshold);
    }

    MemtableCleanerThread<?> getCleaner(Runnable cleaner)
    {
        return cleaner == null ? null : new MemtableCleanerThread<>(this, cleaner);
    }

    @VisibleForTesting
    public void shutdown() throws InterruptedException
    {
        cleaner.shutdown();
        cleaner.awaitTermination(60, TimeUnit.SECONDS);
    }

    public abstract MemtableAllocator newAllocator();

    /**
     * Note the difference between acquire() and allocate(); allocate() makes more resources available to all owners,
     * and acquire() makes shared resources unavailable but still recorded. An Owner must always acquire resources,
     * but only needs to allocate if there are none already available. This distinction is not always meaningful.
     */
    public class SubPool
    {

        // total memory/resource permitted to allocate
        public final long limit;

        // ratio of used to spare (both excluding 'reclaiming') at which to trigger a clean
        public final float cleanThreshold;

        // total bytes allocated and reclaiming
        volatile long allocated;
        volatile long reclaiming;

        // a cache of the calculation determining at what allocation threshold we should next clean
        volatile long nextClean;

        public SubPool(long limit, float cleanThreshold)
        {
            this.limit = limit;
            this.cleanThreshold = cleanThreshold;
        }

        /** Methods for tracking and triggering a clean **/

        boolean needsCleaning()
        {
            // use strictly-greater-than so we don't clean when limit is 0
            return used() > nextClean && updateNextClean();
        }

        void maybeClean()
        {
            if (needsCleaning() && cleaner != null)
                cleaner.trigger();
        }

        private boolean updateNextClean()
        {
            while (true)
            {
                long current = nextClean;
                long reclaiming = this.reclaiming;
                long next =  reclaiming + (long) (this.limit * cleanThreshold);
                if (current == next || nextCleanUpdater.compareAndSet(this, current, next))
                    return used() > next;
            }
        }

        /** Methods to allocate space **/

        boolean tryAllocate(long size)
        {
            while (true)
            {
                long cur;
                if ((cur = allocated) + size > limit)
                    return false;
                if (allocatedUpdater.compareAndSet(this, cur, cur + size))
                    return true;
            }
        }

        /**
         * apply the size adjustment to allocated, bypassing any limits or constraints. If this reduces the
         * allocated total, we will signal waiters
         */
        private void adjustAllocated(long size)
        {
            while (true)
            {
                long cur = allocated;
                if (allocatedUpdater.compareAndSet(this, cur, cur + size))
                    return;
            }
        }

        void allocated(long size)
        {
            assert size >= 0;
            if (size == 0)
                return;

            adjustAllocated(size);
            maybeClean();
        }

        void acquired(long size)
        {
            maybeClean();
        }

        void released(long size)
        {
            assert size >= 0;
            adjustAllocated(-size);
            hasRoom.signalAll();
        }

        void reclaiming(long size)
        {
            if (size == 0)
                return;
            reclaimingUpdater.addAndGet(this, size);
        }

        void reclaimed(long size)
        {
            if (size == 0)
                return;

            reclaimingUpdater.addAndGet(this, -size);
            if (updateNextClean() && cleaner != null)
                cleaner.trigger();
        }

        public long used()
        {
            return allocated;
        }

        public float reclaimingRatio()
        {
            float r = reclaiming / (float) limit;
            if (Float.isNaN(r))
                return 0;
            return r;
        }

        public float usedRatio()
        {
            float r = allocated / (float) limit;
            if (Float.isNaN(r))
                return 0;
            return r;
        }

        public MemtableAllocator.SubAllocator newAllocator()
        {
            return new MemtableAllocator.SubAllocator(this);
        }

        public WaitQueue hasRoom()
        {
            return hasRoom;
        }

        public Timer.Context blockedTimerContext()
        {
            return blockedOnAllocating.time();
        }
    }

    private static final AtomicLongFieldUpdater<SubPool> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(SubPool.class, "reclaiming");
    private static final AtomicLongFieldUpdater<SubPool> allocatedUpdater = AtomicLongFieldUpdater.newUpdater(SubPool.class, "allocated");
    private static final AtomicLongFieldUpdater<SubPool> nextCleanUpdater = AtomicLongFieldUpdater.newUpdater(SubPool.class, "nextClean");

}
