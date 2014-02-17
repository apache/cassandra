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

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Represents an amount of memory used for a given purpose, that can be allocated to specific tasks through
 * child AbstractAllocator objects. AbstractAllocator and MemoryTracker correspond approximately to PoolAllocator and Pool,
 * respectively, with the MemoryTracker bookkeeping the total shared use of resources, and the AbstractAllocator the amount
 * checked out and in use by a specific PoolAllocator.
 *
 * Note the difference between acquire() and allocate(); allocate() makes more resources available to all owners,
 * and acquire() makes shared resources unavailable but still recorded. An Owner must always acquire resources,
 * but only needs to allocate if there are none already available. This distinction is not always meaningful.
 */
public abstract class Pool
{
    // total memory/resource permitted to allocate
    public final long limit;

    // ratio of used to spare (both excluding 'reclaiming') at which to trigger a clean
    public final float cleanThreshold;

    // total bytes allocated and reclaiming
    private AtomicLong allocated = new AtomicLong();
    private AtomicLong reclaiming = new AtomicLong();

    final WaitQueue hasRoom = new WaitQueue();

    // a cache of the calculation determining at what allocation threshold we should next clean, and the cleaner we trigger
    private volatile long nextClean;
    private final PoolCleanerThread<?> cleanerThread;

    public Pool(long limit, float cleanThreshold, Runnable cleaner)
    {
        this.limit = limit;
        this.cleanThreshold = cleanThreshold;
        updateNextClean();
        cleanerThread = cleaner == null ? null : new PoolCleanerThread<>(this, cleaner);
        if (cleanerThread != null)
            cleanerThread.start();
    }

    /** Methods for tracking and triggering a clean **/

    boolean needsCleaning()
    {
        return used() >= nextClean && updateNextClean() && cleanerThread != null;
    }

    void maybeClean()
    {
        if (needsCleaning())
            cleanerThread.trigger();
    }

    private boolean updateNextClean()
    {
        long reclaiming = this.reclaiming.get();
        return used() >= (nextClean = reclaiming
                + (long) (this.limit * cleanThreshold));
    }

    /** Methods to allocate space **/

    boolean tryAllocate(int size)
    {
        while (true)
        {
            long cur;
            if ((cur = allocated.get()) + size > limit)
                return false;
            if (allocated.compareAndSet(cur, cur + size))
            {
                maybeClean();
                return true;
            }
        }
    }

    /**
     * apply the size adjustment to allocated, bypassing any limits or constraints. If this reduces the
     * allocated total, we will signal waiters
     */
    void adjustAllocated(long size)
    {
        if (size == 0)
            return;
        while (true)
        {
            long cur = allocated.get();
            if (allocated.compareAndSet(cur, cur + size))
            {
                if (size > 0)
                {
                    maybeClean();
                }
                return;
            }
        }
    }

    void release(long size)
    {
        adjustAllocated(-size);
        hasRoom.signalAll();
    }

    // space reclaimed should be released prior to calling this, to avoid triggering unnecessary cleans
    void adjustReclaiming(long reclaiming)
    {
        if (reclaiming == 0)
            return;
        this.reclaiming.addAndGet(reclaiming);
        if (reclaiming < 0 && updateNextClean() && cleanerThread != null)
            cleanerThread.trigger();
    }

    public long allocated()
    {
        return allocated.get();
    }

    public long used()
    {
        return allocated.get();
    }

    public long reclaiming()
    {
        return reclaiming.get();
    }

    public abstract PoolAllocator newAllocator(OpOrder writes);
}

