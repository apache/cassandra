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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public abstract class MemtableAllocator
{
    private final SubAllocator onHeap;
    private final SubAllocator offHeap;
    volatile LifeCycle state = LifeCycle.LIVE;

    static enum LifeCycle
    {
        LIVE, DISCARDING, DISCARDED;
        LifeCycle transition(LifeCycle targetState)
        {
            switch (targetState)
            {
                case DISCARDING:
                    assert this == LifeCycle.LIVE;
                    return LifeCycle.DISCARDING;

                case DISCARDED:
                    assert this == LifeCycle.DISCARDING;
                    return LifeCycle.DISCARDED;

                default:
                    throw new IllegalStateException();
            }
        }
    }

    MemtableAllocator(SubAllocator onHeap, SubAllocator offHeap)
    {
        this.onHeap = onHeap;
        this.offHeap = offHeap;
    }

    public abstract Cell clone(Cell cell, CFMetaData cfm, OpOrder.Group writeOp);
    public abstract CounterCell clone(CounterCell cell, CFMetaData cfm, OpOrder.Group writeOp);
    public abstract DeletedCell clone(DeletedCell cell, CFMetaData cfm, OpOrder.Group writeOp);
    public abstract ExpiringCell clone(ExpiringCell cell, CFMetaData cfm, OpOrder.Group writeOp);
    public abstract DecoratedKey clone(DecoratedKey key, OpOrder.Group opGroup);
    public abstract DataReclaimer reclaimer();

    public SubAllocator onHeap()
    {
        return onHeap;
    }

    public SubAllocator offHeap()
    {
        return offHeap;
    }

    /**
     * Mark this allocator reclaiming; this will permit any outstanding allocations to temporarily
     * overshoot the maximum memory limit so that flushing can begin immediately
     */
    public void setDiscarding()
    {
        state = state.transition(LifeCycle.DISCARDING);
        // mark the memory owned by this allocator as reclaiming
        onHeap.markAllReclaiming();
        offHeap.markAllReclaiming();
    }

    /**
     * Indicate the memory and resources owned by this allocator are no longer referenced,
     * and can be reclaimed/reused.
     */
    public void setDiscarded()
    {
        state = state.transition(LifeCycle.DISCARDED);
        // release any memory owned by this allocator; automatically signals waiters
        onHeap.releaseAll();
        offHeap.releaseAll();
    }

    public boolean isLive()
    {
        return state == LifeCycle.LIVE;
    }

    public static interface DataReclaimer
    {
        public DataReclaimer reclaim(Cell cell);
        public DataReclaimer reclaimImmediately(Cell cell);
        public DataReclaimer reclaimImmediately(DecoratedKey key);
        public void cancel();
        public void commit();
    }

    public static final DataReclaimer NO_OP = new DataReclaimer()
    {
        public DataReclaimer reclaim(Cell cell)
        {
            return this;
        }

        public DataReclaimer reclaimImmediately(Cell cell)
        {
            return this;
        }

        public DataReclaimer reclaimImmediately(DecoratedKey key)
        {
            return this;
        }

        @Override
        public void cancel()
        {}

        @Override
        public void commit()
        {}
    };

    /** Mark the BB as unused, permitting it to be reclaimed */
    public static final class SubAllocator
    {
        // the tracker we are owning memory from
        private final MemtablePool.SubPool parent;

        // the amount of memory/resource owned by this object
        private volatile long owns;
        // the amount of memory we are reporting to collect; this may be inaccurate, but is close
        // and is used only to ensure that once we have reclaimed we mark the tracker with the same amount
        private volatile long reclaiming;

        SubAllocator(MemtablePool.SubPool parent)
        {
            this.parent = parent;
        }

        // should only be called once we know we will never allocate to the object again.
        // currently no corroboration/enforcement of this is performed.
        void releaseAll()
        {
            parent.adjustAcquired(-ownsUpdater.getAndSet(this, 0), false);
            parent.adjustReclaiming(-reclaimingUpdater.getAndSet(this, 0));
        }

        // allocate memory in the tracker, and mark ourselves as owning it
        public void allocate(long size, OpOrder.Group opGroup)
        {
            while (true)
            {
                if (parent.tryAllocate(size))
                {
                    acquired(size);
                    return;
                }
                WaitQueue.Signal signal = opGroup.isBlockingSignal(parent.hasRoom().register());
                boolean allocated = parent.tryAllocate(size);
                if (allocated || opGroup.isBlocking())
                {
                    signal.cancel();
                    if (allocated) // if we allocated, take ownership
                        acquired(size);
                    else // otherwise we're blocking so we're permitted to overshoot our constraints, to just allocate without blocking
                        allocated(size);
                    return;
                }
                else
                    signal.awaitUninterruptibly();
            }
        }

        // retroactively mark an amount allocated amd acquired in the tracker, and owned by us
        void allocated(long size)
        {
            parent.adjustAcquired(size, true);
            ownsUpdater.addAndGet(this, size);
        }

        // retroactively mark an amount acquired in the tracker, and owned by us
        void acquired(long size)
        {
            parent.adjustAcquired(size, false);
            ownsUpdater.addAndGet(this, size);
        }

        void release(long size)
        {
            parent.adjustAcquired(-size, false);
            ownsUpdater.addAndGet(this, -size);
        }

        // mark everything we currently own as reclaiming, both here and in our parent
        void markAllReclaiming()
        {
            while (true)
            {
                long cur = owns;
                long prev = reclaiming;
                if (reclaimingUpdater.compareAndSet(this, prev, cur))
                {
                    parent.adjustReclaiming(cur - prev);
                    return;
                }
            }
        }

        public long owns()
        {
            return owns;
        }

        public float ownershipRatio()
        {
            float r = owns / (float) parent.limit;
            if (Float.isNaN(r))
                return 0;
            return r;
        }

        private static final AtomicLongFieldUpdater<SubAllocator> ownsUpdater = AtomicLongFieldUpdater.newUpdater(SubAllocator.class, "owns");
        private static final AtomicLongFieldUpdater<SubAllocator> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(SubAllocator.class, "reclaiming");
    }

}
