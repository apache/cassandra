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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public abstract class MemtableAllocator
{
    private final SubAllocator onHeap;
    private final SubAllocator offHeap;
    volatile LifeCycle state = LifeCycle.LIVE;

    enum LifeCycle
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

    public abstract Row.Builder rowBuilder(OpOrder.Group opGroup);
    public abstract DecoratedKey clone(DecoratedKey key, OpOrder.Group opGroup);
    public abstract EnsureOnHeap ensureOnHeap();

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
            parent.released(ownsUpdater.getAndSet(this, 0));
            parent.reclaimed(reclaimingUpdater.getAndSet(this, 0));
        }

        // like allocate, but permits allocations to be negative
        public void adjust(long size, OpOrder.Group opGroup)
        {
            if (size <= 0)
                released(-size);
            else
                allocate(size, opGroup);
        }

        // allocate memory in the tracker, and mark ourselves as owning it
        public void allocate(long size, OpOrder.Group opGroup)
        {
            assert size >= 0;

            while (true)
            {
                if (parent.tryAllocate(size))
                {
                    acquired(size);
                    return;
                }
                WaitQueue.Signal signal = opGroup.isBlockingSignal(parent.hasRoom().register(parent.blockedTimerContext()));
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

        // retroactively mark an amount allocated and acquired in the tracker, and owned by us
        private void allocated(long size)
        {
            parent.allocated(size);
            ownsUpdater.addAndGet(this, size);
        }

        // retroactively mark an amount acquired in the tracker, and owned by us
        private void acquired(long size)
        {
            parent.acquired(size);
            ownsUpdater.addAndGet(this, size);
        }

        void released(long size)
        {
            parent.released(size);
            ownsUpdater.addAndGet(this, -size);
        }

        // mark everything we currently own as reclaiming, both here and in our parent
        void markAllReclaiming()
        {
            while (true)
            {
                long cur = owns;
                long prev = reclaiming;
                if (!reclaimingUpdater.compareAndSet(this, prev, cur))
                    continue;

                parent.reclaiming(cur - prev);
                return;
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
