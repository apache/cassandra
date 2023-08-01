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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public abstract class MemtableAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableAllocator.class);

    private final SubAllocator onHeap;
    private final SubAllocator offHeap;

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

    public abstract EnsureOnHeap ensureOnHeap();

    public abstract Cloner cloner(OpOrder.Group opGroup);

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
        onHeap.setDiscarding();
        offHeap.setDiscarding();
    }

    /**
     * Indicate the memory and resources owned by this allocator are no longer referenced,
     * and can be reclaimed/reused.
     */
    public void setDiscarded()
    {
        onHeap.setDiscarded();
        offHeap.setDiscarded();
    }

    public boolean isLive()
    {
        return onHeap.state == LifeCycle.LIVE || offHeap.state == LifeCycle.LIVE;
    }

    /** Mark the BB as unused, permitting it to be reclaimed */
    public static class SubAllocator
    {
        // the tracker we are owning memory from
        private final MemtablePool.SubPool parent;

        // the state of the memtable
        private volatile LifeCycle state;

        // the amount of memory/resource owned by this object
        private volatile long owns;
        // the amount of memory we are reporting to collect; this may be inaccurate, but is close
        // and is used only to ensure that once we have reclaimed we mark the tracker with the same amount
        private volatile long reclaiming;

        SubAllocator(MemtablePool.SubPool parent)
        {
            this.parent = parent;
            this.state = LifeCycle.LIVE;
        }

        /**
         * Mark this allocator reclaiming; this will permit any outstanding allocations to temporarily
         * overshoot the maximum memory limit so that flushing can begin immediately
         */
        void setDiscarding()
        {
            state = state.transition(LifeCycle.DISCARDING);
            // mark the memory owned by this allocator as reclaiming
            updateReclaiming();
        }

        /**
         * Indicate the memory and resources owned by this allocator are no longer referenced,
         * and can be reclaimed/reused.
         */
        void setDiscarded()
        {
            state = state.transition(LifeCycle.DISCARDED);
            // release any memory owned by this allocator; automatically signals waiters
            releaseAll();
        }

        /**
         * Should only be called once we know we will never allocate to the object again.
         * currently no corroboration/enforcement of this is performed.
         */
        void releaseAll()
        {
            parent.released(ownsUpdater.getAndSet(this, 0));
            parent.reclaimed(reclaimingUpdater.getAndSet(this, 0));
        }

        /**
         * Like allocate, but permits allocations to be negative.
         */
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
                if (opGroup.isBlocking())
                {
                    allocated(size);
                    return;
                }
                WaitQueue.Signal signal = parent.hasRoom().register(parent.blockedTimerContext(), Timer.Context::stop);
                opGroup.notifyIfBlocking(signal);
                boolean allocated = parent.tryAllocate(size);
                if (allocated)
                {
                    signal.cancel();
                    acquired(size);
                    return;
                }
                else
                    signal.awaitThrowUncheckedOnInterrupt();
            }
        }

        /**
         * Retroactively mark an amount allocated and acquired in the tracker, and owned by us. If the state is discarding,
         * then also update reclaiming since the flush operation is waiting at the barrier for in-flight writes,
         * and it will flush this memory too.
         */
        private void allocated(long size)
        {
            parent.allocated(size);
            ownsUpdater.addAndGet(this, size);

            if (state == LifeCycle.DISCARDING)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Allocated {} bytes whilst discarding", size);
                updateReclaiming();
            }
        }

        /**
         * Retroactively mark an amount acquired in the tracker, and owned by us. If the state is discarding,
         * then also update reclaiming since the flush operation is waiting at the barrier for in-flight writes,
         * and it will flush this memory too.
         */
        private void acquired(long size)
        {
            parent.acquired();
            ownsUpdater.addAndGet(this, size);

            if (state == LifeCycle.DISCARDING)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Allocated {} bytes whilst discarding", size);
                updateReclaiming();
            }
        }

        /**
         * If the state is still live, then we update the memory we own here and in the parent.
         *
         * However, if the state is not live, we do not update it because we would have to update
         * reclaiming too, and it could cause problems to the memtable cleaner algorithm if reclaiming
         * decreased. If the memtable is flushing, soon enough {@link this#releaseAll()} will be called.
         *
         * @param size the size that was released
         */
        void released(long size)
        {
            if (state == LifeCycle.LIVE)
            {
                parent.released(size);
                ownsUpdater.addAndGet(this, -size);
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Tried to release {} bytes whilst discarding", size);
            }
        }

        /**
         * Mark what we currently own as reclaiming, both here and in our parent.
         * This method is called for the first time when the memtable is scheduled for flushing,
         * in which case reclaiming will be zero and we mark everything that we own as reclaiming.
         * Afterwards, if there are in flight writes that have not completed yet, we also mark any
         * more memory that is allocated by these writes as reclaiming, since the memtable is waiting
         * on the barrier for these writes to complete, before it can actually start flushing data.
         */
        void updateReclaiming()
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

        public long getReclaiming()
        {
            return reclaiming;
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
