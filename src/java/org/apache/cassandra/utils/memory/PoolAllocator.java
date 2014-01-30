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
package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PoolAllocator<P extends Pool> extends AbstractAllocator
{
    public final P pool;
    volatile LifeCycle state = LifeCycle.LIVE;

    static enum LifeCycle
    {
        LIVE, DISCARDING, DISCARDED;
        LifeCycle transition(LifeCycle target)
        {
            assert target.ordinal() == ordinal() + 1;
            return target;
        }
    }

    // the amount of memory/resource owned by this object
    private AtomicLong owns = new AtomicLong();
    // the amount of memory we are reporting to collect; this may be inaccurate, but is close
    // and is used only to ensure that once we have reclaimed we mark the tracker with the same amount
    private AtomicLong reclaiming = new AtomicLong();

    PoolAllocator(P pool)
    {
        this.pool = pool;
    }

    /**
     * Mark this allocator as reclaiming; this will mark the memory it owns as reclaiming, so remove it from
     * any calculation deciding if further cleaning/reclamation is necessary.
     */
    public void setDiscarding()
    {
        state = state.transition(LifeCycle.DISCARDING);
        // mark the memory owned by this allocator as reclaiming
        long prev = reclaiming.get();
        long cur = owns.get();
        reclaiming.set(cur);
        pool.adjustReclaiming(cur - prev);
    }

    /**
     * Indicate the memory and resources owned by this allocator are no longer referenced,
     * and can be reclaimed/reused.
     */
    public void setDiscarded()
    {
        state = state.transition(LifeCycle.DISCARDED);
        // release any memory owned by this allocator; automatically signals waiters
        pool.release(owns.getAndSet(0));
        pool.adjustReclaiming(-reclaiming.get());
    }

    public abstract ByteBuffer allocate(int size, OpOrder.Group opGroup);

    /** Mark the BB as unused, permitting it to be reclaimed */
    public abstract void free(ByteBuffer name);

    // mark ourselves as owning memory from the tracker.  meant to be called by subclass
    // allocate method that actually allocates and returns a ByteBuffer
    protected void markAllocated(int size, OpOrder.Group opGroup)
    {
        while (true)
        {
            if (pool.tryAllocate(size))
            {
                acquired(size);
                return;
            }
            WaitQueue.Signal signal = opGroup.isBlockingSignal(pool.hasRoom.register());
            boolean allocated = pool.tryAllocate(size);
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

    // retroactively mark (by-passes any constraints) an amount allocated in the tracker, and owned by us.
    private void allocated(int size)
    {
        pool.adjustAllocated(size);
        owns.addAndGet(size);
    }

    // retroactively mark (by-passes any constraints) an amount owned by us
    private void acquired(int size)
    {
        owns.addAndGet(size);
    }

    // release an amount of memory from our ownership, and deallocate it in the tracker
    void release(int size)
    {
        pool.release(size);
        owns.addAndGet(-size);
    }

    public boolean isLive()
    {
        return state == LifeCycle.LIVE;
    }

    /**
     * Allocate a slice of the given length.
     */
    public ByteBuffer clone(ByteBuffer buffer, OpOrder.Group opGroup)
    {
        assert buffer != null;
        if (buffer.remaining() == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBuffer cloned = allocate(buffer.remaining(), opGroup);

        cloned.mark();
        cloned.put(buffer.duplicate());
        cloned.reset();
        return cloned;
    }

    public ContextAllocator wrap(OpOrder.Group opGroup, ColumnFamilyStore cfs)
    {
        return new ContextAllocator(opGroup, this, cfs);
    }

    @Override
    public long owns()
    {
        return owns.get();
    }

    @Override
    public float ownershipRatio()
    {
        return owns.get() / (float) pool.limit;
    }

    @Override
    public long reclaiming()
    {
        return reclaiming.get();
    }
}