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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class NativeAllocator extends MemtableAllocator
{
    private final static int MAX_REGION_SIZE = 1 * 1024 * 1024;
    private final static int MAX_CLONED_SIZE = 128 * 1024; // bigger than this don't go in the region
    private final static int MIN_REGION_SIZE = 8 * 1024;

    // globally stash any Regions we allocate but are beaten to using, and use these up before allocating any more
    private static final Map<Integer, RaceAllocated> RACE_ALLOCATED = new HashMap<>();

    static
    {
        for(int i = MIN_REGION_SIZE ; i <= MAX_REGION_SIZE; i *= 2)
            RACE_ALLOCATED.put(i, new RaceAllocated());
    }

    private final AtomicReference<Region> currentRegion = new AtomicReference<>();
    private final ConcurrentLinkedQueue<Region> regions = new ConcurrentLinkedQueue<>();
    private final EnsureOnHeap.CloneToHeap cloneToHeap = new EnsureOnHeap.CloneToHeap();

    protected NativeAllocator(NativePool pool)
    {
        super(pool.onHeap.newAllocator(), pool.offHeap.newAllocator());
    }

    private static class CloningBTreeRowBuilder extends BTreeRow.Builder
    {
        final OpOrder.Group writeOp;
        final NativeAllocator allocator;
        private CloningBTreeRowBuilder(OpOrder.Group writeOp, NativeAllocator allocator)
        {
            super(true);
            this.writeOp = writeOp;
            this.allocator = allocator;
        }

        @Override
        public void newRow(Clustering clustering)
        {
            if (clustering != Clustering.STATIC_CLUSTERING)
                clustering = new NativeClustering(allocator, writeOp, clustering);
            super.newRow(clustering);
        }

        @Override
        public void addCell(Cell cell)
        {
            super.addCell(new NativeCell(allocator, writeOp, cell));
        }
    }

    public Row.Builder rowBuilder(OpOrder.Group opGroup)
    {
        return new CloningBTreeRowBuilder(opGroup, this);
    }

    public DecoratedKey clone(DecoratedKey key, OpOrder.Group writeOp)
    {
        return new NativeDecoratedKey(key.getToken(), this, writeOp, key.getKey());
    }

    @Override
    public MemtableAllocator.DataReclaimer reclaimer()
    {
        return NO_OP;
    }

    public EnsureOnHeap ensureOnHeap()
    {
        return cloneToHeap;
    }

    public long allocate(int size, OpOrder.Group opGroup)
    {
        assert size >= 0;
        offHeap().allocate(size, opGroup);
        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (size > MAX_CLONED_SIZE)
            return allocateOversize(size);

        while (true)
        {
            Region region = currentRegion.get();
            long peer;
            if (region != null && (peer = region.allocate(size)) > 0)
                return peer;

            trySwapRegion(region, size);
        }
    }

    private void trySwapRegion(Region current, int minSize)
    {
        // decide how big we want the new region to be:
        //  * if there is no prior region, we set it to min size
        //  * otherwise we double its size; if it's too small to fit the allocation, we round it up to 4-8x its size
        int size;
        if (current == null) size = MIN_REGION_SIZE;
        else size = current.capacity * 2;
        if (minSize > size)
            size = Integer.highestOneBit(minSize) << 3;
        size = Math.min(MAX_REGION_SIZE, size);

        // first we try and repurpose a previously allocated region
        RaceAllocated raceAllocated = RACE_ALLOCATED.get(size);
        Region next = raceAllocated.poll();

        // if there are none, we allocate one
        if (next == null)
            next = new Region(MemoryUtil.allocate(size), size);

        // we try to swap in the region we've obtained;
        // if we fail to swap the region, we try to stash it for repurposing later; if we're out of stash room, we free it
        if (currentRegion.compareAndSet(current, next))
            regions.add(next);
        else if (!raceAllocated.stash(next))
            MemoryUtil.free(next.peer);
    }

    private long allocateOversize(int size)
    {
        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        Region region = new Region(MemoryUtil.allocate(size), size);
        regions.add(region);

        long peer;
        if ((peer = region.allocate(size)) == -1)
            throw new AssertionError();

        return peer;
    }

    public void setDiscarded()
    {
        for (Region region : regions)
            MemoryUtil.free(region.peer);

        super.setDiscarded();
    }

    // used to ensure we don't keep loads of race allocated regions around indefinitely. keeps the total bound on wasted memory low.
    private static class RaceAllocated
    {
        final ConcurrentLinkedQueue<Region> stash = new ConcurrentLinkedQueue<>();
        final Semaphore permits = new Semaphore(8);
        boolean stash(Region region)
        {
            if (!permits.tryAcquire())
                return false;
            stash.add(region);
            return true;
        }
        Region poll()
        {
            Region next = stash.poll();
            if (next != null)
                permits.release();
            return next;
        }
    }

    /**
     * A region of memory out of which allocations are sliced.
     *
     * This serves two purposes:
     *  - to provide a step between initialization and allocation, so that racing to CAS a
     *    new region in is harmless
     *  - encapsulates the allocation offset
     */
    private static class Region
    {
        /**
         * Actual underlying data
         */
        private final long peer;

        private final int capacity;

        /**
         * Offset for the next allocation, or the sentinel value -1
         * which implies that the region is still uninitialized.
         */
        private final AtomicInteger nextFreeOffset = new AtomicInteger(0);

        /**
         * Total number of allocations satisfied from this buffer
         */
        private final AtomicInteger allocCount = new AtomicInteger();

        /**
         * Create an uninitialized region. Note that memory is not allocated yet, so
         * this is cheap.
         *
         * @param peer peer
         */
        private Region(long peer, int capacity)
        {
            this.peer = peer;
            this.capacity = capacity;
        }

        /**
         * Try to allocate <code>size</code> bytes from the region.
         *
         * @return the successful allocation, or null to indicate not-enough-space
         */
        long allocate(int size)
        {
            while (true)
            {
                int oldOffset = nextFreeOffset.get();

                if (oldOffset + size > capacity) // capacity == remaining
                    return -1;

                // Try to atomically claim this region
                if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size))
                {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    return peer + oldOffset;
                }
                // we raced and lost alloc, try again
            }
        }

        @Override
        public String toString()
        {
            return "Region@" + System.identityHashCode(this) +
                    " allocs=" + allocCount.get() + "waste=" +
                    (capacity - nextFreeOffset.get());
        }
    }

}
