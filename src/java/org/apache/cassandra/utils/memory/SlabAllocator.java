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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;
import sun.nio.ch.DirectBuffer;

/**
+ * The SlabAllocator is a bump-the-pointer allocator that allocates
+ * large (1MB) global regions and then doles them out to threads that
+ * request smaller sized (up to 128kb) slices into the array.
 * <p></p>
 * The purpose of this class is to combat heap fragmentation in long lived
 * objects: by ensuring that all allocations with similar lifetimes
 * only to large regions of contiguous memory, we ensure that large blocks
 * get freed up at the same time.
 * <p></p>
 * Otherwise, variable length byte arrays allocated end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 */
public class SlabAllocator extends MemtableBufferAllocator
{
    private static final Logger logger = LoggerFactory.getLogger(SlabAllocator.class);

    private final static int REGION_SIZE = 1024 * 1024;
    private final static int MAX_CLONED_SIZE = 128 * 1024; // bigger than this don't go in the region

    // globally stash any Regions we allocate but are beaten to using, and use these up before allocating any more
    private static final ConcurrentLinkedQueue<Region> RACE_ALLOCATED = new ConcurrentLinkedQueue<>();

    private final AtomicReference<Region> currentRegion = new AtomicReference<>();
    private final AtomicInteger regionCount = new AtomicInteger(0);

    // this queue is used to keep references to off-heap allocated regions so that we can free them when we are discarded
    private final ConcurrentLinkedQueue<Region> offHeapRegions = new ConcurrentLinkedQueue<>();
    private final AtomicLong unslabbedSize = new AtomicLong(0);
    private final boolean allocateOnHeapOnly;
    private final EnsureOnHeap ensureOnHeap;

    SlabAllocator(SubAllocator onHeap, SubAllocator offHeap, boolean allocateOnHeapOnly)
    {
        super(onHeap, offHeap);
        this.allocateOnHeapOnly = allocateOnHeapOnly;
        this.ensureOnHeap = allocateOnHeapOnly ? new EnsureOnHeap.NoOp() : new EnsureOnHeap.CloneToHeap();
    }

    public EnsureOnHeap ensureOnHeap()
    {
        return ensureOnHeap;
    }

    public ByteBuffer allocate(int size)
    {
        return allocate(size, null);
    }

    public ByteBuffer allocate(int size, OpOrder.Group opGroup)
    {
        assert size >= 0;
        if (size == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        (allocateOnHeapOnly ? onHeap() : offHeap()).allocate(size, opGroup);
        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (size > MAX_CLONED_SIZE)
        {
            unslabbedSize.addAndGet(size);
            if (allocateOnHeapOnly)
                return ByteBuffer.allocate(size);
            Region region = new Region(ByteBuffer.allocateDirect(size));
            offHeapRegions.add(region);
            return region.allocate(size);
        }

        while (true)
        {
            Region region = getRegion();

            // Try to allocate from this region
            ByteBuffer cloned = region.allocate(size);
            if (cloned != null)
                return cloned;

            // not enough space!
            currentRegion.compareAndSet(region, null);
        }
    }

    public void setDiscarded()
    {
        for (Region region : offHeapRegions)
            ((DirectBuffer) region.data).cleaner().clean();
        super.setDiscarded();
    }

    /**
     * Get the current region, or, if there is no current region, allocate a new one
     */
    private Region getRegion()
    {
        while (true)
        {
            // Try to get the region
            Region region = currentRegion.get();
            if (region != null)
                return region;

            // No current region, so we want to allocate one. We race
            // against other allocators to CAS in a Region, and if we fail we stash the region for re-use
            region = RACE_ALLOCATED.poll();
            if (region == null)
                region = new Region(allocateOnHeapOnly ? ByteBuffer.allocate(REGION_SIZE) : ByteBuffer.allocateDirect(REGION_SIZE));
            if (currentRegion.compareAndSet(null, region))
            {
                if (!allocateOnHeapOnly)
                    offHeapRegions.add(region);
                regionCount.incrementAndGet();
                logger.trace("{} regions now allocated in {}", regionCount, this);
                return region;
            }

            // someone else won race - that's fine, we'll try to grab theirs
            // in the next iteration of the loop.
            RACE_ALLOCATED.add(region);
        }
    }

    protected AbstractAllocator allocator(OpOrder.Group writeOp)
    {
        return new ContextAllocator(writeOp, this);
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
        private final ByteBuffer data;

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
         * @param buffer bytes
         */
        private Region(ByteBuffer buffer)
        {
            data = buffer;
        }

        /**
         * Try to allocate <code>size</code> bytes from the region.
         *
         * @return the successful allocation, or null to indicate not-enough-space
         */
        public ByteBuffer allocate(int size)
        {
            while (true)
            {
                int oldOffset = nextFreeOffset.get();

                if (oldOffset + size > data.capacity()) // capacity == remaining
                    return null;

                // Try to atomically claim this region
                if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size))
                {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    return (ByteBuffer) data.duplicate().position(oldOffset).limit(oldOffset + size);
                }
                // we raced and lost alloc, try again
            }
        }

        @Override
        public String toString()
        {
            return "Region@" + System.identityHashCode(this) +
                   " allocs=" + allocCount.get() + "waste=" +
                   (data.capacity() - nextFreeOffset.get());
        }
    }
}
