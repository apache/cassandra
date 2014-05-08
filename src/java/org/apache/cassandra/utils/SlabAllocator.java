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
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SlabAllocator is a bump-the-pointer allocator that allocates
 * large (2MB by default) regions and then doles them out to threads that request
 * slices into the array.
 * <p/>
 * The purpose of this class is to combat heap fragmentation in long lived
 * objects: by ensuring that all allocations with similar lifetimes
 * only to large regions of contiguous memory, we ensure that large blocks
 * get freed up at the same time.
 * <p/>
 * Otherwise, variable length byte arrays allocated end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 */
public class SlabAllocator extends Allocator
{
    private static final Logger logger = LoggerFactory.getLogger(SlabAllocator.class);

    private final static int REGION_SIZE = 1024 * 1024;
    private final static int MAX_CLONED_SIZE = 128 * 1024; // bigger than this don't go in the region

    // globally stash any Regions we allocate but are beaten to using, and use these up before allocating any more
    private static final ConcurrentLinkedQueue<Region> RACE_ALLOCATED = new ConcurrentLinkedQueue<>();

    private final AtomicReference<Region> currentRegion = new AtomicReference<Region>();
    private final AtomicInteger regionCount = new AtomicInteger(0);
    private AtomicLong unslabbed = new AtomicLong(0);

    public ByteBuffer allocate(int size)
    {
        assert size >= 0;
        if (size == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (size > MAX_CLONED_SIZE)
        {
            unslabbed.addAndGet(size);
            return ByteBuffer.allocate(size);
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
                region = new Region(REGION_SIZE);
            if (currentRegion.compareAndSet(null, region))
            {
                regionCount.incrementAndGet();
                logger.trace("{} regions now allocated in {}", regionCount, this);
                return region;
            }

            // someone else won race - that's fine, we'll try to grab theirs
            // in the next iteration of the loop.
            RACE_ALLOCATED.add(region);
        }
    }

    /**
     * @return a lower bound on how much space has been allocated
     */
    public long getMinimumSize()
    {
        return unslabbed.get() + (regionCount.get() - 1) * (long)REGION_SIZE;
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
         * @param size in bytes
         */
        private Region(int size)
        {
            data = ByteBuffer.allocate(size);
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
