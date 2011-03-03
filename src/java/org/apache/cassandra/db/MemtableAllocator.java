/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A memstore-local allocation buffer.
 * <p/>
 * The MemtableAllocator is a bump-the-pointer allocator that allocates
 * large (2MB) regions and then doles it out to threads that request
 * slices into the array.
 * <p/>
 * The purpose of this class is to combat heap fragmentation in the
 * memtable: by ensuring that all name and value column data in a given memtable refer
 * only to large regions of contiguous memory, we ensure that large blocks
 * get freed up when the memtable is flushed.
 * <p/>
 * Otherwise, the byte array allocated during insertion end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 * <p/>
 * TODO: we should probably benchmark whether word-aligning the allocations
 * would provide a performance improvement - probably would speed up the
 * Bytes.toLong/Bytes.toInt calls in KeyValue, but some of those are cached
 * anyway
 */
public class MemtableAllocator
{
    private final static int REGION_SIZE = 2 * 1024 * 1024;
    private final static int MAX_CLONED_SIZE = 256 * 1024; // bigger than this don't go in the region

    private final AtomicReference<Region> currentRegion = new AtomicReference<Region>();
    private final Collection<Region> filledRegions = new LinkedBlockingQueue<Region>();

    /**
     * Allocate a slice of the given length.
     * <p/>
     * If the size is larger than the maximum size specified for this
     * allocator, returns null.
     */
    public ByteBuffer clone(ByteBuffer buffer)
    {
        assert buffer != null;

        // satisfy large allocations directly from JVM since they don't cause fragmentation
        // as badly, and fill up our regions quickly
        if (buffer.remaining() > MAX_CLONED_SIZE)
            return ByteBufferUtil.clone(buffer);

        while (true)
        {
            Region region = getRegion();

            // Try to allocate from this region
            ByteBuffer cloned = region.allocate(buffer.remaining());
            if (cloned != null)
            {
                cloned.mark();
                cloned.put(buffer.duplicate());
                cloned.reset();
                return cloned;
            }

            // not enough space!
            tryRetireRegion(region);
        }
    }

    /**
     * Try to retire the current region if it is still <code>region</code>.
     * Postcondition is that curRegion.get() != region
     */
    private void tryRetireRegion(Region region)
    {
        if (currentRegion.compareAndSet(region, null))
        {
            filledRegions.add(region);
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
            // against other allocators to CAS in an uninitialized region
            // (which is cheap to allocate)
            region = new Region(REGION_SIZE);
            if (currentRegion.compareAndSet(null, region))
            {
                // we won race - now we need to actually do the expensive allocation step
                region.init();
                return region;
            }
            // someone else won race - that's fine, we'll try to grab theirs
            // in the next iteration of the loop.
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
        private ByteBuffer data;

        private static final int UNINITIALIZED = -1;
        /**
         * Offset for the next allocation, or the sentinel value -1
         * which implies that the region is still uninitialized.
         */
        private AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

        /**
         * Total number of allocations satisfied from this buffer
         */
        private AtomicInteger allocCount = new AtomicInteger();

        /**
         * Size of region in bytes
         */
        private final int size;

        /**
         * Create an uninitialized region. Note that memory is not allocated yet, so
         * this is cheap.
         *
         * @param size in bytes
         */
        private Region(int size)
        {
            this.size = size;
        }

        /**
         * Actually claim the memory for this region. This should only be called from
         * the thread that constructed the region. It is thread-safe against other
         * threads calling alloc(), who will block until the allocation is complete.
         */
        public void init()
        {
            assert nextFreeOffset.get() == UNINITIALIZED;
            data = ByteBuffer.allocate(size);
            assert data.remaining() == data.capacity();
            // Mark that it's ready for use
            boolean initted = nextFreeOffset.compareAndSet(UNINITIALIZED, 0);
            // We should always succeed the above CAS since only one thread calls init()!
            Preconditions.checkState(initted, "Multiple threads tried to init same region");
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
                if (oldOffset == UNINITIALIZED)
                {
                    // The region doesn't have its data allocated yet.
                    // Since we found this in currentRegion, we know that whoever
                    // CAS-ed it there is allocating it right now. So spin-loop
                    // shouldn't spin long!
                    Thread.yield();
                    continue;
                }

                if (oldOffset + size > data.capacity()) // capacity == remaining
                    return null;

                // Try to atomically claim this region
                if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size))
                {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    return (ByteBuffer) data.duplicate().position(oldOffset);
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
