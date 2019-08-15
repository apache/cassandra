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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.BufferPoolMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.utils.ExecutorUtils.awaitTermination;
import static org.apache.cassandra.utils.ExecutorUtils.shutdownNow;

/**
 * A pool of ByteBuffers that can be recycled.
 */
public class BufferPool
{
    /** The size of a page aligned buffer, 64KiB */
    public static final int CHUNK_SIZE = 64 << 10;

    @VisibleForTesting
    public static long MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;

    @VisibleForTesting
    public static boolean ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = DatabaseDescriptor.getBufferPoolUseHeapIfExhausted();

    @VisibleForTesting
    public static boolean DISABLED = Boolean.parseBoolean(System.getProperty("cassandra.test.disable_buffer_pool", "false"));

    @VisibleForTesting
    public static boolean DEBUG = false;

    private static final Logger logger = LoggerFactory.getLogger(BufferPool.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    /** A global pool of chunks (page aligned buffers) */
    private static final GlobalPool globalPool = new GlobalPool();

    /** A thread local pool of chunks, where chunks come from the global pool */
    private static final FastThreadLocal<LocalPool> localPool = new FastThreadLocal<LocalPool>()
    {
        @Override
        protected LocalPool initialValue()
        {
            return new LocalPool();
        }
    };

    public static ByteBuffer get(int size)
    {
        if (DISABLED)
            return allocate(size, ALLOCATE_ON_HEAP_WHEN_EXAHUSTED);
        else
            return takeFromPool(size, ALLOCATE_ON_HEAP_WHEN_EXAHUSTED);
    }

    public static ByteBuffer get(int size, BufferType bufferType)
    {
        boolean direct = bufferType == BufferType.OFF_HEAP;
        if (DISABLED || !direct)
            return allocate(size, !direct);
        else
            return takeFromPool(size, !direct);
    }

    /** Unlike the get methods, this will return null if the pool is exhausted */
    public static ByteBuffer tryGet(int size)
    {
        if (DISABLED)
            return allocate(size, ALLOCATE_ON_HEAP_WHEN_EXAHUSTED);
        else
            return maybeTakeFromPool(size, ALLOCATE_ON_HEAP_WHEN_EXAHUSTED);
    }

    private static ByteBuffer allocate(int size, boolean onHeap)
    {
        return onHeap
               ? ByteBuffer.allocate(size)
               : ByteBuffer.allocateDirect(size);
    }

    private static ByteBuffer takeFromPool(int size, boolean allocateOnHeapWhenExhausted)
    {
        ByteBuffer ret = maybeTakeFromPool(size, allocateOnHeapWhenExhausted);
        if (ret != null)
            return ret;

        if (logger.isTraceEnabled())
            logger.trace("Requested buffer size {} has been allocated directly due to lack of capacity", FBUtilities.prettyPrintMemory(size));

        return localPool.get().allocate(size, allocateOnHeapWhenExhausted);
    }

    private static ByteBuffer maybeTakeFromPool(int size, boolean allocateOnHeapWhenExhausted)
    {
        if (size < 0)
            throw new IllegalArgumentException("Size must be positive (" + size + ")");

        if (size == 0)
            return EMPTY_BUFFER;

        if (size > CHUNK_SIZE)
        {
            if (logger.isTraceEnabled())
                logger.trace("Requested buffer size {} is bigger than {}, allocating directly",
                             FBUtilities.prettyPrintMemory(size),
                             FBUtilities.prettyPrintMemory(CHUNK_SIZE));

            return localPool.get().allocate(size, allocateOnHeapWhenExhausted);
        }

        return localPool.get().get(size);
    }

    public static void put(ByteBuffer buffer)
    {
        if (!(DISABLED || buffer.hasArray()))
            localPool.get().put(buffer);
    }

    /** This is not thread safe and should only be used for unit testing. */
    @VisibleForTesting
    static void reset()
    {
        localPool.get().reset();
        globalPool.reset();
    }

    @VisibleForTesting
    static Chunk currentChunk()
    {
        return localPool.get().chunks[0];
    }

    @VisibleForTesting
    static int numChunks()
    {
        int ret = 0;
        for (Chunk chunk : localPool.get().chunks)
        {
            if (chunk != null)
                ret++;
        }
        return ret;
    }

    @VisibleForTesting
    static void assertAllRecycled()
    {
        globalPool.debug.check();
    }

    public static long sizeInBytes()
    {
        return globalPool.sizeInBytes();
    }

    static final class Debug
    {
        long recycleRound = 1;
        final Queue<Chunk> allChunks = new ConcurrentLinkedQueue<>();
        void register(Chunk chunk)
        {
            allChunks.add(chunk);
        }
        void recycle(Chunk chunk)
        {
            chunk.lastRecycled = recycleRound;
        }
        void check()
        {
            for (Chunk chunk : allChunks)
                assert chunk.lastRecycled == recycleRound;
            recycleRound++;
        }
    }

    /**
     * A queue of page aligned buffers, the chunks, which have been sliced from bigger chunks,
     * the macro-chunks, also page aligned. Macro-chunks are allocated as long as we have not exceeded the
     * memory maximum threshold, MEMORY_USAGE_THRESHOLD and are never released.
     *
     * This class is shared by multiple thread local pools and must be thread-safe.
     */
    static final class GlobalPool
    {
        /** The size of a bigger chunk, 1-mbit, must be a multiple of CHUNK_SIZE */
        static final int MACRO_CHUNK_SIZE = 1 << 20;

        static
        {
            assert Integer.bitCount(CHUNK_SIZE) == 1; // must be a power of 2
            assert Integer.bitCount(MACRO_CHUNK_SIZE) == 1; // must be a power of 2
            assert MACRO_CHUNK_SIZE % CHUNK_SIZE == 0; // must be a multiple

            if (DISABLED)
                logger.info("Global buffer pool is disabled, allocating {}", ALLOCATE_ON_HEAP_WHEN_EXAHUSTED ? "on heap" : "off heap");
            else
                logger.info("Global buffer pool is enabled, when pool is exhausted (max is {}) it will allocate {}",
                            FBUtilities.prettyPrintMemory(MEMORY_USAGE_THRESHOLD),
                            ALLOCATE_ON_HEAP_WHEN_EXAHUSTED ? "on heap" : "off heap");
        }

        private final Debug debug = new Debug();
        private final Queue<Chunk> macroChunks = new ConcurrentLinkedQueue<>();
        // TODO (future): it would be preferable to use a CLStack to improve cache occupancy; it would also be preferable to use "CoreLocal" storage
        private final Queue<Chunk> chunks = new ConcurrentLinkedQueue<>();
        private final AtomicLong memoryUsage = new AtomicLong();

        /** Return a chunk, the caller will take owership of the parent chunk. */
        public Chunk get()
        {
            while (true)
            {
                Chunk chunk = chunks.poll();
                if (chunk != null)
                    return chunk;

                if (!allocateMoreChunks())
                    // give it one last attempt, in case someone else allocated before us
                    return chunks.poll();
            }
        }

        /**
         * This method might be called by multiple threads and that's fine if we add more
         * than one chunk at the same time as long as we don't exceed the MEMORY_USAGE_THRESHOLD.
         */
        private boolean allocateMoreChunks()
        {
            while (true)
            {
                long cur = memoryUsage.get();
                if (cur + MACRO_CHUNK_SIZE > MEMORY_USAGE_THRESHOLD)
                {
                    noSpamLogger.info("Maximum memory usage reached ({}), cannot allocate chunk of {}",
                                      MEMORY_USAGE_THRESHOLD, MACRO_CHUNK_SIZE);
                    return false;
                }
                if (memoryUsage.compareAndSet(cur, cur + MACRO_CHUNK_SIZE))
                    break;
            }

            // allocate a large chunk
            Chunk chunk;
            try
            {
                chunk = new Chunk(allocateDirectAligned(MACRO_CHUNK_SIZE));
            }
            catch (OutOfMemoryError oom)
            {
                noSpamLogger.error("Buffer pool failed to allocate chunk of {}, current size {} ({}). " +
                                   "Attempting to continue; buffers will be allocated in on-heap memory which can degrade performance. " +
                                   "Make sure direct memory size (-XX:MaxDirectMemorySize) is large enough to accommodate off-heap memtables and caches.",
                                   MACRO_CHUNK_SIZE, sizeInBytes(), oom.toString());
                return false;
            }

            chunk.acquire(null);
            macroChunks.add(chunk);
            for (int i = 0 ; i < MACRO_CHUNK_SIZE ; i += CHUNK_SIZE)
            {
                Chunk add = new Chunk(chunk.get(CHUNK_SIZE));
                chunks.add(add);
                if (DEBUG)
                    debug.register(add);
            }

            return true;
        }

        public void recycle(Chunk chunk)
        {
            chunks.add(chunk);
        }

        public long sizeInBytes()
        {
            return memoryUsage.get();
        }

        /** This is not thread safe and should only be used for unit testing. */
        @VisibleForTesting
        void reset()
        {
            while (!chunks.isEmpty())
                chunks.poll().reset();

            while (!macroChunks.isEmpty())
                macroChunks.poll().reset();

            memoryUsage.set(0);
        }
    }

    /**
     * A thread local class that grabs chunks from the global pool for this thread allocations.
     * Only one thread can do the allocations but multiple threads can release the allocations.
     */
    static final class LocalPool
    {
        private final static BufferPoolMetrics metrics = new BufferPoolMetrics();
        // a microqueue of Chunks:
        //  * if any are null, they are at the end;
        //  * new Chunks are added to the last null index
        //  * if no null indexes available, the smallest is swapped with the last index, and this replaced
        //  * this results in a queue that will typically be visited in ascending order of available space, so that
        //    small allocations preferentially slice from the Chunks with the smallest space available to furnish them
        // WARNING: if we ever change the size of this, we must update removeFromLocalQueue, and addChunk
        private final Chunk[] chunks = new Chunk[3];
        private byte chunkCount = 0;

        public LocalPool()
        {
            localPoolReferences.add(new LocalPoolRef(this, localPoolRefQueue));
        }

        private Chunk addChunkFromGlobalPool()
        {
            Chunk chunk = globalPool.get();
            if (chunk == null)
                return null;

            addChunk(chunk);
            return chunk;
        }

        private void addChunk(Chunk chunk)
        {
            chunk.acquire(this);

            if (chunkCount < 3)
            {
                chunks[chunkCount++] = chunk;
                return;
            }

            int smallestChunkIdx = 0;
            if (chunks[1].free() < chunks[0].free())
                smallestChunkIdx = 1;
            if (chunks[2].free() < chunks[smallestChunkIdx].free())
                smallestChunkIdx = 2;

            chunks[smallestChunkIdx].release();
            if (smallestChunkIdx != 2)
                chunks[smallestChunkIdx] = chunks[2];
            chunks[2] = chunk;
        }

        public ByteBuffer get(int size)
        {
            for (Chunk chunk : chunks)
            { // first see if our own chunks can serve this buffer
                if (chunk == null)
                    break;

                ByteBuffer buffer = chunk.get(size);
                if (buffer != null)
                    return buffer;
            }

            // else ask the global pool
            Chunk chunk = addChunkFromGlobalPool();
            if (chunk != null)
                return chunk.get(size);

           return null;
        }

        private ByteBuffer allocate(int size, boolean onHeap)
        {
            metrics.misses.mark();
            return BufferPool.allocate(size, onHeap);
        }

        public void put(ByteBuffer buffer)
        {
            Chunk chunk = Chunk.getParentChunk(buffer);
            if (chunk == null)
            {
                FileUtils.clean(buffer);
                return;
            }

            LocalPool owner = chunk.owner;
            // ask the free method to take exclusive ownership of the act of recycling
            // if we are either: already not owned by anyone, or owned by ourselves
            long free = chunk.free(buffer, owner == null | owner == this);
            if (free == 0L)
            {
                // 0L => we own recycling responsibility, so must recycle;
                chunk.recycle();
                // if we are also the owner, we must remove the Chunk from our local queue
                if (owner == this)
                    removeFromLocalQueue(chunk);
            }
            else if (((free == -1L) && owner != this) && chunk.owner == null)
            {
                // although we try to take recycle ownership cheaply, it is not always possible to do so if the owner is racing to unset.
                // we must also check after completely freeing if the owner has since been unset, and try to recycle
                chunk.tryRecycle();
            }
        }

        private void removeFromLocalQueue(Chunk chunk)
        {
            // since we only have three elements in the queue, it is clearer, easier and faster to just hard code the options
            if (chunks[0] == chunk)
            {   // remove first by shifting back second two
                chunks[0] = chunks[1];
                chunks[1] = chunks[2];
            }
            else if (chunks[1] == chunk)
            {   // remove second by shifting back last
                chunks[1] = chunks[2];
            }
            else assert chunks[2] == chunk;
            // whatever we do, the last element myst be null
            chunks[2] = null;
            chunkCount--;
        }

        @VisibleForTesting
        void reset()
        {
            chunkCount = 0;
            for (int i = 0; i < chunks.length; i++)
            {
                if (chunks[i] != null)
                {
                    chunks[i].owner = null;
                    chunks[i].freeSlots = 0L;
                    chunks[i].recycle();
                    chunks[i] = null;
                }
            }
        }
    }

    private static final class LocalPoolRef extends  PhantomReference<LocalPool>
    {
        private final Chunk[] chunks;
        public LocalPoolRef(LocalPool localPool, ReferenceQueue<? super LocalPool> q)
        {
            super(localPool, q);
            chunks = localPool.chunks;
        }

        public void release()
        {
            for (int i = 0 ; i < chunks.length ; i++)
            {
                if (chunks[i] != null)
                {
                    chunks[i].release();
                    chunks[i] = null;
                }
            }
        }
    }

    private static final ConcurrentLinkedQueue<LocalPoolRef> localPoolReferences = new ConcurrentLinkedQueue<>();

    private static final ReferenceQueue<Object> localPoolRefQueue = new ReferenceQueue<>();
    private static final InfiniteLoopExecutor EXEC = new InfiniteLoopExecutor("LocalPool-Cleaner", BufferPool::cleanupOneReference).start();

    private static void cleanupOneReference() throws InterruptedException
    {
        Object obj = localPoolRefQueue.remove(100);
        if (obj instanceof LocalPoolRef)
        {
            ((LocalPoolRef) obj).release();
            localPoolReferences.remove(obj);
        }
    }

    private static ByteBuffer allocateDirectAligned(int capacity)
    {
        int align = MemoryUtil.pageSize();
        if (Integer.bitCount(align) != 1)
            throw new IllegalArgumentException("Alignment must be a power of 2");

        ByteBuffer buffer = ByteBuffer.allocateDirect(capacity + align);
        long address = MemoryUtil.getAddress(buffer);
        long offset = address & (align -1); // (address % align)

        if (offset == 0)
        { // already aligned
            buffer.limit(capacity);
        }
        else
        { // shift by offset
            int pos = (int)(align - offset);
            buffer.position(pos);
            buffer.limit(pos + capacity);
        }

        return buffer.slice();
    }

    /**
     * A memory chunk: it takes a buffer (the slab) and slices it
     * into smaller buffers when requested.
     *
     * It divides the slab into 64 units and keeps a long mask, freeSlots,
     * indicating if a unit is in use or not. Each bit in freeSlots corresponds
     * to a unit, if the bit is set then the unit is free (available for allocation)
     * whilst if it is not set then the unit is in use.
     *
     * When we receive a request of a given size we round up the size to the nearest
     * multiple of allocation units required. Then we search for n consecutive free units,
     * where n is the number of units required. We also align to page boundaries.
     *
     * When we reiceve a release request we work out the position by comparing the buffer
     * address to our base address and we simply release the units.
     */
    final static class Chunk
    {
        private final ByteBuffer slab;
        private final long baseAddress;
        private final int shift;

        private volatile long freeSlots;
        private static final AtomicLongFieldUpdater<Chunk> freeSlotsUpdater = AtomicLongFieldUpdater.newUpdater(Chunk.class, "freeSlots");

        // the pool that is _currently allocating_ from this Chunk
        // if this is set, it means the chunk may not be recycled because we may still allocate from it;
        // if it has been unset the local pool has finished with it, and it may be recycled
        private volatile LocalPool owner;
        private long lastRecycled;
        private final Chunk original;

        Chunk(Chunk recycle)
        {
            assert recycle.freeSlots == 0L;
            this.slab = recycle.slab;
            this.baseAddress = recycle.baseAddress;
            this.shift = recycle.shift;
            this.freeSlots = -1L;
            this.original = recycle.original;
            if (DEBUG)
                globalPool.debug.recycle(original);
        }

        Chunk(ByteBuffer slab)
        {
            assert !slab.hasArray();
            this.slab = slab;
            this.baseAddress = MemoryUtil.getAddress(slab);

            // The number of bits by which we need to shift to obtain a unit
            // "31 &" is because numberOfTrailingZeros returns 32 when the capacity is zero
            this.shift = 31 & (Integer.numberOfTrailingZeros(slab.capacity() / 64));
            // -1 means all free whilst 0 means all in use
            this.freeSlots = slab.capacity() == 0 ? 0L : -1L;
            this.original = DEBUG ? this : null;
        }

        /**
         * Acquire the chunk for future allocations: set the owner and prep
         * the free slots mask.
         */
        void acquire(LocalPool owner)
        {
            assert this.owner == null;
            this.owner = owner;
        }

        /**
         * Set the owner to null and return the chunk to the global pool if the chunk is fully free.
         * This method must be called by the LocalPool when it is certain that
         * the local pool shall never try to allocate any more buffers from this chunk.
         */
        void release()
        {
            this.owner = null;
            tryRecycle();
        }

        void tryRecycle()
        {
            assert owner == null;
            if (isFree() && freeSlotsUpdater.compareAndSet(this, -1L, 0L))
                recycle();
        }

        void recycle()
        {
            assert freeSlots == 0L;
            globalPool.recycle(new Chunk(this));
        }

        /**
         * We stash the chunk in the attachment of a buffer
         * that was returned by get(), this method simply
         * retrives the chunk that sliced a buffer, if any.
         */
        static Chunk getParentChunk(ByteBuffer buffer)
        {
            Object attachment = MemoryUtil.getAttachment(buffer);

            if (attachment instanceof Chunk)
                return (Chunk) attachment;

            if (attachment instanceof Ref)
                return ((Ref<Chunk>) attachment).get();

            return null;
        }

        ByteBuffer setAttachment(ByteBuffer buffer)
        {
            if (Ref.DEBUG_ENABLED)
                MemoryUtil.setAttachment(buffer, new Ref<>(this, null));
            else
                MemoryUtil.setAttachment(buffer, this);

            return buffer;
        }

        boolean releaseAttachment(ByteBuffer buffer)
        {
            Object attachment = MemoryUtil.getAttachment(buffer);
            if (attachment == null)
                return false;

            if (attachment instanceof Ref)
                ((Ref<Chunk>) attachment).release();

            return true;
        }

        @VisibleForTesting
        void reset()
        {
            Chunk parent = getParentChunk(slab);
            if (parent != null)
                parent.free(slab, false);
            else
                FileUtils.clean(slab);
        }

        @VisibleForTesting
        long setFreeSlots(long val)
        {
            long ret = freeSlots;
            freeSlots = val;
            return ret;
        }

        int capacity()
        {
            return 64 << shift;
        }

        final int unit()
        {
            return 1 << shift;
        }

        final boolean isFree()
        {
            return freeSlots == -1L;
        }

        /** The total free size */
        int free()
        {
            return Long.bitCount(freeSlots) * unit();
        }

        /**
         * Return the next available slice of this size. If
         * we have exceeded the capacity we return null.
         */
        ByteBuffer get(int size)
        {
            // how many multiples of our units is the size?
            // we add (unit - 1), so that when we divide by unit (>>> shift), we effectively round up
            int slotCount = (size - 1 + unit()) >>> shift;

            // if we require more than 64 slots, we cannot possibly accommodate the allocation
            if (slotCount > 64)
                return null;

            // convert the slotCount into the bits needed in the bitmap, but at the bottom of the register
            long slotBits = -1L >>> (64 - slotCount);

            // in order that we always allocate page aligned results, we require that any allocation is "somewhat" aligned
            // i.e. any single unit allocation can go anywhere; any 2 unit allocation must begin in one of the first 3 slots
            // of a page; a 3 unit must go in the first two slots; and any four unit allocation must be fully page-aligned

            // to achieve this, we construct a searchMask that constrains the bits we find to those we permit starting
            // a match from. as we find bits, we remove them from the mask to continue our search.
            // this has an odd property when it comes to concurrent alloc/free, as we can safely skip backwards if
            // a new slot is freed up, but we always make forward progress (i.e. never check the same bits twice),
            // so running time is bounded
            long searchMask = 0x1111111111111111L;
            searchMask *= 15L >>> ((slotCount - 1) & 3);
            // i.e. switch (slotCount & 3)
            // case 1: searchMask = 0xFFFFFFFFFFFFFFFFL
            // case 2: searchMask = 0x7777777777777777L
            // case 3: searchMask = 0x3333333333333333L
            // case 0: searchMask = 0x1111111111111111L

            // truncate the mask, removing bits that have too few slots proceeding them
            searchMask &= -1L >>> (slotCount - 1);

            // this loop is very unroll friendly, and would achieve high ILP, but not clear if the compiler will exploit this.
            // right now, not worth manually exploiting, but worth noting for future
            while (true)
            {
                long cur = freeSlots;
                // find the index of the lowest set bit that also occurs in our mask (i.e. is permitted alignment, and not yet searched)
                // we take the index, rather than finding the lowest bit, since we must obtain it anyway, and shifting is more efficient
                // than multiplication
                int index = Long.numberOfTrailingZeros(cur & searchMask);

                // if no bit was actually found, we cannot serve this request, so return null.
                // due to truncating the searchMask this immediately terminates any search when we run out of indexes
                // that could accommodate the allocation, i.e. is equivalent to checking (64 - index) < slotCount
                if (index == 64)
                    return null;

                // remove this bit from our searchMask, so we don't return here next round
                searchMask ^= 1L << index;
                // if our bits occur starting at the index, remove ourselves from the bitmask and return
                long candidate = slotBits << index;
                if ((candidate & cur) == candidate)
                {
                    // here we are sure we will manage to CAS successfully without changing candidate because
                    // there is only one thread allocating at the moment, the concurrency is with the release
                    // operations only
                    while (true)
                    {
                        // clear the candidate bits (freeSlots &= ~candidate)
                        if (freeSlotsUpdater.compareAndSet(this, cur, cur & ~candidate))
                            break;

                        cur = freeSlots;
                        // make sure no other thread has cleared the candidate bits
                        assert ((candidate & cur) == candidate);
                    }
                    return get(index << shift, size);
                }
            }
        }

        private ByteBuffer get(int offset, int size)
        {
            slab.limit(offset + size);
            slab.position(offset);

            return setAttachment(slab.slice());
        }

        /**
         * Round the size to the next unit multiple.
         */
        int roundUp(int v)
        {
            return BufferPool.roundUp(v, unit());
        }

        /**
         * Release a buffer. Return:
         *    0L if the buffer must be recycled after the call;
         *   -1L if it is free (and so we should tryRecycle if owner is now null)
         *    some other value otherwise
         **/
        long free(ByteBuffer buffer, boolean tryRelease)
        {
            if (!releaseAttachment(buffer))
                return 1L;

            long address = MemoryUtil.getAddress(buffer);
            assert (address >= baseAddress) & (address <= baseAddress + capacity());

            int position = (int)(address - baseAddress);
            int size = roundUp(buffer.capacity());

            position >>= shift;
            int slotCount = size >> shift;

            long slotBits = (1L << slotCount) - 1;
            long shiftedSlotBits = (slotBits << position);

            if (slotCount == 64)
            {
                assert size == capacity();
                assert position == 0;
                shiftedSlotBits = -1L;
            }

            long next;
            while (true)
            {
                long cur = freeSlots;
                next = cur | shiftedSlotBits;
                assert next == (cur ^ shiftedSlotBits); // ensure no double free
                if (tryRelease && (next == -1L))
                    next = 0L;
                if (freeSlotsUpdater.compareAndSet(this, cur, next))
                    return next;
            }
        }

        @Override
        public String toString()
        {
            return String.format("[slab %s, slots bitmap %s, capacity %d, free %d]", slab, Long.toBinaryString(freeSlots), capacity(), free());
        }
    }

    @VisibleForTesting
    public static int roundUpNormal(int size)
    {
        return roundUp(size, CHUNK_SIZE / 64);
    }

    private static int roundUp(int size, int unit)
    {
        int mask = unit - 1;
        return (size + mask) & ~mask;
    }

    @VisibleForTesting
    public static void shutdownLocalCleaner(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        shutdownNow(Arrays.asList(EXEC));
        awaitTermination(timeout, unit, Arrays.asList(EXEC));
    }
}
