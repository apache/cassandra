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
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.BufferPoolMetrics;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Ref;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.cassandra.utils.ExecutorUtils.*;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;
import static org.apache.cassandra.utils.memory.MemoryUtil.isExactlyDirect;

/**
 * A pool of ByteBuffers that can be recycled to reduce system direct memory fragmentation and improve buffer allocation
 * performance.
 * <p/>
 *
 * Each {@link BufferPool} instance has one {@link GlobalPool} which allocates two kinds of chunks:
 * <ul>
 *     <li>Macro Chunk
 *       <ul>
 *         <li>A memory slab that has size of MACRO_CHUNK_SIZE which is 64 * NORMAL_CHUNK_SIZE</li>
 *         <li>Used to allocate normal chunk with size of NORMAL_CHUNK_SIZE</li>
 *       </ul>
 *     </li>
 *     <li>Normal Chunk
 *       <ul>
 *         <li>Used by {@link LocalPool} to serve buffer allocation</li>
 *         <li>Minimum allocation unit is NORMAL_CHUNK_SIZE / 64</li>
 *       </ul>
 *     </li>
 * </ul>
 *
 * {@link GlobalPool} maintains two kinds of freed chunks, fully freed chunks where all buffers are released, and
 * partially freed chunks where some buffers are not released, eg. held by {@link org.apache.cassandra.cache.ChunkCache}.
 * Partially freed chunks are used to improve cache utilization and have lower priority compared to fully freed chunks.
 *
 * <p/>
 *
 * {@link LocalPool} is a thread local pool to serve buffer allocation requests. There are two kinds of local pool:
 * <ul>
 *     <li>Normal Pool:
 *       <ul>
 *         <li>used to serve allocation size that is larger than half of NORMAL_ALLOCATION_UNIT but less than NORMAL_CHUNK_SIZE</li>
 *         <li>when there is insufficient space in the local queue, it will request global pool for more normal chunks</li>
 *         <li>when normal chunk is recycled either fully or partially, it will be passed to global pool to be used by other pools</li>
 *       </ul>
 *     </li>
 *     <li>Tiny Pool:
 *       <ul>
 *         <li>used to serve allocation size that is less than NORMAL_ALLOCATION_UNIT</li>
 *         <li>when there is insufficient space in the local queue, it will request parent normal pool for more tiny chunks</li>
 *         <li>when tiny chunk is fully freed, it will be passed to paretn normal pool and corresponding buffer in the parent normal chunk is freed</li>
 *       </ul>
 *     </li>
 * </ul>
 *
 * Note: even though partially freed chunks improves cache utilization when chunk cache holds outstanding buffer for
 * arbitrary period, there is still fragmentation in the partially freed chunk because of non-uniform allocation size.
 */
public class BufferPool
{
    /** The size of a page aligned buffer, 128KiB */
    public static final int NORMAL_CHUNK_SIZE = 128 << 10;
    public static final int NORMAL_ALLOCATION_UNIT = NORMAL_CHUNK_SIZE / 64;
    public static final int TINY_CHUNK_SIZE = NORMAL_ALLOCATION_UNIT;
    public static final int TINY_ALLOCATION_UNIT = TINY_CHUNK_SIZE / 64;
    public static final int TINY_ALLOCATION_LIMIT = TINY_CHUNK_SIZE / 2;

    private static final Logger logger = LoggerFactory.getLogger(BufferPool.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    private volatile Debug debug = Debug.NO_OP;

    protected final String name;
    protected final BufferPoolMetrics metrics;
    private final long memoryUsageThreshold;
    private final String readableMemoryUsageThreshold;

    /**
     * Size of unpooled buffer being allocated outside of buffer pool in bytes.
     */
    private final LongAdder overflowMemoryUsage = new LongAdder();

    /**
     * Size of buffer being used in bytes, including pooled buffer and unpooled buffer.
     */
    private final LongAdder memoryInUse = new LongAdder();

    /**
     * Size of allocated buffer pool slabs in bytes
     */
    private final AtomicLong memoryAllocated = new AtomicLong();

    /** A global pool of chunks (page aligned buffers) */
    private final GlobalPool globalPool;

    /** Allow partially freed chunk to be recycled for allocation*/
    private final boolean recyclePartially;

    /** A thread local pool of chunks, where chunks come from the global pool */
    private final FastThreadLocal<LocalPool> localPool = new FastThreadLocal<LocalPool>()
    {
        @Override
        protected LocalPool initialValue()
        {
            return new LocalPool();
        }

        protected void onRemoval(LocalPool value)
        {
            value.release();
        }
    };

    private final Set<LocalPoolRef> localPoolReferences = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ReferenceQueue<Object> localPoolRefQueue = new ReferenceQueue<>();
    private final InfiniteLoopExecutor localPoolCleaner;

    public BufferPool(String name, long memoryUsageThreshold, boolean recyclePartially)
    {
        this.name = name;
        this.memoryUsageThreshold = memoryUsageThreshold;
        this.readableMemoryUsageThreshold = prettyPrintMemory(memoryUsageThreshold);
        this.globalPool = new GlobalPool();
        this.metrics = new BufferPoolMetrics(name, this);
        this.recyclePartially = recyclePartially;
        this.localPoolCleaner = new InfiniteLoopExecutor("LocalPool-Cleaner-" + name, this::cleanupOneReference).start();
    }

    /**
     * @return a local pool instance and caller is responsible to release the pool
     */
    public LocalPool create()
    {
        return new LocalPool();
    }

    public ByteBuffer get(int size, BufferType bufferType)
    {
        if (bufferType == BufferType.ON_HEAP)
            return allocate(size, bufferType);
        else
            return localPool.get().get(size);
    }

    public ByteBuffer getAtLeast(int size, BufferType bufferType)
    {
        if (bufferType == BufferType.ON_HEAP)
            return allocate(size, bufferType);
        else
            return localPool.get().getAtLeast(size);
    }

    /** Unlike the get methods, this will return null if the pool is exhausted */
    public ByteBuffer tryGet(int size)
    {
        return localPool.get().tryGet(size, false);
    }

    public ByteBuffer tryGetAtLeast(int size)
    {
        return localPool.get().tryGet(size, true);
    }

    private ByteBuffer allocate(int size, BufferType bufferType)
    {
        updateOverflowMemoryUsage(size);
        return bufferType == BufferType.ON_HEAP
               ? ByteBuffer.allocate(size)
               : ByteBuffer.allocateDirect(size);
    }

    public void put(ByteBuffer buffer)
    {
        if (isExactlyDirect(buffer))
            localPool.get().put(buffer);
        else
            updateOverflowMemoryUsage(-buffer.capacity());
    }

    public void putUnusedPortion(ByteBuffer buffer)
    {
        if (isExactlyDirect(buffer))
        {
            LocalPool pool = localPool.get();
            if (buffer.limit() > 0)
                pool.putUnusedPortion(buffer);
            else
                pool.put(buffer);
        }
    }

    private void updateOverflowMemoryUsage(int size)
    {
        overflowMemoryUsage.add(size);
    }

    public void setRecycleWhenFreeForCurrentThread(boolean recycleWhenFree)
    {
        localPool.get().recycleWhenFree(recycleWhenFree);
    }

    /**
     * @return buffer size being allocated, including pooled buffers and unpooled buffers
     */
    public long sizeInBytes()
    {
        return memoryAllocated.get() + overflowMemoryUsage.longValue();
    }

    /**
     * @return buffer size being used, including used pooled buffers and unpooled buffers
     */
    public long usedSizeInBytes()
    {
        return memoryInUse.longValue() + overflowMemoryUsage.longValue();
    }

    /**
     * @return unpooled buffer size being allocated outside of buffer pool.
     */
    public long overflowMemoryInBytes()
    {
        return overflowMemoryUsage.longValue();
    }

    /**
     * @return maximum pooled buffer size in bytes
     */
    public long memoryUsageThreshold()
    {
        return memoryUsageThreshold;
    }

    @VisibleForTesting
    public GlobalPool globalPool()
    {
        return globalPool;
    }

    interface Debug
    {
        public static Debug NO_OP = new Debug()
        {
            @Override
            public void registerNormal(Chunk chunk) {}
            @Override
            public void recycleNormal(Chunk oldVersion, Chunk newVersion) {}
            @Override
            public void recyclePartial(Chunk chunk) { }
        };

        void registerNormal(Chunk chunk);
        void recycleNormal(Chunk oldVersion, Chunk newVersion);
        void recyclePartial(Chunk chunk);
    }

    public void debug(Debug setDebug)
    {
        assert setDebug != null;
        this.debug = setDebug;
    }

    interface Recycler
    {
        /**
         * Recycle a fully freed chunk
         */
        void recycle(Chunk chunk);

        /**
         * @return true if chunk can be reused before fully freed.
         */
        boolean canRecyclePartially();

        /**
         * Recycle a partially freed chunk
         */
        void recyclePartially(Chunk chunk);
    }

    /**
     * A queue of page aligned buffers, the chunks, which have been sliced from bigger chunks,
     * the macro-chunks, also page aligned. Macro-chunks are allocated as long as we have not exceeded the
     * memory maximum threshold, MEMORY_USAGE_THRESHOLD and are never released.
     *
     * This class is shared by multiple thread local pools and must be thread-safe.
     */
    final class GlobalPool implements Supplier<Chunk>, Recycler
    {
        /** The size of a bigger chunk, 1 MiB, must be a multiple of NORMAL_CHUNK_SIZE */
        static final int MACRO_CHUNK_SIZE = 64 * NORMAL_CHUNK_SIZE;
        private final String READABLE_MACRO_CHUNK_SIZE = prettyPrintMemory(MACRO_CHUNK_SIZE);

        private final Queue<Chunk> macroChunks = new ConcurrentLinkedQueue<>();
        // TODO (future): it would be preferable to use a CLStack to improve cache occupancy; it would also be preferable to use "CoreLocal" storage
        // It contains fully free chunks and when it runs out, partially freed chunks will be used.
        private final Queue<Chunk> chunks = new ConcurrentLinkedQueue<>();
        // Partially freed chunk which is recirculated whenever chunk has free spaces to
        // improve buffer utilization when chunk cache is holding a piece of buffer for a long period.
        // Note: fragmentation still exists, as holes are with different sizes.
        private final Queue<Chunk> partiallyFreedChunks = new ConcurrentLinkedQueue<>();

        /** Used in logging statements to lazily build a human-readable current memory usage. */
        private final Object readableMemoryUsage =
            new Object() { @Override public String toString() { return prettyPrintMemory(sizeInBytes()); } };

        public GlobalPool()
        {
            assert Integer.bitCount(NORMAL_CHUNK_SIZE) == 1; // must be a power of 2
            assert Integer.bitCount(MACRO_CHUNK_SIZE) == 1; // must be a power of 2
            assert MACRO_CHUNK_SIZE % NORMAL_CHUNK_SIZE == 0; // must be a multiple
        }

        /** Return a chunk, the caller will take owership of the parent chunk. */
        public Chunk get()
        {
            Chunk chunk = chunks.poll();
            if (chunk != null)
                return chunk;

            chunk = allocateMoreChunks();
            if (chunk != null)
                return chunk;

            // another thread may have just allocated last macro chunk, so make one final attempt before returning null
            chunk = chunks.poll();

            // try to use partially freed chunk if there is no more fully freed chunk.
            return chunk == null ? partiallyFreedChunks.poll() : chunk;
        }

        /**
         * This method might be called by multiple threads and that's fine if we add more
         * than one chunk at the same time as long as we don't exceed the MEMORY_USAGE_THRESHOLD.
         */
        private Chunk allocateMoreChunks()
        {
            while (true)
            {
                long cur = memoryAllocated.get();
                if (cur + MACRO_CHUNK_SIZE > memoryUsageThreshold)
                {
                    if (memoryUsageThreshold > 0)
                    {
                        noSpamLogger.info("Maximum memory usage reached ({}), cannot allocate chunk of {}",
                                          readableMemoryUsageThreshold, READABLE_MACRO_CHUNK_SIZE);
                    }
                    return null;
                }
                if (memoryAllocated.compareAndSet(cur, cur + MACRO_CHUNK_SIZE))
                    break;
            }

            // allocate a large chunk
            Chunk chunk;
            try
            {
                chunk = new Chunk(null, allocateDirectAligned(MACRO_CHUNK_SIZE));
            }
            catch (OutOfMemoryError oom)
            {
                noSpamLogger.error("{} buffer pool failed to allocate chunk of {}, current size {} ({}). " +
                                   "Attempting to continue; buffers will be allocated in on-heap memory which can degrade performance. " +
                                   "Make sure direct memory size (-XX:MaxDirectMemorySize) is large enough to accommodate off-heap memtables and caches.",
                                   name, READABLE_MACRO_CHUNK_SIZE, readableMemoryUsage, oom.getClass().getName());
                return null;
            }

            chunk.acquire(null);
            macroChunks.add(chunk);

            final Chunk callerChunk = new Chunk(this, chunk.get(NORMAL_CHUNK_SIZE));
            debug.registerNormal(callerChunk);
            for (int i = NORMAL_CHUNK_SIZE; i < MACRO_CHUNK_SIZE; i += NORMAL_CHUNK_SIZE)
            {
                Chunk add = new Chunk(this, chunk.get(NORMAL_CHUNK_SIZE));
                chunks.add(add);
                debug.registerNormal(add);
            }
            return callerChunk;
        }

        @Override
        public void recycle(Chunk chunk)
        {
            Chunk recycleAs = new Chunk(chunk);
            debug.recycleNormal(chunk, recycleAs);
            chunks.add(recycleAs);
        }

        @Override
        public void recyclePartially(Chunk chunk)
        {
            debug.recyclePartial(chunk);
            partiallyFreedChunks.add(chunk);
        }

        @Override
        public boolean canRecyclePartially()
        {
            return recyclePartially;
        }

        /** This is not thread safe and should only be used for unit testing. */
        @VisibleForTesting
        void unsafeFree()
        {
            while (!chunks.isEmpty())
                chunks.poll().unsafeFree();

            while (!partiallyFreedChunks.isEmpty())
                partiallyFreedChunks.poll().unsafeFree();

            while (!macroChunks.isEmpty())
                macroChunks.poll().unsafeFree();
        }

        @VisibleForTesting
        boolean isPartiallyFreed(Chunk chunk)
        {
            return partiallyFreedChunks.contains(chunk);
        }

        @VisibleForTesting
        boolean isFullyFreed(Chunk chunk)
        {
            return chunks.contains(chunk);
        }
    }

    private static class MicroQueueOfChunks
    {

        // a microqueue of Chunks:
        //  * if any are null, they are at the end;
        //  * new Chunks are added to the last null index
        //  * if no null indexes available, the smallest is swapped with the last index, and this replaced
        //  * this results in a queue that will typically be visited in ascending order of available space, so that
        //    small allocations preferentially slice from the Chunks with the smallest space available to furnish them
        // WARNING: if we ever change the size of this, we must update removeFromLocalQueue, and addChunk
        private Chunk chunk0, chunk1, chunk2;
        private int count;

        // add a new chunk, if necessary evicting the chunk with the least available memory (returning the evicted chunk)
        private Chunk add(Chunk chunk)
        {
            switch (count)
            {
                case 0:
                    chunk0 = chunk;
                    count = 1;
                    break;
                case 1:
                    chunk1 = chunk;
                    count = 2;
                    break;
                case 2:
                    chunk2 = chunk;
                    count = 3;
                    break;
                case 3:
                {
                    Chunk release;
                    int chunk0Free = chunk0.freeSlotCount();
                    int chunk1Free = chunk1.freeSlotCount();
                    int chunk2Free = chunk2.freeSlotCount();
                    if (chunk0Free < chunk1Free)
                    {
                        if (chunk0Free < chunk2Free)
                        {
                            release = chunk0;
                            chunk0 = chunk;
                        }
                        else
                        {
                            release = chunk2;
                            chunk2 = chunk;
                        }
                    }
                    else
                    {
                        if (chunk1Free < chunk2Free)
                        {
                            release = chunk1;
                            chunk1 = chunk;
                        }
                        else
                        {
                            release = chunk2;
                            chunk2 = chunk;
                        }
                    }
                    return release;
                }
                default:
                    throw new IllegalStateException();
            }
            return null;
        }

        private void remove(Chunk chunk)
        {
            // since we only have three elements in the queue, it is clearer, easier and faster to just hard code the options
            if (chunk0 == chunk)
            {   // remove first by shifting back second two
                chunk0 = chunk1;
                chunk1 = chunk2;
            }
            else if (chunk1 == chunk)
            {   // remove second by shifting back last
                chunk1 = chunk2;
            }
            else if (chunk2 != chunk)
            {
                return;
            }
            // whatever we do, the last element must be null
            chunk2 = null;
            --count;
        }

        ByteBuffer get(int size, boolean sizeIsLowerBound, ByteBuffer reuse)
        {
            ByteBuffer buffer;
            if (null != chunk0)
            {
                if (null != (buffer = chunk0.get(size, sizeIsLowerBound, reuse)))
                    return buffer;
                if (null != chunk1)
                {
                    if (null != (buffer = chunk1.get(size, sizeIsLowerBound, reuse)))
                        return buffer;
                    if (null != chunk2 && null != (buffer = chunk2.get(size, sizeIsLowerBound, reuse)))
                        return buffer;
                }
            }
            return null;
        }

        private void forEach(Consumer<Chunk> consumer)
        {
            forEach(consumer, count, chunk0, chunk1, chunk2);
        }

        private void clearForEach(Consumer<Chunk> consumer)
        {
            Chunk chunk0 = this.chunk0, chunk1 = this.chunk1, chunk2 = this.chunk2;
            this.chunk0 = this.chunk1 = this.chunk2 = null;
            forEach(consumer, count, chunk0, chunk1, chunk2);
            count = 0;
        }

        private static void forEach(Consumer<Chunk> consumer, int count, Chunk chunk0, Chunk chunk1, Chunk chunk2)
        {
            switch (count)
            {
                case 3:
                    consumer.accept(chunk2);
                case 2:
                    consumer.accept(chunk1);
                case 1:
                    consumer.accept(chunk0);
            }
        }

        private <T> void removeIf(BiPredicate<Chunk, T> predicate, T value)
        {
            // do not release matching chunks before we move null chunks to the back of the queue;
            // because, with current buffer release from another thread, "chunk#release()" may eventually come back to
            // "removeIf" causing NPE as null chunks are not at the back of the queue.
            Chunk toRelease0 = null, toRelease1 = null, toRelease2 = null;

            try
            {
                switch (count)
                {
                    case 3:
                        if (predicate.test(chunk2, value))
                        {
                            --count;
                            toRelease2 = chunk2;
                            chunk2 = null;
                        }
                    case 2:
                        if (predicate.test(chunk1, value))
                        {
                            --count;
                            toRelease1 = chunk1;
                            chunk1 = null;
                        }
                    case 1:
                        if (predicate.test(chunk0, value))
                        {
                            --count;
                            toRelease0 = chunk0;
                            chunk0 = null;
                        }
                        break;
                    case 0:
                        return;
                }
                switch (count)
                {
                    case 2:
                        // Find the only null item, and shift non-null so that null is at chunk2
                        if (chunk0 == null)
                        {
                            chunk0 = chunk1;
                            chunk1 = chunk2;
                            chunk2 = null;
                        }
                        else if (chunk1 == null)
                        {
                            chunk1 = chunk2;
                            chunk2 = null;
                        }
                        break;
                    case 1:
                        // Find the only non-null item, and shift it to chunk0
                        if (chunk1 != null)
                        {
                            chunk0 = chunk1;
                            chunk1 = null;
                        }
                        else if (chunk2 != null)
                        {
                            chunk0 = chunk2;
                            chunk2 = null;
                        }
                        break;
                }
            }
            finally
            {
                if (toRelease0 != null)
                    toRelease0.release();

                if (toRelease1 != null)
                    toRelease1.release();

                if (toRelease2 != null)
                    toRelease2.release();
            }
        }

        private void release()
        {
            clearForEach(Chunk::release);
        }

        private void unsafeRecycle()
        {
            clearForEach(Chunk::unsafeRecycle);
        }
    }

    /**
     * A thread local class that grabs chunks from the global pool for this thread allocations.
     * Only one thread can do the allocations but multiple threads can release the allocations.
     */
    public final class LocalPool implements Recycler
    {
        private final Queue<ByteBuffer> reuseObjects;
        private final Supplier<Chunk> parent;
        private final LocalPoolRef leakRef;

        private final MicroQueueOfChunks chunks = new MicroQueueOfChunks();
        /**
         * If we are on outer LocalPool, whose chunks are == NORMAL_CHUNK_SIZE, we may service allocation requests
         * for buffers much smaller than
         */
        private LocalPool tinyPool;
        private final int tinyLimit;
        private boolean recycleWhenFree = true;

        public LocalPool()
        {
            this.parent = globalPool;
            this.tinyLimit = TINY_ALLOCATION_LIMIT;
            this.reuseObjects = new ArrayDeque<>();
            localPoolReferences.add(leakRef = new LocalPoolRef(this, localPoolRefQueue));
        }

        /**
         * Invoked by an existing LocalPool, to create a child pool
         */
        private LocalPool(LocalPool parent)
        {
            this.parent = () -> {
                ByteBuffer buffer = parent.tryGetInternal(TINY_CHUNK_SIZE, false);
                return buffer == null ? null : new Chunk(parent, buffer);
            };
            this.tinyLimit = 0; // we only currently permit one layer of nesting (which brings us down to 32 byte allocations, so is plenty)
            this.reuseObjects = parent.reuseObjects; // we share the same ByteBuffer object reuse pool, as we both have the same exclusive access to it
            localPoolReferences.add(leakRef = new LocalPoolRef(this, localPoolRefQueue));
        }

        private LocalPool tinyPool()
        {
            if (tinyPool == null)
                tinyPool = new LocalPool(this).recycleWhenFree(recycleWhenFree);
            return tinyPool;
        }

        public void put(ByteBuffer buffer)
        {
            Chunk chunk = Chunk.getParentChunk(buffer);
            int size = buffer.capacity();

            if (chunk == null)
            {
                FileUtils.clean(buffer);
                updateOverflowMemoryUsage(-size);
            }
            else
            {
                put(buffer, chunk);
                memoryInUse.add(-size);
            }
        }

        private void put(ByteBuffer buffer, Chunk chunk)
        {
            LocalPool owner = chunk.owner;
            if (owner != null && owner == tinyPool)
            {
                tinyPool.put(buffer, chunk);
                return;
            }

            // ask the free method to take exclusive ownership of the act of recycling if chunk is owned by ourselves
            long free = chunk.free(buffer, owner == this && recycleWhenFree);
            // free:
            // *     0L: current pool must be the owner. we can fully recyle the chunk.
            // *    -1L:
            //          * for normal chunk:
            //              a) if it has owner, do nothing.
            //              b) if it not owner, try to recyle it either fully or partially if not already recyled.
            //          * for tiny chunk:
            //              a) if it has owner, do nothing.
            //              b) if it has not owner, recycle the tiny chunk back to parent chunk
            // * others:
            //          * for normal chunk:  partial recycle the chunk if it can be partially recycled but not yet recycled.
            //          * for tiny chunk: do nothing.
            if (free == 0L)
            {
                assert owner == this;
                // 0L => we own recycling responsibility, so must recycle;
                // We must remove the Chunk from our local queue
                remove(chunk);
                chunk.recycle();
            }
            else if (free == -1L && owner != this && chunk.owner == null && !chunk.recycler.canRecyclePartially())
            {
                // although we try to take recycle ownership cheaply, it is not always possible to do so if the owner is racing to unset.
                // we must also check after completely freeing if the owner has since been unset, and try to recycle
                chunk.tryRecycle();
            }
            else if (chunk.owner == null && chunk.recycler.canRecyclePartially() && chunk.setInUse(Chunk.Status.EVICTED))
            {
                // re-cirlate partially freed normal chunk to global list
                chunk.partiallyRecycle();
            }

            if (owner == this)
            {
                MemoryUtil.setAttachment(buffer, null);
                MemoryUtil.setDirectByteBuffer(buffer, 0, 0);
                reuseObjects.add(buffer);
            }
        }

        public void putUnusedPortion(ByteBuffer buffer)
        {
            Chunk chunk = Chunk.getParentChunk(buffer);
            int size = buffer.capacity() - buffer.limit();

            if (chunk == null)
            {
                updateOverflowMemoryUsage(-size);
                return;
            }

            chunk.freeUnusedPortion(buffer);
            memoryInUse.add(-size);
        }

        public ByteBuffer get(int size)
        {
            return get(size, false);
        }

        public ByteBuffer getAtLeast(int size)
        {
            return get(size, true);
        }

        private ByteBuffer get(int size, boolean sizeIsLowerBound)
        {
            ByteBuffer ret = tryGet(size, sizeIsLowerBound);
            if (ret != null)
            {
                metrics.hits.mark();
                memoryInUse.add(ret.capacity());
                return ret;
            }

            if (size > NORMAL_CHUNK_SIZE)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Requested buffer size {} is bigger than {}; allocating directly",
                                 prettyPrintMemory(size),
                                 prettyPrintMemory(NORMAL_CHUNK_SIZE));
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Requested buffer size {} has been allocated directly due to lack of capacity", prettyPrintMemory(size));
            }

            metrics.misses.mark();
            return allocate(size, BufferType.OFF_HEAP);
        }

        private ByteBuffer tryGet(int size, boolean sizeIsLowerBound)
        {
            LocalPool pool = this;
            if (size <= tinyLimit)
            {
                if (size <= 0)
                {
                    if (size == 0)
                        return EMPTY_BUFFER;
                    throw new IllegalArgumentException("Size must be non-negative (" + size + ')');
                }

                pool = tinyPool();
            }
            else if (size > NORMAL_CHUNK_SIZE)
            {
                return null;
            }

            return pool.tryGetInternal(size, sizeIsLowerBound);
        }

        @Inline
        private ByteBuffer tryGetInternal(int size, boolean sizeIsLowerBound)
        {
            ByteBuffer reuse = this.reuseObjects.poll();
            ByteBuffer buffer = chunks.get(size, sizeIsLowerBound, reuse);
            if (buffer != null)
            {
                return buffer;
            }

            // else ask the global pool
            Chunk chunk = addChunkFromParent();
            if (chunk != null)
            {
                ByteBuffer result = chunk.get(size, sizeIsLowerBound, reuse);
                if (result != null)
                    return result;
            }

            if (reuse != null)
                this.reuseObjects.add(reuse);
            return null;
        }

        // recycle entire tiny chunk from tiny pool back to local pool
        @Override
        public void recycle(Chunk chunk)
        {
            ByteBuffer buffer = chunk.slab;
            Chunk parentChunk = Chunk.getParentChunk(buffer);
            put(buffer, parentChunk);
        }

        @Override
        public void recyclePartially(Chunk chunk)
        {
            throw new UnsupportedOperationException("Tiny chunk doesn't support partial recycle.");
        }

        @Override
        public boolean canRecyclePartially()
        {
            // tiny pool doesn't support partial recycle, as we want to have tiny chunk fully freed and put back to
            // parent normal chunk.
            return false;
        }

        private void remove(Chunk chunk)
        {
            chunks.remove(chunk);
            if (tinyPool != null)
                tinyPool.chunks.removeIf((child, parent) -> Chunk.getParentChunk(child.slab) == parent, chunk);
        }

        private Chunk addChunkFromParent()
        {
            Chunk chunk = parent.get();
            if (chunk == null)
                return null;

            addChunk(chunk);
            return chunk;
        }

        private void addChunk(Chunk chunk)
        {
            chunk.acquire(this);
            Chunk evict = chunks.add(chunk);
            if (evict != null)
            {
                if (tinyPool != null)
                    // releasing tiny chunks may result in releasing current evicted chunk
                    tinyPool.chunks.removeIf((child, parent) -> Chunk.getParentChunk(child.slab) == parent, evict);
                evict.release();
                // Mark it as evicted and will be eligible for partial recyle if recycler allows
                evict.setEvicted(Chunk.Status.IN_USE);
            }
        }

        public void release()
        {
            chunks.release();
            reuseObjects.clear();
            localPoolReferences.remove(leakRef);
            leakRef.clear();
            if (tinyPool != null)
                tinyPool.release();
        }

        @VisibleForTesting
        void unsafeRecycle()
        {
            chunks.unsafeRecycle();
        }

        @VisibleForTesting
        public boolean isTinyPool()
        {
            return !(parent instanceof GlobalPool);
        }

        public LocalPool recycleWhenFree(boolean recycleWhenFree)
        {
            this.recycleWhenFree = recycleWhenFree;
            if (tinyPool != null)
                tinyPool.recycleWhenFree = recycleWhenFree;
            return this;
        }
    }

    private static final class LocalPoolRef extends PhantomReference<LocalPool>
    {
        private final MicroQueueOfChunks chunks;
        public LocalPoolRef(LocalPool localPool, ReferenceQueue<? super LocalPool> q)
        {
            super(localPool, q);
            chunks = localPool.chunks;
        }

        public void release()
        {
            chunks.release();
        }
    }

    private void cleanupOneReference() throws InterruptedException
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
        enum Status
        {
            /** The slab is serving or ready to serve requests */
            IN_USE,
            /** The slab is not serving requests and ready for partial recycle*/
            EVICTED;
        }

        private final ByteBuffer slab;
        final long baseAddress;
        private final int shift;

        // it may be 0L when all slots are allocated after "get" or when all slots are freed after "free"
        private volatile long freeSlots;
        private static final AtomicLongFieldUpdater<Chunk> freeSlotsUpdater = AtomicLongFieldUpdater.newUpdater(Chunk.class, "freeSlots");

        // the pool that is _currently allocating_ from this Chunk
        // if this is set, it means the chunk may not be recycled because we may still allocate from it;
        // if it has been unset the local pool has finished with it, and it may be recycled
        private volatile LocalPool owner;
        private final Recycler recycler;

        private static final AtomicReferenceFieldUpdater<Chunk, Status> statusUpdater =
                AtomicReferenceFieldUpdater.newUpdater(Chunk.class, Status.class, "status");
        private volatile Status status = Status.IN_USE;

        @VisibleForTesting
        Object debugAttachment;

        Chunk(Chunk recycle)
        {
            assert recycle.freeSlots == 0L;
            this.slab = recycle.slab;
            this.baseAddress = recycle.baseAddress;
            this.shift = recycle.shift;
            this.freeSlots = -1L;
            this.recycler = recycle.recycler;
        }

        Chunk(Recycler recycler, ByteBuffer slab)
        {
            assert MemoryUtil.isExactlyDirect(slab);
            this.recycler = recycler;
            this.slab = slab;
            this.baseAddress = MemoryUtil.getAddress(slab);

            // The number of bits by which we need to shift to obtain a unit
            // "31 &" is because numberOfTrailingZeros returns 32 when the capacity is zero
            this.shift = 31 & (Integer.numberOfTrailingZeros(slab.capacity() / 64));
            // -1 means all free whilst 0 means all in use
            this.freeSlots = slab.capacity() == 0 ? 0L : -1L;
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
            recycler.recycle(this);
        }

        public void partiallyRecycle()
        {
            assert owner == null;
            recycler.recyclePartially(this);
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

        void setAttachment(ByteBuffer buffer)
        {
            if (Ref.DEBUG_ENABLED)
                MemoryUtil.setAttachment(buffer, new Ref<>(this, null));
            else
                MemoryUtil.setAttachment(buffer, this);
        }

        boolean releaseAttachment(ByteBuffer buffer)
        {
            Object attachment = MemoryUtil.getAttachment(buffer);
            if (attachment == null)
                return false;

            if (Ref.DEBUG_ENABLED)
                ((Ref<Chunk>) attachment).release();

            return true;
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

        int freeSlotCount()
        {
            return Long.bitCount(freeSlots);
        }

        ByteBuffer get(int size)
        {
            return get(size, false, null);
        }

        /**
         * Return the next available slice of this size. If
         * we have exceeded the capacity we return null.
         */
        ByteBuffer get(int size, boolean sizeIsLowerBound, ByteBuffer into)
        {
            // how many multiples of our units is the size?
            // we add (unit - 1), so that when we divide by unit (>>> shift), we effectively round up
            int slotCount = (size - 1 + unit()) >>> shift;
            if (sizeIsLowerBound)
                size = slotCount << shift;

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
                    return set(index << shift, size, into);
                }
            }
        }

        private ByteBuffer set(int offset, int size, ByteBuffer into)
        {
            if (into == null)
                into = MemoryUtil.getHollowDirectByteBuffer(ByteOrder.BIG_ENDIAN);
            MemoryUtil.sliceDirectByteBuffer(slab, into, offset, size);
            setAttachment(into);
            return into;
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

            int size = roundUp(buffer.capacity());
            long address = MemoryUtil.getAddress(buffer);
            assert (address >= baseAddress) & (address + size <= baseAddress + capacity());

            int position = ((int)(address - baseAddress)) >> shift;

            int slotCount = size >> shift;
            long slotBits = 0xffffffffffffffffL >>> (64 - slotCount);
            long shiftedSlotBits = (slotBits << position);

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

        void freeUnusedPortion(ByteBuffer buffer)
        {
            int size = roundUp(buffer.limit());
            int capacity = roundUp(buffer.capacity());
            if (size == capacity)
                return;

            long address = MemoryUtil.getAddress(buffer);
            assert (address >= baseAddress) & (address + size <= baseAddress + capacity());

            // free any spare slots above the size we are using
            int position = ((int)(address + size - baseAddress)) >> shift;
            int slotCount = (capacity - size) >> shift;

            long slotBits = 0xffffffffffffffffL >>> (64 - slotCount);
            long shiftedSlotBits = (slotBits << position);

            long next;
            while (true)
            {
                long cur = freeSlots;
                next = cur | shiftedSlotBits;
                assert next == (cur ^ shiftedSlotBits); // ensure no double free
                if (freeSlotsUpdater.compareAndSet(this, cur, next))
                    break;
            }
            MemoryUtil.setByteBufferCapacity(buffer, size);
        }

        @Override
        public String toString()
        {
            return String.format("[slab %s, slots bitmap %s, capacity %d, free %d]", slab, Long.toBinaryString(freeSlots), capacity(), free());
        }

        @VisibleForTesting
        public LocalPool owner()
        {
            return this.owner;
        }

        @VisibleForTesting
        void unsafeFree()
        {
            Chunk parent = getParentChunk(slab);
            if (parent != null)
                parent.free(slab, false);
            else
                FileUtils.clean(slab);
        }

        static void unsafeRecycle(Chunk chunk)
        {
            if (chunk != null)
            {
                chunk.owner = null;
                chunk.freeSlots = 0L;
                chunk.recycle();
            }
        }

        Status status()
        {
            return status;
        }

        private boolean setStatus(Status current, Status update)
        {
            return statusUpdater.compareAndSet(this, current, update);
        }

        boolean setInUse(Status prev)
        {
            return setStatus(prev, Status.IN_USE);
        }

        boolean setEvicted(Status prev)
        {
            return setStatus(prev, Status.EVICTED);
        }
    }

    @VisibleForTesting
    public static int roundUp(int size)
    {
        if (size <= TINY_ALLOCATION_LIMIT)
            return roundUp(size, TINY_ALLOCATION_UNIT);
        return roundUp(size, NORMAL_ALLOCATION_UNIT);
    }

    @VisibleForTesting
    public static int roundUp(int size, int unit)
    {
        int mask = unit - 1;
        return (size + mask) & ~mask;
    }

    @VisibleForTesting
    public void shutdownLocalCleaner(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        shutdownNow(of(localPoolCleaner));
        awaitTermination(timeout, unit, of(localPoolCleaner));
    }

    @VisibleForTesting
    public BufferPoolMetrics metrics()
    {
        return metrics;
    }

    /** This is not thread safe and should only be used for unit testing. */
    @VisibleForTesting
    public void unsafeReset()
    {
        overflowMemoryUsage.reset();
        memoryInUse.reset();
        memoryAllocated.set(0);
        localPool.get().unsafeRecycle();
        globalPool.unsafeFree();
    }

    @VisibleForTesting
    Chunk unsafeCurrentChunk()
    {
        return localPool.get().chunks.chunk0;
    }

    @VisibleForTesting
    int unsafeNumChunks()
    {
        LocalPool pool = localPool.get();
        return   (pool.chunks.chunk0 != null ? 1 : 0)
                 + (pool.chunks.chunk1 != null ? 1 : 0)
                 + (pool.chunks.chunk2 != null ? 1 : 0);
    }
}
