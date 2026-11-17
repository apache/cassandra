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

package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.MicrometerCompressionChunkOffsetCacheMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static org.apache.cassandra.io.compress.CompressionMetadata.NATIVE_MEMORY_USAGE;

/**
 * Block cache for compression chunk offsets stored in the compression info file.
 * <p>
 * The cache stores blocks of offsets in native memory allocated via {@link MemoryUtil#allocate(long)}, same as
 * {@link CompressionChunkOffsets.InMemory}, so it doesn't count towards {@code -XX:MaxDirectMemorySize} to avoid impact
 * production server config. Each block is reference-counted and freed on eviction. This cache is intended for use by
 * {@link org.apache.cassandra.io.compress.CompressionChunkOffsets.BlockCache}.
 * <p>
 * Note that Blocks are weighted by native memory capacity; key overhead is not included.
 */
public class CompressionChunkOffsetCache implements CacheSize
{
    private static volatile CompressionChunkOffsetCache INSTANCE;

    public static CompressionChunkOffsetCache get()
    {
        // do not initialize the cache if cache size is non-positive
        long cacheSizeInBytes = getCacheSizeInBytes(PlatformDependent.maxDirectMemory());
        Preconditions.checkArgument(cacheSizeInBytes > 0, "CompressionChunkOffsetCache size must be positive, but got " + cacheSizeInBytes);

        if (INSTANCE == null)
        {
            synchronized (CompressionChunkOffsetCache.class)
            {
                if (INSTANCE == null)
                    INSTANCE = new CompressionChunkOffsetCache(cacheSizeInBytes);
            }
        }
        return INSTANCE;
    }

    @VisibleForTesting
    static synchronized void resetCache()
    {
        if (INSTANCE != null)
            INSTANCE.clear();

        INSTANCE = null;
    }

    static long getCacheSizeInBytes(long maxDirectMemoryInSize)
    {
        String configString = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_BLOCK_CACHE_SIZE.getString();
        return FBUtilities.parseHumanReadableConfig(maxDirectMemoryInSize, configString);
    }

    private final Cache<BlockKey, OffsetsBlock> cache;
    private final MicrometerCompressionChunkOffsetCacheMetrics metrics;
    private final long maxSizeInBytes;

    CompressionChunkOffsetCache(long maxSizeInBytes)
    {
        this.maxSizeInBytes = maxSizeInBytes;
        this.metrics = new MicrometerCompressionChunkOffsetCacheMetrics(this, "compression_chunk_offsets_cache");
        RemovalListener<BlockKey, OffsetsBlock> remover = (key, value, cause) ->
        {
            if (value != null)
                value.release();
        };
        cache = Caffeine.newBuilder()
                        .maximumWeight(maxSizeInBytes)
                        .weigher((BlockKey key, OffsetsBlock value) -> Math.min(Integer.MAX_VALUE, value.capacity()))
                        .removalListener(remover)
                        .recordStats(() -> metrics)
                        .build();
    }

    /**
     * Get a cached offsets block for the provided file id and block index, loading it from disk if absent.
     *
     * @param fileId local id for the compression info file
     * @param blockIndex index of the offsets block
     * @param blockLoader loader used to create a block on cache miss. The cache key is provided to the loader so
     *                    callers can reuse one loader instead of allocating a closure per lookup.
     * @return cached or newly loaded offsets block
     */
    OffsetsBlock getBlock(long fileId, int blockIndex, Function<BlockKey, OffsetsBlock> blockLoader)
    {
        BlockKey key = new BlockKey(fileId, blockIndex);
        return cache.get(key, blockLoader);
    }

    public MicrometerCompressionChunkOffsetCacheMetrics getMetrics()
    {
        return metrics;
    }

    void invalidate(BlockKey blockKey)
    {
        cache.invalidate(blockKey);
    }

    @Override
    public long capacity()
    {
        return maxSizeInBytes;
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException("Compression chunk offsets cache size cannot be changed.");
    }

    @Override
    public int size()
    {
        return cache.asMap().size();
    }

    @Override
    public long weightedSize()
    {
        return cache.policy()
                    .eviction()
                    .map(policy -> policy.weightedSize().orElseGet(cache::estimatedSize))
                    .orElseGet(cache::estimatedSize);
    }

    @VisibleForTesting
    public void clear()
    {
        cache.invalidateAll();
        cache.cleanUp();
    }

    public static final class BlockKey
    {
        private static final Hasher64 hasher = Hashing.metroHash64();

        private final long fileId;
        private final int blockIndex;
        private final int hashcode;

        public BlockKey(long fileId, int blockIndex)
        {
            this.fileId = fileId;
            this.blockIndex = blockIndex;
            this.hashcode = hasher.hashLongLongToInt(fileId, blockIndex);
        }

        /**
         * @return index of the offsets block within the compression info file
         */
        int blockIndex()
        {
            return blockIndex;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;
            BlockKey that = (BlockKey) other;
            return blockIndex == that.blockIndex && fileId == that.fileId;
        }

        @Override
        public int hashCode()
        {
            return hashcode;
        }
    }

    @VisibleForTesting
    static int blockBufferSize()
    {
        int configuredSize = CassandraRelevantProperties.COMPRESSION_CHUNK_OFFSETS_CACHE_BLOCK_SIZE.getInt();
        // round to multiple of 8 bytes
        return Math.max(Long.BYTES, (configuredSize / Long.BYTES) * Long.BYTES);
    }

    /**
     * A cached block of chunk offsets. It owns native memory and is reference-counted to ensure the memory
     * is released only after eviction and last user release.
     */
    public static class OffsetsBlock
    {
        private static final AtomicIntegerFieldUpdater<OffsetsBlock> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(OffsetsBlock.class, "references");
        private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

        // 0 if capacity is 0
        private final long address;
        // big-endian view over the native memory, compression info file stores offsets in big-endian
        private final ByteBuffer offsetsBuffer;
        private final int capacity;

        volatile int references;

        private OffsetsBlock(long address, int capacity)
        {
            this.address = address;
            this.capacity = capacity;
            this.offsetsBuffer = capacity == 0 ? EMPTY_BUFFER : MemoryUtil.getByteBuffer(address, capacity, ByteOrder.BIG_ENDIAN);
            this.references = 1;
            NATIVE_MEMORY_USAGE.addAndGet(capacity);
        }

        /**
         * Allocate a block with the given capacity in native memory. Caller must fill the block via {@link #buffer()}
         * before publishing it, and must {@link #release()} it if loading fails.
         *
         * @param capacity size of the block in bytes, must be a multiple of {@link Long#BYTES}
         * @return a block with one reference
         */
        public static OffsetsBlock allocate(int capacity)
        {
            Preconditions.checkArgument(capacity >= 0 && capacity % Long.BYTES == 0,
                                        "Offsets block capacity must be a non-negative multiple of %s, but got %s", Long.BYTES, capacity);
            long address = capacity == 0 ? 0 : MemoryUtil.allocate(capacity);
            return new OffsetsBlock(address, capacity);
        }

        /**
         * @return a new big-endian buffer over the whole block with independent position and limit, used to load
         *         offsets into the block. It must not be used after the block is released.
         */
        ByteBuffer buffer()
        {
            return offsetsBuffer.duplicate().order(ByteOrder.BIG_ENDIAN);
        }

        /**
         * Increment the reference count if the block has not been released.
         *
         * @return true if the reference was acquired; false if already released
         */
        boolean ref()
        {
            int refCount;
            do
            {
                refCount = references;

                if (refCount == 0)
                    return false; // Payload was released before we managed to reference it.
            }
            while (!referencesUpdater.compareAndSet(this, refCount, refCount + 1));

            return true;
        }

        /**
         * Decrement the reference count and free off-heap memory when it reaches zero.
         */
        void release()
        {
            if (referencesUpdater.decrementAndGet(this) == 0)
            {
                if (address != 0)
                    MemoryUtil.free(address, capacity);
                NATIVE_MEMORY_USAGE.addAndGet(-capacity);
            }
        }

        /**
         * @return off-heap capacity of this block in bytes
         */
        public int capacity()
        {
            return capacity;
        }

        /**
         * @return number of chunk offsets stored in this block
         */
        public int count()
        {
            return capacity / Long.BYTES;
        }

        /**
         * Return the chunk offset at the given index within this block. Must be called after successful {@link #ref()}.
         *
         * @param index offset index within this block
         * @return chunk offset value
         */
        public long getLongAtIndex(int index)
        {
            return offsetsBuffer.getLong(index * Long.BYTES);
        }
    }
}
