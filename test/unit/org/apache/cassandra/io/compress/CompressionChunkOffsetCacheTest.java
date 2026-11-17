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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.dynatrace.hash4j.hashing.Hashing;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import org.junit.Test;

import org.awaitility.Awaitility;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.MicrometerCompressionChunkOffsetCacheMetrics;
import org.apache.cassandra.utils.concurrent.Ref;
import sun.nio.ch.DirectBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionChunkOffsetCacheTest
{
    @Test
    public void offsetsBlockRefCounting()
    {
        CompressionChunkOffsetCache.OffsetsBlock block = CompressionChunkOffsetCache.OffsetsBlock.allocate(16);

        assertThat(block.references).isEqualTo(1);
        assertThat(block.ref()).isTrue();
        assertThat(block.references).isEqualTo(2);

        block.release();
        assertThat(block.references).isEqualTo(1);

        block.release();
        assertThat(block.references).isEqualTo(0);
        assertThat(block.ref()).isFalse();
    }

    @Test
    public void testEmptyOffsetsBlock()
    {
        long before = CompressionMetadata.nativeMemoryAllocated();
        CompressionChunkOffsetCache.OffsetsBlock block = CompressionChunkOffsetCache.OffsetsBlock.allocate(0);
        assertThat(block.count()).isZero();
        assertThat(block.capacity()).isZero();
        block.release();
        assertThat(CompressionMetadata.nativeMemoryAllocated()).isEqualTo(before);
    }

    @Test
    public void testInvalidOffsetsBlockCapacity()
    {
        assertThatThrownBy(() -> CompressionChunkOffsetCache.OffsetsBlock.allocate(-8)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CompressionChunkOffsetCache.OffsetsBlock.allocate(12)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testOffsetsBlockNotAllocatedInDirectMemory()
    {
        // Buffers allocated via ByteBuffer.allocateDirect are reserved against -XX:MaxDirectMemorySize and always own
        // a Cleaner. Check the root buffer of the block's view rather than global direct memory usage
        ByteBuffer direct = ByteBuffer.allocateDirect(Long.BYTES);
        try
        {
            assertThat(rootBuffer(direct.duplicate()).cleaner()).isNotNull();
        }
        finally
        {
            FileUtils.clean(direct);
        }

        CompressionChunkOffsetCache.OffsetsBlock block = CompressionChunkOffsetCache.OffsetsBlock.allocate(Long.BYTES);
        try
        {
            ByteBuffer buffer = block.buffer();
            assertThat(buffer.isDirect()).isTrue();
            assertThat(rootBuffer(buffer).cleaner()).isNull();
        }
        finally
        {
            block.release();
        }
    }

    private static DirectBuffer rootBuffer(ByteBuffer view)
    {
        Object attachment = ((DirectBuffer) view).attachment();
        return attachment instanceof DirectBuffer ? (DirectBuffer) attachment : (DirectBuffer) view;
    }

    @Test
    public void testBlockKeyHashUsesFileIdAndBlockIndex()
    {
        CompressionChunkOffsetCache.BlockKey key = new CompressionChunkOffsetCache.BlockKey(11L, 7);

        assertThat(key.hashCode()).isEqualTo(Hashing.metroHash64().hashLongLongToInt(11L, 7));
        assertThat(key.equals(key)).isTrue();
        assertThat(key).isEqualTo(new CompressionChunkOffsetCache.BlockKey(11L, 7));
        assertThat(key).isNotEqualTo(new CompressionChunkOffsetCache.BlockKey(12L, 7));
        assertThat(key).isNotEqualTo(new CompressionChunkOffsetCache.BlockKey(11L, 8));
        assertThatThrownBy(() -> key.equals(new Object())).isInstanceOf(ClassCastException.class);
    }

    @Test
    public void testUnsupportedCapacityChange()
    {
        CompressionChunkOffsetCache cache = new CompressionChunkOffsetCache(1024);

        assertThatThrownBy(() -> cache.setCapacity(2048))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Compression chunk offsets cache size cannot be changed.");
    }

    @Test
    public void testEmptyOffsets()
    {
        CompressionChunkOffsets offsets = new CompressionChunkOffsets.Empty();

        assertThat(offsets.size()).isZero();
        assertThat(offsets.offHeapMemoryUsed()).isZero();
        assertThat(offsets.compressedFileLength()).isZero();
        assertThatThrownBy(() -> offsets.get(0))
            .isInstanceOf(IndexOutOfBoundsException.class)
            .hasMessageContaining("Chunk index 0 out of bounds");

        offsets.addTo(new Ref.IdentityCollection(Collections.emptySet()));
        offsets.close();
    }

    @Test
    public void testOffheapMemoryUsage()
    {
        long before = CompressionMetadata.nativeMemoryAllocated();
        CompressionChunkOffsetCache cache = new CompressionChunkOffsetCache(100 * 1024);
        long fileId = 1;

        CompressionChunkOffsetCache.OffsetsBlock block =
        cache.getBlock(fileId, 0, key -> CompressionChunkOffsetCache.OffsetsBlock.allocate(1024));

        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
             assertThat(cache.weightedSize()).isEqualTo(1024L);
             assertThat(CompressionMetadata.nativeMemoryAllocated()).isGreaterThanOrEqualTo(before + 1024L);
         });

        cache.clear();
        Awaitility.await("Cache eviction")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(cache.weightedSize()).isEqualTo(0));
        Awaitility.await("Native memory released")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(CompressionMetadata.nativeMemoryAllocated()).isEqualTo(before));
    }

    @Test
    public void testMetrics()
    {
        CompressionChunkOffsetCache cache = new CompressionChunkOffsetCache(1024);
        MicrometerCompressionChunkOffsetCacheMetrics metrics = cache.getMetrics();
        assertThat(metrics.hits()).isEqualTo(0);
        assertThat(metrics.hitRate()).isNaN();
        assertThat(metrics.misses()).isEqualTo(0);
        assertThat(metrics.entries()).isEqualTo(0);
        assertThat(metrics.capacity()).isEqualTo(1024);
        assertThat(metrics.size()).isEqualTo(0);

        long fileId = 1;

        // 1st miss
        cache.getBlock(fileId, 0, key -> CompressionChunkOffsetCache.OffsetsBlock.allocate(1024));
        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertThat(metrics.hits()).isEqualTo(0);
                      assertThat(metrics.hitRate()).isNaN();
                      assertThat(metrics.misses()).isEqualTo(1);
                      assertThat(metrics.missLatency()).isGreaterThan(0);
                      assertThat(metrics.entries()).isEqualTo(1);
                      assertThat(metrics.capacity()).isEqualTo(1024);
                      assertThat(metrics.size()).isEqualTo(1024);
                  });

        // 1st hit
        cache.getBlock(fileId, 0, key -> CompressionChunkOffsetCache.OffsetsBlock.allocate(1024));
        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertThat(metrics.hits()).isEqualTo(1);
                      assertThat(metrics.hitRate()).isEqualTo(0.5);
                      assertThat(metrics.misses()).isEqualTo(1);
                      assertThat(metrics.missLatency()).isGreaterThan(0);
                      assertThat(metrics.entries()).isEqualTo(1);
                      assertThat(metrics.capacity()).isEqualTo(1024);
                      assertThat(metrics.size()).isEqualTo(1024);
                  });

        // load another block to evict previous block
        cache.getBlock(fileId, 1, key -> CompressionChunkOffsetCache.OffsetsBlock.allocate(1024));
        Awaitility.await("metrics update")
                  .atMost(10, TimeUnit.SECONDS)
                  .untilAsserted(() -> {
                      assertThat(metrics.hits()).isEqualTo(1);
                      assertThat(metrics.hitRate()).isLessThan(0.5).isGreaterThan(0.0);
                      assertThat(metrics.misses()).isEqualTo(2);
                      assertThat(metrics.missLatency()).isGreaterThan(0);
                      assertThat(metrics.entries()).isEqualTo(1);
                      assertThat(metrics.capacity()).isEqualTo(1024);
                      assertThat(metrics.size()).isEqualTo(1024);
                  });

        assertThat(metrics.requests()).isEqualTo(3);
        assertThat(metrics.hitOneMinuteRate()).isNaN();
        assertThat(metrics.hitFiveMinuteRate()).isNaN();
        assertThat(metrics.hitFifteenMinuteRate()).isNaN();
        assertThat(metrics.requestsFifteenMinuteRate()).isNaN();

        CacheStats snapshot = metrics.snapshot();
        assertThat(snapshot.hitCount()).isEqualTo(1);
        assertThat(snapshot.missCount()).isEqualTo(2);
        assertThat(snapshot.loadSuccessCount()).isEqualTo(2);
        assertThat(metrics.toString()).isNotEmpty();

        long evictionCount = snapshot.evictionCount();
        metrics.recordEviction(1, RemovalCause.EXPIRED);
        assertThat(metrics.snapshot().evictionCount()).isEqualTo(evictionCount + 1);
        metrics.recordEviction(1, RemovalCause.REPLACED);
        assertThat(metrics.snapshot().evictionCount()).isEqualTo(evictionCount + 1);
        metrics.recordLoadFailure(1L);
    }
}
