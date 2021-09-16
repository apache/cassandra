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

package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CodahaleChunkCacheMetricsTest
{
    private ChunkCache mockChunkCache;

    private ChunkCacheMetrics chunkCacheMetrics;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void before()
    {
        mockChunkCache = Mockito.mock(ChunkCache.class);

        // Make sure to not use micrometer metrics
        System.setProperty("cassandra.use_micrometer_metrics", "false");

        chunkCacheMetrics = ChunkCacheMetrics.create(mockChunkCache);
        assertTrue(chunkCacheMetrics instanceof CodahaleChunkCacheMetrics);
    }

    @After
    public void after()
    {
        chunkCacheMetrics.reset();
    }

    @Test
    public void testHitRate()
    {
        chunkCacheMetrics.recordHits(90);
        assertEquals(90, chunkCacheMetrics.hits());

        chunkCacheMetrics.recordMisses(10);
        assertEquals(10, chunkCacheMetrics.misses());

        // Verify requests = hits + misses
        assertEquals(100, chunkCacheMetrics.requests());

        // Verify hitrate
        assertEquals(0.9, chunkCacheMetrics.hitRate(), 0.0);
    }

    @Test
    public void testCommonChunkCacheMetrics()
    {
        // No-op
        chunkCacheMetrics.recordEviction();

        // No-op
        chunkCacheMetrics.recordLoadFailure(25);

        chunkCacheMetrics.recordLoadSuccess(TimeUnit.MILLISECONDS.toNanos(15));

        assertEquals(0.0, chunkCacheMetrics.missLatency(), 0.0);

        // Cache size was statically initialized 
        assertEquals(ChunkCache.cacheSize, chunkCacheMetrics.capacity());

        assertEquals(0, chunkCacheMetrics.size());

        assertEquals(0, chunkCacheMetrics.entries());

        assertEquals(0, chunkCacheMetrics.requestsFifteenMinuteRate());

        assertEquals(0, chunkCacheMetrics.hitsFifteenMinuteRate());

        CacheStats snapshot = chunkCacheMetrics.snapshot();
        assertNotNull(snapshot);
        assertEquals(chunkCacheMetrics.hits(), snapshot.hitCount());
        assertEquals(chunkCacheMetrics.misses(), snapshot.missCount());

        String toString = chunkCacheMetrics.toString();
        assertNotNull(toString);
        Assertions.assertThat(toString).contains("Capacity:");
    }

    @Test
    public void testReset()
    {
        chunkCacheMetrics.recordHits(90);
        assertEquals(90, chunkCacheMetrics.hits());
        chunkCacheMetrics.recordMisses(10);
        assertEquals(10, chunkCacheMetrics.misses());

        chunkCacheMetrics.reset();

        assertEquals(0, chunkCacheMetrics.hits());
        assertEquals(0, chunkCacheMetrics.misses());
        assertEquals(0, chunkCacheMetrics.requests());
    }
}
