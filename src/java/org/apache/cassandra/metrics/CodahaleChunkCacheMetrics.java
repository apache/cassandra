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
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Codahale implementation for the chunk cache metrics.
 */
public class CodahaleChunkCacheMetrics implements ChunkCacheMetrics
{
    /** Metrics in common with ICache implementations */
    private final CacheMetrics metrics;

    /** Latency of misses */
    public final Timer missLatency;

    /**
     * Create metrics for the provided chunk cache.
     *
     * @param cache Chunk cache to measure metrics
     */
    CodahaleChunkCacheMetrics(ChunkCache cache)
    {
        metrics = new CacheMetrics("ChunkCache", cache);
        missLatency = metrics.registerTimer("MissLatency");
    }

    @Override
    public void recordHits(int count)
    {
        metrics.requests.mark(count);
        metrics.hits.mark(count);
    }

    @Override
    public void recordMisses(int count)
    {
        metrics.requests.mark(count);
        metrics.misses.mark(count);
    }

    @Override
    public void recordLoadSuccess(long loadTime)
    {
        missLatency.update(loadTime, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordLoadFailure(long loadTime)
    {
    }

    @Override
    public void recordEviction()
    {
    }

    @Override
    public double hitRate()
    {
        return metrics.hitRate.getValue();
    }

    @Override
    public long requests()
    {
        return metrics.requests.getCount();
    }

    @Override
    public long misses()
    {
        return metrics.misses.getCount();
    }

    @Override
    public long hits()
    {
        return metrics.hits.getCount();
    }

    @Override
    public double missLatency()
    {
        return missLatency.getOneMinuteRate();
    }

    @Override
    public long capacity()
    {
        return metrics.capacity.getValue();
    }

    @Override
    public long size()
    {
        return metrics.size.getValue();
    }

    @Override
    public long entries()
    {
        return metrics.entries.getValue();
    }

    public long requestsFifteenMinuteRate()
    {
        return (long) metrics.requests.getFifteenMinuteRate();
    }

    public long hitsFifteenMinuteRate()
    {
        return (long) metrics.hits.getFifteenMinuteRate();
    }

    @Nonnull
    @Override
    public CacheStats snapshot()
    {
        return new CacheStats(metrics.hits.getCount(), metrics.misses.getCount(), missLatency.getCount(), 0L, missLatency.getCount(), 0L, 0L);
    }

    @Override
    @VisibleForTesting
    public void reset()
    {
        metrics.reset();
    }

    @Override
    public String toString()
    {
        return "Chunk cache metrics: " + System.lineSeparator() +
               "Miss latency in seconds: " + missLatency() + System.lineSeparator() +
               "Misses count: " + misses() + System.lineSeparator() +
               "Hits count: " + hits() + System.lineSeparator() +
               "Cache requests count: " + requests() + System.lineSeparator() +
               "Moving hit rate: " + hitRate() + System.lineSeparator() +
               "Num entries: " + entries() + System.lineSeparator() +
               "Size in memory: " + FBUtilities.prettyPrintMemory(size()) + System.lineSeparator() +
               "Capacity: " + FBUtilities.prettyPrintMemory(capacity());
    }
}
