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

import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import org.apache.cassandra.cache.ChunkCache;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@code ICache}.
 */
public class ChunkCacheMetrics extends CacheMetrics implements StatsCounter
{
    /** Latency of misses */
    public final Timer missLatency;

    /**
     * Create metrics for the provided chunk cache.
     *
     * @param cache Chunk cache to measure metrics
     */
    public ChunkCacheMetrics(ChunkCache cache)
    {
        super("ChunkCache", cache);
        missLatency = Metrics.timer(factory.createMetricName("MissLatency"));
    }

    @Override
    public void recordHits(int count)
    {
        requests.mark(count);
        hits.mark(count);
    }

    @Override
    public void recordMisses(int count)
    {
        requests.mark(count);
        misses.mark(count);
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
    public void recordEviction(int weight, RemovalCause cause)
    {
    }

    @Nonnull
    @Override
    public CacheStats snapshot()
    {
        return CacheStats.of(hits.getCount(), misses.getCount(), missLatency.getCount(), 0L, missLatency.getCount(), 0L, 0L);
    }
}
