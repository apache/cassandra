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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MovingAverage;

/**
 * Micrometer implementation for the chunk cache metrics.
 */
public class MicrometerChunkCacheMetrics extends MicrometerMetrics implements ChunkCacheMetrics
{
    public static final String CHUNK_CACHE_MISS_LATENCY = "chunk_cache_miss_latency_seconds";
    public static final String CHUNK_CACHE_MISSES = "chunk_cache_misses";
    public static final String CHUNK_CACHE_HITS = "chunk_cache_hits";
    public static final String CHUNK_CACHE_REQUESTS = "chunk_cache_requests";
    public static final String CHUNK_CACHE_EVICTIONS = "chunk_cache_evictions";
    public static final String CHUNK_CACHE_CAPACITY = "chunk_cache_capacity";
    public static final String CHUNK_CACHE_SIZE = "chunk_cache_size";
    public static final String CHUNK_CACHE_NUM_ENTRIES = "chunk_cache_num_entries";
    public static final String CHUNK_CACHE_HIT_RATE = "chunk_cache_hit_rate";

    @VisibleForTesting
    protected final static long hitRateUpdateInterval = TimeUnit.MILLISECONDS.toNanos(100);

    private final CacheSize cache;
    private final MovingAverage hitRate;
    private final AtomicLong hitRateUpdateTime;

    private volatile Timer missLatency;
    private volatile Counter misses;
    private volatile Counter hits;
    private volatile Counter requests;
    private volatile Counter evictions;

    MicrometerChunkCacheMetrics(CacheSize cache)
    {
        this.cache = cache;
        this.hitRate = ExpMovingAverage.decayBy1000();
        this.hitRateUpdateTime = new AtomicLong(System.nanoTime());

        this.missLatency = timer(CHUNK_CACHE_MISS_LATENCY);
        this.misses = counter(CHUNK_CACHE_MISSES);
        this.hits = counter(CHUNK_CACHE_HITS);
        this.requests = counter(CHUNK_CACHE_REQUESTS);
        this.evictions = counter(CHUNK_CACHE_EVICTIONS);
    }

    @Override
    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        super.register(newRegistry, newTags);

        gauge(CHUNK_CACHE_CAPACITY, cache, CacheSize::capacity);
        gauge(CHUNK_CACHE_SIZE, cache, CacheSize::weightedSize);
        gauge(CHUNK_CACHE_NUM_ENTRIES, cache, CacheSize::size);
        gauge(CHUNK_CACHE_HIT_RATE, hitRate, MovingAverage::get);

        this.missLatency = timer(CHUNK_CACHE_MISS_LATENCY);
        this.misses = counter(CHUNK_CACHE_MISSES);
        this.hits = counter(CHUNK_CACHE_HITS);
        this.requests = counter(CHUNK_CACHE_REQUESTS);
        this.evictions = counter(CHUNK_CACHE_EVICTIONS);
    }

    @Override
    public void recordMisses(int count)
    {
        misses.increment(count);
        requests.increment(count);
        updateHitRate();
    }

    private void updateHitRate()
    {
        long lastUpdate = hitRateUpdateTime.get();
        if ((System.nanoTime() - lastUpdate) >= hitRateUpdateInterval)
        {
            if (hitRateUpdateTime.compareAndSet(lastUpdate, System.nanoTime()))
            {
                double numRequests = requests.count();
                if (numRequests > 0)
                    hitRate.update(hits.count() / numRequests);
                else
                    hitRate.update(0);
            }
        }
    }

    @Override
    public void recordLoadSuccess(long val)
    {
        missLatency.record(val, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordLoadFailure(long val)
    {
    }

    @Override
    public void recordEviction()
    {
        evictions.increment();
    }

    @Override
    public void recordEviction(int weight)
    {
    }

    @Override
    public void recordHits(int count)
    {
        hits.increment(count);
        requests.increment(count);
        updateHitRate();
    }

    @Override
    public double hitRate()
    {
        return hitRate.get();
    }

    @Override
    public long requests()
    {
        return (long) requests.count();
    }

    @Override
    public long misses()
    {
        return (long) misses.count();
    }

    @Override
    public long hits()
    {
        return (long) hits.count();
    }

    @Override
    public double missLatency()
    {
        return missLatency.mean(TimeUnit.NANOSECONDS);
    }

    @Override
    public long capacity()
    {
        return cache.capacity();
    }

    @Override
    public long size()
    {
        return cache.weightedSize();
    }

    public long entries()
    {
        return cache.size();
    }

    public long requestsFifteenMinuteRate()
    {
        return 0;
    }

    public long hitsFifteenMinuteRate()
    {
        return 0;
    }

    @Override
    @VisibleForTesting
    public void reset()
    {
        // This method is only used for unit tests, and unit tests only use the codahale implementation
        throw new UnsupportedOperationException("This was not expected to be called and should be implemented if required");
    }

    @Nonnull
    @Override
    public CacheStats snapshot()
    {
        return new CacheStats((long) hits.count(), (long) misses.count(), missLatency.count(),
                0L, (long) missLatency.totalTime(TimeUnit.NANOSECONDS), (long) evictions.count(), 0L);
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
