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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.MovingAverage;

public class MicrometerCacheMetrics extends MicrometerMetrics implements CacheMetrics
{
    @VisibleForTesting
    final static long hitRateUpdateIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);

    private final String metricsPrefix;
    private final CacheSize cache;
    private final MovingAverage hitRate;
    private final AtomicLong hitRateUpdateTime;

    private volatile Counter misses;
    private volatile Counter hits;
    private volatile Counter requests;

    public MicrometerCacheMetrics(String metricsPrefix, CacheSize cache)
    {
        this.metricsPrefix = metricsPrefix;
        this.cache = cache;
        this.hitRate = ExpMovingAverage.decayBy1000();
        this.hitRateUpdateTime = new AtomicLong(ApproximateTime.nanoTime());

        this.misses = counter(metricsPrefix + "_misses");
        this.hits = counter(metricsPrefix + "_hits");
        this.requests = counter(metricsPrefix + "_requests");
    }

    @Override
    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        super.register(newRegistry, newTags);

        gauge(metricsPrefix + "_capacity", cache, CacheSize::capacity);
        gauge(metricsPrefix + "_size", cache, CacheSize::weightedSize);
        gauge(metricsPrefix + "_num_entries", cache, CacheSize::size);
        gauge(metricsPrefix + "_hit_rate", hitRate, MovingAverage::get);

        this.misses = counter(metricsPrefix + "_misses");
        this.hits = counter(metricsPrefix + "_hits");
        this.requests = counter(metricsPrefix + "_requests");
    }

    @Override
    public long requests()
    {
        return (long) requests.count();
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

    @Override
    public long entries()
    {
        return cache.size();
    }

    @Override
    public long hits()
    {
        return (long) hits.count();
    }

    @Override
    public long misses()
    {
        return (long) misses.count();
    }

    @Override
    public double hitRate()
    {
        return hitRate.get();
    }

    @Override
    public double hitOneMinuteRate()
    {
        return Double.NaN;
    }

    @Override
    public double hitFiveMinuteRate()
    {
        return Double.NaN;
    }

    @Override
    public double hitFifteenMinuteRate()
    {
        return Double.NaN;
    }

    @Override
    public double requestsFifteenMinuteRate()
    {
        return Double.NaN;
    }

    @Override
    public void recordHits(int count)
    {
        hits.increment(count);
        requests.increment(count);
        updateHitRate();
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
        if (ApproximateTime.nanoTime() - lastUpdate > hitRateUpdateIntervalNanos)
        {
            if (hitRateUpdateTime.compareAndSet(lastUpdate, ApproximateTime.nanoTime()))
            {
                double numRequests = requests.count();
                if (numRequests > 0)
                    hitRate.update(hits.count() / numRequests);
                else
                    hitRate.update(0);
            }
        }
    }
}