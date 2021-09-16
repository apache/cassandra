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

import java.util.function.DoubleSupplier;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.*;
import org.apache.cassandra.cache.CacheSize;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMetrics
{
    /** Cache capacity in bytes */
    public final Gauge<Long> capacity;
    /** Total size of cache, in bytes */
    public final Gauge<Long> size;
    /** Total number of cache entries */
    public final Gauge<Integer> entries;

    /** Total number of cache hits */
    public final Meter hits;
    /** Total number of cache misses */
    public final Meter misses;
    /** Total number of cache requests */
    public final Meter requests;

    /** all time cache hit rate */
    public final Gauge<Double> hitRate;
    /** 1m hit rate */
    public final Gauge<Double> oneMinuteHitRate;
    /** 5m hit rate */
    public final Gauge<Double> fiveMinuteHitRate;
    /** 15m hit rate */
    public final Gauge<Double> fifteenMinuteHitRate;

    protected final MetricNameFactory factory;

    /**
     * Create metrics for given cache.
     *
     * @param type Type of Cache to identify metrics.
     * @param cache Cache to measure metrics
     */
    public CacheMetrics(String type, CacheSize cache)
    {
        factory = new DefaultNameFactory("Cache", type);

        capacity = registerGauge("Capacity", cache::capacity);
        size = registerGauge("Size", cache::weightedSize);
        entries = registerGauge("Entries", cache::size);

        requests = registerMeter("Requests");

        hits = registerMeter("Hits");
        misses = registerMeter("Misses");

        hitRate = registerGauge("HitRate", ratioGauge(hits::getCount, requests::getCount));
        oneMinuteHitRate = registerGauge("OneMinuteHitRate", ratioGauge(hits::getOneMinuteRate, requests::getOneMinuteRate));
        fiveMinuteHitRate = registerGauge("FiveMinuteHitRate", ratioGauge(hits::getFiveMinuteRate, requests::getFiveMinuteRate));
        fifteenMinuteHitRate = registerGauge("FifteenMinuteHitRate", ratioGauge(hits::getFifteenMinuteRate, requests::getFifteenMinuteRate));
    }

    protected final <T> Gauge<T> registerGauge(String name, Gauge<T> gauge)
    {
        return Metrics.register(factory.createMetricName(name), gauge);
    }

    protected final Meter registerMeter(String name)
    {
        return Metrics.meter(factory.createMetricName(name));
    }

    protected final Timer registerTimer(String name)
    {
        return Metrics.timer(factory.createMetricName(name));
    }

    @VisibleForTesting
    public void reset()
    {
        // No actual reset happens. The Meter counter is put to zero but will not reset the moving averages
        // It rather injects a weird value into them.
        // This method is being only used by CacheMetricsTest and CachingBench so fixing this issue was acknowledged
        // but not considered mandatory to be fixed now (CASSANDRA-16228)
        hits.mark(-hits.getCount());
        misses.mark(-misses.getCount());
        requests.mark(-requests.getCount());
    }

    private static RatioGauge ratioGauge(DoubleSupplier numeratorSupplier, DoubleSupplier denominatorSupplier)
    {
        return new RatioGauge()
        {
            @Override
            public Ratio getRatio()
            {
                return Ratio.of(numeratorSupplier.getAsDouble(), denominatorSupplier.getAsDouble());
            }
        };
    }
}
