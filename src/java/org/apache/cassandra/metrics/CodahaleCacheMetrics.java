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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.cache.CacheSize;

import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@code ICache}.
 */
public class CodahaleCacheMetrics implements CacheMetrics
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

    public final String cacheType;

    private final MetricNameFactory factory;

    /**
     * Create metrics for given cache.
     *
     * @param type Type of Cache to identify metrics.
     * @param cache Cache to measure metrics
     */
    public CodahaleCacheMetrics(String type, final CacheSize cache)
    {
        cacheType = type;
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

    @Override
    public long requests()
    {
        return requests.getCount();
    }

    @Override
    public long capacity()
    {
        return capacity.getValue();
    }

    @Override
    public long size()
    {
        return size.getValue();
    }

    @Override
    public long entries()
    {
        return entries.getValue();
    }

    @Override
    public long hits()
    {
        return hits.getCount();
    }

    @Override
    public long misses()
    {
        return misses.getCount();
    }

    @Override
    public double hitRate()
    {
        return hitRate.getValue();
    }

    @Override
    public double hitOneMinuteRate()
    {
        return oneMinuteHitRate.getValue();
    }

    @Override
    public double hitFiveMinuteRate()
    {
        return fiveMinuteHitRate.getValue();
    }

    @Override
    public double hitFifteenMinuteRate()
    {
        return fifteenMinuteHitRate.getValue();
    }

    @Override
    public double requestsFifteenMinuteRate()
    {
        return requests.getFifteenMinuteRate();
    }

    @Override
    public void recordHits(int count)
    {
        requests.mark();
        hits.mark(count);
    }

    @Override
    public void recordMisses(int count)
    {
        requests.mark();
        misses.mark(count);
    }

    /**
     * Computes the ratio between the specified numerator and denominator
     *
     * @param numerator the numerator
     * @param denominator the denominator
     * @return the ratio between the numerator and the denominator
     */
    private static double ratio(double numerator, double denominator)
    {
        if (isNaN(denominator) || isInfinite(denominator) || denominator == 0)
            return Double.NaN;

        return numerator / denominator;
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

    /**
     * Returns a {@code Gauge} that will compute the ratio between the number supplied by the suppliers.
     *
     * <p>{@code RatioGauge} create {@code Ratio} objects for each call which is a bit inefficcient. That method
     * computes the ratio using a simple method call.</p>
     *
     * @param numeratorSupplier the supplier for the numerator
     * @param denominatorSupplier the supplier for the denominator
     * @return a {@code Gauge} that will compute the ratio between the number supplied by the suppliers
     */
    public static Gauge<Double> ratioGauge(DoubleSupplier numeratorSupplier, DoubleSupplier denominatorSupplier)
    {
        return () -> ratio(numeratorSupplier.getAsDouble(), denominatorSupplier.getAsDouble());
    }
}