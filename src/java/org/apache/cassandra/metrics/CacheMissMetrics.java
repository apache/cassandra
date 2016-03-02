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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import org.apache.cassandra.cache.CacheSize;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMissMetrics
{
    /** Cache capacity in bytes */
    public final Gauge<Long> capacity;
    /** Total number of cache hits */
    public final Meter misses;
    /** Total number of cache requests */
    public final Meter requests;
    /** Latency of misses */
    public final Timer missLatency;
    /** all time cache hit rate */
    public final Gauge<Double> hitRate;
    /** 1m hit rate */
    public final Gauge<Double> oneMinuteHitRate;
    /** 5m hit rate */
    public final Gauge<Double> fiveMinuteHitRate;
    /** 15m hit rate */
    public final Gauge<Double> fifteenMinuteHitRate;
    /** Total size of cache, in bytes */
    public final Gauge<Long> size;
    /** Total number of cache entries */
    public final Gauge<Integer> entries;

    /**
     * Create metrics for given cache.
     *
     * @param type Type of Cache to identify metrics.
     * @param cache Cache to measure metrics
     */
    public CacheMissMetrics(String type, final CacheSize cache)
    {
        MetricNameFactory factory = new DefaultNameFactory("Cache", type);

        capacity = Metrics.register(factory.createMetricName("Capacity"), (Gauge<Long>) cache::capacity);
        misses = Metrics.meter(factory.createMetricName("Misses"));
        requests = Metrics.meter(factory.createMetricName("Requests"));
        missLatency = Metrics.timer(factory.createMetricName("MissLatency"));
        hitRate = Metrics.register(factory.createMetricName("HitRate"), new RatioGauge()
        {
            @Override
            public Ratio getRatio()
            {
                long req = requests.getCount();
                long mis = misses.getCount();
                return Ratio.of(req - mis, req);
            }
        });
        oneMinuteHitRate = Metrics.register(factory.createMetricName("OneMinuteHitRate"), new RatioGauge()
        {
            protected Ratio getRatio()
            {
                double req = requests.getOneMinuteRate();
                double mis = misses.getOneMinuteRate();
                return Ratio.of(req - mis, req);
            }
        });
        fiveMinuteHitRate = Metrics.register(factory.createMetricName("FiveMinuteHitRate"), new RatioGauge()
        {
            protected Ratio getRatio()
            {
                double req = requests.getFiveMinuteRate();
                double mis = misses.getFiveMinuteRate();
                return Ratio.of(req - mis, req);
            }
        });
        fifteenMinuteHitRate = Metrics.register(factory.createMetricName("FifteenMinuteHitRate"), new RatioGauge()
        {
            protected Ratio getRatio()
            {
                double req = requests.getFifteenMinuteRate();
                double mis = misses.getFifteenMinuteRate();
                return Ratio.of(req - mis, req);
            }
        });
        size = Metrics.register(factory.createMetricName("Size"), (Gauge<Long>) cache::weightedSize);
        entries = Metrics.register(factory.createMetricName("Entries"), (Gauge<Integer>) cache::size);
    }

    public void reset()
    {
        requests.mark(-requests.getCount());
        misses.mark(-misses.getCount());
    }
}
