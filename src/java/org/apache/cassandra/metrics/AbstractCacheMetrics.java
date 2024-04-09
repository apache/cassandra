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
import com.codahale.metrics.RatioGauge;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Base class for metrics for weighted or unweighted caches.
 */
public abstract class AbstractCacheMetrics
{
    /**
     * Type of the cache, basically its name
     */
    public final String type;

    /**
     * Total number of cache hits
     */
    public final Meter hits;
    /**
     * Total number of cache misses
     */
    public final Meter misses;
    /**
     * Total number of cache requests
     */
    public final Meter requests;

    /**
     * all time cache hit rate
     */
    public final Gauge<Double> hitRate;
    /**
     * 1m hit rate
     */
    public final Gauge<Double> oneMinuteHitRate;
    /**
     * 5m hit rate
     */
    public final Gauge<Double> fiveMinuteHitRate;
    /**
     * 15m hit rate
     */
    public final Gauge<Double> fifteenMinuteHitRate;

    protected final MetricNameFactory factory;

    protected AbstractCacheMetrics(MetricNameFactory metricNameFactory, String type)
    {
        this.type = type;
        factory = metricNameFactory;
        hits = Metrics.meter(factory.createMetricName("Hits"));
        misses = Metrics.meter(factory.createMetricName("Misses"));
        requests = Metrics.meter(factory.createMetricName("Requests"));

        hitRate = Metrics.register(factory.createMetricName("HitRate"),
                                   ratioGauge(hits::getCount, requests::getCount));
        oneMinuteHitRate = Metrics.register(factory.createMetricName("OneMinuteHitRate"),
                                            ratioGauge(hits::getOneMinuteRate, requests::getOneMinuteRate));
        fiveMinuteHitRate = Metrics.register(factory.createMetricName("FiveMinuteHitRate"),
                                             ratioGauge(hits::getFiveMinuteRate, requests::getFiveMinuteRate));
        fifteenMinuteHitRate = Metrics.register(factory.createMetricName("FifteenMinuteHitRate"),
                                                ratioGauge(hits::getFifteenMinuteRate, requests::getFifteenMinuteRate));
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
