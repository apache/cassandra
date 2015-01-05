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

import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;
import org.apache.cassandra.cache.ICache;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMetrics
{
    /** Cache capacity in bytes */
    public final Gauge<Long> capacity;
    /** Total number of cache hits */
    public final Meter hits;
    /** Total number of cache requests */
    public final Meter requests;
    /** cache hit rate */
    public final Gauge<Double> hitRate;
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
    public CacheMetrics(String type, final ICache cache)
    {
        MetricNameFactory factory = new DefaultNameFactory("Cache", type);

        capacity = Metrics.register(factory.createMetricName("Capacity"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cache.capacity();
            }
        });
        hits = Metrics.meter(factory.createMetricName("Hits"));
        requests = Metrics.meter(factory.createMetricName("Requests"));
        hitRate = Metrics.register(factory.createMetricName("HitRate"), new RatioGauge()
        {
            @Override
            public Ratio getRatio()
            {
                return Ratio.of(hits.getCount(), requests.getCount());
            }
        });
        size = Metrics.register(factory.createMetricName("Size"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cache.weightedSize();
            }
        });
        entries = Metrics.register(factory.createMetricName("Entries"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return cache.size();
            }
        });
    }
}
