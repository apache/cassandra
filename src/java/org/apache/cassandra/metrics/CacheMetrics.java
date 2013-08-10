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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;

import org.apache.cassandra.cache.ICache;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMetrics
{
    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "Cache";

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

    private final AtomicLong lastRequests = new AtomicLong(0);
    private final AtomicLong lastHits = new AtomicLong(0);

    /**
     * Create metrics for given cache.
     *
     * @param type Type of Cache to identify metrics.
     * @param cache Cache to measure metrics
     */
    public CacheMetrics(String type, final ICache cache)
    {
        capacity = CassandraMetricRegistry.register(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "Capacity", type), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cache.capacity();
            }
        });
        hits = CassandraMetricRegistry.get().meter(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "Hits", type));
        
        requests = CassandraMetricRegistry.get().meter(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "Requests", type));
        hitRate = CassandraMetricRegistry.register(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "HitRate", type), new RatioGauge()
        {
            protected double getNumerator()
            {
                return hits.getCount();
            }

            protected double getDenominator()
            {
                return requests.getCount();
            }
            
            public Ratio getRatio()
            {
                return Ratio.of(getNumerator(),  getDenominator());
            }
        });
        size = CassandraMetricRegistry.register(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "Size", type), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cache.weightedSize();
            }
        });
        entries = CassandraMetricRegistry.register(MetricRegistry.name(GROUP_NAME, TYPE_NAME, "Entries", type), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return cache.size();
            }
        });
    }

    // for backward compatibility
    @Deprecated
    public double getRecentHitRate()
    {
        long r = requests.getCount();
        long h = hits.getCount();
        try
        {
            return ((double)(h - lastHits.get())) / (r - lastRequests.get());
        }
        finally
        {
            lastRequests.set(r);
            lastHits.set(h);
        }
    }
}
