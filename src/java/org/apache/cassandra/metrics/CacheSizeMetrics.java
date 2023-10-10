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
import org.apache.cassandra.cache.CacheSize;

import static org.apache.cassandra.metrics.CacheMetrics.CACHE;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CacheSizeMetrics
{
    public static final String CAPACITY = "Capacity";
    public static final String SIZE = "Size";
    public static final String ENTRIES = "Entries";
    /**
     * Cache capacity in bytes
     */
    public final Gauge<Long> capacity;

    /**
     * Total size of cache, in bytes
     */
    public final Gauge<Long> size;

    /**
     * Total number of cache entries
     */
    public final Gauge<Integer> entries;

    /**
     * Create metrics for the given cache supporting entity.
     *
     * @param type  Type of Cache to identify metrics.
     * @param cache Cache to measure metrics
     */
    public CacheSizeMetrics(String type, CacheSize cache)
    {
        this(new DefaultNameFactory(CACHE, type), cache);
    }

    public CacheSizeMetrics(MetricNameFactory factory, CacheSize cache)
    {
        capacity = Metrics.register(factory.createMetricName(CAPACITY), cache::capacity);
        size = Metrics.register(factory.createMetricName(SIZE), cache::weightedSize);
        entries = Metrics.register(factory.createMetricName(ENTRIES), cache::size);
    }
}
