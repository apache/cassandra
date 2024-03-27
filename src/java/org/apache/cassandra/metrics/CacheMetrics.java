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

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMetrics extends AbstractCacheMetrics
{
    public static final String TYPE_NAME = "Cache";
    /** Cache capacity in bytes */
    public final Gauge<Long> capacity;

    /** Total size of cache, in bytes */
    public final Gauge<Long> size;

    /** Total number of cache entries */
    public final Gauge<Integer> entries;

    /**
     * Create metrics for given cache.
     *
     * @param type Type of Cache to identify metrics
     * @param cache Weighted Cache to measure metrics
     */
    public CacheMetrics(String type, CacheSize cache)
    {
        super(new DefaultNameFactory(TYPE_NAME, type), type);
        capacity = Metrics.register(factory.createMetricName("Capacity"), cache::capacity);
        size = Metrics.register(factory.createMetricName("Size"), cache::weightedSize);
        entries = Metrics.register(factory.createMetricName("Entries"), cache::size);
    }
}
