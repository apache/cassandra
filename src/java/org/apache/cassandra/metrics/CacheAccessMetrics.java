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

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CacheAccessMetrics
{
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

    public final RatioGaugeSet hitRate;

    public final RatioGaugeSet missRate;

    protected final MetricNameFactory factory;

    public CacheAccessMetrics(MetricNameFactory factory)
    {
        this.factory = factory;

        this.hits = Metrics.meter(factory.createMetricName("Hits"));
        this.misses = Metrics.meter(factory.createMetricName("Misses"));
        this.requests = Metrics.meter(factory.createMetricName("Requests"));

        this.hitRate = new RatioGaugeSet(hits, requests, factory, "%sHitRate");
        this.missRate = new RatioGaugeSet(misses, requests, factory, "%sMissRate");
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
}
