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
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

class CodahaleBufferPoolMetrics implements BufferPoolMetrics
{
    /** Total number of hits */
    private final Meter hits;

    /** Total number of misses */
    private final Meter misses;

    /** Total size of buffer pools, in bytes, including overflow allocation */
    private final Gauge<Long> size;

    /** Total size, in bytes, of active buffered being used from the pool currently + overflow */
    private final Gauge<Long> usedSize;

    /**
     * Total size, in bytes, of direct or heap buffers allocated by the pool but not part of the pool
     * either because they are too large to fit or because the pool has exceeded its maximum limit or because it's
     * on-heap allocation.
     */
    private final Gauge<Long> overflowSize;

    public CodahaleBufferPoolMetrics(String scope, BufferPool bufferPool)
    {
        MetricNameFactory factory = new DefaultNameFactory("BufferPool", scope);

        hits = Metrics.meter(factory.createMetricName("Hits"));

        misses = Metrics.meter(factory.createMetricName("Misses"));

        overflowSize = Metrics.register(factory.createMetricName("OverflowSize"), bufferPool::overflowMemoryInBytes);

        usedSize = Metrics.register(factory.createMetricName("UsedSize"), bufferPool::usedSizeInBytes);

        size = Metrics.register(factory.createMetricName("Size"), bufferPool::sizeInBytes);
    }

    @Override
    public void markHit()
    {
        hits.mark();
    }

    @Override
    public long hits()
    {
        return hits.getCount();
    }

    @Override
    public void markMissed()
    {
        misses.mark();
    }

    @Override
    public long misses()
    {
        return misses.getCount();
    }

    @Override
    public long overflowSize()
    {
        return overflowSize.getValue();
    }

    @Override
    public long usedSize()
    {
        return usedSize.getValue();
    }

    @Override
    public long size()
    {
        return size.getValue();
    }

    @Override
    public void register3xAlias()
    {
        MetricNameFactory legacyFactory = new DefaultNameFactory("BufferPool");
        Metrics.registerMBean(misses, legacyFactory.createMetricName("Misses").getMBeanName());
        Metrics.registerMBean(size, legacyFactory.createMetricName("Size").getMBeanName());
    }
}
