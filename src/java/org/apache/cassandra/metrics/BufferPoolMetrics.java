/**
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
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class BufferPoolMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("BufferPool");

    /** Total number of misses */
    public final Meter misses;

    /** Total size of buffer pools, in bytes */
    public final Gauge<Long> size;

    public BufferPoolMetrics()
    {
        misses = Metrics.meter(factory.createMetricName("Misses"));

        size = Metrics.register(factory.createMetricName("Size"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return BufferPool.sizeInBytes();
            }
        });
    }
}
