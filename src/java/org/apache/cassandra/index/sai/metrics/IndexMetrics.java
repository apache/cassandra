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
package org.apache.cassandra.index.sai.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import org.apache.cassandra.index.sai.IndexContext;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class IndexMetrics extends AbstractMetrics
{
    public final Timer memtableIndexWriteLatency;
    public final Gauge<Long> liveMemtableIndexWriteCount;
    public final Gauge<Long> memtableIndexBytes;

    public IndexMetrics(IndexContext context)
    {
        super(context.getKeyspace(), context.getTable(), context.getIndexName(), "IndexMetrics");

        memtableIndexWriteLatency = Metrics.timer(createMetricName("MemtableIndexWriteLatency"));
        liveMemtableIndexWriteCount = Metrics.register(createMetricName("LiveMemtableIndexWriteCount"), context::liveMemtableWriteCount);
        memtableIndexBytes = Metrics.register(createMetricName("MemtableIndexBytes"), context::estimatedMemIndexMemoryUsed);
    }
}
