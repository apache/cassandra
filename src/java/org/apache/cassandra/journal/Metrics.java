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
package org.apache.cassandra.journal;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

final class Metrics<K, V>
{
    private static final String WAITING_ON_FLUSH      = "WaitingOnFlush";
    private static final String WAITING_ON_ALLOCATION = "WaitingOnSegmentAllocation";
    private static final String WRITTEN_ENTRIES       = "WrittenEntries";
    private static final String PENDING_ENTRIES       = "PendingEntries";

    /**
     * The time spent waiting on journal flush; for {@link org.apache.cassandra.journal.Params.FlushMode#PERIODIC}
     * this is only occurs when the flush is lagging its flush interval.
     */
    Timer waitingOnFlush;

    /** Time spent waiting for a segment to be allocated - under normal conditions this should be zero */
    Timer waitingOnSegmentAllocation;

    /** Number of pending (flush) entries */
    Gauge<Long> pendingEntries;

    /** Number of written (flushed) entries */
    Gauge<Long> writtenEntries;

    private final MetricNameFactory factory;

    Metrics(String name)
    {
        this.factory = new DefaultNameFactory("Journal", name);
    }

    void register(Flusher<K, V> flusher)
    {
        waitingOnFlush = CassandraMetricsRegistry.Metrics.timer(createName(WAITING_ON_FLUSH));
        waitingOnSegmentAllocation = CassandraMetricsRegistry.Metrics.timer(createName(WAITING_ON_ALLOCATION));
        pendingEntries = CassandraMetricsRegistry.Metrics.register(createName(PENDING_ENTRIES), flusher::pendingEntries);
        writtenEntries = CassandraMetricsRegistry.Metrics.register(createName(WRITTEN_ENTRIES), flusher::writtenEntries);
    }

    void deregister()
    {
        CassandraMetricsRegistry.Metrics.remove(createName(WAITING_ON_FLUSH));
        CassandraMetricsRegistry.Metrics.remove(createName(WAITING_ON_ALLOCATION));
        CassandraMetricsRegistry.Metrics.remove(createName(PENDING_ENTRIES));
        CassandraMetricsRegistry.Metrics.remove(createName(WRITTEN_ENTRIES));
    }

    private CassandraMetricsRegistry.MetricName createName(String metricName)
    {
        return factory.createMetricName(metricName);
    }
}
