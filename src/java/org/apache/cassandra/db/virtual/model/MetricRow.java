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

package org.apache.cassandra.db.virtual.model;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metric row representation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class MetricRow
{
    private final String key;
    private final Metric metricEntry;

    public MetricRow(String key, Metric metricEntry)
    {
        this.key = key;
        this.metricEntry = metricEntry;
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public String name()
    {
        return key;
    }

    @Column
    public String scope()
    {
        return Metrics.getMetricScope(key);
    }

    @Column
    public String type()
    {
        Class<?> clazz = metricEntry.getClass();
        if (Counter.class.isAssignableFrom(clazz))
            return "counter";
        else if (Gauge.class.isAssignableFrom(clazz))
            return "gauge";
        else if (Histogram.class.isAssignableFrom(clazz))
            return "histogram";
        else if (Meter.class.isAssignableFrom(clazz))
            return "meter";
        else if (Timer.class.isAssignableFrom(clazz))
            return "timer";
        else
            throw new IllegalStateException("Unknown metric type: " + metricEntry.getClass());
    }

    @Column
    public String value()
    {
        return CassandraMetricsRegistry.getValueAsString(metricEntry);
    }
}
