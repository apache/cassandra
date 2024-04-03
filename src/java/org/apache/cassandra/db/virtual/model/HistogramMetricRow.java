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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Historgam metric representation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class HistogramMetricRow
{
    private final String key;
    private final Snapshot value;

    public HistogramMetricRow(String key, Metric value)
    {
        this.key = key;
        this.value = ((Histogram) value).getSnapshot();
    }

    @Column
    public String scope()
    {
        return Metrics.getMetricScope(key);
    }

    @Column(type = Column.Type.PARTITION_KEY)
    public String name()
    {
        return key;
    }

    @Column
    public double p75th()
    {
        return value.get75thPercentile();
    }

    @Column
    public double p95th()
    {
        return value.get95thPercentile();
    }

    @Column
    public double p98th()
    {
        return value.get98thPercentile();
    }

    @Column
    public double p99th()
    {
        return value.get99thPercentile();
    }

    @Column
    public double p999th()
    {
        return value.get999thPercentile();
    }

    @Column
    public long max()
    {
        return value.getMax();
    }

    @Column
    public double mean()
    {
        return value.getMean();
    }

    @Column
    public long min()
    {
        return value.getMin();
    }
}
