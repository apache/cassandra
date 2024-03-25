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

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Timer metric representation for a {@link org.apache.cassandra.db.virtual.CollectionVirtualTableAdapter}.
 */
public class TimerMetricRow
{
    private final String key;
    private final Timer value;

    public TimerMetricRow(String key, Metric value)
    {
        this.key = key;
        this.value = (Timer) value;
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
    public long count()
    {
        return value.getCount();
    }

    @Column
    public double fifteenMinuteRate()
    {
        return value.getFifteenMinuteRate();
    }

    @Column
    public double fiveMinuteRate()
    {
        return value.getFiveMinuteRate();
    }

    @Column
    public double meanRate()
    {
        return value.getMeanRate();
    }

    @Column
    public double oneMinuteRate()
    {
        return value.getOneMinuteRate();
    }
}
