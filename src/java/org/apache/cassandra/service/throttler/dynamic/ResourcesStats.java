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

package org.apache.cassandra.service.throttler.dynamic;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class ResourcesStats
{
    public static final MetricNameFactory factory = new DefaultNameFactory("ResourcesStats");
    Gauge<Long> cpuUtil1Gauge;
    Gauge<Long> cpuUtil2Gauge;

    Gauge<Integer> pendingReadsGauge;
    Gauge<Integer> pendingMutationsGauge;

    Meter cpuUtil1Meter;
    Meter cpuUtil2Meter;
    Meter pendingReadsMeter;
    Meter pendingMutationsMeter;

    long cpuUtil1CurVal;
    long cpuUtil2CurVal;
    int pendingReadsCurVal;
    int pendingMutationsCurVal;


    ResourcesStats()
    {
        cpuUtil1Meter = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("CpuUtil1"));
        cpuUtil2Meter = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("CpuUtil2"));
        pendingReadsMeter = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("PendingReads"));
        pendingMutationsMeter = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("PendingMutations"));

        cpuUtil1Gauge = Metrics.register(factory.createMetricName("CpuUtil1Current"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cpuUtil1CurVal;
            }
        });
        cpuUtil2Gauge = Metrics.register(factory.createMetricName("CpuUtil2Current"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return cpuUtil2CurVal;
            }
        });
        pendingReadsGauge = Metrics.register(factory.createMetricName("PendingReadsCurrent"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return pendingReadsCurVal;
            }
        });
        pendingMutationsGauge = Metrics.register(factory.createMetricName("PendingMutationsCurrent"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return pendingMutationsCurVal;
            }
        });
    }

    public long getCpuUtil1Cur()
    {
        return this.cpuUtil1Gauge.getValue();
    }

    public long getCpuUtil1OneMinute()
    {
        return Double.valueOf(this.cpuUtil1Meter.getOneMinuteRate()).longValue();
    }

    public long getCpuUtil1FiveMinute()
    {
        return Double.valueOf(this.cpuUtil1Meter.getFiveMinuteRate()).longValue();
    }

    public long getCpuUtil1FifteenMinute()
    {
        return Double.valueOf(this.cpuUtil1Meter.getFifteenMinuteRate()).longValue();
    }

    public void setCpuUtil1(long cpuUtil1CurVal)
    {
        this.cpuUtil1CurVal = cpuUtil1CurVal;
        cpuUtil1Meter.mark(this.cpuUtil1CurVal);
    }

    public long getCpuUtil2Cur()
    {
        return this.cpuUtil2Gauge.getValue();
    }

    public long getCpuUtil2OneMinute()
    {
        return Double.valueOf(this.cpuUtil2Meter.getOneMinuteRate()).longValue();
    }

    public long getCpuUtil2FiveMinute()
    {
        return Double.valueOf(this.cpuUtil2Meter.getFiveMinuteRate()).longValue();
    }

    public long getCpuUtil2FifteenMinute()
    {
        return Double.valueOf(this.cpuUtil2Meter.getFifteenMinuteRate()).longValue();
    }

    public void setCpuUtil2(long cpuUtil2CurVal)
    {
        this.cpuUtil2CurVal = cpuUtil2CurVal;
        cpuUtil2Meter.mark(this.cpuUtil2CurVal);
    }

    public int getPendingReadsCur()
    {
        return this.pendingReadsGauge.getValue();
    }

    public long getPendingReadsOneMinute()
    {
        return Double.valueOf(this.pendingReadsMeter.getOneMinuteRate()).longValue();
    }

    public long getPendingReadsFiveMinute()
    {
        return Double.valueOf(this.pendingReadsMeter.getFiveMinuteRate()).longValue();
    }

    public long getPendingReadsFifteenMinute()
    {
        return Double.valueOf(this.pendingReadsMeter.getFifteenMinuteRate()).longValue();
    }

    public void setPendingReads(int pendingRead)
    {
        this.pendingReadsCurVal = pendingRead;
        pendingReadsMeter.mark(this.pendingReadsCurVal);
    }

    public int getPendingMutationsCur()
    {
        return this.pendingMutationsGauge.getValue();
    }

    public long getPendingMutationsOneMinute()
    {
        return Double.valueOf(this.pendingMutationsMeter.getOneMinuteRate()).longValue();
    }

    public long getPendingMutationsFiveMinute()
    {
        return Double.valueOf(this.pendingMutationsMeter.getFiveMinuteRate()).longValue();
    }

    public long getPendingMutationsFifteenMinute()
    {
        return Double.valueOf(this.pendingMutationsMeter.getFifteenMinuteRate()).longValue();
    }

    public void setPendingMutations(int pendingMutations)
    {
        this.pendingMutationsCurVal = pendingMutations;
        pendingMutationsMeter.mark(this.pendingMutationsCurVal);
    }
}
