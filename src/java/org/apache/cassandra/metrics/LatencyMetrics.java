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

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;

import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Metrics about latencies
 */
public class LatencyMetrics
{
    /** Latency */
    public final Timer latency;
    /** Total latency in micro sec */
    public final Counter totalLatency;

    protected final MetricNameFactory factory;
    protected final String namePrefix;

    @Deprecated public final EstimatedHistogram totalLatencyHistogram = new EstimatedHistogram();
    @Deprecated public final EstimatedHistogram recentLatencyHistogram = new EstimatedHistogram();
    protected long lastLatency;
    protected long lastOpCount;

    /**
     * Create LatencyMetrics with given group, type, and scope. Name prefix for each metric will be empty.
     *
     * @param type Type name
     * @param scope Scope
     */
    public LatencyMetrics(String type, String scope)
    {
        this(type, "", scope);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param type Type name
     * @param namePrefix Prefix to append to each metric name
     * @param scope Scope of metrics
     */
    public LatencyMetrics(String type, String namePrefix, String scope)
    {
        this(new DefaultNameFactory(type, scope), namePrefix);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param factory MetricName factory to use
     * @param namePrefix Prefix to append to each metric name
     */
    public LatencyMetrics(MetricNameFactory factory, String namePrefix)
    {
        this.factory = factory;
        this.namePrefix = namePrefix;

        latency = Metrics.newTimer(factory.createMetricName(namePrefix + "Latency"), TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
        totalLatency = Metrics.newCounter(factory.createMetricName(namePrefix + "TotalLatency"));
    }

    /** takes nanoseconds **/
    public void addNano(long nanos)
    {
        // convert to microseconds. 1 millionth
        addMicro(nanos / 1000);
    }

    public void addMicro(long micros)
    {
        synchronized (this)
        {
            latency.update(micros, TimeUnit.MICROSECONDS);
            totalLatency.inc(micros);
        }
        totalLatencyHistogram.add(micros);
        recentLatencyHistogram.add(micros);
    }

    public void release()
    {
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(namePrefix + "Latency"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(namePrefix + "TotalLatency"));
    }

    @Deprecated
    public double getRecentLatency()
    {
        long ops = 0;
        long n = 0;
        synchronized (this)
        {
            ops = latency.count();
            n = totalLatency.count();
        }
        try
        {
            return ((double) n - lastLatency) / (ops - lastOpCount);
        }
        finally
        {
            lastLatency = n;
            lastOpCount = ops;
        }
    }
}
