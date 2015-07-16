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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


/**
 * Metrics about latencies
 */
public class LatencyMetrics
{
    /** Latency */
    public final Timer latency;
    /** Total latency in micro sec */
    public final Counter totalLatency;

    /** parent metrics to replicate any updates to **/
    private List<LatencyMetrics> parents = Lists.newArrayList();
    
    protected final MetricNameFactory factory;
    protected final MetricNameFactory aliasFactory;
    protected final String namePrefix;

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
        this(factory, null, namePrefix);
    }

    public LatencyMetrics(MetricNameFactory factory, MetricNameFactory aliasFactory, String namePrefix)
    {
        this.factory = factory;
        this.aliasFactory = aliasFactory;
        this.namePrefix = namePrefix;

        if (aliasFactory == null)
        {
            latency = Metrics.timer(factory.createMetricName(namePrefix + "Latency"));
            totalLatency = Metrics.counter(factory.createMetricName(namePrefix + "TotalLatency"));
        }
        else
        {
            latency = Metrics.timer(factory.createMetricName(namePrefix + "Latency"), aliasFactory.createMetricName(namePrefix + "Latency"));
            totalLatency = Metrics.counter(factory.createMetricName(namePrefix + "TotalLatency"), aliasFactory.createMetricName(namePrefix + "TotalLatency"));
        }
    }
    
    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.  Any updates
     * to this will also run on parent
     *
     * @param factory MetricName factory to use
     * @param namePrefix Prefix to append to each metric name
     * @param parents any amount of parents to replicate updates to
     */
    public LatencyMetrics(MetricNameFactory factory, String namePrefix, LatencyMetrics ... parents)
    {
        this(factory, null, namePrefix);
        this.parents.addAll(ImmutableList.copyOf(parents));
    }

    /** takes nanoseconds **/
    public void addNano(long nanos)
    {
        // convert to microseconds. 1 millionth
        latency.update(nanos, TimeUnit.NANOSECONDS);
        totalLatency.inc(nanos / 1000);
        for(LatencyMetrics parent : parents)
        {
            parent.addNano(nanos);
        }
    }

    public void release()
    {
        if (aliasFactory == null)
        {
            Metrics.remove(factory.createMetricName(namePrefix + "Latency"));
            Metrics.remove(factory.createMetricName(namePrefix + "TotalLatency"));
        }
        else
        {
            Metrics.remove(factory.createMetricName(namePrefix + "Latency"), aliasFactory.createMetricName(namePrefix + "Latency"));
            Metrics.remove(factory.createMetricName(namePrefix + "TotalLatency"), aliasFactory.createMetricName(namePrefix + "TotalLatency"));
        }
    }
}
