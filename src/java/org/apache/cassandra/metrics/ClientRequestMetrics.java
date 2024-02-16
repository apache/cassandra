/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.metrics;


import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


public class ClientRequestMetrics
{
    public final Meter timeouts;
    public final Meter unavailables;
    public final Meter failures;

    /**
     * this is the metric that measures the actual execution time of a certain request;
     * for example, the duration of StorageProxy::readRegular method for regular reads
     */
    public final LatencyMetrics executionTimeMetrics;

    /**
     * this is the metric that measures the time a request spent in the system;
     * for example, the duration between queryStartNanoTime and the end of StorageProxy::readRegular method for regular reads
     */
    public final LatencyMetrics serviceTimeMetrics;

    protected final String namePrefix;
    protected final MetricNameFactory factory;

    public ClientRequestMetrics(String scope, String prefix)
    {
        namePrefix = prefix;
        factory = new DefaultNameFactory("ClientRequest", scope);
        timeouts = Metrics.meter(factory.createMetricName(namePrefix + "Timeouts"));
        unavailables = Metrics.meter(factory.createMetricName(namePrefix + "Unavailables"));
        failures = Metrics.meter(factory.createMetricName(namePrefix + "Failures"));
        executionTimeMetrics = new LatencyMetrics(factory, namePrefix);
        serviceTimeMetrics = new LatencyMetrics(factory, namePrefix + "ServiceTime");
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName(namePrefix + "Timeouts"));
        Metrics.remove(factory.createMetricName(namePrefix + "Unavailables"));
        Metrics.remove(factory.createMetricName(namePrefix + "Failures"));
        executionTimeMetrics.release();
        serviceTimeMetrics.release();
    }
}
