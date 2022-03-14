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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for read coordination behaviors.
 */
public final class ReadCoordinationMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("ReadCoordination");

    public static final Counter nonreplicaRequests = Metrics.counter(factory.createMetricName("LocalNodeNonreplicaRequests"));
    public static final Counter preferredOtherReplicas = Metrics.counter(factory.createMetricName("PreferredOtherReplicas"));

    private static final ConcurrentMap<InetAddressAndPort, Histogram> replicaLatencies = new ConcurrentHashMap<>();

    public static void updateReplicaLatency(InetAddressAndPort address, long latency, TimeUnit timeUnit)
    {
        if (latency >= DatabaseDescriptor.getReadRpcTimeout(timeUnit))
            return; // don't track timeouts

        Histogram histogram = replicaLatencies.get(address);

        // avoid computeIfAbsent() call on the common path
        if (null == histogram)
            histogram = replicaLatencies.computeIfAbsent(address, ReadCoordinationMetrics::createHistogram);

        histogram.update(latency);
    }

    private static Histogram createHistogram(InetAddressAndPort h)
    {
        CassandraMetricsRegistry.MetricName metricName = DefaultNameFactory.createMetricName("ReadCoordination", "ReplicaLatency", h.getHostAddressAndPort().replace(':', '.'));
        return Metrics.histogram(metricName, false);
    }
    
    @VisibleForTesting
    static Histogram getReplicaLatencyHistogram(InetAddressAndPort address)
    {
        return replicaLatencies.get(address);
    }
}
