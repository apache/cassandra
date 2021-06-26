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

import com.codahale.metrics.Gauge;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.InboundMessageHandlers;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

/**
 * Metrics for internode connections.
 */
public class InternodeInboundMetrics
{
    private final MetricName corruptFramesRecovered;
    private final MetricName corruptFramesUnrecovered;
    private final MetricName errorBytes;
    private final MetricName errorCount;
    private final MetricName expiredBytes;
    private final MetricName expiredCount;
    private final MetricName pendingBytes;
    private final MetricName pendingCount;
    private final MetricName processedBytes;
    private final MetricName processedCount;
    private final MetricName receivedBytes;
    private final MetricName receivedCount;
    private final MetricName throttledCount;
    private final MetricName throttledNanos;

    /**
     * Create metrics for given inbound message handlers.
     *
     * @param peer IP address and port to use for metrics label
     */
    public InternodeInboundMetrics(InetAddressAndPort peer, InboundMessageHandlers handlers)
    {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        MetricNameFactory factory = new DefaultNameFactory("InboundConnection", peer.getHostAddressAndPortForJMX());

        register(corruptFramesRecovered = factory.createMetricName("CorruptFramesRecovered"), handlers::corruptFramesRecovered);
        register(corruptFramesUnrecovered = factory.createMetricName("CorruptFramesUnrecovered"), handlers::corruptFramesUnrecovered);
        register(errorBytes = factory.createMetricName("ErrorBytes"), handlers::errorBytes);
        register(errorCount = factory.createMetricName("ErrorCount"), handlers::errorCount);
        register(expiredBytes = factory.createMetricName("ExpiredBytes"), handlers::expiredBytes);
        register(expiredCount = factory.createMetricName("ExpiredCount"), handlers::expiredCount);
        register(pendingBytes = factory.createMetricName("ScheduledBytes"), handlers::scheduledBytes);
        register(pendingCount = factory.createMetricName("ScheduledCount"), handlers::scheduledCount);
        register(processedBytes = factory.createMetricName("ProcessedBytes"), handlers::processedBytes);
        register(processedCount = factory.createMetricName("ProcessedCount"), handlers::processedCount);
        register(receivedBytes = factory.createMetricName("ReceivedBytes"), handlers::receivedBytes);
        register(receivedCount = factory.createMetricName("ReceivedCount"), handlers::receivedCount);
        register(throttledCount = factory.createMetricName("ThrottledCount"), handlers::throttledCount);
        register(throttledNanos = factory.createMetricName("ThrottledNanos"), handlers::throttledNanos);
    }

    public void release()
    {
        remove(corruptFramesRecovered);
        remove(corruptFramesUnrecovered);
        remove(errorBytes);
        remove(errorCount);
        remove(expiredBytes);
        remove(expiredCount);
        remove(pendingBytes);
        remove(pendingCount);
        remove(processedBytes);
        remove(processedCount);
        remove(receivedBytes);
        remove(receivedCount);
        remove(throttledCount);
        remove(throttledNanos);
    }

    private static void register(MetricName name, Gauge gauge)
    {
        CassandraMetricsRegistry.Metrics.register(name, gauge);
    }

    private static void remove(MetricName name)
    {
        CassandraMetricsRegistry.Metrics.remove(name);
    }
}
