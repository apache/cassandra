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
import com.codahale.metrics.Meter;
import org.apache.cassandra.net.OutboundConnections;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Metrics for internode connections.
 */
public class InternodeOutboundMetrics
{
    public static final String TYPE_NAME = "Connection";

    /** Total number of callbacks that were not completed successfully for messages that were sent to this node
     * TODO this was always broken, as it never counted those messages without callbacks? So perhaps we can redefine it. */
    public static final Meter totalExpiredCallbacks = Metrics.meter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalTimeouts", null));

    /** Number of timeouts for specific IP */
    public final Meter expiredCallbacks;

    public final String address;
    /** Pending tasks for large message TCP Connections */
    public final Gauge<Integer> largeMessagePendingTasks;
    /** Pending bytes for large message TCP Connections */
    public final Gauge<Long> largeMessagePendingBytes;
    /** Completed tasks for large message TCP Connections */
    public final Gauge<Long> largeMessageCompletedTasks;
    /** Completed bytes for large message TCP Connections */
    public final Gauge<Long> largeMessageCompletedBytes;
    /** Dropped tasks for large message TCP Connections */
    public final Gauge<Long> largeMessageDropped;
    /** Dropped tasks because of timeout for large message TCP Connections */
    public final Gauge<Long> largeMessageDroppedTasksDueToTimeout;
    /** Dropped bytes because of timeout for large message TCP Connections */
    public final Gauge<Long> largeMessageDroppedBytesDueToTimeout;
    /** Dropped tasks because of overload for large message TCP Connections */
    public final Gauge<Long> largeMessageDroppedTasksDueToOverload;
    /** Dropped bytes because of overload for large message TCP Connections */
    public final Gauge<Long> largeMessageDroppedBytesDueToOverload;
    /** Dropped tasks because of error for large message TCP Connections */
    public final Gauge<Long> largeMessageDroppedTasksDueToError;
    /** Dropped bytes because of error for large message TCP Connections */
    public final Gauge<Long> largeMessageDroppedBytesDueToError;
    /** Pending tasks for small message TCP Connections */
    public final Gauge<Integer> smallMessagePendingTasks;
    /** Pending bytes for small message TCP Connections */
    public final Gauge<Long> smallMessagePendingBytes;
    /** Completed tasks for small message TCP Connections */
    public final Gauge<Long> smallMessageCompletedTasks;
    /** Completed bytes for small message TCP Connections */
    public final Gauge<Long> smallMessageCompletedBytes;
    /** Dropped tasks for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedTasks;
    /** Dropped tasks because of timeout for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedTasksDueToTimeout;
    /** Dropped bytes because of timeout for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedBytesDueToTimeout;
    /** Dropped tasks because of overload for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedTasksDueToOverload;
    /** Dropped bytes because of overload for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedBytesDueToOverload;
    /** Dropped tasks because of error for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedTasksDueToError;
    /** Dropped bytes because of error for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedBytesDueToError;
    /** Pending tasks for small message TCP Connections */
    public final Gauge<Integer> urgentMessagePendingTasks;
    /** Pending bytes for urgent message TCP Connections */
    public final Gauge<Long> urgentMessagePendingBytes;
    /** Completed tasks for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageCompletedTasks;
    /** Completed bytes for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageCompletedBytes;
    /** Dropped tasks for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageDroppedTasks;
    /** Dropped tasks because of timeout for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageDroppedTasksDueToTimeout;
    /** Dropped bytes because of timeout for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageDroppedBytesDueToTimeout;
    /** Dropped tasks because of overload for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageDroppedTasksDueToOverload;
    /** Dropped bytes because of overload for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageDroppedBytesDueToOverload;
    /** Dropped tasks because of error for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageDroppedTasksDueToError;
    /** Dropped bytes because of error for urgent message TCP Connections */
    public final Gauge<Long> urgentMessageDroppedBytesDueToError;
    
    private final MetricNameFactory factory;

    /**
     * Create metrics for given connection pool.
     *
     * @param ip IP address to use for metrics label
     */
    public InternodeOutboundMetrics(InetAddressAndPort ip, final OutboundConnections messagingPool)
    {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        address = ip.getHostAddressAndPortForJMX();

        factory = new DefaultNameFactory("Connection", address);

        largeMessagePendingTasks = Metrics.register(factory.createMetricName("LargeMessagePendingTasks"), messagingPool.large::pendingCount);
        largeMessagePendingBytes = Metrics.register(factory.createMetricName("LargeMessagePendingBytes"), messagingPool.large::pendingBytes);
        largeMessageCompletedTasks = Metrics.register(factory.createMetricName("LargeMessageCompletedTasks"),messagingPool.large::sentCount);
        largeMessageCompletedBytes = Metrics.register(factory.createMetricName("LargeMessageCompletedBytes"),messagingPool.large::sentBytes);
        largeMessageDropped = Metrics.register(factory.createMetricName("LargeMessageDroppedTasks"), messagingPool.large::dropped);
        largeMessageDroppedTasksDueToOverload = Metrics.register(factory.createMetricName("LargeMessageDroppedTasksDueToOverload"), messagingPool.large::overloadedCount);
        largeMessageDroppedBytesDueToOverload = Metrics.register(factory.createMetricName("LargeMessageDroppedBytesDueToOverload"), messagingPool.large::overloadedBytes);
        largeMessageDroppedTasksDueToTimeout = Metrics.register(factory.createMetricName("LargeMessageDroppedTasksDueToTimeout"), messagingPool.large::expiredCount);
        largeMessageDroppedBytesDueToTimeout = Metrics.register(factory.createMetricName("LargeMessageDroppedBytesDueToTimeout"), messagingPool.large::expiredBytes);
        largeMessageDroppedTasksDueToError = Metrics.register(factory.createMetricName("LargeMessageDroppedTasksDueToError"), messagingPool.large::errorCount);
        largeMessageDroppedBytesDueToError = Metrics.register(factory.createMetricName("LargeMessageDroppedBytesDueToError"), messagingPool.large::errorBytes);
        smallMessagePendingTasks = Metrics.register(factory.createMetricName("SmallMessagePendingTasks"), messagingPool.small::pendingCount);
        smallMessagePendingBytes = Metrics.register(factory.createMetricName("SmallMessagePendingBytes"), messagingPool.small::pendingBytes);
        smallMessageCompletedTasks = Metrics.register(factory.createMetricName("SmallMessageCompletedTasks"), messagingPool.small::sentCount);
        smallMessageCompletedBytes = Metrics.register(factory.createMetricName("SmallMessageCompletedBytes"),messagingPool.small::sentBytes);
        smallMessageDroppedTasks = Metrics.register(factory.createMetricName("SmallMessageDroppedTasks"), messagingPool.small::dropped);
        smallMessageDroppedTasksDueToOverload = Metrics.register(factory.createMetricName("SmallMessageDroppedTasksDueToOverload"), messagingPool.small::overloadedCount);
        smallMessageDroppedBytesDueToOverload = Metrics.register(factory.createMetricName("SmallMessageDroppedBytesDueToOverload"), messagingPool.small::overloadedBytes);
        smallMessageDroppedTasksDueToTimeout = Metrics.register(factory.createMetricName("SmallMessageDroppedTasksDueToTimeout"), messagingPool.small::expiredCount);
        smallMessageDroppedBytesDueToTimeout = Metrics.register(factory.createMetricName("SmallMessageDroppedBytesDueToTimeout"), messagingPool.small::expiredBytes);
        smallMessageDroppedTasksDueToError = Metrics.register(factory.createMetricName("SmallMessageDroppedTasksDueToError"), messagingPool.small::errorCount);
        smallMessageDroppedBytesDueToError = Metrics.register(factory.createMetricName("SmallMessageDroppedBytesDueToError"), messagingPool.small::errorBytes);
        urgentMessagePendingTasks = Metrics.register(factory.createMetricName("UrgentMessagePendingTasks"), messagingPool.urgent::pendingCount);
        urgentMessagePendingBytes = Metrics.register(factory.createMetricName("UrgentMessagePendingBytes"), messagingPool.urgent::pendingBytes);
        urgentMessageCompletedTasks = Metrics.register(factory.createMetricName("UrgentMessageCompletedTasks"), messagingPool.urgent::sentCount);
        urgentMessageCompletedBytes = Metrics.register(factory.createMetricName("UrgentMessageCompletedBytes"),messagingPool.urgent::sentBytes);
        urgentMessageDroppedTasks = Metrics.register(factory.createMetricName("UrgentMessageDroppedTasks"), messagingPool.urgent::dropped);
        urgentMessageDroppedTasksDueToOverload = Metrics.register(factory.createMetricName("UrgentMessageDroppedTasksDueToOverload"), messagingPool.urgent::overloadedCount);
        urgentMessageDroppedBytesDueToOverload = Metrics.register(factory.createMetricName("UrgentMessageDroppedBytesDueToOverload"), messagingPool.urgent::overloadedBytes);
        urgentMessageDroppedTasksDueToTimeout = Metrics.register(factory.createMetricName("UrgentMessageDroppedTasksDueToTimeout"), messagingPool.urgent::expiredCount);
        urgentMessageDroppedBytesDueToTimeout = Metrics.register(factory.createMetricName("UrgentMessageDroppedBytesDueToTimeout"), messagingPool.urgent::expiredBytes);
        urgentMessageDroppedTasksDueToError = Metrics.register(factory.createMetricName("UrgentMessageDroppedTasksDueToError"), messagingPool.urgent::errorCount);
        urgentMessageDroppedBytesDueToError = Metrics.register(factory.createMetricName("UrgentMessageDroppedBytesDueToError"), messagingPool.urgent::errorBytes);
        expiredCallbacks = Metrics.meter(factory.createMetricName("Timeouts"));

        // deprecated
        Metrics.register(factory.createMetricName("GossipMessagePendingTasks"), (Gauge<Integer>) messagingPool.urgent::pendingCount);
        Metrics.register(factory.createMetricName("GossipMessageCompletedTasks"), (Gauge<Long>) messagingPool.urgent::sentCount);
        Metrics.register(factory.createMetricName("GossipMessageDroppedTasks"), (Gauge<Long>) messagingPool.urgent::dropped);
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("LargeMessagePendingTasks"));
        Metrics.remove(factory.createMetricName("LargeMessagePendingBytes"));
        Metrics.remove(factory.createMetricName("LargeMessageCompletedTasks"));
        Metrics.remove(factory.createMetricName("LargeMessageCompletedBytes"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedTasks"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedTasksDueToTimeout"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedBytesDueToTimeout"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedTasksDueToOverload"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedBytesDueToOverload"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedTasksDueToError"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedBytesDueToError"));
        Metrics.remove(factory.createMetricName("SmallMessagePendingTasks"));
        Metrics.remove(factory.createMetricName("SmallMessagePendingBytes"));
        Metrics.remove(factory.createMetricName("SmallMessageCompletedTasks"));
        Metrics.remove(factory.createMetricName("SmallMessageCompletedBytes"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedTasks"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedTasksDueToTimeout"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedBytesDueToTimeout"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedTasksDueToOverload"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedBytesDueToOverload"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedTasksDueToError"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedBytesDueToError"));
        Metrics.remove(factory.createMetricName("GossipMessagePendingTasks"));
        Metrics.remove(factory.createMetricName("GossipMessageCompletedTasks"));
        Metrics.remove(factory.createMetricName("GossipMessageDroppedTasks"));
        Metrics.remove(factory.createMetricName("UrgentMessagePendingTasks"));
        Metrics.remove(factory.createMetricName("UrgentMessagePendingBytes"));
        Metrics.remove(factory.createMetricName("UrgentMessageCompletedTasks"));
        Metrics.remove(factory.createMetricName("UrgentMessageCompletedBytes"));
        Metrics.remove(factory.createMetricName("UrgentMessageDroppedTasks"));
        Metrics.remove(factory.createMetricName("UrgentMessageDroppedTasksDueToTimeout"));
        Metrics.remove(factory.createMetricName("UrgentMessageDroppedBytesDueToTimeout"));
        Metrics.remove(factory.createMetricName("UrgentMessageDroppedTasksDueToOverload"));
        Metrics.remove(factory.createMetricName("UrgentMessageDroppedBytesDueToOverload"));
        Metrics.remove(factory.createMetricName("UrgentMessageDroppedTasksDueToError"));
        Metrics.remove(factory.createMetricName("UrgentMessageDroppedBytesDueToError"));
        Metrics.remove(factory.createMetricName("Timeouts"));
    }
}
