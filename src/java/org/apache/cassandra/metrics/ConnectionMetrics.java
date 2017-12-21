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

import java.net.InetAddress;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.apache.cassandra.net.async.OutboundMessagingPool;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for internode connections.
 */
public class ConnectionMetrics
{
    public static final String TYPE_NAME = "Connection";

    /** Total number of timeouts happened on this node */
    public static final Meter totalTimeouts = Metrics.meter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalTimeouts", null));

    public final String address;
    /** Pending tasks for large message TCP Connections */
    public final Gauge<Integer> largeMessagePendingTasks;
    /** Completed tasks for large message TCP Connections */
    public final Gauge<Long> largeMessageCompletedTasks;
    /** Dropped tasks for large message TCP Connections */
    public final Gauge<Long> largeMessageDroppedTasks;
    /** Pending tasks for small message TCP Connections */
    public final Gauge<Integer> smallMessagePendingTasks;
    /** Completed tasks for small message TCP Connections */
    public final Gauge<Long> smallMessageCompletedTasks;
    /** Dropped tasks for small message TCP Connections */
    public final Gauge<Long> smallMessageDroppedTasks;
    /** Pending tasks for gossip message TCP Connections */
    public final Gauge<Integer> gossipMessagePendingTasks;
    /** Completed tasks for gossip message TCP Connections */
    public final Gauge<Long> gossipMessageCompletedTasks;
    /** Dropped tasks for gossip message TCP Connections */
    public final Gauge<Long> gossipMessageDroppedTasks;

    /** Number of timeouts for specific IP */
    public final Meter timeouts;

    private final MetricNameFactory factory;

    /**
     * Create metrics for given connection pool.
     *
     * @param ip IP address to use for metrics label
     */
    public ConnectionMetrics(InetAddress ip, final OutboundMessagingPool messagingPool)
    {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        address = ip.getHostAddress().replace(':', '.');

        factory = new DefaultNameFactory("Connection", address);

        largeMessagePendingTasks = Metrics.register(factory.createMetricName("LargeMessagePendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return messagingPool.largeMessageChannel.getPendingMessages();
            }
        });
        largeMessageCompletedTasks = Metrics.register(factory.createMetricName("LargeMessageCompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return messagingPool.largeMessageChannel.getCompletedMessages();
            }
        });
        largeMessageDroppedTasks = Metrics.register(factory.createMetricName("LargeMessageDroppedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return messagingPool.largeMessageChannel.getDroppedMessages();
            }
        });
        smallMessagePendingTasks = Metrics.register(factory.createMetricName("SmallMessagePendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return messagingPool.smallMessageChannel.getPendingMessages();
            }
        });
        smallMessageCompletedTasks = Metrics.register(factory.createMetricName("SmallMessageCompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return messagingPool.smallMessageChannel.getCompletedMessages();
            }
        });
        smallMessageDroppedTasks = Metrics.register(factory.createMetricName("SmallMessageDroppedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return messagingPool.smallMessageChannel.getDroppedMessages();
            }
        });
        gossipMessagePendingTasks = Metrics.register(factory.createMetricName("GossipMessagePendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return messagingPool.gossipChannel.getPendingMessages();
            }
        });
        gossipMessageCompletedTasks = Metrics.register(factory.createMetricName("GossipMessageCompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return messagingPool.gossipChannel.getCompletedMessages();
            }
        });
        gossipMessageDroppedTasks = Metrics.register(factory.createMetricName("GossipMessageDroppedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return messagingPool.gossipChannel.getDroppedMessages();
            }
        });
        timeouts = Metrics.meter(factory.createMetricName("Timeouts"));
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("LargeMessagePendingTasks"));
        Metrics.remove(factory.createMetricName("LargeMessageCompletedTasks"));
        Metrics.remove(factory.createMetricName("LargeMessageDroppedTasks"));
        Metrics.remove(factory.createMetricName("SmallMessagePendingTasks"));
        Metrics.remove(factory.createMetricName("SmallMessageCompletedTasks"));
        Metrics.remove(factory.createMetricName("SmallMessageDroppedTasks"));
        Metrics.remove(factory.createMetricName("GossipMessagePendingTasks"));
        Metrics.remove(factory.createMetricName("GossipMessageCompletedTasks"));
        Metrics.remove(factory.createMetricName("GossipMessageDroppedTasks"));
        Metrics.remove(factory.createMetricName("Timeouts"));
    }
}
