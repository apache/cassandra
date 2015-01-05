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

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;


import org.apache.cassandra.net.OutboundTcpConnectionPool;

/**
 * Metrics for {@link OutboundTcpConnectionPool}.
 */
public class ConnectionMetrics
{
    public static final String TYPE_NAME = "Connection";

    /** Total number of timeouts happened on this node */
    public static final Meter totalTimeouts = Metrics.meter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalTimeouts", null));

    public final String address;
    /** Pending tasks for Command(Mutations, Read etc) TCP Connections */
    public final Gauge<Integer> commandPendingTasks;
    /** Completed tasks for Command(Mutations, Read etc) TCP Connections */
    public final Gauge<Long> commandCompletedTasks;
    /** Dropped tasks for Command(Mutations, Read etc) TCP Connections */
    public final Gauge<Long> commandDroppedTasks;
    /** Pending tasks for Response(GOSSIP & RESPONSE) TCP Connections */
    public final Gauge<Integer> responsePendingTasks;
    /** Completed tasks for Response(GOSSIP & RESPONSE) TCP Connections */
    public final Gauge<Long> responseCompletedTasks;
    /** Number of timeouts for specific IP */
    public final Meter timeouts;

    private final MetricNameFactory factory;

    /**
     * Create metrics for given connection pool.
     *
     * @param ip IP address to use for metrics label
     * @param connectionPool Connection pool
     */
    public ConnectionMetrics(InetAddress ip, final OutboundTcpConnectionPool connectionPool)
    {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        address = ip.getHostAddress().replaceAll(":", ".");

        factory = new DefaultNameFactory("Connection", address);

        commandPendingTasks = Metrics.register(factory.createMetricName("CommandPendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return connectionPool.cmdCon.getPendingMessages();
            }
        });
        commandCompletedTasks = Metrics.register(factory.createMetricName("CommandCompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return connectionPool.cmdCon.getCompletedMesssages();
            }
        });
        commandDroppedTasks = Metrics.register(factory.createMetricName("CommandDroppedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return connectionPool.cmdCon.getDroppedMessages();
            }
        });
        responsePendingTasks = Metrics.register(factory.createMetricName("ResponsePendingTasks"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return connectionPool.ackCon.getPendingMessages();
            }
        });
        responseCompletedTasks = Metrics.register(factory.createMetricName("ResponseCompletedTasks"), new Gauge<Long>()
        {
            public Long getValue()
            {
                return connectionPool.ackCon.getCompletedMesssages();
            }
        });
        timeouts = Metrics.meter(factory.createMetricName("Timeouts"));
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName("CommandPendingTasks"));
        Metrics.remove(factory.createMetricName("CommandCompletedTasks"));
        Metrics.remove(factory.createMetricName("CommandDroppedTasks"));
        Metrics.remove(factory.createMetricName("ResponsePendingTasks"));
        Metrics.remove(factory.createMetricName("ResponseCompletedTasks"));
        Metrics.remove(factory.createMetricName("Timeouts"));
    }
}
