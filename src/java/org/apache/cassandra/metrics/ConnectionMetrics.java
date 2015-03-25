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
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

import org.apache.cassandra.net.OutboundTcpConnectionPool;

/**
 * Metrics for {@link OutboundTcpConnectionPool}.
 */
public class ConnectionMetrics
{
    public static final String TYPE_NAME = "Connection";

    /** Total number of timeouts happened on this node */
    public static final Meter totalTimeouts = Metrics.newMeter(DefaultNameFactory.createMetricName(TYPE_NAME, "TotalTimeouts", null), "total timeouts", TimeUnit.SECONDS);
    private static long recentTimeouts;

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

    private long recentTimeoutCount;

    /**
     * Create metrics for given connection pool.
     *
     * @param ip IP address to use for metrics label
     * @param connectionPool Connection pool
     */
    public ConnectionMetrics(InetAddress ip, final OutboundTcpConnectionPool connectionPool)
    {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        address = ip.getHostAddress().replace(':', '.');

        factory = new DefaultNameFactory("Connection", address);

        commandPendingTasks = Metrics.newGauge(factory.createMetricName("CommandPendingTasks"), new Gauge<Integer>()
        {
            public Integer value()
            {
                return connectionPool.cmdCon.getPendingMessages();
            }
        });
        commandCompletedTasks = Metrics.newGauge(factory.createMetricName("CommandCompletedTasks"), new Gauge<Long>()
        {
            public Long value()
            {
                return connectionPool.cmdCon.getCompletedMesssages();
            }
        });
        commandDroppedTasks = Metrics.newGauge(factory.createMetricName("CommandDroppedTasks"), new Gauge<Long>()
        {
            public Long value()
            {
                return connectionPool.cmdCon.getDroppedMessages();
            }
        });
        responsePendingTasks = Metrics.newGauge(factory.createMetricName("ResponsePendingTasks"), new Gauge<Integer>()
        {
            public Integer value()
            {
                return connectionPool.ackCon.getPendingMessages();
            }
        });
        responseCompletedTasks = Metrics.newGauge(factory.createMetricName("ResponseCompletedTasks"), new Gauge<Long>()
        {
            public Long value()
            {
                return connectionPool.ackCon.getCompletedMesssages();
            }
        });
        timeouts = Metrics.newMeter(factory.createMetricName("Timeouts"), "timeouts", TimeUnit.SECONDS);
    }

    public void release()
    {
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("CommandPendingTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("CommandCompletedTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("CommandDroppedTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("ResponsePendingTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("ResponseCompletedTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("Timeouts"));
    }

    @Deprecated
    public static long getRecentTotalTimeout()
    {
        long total = totalTimeouts.count();
        long recent = total - recentTimeouts;
        recentTimeouts = total;
        return recent;
    }

    @Deprecated
    public long getRecentTimeout()
    {
        long timeoutCount = timeouts.count();
        long recent = timeoutCount - recentTimeoutCount;
        recentTimeoutCount = timeoutCount;
        return recent;
    }
}
