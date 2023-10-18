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
package org.apache.cassandra.net;



import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * MBean exposing MessagingService metrics plus allowing to enable/disable back-pressure.
 */
public interface MessagingServiceMBean
{
    /**
     * Pending tasks for large message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Integer> getLargeMessagePendingTasks();
    public Map<String, Integer> getLargeMessagePendingTasksWithPort();

    /**
     * Completed tasks for large message) TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Long> getLargeMessageCompletedTasks();
    public Map<String, Long> getLargeMessageCompletedTasksWithPort();

    /**
     * Dropped tasks for large message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Long> getLargeMessageDroppedTasks();
    public Map<String, Long> getLargeMessageDroppedTasksWithPort();


    /**
     * Pending tasks for small message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Integer> getSmallMessagePendingTasks();
    public Map<String, Integer> getSmallMessagePendingTasksWithPort();


    /**
     * Completed tasks for small message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Long> getSmallMessageCompletedTasks();
    public Map<String, Long> getSmallMessageCompletedTasksWithPort();


    /**
     * Dropped tasks for small message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Long> getSmallMessageDroppedTasks();
    public Map<String, Long> getSmallMessageDroppedTasksWithPort();


    /**
     * Pending tasks for gossip message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Integer> getGossipMessagePendingTasks();
    public Map<String, Integer> getGossipMessagePendingTasksWithPort();

    /**
     * Completed tasks for gossip message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Long> getGossipMessageCompletedTasks();
    public Map<String, Long> getGossipMessageCompletedTasksWithPort();

    /**
     * Dropped tasks for gossip message TCP Connections
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Long> getGossipMessageDroppedTasks();
    public Map<String, Long> getGossipMessageDroppedTasksWithPort();

    /**
     * dropped message counts for server lifetime
     */
    public Map<String, Integer> getDroppedMessages();

    /**
     * Total number of timeouts happened on this node
     */
    public long getTotalTimeouts();

    /**
     * Number of timeouts per host
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Long> getTimeoutsPerHost();
    public Map<String, Long> getTimeoutsPerHostWithPort();

    /**
     * Back-pressure rate limiting per host
     * @deprecated See CASSANDRA-7544
     */
    @Deprecated(since = "4.0")
    public Map<String, Double> getBackPressurePerHost();

    /**
     * Enable/Disable back-pressure
     * @deprecated See CASSANDRA-15375
     */
    @Deprecated(since = "4.0")
    public void setBackPressureEnabled(boolean enabled);

    /**
     * Get back-pressure enabled state
     * @deprecated See CASSANDRA-15375
     */
    @Deprecated(since = "4.0")
    public boolean isBackPressureEnabled();

    public int getVersion(String address) throws UnknownHostException;

    void reloadSslCertificates() throws IOException;
}
