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

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.MBeanWrapper;

public class MessagingServiceMBeanImpl implements MessagingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    // we use CHM deliberately instead of NBHM, as both are non-blocking for readers (which this map mostly is used for)
    // and CHM permits prompter GC
    public final ConcurrentMap<InetAddressAndPort, OutboundConnections> channelManagers = new ConcurrentHashMap<>();
    public final ConcurrentMap<InetAddressAndPort, InboundMessageHandlers> messageHandlers = new ConcurrentHashMap<>();

    public final EndpointMessagingVersions versions;
    public final MessagingMetrics metrics;

    public MessagingServiceMBeanImpl(boolean testOnly, EndpointMessagingVersions versions, MessagingMetrics metrics)
    {
        this.versions = versions;
        this.metrics = metrics;
        if (!testOnly)
        {
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
            metrics.scheduleLogging();
        }
    }

    @Override
    public Map<String, Integer> getLargeMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(false), entry.getValue().large.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(false), entry.getValue().large.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(false), entry.getValue().large.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getSmallMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(false), entry.getValue().small.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(false), entry.getValue().small.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(false), entry.getValue().small.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getGossipMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(false), entry.getValue().urgent.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(false), entry.getValue().urgent.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(false), entry.getValue().urgent.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getLargeMessagePendingTasksWithPort()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(), entry.getValue().large.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageCompletedTasksWithPort()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(), entry.getValue().large.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageDroppedTasksWithPort()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(), entry.getValue().large.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getSmallMessagePendingTasksWithPort()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(), entry.getValue().small.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageCompletedTasksWithPort()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(), entry.getValue().small.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageDroppedTasksWithPort()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(), entry.getValue().small.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getGossipMessagePendingTasksWithPort()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(), entry.getValue().urgent.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageCompletedTasksWithPort()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(), entry.getValue().urgent.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageDroppedTasksWithPort()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(), entry.getValue().urgent.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getDroppedMessages()
    {
        return metrics.getDroppedMessages();
    }

    @Override
    public long getTotalTimeouts()
    {
        return InternodeOutboundMetrics.totalExpiredCallbacks.getCount();
    }

    // these are not messages that time out on sending, but callbacks that timedout without receiving a response
    @Override
    public Map<String, Long> getTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
        {
            String ip = entry.getKey().toString(false);
            long recent = entry.getValue().expiredCallbacks();
            result.put(ip, recent);
        }
        return result;
    }

    // these are not messages that time out on sending, but callbacks that timedout without receiving a response
    @Override
    public Map<String, Long> getTimeoutsPerHostWithPort()
    {
        Map<String, Long> result = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
        {
            String ip = entry.getKey().toString();
            long recent = entry.getValue().expiredCallbacks();
            result.put(ip, recent);
        }
        return result;
    }

    @Override
    public Map<String, Double> getBackPressurePerHost()
    {
        throw new UnsupportedOperationException("This feature has been removed");
    }

    @Override
    public void setBackPressureEnabled(boolean enabled)
    {
        throw new UnsupportedOperationException("This feature has been removed");
    }

    @Override
    public boolean isBackPressureEnabled()
    {
        return false;
    }

    @Override
    public void reloadSslCertificates()
    {
        SSLFactory.forceCheckCertFiles();
    }

    @Override
    public int getVersion(String address) throws UnknownHostException
    {
        return versions.get(address);
    }
}
