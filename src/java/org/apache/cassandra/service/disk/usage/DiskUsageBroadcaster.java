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

package org.apache.cassandra.service.disk.usage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Starts {@link DiskUsageMonitor} to monitor local disk usage state and broadcast new state via Gossip.
 * At the same time, it caches cluster's disk usage state received via Gossip.
 */
public class DiskUsageBroadcaster implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(DiskUsageBroadcaster.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 10, TimeUnit.MINUTES);

    public static final DiskUsageBroadcaster instance = new DiskUsageBroadcaster(DiskUsageMonitor.instance);

    private final DiskUsageMonitor monitor;
    private final ConcurrentMap<InetAddressAndPort, DiskUsageState> usageInfo = new ConcurrentHashMap<>();
    private volatile boolean hasStuffedOrFullNode = false;

    @VisibleForTesting
    public DiskUsageBroadcaster(DiskUsageMonitor monitor)
    {
        this.monitor = monitor;
        Gossiper.instance.register(this);
    }

    /**
     * @return {@code true} if any node in the cluster is STUFFED OR FULL
     */
    public boolean hasStuffedOrFullNode()
    {
        return hasStuffedOrFullNode;
    }

    /**
     * @return {@code true} if given node's disk usage is FULL
     */
    public boolean isFull(InetAddressAndPort endpoint)
    {
        return state(endpoint).isFull();
    }

    /**
     * @return {@code true} if given node's disk usage is STUFFED
     */
    public boolean isStuffed(InetAddressAndPort endpoint)
    {
        return state(endpoint).isStuffed();
    }

    @VisibleForTesting
    public DiskUsageState state(InetAddressAndPort endpoint)
    {
        return usageInfo.getOrDefault(endpoint, DiskUsageState.NOT_AVAILABLE);
    }

    public void startBroadcasting()
    {
        monitor.start(newState -> {

            if (logger.isTraceEnabled())
                logger.trace("Disseminating disk usage info: {}", newState);

            Gossiper.instance.addLocalApplicationState(ApplicationState.DISK_USAGE,
                                                       StorageService.instance.valueFactory.diskUsage(newState.name()));
        });
    }

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.DISK_USAGE)
            return;

        DiskUsageState usageState = DiskUsageState.NOT_AVAILABLE;
        try
        {
            usageState = DiskUsageState.valueOf(value.value);
        }
        catch (IllegalArgumentException e)
        {
            noSpamLogger.warn(String.format("Found unknown DiskUsageState: %s. Using default state %s instead.",
                                            value.value, usageState));
        }
        usageInfo.put(endpoint, usageState);

        hasStuffedOrFullNode = usageState.isStuffedOrFull() || computeHasStuffedOrFullNode();
    }

    private boolean computeHasStuffedOrFullNode()
    {
        for (DiskUsageState replicaState : usageInfo.values())
        {
            if (replicaState.isStuffedOrFull())
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        updateDiskUsage(endpoint, epState);
    }

    @Override
    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // nothing to do here
    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
        updateDiskUsage(endpoint, state);
    }

    @Override
    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        // do nothing, as we don't care about dead nodes
    }

    @Override
    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {
        updateDiskUsage(endpoint, state);
    }

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        usageInfo.remove(endpoint);
        hasStuffedOrFullNode = usageInfo.values().stream().anyMatch(DiskUsageState::isStuffedOrFull);
    }

    private void updateDiskUsage(InetAddressAndPort endpoint, EndpointState state)
    {
        VersionedValue localValue = state.getApplicationState(ApplicationState.DISK_USAGE);

        if (localValue != null)
        {
            onChange(endpoint, ApplicationState.DISK_USAGE, localValue);
        }
    }
}
