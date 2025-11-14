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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.membership.Location;
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
    private final ConcurrentMap<String, Set<InetAddressAndPort>> fullNodesByDatacenter = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<InetAddressAndPort>> stuffedNodesByDatacenter = new ConcurrentHashMap<>();

    @VisibleForTesting
    public DiskUsageBroadcaster(DiskUsageMonitor monitor)
    {
        this.monitor = monitor;
        // TODO: switch to TCM?
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

    /**
     * @return {@code true} if there exists any node in the datacenter of {@code endpoint} which has FULL disk usage.
     */
    @VisibleForTesting
    public boolean isDatacenterFull(String datacenter)
    {
        if (!hasStuffedOrFullNode())
        {
            return false;
        }
        Set<InetAddressAndPort> fullNodes = fullNodesByDatacenter.get(datacenter);
        return fullNodes != null && !fullNodes.isEmpty();
    }

    /**
     * @return {@code true} if there exists any node in the datacenter of {@code endpoint} which has FULL disk usage
     */
    @VisibleForTesting
    public boolean isDatacenterStuffed(String datacenter)
    {
        if (!hasStuffedOrFullNode())
        {
            return false;
        }
        Set<InetAddressAndPort> stuffedNodes = stuffedNodesByDatacenter.get(datacenter);
        return stuffedNodes != null && !stuffedNodes.isEmpty();
    }

    @VisibleForTesting
    public DiskUsageState state(InetAddressAndPort endpoint)
    {
        return usageInfo.getOrDefault(endpoint, DiskUsageState.NOT_AVAILABLE);
    }

    public void startBroadcasting()
    {
        monitor.start(newState -> {
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

        computeUsageStateForEpDatacenter(endpoint, usageState);
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

    /**
     * Update the set of full nodes by datacenter based on the disk usage state for the given endpoint.
     * If the node is FULL, add it to the set for its datacenter. Otherwise, remove it from the set.
     * This method is idempotent - adding an already-present node or removing an absent node has no effect.
     *
     * @param endpoint   The endpoint whose state has changed.
     * @param usageState The new disk usage state value.
     */
    private void computeUsageStateForEpDatacenter(InetAddressAndPort endpoint, DiskUsageState usageState)
    {
        Location location = location(endpoint);
        if (location.equals(Location.UNKNOWN))
        {
            noSpamLogger.warn("Unable to track disk usage by datacenter for endpoint {} because we are unable to determine its location.",
                         endpoint);
            return;
        }

        String datacenter = location.datacenter;
        if (usageState.isFull())
        {
            // Add this node to the set of full nodes for its datacenter and remove it from the stuffed nodes
            // if it was there.
            fullNodesByDatacenter.computeIfAbsent(datacenter, dc -> ConcurrentHashMap.newKeySet())
                                 .add(endpoint);
            noSpamLogger.debug("Endpoint {} is FULL, added to full nodes set for datacenter {}", endpoint, datacenter);
            Set<InetAddressAndPort> stuffedNodes = stuffedNodesByDatacenter.get(datacenter);
            if (stuffedNodes != null && stuffedNodes.remove(endpoint))
            {
                noSpamLogger.debug("Endpoint {} is now FULL. Removed it from the stuffed nodes set for datacenter {}",
                             endpoint, datacenter);
            }
        }
        else if (usageState.isStuffed())
        {
            // Add this node to the set of stuffed nodes for its datacenter and remove it from the full nodes
            // if it was there.
            stuffedNodesByDatacenter.computeIfAbsent(datacenter, dc -> ConcurrentHashMap.newKeySet())
                                    .add(endpoint);
            noSpamLogger.debug("Endpoint {} is now STUFFED. Added it to the stuffed nodes set for datacenter {}",
                         endpoint, datacenter);
            Set<InetAddressAndPort> fullNodes = fullNodesByDatacenter.get(datacenter);
            if (fullNodes != null && fullNodes.remove(endpoint))
            {
                noSpamLogger.debug("Endpoint {} is now STUFFED. Removed it from full nodes set for datacenter {}",
                             endpoint, datacenter);
            }
        }
        else
        {
            // Remove this node from the set of full nodes and set of stuffed nodes for its datacenter if it was there.
            Set<InetAddressAndPort> fullNodes = fullNodesByDatacenter.get(datacenter);
            if (fullNodes != null && fullNodes.remove(endpoint))
            {
                noSpamLogger.debug("Endpoint {} is no longer STUFFED or FULL, removed from stuffed for datacenter {}",
                             endpoint, datacenter);
            }
            Set<InetAddressAndPort> stuffedNodes = stuffedNodesByDatacenter.get(datacenter);
            if (stuffedNodes != null && stuffedNodes.remove(endpoint))
            {
                noSpamLogger.debug("Endpoint {} is no longer STUFFED, removed from the stuffed set for datacenter {}",
                             endpoint, datacenter);
            }
        }
    }

    private Location location(InetAddressAndPort endpoint)
    {
        Locator locator = DatabaseDescriptor.getLocator();
        if (locator == null)
        {
            noSpamLogger.warn("Unable to track disk usage by datacenter for endpoint {} because locator is null",
                              endpoint);
            return Location.UNKNOWN;
        }
        Location location = locator.location(endpoint);
        return location != null ? location : Location.UNKNOWN;
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
        updateDiskUsageStateForDatacenterOnRemoval(endpoint);
        usageInfo.remove(endpoint);
        hasStuffedOrFullNode = usageInfo.values().stream().anyMatch(DiskUsageState::isStuffedOrFull);
    }

    private void updateDiskUsageStateForDatacenterOnRemoval(InetAddressAndPort endpoint)
    {
        Location nodeLocation = location(endpoint);
        if (nodeLocation.equals(Location.UNKNOWN))
        {
            logger.debug("Unable to determine location for removed endpoint {}. Will not update datacenter tracking.", endpoint);
            return;
        }

        String datacenter = nodeLocation.datacenter;
        // Remove the endpoint from the full nodes and stuffed nodes set for its datacenter
        Set<InetAddressAndPort> fullNodes = fullNodesByDatacenter.get(datacenter);
        if (fullNodes != null && fullNodes.remove(endpoint))
        {
            logger.debug("Removed endpoint {} from full nodes set for datacenter {} on node removal", endpoint, datacenter);
        }
        Set<InetAddressAndPort> stuffedNodes = stuffedNodesByDatacenter.get(datacenter);
        if (stuffedNodes != null && stuffedNodes.remove(endpoint))
        {
            logger.debug("Removed endpoint {} from stuffed nodes set for datacenter {} on node removal", endpoint, datacenter);
        }
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
