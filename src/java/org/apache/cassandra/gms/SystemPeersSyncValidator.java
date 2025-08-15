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

package org.apache.cassandra.gms;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.GossipMetrics;
import org.apache.cassandra.utils.FBUtilities;

public class SystemPeersSyncValidator implements Runnable
{
    public final static Logger logger = LoggerFactory.getLogger(SystemPeersSyncValidator.class);
    public final static SystemPeersSyncValidator instance = new SystemPeersSyncValidator();
    ScheduledFuture<?> validateTask;

    /**
     * Periodic task that validates consistency between the `system.peers_v2` table and the in-memory endpoint state map.
     * <p>
     * In a healthy cluster, the `system.peers_v2` table should only contain entries for endpoints that are currently known
     * to the Gossiper. This assumption is critical for logic that depends on reading system.peers_v2, e.g.
     * 1. {@code Gossiper.instance.examineShadowState()}
     * 2. {@code Gossiper.instance.addSavedEndpoint()}
     * 3. {@code Gossiper.instance.start()}
     * etc.
     * <p>
     * which load info from local system.peers_v2 through:
     * - {@code SystemKeyspace.loadHostIds()}
     * - {@code SystemKeyspace.loadTokens()}
     *
     * However, due to some edge cases `system.peers_v2` may contain stale or orphaned entries that no longer present in
     * {@code Gossiper}'s {@code endpointStateMap}.
     * These orphaned entries can cause unexpceted behavior in gossip and peer discovery mechanisms.
     * <p>
     * This task detects such inconsistencies by periodically scanning the table and logging any desynchronized entries.
     */
    @Override
    public void run()
    {
        // Note: here we're not acquiring lock on either endpointStateMap or system table, it's possible that some
        // updates happens between the time we read the system table and the time we read the endpointStateMap.
        Set<InetAddressAndPort> peers = SystemKeyspace.loadPeers();
        Map<InetAddressAndPort, EndpointState> endpointStatesMap = Gossiper.instance.getEndpointStateMap();
        Set<InetAddressAndPort> endpointStates = endpointStatesMap.keySet();

        // Orphan endpoint exists in system.peers_v2 but missing in Gossiper state
        Set<InetAddressAndPort> orphanPeers = new HashSet<>(peers);
        orphanPeers.removeAll(endpointStates);
        if (!orphanPeers.isEmpty())
        {
            logger.warn("Detected {} orphan entries in system.peers_v2 not present in Gossiper state: {}.",
                        orphanPeers.size(), orphanPeers);
            GossipMetrics.orphanPeerInSystemTable.inc();
        }

        // Endpoint exists in Gossiper but missing in system.peers_v2
        Set<InetAddressAndPort> missingPeers = new HashSet<>(endpointStates);
        missingPeers.removeAll(peers);
        // remove myself
        missingPeers.remove(FBUtilities.getBroadcastAddressAndPort());
        // remove endpoints that are "AdministrativelyInactive" (LEFT/TOKEN_REMOVED/HIBERNATE)
        missingPeers.removeIf(e -> Gossiper.instance.isAdministrativelyInactiveState(endpointStatesMap.get(e)));
        if (!missingPeers.isEmpty())
        {
            logger.warn("Detected {} missing entries in system.peers_v2 that are present and !(LEFT/TOKEN_REMOVED/HIBERNATE) in Gossiper state: {}.",
                        missingPeers.size(), missingPeers);
            GossipMetrics.missingPeerInGossip.inc();
        }
    }

    SystemPeersSyncValidator() {}

    public synchronized void setup()
    {
        if (isRunning())
        {
            logger.warn("SystemPeersSyncValidator is already running, skipping setup.");
            return;
        }

        if (DatabaseDescriptor.getSystemPeersSyncValidatorIntervalInMin() <= 0)
        {
            logger.info("SystemPeersSyncValidator is disabled: system_peers_sync_validator_interval={} <= 0 (min).",
                        DatabaseDescriptor.getSystemPeersSyncValidatorIntervalInMin());
            return;
        }
        validateTask = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(SystemPeersSyncValidator.instance,
                                                                               1,
                                                                               DatabaseDescriptor.getSystemPeersSyncValidatorIntervalInMin(),
                                                                               TimeUnit.MINUTES);
        logger.info("SystemPeersSyncValidator has been started.");
    }

    public synchronized boolean isRunning()
    {
        ScheduledFuture<?> validateTask = this.validateTask;
        return validateTask != null && !validateTask.isDone() && !validateTask.isCancelled();
    }

    public synchronized void stop()
    {
        if (isRunning())
        {
            if (validateTask != null)
                validateTask.cancel(true);
            validateTask = null;
            logger.info("SystemPeersSyncValidator has been stopped.");
        }
    }
}
