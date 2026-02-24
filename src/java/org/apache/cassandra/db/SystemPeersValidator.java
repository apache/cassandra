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

package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.virtual.PeersTable;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.LEGACY_PEERS;
import static org.apache.cassandra.db.SystemKeyspace.PEERS_V2;

/**
 * Validator to ensure system.peers and system.peers_v2 tables match ClusterMetadata on startup.
 * This is critical for backward compatibility as older clients and tools read from these
 * legacy tables while TCM uses ClusterMetadata as the source of truth.
 *
 * The validator detects inconsistencies and automatically repairs them by synchronizing
 * the peers tables with the current ClusterMetadata.
 */
public class SystemPeersValidator
{
    private static final Logger logger = LoggerFactory.getLogger(SystemPeersValidator.class);

    public static void validateAndRepair(ClusterMetadata metadata)
    {
        if (metadata != null)
        {
            Set<NodeId> expectedNodes = getExpectedPeerNodes(metadata);
            Set<InetAddress> legacyPeersEntries = getLegacyPeersEntries();

            Map<InetAddressAndPort, NodeId> expectedEndpoints = new HashMap<>();
            Map<InetAddress, NodeId> expectedAddresses = new HashMap<>();
            for (NodeId nodeId : expectedNodes)
            {
                InetAddressAndPort endpoint = metadata.directory.endpoint(nodeId);
                expectedEndpoints.put(endpoint, nodeId);
                expectedAddresses.put(endpoint.getAddress(), nodeId);
            }

            Set<InetAddressAndPort> peersV2Entries = getPeersV2Entries();
            for (Map.Entry<InetAddressAndPort, NodeId> entry : expectedEndpoints.entrySet())
            {
                InetAddressAndPort endpoint = entry.getKey();
                NodeId nodeId = entry.getValue();

                boolean inPeersV2 = peersV2Entries.contains(endpoint);
                boolean inLegacyPeers = legacyPeersEntries.contains(endpoint.getAddress());

                if (!inPeersV2 || !inLegacyPeers)
                {
                    logger.info("Repairing missing peer entry for {} (nodeId={}, inPeersV2={}, inLegacyPeers={})",
                                endpoint, nodeId, inPeersV2, inLegacyPeers);
                    PeersTable.updateLegacyPeerTable(nodeId, metadata, metadata);
                }
            }

            String deleteV2Query = String.format("DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?",
                                                 SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
            for (InetAddressAndPort endpoint : peersV2Entries)
            {
                if (!expectedEndpoints.containsKey(endpoint))
                {
                    logger.info("Removing stale entry from {}: endpoint {} not found in ClusterMetadata",
                                PEERS_V2, endpoint);
                    executeInternal(deleteV2Query, endpoint.getAddress(), endpoint.getPort());
                }
            }

            String deleteQuery = String.format("DELETE FROM %s.%s WHERE peer = ?",
                                                 SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
            for (InetAddress address : legacyPeersEntries)
            {
                if (!expectedAddresses.containsKey(address))
                {
                    logger.info("Removing stale entry from {}: address {} not found in ClusterMetadata",
                                LEGACY_PEERS, address);
                    executeInternal(deleteQuery, address);
                }
            }
        }
    }

    private static Set<NodeId> getExpectedPeerNodes(ClusterMetadata metadata)
    {
        Set<NodeId> expectedNodes = new HashSet<>();
        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();

        for (InetAddressAndPort endpoint : metadata.directory.allJoinedEndpoints())
        {
            if (endpoint.equals(localEndpoint)) continue;
            expectedNodes.add(metadata.directory.peerId(endpoint));
        }
        return expectedNodes;
    }

    private static Set<InetAddressAndPort> getPeersV2Entries()
    {
        Set<InetAddressAndPort> entries = new HashSet<>();
        String query = String.format("SELECT peer, peer_port FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
        UntypedResultSet rows = executeInternal(query);

        for (UntypedResultSet.Row row : rows)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByAddressOverrideDefaults(
                row.getInetAddress("peer"),
                row.getInt("peer_port"));
            entries.add(endpoint);
        }
        return entries;
    }

    private static Set<InetAddress> getLegacyPeersEntries()
    {
        Set<InetAddress> entries = new HashSet<>();
        String query = String.format("SELECT peer FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
        UntypedResultSet rows = executeInternal(query);

        for (UntypedResultSet.Row row : rows)
            entries.add(row.getInetAddress("peer"));

        return entries;
    }
}