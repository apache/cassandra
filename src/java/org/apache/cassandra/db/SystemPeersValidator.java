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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.PeersTable;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
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
        Map<InetAddressAndPort, UntypedResultSet.Row> peersV2Rows = getPeersV2Rows();
        Map<InetAddress, UntypedResultSet.Row> legacyPeersRows = getLegacyPeersRows();

        Map<InetAddressAndPort, NodeId> expectedEndpoints = new HashMap<>();
        Map<InetAddress, NodeId> expectedAddresses = new HashMap<>();
        for (NodeId nodeId : getExpectedPeerNodes(metadata))
        {
            InetAddressAndPort endpoint = metadata.directory.endpoint(nodeId);
            expectedEndpoints.put(endpoint, nodeId);
            expectedAddresses.put(endpoint.getAddress(), nodeId);
        }

        String deleteV2Query = String.format("DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?",
                                               SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
        for (InetAddressAndPort endpoint : peersV2Rows.keySet())
        {
            if (!expectedEndpoints.containsKey(endpoint))
            {
                logger.info("Removing stale peer {} from {}", endpoint, PEERS_V2);
                executeInternal(deleteV2Query, endpoint.getAddress(), endpoint.getPort());
            }
        }

        String deleteLegacyQuery = String.format("DELETE FROM %s.%s WHERE peer = ?",
                                                   SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                                   LEGACY_PEERS);
        for (InetAddress address : legacyPeersRows.keySet())
        {
            if (!expectedAddresses.containsKey(address))
            {
                logger.info("Removing stale peer {} from {}", address, LEGACY_PEERS);
                executeInternal(deleteLegacyQuery, address);
            }
        }

        for (Map.Entry<InetAddressAndPort, NodeId> entry : expectedEndpoints.entrySet())
        {
            NodeId nodeId = entry.getValue();
            InetAddressAndPort endpoint = entry.getKey();
            UntypedResultSet.Row v2Row = peersV2Rows.get(endpoint);
            UntypedResultSet.Row legacyRow = legacyPeersRows.get(endpoint.getAddress());

            List<String> v2Discrepancies = collectV2Discrepancies(v2Row, nodeId, metadata);
            List<String> legacyDiscrepancies = collectLegacyDiscrepancies(legacyRow, nodeId, metadata);

            boolean v2NeedsRepair = logDiscrepancies(v2Row, endpoint, PEERS_V2, v2Discrepancies);
            boolean legacyNeedsRepair = logDiscrepancies(legacyRow, endpoint, LEGACY_PEERS, legacyDiscrepancies);

            if (v2NeedsRepair || legacyNeedsRepair)
                PeersTable.updateLegacyPeerTable(nodeId, metadata, metadata);
        }
    }

    private static boolean logDiscrepancies(UntypedResultSet.Row row,
                                            InetAddressAndPort endpoint,
                                            String table,
                                            List<String> discrepancies)
    {
        if (row == null)
        {
            logger.info("Adding missing peer {} to {}", endpoint, table);
            return true;
        }
        if (!discrepancies.isEmpty())
        {
            logger.info("Updating peer {} in {} for stale fields {}", endpoint, table, discrepancies);
            return true;
        }
        return false;
    }

    private static List<String> collectV2Discrepancies(UntypedResultSet.Row row,
                                                       NodeId nodeId,
                                                       ClusterMetadata metadata)
    {
        if (row == null)
            return Collections.emptyList();

        NodeAddresses addresses = metadata.directory.getNodeAddresses(nodeId);
        Location location = metadata.directory.location(nodeId);
        List<String> discrepancies = new ArrayList<>();
        collectIfStale(discrepancies, "preferred_ip", row.getInetAddress("preferred_ip"),
                       addresses.broadcastAddress.getAddress());
        collectIfStale(discrepancies, "preferred_port", row.getInt("preferred_port"),
                       addresses.broadcastAddress.getPort());
        collectIfStale(discrepancies, "native_address", row.getInetAddress("native_address"),
                       addresses.nativeAddress.getAddress());
        collectIfStale(discrepancies, "native_port", row.getInt("native_port"),
                       addresses.nativeAddress.getPort());
        collectIfStale(discrepancies, "data_center", row.getString("data_center"), location.datacenter);
        collectIfStale(discrepancies, "rack", row.getString("rack"), location.rack);
        collectIfStale(discrepancies, "host_id", row.getUUID("host_id"), nodeId.toUUID());
        collectIfStale(discrepancies, "release_version", row.getString("release_version"),
                       metadata.directory.version(nodeId).cassandraVersion.toString());
        collectIfStale(discrepancies, "schema_version", row.getUUID("schema_version"),
                       metadata.schema.getVersion());
        collectIfStale(discrepancies, "tokens", row.getSet("tokens", UTF8Type.instance),
                       SystemKeyspace.tokensAsSet(metadata.tokenMap.tokens(nodeId)));
        return discrepancies;
    }

    private static List<String> collectLegacyDiscrepancies(UntypedResultSet.Row row, NodeId nodeId,
                                                           ClusterMetadata metadata)
    {
        if (row == null)
            return Collections.emptyList();

        NodeAddresses addresses = metadata.directory.getNodeAddresses(nodeId);
        Location location = metadata.directory.location(nodeId);
        List<String> discrepancies = new ArrayList<>();
        collectIfStale(discrepancies, "preferred_ip", row.getInetAddress("preferred_ip"),
                       addresses.broadcastAddress.getAddress());
        collectIfStale(discrepancies, "rpc_address", row.getInetAddress("rpc_address"),
                       addresses.nativeAddress.getAddress());
        collectIfStale(discrepancies, "data_center", row.getString("data_center"), location.datacenter);
        collectIfStale(discrepancies, "rack", row.getString("rack"), location.rack);
        collectIfStale(discrepancies, "host_id", row.getUUID("host_id"), nodeId.toUUID());
        collectIfStale(discrepancies, "release_version", row.getString("release_version"),
                       metadata.directory.version(nodeId).cassandraVersion.toString());
        collectIfStale(discrepancies, "schema_version", row.getUUID("schema_version"),
                       metadata.schema.getVersion());
        collectIfStale(discrepancies, "tokens", row.getSet("tokens", UTF8Type.instance),
                       SystemKeyspace.tokensAsSet(metadata.tokenMap.tokens(nodeId)));
        return discrepancies;
    }

    private static void collectIfStale(List<String> discrepancies, String field, Object actual, Object expected)
    {
        if (!Objects.equals(actual, expected))
            discrepancies.add(field);
    }

    private static Set<NodeId> getExpectedPeerNodes(ClusterMetadata metadata)
    {
        Set<NodeId> expectedNodes = new HashSet<>();
        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();
        for (InetAddressAndPort endpoint : metadata.directory.allJoinedEndpoints())
        {
            if (!endpoint.equals(localEndpoint))
                expectedNodes.add(metadata.directory.peerId(endpoint));
        }

        return expectedNodes;
    }

    private static Map<InetAddressAndPort, UntypedResultSet.Row> getPeersV2Rows()
    {
        Map<InetAddressAndPort, UntypedResultSet.Row> rows = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
        for (UntypedResultSet.Row row : executeInternal(query))
            rows.put(InetAddressAndPort.getByAddressOverrideDefaults(row.getInetAddress("peer"),
                                                                     row.getInt("peer_port")), row);
        return rows;
    }

    private static Map<InetAddress, UntypedResultSet.Row> getLegacyPeersRows()
    {
        Map<InetAddress, UntypedResultSet.Row> rows = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
        for (UntypedResultSet.Row row : executeInternal(query))
            rows.put(row.getInetAddress("peer"), row);

        return rows;
    }
}
