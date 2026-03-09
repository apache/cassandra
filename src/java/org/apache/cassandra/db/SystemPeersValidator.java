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
 * Validator to ensure system.peers and system.peers_v2 tables match ClusterMetadata.
 * These tables are maintained for existing clients and tools which read from them to determine
 * topology and schema information, while Cassandra itself uses ClusterMetadata as the source of truth.
 *
 * This validator detects inconsistencies and automatically repairs them by synchronizing
 * the peers tables with the current ClusterMetadata.
 *
 * The system_views.peers virtual table provides a live view on the current ClusterMetadata
 * and includes all members of the cluster, unlike the legacy tables which exclude the local node.
 */
public final class SystemPeersValidator
{
    private static final Logger logger = LoggerFactory.getLogger(SystemPeersValidator.class);
    private static final String SELECT_PEERS_V2_QUERY = String.format("SELECT * FROM %s.%s",
                                                                      SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
    private static final String SELECT_PEERS_QUERY = String.format("SELECT * FROM %s.%s",
                                                                    SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);
    private static final String DELETE_PEERS_V2_QUERY = String.format("DELETE FROM %s.%s WHERE peer = ? AND peer_port = ?",
                                                                     SchemaConstants.SYSTEM_KEYSPACE_NAME, PEERS_V2);
    private static final String DELETE_PEERS_QUERY = String.format("DELETE FROM %s.%s WHERE peer = ?",
                                                                   SchemaConstants.SYSTEM_KEYSPACE_NAME, LEGACY_PEERS);

    public static void validateAndRepair(ClusterMetadata metadata)
    {
        Map<InetAddressAndPort, UntypedResultSet.Row> peersV2Rows = getPeersV2Rows();
        Map<InetAddress, UntypedResultSet.Row> peersRows = getPeersRows();

        Map<InetAddressAndPort, NodeId> knownEndpoints = new HashMap<>();
        Set<InetAddress> knownAddresses = new HashSet<>();
        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();
        for (InetAddressAndPort endpoint : metadata.directory.allJoinedEndpoints())
        {
            if (endpoint.equals(localEndpoint))
                continue;
            knownEndpoints.put(endpoint, metadata.directory.peerId(endpoint));
            knownAddresses.add(endpoint.getAddress());
        }

        for (InetAddressAndPort endpoint : peersV2Rows.keySet())
        {
            if (!knownEndpoints.containsKey(endpoint))
            {
                logger.info("Removing stale peer {} from {}", endpoint, PEERS_V2);
                executeInternal(DELETE_PEERS_V2_QUERY, endpoint.getAddress(), endpoint.getPort());
            }
        }

        for (InetAddress address : peersRows.keySet())
        {
            if (!knownAddresses.contains(address))
            {
                logger.info("Removing stale peer {} from {}", address, LEGACY_PEERS);
                executeInternal(DELETE_PEERS_QUERY, address);
            }
        }

        for (Map.Entry<InetAddressAndPort, NodeId> entry : knownEndpoints.entrySet())
        {
            NodeId nodeId = entry.getValue();
            InetAddressAndPort endpoint = entry.getKey();
            UntypedResultSet.Row peersV2Row = peersV2Rows.get(endpoint);
            UntypedResultSet.Row peersRow = peersRows.get(endpoint.getAddress());

            boolean peersV2NeedsRepair = needsRepair(peersV2Row, endpoint, PEERS_V2, nodeId, metadata);
            boolean peersNeedsRepair = needsRepair(peersRow, endpoint, LEGACY_PEERS, nodeId, metadata);

            if (peersV2NeedsRepair || peersNeedsRepair)
                PeersTable.updateLegacyPeerTable(nodeId, metadata, metadata);
        }
    }

    private static boolean needsRepair(UntypedResultSet.Row row, InetAddressAndPort endpoint,
                                       String table, NodeId nodeId, ClusterMetadata metadata)
    {
        if (row == null)
        {
            logger.info("Adding missing peer {} to {}", endpoint, table);
            return true;
        }
        boolean isEquivalent = PEERS_V2.equals(table)
                               ? peersV2RowIsEquivalent(row, nodeId, metadata)
                               : peersRowIsEquivalent(row, nodeId, metadata);
        if (!isEquivalent)
        {
            logger.info("Repairing mismatched peer {} in {}: {}", endpoint, table, row);
            return true;
        }
        return false;
    }

    private static boolean peersV2RowIsEquivalent(UntypedResultSet.Row row, NodeId nodeId, ClusterMetadata metadata)
    {
        NodeAddresses addresses = metadata.directory.getNodeAddresses(nodeId);
        return commonColumnsAreEquivalent(row, nodeId, addresses, metadata)
               && row.has("preferred_port") && row.getInt("preferred_port") == addresses.broadcastAddress.getPort()
               && row.has("native_port") && row.getInt("native_port") == addresses.nativeAddress.getPort();
    }

    private static boolean peersRowIsEquivalent(UntypedResultSet.Row row, NodeId nodeId, ClusterMetadata metadata)
    {
        NodeAddresses addresses = metadata.directory.getNodeAddresses(nodeId);
        return commonColumnsAreEquivalent(row, nodeId, addresses, metadata);
    }

    private static boolean commonColumnsAreEquivalent(UntypedResultSet.Row row, NodeId nodeId,
                                                      NodeAddresses addresses, ClusterMetadata metadata)
    {
        Location location = metadata.directory.location(nodeId);
        // This column is differently named in the peers and peers_v2 tables
        String nativeAddressColumn = row.has("native_address") ? "native_address" : "rpc_address";

        // Check existence first because row.getXXX can NPE if the column is not present
        return row.has("preferred_ip") && Objects.equals(row.getInetAddress("preferred_ip"), addresses.broadcastAddress.getAddress())
               && row.has(nativeAddressColumn) && Objects.equals(row.getInetAddress(nativeAddressColumn), addresses.nativeAddress.getAddress())
               && row.has("data_center") && Objects.equals(row.getString("data_center"), location.datacenter)
               && row.has("rack") && Objects.equals(row.getString("rack"), location.rack)
               && row.has("host_id") && Objects.equals(row.getUUID("host_id"), nodeId.toUUID())
               && row.has("release_version") && Objects.equals(row.getString("release_version"), metadata.directory.version(nodeId).cassandraVersion.toString())
               && row.has("schema_version") && Objects.equals(row.getUUID("schema_version"), metadata.schema.getVersion())
               && row.has("tokens") && Objects.equals(row.getSet("tokens", UTF8Type.instance), SystemKeyspace.tokensAsSet(metadata.tokenMap.tokens(nodeId)));
    }

    private static Map<InetAddressAndPort, UntypedResultSet.Row> getPeersV2Rows()
    {
        Map<InetAddressAndPort, UntypedResultSet.Row> rows = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal(SELECT_PEERS_V2_QUERY))
            rows.put(InetAddressAndPort.getByAddressOverrideDefaults(row.getInetAddress("peer"),
                                                                     row.getInt("peer_port")), row);
        return rows;
    }

    private static Map<InetAddress, UntypedResultSet.Row> getPeersRows()
    {
        Map<InetAddress, UntypedResultSet.Row> rows = new HashMap<>();
        for (UntypedResultSet.Row row : executeInternal(SELECT_PEERS_QUERY))
            rows.put(row.getInetAddress("peer"), row);
        return rows;
    }
}
