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

package org.apache.cassandra.db.virtual;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.SystemKeyspace.LEGACY_PEERS;
import static org.apache.cassandra.db.SystemKeyspace.PEERS_V2;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;

public class PeersTable extends AbstractVirtualTable
{
    public static String PEER = "peer";
    public static String PEER_PORT = "peer_port";
    public static String DATA_CENTER = "data_center";
    public static String HOST_ID = "host_id";
    public static String PREFERRED_IP = "preferred_ip";
    public static String PREFERRED_PORT = "preferred_port";
    public static String RACK = "rack";
    public static String RELEASE_VERSION = "release_version";
    public static String NATIVE_ADDRESS = "native_address";
    public static String NATIVE_PORT = "native_port";
    public static String SCHEMA_VERSION = "schema_version";
    public static String TOKENS = "tokens";
    public static String STATE = "state";

    public PeersTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "peers")
                           .comment("Peers")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(InetAddressType.instance))
                           .addPartitionKeyColumn(PEER, InetAddressType.instance)
                           .addClusteringColumn(PEER_PORT, Int32Type.instance)
                           .addRegularColumn(DATA_CENTER, UTF8Type.instance)
                           .addRegularColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(HOST_ID, UUIDType.instance)
                           .addRegularColumn(PREFERRED_IP, InetAddressType.instance)
                           .addRegularColumn(PREFERRED_PORT, Int32Type.instance)
                           .addRegularColumn(NATIVE_ADDRESS, InetAddressType.instance)
                           .addRegularColumn(NATIVE_PORT, Int32Type.instance)
                           .addRegularColumn(RELEASE_VERSION, UTF8Type.instance)
                           .addRegularColumn(SCHEMA_VERSION, UUIDType.instance)
                           .addRegularColumn(STATE, UTF8Type.instance)
                           .addRegularColumn(TOKENS, SetType.getInstance(UTF8Type.instance, false))
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        ClusterMetadata metadata = ClusterMetadata.current();
        for (InetAddressAndPort addr : metadata.directory.allAddresses())
        {
            NodeId peer = metadata.directory.peerId(addr);

            NodeAddresses addresses = metadata.directory.getNodeAddresses(peer);
            result.row(addr.getAddress(), addr.getPort())
                  .column(DATA_CENTER, metadata.directory.location(peer).datacenter)
                  .column(RACK, metadata.directory.location(peer).rack)
                  .column(HOST_ID, peer.uuid)
                  .column(PREFERRED_IP, addresses.broadcastAddress.getAddress())
                  .column(PREFERRED_PORT, addresses.broadcastAddress.getPort())
                  .column(NATIVE_ADDRESS, addresses.nativeAddress.getAddress())
                  .column(NATIVE_PORT, addresses.nativeAddress.getPort())
                  .column(RELEASE_VERSION, metadata.directory.version(peer).cassandraVersion.toString())
                  .column(SCHEMA_VERSION, Schema.instance.getVersion()) //TODO
                  .column(STATE, metadata.directory.peerState(peer).toString())
                  .column(TOKENS, new HashSet<>(metadata.tokenMap.tokens(peer).stream().map((token) -> token.getToken().getTokenValue().toString()).collect(Collectors.toList())));
        }

        return result;
    }

    public static void initializeLegacyPeerTables(ClusterMetadata prev, ClusterMetadata next)
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE %s.%s", SYSTEM_KEYSPACE_NAME, PEERS_V2));
        QueryProcessor.executeInternal(String.format("TRUNCATE %s.%s", SYSTEM_KEYSPACE_NAME, LEGACY_PEERS));

        for (NodeId nodeId : next.directory.peerIds())
            updateLegacyPeerTable(nodeId, prev, next);
    }

    private static String peers_v2_query = "INSERT INTO %s.%s ("
                                            + "peer, peer_port, "
                                            + "preferred_ip, preferred_port, "
                                            + "native_address, native_port, "
                                            + "data_center, rack, "
                                            + "host_id, "
                                            + "release_version, "
                                            + "schema_version,"
                                            + "tokens) " +
                                            "VALUES " +
                                            "(?,?,?,?,?,?,?,?,?,?,?,?)";

    private static String legacy_peers_query = "INSERT INTO %s.%s ("
                                               + "peer, preferred_ip, rpc_address, "
                                               + "data_center, rack, "
                                               + "host_id, "
                                               + "release_version, "
                                               + "schema_version,"
                                               + "tokens) " +
                                               "VALUES " +
                                               "(?,?,?,?,?,?,?,?,?)";

    private static String peers_delete_query = "DELETE FROM %s.%s WHERE peer=? and peer_port=?";
    private static String legacy_peers_delete_query = "DELETE FROM %s.%s WHERE peer=?";

    private static final Logger logger = LoggerFactory.getLogger(PeersTable.class);
    public static void updateLegacyPeerTable(NodeId nodeId, ClusterMetadata prev, ClusterMetadata next)
    {
        if (nodeId.equals(next.directory.peerId(FBUtilities.getBroadcastAddressAndPort())))
            return;

        if (next.directory.peerState(nodeId) == null || next.directory.peerState(nodeId) == NodeState.LEFT)
        {
            NodeAddresses addresses = prev.directory.getNodeAddresses(nodeId);
            logger.debug("Purging {} from system.peers_v2 table", addresses);
            QueryProcessor.executeInternal(String.format(peers_delete_query, SYSTEM_KEYSPACE_NAME, PEERS_V2), addresses.broadcastAddress.getAddress(), addresses.broadcastAddress.getPort());
            QueryProcessor.executeInternal(String.format(legacy_peers_delete_query, SYSTEM_KEYSPACE_NAME, LEGACY_PEERS), addresses.broadcastAddress.getAddress());
        }
        else
        {
            NodeAddresses addresses = next.directory.getNodeAddresses(nodeId);
            Location location = next.directory.location(nodeId);

            Set<String> tokens = SystemKeyspace.tokensAsSet(next.tokenMap.tokens(nodeId));
            QueryProcessor.executeInternal(String.format(peers_v2_query, SYSTEM_KEYSPACE_NAME, PEERS_V2),
                                           addresses.broadcastAddress.getAddress(), addresses.broadcastAddress.getPort(),
                                           addresses.broadcastAddress.getAddress(), addresses.broadcastAddress.getPort(),
                                           addresses.nativeAddress.getAddress(), addresses.nativeAddress.getPort(),
                                           location.datacenter, location.rack,
                                           nodeId.uuid,
                                           next.directory.version(nodeId).cassandraVersion.toString(),
                                           next.schema.getVersion(),
                                           tokens);

            QueryProcessor.executeInternal(String.format(legacy_peers_query, SYSTEM_KEYSPACE_NAME, LEGACY_PEERS),
                                           addresses.broadcastAddress.getAddress(), addresses.broadcastAddress.getAddress(), addresses.nativeAddress.getAddress(),
                                           location.datacenter, location.rack,
                                           nodeId.uuid,
                                           next.directory.version(nodeId).cassandraVersion.toString(),
                                           next.schema.getVersion(),
                                           tokens);
        }
    }
}