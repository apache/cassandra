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

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.EndpointLookup;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.ReplicaPeers;
import org.apache.cassandra.tcm.ownership.ReplicaPeers.Options;
import org.apache.cassandra.tcm.ownership.ReplicaPeers.PeerSets;

/**
 * A virtual table that exposes the replica peers of the current node.
 *
 * Replica peers are nodes that share ownership of the same token ranges. There is one row per peer, ordered by the
 * peer's datacenter and then its node id. {@code read_peer} and {@code write_peer} tell whether the peer shares data
 * with this node in the read and in the write placement; the two differ only while nodes are joining or leaving.
 * The data is sourced from the node's current cluster metadata.
 */
public final class ReplicaPeersTable extends AbstractVirtualTable
{
    private static final String NODE_ID = "node_id";
    private static final String PEER_DC = "peer_dc";
    private static final String PEER_ID = "peer_id";
    private static final String PEER_ADDRESS = "peer_address";
    private static final String READ_PEER = "read_peer";
    private static final String WRITE_PEER = "write_peer";
    private static final String EPOCH = "epoch";

    ReplicaPeersTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "replica_peers")
                           .comment("Replica peers of current node")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(Int32Type.instance))
                           .addPartitionKeyColumn(NODE_ID, Int32Type.instance)
                           .addClusteringColumn(PEER_DC, UTF8Type.instance)
                           .addClusteringColumn(PEER_ID, Int32Type.instance)
                           .addRegularColumn(PEER_ADDRESS, UTF8Type.instance)
                           .addRegularColumn(READ_PEER, BooleanType.instance)
                           .addRegularColumn(WRITE_PEER, BooleanType.instance)
                           .addRegularColumn(EPOCH, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        ClusterMetadata clusterMetadata = ClusterMetadata.current();

        if (clusterMetadata.myNodeState() == NodeState.LEFT)
        {
            throw new InvalidRequestException("This node has left the cluster.");
        }

        NodeId myNodeId = clusterMetadata.myNodeId();
        if (myNodeId == NodeId.UNREGISTERED)
        {
            throw new InvalidRequestException("This node is not yet registered in cluster metadata.");
        }

        Options options = Options.resolve(clusterMetadata, String.valueOf(myNodeId.id()), Set.of(), Set.of(), Set.of());
        ReplicaPeers replicaPeers = ReplicaPeers.compute(clusterMetadata, options);
        PeerSets peerSets = replicaPeers.peersOf(myNodeId);

        SortedSet<String> datacenters = new TreeSet<>(peerSets.read().keySet());
        datacenters.addAll(peerSets.write().keySet());
        long epoch = clusterMetadata.epoch.getEpoch();
        // The current address of each peer, which for a CMS member may be a new address not yet committed
        EndpointLookup lookup = clusterMetadata.endpointLookup();

        SimpleDataSet result = new SimpleDataSet(metadata);
        for (String dc : datacenters)
        {
            SortedSet<NodeId> readPeers = peerSets.read().getOrDefault(dc, Collections.emptySortedSet());
            SortedSet<NodeId> writePeers = peerSets.write().getOrDefault(dc, Collections.emptySortedSet());

            SortedSet<NodeId> peers = new TreeSet<>(readPeers);
            peers.addAll(writePeers);
            for (NodeId peer : peers)
            {
                InetAddressAndPort endpoint = lookup.endpoint(peer);
                result.row(myNodeId.id(), dc, peer.id())
                      .column(PEER_ADDRESS, endpoint == null ? null : endpoint.getHostAddress(true))
                      .column(READ_PEER, readPeers.contains(peer))
                      .column(WRITE_PEER, writePeers.contains(peer))
                      .column(EPOCH, epoch);
            }
        }
        return result;
    }
}
