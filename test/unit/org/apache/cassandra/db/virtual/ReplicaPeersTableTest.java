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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.CassandraTestBase.DisableMBeanRegistration;
import org.apache.cassandra.CassandraTestBase.PrepareServerNoRegister;
import org.apache.cassandra.CassandraTestBase.UseMurmur3Partitioner;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.ReplicaPeers;
import org.apache.cassandra.tcm.ownership.ReplicaPeers.Options;
import org.apache.cassandra.tcm.ownership.ReplicaPeers.PeerSets;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@PrepareServerNoRegister
@DisableMBeanRegistration
@UseMurmur3Partitioner
public class ReplicaPeersTableTest extends CassandraTestBase
{
    private static final String KS = "vts_replica_peers";
    private static final String SELECT_REPLICA_PEERS = "SELECT * FROM " + KS + ".replica_peers";
    private static final String TEST_KEYSPACE = "replica_peers_table_ks";

    @BeforeClass
    public static void registerTable()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS, ImmutableList.of(new ReplicaPeersTable(KS))));
    }

    @Before
    public void setup()
    {
        ServerTestUtils.recreateCMS();
    }

    @After
    public void teardown()
    {
        ServerTestUtils.resetCMS();
    }

    @Test
    public void testQueryRejectedBeforeNodeRegistration()
    {
        assertThatThrownBy(() -> QueryProcessor.executeInternal(SELECT_REPLICA_PEERS))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("not yet registered");
    }

    @Test
    public void testRegisteredNodeWithoutRangesHasNoPeers()
    {
        ClusterMetadataTestHelper.register(1, "dc1", "rack1");
        join(2, 200, "dc1");
        createKeyspace("{'class': 'SimpleStrategy', 'replication_factor': 2}");

        assertThat(QueryProcessor.executeInternal(SELECT_REPLICA_PEERS)).isEmpty();
    }

    @Test
    public void testNodeListsPeersInEveryDatacenter()
    {
        NodeId self = join(1, 100, "dc1");
        join(2, 200, "dc2");
        join(3, 300, "dc1");
        join(4, 400, "dc2");
        createKeyspace("{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");

        ClusterMetadata metadata = ClusterMetadata.current();
        UntypedResultSet rows = QueryProcessor.executeInternal(SELECT_REPLICA_PEERS);

        // NTS replicates every range to both datacenters, so every other node is a read and write peer. Rows are
        // ordered by datacenter and then node id.
        assertThat(rowsOf(rows)).containsExactly(peerRow(metadata, self, "dc1", 3, true, true),
                                                 peerRow(metadata, self, "dc2", 2, true, true),
                                                 peerRow(metadata, self, "dc2", 4, true, true));

        // Same answer as the class it wraps, on the same snapshot
        Options options = Options.resolve(metadata, String.valueOf(self.id()), null, null, null);
        PeerSets expected = ReplicaPeers.compute(metadata, options).peersOf(self);
        assertThat(peerIds(rows, "dc1")).isEqualTo(ids(expected.read().get("dc1")));
        assertThat(peerIds(rows, "dc2")).isEqualTo(ids(expected.read().get("dc2")));
    }

    @Test
    public void testBootstrappingNodeIsAWritePeerOnly()
    {
        NodeId self = join(1, 100, "dc1");
        for (int i = 2; i <= 4; i++)
            join(i, i * 100L, "dc1");
        createKeyspace("{'class': 'SimpleStrategy', 'replication_factor': 3}");

        startJoin(5, 250, "dc1");

        ClusterMetadata metadata = ClusterMetadata.current();
        assertThat(rowsOf(QueryProcessor.executeInternal(SELECT_REPLICA_PEERS)))
            .containsExactly(peerRow(metadata, self, "dc1", 2, true, true),
                             peerRow(metadata, self, "dc1", 3, true, true),
                             peerRow(metadata, self, "dc1", 4, true, true),
                             peerRow(metadata, self, "dc1", 5, false, true));
    }

    @Test
    public void testFiltersOnNonClusteringColumns()
    {
        NodeId self = join(1, 100, "dc1");
        for (int i = 2; i <= 4; i++)
            join(i, i * 100L, "dc1");
        createKeyspace("{'class': 'SimpleStrategy', 'replication_factor': 3}");
        startJoin(5, 250, "dc1");

        // Virtual tables filter implicitly, so regular columns can be restricted without ALLOW FILTERING
        ClusterMetadata metadata = ClusterMetadata.current();
        assertThat(rowsOf(QueryProcessor.executeInternal(SELECT_REPLICA_PEERS + " WHERE read_peer = false")))
            .containsExactly(peerRow(metadata, self, "dc1", 5, false, true));

        String address = ClusterMetadataTestHelper.addr(3).getHostAddress(true);
        assertThat(rowsOf(QueryProcessor.executeInternal(SELECT_REPLICA_PEERS + " WHERE peer_address = ?", address)))
            .containsExactly(peerRow(metadata, self, "dc1", 3, true, true));
    }

    @Test
    public void testQueryRejectedAfterNodeLeftTheCluster()
    {
        NodeId self = join(1, 100, "dc1");
        join(2, 200, "dc1");

        // A real decommission is rejected because this node is the CMS, so install the LEFT state directly
        ClusterMetadata current = ClusterMetadata.current();
        ClusterMetadata left = current.transformer()
                                      .withNodeState(self, NodeState.LEFT)
                                      .build()
                                      .metadata
                                      .forceEpoch(current.epoch.nextEpoch());
        ClusterMetadataService.instance().commit(new ForceSnapshot(left));

        assertThatThrownBy(() -> QueryProcessor.executeInternal(SELECT_REPLICA_PEERS))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("has left the cluster");
    }

    /** Every row as a list of its values, in table column order. */
    private static List<List<Object>> rowsOf(UntypedResultSet rows)
    {
        List<List<Object>> result = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            result.add(List.of(row.getInt("node_id"),
                               row.getString("peer_dc"),
                               row.getInt("peer_id"),
                               row.getString("peer_address"),
                               row.getBoolean("read_peer"),
                               row.getBoolean("write_peer"),
                               row.getLong("epoch")));
        return result;
    }

    /** The row expected for the peer at {@code peerIdx}, in the same form as {@link #rowsOf}. */
    private static List<Object> peerRow(ClusterMetadata metadata, NodeId self, String dc, int peerIdx,
                                        boolean readPeer, boolean writePeer)
    {
        InetAddressAndPort address = ClusterMetadataTestHelper.addr(peerIdx);
        return List.of(self.id(),
                       dc,
                       metadata.directory.peerId(address).id(),
                       address.getHostAddress(true),
                       readPeer,
                       writePeer,
                       metadata.epoch.getEpoch());
    }

    private static List<Integer> peerIds(UntypedResultSet rows, String dc)
    {
        List<Integer> ids = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
        {
            if (row.getString("peer_dc").equals(dc))
                ids.add(row.getInt("peer_id"));
        }
        return ids;
    }

    private static List<Integer> ids(Iterable<NodeId> nodeIds)
    {
        List<Integer> ids = new ArrayList<>();
        nodeIds.forEach(id -> ids.add(id.id()));
        return ids;
    }

    private static NodeId join(int idx, long token, String dc)
    {
        InetAddressAndPort address = ClusterMetadataTestHelper.addr(idx);
        ClusterMetadataTestHelper.addEndpoint(address, new Murmur3Partitioner.LongToken(token), dc, "rack1");
        return ClusterMetadata.current().directory.peerId(address);
    }

    /** Register a node and stop after START_JOIN, so it is in the write placement but not yet the read placement. */
    private static void startJoin(int idx, long token, String dc)
    {
        InetAddressAndPort address = ClusterMetadataTestHelper.addr(idx);
        ClusterMetadataTestHelper.register(address, dc, "rack1");
        ClusterMetadataTestHelper.lazyJoin(address, new Murmur3Partitioner.LongToken(token))
                                 .prepareJoin()
                                 .startJoin();
    }

    private static void createKeyspace(String replication)
    {
        ClusterMetadataTestHelper.createKeyspace("CREATE KEYSPACE " + TEST_KEYSPACE +
                                                 " WITH REPLICATION = " + replication + ';');
    }
}
