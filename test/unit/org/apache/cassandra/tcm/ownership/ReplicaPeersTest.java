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

package org.apache.cassandra.tcm.ownership;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.CassandraTestBase.DisableMBeanRegistration;
import org.apache.cassandra.CassandraTestBase.PrepareServerNoRegister;
import org.apache.cassandra.CassandraTestBase.UseMurmur3Partitioner;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.CMSLookup;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.ReplicaPeers.Options;
import org.apache.cassandra.utils.PojoToString;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the node-centric view of replicas based on TCM data placements.
 */
@PrepareServerNoRegister
@DisableMBeanRegistration
@UseMurmur3Partitioner
public class ReplicaPeersTest extends CassandraTestBase
{
    private static final String RF1_KEYSPACE = "replica_peers_rf1";
    private static final String RF2_KEYSPACE = "replica_peers_rf2";
    private static final String RF3_KEYSPACE = "replica_peers_rf3";
    private static final String RF3_TWIN_KEYSPACE = "replica_peers_rf3_twin";
    private static final String NTS_KEYSPACE = "replica_peers_nts";

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
    public void testSingleNodeClusterHasNoPeers()
    {
        join(1, 100, "dc1");
        createKeyspace(RF1_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 1}");

        ReplicaPeers peers = compute(RF1_KEYSPACE);
        assertThat(peers.peersByNodeId()).hasSize(1);
        assertThat(peers.peersOf(new NodeId(1))).isNotNull();
        assertThat(peers.peersByNodeId().values().iterator().next().all()).isEmpty();
        assertFormattable(peers);
    }

    @Test
    public void testRingPeersAtRf3()
    {
        int size = 6;
        for (int i = 1; i <= size; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");

        ReplicaPeers peers = compute(RF3_KEYSPACE);
        assertThat(peers.peersByNodeId()).hasSize(size);

        // A node co-replicates with the two nodes preceding and the two following it on the ring, and no others.
        for (int i = 1; i <= size; i++)
        {
            Set<Integer> expected = new TreeSet<>(Set.of(wrap(size, i - 2), wrap(size, i - 1), wrap(size, i + 1), wrap(size, i + 2)));
            assertThat(flatten(sets(peers, i).read()))
                .as("read peers of node " + i)
                .isEqualTo(expected);
            assertThat(flatten(sets(peers, i).write()))
                .as("write peers of node " + i)
                .isEqualTo(expected);
        }

        assertSymmetric(peers);
        assertFormattable(peers);
    }

    @Test
    public void testEveryNodeIsAPeerWhenRfEqualsClusterSize()
    {
        for (int i = 1; i <= 3; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");

        ReplicaPeers peers = compute(RF3_KEYSPACE);
        for (int i = 1; i <= 3; i++)
        {
            Set<Integer> others = new TreeSet<>(Set.of(1, 2, 3));
            others.remove(i);
            assertThat(flatten(sets(peers, i).read())).isEqualTo(others);
        }
        assertSymmetric(peers);
    }

    @Test
    public void testPeersAreBucketedByDatacenter()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc2");
        join(3, 300, "dc1");
        join(4, 400, "dc2");
        createKeyspace(NTS_KEYSPACE, "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");

        ReplicaPeers peers = compute(NTS_KEYSPACE);

        // RF equals the node count in each DC, so every node replicates every range.
        ReplicaPeers.PeerSets node1Peers = sets(peers, 1);
        assertThat(node1Peers.read().keySet()).isEqualTo(Set.of("dc1", "dc2"));
        assertThat(ids(node1Peers.read().get("dc1"))).isEqualTo(Set.of(3));
        assertThat(ids(node1Peers.read().get("dc2"))).isEqualTo(Set.of(2, 4));

        // Cross-DC peers are present without being asked for, which is what makes them separable.
        ReplicaPeers.PeerSets node2 = sets(peers, 2);
        assertThat(ids(node2.read().get("dc1"))).isEqualTo(Set.of(1, 3));
        assertThat(ids(node2.read().get("dc2"))).isEqualTo(Set.of(4));

        assertSymmetric(peers);
        assertFormattable(peers);
    }

    @Test
    public void testDatacenterFilterSelectsNodesReportedOnButNotTheirPeers()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc2");
        join(3, 300, "dc1");
        join(4, 400, "dc2");
        createKeyspace(NTS_KEYSPACE, "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");

        ClusterMetadata metadata = ClusterMetadata.current();
        ReplicaPeers unfiltered = compute(metadata, Set.of(NTS_KEYSPACE));
        ReplicaPeers peers = ReplicaPeers.compute(metadata, options(metadata, null, Set.of("dc1"), Set.of(NTS_KEYSPACE), null));

        // Only dc1 nodes are reported on.
        assertThat(nodeIds(peers)).isEqualTo(Set.of(1, 3));
        for (int i : new int[]{ 1, 3 })
        {
            // Peers in every datacenter are listed.
            assertThat(sets(peers, i).read().keySet()).isEqualTo(Set.of("dc1", "dc2"));
            assertThat(flatten(sets(peers, i).read())).isEqualTo(flatten(sets(unfiltered, i).read()));
            assertThat(flatten(sets(peers, i).write())).isEqualTo(flatten(sets(unfiltered, i).write()));
        }
        assertThat(ids(sets(peers, 1).read().get("dc1"))).isEqualTo(Set.of(3));
        assertThat(ids(sets(peers, 1).read().get("dc2"))).isEqualTo(Set.of(2, 4));

        // dc2 peers are not reported on, but the endpoints section must still resolve them.
        assertThat(((Map<?, ?>) peers.toMap().get("endpoints")).keySet()).isEqualTo(Set.of(1, 2, 3, 4));
        assertFormattable(peers);
    }

    @Test
    public void testNamedNodeListsPeersInEveryDatacenter()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc2");
        join(3, 300, "dc1");
        join(4, 400, "dc2");
        createKeyspace(NTS_KEYSPACE, "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");

        ClusterMetadata metadata = ClusterMetadata.current();
        ReplicaPeers peers = ReplicaPeers.compute(metadata, options(metadata, "2", null, Set.of(NTS_KEYSPACE), null));
        assertThat(nodeIds(peers)).isEqualTo(Set.of(2));
        // The cross-DC peers of a dc2 node, which is what an operator working on dc2 needs to see.
        assertThat(ids(sets(peers, 2).read().get("dc1"))).isEqualTo(Set.of(1, 3));
        assertThat(ids(sets(peers, 2).read().get("dc2"))).isEqualTo(Set.of(4));
        assertThat(ids(sets(peers, 2).write().get("dc1"))).isEqualTo(Set.of(1, 3));
        assertThat(ids(sets(peers, 2).write().get("dc2"))).isEqualTo(Set.of(4));
    }

    @Test
    public void testEveryDatacenterRequestedIsTheSameAsNoFilter()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc2");
        join(3, 300, "dc1");
        join(4, 400, "dc2");
        createKeyspace(NTS_KEYSPACE, "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}");

        ClusterMetadata metadata = ClusterMetadata.current();
        ReplicaPeers peers = ReplicaPeers.compute(metadata, options(metadata, null, Set.of("dc1", "dc2"), Set.of(NTS_KEYSPACE), null));
        assertThat(peers.toMap()).isEqualTo(compute(metadata, Set.of(NTS_KEYSPACE)).toMap());
        assertSymmetric(peers);
    }

    @Test
    public void testNodeCannotBeCombinedWithDatacenters()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc2");
        createKeyspace(NTS_KEYSPACE, "{'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}");

        ClusterMetadata metadata = ClusterMetadata.current();
        // Rejected whether or not the node is in one of the datacenters, since a node already determines its own.
        for (Set<String> dcs : List.of(Set.of("dc1"), Set.of("dc2"), Set.of("dc1", "dc2")))
        {
            assertThatThrownBy(() -> options(metadata, "1", dcs, Set.of(NTS_KEYSPACE), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot combine a node with a datacenter restriction");
        }
        // An empty datacenter set means no restriction, so it combines with a node.
        assertThat(nodeIds(ReplicaPeers.compute(metadata, options(metadata, "1", Set.of(), Set.of(NTS_KEYSPACE), null)))).isEqualTo(Set.of(1));
    }

    @Test
    public void testUnionsAcrossReplicationOfEverySelectedKeyspace()
    {
        for (int i = 1; i <= 6; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF2_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 2}");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");

        ClusterMetadata metadata = ClusterMetadata.current();
        Set<Integer> rf2Peers = flatten(sets(compute(metadata, Set.of(RF2_KEYSPACE)), 1).read());
        Set<Integer> rf3Peers = flatten(sets(compute(metadata, Set.of(RF3_KEYSPACE)), 1).read());
        Set<Integer> both = flatten(sets(compute(metadata, Set.of(RF2_KEYSPACE, RF3_KEYSPACE)), 1).read());

        assertThat(rf2Peers).isEqualTo(Set.of(2, 6));
        assertThat(rf3Peers).isEqualTo(Set.of(2, 3, 5, 6));
        // The union must satisfy both configurations at once, which is the whole point of unioning.
        Set<Integer> union = new TreeSet<>(rf2Peers);
        union.addAll(rf3Peers);
        assertThat(both).isEqualTo(union);
        assertThat(both).isEqualTo(rf3Peers);
    }

    @Test
    public void testKeyspacesSharingReplicationAreCollapsed()
    {
        int size = 4;
        for (int i = 1; i <= size; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        createKeyspace(RF3_TWIN_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");

        ClusterMetadata metadata = ClusterMetadata.current();
        ReplicaPeers rf3Replicas = compute(metadata, Set.of(RF3_KEYSPACE));
        ReplicaPeers combinedReplicas = compute(metadata, Set.of(RF3_KEYSPACE, RF3_TWIN_KEYSPACE));

        assertThat(combinedReplicas.keyspaces()).isEqualTo(Set.of(RF3_KEYSPACE, RF3_TWIN_KEYSPACE));
        for (int i = 1; i <= size; i++)
            assertThat(flatten(sets(combinedReplicas, i).read())).isEqualTo(flatten(sets(rf3Replicas, i).read()));
    }

    @Test
    public void testEveryReplicatedKeyspaceIsIncludedByDefault()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc1");
        createKeyspace(RF2_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 2}");

        ClusterMetadata metadata = ClusterMetadata.current();

        // Replicated system keyspaces constrain rolling operations just as user keyspaces do.
        ReplicaPeers everything = ReplicaPeers.compute(metadata, options(metadata, null, null, null, null));
        assertThat(everything.keyspaces()).contains(RF2_KEYSPACE, SchemaConstants.METADATA_KEYSPACE_NAME);

        // An empty exclusion list means the same as none.
        ReplicaPeers emptyExclusions = ReplicaPeers.compute(metadata, options(metadata, null, null, null, Set.of()));
        assertThat(emptyExclusions.keyspaces()).isEqualTo(everything.keyspaces());

        ReplicaPeers excluded = ReplicaPeers.compute(metadata, options(metadata, null, null, null,
                                                                       Set.of(SchemaConstants.METADATA_KEYSPACE_NAME)));
        assertThat(excluded.keyspaces()).contains(RF2_KEYSPACE)
                                        .doesNotContain(SchemaConstants.METADATA_KEYSPACE_NAME);

        // LocalStrategy keyspaces can never establish a peer, so they are never selected.
        for (String keyspace : everything.keyspaces())
        {
            assertThat(metadata.schema.getKeyspaces().getNullable(keyspace).params.replication.isLocal())
                .as(keyspace)
                .isFalse();
        }
    }

    @Test
    public void testCmsPeersMatchTheCmsPlacement()
    {
        for (int i = 1; i <= 4; i++)
            join(i, i * 100L, "dc1");
        ClusterMetadata metadata = withCms(Set.of(1, 2), Set.of(3));

        ReplicaPeers peers = computeCms(metadata);
        // Full members read and write, a joining member only writes, and a node outside the CMS does neither.
        assertThat(flatten(sets(peers, 1).read())).isEqualTo(Set.of(2));
        assertThat(flatten(sets(peers, 1).write())).isEqualTo(Set.of(2, 3));
        assertThat(flatten(sets(peers, 3).read())).isEmpty();
        assertThat(flatten(sets(peers, 3).write())).isEqualTo(Set.of(1, 2));
        assertThat(sets(peers, 4).all()).isEmpty();

        // Peers are taken from CMS membership rather than from the CMS placement, so they must agree with it.
        DataPlacement placement = metadata.placement(metadata.schema.getKeyspaces()
                                                                    .getNullable(SchemaConstants.METADATA_KEYSPACE_NAME)
                                                                    .params.replication);
        for (int i = 1; i <= 4; i++)
        {
            NodeId nodeId = metadata.directory.peerId(ClusterMetadataTestHelper.addr(i));
            assertThat(flatten(sets(peers, nodeId).read()))
                .as("read peers of " + nodeId)
                .isEqualTo(peersIn(metadata, placement.reads, nodeId));
            assertThat(flatten(sets(peers, nodeId).write()))
                .as("write peers of " + nodeId)
                .isEqualTo(peersIn(metadata, placement.writes, nodeId));
        }
        assertSymmetric(peers);
        assertFormattable(peers);
    }

    @Test
    public void testCmsMemberWithUncommittedAddressChangeIsAPeer()
    {
        for (int i = 1; i <= 3; i++)
            join(i, i * 100L, "dc1");
        ClusterMetadata metadata = withCms(Set.of(1, 2, 3), Set.of());

        // A CMS member has moved to an address which is not yet committed to the directory, as Startup does when it
        // finds CMS members answering at new addresses. The CMS placement then names the new address.
        InetAddressAndPort newAddress = ClusterMetadataTestHelper.addr(99);
        overrideAddress(metadata, 2, newAddress);
        // Precondition: the placement and the directory disagree on the moved member's address.
        assertThat(metadata.directory.peerId(newAddress)).isNull();
        assertThat(metadata.fullCMSMembers()).contains(newAddress);

        ReplicaPeers peers = computeCms(metadata);
        for (int i = 1; i <= 3; i++)
        {
            NodeId nodeId = metadata.directory.peerId(ClusterMetadataTestHelper.addr(i));
            Set<Integer> others = new TreeSet<>(Set.of(1, 2, 3));
            others.remove(nodeId.id());
            assertThat(flatten(sets(peers, nodeId).read())).isEqualTo(others);
            assertThat(flatten(sets(peers, nodeId).write())).isEqualTo(others);
        }

        // The moved member is reported at the address it answers on, and every other node at its committed address.
        NodeId moved = metadata.directory.peerId(ClusterMetadataTestHelper.addr(2));
        Map<?, ?> endpoints = (Map<?, ?>) peers.toMap().get("endpoints");
        for (int i = 1; i <= 3; i++)
        {
            NodeId nodeId = metadata.directory.peerId(ClusterMetadataTestHelper.addr(i));
            InetAddressAndPort expected = nodeId.equals(moved) ? newAddress : ClusterMetadataTestHelper.addr(i);
            assertThat(endpoints.get(nodeId.id())).isEqualTo(expected.getHostAddress(true));
        }
        assertFormattable(peers);
    }

    @Test
    public void testCmsMemberMovedToAnotherNodesAddressIsNotConfusedWithIt()
    {
        for (int i = 1; i <= 4; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        ClusterMetadata metadata = withCms(Set.of(1, 2, 3), Set.of());
        Map<String, Object> rf3Before = compute(metadata, Set.of(RF3_KEYSPACE)).toMap();
        Object peersBefore = rf3Before.get("peers");
        Object endpointsBefore = rf3Before.get("endpoints");

        // The CMS placement names CMS member 2 by the address which the directory still records for node 4.
        InetAddressAndPort fourth = ClusterMetadataTestHelper.addr(4);
        overrideAddress(metadata, 2, fourth);
        assertThat(metadata.fullCMSMembers()).contains(fourth);

        ReplicaPeers cmsPeers = computeCms(metadata);
        assertThat(flatten(sets(cmsPeers, 1).read())).isEqualTo(Set.of(2, 3));
        assertThat(flatten(sets(cmsPeers, 2).read())).isEqualTo(Set.of(1, 3));
        // Node 4 is not a CMS member, whatever address a CMS member is overridden to.
        assertThat(sets(cmsPeers, 4).all()).isEmpty();

        // Data placements name node 4 by its committed address, so its peers are unaffected by the override.
        Map<String, Object> rf3After = compute(metadata, Set.of(RF3_KEYSPACE)).toMap();
        assertThat(rf3After.get("peers")).isEqualTo(peersBefore);
        // Only the moved member's reported address changes, to the one it answers on.
        Map<Object, Object> expectedEndpoints = new HashMap<>((Map<?, ?>) endpointsBefore);
        expectedEndpoints.put(metadata.directory.peerId(ClusterMetadataTestHelper.addr(2)).id(), fourth.getHostAddress(true));
        assertThat(rf3After.get("endpoints")).isEqualTo(expectedEndpoints);
    }

    @Test
    public void testReportsNodesWhichAreRegisteredButHoldNoRanges()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc1");
        createKeyspace(RF2_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 2}");
        NodeId registeredOnly = ClusterMetadataTestHelper.register(ClusterMetadataTestHelper.addr(9), "dc1", "rack1");

        ReplicaPeers peers = compute(RF2_KEYSPACE);
        // A registered node with no ranges must be reported with no peers rather than omitted, so that callers can
        // tell it apart from a node we have never heard of.
        assertThat(peers.peersByNodeId()).containsKey(registeredOnly);
        assertThat(peers.peersByNodeId().get(registeredOnly).all()).isEmpty();
    }

    @Test
    public void testBootstrappingNodeIsAWritePeerBeforeItIsAReadPeer()
    {
        for (int i = 1; i <= 4; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");

        InetAddressAndPort joining = ClusterMetadataTestHelper.addr(5);
        ClusterMetadataTestHelper.register(joining, "dc1", "rack1");
        // Stop after START_JOIN: the node is in the write placement but not yet in the read placement.
        ClusterMetadataTestHelper.lazyJoin(joining, new Murmur3Partitioner.LongToken(250))
                                 .prepareJoin()
                                 .startJoin();

        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId joiningId = metadata.directory.peerId(joining);
        ReplicaPeers peers = compute(metadata, Set.of(RF3_KEYSPACE));

        ReplicaPeers.PeerSets joiningPeers = sets(peers, joiningId);
        // A bootstrapping node reads nothing yet, but is already a write replica.
        assertThat(joiningPeers.read()).isEmpty();
        assertThat(joiningPeers.write()).isNotEmpty();

        boolean writePeerSomewhere = false;
        for (Map.Entry<NodeId, ReplicaPeers.PeerSets> entry : peers.peersByNodeId().entrySet())
        {
            if (entry.getKey().equals(joiningId))
                continue;
            // The joining node is not a read peer of any other node.
            assertThat(flatten(entry.getValue().read()))
                .as("read peers of " + entry.getKey())
                .doesNotContain(joiningId.id());
            if (flatten(entry.getValue().write()).contains(joiningId.id()))
                writePeerSomewhere = true;
        }
        // The joining node must be a write peer of the nodes it is taking over from.
        assertThat(writePeerSomewhere).isTrue();
        assertFormattable(peers);
    }

    @Test
    public void testLeavingNodeRemainsAPeerUntilItHasFinished()
    {
        for (int i = 1; i <= 4; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");

        InetAddressAndPort leaving = ClusterMetadataTestHelper.addr(4);
        ClusterMetadataTestHelper.lazyLeave(leaving).prepareLeave().startLeave();

        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId leavingId = metadata.directory.peerId(leaving);
        ReplicaPeers peers = compute(metadata, Set.of(RF3_KEYSPACE));

        // It is still a read replica, so operating on it and one of its peers at once is still unsafe.
        assertThat(sets(peers, leavingId).read()).isNotEmpty();
        assertFormattable(peers);
    }

    @Test
    public void testLeftNodeIsNotReported()
    {
        for (int i = 1; i <= 4; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        NodeId leftId = leave(4);

        ClusterMetadata metadata = ClusterMetadata.current();
        ReplicaPeers peers = compute(metadata, Set.of(RF3_KEYSPACE));

        // A LEFT node is not part of the ring, so it must not be reported on as though it were a rangeless node.
        assertThat(nodeIds(peers)).isEqualTo(Set.of(1, 2, 3));
        // A LEFT node is nobody's peer.
        for (ReplicaPeers.PeerSets sets : peers.peersByNodeId().values())
            assertThat(sets.all()).doesNotContain(leftId);
        assertThat(endpointIds(peers.toMap())).doesNotContain(leftId.id());
        assertSymmetric(peers);
        assertFormattable(peers);
    }

    @Test
    public void testLeftNodeCannotBeNamed()
    {
        for (int i = 1; i <= 4; i++)
            join(i, i * 100L, "dc1");
        createKeyspace(RF3_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 3}");
        NodeId leftId = leave(4);

        ClusterMetadata metadata = ClusterMetadata.current();
        for (String identifier : new String[]{ Integer.toString(leftId.id()),
                                               ClusterMetadataTestHelper.addr(4).getHostAddress(true),
                                               metadata.directory.hostId(leftId).toString() })
        {
            assertThatThrownBy(() -> Options.resolveNode(metadata, identifier))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has left the cluster");
            assertThatThrownBy(() -> options(metadata, identifier, null, Set.of(RF3_KEYSPACE), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has left the cluster");
        }
    }

    @Test
    public void testDatacenterWhoseNodesHaveAllLeftIsUnknown()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc1");
        join(3, 300, "dc2");
        createKeyspace(RF2_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 2}");

        ClusterMetadata before = ClusterMetadata.current();
        assertThat(nodeIds(ReplicaPeers.compute(before, options(before, null, Set.of("dc2"), Set.of(RF2_KEYSPACE), null))))
            .isEqualTo(Set.of(3));

        leave(3);
        ClusterMetadata metadata = ClusterMetadata.current();
        // Precondition: the directory still remembers dc2.
        assertThat(metadata.directory.knownDatacenters()).contains("dc2");

        assertThatThrownBy(() -> options(metadata, null, Set.of("dc2"), Set.of(RF2_KEYSPACE), null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown datacenter: dc2");

        assertThat(nodeIds(ReplicaPeers.compute(metadata, options(metadata, null, Set.of("dc1"), Set.of(RF2_KEYSPACE), null))))
            .isEqualTo(Set.of(1, 2));
    }

    @Test
    public void testResolvesAnAddressReportedForACmsMemberWithUncommittedAddressChange()
    {
        for (int i = 1; i <= 3; i++)
            join(i, i * 100L, "dc1");
        ClusterMetadata metadata = withCms(Set.of(1, 2, 3), Set.of());
        NodeId moved = metadata.directory.peerId(ClusterMetadataTestHelper.addr(2));

        InetAddressAndPort newAddress = ClusterMetadataTestHelper.addr(99);
        overrideAddress(metadata, 2, newAddress);

        // The address the output reports for the node resolves to it, and so does its committed address
        String reported = (String) ((Map<?, ?>) computeCms(metadata).toMap().get("endpoints")).get(moved.id());
        assertThat(reported).isEqualTo(newAddress.getHostAddress(true));
        assertThat(Options.resolveNode(metadata, reported)).isEqualTo(moved);
        assertThat(Options.resolveNode(metadata, ClusterMetadataTestHelper.addr(2).getHostAddress(true))).isEqualTo(moved);
    }

    @Test
    public void testAddressOfTwoNodesDuringUncommittedAddressChangeIsAmbiguous()
    {
        for (int i = 1; i <= 4; i++)
            join(i, i * 100L, "dc1");
        ClusterMetadata metadata = withCms(Set.of(1, 2, 3), Set.of());

        // CMS member 2 is reported at the address which the directory still records for node 4
        InetAddressAndPort fourth = ClusterMetadataTestHelper.addr(4);
        overrideAddress(metadata, 2, fourth);

        assertThatThrownBy(() -> Options.resolveNode(metadata, fourth.getHostAddress(true)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("name the node by its id");
        // Either node can still be named by its id
        NodeId moved = metadata.directory.peerId(ClusterMetadataTestHelper.addr(2));
        assertThat(Options.resolveNode(metadata, Integer.toString(moved.id()))).isEqualTo(moved);
    }

    @Test
    public void testResolvesEveryAcceptedNodeIdentifierForm() throws Exception
    {
        join(1, 100, "dc1");
        join(2, 200, "dc1");

        ClusterMetadata metadata = ClusterMetadata.current();
        InetAddressAndPort second = ClusterMetadataTestHelper.addr(2);
        NodeId expected = metadata.directory.peerId(second);

        // A bare number must be read as a node id: InetAddressAndPort.getByName("2") would resolve it to 0.0.0.2.
        assertThat(Options.resolveNode(metadata, Integer.toString(expected.id()))).isEqualTo(expected);
        assertThat(Options.resolveNode(metadata, second.getHostAddress(false))).isEqualTo(expected);
        assertThat(Options.resolveNode(metadata, second.getHostAddress(true))).isEqualTo(expected);
        assertThat(Options.resolveNode(metadata, metadata.directory.hostId(expected).toString())).isEqualTo(expected);

        assertThatThrownBy(() -> Options.resolveNode(metadata, ""))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Options.resolveNode(metadata, "9999"))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Options.resolveNode(metadata, "127.0.0.99"))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Options.resolveNode(metadata, UUID.randomUUID().toString()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testRejectsUnusableArguments()
    {
        join(1, 100, "dc1");
        join(2, 200, "dc1");
        createKeyspace(RF2_KEYSPACE, "{'class': 'SimpleStrategy', 'replication_factor': 2}");

        ClusterMetadata metadata = ClusterMetadata.current();

        assertThatThrownBy(() -> options(metadata, null, null, Set.of("no_such_keyspace"), null))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> options(metadata, null, null, null, Set.of("no_such_keyspace")))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> options(metadata, null, Set.of("no_such_dc"), null, null))
            .isInstanceOf(IllegalArgumentException.class);
        // Naming a keyspace and an exclusion list at once is ambiguous.
        assertThatThrownBy(() -> options(metadata, null, null, Set.of(RF2_KEYSPACE), Set.of(RF2_KEYSPACE)))
            .isInstanceOf(IllegalArgumentException.class);
        // Excluding every replicated keyspace would report no peers for any node, which reads as safe.
        Set<String> replicated = ReplicaPeers.compute(metadata, options(metadata, null, null, null, null)).keyspaces();
        assertThatThrownBy(() -> options(metadata, null, null, null, replicated))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No replicated keyspace is left");
    }

    private static int wrap(int size, int idx)
    {
        return ((idx - 1 + size) % size) + 1;
    }

    @SuppressWarnings("UnusedReturnValue")
    private static NodeId join(int idx, long token, String dc)
    {
        InetAddressAndPort address = ClusterMetadataTestHelper.addr(idx);
        ClusterMetadataTestHelper.addEndpoint(address, new Murmur3Partitioner.LongToken(token), dc, "rack1");
        return ClusterMetadata.current().directory.peerId(address);
    }

    /** Decommission a node to completion, asserting that the directory retains it in the LEFT state. */
    private static NodeId leave(int idx)
    {
        InetAddressAndPort address = ClusterMetadataTestHelper.addr(idx);
        NodeId nodeId = ClusterMetadata.current().directory.peerId(address);
        ClusterMetadataTestHelper.leave(address);
        // Precondition: a decommissioned node stays in the directory as LEFT.
        assertThat(ClusterMetadata.current().directory.peerState(nodeId)).isEqualTo(NodeState.LEFT);
        return nodeId;
    }

    private static void createKeyspace(String name, String replication)
    {
        ClusterMetadataTestHelper.createKeyspace("CREATE KEYSPACE " + name + " WITH REPLICATION = " + replication + ';');
    }

    /**
     * The current metadata with exactly the given nodes as full and as joining CMS members. Not committed, since an
     * address override is applied to the instance itself.
     */
    private static ClusterMetadata withCms(Set<Integer> full, Set<Integer> joining)
    {
        ClusterMetadata current = ClusterMetadata.current();
        Set<NodeId> fullIds = nodeIdsOf(current, full);
        Set<NodeId> joiningIds = nodeIdsOf(current, joining);

        ClusterMetadata.Transformer transformer = current.transformer();
        for (NodeId nodeId : current.fullCMSMemberIds())
        {
            if (!fullIds.contains(nodeId))
                transformer = transformer.leaveCMS(nodeId);
        }
        for (NodeId nodeId : fullIds)
        {
            if (!current.fullCMSMemberIds().contains(nodeId))
                transformer = transformer.startJoiningCMS(nodeId).finishJoiningCMS(nodeId);
        }
        for (NodeId nodeId : joiningIds)
            transformer = transformer.startJoiningCMS(nodeId);

        ClusterMetadata metadata = transformer.build().metadata;
        assertThat(metadata.cmsMembership.fullMembers()).isEqualTo(fullIds);
        assertThat(metadata.cmsMembership.joiningMembers()).isEqualTo(joiningIds);
        return metadata;
    }

    private static Set<NodeId> nodeIdsOf(ClusterMetadata metadata, Set<Integer> indexes)
    {
        Set<NodeId> nodeIds = new HashSet<>();
        for (int idx : indexes)
            nodeIds.add(metadata.directory.peerId(ClusterMetadataTestHelper.addr(idx)));
        return nodeIds;
    }

    /** Point the CMS lookup of {@code metadata} at {@code address} for node {@code idx}, as Startup does. */
    private static void overrideAddress(ClusterMetadata metadata, int idx, InetAddressAndPort address)
    {
        InetAddressAndPort committed = ClusterMetadataTestHelper.addr(idx);
        NodeId nodeId = metadata.directory.peerId(committed);
        assertThat(metadata.initCMSLookup(CMSLookup.builder(metadata)
                                                   .withOverride(nodeId, committed, address)
                                                   .build())).isTrue();
    }

    private static ReplicaPeers computeCms(ClusterMetadata metadata)
    {
        return compute(metadata, Set.of(SchemaConstants.METADATA_KEYSPACE_NAME));
    }

    /** The other replicas of {@code nodeId} across every range of {@code groups}, resolved through the directory. */
    private static Set<Integer> peersIn(ClusterMetadata metadata, ReplicaGroups groups, NodeId nodeId)
    {
        Set<Integer> peers = new TreeSet<>();
        for (VersionedEndpoints.ForRange forRange : groups.endpoints)
        {
            Set<Integer> replicas = new TreeSet<>();
            forRange.get().endpoints().forEach(endpoint -> replicas.add(metadata.directory.peerId(endpoint).id()));
            if (replicas.remove(nodeId.id()))
                peers.addAll(replicas);
        }
        return peers;
    }

    private static ReplicaPeers compute(String keyspace)
    {
        return compute(ClusterMetadata.current(), Set.of(keyspace));
    }

    private static ReplicaPeers compute(ClusterMetadata metadata, Set<String> keyspaces)
    {
        return ReplicaPeers.compute(metadata, options(metadata, null, null, keyspaces, null));
    }

    private static Options options(ClusterMetadata metadata,
                                                String node,
                                                Set<String> datacenters,
                                                Set<String> keyspaces,
                                                Set<String> excludedKeyspaces)
    {
        return Options.resolve(metadata, node, datacenters, keyspaces, excludedKeyspaces);
    }

    private static ReplicaPeers.PeerSets sets(ReplicaPeers peers, int nodeIdx)
    {
        return sets(peers, ClusterMetadata.current().directory.peerId(ClusterMetadataTestHelper.addr(nodeIdx)));
    }

    private static ReplicaPeers.PeerSets sets(ReplicaPeers peers, NodeId nodeId)
    {
        ReplicaPeers.PeerSets sets = peers.peersOf(nodeId);
        assertThat(sets)
            .as("peer sets of " + nodeId)
            .isNotNull();
        return sets;
    }

    private static Set<Integer> nodeIds(ReplicaPeers peers)
    {
        Set<Integer> ids = new TreeSet<>();
        peers.peersByNodeId().keySet().forEach(id -> ids.add(id.id()));
        return ids;
    }

    private static Set<Integer> flatten(SortedMap<String, SortedSet<NodeId>> byDatacenter)
    {
        Set<Integer> ids = new TreeSet<>();
        byDatacenter.values().forEach(peers -> ids.addAll(ids(peers)));
        return ids;
    }

    private static Set<Integer> ids(Set<NodeId> nodeIds)
    {
        Set<Integer> ids = new TreeSet<>();
        if (nodeIds != null)
            nodeIds.forEach(id -> ids.add(id.id()));
        return ids;
    }

    /** The node ids resolved by the endpoints section of a {@link ReplicaPeers#toMap()} tree. */
    private static Set<Object> endpointIds(Map<String, Object> tree)
    {
        return new HashSet<>(((Map<?, ?>) tree.get("endpoints")).keySet());
    }

    /**
     * Asserts the following symmetry property:
     *
     * <p>If node 'y' is reported as a peer of node 'x', then node 'x' should be reported as a peer of node 'y'.
     */
    private static void assertSymmetric(ReplicaPeers peers)
    {
        Directory directory = ClusterMetadata.current().directory;

        for (NodeId from : peers.peersByNodeId().keySet())
        {
            String fromDc = directory.location(from).datacenter;

            for (String side : List.of("read", "write"))
            {
                for (Map.Entry<String, SortedSet<NodeId>> entry : side(peers.peersOf(from), side).entrySet())
                {
                    for (NodeId to : entry.getValue())
                    {
                        assertThat(directory.location(to).datacenter)
                            .as(side + " peer " + to + " of " + from + " is listed under its own datacenter")
                            .isEqualTo(entry.getKey());
                        assertHasPeer(peers, to, from, fromDc, side);
                    }
                }
            }
        }
    }

    /** Asserts that {@code peer} is listed as a {@code side} peer of {@code node}, under the datacenter {@code dc}. */
    private static void assertHasPeer(ReplicaPeers peers, NodeId node, NodeId peer, String dc, String side)
    {
        ReplicaPeers.PeerSets sets = peers.peersOf(node);
        assertThat(sets)
            .as("peer sets of " + node)
            .isNotNull();
        assertThat(side(sets, side).get(dc))
            .as(peer + " is a " + side + " peer of " + node + " in " + dc)
            .isNotNull()
            .contains(peer);
    }

    private static SortedMap<String, SortedSet<NodeId>> side(ReplicaPeers.PeerSets sets, String side)
    {
        return side.equals("read") ? sets.read() : sets.write();
    }

    /** The formatter rejects booleans and nulls, so every payload must be run through it. */
    private static void assertFormattable(ReplicaPeers peers)
    {
        Map<String, Object> tree = peers.toMap();
        assertThat(PojoToString.pojoMapToString(tree, "YAML")).isNotNull();
        assertThat(PojoToString.pojoMapToString(tree, "JSON")).isNotNull();
        assertThat(PojoToString.pojoMapToString(tree, "MINIFIED-JSON")).isNotNull();
    }
}
