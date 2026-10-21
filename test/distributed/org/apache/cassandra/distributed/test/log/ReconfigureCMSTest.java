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

package org.apache.cassandra.distributed.test.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.cassandra.schema.SchemaConstants.METADATA_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.psjava.util.AssertStatus.assertTrue;

/**
 * @see org.apache.cassandra.tools.nodetool.CMSAdmin.ReconfigureCMS
 */
public class ReconfigureCMSTest extends FuzzTestBase
{
    @Test
    public void expandAndShrinkCMSTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build(6)
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                      .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.setUncaughtExceptionsFilter(t -> t.getMessage() != null && t.getMessage().contains("There are not enough nodes in dc0 datacenter to satisfy replication factor"));
            Random rnd = new Random(2);
            Supplier<Integer> nodeSelector = () -> rnd.nextInt(cluster.size() - 1) + 1;
            cluster.get(nodeSelector.get()).nodetoolResult("cms", "reconfigure", "0").asserts().failure();
            cluster.get(nodeSelector.get()).nodetoolResult("cms", "reconfigure", "500").asserts().failure();
            cluster.get(nodeSelector.get()).nodetoolResult("cms", "reconfigure", "5").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertEquals(5, metadata.fullCMSMembers().size());
                assertEquals(ReplicationParams.simpleMeta(5, metadata.directory.knownDatacenters()),
                             metadata.schema.getKeyspaceMetadata(METADATA_KEYSPACE_NAME).params.replication);
            });
            cluster.stream().forEach(i -> {
                Assert.assertTrue(i.executeInternal(String.format("SELECT * FROM %s.%s", METADATA_KEYSPACE_NAME, DistributedMetadataLogKeyspace.TABLE_NAME)).length > 0);
            });

            cluster.get(nodeSelector.get()).nodetoolResult("cms", "reconfigure", "1").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertEquals(1, metadata.fullCMSMembers().size());
                assertEquals(ReplicationParams.simpleMeta(1, metadata.directory.knownDatacenters()),
                             metadata.schema.getKeyspaceMetadata(METADATA_KEYSPACE_NAME).params.replication);
            });
        }
    }

    @Test
    public void cancelCMSReconfigurationTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                      .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                              .set("progress_barrier_default_consistency_level", ConsistencyLevel.ALL)
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadataService.instance().commit(new PrepareCMSReconfiguration.Complex(ReplicationParams.simple(3).asMeta(), Collections.emptySet()));
                ReconfigureCMS reconfigureCMS = (ReconfigureCMS) ClusterMetadata.current().inProgressSequences.get(ReconfigureCMS.SequenceKey.instance);
                ClusterMetadataService.instance().commit(reconfigureCMS.next);
                ProgressBarrier.propagateLast(MetaStrategy.affectedRanges(ClusterMetadata.current()));
                try
                {
                    ClusterMetadataService.instance().commit(reconfigureCMS.next);
                    Assert.fail("Should not be possible to commit same `advance` twice");
                }
                catch (Throwable t)
                {
                    Assert.assertTrue(t.getMessage().contains("This transformation (0) has already been applied"));
                }
                reconfigureCMS = (ReconfigureCMS) ClusterMetadata.current().inProgressSequences.get(ReconfigureCMS.SequenceKey.instance);
                Assert.assertNotNull(reconfigureCMS.next.activeTransition);
            });
            cluster.get(1).nodetoolResult("cms", "reconfigure", "--cancel").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                ProgressBarrier.propagateLast(MetaStrategy.affectedRanges(ClusterMetadata.current()));
                ClusterMetadata metadata = ClusterMetadata.current();
                Assert.assertNull(metadata.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance));
                assertEquals(2, metadata.fullCMSMembers().size());
                ReplicationParams params = ReplicationParams.meta(metadata);
                DataPlacement placements = metadata.placements().get(params);
                assertTrue(placements.reads.equivalentTo(placements.writes));
                assertEquals(metadata.fullCMSMembers().size(), Integer.parseInt(params.asMap().get("dc0")));
            });

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadataService.instance().commit(new PrepareCMSReconfiguration.Complex(ReplicationParams.simple(4).asMeta(), Collections.emptySet()));
                ProgressBarrier.propagateLast(MetaStrategy.affectedRanges(ClusterMetadata.current()));

                ReconfigureCMS reconfigureCMS = (ReconfigureCMS) ClusterMetadata.current().inProgressSequences.get(ReconfigureCMS.SequenceKey.instance);
                ClusterMetadataService.instance().commit(reconfigureCMS.next);
                ProgressBarrier.propagateLast(MetaStrategy.affectedRanges(ClusterMetadata.current()));
                reconfigureCMS = (ReconfigureCMS) ClusterMetadata.current().inProgressSequences.get(ReconfigureCMS.SequenceKey.instance);
                ClusterMetadataService.instance().commit(reconfigureCMS.next);
                ProgressBarrier.propagateLast(MetaStrategy.affectedRanges(ClusterMetadata.current()));
                reconfigureCMS = (ReconfigureCMS) ClusterMetadata.current().inProgressSequences.get(ReconfigureCMS.SequenceKey.instance);
                Assert.assertNull(reconfigureCMS.next.activeTransition);
            });
            cluster.get(1).nodetoolResult("cms", "reconfigure", "--cancel").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                ProgressBarrier.propagateLast(MetaStrategy.affectedRanges(ClusterMetadata.current()));
                ClusterMetadata metadata = ClusterMetadata.current();
                Assert.assertNull(metadata.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance));
                Assert.assertTrue(metadata.fullCMSMembers().contains(FBUtilities.getBroadcastAddressAndPort()));
                assertEquals(3, metadata.fullCMSMembers().size());
                DataPlacement placements = metadata.placements().get(ReplicationParams.meta(metadata));
                Assert.assertTrue(placements.reads.equivalentTo(placements.writes));
            });
        }
    }

    @Test
    public void testReconfigureTooManyNodesDown() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(conf -> conf.with(Feature.NETWORK, Feature.GOSSIP))
                                      .start()))
        {
            cluster.get(2).shutdown().get();
            cluster.get(3).shutdown().get();
            // Fails as the CMS size would be less than a quorum of what was specified (i.e. 3/2 + 1)
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().failure();
            cluster.get(2).startup();
            cluster.get(1).runOnInstance(() -> assertEquals(1, ClusterMetadata.current().fullCMSMembers().size()));

            // Succeeds, but flags that a further reconfiguration is required
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(1).runOnInstance(() -> assertEquals(2, ClusterMetadata.current().fullCMSMembers().size()));
            cluster.get(1).runOnInstance(() -> assertTrue(PrepareCMSReconfiguration.needsReconfiguration(ClusterMetadata.current())));

            // All good
            cluster.get(3).startup();
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(1).runOnInstance(() -> assertEquals(3, ClusterMetadata.current().fullCMSMembers().size()));
            cluster.get(1).runOnInstance(() -> assertFalse(PrepareCMSReconfiguration.needsReconfiguration(ClusterMetadata.current())));
        }
    }

    @Test
    public void testReplaceSameSize() throws IOException, ExecutionException, InterruptedException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .withTokenSupplier(node -> even.token(node == 4 ? 2 : node))
                                           .start()))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(2).shutdown().get();
            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, cluster.get(2), props -> {
                // since we have a downed host there might be a schema version which is old show up but
                // can't be fetched since the host is down...
                props.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
            });
            // wait till the replacing node is in the ring
            awaitRingJoin(cluster.get(1), replacingNode);
            awaitRingJoin(replacingNode, cluster.get(1));
            replacingNode.runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertTrue(metadata.isCMSMember());
                assertEquals(3, metadata.fullCMSMembers().size());
            });
        }
    }

    @Test
    public void testReconfigurePickAliveNodesIfPossible() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(5)
                                           .withConfig(conf -> conf.with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.get(2).shutdown().get();
            cluster.get(3).shutdown().get();
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(2).startup();
            cluster.get(3).startup();

            Set<String> expectedCMSMembers = expectedCMS(cluster, 1, 4, 5);
            cluster.forEach(inst -> assertEquals(expectedCMSMembers, ClusterUtils.getCMSMembers(inst)));
        }
    }

    @Test
    public void testIgnoredNodesRemainExcludedWhileDecommissioning() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(5)
                                           .withConfig(conf -> conf.with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            // A single reconfiguration up front, excluding the nodes which are about to be decommissioned. Ignoring
            // nodes 2 and 3 must produce the same placement as those nodes being down (see
            // testReconfigurePickAliveNodesIfPossible), even though every node here is up.
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3",
                                          "--ignore", broadcastAddress(cluster, 2),
                                          "--ignore", broadcastAddress(cluster, 3))
                   .asserts().success();

            Set<String> expectedCMSMembers = expectedCMS(cluster, 1, 4, 5);
            cluster.forEach(inst -> assertEquals(expectedCMSMembers, ClusterUtils.getCMSMembers(inst)));

            // The ignore list is not persisted, so while the ignored nodes are still members of the cluster the CMS
            // is legitimately reported as not matching the placement they imply.
            cluster.get(1).runOnInstance(() -> assertTrue(PrepareCMSReconfiguration.needsReconfiguration(ClusterMetadata.current())));

            // Decommissioning a node which is not a CMS member does not trigger a reconfiguration, so membership is
            // stable for the duration of the shrink and no further reconfiguration is required per departing node.
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
            assertEquals(expectedCMSMembers, ClusterUtils.getCMSMembers(cluster.get(1)));

            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();
            assertEquals(expectedCMSMembers, ClusterUtils.getCMSMembers(cluster.get(1)));

            // Once the ignored nodes have left, the CMS matches the placement implied by the remaining nodes again.
            cluster.get(1).runOnInstance(() -> assertFalse(PrepareCMSReconfiguration.needsReconfiguration(ClusterMetadata.current())));
        }
    }

    @Test
    public void testReconfigureIgnoreRejectsUnknownAndExcessiveHosts() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(conf -> conf.with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            // Each case below fails for a different reason, so assert on the message rather than just the exit status.
            // A bare failure() would pass even if the relevant check were removed.

            // A host which is not part of the cluster is rejected rather than silently ignored.
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3", "--ignore", "127.0.0.99")
                   .asserts().failure()
                   .errorContains("don't exist in the cluster")
                   .errorContains("127.0.0.99");

            // A host which cannot be resolved at all is rejected rather than being silently dropped from the list.
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3", "--ignore", "999.999.999.999")
                   .asserts().failure()
                   .errorContains("Unknown host in ignore list: 999.999.999.999");

            // Ignoring so many nodes that fewer than a quorum of the requested members can be placed is rejected.
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3",
                                          "--ignore", broadcastAddress(cluster, 2),
                                          "--ignore", broadcastAddress(cluster, 3))
                   .asserts().failure()
                   .errorContains("Too many nodes are currently DOWN or ignored to safely perform the reconfiguration");

            // Ignoring every joined node leaves placement with no candidates at all. This is rejected up front, as
            // the placement strategy would otherwise fail on an assertion rather than reporting anything useful.
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3",
                                          "--ignore", broadcastAddress(cluster, 1),
                                          "--ignore", broadcastAddress(cluster, 2),
                                          "--ignore", broadcastAddress(cluster, 3))
                   .asserts().failure()
                   .errorContains("Cannot reconfigure CMS as all joined nodes are DOWN or ignored");

            // Ignored hosts are meaningless when resuming or cancelling. Note that cancelling with nothing in flight
            // fails on its own, so only the message distinguishes the guard from that unrelated failure.
            cluster.get(1).nodetoolResult("cms", "reconfigure", "--resume", "--ignore", broadcastAddress(cluster, 2))
                   .asserts().failure()
                   .errorContains("Ignored hosts should not be set if previous operation is resumed");
            cluster.get(1).nodetoolResult("cms", "reconfigure", "--cancel", "--ignore", broadcastAddress(cluster, 2))
                   .asserts().failure()
                   .errorContains("Ignored hosts should not be set when cancelling a reconfiguration");
        }
    }

    @Test
    public void testReconfigurationViolatesRackDiversityIfNecessary() throws Exception
    {
        // rack1: node1, node3
        // rack2: node2
        // rack4: node4
        // ideal placement for CMS is 1, 2, 4 but if 2 is down, violate rack diversity and pick 1, 3, 4
        try (Cluster cluster = init(Cluster.build(4)
                                           .withNodeIdTopology(networkTopology(4, (nodeid) -> nodeid % 2 == 1 ? dcAndRack("dc1", "rack1")
                                                                                                              : dcAndRack("dc1", "rack" + nodeid)))
                                           .withConfig(conf -> conf.with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            Set<String> rackDiverse = expectedCMS(cluster, 1, 2, 4);
            cluster.forEach(inst -> assertEquals(rackDiverse, ClusterUtils.getCMSMembers(inst)));
            cluster.get(2).shutdown().get();
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(2).startup();
            Set<String> notRackDiverse = expectedCMS(cluster, 1, 4, 3);
            cluster.forEach(inst -> assertEquals(notRackDiverse, ClusterUtils.getCMSMembers(inst)));
        }
    }

    @Test
    public void cmsTopologyChangePaxosTest() throws Throwable
    {
        // Use a 4 node cluster so we have room to decommission one node while still maintaining RF
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(c -> c.with(Feature.NETWORK))
                                        .withoutVNodes()
                                        .start())
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);
            IInvokableInstance node4 = cluster.get(4);

            // no paxos repair history initially
            PaxosRepairHistory empty = PaxosRepairHistory.empty(MetaStrategy.partitioner);
            cluster.forEach(i -> assertEquals(empty, paxosRepairHistory(i)));

            node1.nodetoolResult("cms", "reconfigure", "2").asserts().success();
            // Nodes 3 & 4 are not involved in the first cms reconfiguration, so should still have no paxos repair
            // history for the metadata log table
            assertEquals(empty, paxosRepairHistory(node3));
            assertEquals(empty, paxosRepairHistory(node4));
            // Node 1 & 2 should have completed a paxos repair. For this keyspace, that is always over the entire
            // range, so there is only ever a single entry in the repair history which equates to prh.size() == 0
            PaxosRepairHistory node1History = paxosRepairHistory(node1);
            assertEquals(0, node1History.size());
            assertEquals(node1History, paxosRepairHistory(node2));

            // node 1 leaving should cause a cms reconfiguration which runs a paxos repair which involves nodes 2 & 3
            // does participate in while node 4 remains uninvolved.
            node1.nodetoolResult("decommission").asserts().success();
            assertEquals(empty, paxosRepairHistory(node4));

            PaxosRepairHistory node3History = paxosRepairHistory(node3);
            assertEquals(0, node3History.size());
            assertEquals(node3History, paxosRepairHistory(node2));
            // verify that the ballot for this second repair is > the one for the first
            Ballot node3Ballot = node3History.ballotForToken(MetaStrategy.partitioner.getMinimumToken());
            Ballot node1Ballot = node1History.ballotForToken(MetaStrategy.partitioner.getMinimumToken());
            assertTrue(node3Ballot.unixMicros() > node1Ballot.unixMicros());
        }
    }

    @Test
    public void testReconfigurePaxosRepairDisabled() throws IOException
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(c -> c.with(Feature.NETWORK)
                                                          .set("paxos_repair_enabled", "false"))
                                        .withoutVNodes()
                                        .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
        }
    }

    private PaxosRepairHistory paxosRepairHistory(IInvokableInstance instance)
    {
        Object[][] rows = instance.executeInternal("select points from system.paxos_repair_history " +
                                                   "where keyspace_name = ? " +
                                                   "and table_name = ?",
                                                   METADATA_KEYSPACE_NAME,
                                                   DistributedMetadataLogKeyspace.TABLE_NAME);

        if (rows.length == 0)
            return PaxosRepairHistory.empty(METADATA_KEYSPACE_NAME, DistributedMetadataLogKeyspace.TABLE_NAME);
        assertEquals(1, rows.length);
        //noinspection unchecked
        List<ByteBuffer> points = (List<ByteBuffer>)rows[0][0];
        return PaxosRepairHistory.fromTupleBufferList(MetaStrategy.partitioner, points);
    }

    // We can't assume that nodeId matches endpoint (ie node3 = 127.0.0.3 etc)
    private Set<String> expectedCMS(Cluster cluster, int... instanceIds)
    {
        Set<String> expectedCMSMembers = new HashSet<>(instanceIds.length);
        for (int id : instanceIds)
            expectedCMSMembers.add(cluster.get(id).config().broadcastAddress().getAddress().toString());
        return expectedCMSMembers;
    }

    private String broadcastAddress(Cluster cluster, int instanceId)
    {
        return cluster.get(instanceId).config().broadcastAddress().getAddress().getHostAddress();
    }
}
