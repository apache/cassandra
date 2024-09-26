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
import java.util.Collections;
import java.util.HashSet;
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
import org.apache.cassandra.schema.SchemaConstants;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.psjava.util.AssertStatus.assertTrue;

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
                                    metadata.placements.keys().stream().filter(ReplicationParams::isMeta).findFirst().get());
            });
            cluster.stream().forEach(i -> {
                Assert.assertTrue(i.executeInternal(String.format("SELECT * FROM %s.%s", SchemaConstants.METADATA_KEYSPACE_NAME, DistributedMetadataLogKeyspace.TABLE_NAME)).length > 0);
            });

            cluster.get(nodeSelector.get()).nodetoolResult("cms", "reconfigure", "1").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertEquals(1, metadata.fullCMSMembers().size());
                assertEquals(ReplicationParams.simpleMeta(1, metadata.directory.knownDatacenters()),
                                    metadata.placements.keys().stream().filter(ReplicationParams::isMeta).findFirst().get());
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
                DataPlacement placements = metadata.placements.get(params);
                assertEquals(placements.reads, placements.writes);
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
                DataPlacement placements = metadata.placements.get(ReplicationParams.meta(metadata));
                assertEquals(placements.reads, placements.writes);
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
                assertTrue(metadata.isCMSMember(FBUtilities.getBroadcastAddressAndPort()));
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

    // We can't assume that nodeId matches endpoint (ie node3 = 127.0.0.3 etc)
    private Set<String> expectedCMS(Cluster cluster, int... instanceIds)
    {
        Set<String> expectedCMSMembers = new HashSet<>(instanceIds.length);
        for (int id : instanceIds)
            expectedCMSMembers.add(cluster.get(id).config().broadcastAddress().getAddress().toString());
        return expectedCMSMembers;
    }
}
