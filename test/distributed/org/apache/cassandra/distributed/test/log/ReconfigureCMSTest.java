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

import java.util.Random;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
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
                Assert.assertEquals(5, metadata.fullCMSMembers().size());
                Assert.assertEquals(ReplicationParams.simpleMeta(5, metadata.directory.knownDatacenters()),
                                    metadata.placements.keys().stream().filter(ReplicationParams::isMeta).findFirst().get());
            });
            cluster.stream().forEach(i -> {
                Assert.assertTrue(i.executeInternal(String.format("SELECT * FROM %s.%s", SchemaConstants.METADATA_KEYSPACE_NAME, DistributedMetadataLogKeyspace.TABLE_NAME)).length > 0);
            });

            cluster.get(nodeSelector.get()).nodetoolResult("cms", "reconfigure", "1").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                Assert.assertEquals(1, metadata.fullCMSMembers().size());
                Assert.assertEquals(ReplicationParams.simpleMeta(1, metadata.directory.knownDatacenters()),
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
                ClusterMetadataService.instance().commit(new PrepareCMSReconfiguration.Complex(ReplicationParams.simple(3).asMeta()));
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
                Assert.assertEquals(2, metadata.fullCMSMembers().size());
                ReplicationParams params = ReplicationParams.meta(metadata);
                DataPlacement placements = metadata.placements.get(params);
                Assert.assertEquals(placements.reads, placements.writes);
                Assert.assertEquals(metadata.fullCMSMembers().size(), Integer.parseInt(params.asMap().get("dc0")));
            });

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadataService.instance().commit(new PrepareCMSReconfiguration.Complex(ReplicationParams.simple(4).asMeta()));
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
                Assert.assertEquals(3, metadata.fullCMSMembers().size());
                DataPlacement placements = metadata.placements.get(ReplicationParams.meta(metadata));
                Assert.assertEquals(placements.reads, placements.writes);
            });
        }
    }
}
