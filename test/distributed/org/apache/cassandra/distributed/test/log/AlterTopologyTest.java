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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.sequences.SequencesUtils.ClearLockedRanges;
import org.apache.cassandra.tcm.sequences.SequencesUtils.LockRanges;
import org.apache.cassandra.tcm.transformations.AlterTopology;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import static java.time.Duration.ofSeconds;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.junit.Assert.assertEquals;

public class AlterTopologyTest extends FuzzTestBase
{
    @Test
    public void testTopologyChanges() throws Exception
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "change_topology_test", 1000);
        try (Cluster cluster = builder().withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withRack("dc1", "rack1", 1)
                                        .withRack("dc1", "rack2", 1)
                                        .withRack("dc1", "rack3", 1)
                                        .withRack("dc1", "rack4", 1)
                                        .withConfig(config -> config.with(GOSSIP))
                                        .withNodes(4)
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);

            withRandom(rng -> {
                SchemaSpec schema = schemaGen.generate(rng);
                Generators.TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), 1000)));
                Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), 1000));

                HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                     (hb) -> InJvmDTestVisitExecutor.builder()
                                                                                                    .nodeSelector(i -> 1)
                                                                                                    .build(schema, hb, cluster));
                history.custom(() -> {
                    cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE +
                                         " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1' : 3 };");
                    cluster.schemaChange(schema.compile());
                    waitForCMSToQuiesce(cluster, cmsInstance);
                }, "Setup");


                Runnable writeAndValidate = () -> {
                    for (int i = 0; i < 2000; i++)
                        history.insert(pkGen.generate(rng), ckGen.generate(rng));

                    for (int pk : pkGen.generated())
                        history.selectPartition(pk);
                };
                writeAndValidate.run();

                cluster.forEach(i -> i.runOnInstance(() -> {
                    CustomTransformation.registerExtension(LockRanges.NAME, LockRanges.serializer);
                    CustomTransformation.registerExtension(ClearLockedRanges.NAME, ClearLockedRanges.serializer);
                }));

                // a dc change which affects placements is not allowed, so expect a rejection
                history.custom(() -> {
                    cmsInstance.runOnInstance(() -> {
                        PlacementProvider pp = ClusterMetadataService.instance().placementProvider();
                        NodeId id = ClusterMetadata.current().myNodeId();
                        Map<NodeId, Location> updates = new HashMap<>();
                        updates.put(id, new Location("dcX", "rack1"));
                        assertAlterTopologyRejection(pp, updates, "Proposed updates modify data placements");
                    });
                }, "DC change affecting placements");

                // a rack change which affects placements is also not allowed
                history.custom(() -> {
                    cmsInstance.runOnInstance(() -> {
                        PlacementProvider pp = ClusterMetadataService.instance().placementProvider();
                        NodeId id = ClusterMetadata.current().myNodeId();
                        Map<NodeId, Location> updates = new HashMap<>();
                        updates.put(id, new Location("dc1", "rack2"));
                        assertAlterTopologyRejection(pp, updates, "Proposed updates modify data placements");
                    });
                },"Rack change affecting placements ");

                // submit an update which would not modify placements so would normally be accepted
                history.custom(() -> {
                   cmsInstance.runOnInstance(() -> {
                       PlacementProvider pp = ClusterMetadataService.instance().placementProvider();
                       NodeId id = ClusterMetadata.current().myNodeId();
                       Map<NodeId, Location> updates = new HashMap<>();
                       updates.put(id, new Location("dc1", "rack99"));
                       // if there are locked ranges, implying in-progress range movements, any update is rejected
                       ClusterMetadataService.instance().commit(new CustomTransformation(LockRanges.NAME, new LockRanges()));
                       assertAlterTopologyRejection(pp, updates, "The requested topology changes cannot be executed while there are ongoing range movements");

                       // but if no movements are in flight, the update is allowed
                       ClusterMetadataService.instance().commit(new CustomTransformation(ClearLockedRanges.NAME, new ClearLockedRanges()));
                       ClusterMetadataService.instance().commit(new AlterTopology(updates, pp));
                       if (!ClusterMetadata.current().directory.location(id).rack.equals("rack99"))
                           throw new AssertionError("Expected rack to have changed");
                   });
               }, "Rack change not affecting placements");

               // changing multiple/all racks atomically
               history.custom(() -> {
                  cmsInstance.runOnInstance(() -> {
                       PlacementProvider pp = ClusterMetadataService.instance().placementProvider();
                       Map<NodeId, Location> updates = new HashMap<>();
                       Directory dir = ClusterMetadata.current().directory;
                       for (NodeId nodeId : dir.peerIds())
                           updates.put(nodeId, new Location("dc1", "rack" + (nodeId.id() + 100)));

                       ClusterMetadataService.instance().commit(new AlterTopology(updates, pp));
                       dir = ClusterMetadata.current().directory;
                       for (NodeId nodeId : dir.peerIds())
                           if (!ClusterMetadata.current().directory.location(nodeId).rack.equals("rack" + (nodeId.id() + 100)))
                               throw new AssertionError("Expected rack to have changed");
                  });
               }, "Modify all racks not affecting placements");

               // renaming a datacenter is supported, as long as it is not referenced in any replication params as that
               // would impact placements
               history.custom(() -> {
                   cmsInstance.runOnInstance(() -> {
                       PlacementProvider pp = ClusterMetadataService.instance().placementProvider();
                      Map<NodeId, Location> updates = new HashMap<>();
                       Directory dir = ClusterMetadata.current().directory;
                       for (NodeId nodeId : dir.peerIds())
                           updates.put(nodeId, new Location("renamed_dc", dir.location(nodeId).rack));
                       assertAlterTopologyRejection(pp, updates, "Proposed updates modify data placements");
                   });
               }, "Renaming DC referenced in replication params");

               // after modifying replication for the test keyspace, this should be allowed
               history.custom(() -> {
                   cmsInstance.runOnInstance(() -> {
                       PlacementProvider pp = ClusterMetadataService.instance().placementProvider();
                       Map<NodeId, Location> updates = new HashMap<>();
                       QueryProcessor.executeInternal("ALTER KEYSPACE " + KEYSPACE +
                                                      " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3 };");
                       Directory dir = ClusterMetadata.current().directory;
                       for (NodeId nodeId : dir.peerIds())
                           updates.put(nodeId, new Location("renamed_dc", dir.location(nodeId).rack));

                       ClusterMetadataService.instance().commit(new AlterTopology(updates, pp));

                       for (NodeId nodeId : dir.peerIds())
                           if (!ClusterMetadata.current().directory.location(nodeId).datacenter.equals("renamed_dc"))
                               throw new AssertionError("Expected dc to have changed");

                       // modify both datacenter and racks
                       dir = ClusterMetadata.current().directory;
                       for (NodeId nodeId : dir.peerIds())
                           updates.put(nodeId, new Location("renamed_dc_again", "rack" + (nodeId.id() + 200)));

                       ClusterMetadataService.instance().commit(new AlterTopology(updates, pp));
                       dir = ClusterMetadata.current().directory;
                       for (NodeId nodeId : dir.peerIds())
                           if (!ClusterMetadata.current().directory.location(nodeId).equals(new Location("renamed_dc_again", "rack" + (nodeId.id() + 200))))
                               throw new AssertionError("Expected dc to have changed");
                   });
                   waitForCMSToQuiesce(cluster, cmsInstance);
               },"Renaming DC not referenced in replication params");

               // updates to system tables run asynchronously so spin until they're done
               history.custom(() -> {
                  cluster.forEach(i -> await(60).until(() -> i.callOnInstance(() -> {
                      ClusterMetadata metadata = ClusterMetadata.current();
                      NodeId myId = metadata.myNodeId();
                      Directory dir = metadata.directory;
                      for (NodeId nodeId : dir.peerIds())
                      {
                          String query = nodeId.equals(myId)
                                         ? "select data_center, rack from system.local"
                                         : String.format("select data_center, rack from system.peers_v2 where peer = '%s'",
                                                         dir.endpoint(nodeId).getHostAddress(false));
                          UntypedResultSet res = QueryProcessor.executeInternal(query);
                          if (!res.one().getString("data_center").equals("renamed_dc_again"))
                              return false;
                          if (!res.one().getString("rack").equals("rack" + (nodeId.id() + 200)))
                              return false;
                      }
                      return true;
                  })));
               }, "Verify local system table updates");

               // check gossip is also updated
               history.custom(() -> {
                  Map<String, Map<String, String>> gossipInfo = ClusterUtils.gossipInfo(cmsInstance);
                  gossipInfo.forEach((ep, states) -> {
                      String nodeId = states.get("HOST_ID").split(":")[1];
                      String dc = states.get("DC").split(":")[1];
                      assertEquals("renamed_dc_again", dc);
                      String rack = states.get("RACK").split(":")[1];
                      String expected = "rack" + (NodeId.fromString(nodeId).id() + 200);
                      assertEquals(expected, rack);
                  });
                }, "Verify gossip state");

               writeAndValidate.run();
            });
        }
    }

    private static void assertAlterTopologyRejection(PlacementProvider pp, Map<NodeId, Location> updates, String error)
    {
        ClusterMetadataService.instance()
                              .commit(new AlterTopology(updates, pp),
                                      m -> { throw new AssertionError("Expected rejection");},
                                      (c, r) -> {
                                          if (!(c == ExceptionCode.INVALID && r.startsWith(error)))
                                              throw new AssertionError("Unexpected failure response: " + r);
                                          return ClusterMetadata.current();
                                      });

    }

    private static ConditionFactory await(int seconds)
    {
        return Awaitility.await().atMost(ofSeconds(seconds)).pollDelay(ofSeconds(1));
    }

}
