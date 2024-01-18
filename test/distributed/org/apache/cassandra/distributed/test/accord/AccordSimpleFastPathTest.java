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

package org.apache.cassandra.distributed.test.accord;

import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.service.accord.AccordFastPath;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Assert;
import org.junit.Test;

import accord.local.Node;
import accord.topology.Topology;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.accord.AccordConfigurationService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccordSimpleFastPathTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordSimpleFastPathTest.class);

    private static Node.Id id(int i)
    {
        return new Node.Id(i);
    }

    private static Set<Node.Id> idSet(int... ids)
    {
        Set<Node.Id> result = new HashSet<>();
        for (int id: ids)
            result.add(id(id));
        return result;
    }

    private static InetAddressAndPort ep(int i)
    {
        try
        {
            return InetAddressAndPort.getByName(String.format("127.0.0.%s:7012", i));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Set<InetAddressAndPort> epSet(int... eps)
    {
        Set<InetAddressAndPort> result = new HashSet<>();
        for (int ep: eps)
            result.add(ep(ep));
        return result;
    }

    @Test
    public void downNodesRemovedFromFastPath() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.NETWORK).set("accord.enabled", "true"))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c))");
            String query = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM ks.tbl WHERE k=0 AND c=0;\n" +
                           "COMMIT TRANSACTION";
            cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY);

            InetAddressAndPort node1Addr = InetAddressAndPort.getByAddress(cluster.get(1).broadcastAddress());
            InetAddressAndPort node2Addr = InetAddressAndPort.getByAddress(cluster.get(2).broadcastAddress());
            InetAddressAndPort node3Addr = InetAddressAndPort.getByAddress(cluster.get(3).broadcastAddress());
            int node3Id = cluster.get(3).callOnInstance(() -> ClusterMetadata.current().directory.peerId(FBUtilities.getBroadcastAddressAndPort()).id());
            long preShutDownEpoch = cluster.stream().map(ii -> ii.callOnInstance(() -> {
                ClusterMetadata cm = ClusterMetadata.current();
                AccordFastPath accordFastPath = cm.accordFastPath;
                Assert.assertEquals(idSet(), accordFastPath.unavailableIds());

                long epoch = cm.epoch.getEpoch();
                AccordConfigurationService configService = ((AccordService) AccordService.instance()).configurationService();
                Topology topology = configService.getTopologyForEpoch(epoch);
                Assert.assertFalse(topology.shards().isEmpty());
                topology.shards().forEach(shard -> Assert.assertEquals(idSet(1, 2, 3), shard.fastPathElectorate));
                return cm.epoch.getEpoch();
            })).max(Comparator.naturalOrder()).get();

            cluster.get(1).runOnInstance(() -> {
                FailureDetector.instance.forceConviction(InetAddressAndPort.getByAddress(node3Addr));
                // update is performed in another thread, wait for it to be applied locally before returning
                for (int i=0; i<10; i++)
                {
                    if (ClusterMetadata.current().epoch.getEpoch() == preShutDownEpoch)
                        FBUtilities.sleepQuietly(100);
                    else
                        break;
                }
                assert ClusterMetadata.current().epoch.getEpoch() > preShutDownEpoch;
            });

            cluster.get(1, 2).forEach(ii -> {
                logger.info("Checking instance {} -> {}", ii, ii.broadcastAddress());
                ii.runOnInstance(() -> {
                    ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(preShutDownEpoch + 1));
                    ClusterMetadata cm = ClusterMetadata.current();
                    AccordFastPath accordFastPath = cm.accordFastPath;
                    Assert.assertEquals(preShutDownEpoch + 1, cm.epoch.getEpoch());
                    Assert.assertEquals(idSet(node3Id), accordFastPath.unavailableIds());
                });

            }
            );

            // confirm a duplicate conviction doesn't create a new epoch
            cluster.get(2).runOnInstance(() -> {
                FailureDetector.instance.forceConviction(InetAddressAndPort.getByAddress(node3Addr));
            });

            cluster.get(1, 2).forEach(ii -> ii.runOnInstance(() -> {
                ClusterMetadata cm = ClusterMetadata.current();
                Assert.assertEquals(preShutDownEpoch + 1, cm.epoch.getEpoch());
            }));
        }
    }
}
