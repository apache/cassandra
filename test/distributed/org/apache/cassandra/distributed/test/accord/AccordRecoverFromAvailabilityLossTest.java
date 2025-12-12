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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.topology.EpochReady;
import accord.topology.TopologyManager;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordOperations;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;

public class AccordRecoverFromAvailabilityLossTest extends AccordBootstrapTestBase
{
    private static void setConfig(IInstanceConfig config)
    {
        config.set("accord.command_store_shard_count", 2)
              .set("accord.queue_shard_count", 2)
              .set("accord.shard_durability_cycle", "20s")
              .set("accord.shard_durability_target_splits", "1")
              .set("accord.retry_syncpoint", "1s*attempts")
              .set("accord.retry_durability", "1s*attempts")
              .with(NETWORK, GOSSIP);
    }

    @Test
    public void replaceWithAvailabilityLossTest() throws Throwable
    {
        int originalNodeCount = 3;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = Cluster.build().withNodes(originalNodeCount)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                      .withConfig(AccordRecoverFromAvailabilityLossTest::setConfig)
                                      .start())
        {
            cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().reconfigureCMS(ReplicationParams.simpleMeta(3, ClusterMetadata.current().directory.knownDatacenters())));
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key(k, c)) WITH transactional_mode='full'");

            awaitMaxEpochReadyToRead(cluster);
            int removeIdx = 3;
            int removeId = cluster.get(removeIdx).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());

            for (int key = 0; key < 50; key++)
            {
                String query = "BEGIN TRANSACTION\n" +
                               "  LET row1 = (SELECT * FROM ks.tbl WHERE k = " + key + " AND c = 0);\n" +
                               "  SELECT row1.v;\n" +
                               "  IF row1 IS NULL THEN\n" +
                               "    INSERT INTO ks.tbl (k, c, v) VALUES (" + key + ", " + key + ", " + key + ");\n" +
                               "  END IF\n" +
                               "COMMIT TRANSACTION";
                AccordTestBase.executeWithRetry(cluster, query);
            }

            FBUtilities.waitOnFuture(cluster.get(removeIdx).shutdown(false));

            {
                List<Future<?>> results = new ArrayList<>();
                for (int key = 50; key < 100; key++)
                {
                    String query = "BEGIN TRANSACTION\n" +
                                   "  LET row1 = (SELECT * FROM ks.tbl WHERE k = " + key + " AND c = 0);\n" +
                                   "  SELECT row1.v;\n" +
                                   "  IF row1 IS NULL THEN\n" +
                                   "    INSERT INTO ks.tbl (k, c, v) VALUES (" + key + ", " + key + ", " + key + ");\n" +
                                   "  END IF\n" +
                                   "COMMIT TRANSACTION";

                    results.add(cluster.coordinator(2).asyncExecuteWithResult(query, ConsistencyLevel.SERIAL));
                }
                try { FBUtilities.waitOnFutures(results); }
                catch (Throwable t) {}
            }

            Future<?> future = cluster.get(2).asyncAcceptsOnInstance((Integer id) -> {
                while (true)
                {
                    try
                    {
                        AccordOperations.instance.accordMarkHardRemoved(Set.of(new NodeId(id)), false);
                        break;
                    }
                    catch (Throwable t)
                    {
                        try
                        {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }).apply(removeId);
            ClusterUtils.replaceHostAndStart(cluster, cluster.get(removeIdx), (i1, i2) -> {}, AccordRecoverFromAvailabilityLossTest::setConfig);
            future.get();

            awaitMaxEpochReadyToRead(cluster);

            cluster.get(4).runOnInstance(() -> {
                List<Range<Token>> ranges = StorageService.instance.getLocalRanges("ks");
                TopologyManager topologyManager = service().node().topology();
                for (long epoch = topologyManager.minEpoch() ; epoch <= topologyManager.epoch() ; ++epoch)
                {
                    CountDownLatch latch = new CountDownLatch(1);
                    topologyManager.epochReady(epoch, EpochReady::data).invokeIfSuccess(latch::countDown);
                    while (true)
                    {
                        try
                        {
                            if (latch.await(1L, TimeUnit.SECONDS))
                                break;
                        }
                        catch (InterruptedException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                }

                for (int key = 0; key < 100; key++)
                {
                    UntypedResultSet result = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k=?", key);
                    PartitionKey partitionKey = pk(key, "ks", "tbl");
                    if (ranges.stream().anyMatch(range -> range.contains(partitionKey.token())))
                    {
                        UntypedResultSet.Row row;
                        if (key < 50) row = getOnlyElement(result);
                        else try { row = getOnlyElement(result); } catch (NoSuchElementException e) { continue; }
                        Assert.assertEquals(key, row.getInt("c"));
                        Assert.assertEquals(key, row.getInt("v"));

                        getBlocking(service().node().commandStores().forEach("Test", RoutingKeys.of(partitionKey.toUnseekable()), Long.MIN_VALUE, Long.MAX_VALUE, safeStore -> {
                            if (safeStore.ranges().currentRanges().contains(partitionKey))
                            {
                                AccordSafeCommandStore ss = (AccordSafeCommandStore) safeStore;
                                Assert.assertFalse(ss.bootstrapBeganAt().isEmpty());
                                Assert.assertFalse(ss.safeToReadAt().isEmpty());

                                Assert.assertEquals(1, ss.bootstrapBeganAt().entrySet().stream()
                                                                   .filter(entry -> entry.getValue().contains(partitionKey))
                                                                   .map(entry -> {
                                                                       Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                                       return entry;
                                                                   }).count());
                                Assert.assertEquals(1, ss.safeToReadAt().entrySet().stream()
                                                                   .filter(entry -> entry.getValue().contains(partitionKey))
                                                                   .map(entry -> {
                                                                       Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                                       return entry;
                                                                   }).count());
                            }
                        }));
                    }
                    else
                    {
                        Assert.assertTrue(result.isEmpty());
                    }
                }
            });
        }
    }
}
