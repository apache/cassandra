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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.topology.TopologyManager;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamState;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;

public class AccordBootstrapTest extends AccordBootstrapTestBase
{
    protected IInvokableInstance failedAndResumeBootstrapAndJoinNode(Cluster cluster)
    {
        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("auto_bootstrap", true);
        config.set("accord.shard_durability_target_splits", "1");
        config.set("accord.shard_durability_cycle", "20s");
        config.set("accord.retry_join_bootstrap", "10s,attempts=1");
        cluster.forEach(instance -> instance.runOnInstance(() -> StreamListener.listener.failStream = true));
        IInvokableInstance newInstance = cluster.bootstrap(config);
        newInstance.startup(cluster);
        spinUntilTrue(() -> cluster.stream().anyMatch(instance -> instance.callOnInstance(() -> StreamListener.listener.hasFailedStream)));
        newInstance.shutdown(false);
        cluster.get(1, 2).forEach(instance -> instance.runOnInstance(() -> StreamListener.listener.failStream = false));
        newInstance.startup(cluster);
        // todo: re-add once we fix write survey/join ring = false mode
//        withProperty(BOOTSTRAP_SCHEMA_DELAY_MS.getKey(), Integer.toString(90 * 1000),
//                     () -> withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster)));
//        newInstance.nodetoolResult("join").asserts().success();
        newInstance.nodetoolResult("cms", "describe").asserts().success(); // just make sure we're joined, remove later
        return newInstance;
    }

    private static class StreamListener implements StreamManager.StreamListener
    {
        private static boolean isRegistered = false;
        private static final StreamListener listener = new StreamListener();

        private final List<StreamResultFuture> registered = new ArrayList<>();
        private boolean failStream, hasFailedStream;

        static synchronized void register()
        {
            if (isRegistered)
                return;
            StreamManager.instance.addListener(listener);
            isRegistered = true;
        }

        public synchronized void onRegister(StreamResultFuture result)
        {
            registered.add(result);
            if (failStream)
            {
                result.addEventListener(new StreamEventHandler()
                      {
                          @Override
                          public void handleStreamEvent(StreamEvent event)
                          {
                              if (event.eventType == StreamEvent.Type.STREAM_PREPARED)
                              {
                                  result.getCoordinator().getAllStreamSessions().forEach(StreamSession::abort);
                                  hasFailedStream = true;
                              }
                          }

                          @Override
                          public void onSuccess(StreamState result)
                          {

                          }

                          @Override
                          public void onFailure(Throwable t)
                          {

                          }
                      }
                  );
            }
        }

        public synchronized void forSession(Consumer<StreamSession> consumer)
        {
            registered.forEach(future -> {
                future.getCoordinator().getAllStreamSessions().forEach(consumer);
            });
        }
    }

    @Test
    public void bootstrapTest() throws Throwable
    {
        bootstrapTest(cluster -> {
            bootstrapAndJoinNode(cluster);
            awaitMaxEpochReadyToRead(cluster);
        });
    }

    @Test
    public void resumeBootstrapTest() throws Throwable
    {
        bootstrapTest(cluster -> {
            failedAndResumeBootstrapAndJoinNode(cluster);
            awaitMaxEpochReadyToRead(cluster);
        });
    }

    public void bootstrapTest(Consumer<Cluster> bootstrapAndJoinNode) throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = Cluster.build().withNodes(originalNodeCount)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                      .withConfig(config -> config.set("accord.command_store_shard_count", 2)
                                                                  .set("accord.queue_shard_count", 2)
                                                                  .set("accord.shard_durability_cycle", "20s")
                                                                  .set("accord.shard_durability_target_splits", "1")
                                                                  .set("accord.retry_syncpoint", "1s*attempts")
                                                                  .set("accord.retry_durability", "1s*attempts")
                                                                  .with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key(k, c)) WITH transactional_mode='full'");

            awaitMaxEpochReadyToRead(cluster);
            for (IInvokableInstance node : cluster)
                node.runOnInstance(StreamListener::register);
            awaitMaxEpochReadyToRead(cluster);

            for (int key = 0; key < 100; key++)
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

            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(() -> {
                    Assert.assertTrue(StreamListener.listener.registered.isEmpty());
                });
            }

            bootstrapAndJoinNode.accept(cluster);

            InetAddress node3Addr = cluster.get(3).broadcastAddress().getAddress();
            for (IInvokableInstance node : cluster.get(1, 2))
            {
                node.runOnInstance(() -> {

                    StreamListener.listener.forSession(session -> {
                        Assert.assertEquals(node3Addr, session.peer.getAddress());
                        Assert.assertEquals(0, session.getNumRequests());
                        Assert.assertTrue(session.getNumKeyspaceTransfers() > 0);
                    });

                    service().node().commandStores().forAllUnsafe(unsafeStore -> {
                        AccordCommandStore ss = (AccordCommandStore) unsafeStore;
                        Assert.assertEquals(Timestamp.NONE, getOnlyElement(ss.unsafeGetBootstrapBeganAt().keySet()));
                        Assert.assertEquals(Timestamp.NONE, getOnlyElement(ss.unsafeGetSafeToRead().keySet()));
                    });
                });
            }

            cluster.get(3).runOnInstance(() -> {
                List<Range<Token>> ranges = StorageService.instance.getLocalRanges("ks");
                TopologyManager topologyManager = service().node().topology();
                for (long epoch = topologyManager.minEpoch() ; epoch <= topologyManager.epoch() ; ++epoch)
                {
                    CountDownLatch latch = new CountDownLatch(1);
                    topologyManager.epochReady(epoch).data.invokeIfSuccess(latch::countDown);
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
                        UntypedResultSet.Row row = getOnlyElement(result);
                        Assert.assertEquals(key, row.getInt("c"));
                        Assert.assertEquals(key, row.getInt("v"));

                        getBlocking(service().node().commandStores().forEach("Test", RoutingKeys.of(partitionKey.toUnseekable()), Long.MIN_VALUE, Long.MAX_VALUE, safeStore -> {
                            if (safeStore.ranges().currentRanges().contains(partitionKey))
                            {
                                AccordSafeCommandStore ss = (AccordSafeCommandStore) safeStore;
                                Assert.assertFalse(ss.bootstrapBeganAt().isEmpty());
                                Assert.assertFalse(ss.safeToReadAt().isEmpty());

                                Assert.assertTrue(ss.bootstrapBeganAt().entrySet().stream()
                                                    .filter(entry -> entry.getValue().contains(partitionKey))
                                                    .anyMatch(entry -> {
                                                        Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                        return true;
                                                    }));
                                Assert.assertTrue(ss.safeToReadAt().entrySet().stream()
                                                    .filter(entry -> entry.getValue().contains(partitionKey))
                                                    .anyMatch(entry -> {
                                                        Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                        return true;
                                                    }));
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
