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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.primitives.Timestamp;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordConfigurationService;
import org.apache.cassandra.service.accord.AccordConfigurationService.EpochSnapshot;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static accord.utils.async.AsyncChains.awaitUninterruptiblyAndRethrow;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class AccordBootstrapTest extends TestBaseImpl
{
    private static DecoratedKey dk(int key)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        return partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }

    private static PartitionKey pk(int key, String keyspace, String table)
    {
        TableId tid = Schema.instance.getTableMetadata(keyspace, table).id;
        return new PartitionKey(keyspace, tid, dk(key));
    }

    protected void bootstrapAndJoinNode(Cluster cluster)
    {
        IInstanceConfig config = cluster.newInstanceConfig();
        config.set("auto_bootstrap", true);
        IInvokableInstance newInstance = cluster.bootstrap(config);
        newInstance.startup(cluster);
        // todo: re-add once we fix write survey/join ring = false mode
//        withProperty(BOOTSTRAP_SCHEMA_DELAY_MS.getKey(), Integer.toString(90 * 1000),
//                     () -> withProperty("cassandra.join_ring", false, () -> newInstance.startup(cluster)));
//        newInstance.nodetoolResult("join").asserts().success();
        newInstance.nodetoolResult("describecms").asserts().success(); // just make sure we're joined, remove later
    }

    private static AccordService service()
    {
        return (AccordService) AccordService.instance();
    }

    private static void awaitEpoch(long epoch)
    {
        try
        {
            boolean completed = service().epochReady(Epoch.create(epoch)).await(60, TimeUnit.SECONDS);
            Assertions.assertThat(completed)
                      .describedAs("Epoch %s did not become ready within timeout on %s -> %s",
                                   epoch, FBUtilities.getBroadcastAddressAndPort(),
                                   service().configurationService().getEpochSnapshot(epoch))
                      .isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void awaitLocalSyncNotification(long epoch)
    {
        try
        {
            AccordConfigurationService configService = service().configurationService();
            boolean completed = configService.localSyncNotified(epoch).await(5, TimeUnit.SECONDS);
            Assert.assertTrue(String.format("Local sync notification for epoch %s did not become ready within timeout on %s",
                                            epoch, FBUtilities.getBroadcastAddressAndPort()), completed);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static long maxEpoch(Cluster cluster)
    {
        return cluster.stream().mapToLong(node -> node.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch())).max().getAsLong();
    }

    private static class StreamListener implements StreamManager.StreamListener
    {
        private static boolean isRegistered = false;
        private static final StreamListener listener = new StreamListener();

        private final List<StreamResultFuture> registered = new ArrayList<>();

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

        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = Cluster.build().withNodes(originalNodeCount)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            long initialMax = maxEpoch(cluster);
            for (IInvokableInstance node : cluster)
            {

                node.runOnInstance(() -> {
                    Assert.assertEquals(initialMax, ClusterMetadata.current().epoch.getEpoch());
                    awaitEpoch(initialMax);
                    AccordConfigurationService configService = service().configurationService();
                    long minEpoch = configService.minEpoch();

                    Assert.assertEquals(initialMax, configService.maxEpoch());

                    for (long epoch = minEpoch; epoch < initialMax; epoch++)
                    {
                        awaitEpoch(epoch);
                        Assert.assertEquals(EpochSnapshot.completed(epoch), configService.getEpochSnapshot(epoch));
                    }

                    awaitLocalSyncNotification(initialMax);
                    Assert.assertEquals(EpochSnapshot.completed(initialMax), configService.getEpochSnapshot(initialMax));
                });
            }

            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(StreamListener::register);
            }

            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key(k, c))");

            long schemaChangeMax = maxEpoch(cluster);
            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(() -> {
                    ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(schemaChangeMax));
                    awaitEpoch(schemaChangeMax);
                    AccordConfigurationService configService = service().configurationService();

                    for (long epoch = initialMax + 1; epoch <= schemaChangeMax; epoch++)
                    {
                        awaitLocalSyncNotification(epoch);
                        Assert.assertEquals(EpochSnapshot.completed(epoch), configService.getEpochSnapshot(epoch));
                    }
                });
            }

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

            bootstrapAndJoinNode(cluster);
            long bootstrapMax = maxEpoch(cluster);
            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(() -> {
                    ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(bootstrapMax));
                    Assert.assertEquals(bootstrapMax, ClusterMetadata.current().epoch.getEpoch());
                    AccordService service = (AccordService) AccordService.instance();
                    awaitEpoch(bootstrapMax);
                    AccordConfigurationService configService = service.configurationService();

                    awaitLocalSyncNotification(bootstrapMax);
                    Assert.assertEquals(EpochSnapshot.completed(bootstrapMax), configService.getEpochSnapshot(bootstrapMax));
                });
            }

            InetAddress node3Addr = cluster.get(3).broadcastAddress().getAddress();
            for (IInvokableInstance node : cluster.get(1, 2))
            {
                node.runOnInstance(() -> {

                    StreamListener.listener.forSession(session -> {
                        Assert.assertEquals(node3Addr, session.peer.getAddress());
                        Assert.assertEquals(0, session.getNumRequests());
                        Assert.assertTrue(session.getNumTransfers() > 0);
                    });

                    awaitUninterruptiblyAndRethrow(service().node().commandStores().forEach(safeStore -> {
                        CommandStore commandStore = safeStore.commandStore();
                        Assert.assertEquals(0, commandStore.maxBootstrapEpoch());
                        Assert.assertEquals(Timestamp.NONE, getOnlyElement(commandStore.bootstrapBeganAt().keySet()));
                        Assert.assertEquals(Timestamp.NONE, getOnlyElement(commandStore.safeToRead().keySet()));
                    }));
                });
            }

            cluster.get(3).runOnInstance(() -> {
                List<Range<Token>> ranges = StorageService.instance.getLocalRanges("ks");
                for (int key = 0; key < 100; key++)
                {
                    UntypedResultSet result = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k=?", key);
                    PartitionKey partitionKey = pk(key, "ks", "tbl");
                    if (ranges.stream().anyMatch(range -> range.contains(partitionKey.token())))
                    {
                        UntypedResultSet.Row row = getOnlyElement(result);
                        Assert.assertEquals(key, row.getInt("c"));
                        Assert.assertEquals(key, row.getInt("v"));

                        awaitUninterruptiblyAndRethrow(service().node().commandStores().forEach(safeStore -> {
                            if (safeStore.ranges().currentRanges().contains(partitionKey))
                            {
                                CommandStore commandStore = safeStore.commandStore();
                                Assert.assertTrue(commandStore.maxBootstrapEpoch() > 0);
                                Assert.assertFalse(commandStore.bootstrapBeganAt().isEmpty());
                                Assert.assertFalse(commandStore.safeToRead().isEmpty());

                                Assert.assertEquals(1, commandStore.bootstrapBeganAt().entrySet().stream()
                                                                   .filter(entry -> entry.getValue().contains(partitionKey))
                                                                   .map(entry -> {
                                                                       Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                                       return entry;
                                                                   }).count());
                                Assert.assertEquals(1, commandStore.safeToRead().entrySet().stream()
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

    @Test
    public void moveTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(3)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            long initialMax = maxEpoch(cluster);
            long[] tokens = new long[3];
            for (int i=0; i<3; i++)
            {
                tokens[i] = cluster.get(i+1).callOnInstance(() -> Long.valueOf(getOnlyElement(StorageService.instance.getTokens())));
            }

            for (IInvokableInstance node : cluster)
            {

                node.runOnInstance(() -> {
                    Assert.assertEquals(initialMax, ClusterMetadata.current().epoch.getEpoch());
                    awaitEpoch(initialMax);
                    AccordConfigurationService configService = service().configurationService();
                    long minEpoch = configService.minEpoch();

                    Assert.assertEquals(initialMax, configService.maxEpoch());

                    for (long epoch = minEpoch; epoch < initialMax; epoch++)
                    {
                        awaitEpoch(epoch);
                        Assert.assertEquals(EpochSnapshot.completed(epoch), configService.getEpochSnapshot(epoch));
                    }

                    awaitLocalSyncNotification(initialMax);
                    Assert.assertEquals(EpochSnapshot.completed(initialMax), configService.getEpochSnapshot(initialMax));
                });
            }

            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key(k, c))");

            long schemaChangeMax = maxEpoch(cluster);
            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(() -> {
                    Assert.assertEquals(schemaChangeMax, ClusterMetadata.current().epoch.getEpoch());
                    AccordService service = (AccordService) AccordService.instance();
                    awaitEpoch(schemaChangeMax);
                    AccordConfigurationService configService = service.configurationService();

                    for (long epoch = initialMax + 1; epoch <= schemaChangeMax; epoch++)
                    {
                        awaitLocalSyncNotification(epoch);
                        Assert.assertEquals(EpochSnapshot.completed(epoch), configService.getEpochSnapshot(epoch));
                    }
                });
            }

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

            long token = ((tokens[1] - tokens[0]) / 2) + tokens[0];
            long preMove = maxEpoch(cluster);

            cluster.get(1).runOnInstance(() -> StorageService.instance.move(Long.toString(token)));

            long moveMax = maxEpoch(cluster);
            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(() -> {
                    ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(moveMax));
                    Assert.assertEquals(moveMax, ClusterMetadata.current().epoch.getEpoch());
                    AccordService service = (AccordService) AccordService.instance();
                    awaitEpoch(moveMax);
                    AccordConfigurationService configService = service.configurationService();

                    awaitLocalSyncNotification(moveMax);
                    Assert.assertEquals(EpochSnapshot.completed(moveMax), configService.getEpochSnapshot(moveMax));
                });
            }

            for (IInvokableInstance node : cluster)
            {
                node.runOnInstance(() -> {
                    // validate streaming
                    List<Range<Token>> ranges = StorageService.instance.getLocalRanges("ks");
                    TableId tableId = Schema.instance.getTableMetadata("ks", "tbl").id;
                    for (int key = 0; key < 100; key++)
                    {
                        DecoratedKey dk = dk(key);
                        UntypedResultSet result = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k=?", key);
                        if (ranges.stream().anyMatch(range -> range.contains(dk.getToken())))
                        {
                            UntypedResultSet.Row row = getOnlyElement(result);
                            Assert.assertEquals(key, row.getInt("c"));
                            Assert.assertEquals(key, row.getInt("v"));

                            PartitionKey partitionKey = new PartitionKey("ks", tableId, dk);

                            awaitUninterruptiblyAndRethrow(service().node().commandStores().forEach(PreLoadContext.contextFor(partitionKey),
                                                                                          partitionKey.toUnseekable(), moveMax, moveMax,
                                                                                          safeStore -> {
                                if (!safeStore.ranges().allAt(preMove).contains(partitionKey))
                                {
                                    CommandStore commandStore = safeStore.commandStore();
                                    Assert.assertTrue(commandStore.maxBootstrapEpoch() > 0);
                                    Assert.assertFalse(commandStore.bootstrapBeganAt().isEmpty());
                                    Assert.assertFalse(commandStore.safeToRead().isEmpty());

                                    Assert.assertEquals(1, commandStore.bootstrapBeganAt().entrySet().stream()
                                                                       .filter(entry -> entry.getValue().contains(partitionKey))
                                                                       .map(entry -> {
                                                                           Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                                           return entry;
                                                                       }).count());
                                    Assert.assertEquals(1, commandStore.safeToRead().entrySet().stream()
                                                                       .filter(entry -> entry.getValue().contains(partitionKey))
                                                                       .map(entry -> {
                                                                           Assert.assertTrue(entry.getKey().compareTo(Timestamp.NONE) > 0);
                                                                           return entry;
                                                                       }).count());
                                }
                            }));
                        }
                    }
                });
            }
        }
    }
}
