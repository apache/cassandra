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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class AccordImportSSTableFailureTest extends AccordImportSSTableTestBase
{
    @Test
    public void testConcurrentTopologyChangeFailsImport() throws Throwable
    {
        String file = writeSSTables(new int[] { 1 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(AccordImportSSTableTestBase.ByteBuddyInjections.StallImportTxn.install(1))
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            Thread importer = new Thread(() -> {
                cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    Set<String> paths = Set.of(file);
                    Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true))
                              .isInstanceOf(RuntimeException.class)
                              .hasMessageContaining("Failed adding SSTables on local node; note the import may still have been committed by a recovery coordinator")
                              .cause();
                });
            }, "importer");

            importer.start();

            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.move(Long.toString(Long.parseLong(getOnlyElement(StorageService.instance.getTokens())) + 1));
                State.waitForTopologyChange.countDown();
            });

            importer.join();

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 0);
        }
    }

    @Test
    public void testStreamingFailedPendingSSTablesCleanedUp() throws Throwable
    {
        String file = writeSSTables(new int[] { 1, 2, 3 });

        int FAILED_STREAM = 3;
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(ByteBuddyInjections.FailIncomingStream.install(FAILED_STREAM))
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true));
            });

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            // We exclude the missed instance because the SSTables streamed to the pending directory
            // are not linked to LocalTransfers and hence cleanup does not know which SSTables to clean up
            assertPendingDirs(cluster.stream().filter(instance -> instance != cluster.get(FAILED_STREAM)).collect(Collectors.toList()), (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });
            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));
        }
    }

    @Test
    public void testRecoveryCoordinatorFinishesImport() throws Throwable
    {
        String file = writeSSTables(new int[] { 1, 2, 3 });

        // We disable local delivery so that we can stimulate a network partition by dropping
        // ACCORD_STABLE_THEN_READ_REQ messages
        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1).withConfig((config) ->
                                                                             config
                                                                             .set("accord.recover_txn", "100ms")
                                                                             .set("accord.permit_local_delivery", false)
                                                                             .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            // Simulate a network partition right before the StableThenRead message is sent
            // so that a recovery coordinator can pick up the ImportTxn
            cluster.filters().outbound().messagesMatching((from, to, msg) -> {
                if (from == 1 && msg.verb() == Verb.ACCORD_STABLE_THEN_READ_REQ.id)
                {
                    cluster.filters().outbound().from(1).drop();
                    return true;
                }
                return false;
            }).drop();

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);

                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true))
                          .isInstanceOf(RuntimeException.class)
                          .hasMessageContaining("Failed adding SSTables on local node; note the import may still have been committed by a recovery coordinator")
                          .cause()
                          .isInstanceOf(RuntimeException.class)
                          .hasMessageContaining("SSTable import failed locally; however the operation may still be applied by the recovery coordinator");
            });

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 1);
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });
            cluster.filters().reset();
        }
    }

    @Test
    public void testRecoveryCoordinatorFinishesImport2() throws Throwable
    {
        String file = writeSSTables(new int[] { 1, 2, 3 });

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1).withConfig((config) ->
                                                                             config
                                                                             .set("accord.recover_txn", "100ms")
                                                                             .set("accord.permit_local_delivery", false)
                                                                             .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            // Node 1 sends the StableThenRead message to node 2 and then node 1 fails, so the
            // only existence of the stable message is at node 2
            cluster.filters().outbound().messagesMatching((from, to, msg) -> {
                if (from == 1 && msg.verb() == Verb.ACCORD_STABLE_THEN_READ_REQ.id)
                {
                    // We prevent nodes 1 & 3 from receiving the StableThenRead message,
                    // from node 1 and then prevent node 1 from receiving any more messages
                    cluster.filters().outbound().from(1).to(1, 3).drop();
                    cluster.filters().inbound().to(1).drop();

                    // We still want node 2 to receive the message, so the ImportTxn is
                    // stable, however once that is done we do not want to receive any more messages
                    // from node 1
                    if (to == 2)
                    {
                        cluster.filters().outbound().from(1).drop();
                        return false;
                    }
                    return true;
                }
                return false;
            }).drop();

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true))
                          .isInstanceOf(RuntimeException.class)
                          .isInstanceOf(RuntimeException.class)
                          .hasMessageContaining("Failed adding SSTables on local node; note the import may still have been committed by a recovery coordinator")
                          .cause()
                          .isInstanceOf(RuntimeException.class)
                          .hasMessageContaining("SSTable import failed locally; however the operation may still be applied by the recovery coordinator");
            });

            Iterable<IInvokableInstance> up = cluster.stream()
                                                     .filter(instance -> instance != cluster.get(1))
                                                     .collect(Collectors.toList());

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(up, 1);
            assertLocalSelect(up, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });
            cluster.filters().reset();
        }
    }

    @Test
    public void testImportRecoversOnNodeCrash() throws Throwable
    {
        String file = writeSSTables(new int[]{ 1 }, new int[]{ 8 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(ByteBuddyInjections.AwaitNthDiskMove.install(2))
                                             .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                             .start()))
        {
            createSchema(cluster);

            Thread importer = new Thread(() -> {
                cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    cfs.importNewSSTables(Set.of(file), true, true, true, true, true, true, true);
                });
            }, "importer");

            importer.start();

            // We wait here until we are performing the activation of the second SSTable
            State.waitForCrash.await();

            cluster.get(2).shutdown().get();

            // We prevent moveAndOpenSSTable from actually performing the import until we have restarted
            State.crashed.set(true);

            cluster.get(2).startup();

            importer.join();

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 2);
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(8, 1)); });
        }
    }

    @Test
    public void testImportRecoversNodeOnAbstractReplayerPath() throws Throwable
    {
        String file = writeSSTables(new int[]{ 1 }, new int[]{ 8 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer((cl, tg, num, gen) -> {
                                                 ByteBuddyInjections.AwaitNthDiskMove.install(2).initialise(cl, tg, num, gen);
                                                 ByteBuddyInjections.ReplayJournalWithNullMinSegment.install(2).initialise(cl, tg, num, gen);
                                             })
                                             .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                             .start()))
        {
            createSchema(cluster);

            Thread importer = new Thread(() -> {
                cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    cfs.importNewSSTables(Set.of(file), true, true, true, true, true, true, true);
                });
            }, "importer");

            importer.start();

            // We wait here until we are performing the activation of the second SSTable
            State.waitForCrash.await();

            cluster.get(2).shutdown().get();

            // We prevent moveAndOpenSSTable from actually performing the import until we have restarted
            State.crashed.set(true);

            cluster.get(2).startup();

            importer.join();

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 2);
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(8, 1)); });
        }
    }

    @Test
    public void testImportFailsActivation() throws Throwable
    {
        State.failOnDiskMoveAttempt.set(1);
        String file = writeSSTables(new int[] { 1 });

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(ByteBuddyInjections.FailNthDiskMove.install(2))
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            createSchema(cluster);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 1);
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1)); });
        }
    }

    @Test
    public void testImportFailsActivation2() throws Throwable
    {
        State.failOnDiskMoveAttempt.set(2);
        String file = writeSSTables(new int[]{ 1 }, new int[]{ 8 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(ByteBuddyInjections.FailNthDiskMove.install(2))
                                             .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                             .start()))
        {
            createSchema(cluster);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                cfs.importNewSSTables(Set.of(file), true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 2);
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(8, 1)); });
        }
    }

    @Test
    public void testImportAppliesOnPartitionedReplica() throws Throwable
    {
        String file = writeSSTables(new int[] { 1, 2, 3 });

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                             .start()))
        {
            createSchema(cluster);

            // Partition node3 from the consensus phases only.
            cluster.filters()
                   .verbs(Verb.ACCORD_PRE_ACCEPT_REQ.id,
                          Verb.ACCORD_ACCEPT_REQ.id,
                          Verb.ACCORD_NOT_ACCEPT_REQ.id,
                          Verb.ACCORD_COMMIT_REQ.id,
                          Verb.ACCORD_STABLE_THEN_READ_REQ.id,
                          Verb.ACCORD_APPLY_REQ.id)
                   .to(3)
                   .drop();

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                cfs.importNewSSTables(Set.of(file), true, true, true, true, true, true, true);
            });

            // Wait for Propogate message to be sent to catch up node 3
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            assertLocalSelect(cluster, rows -> assertRows(rows, row(1, 1), row(2, 1), row(3, 1)));
            assertSSTableCount(cluster, 1);
            cluster.filters().reset();
        }
    }
}
