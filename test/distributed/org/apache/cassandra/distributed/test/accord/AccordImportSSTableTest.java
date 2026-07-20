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

import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.CoordinatedTransfer;
import org.apache.cassandra.service.accord.LocalTransfers;
import org.apache.cassandra.utils.Shared;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccordImportSSTableTest extends TestBaseImpl
{

    private static final String TABLE = "tbl";
    private static final String KEYSPACE_TABLE = String.format("%s.%s", KEYSPACE, TABLE);
    private static final String TABLE_SCHEMA_CQL = String.format(withKeyspace("CREATE TABLE %s." + TABLE + " (k int primary key, v int);"));

    @Test
    public void testImportSSTables() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder1 = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + "(k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder1.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
        }

        CQLSSTableWriter.Builder builder2 = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + "(k, v) " + "VALUES (?, ?)");


        try (CQLSSTableWriter writer = builder2.build())
        {
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            // Disable autocompaction so when we go to check the number of SSTables they correspond to the SSTables that we have imported
            cluster.forEach(instance -> {
                instance.runOnInstance(() -> {
                    ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).disableAutoCompaction();
                });
            });

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

            // Assert that each node has 2 SSTables
            assertSSTableCount(cluster, 2);

            // Assert that each node has the correct values
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });

            // Assert that SSTables are moved from the pending directories
            assertPendingDirs(cluster, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });
        }
    }

    /**
     * This might have a potential issue with us throwing an exception instead of having a custom class to notify
     * that the read was a failure.
     */
    @Test
    public void testSSTableImportWithConcurrentTopologyChangeFails() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + "(k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(ByteBuddyInjections.StallImportTxn.install(1))
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            Thread importer = new Thread(() -> {
                cluster.get(1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    Set<String> paths = Set.of(file);
                    Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true))
                              .isInstanceOf(RuntimeException.class)
                              .hasMessageContaining("Failed adding SSTables");
                });
            }, "importer");
            importer.start();

            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.move(Long.toString(Long.parseLong(getOnlyElement(StorageService.instance.getTokens())) + 1));
                State.waitForTopologyChange.countDown();
            });

            importer.join();

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

            assertSSTableCount(cluster, 0);
        }
    }

    @Test
    @Ignore
    public void testSSTableImportReplicaDown() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + "(k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        int FAILED_REPLICA = 2;
        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1).withConfig((config) ->
                                                                             config.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            cluster.get(FAILED_REPLICA).shutdown().get();

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true))
                          .isInstanceOf(RuntimeException.class)
                          .cause();
            });

            Iterable<IInvokableInstance> up = cluster.stream()
                                                     .filter(instance -> instance != cluster.get(FAILED_REPLICA))
                                                     .collect(Collectors.toList());

            assertPendingDirs(up, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });

            assertLocalSelect(up, rows -> assertRows(rows, EMPTY_ROWS));
        }
    }

    @Test
    public void testSSTableImportStreamingFailedCleanup() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        int FAILED_STREAM = 3;
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(ByteBuddyInjections.FailIncomingStream.install(FAILED_STREAM))
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true));
            });

            // We exclude the missed instance because the SSTables streamed to the pending directory
            // are not linked to LocalTransfers and hence cleanup does not know which SSTables to clean up
            assertPendingDirs(cluster.stream().filter(instance -> instance != cluster.get(FAILED_STREAM)).collect(Collectors.toList()), (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });

            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));
        }
    }

    @Test
    public void testSSTableImportBounceAfterPending() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        // We disable local delivery so that we can stimulate a network partition by dropping
        // ACCORD_STABLE_THEN_READ_REQ messages
        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1).withConfig((config) ->
                                                                             config
                                                                             .set("accord.permit_local_delivery", false)
                                                                             .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            // Prevent SSTables from being moved to the live set
            cluster.filters().outbound().verbs(Verb.ACCORD_STABLE_THEN_READ_REQ.id).drop();

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true));
            });

            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));

            bounce(cluster);

            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));
        }
    }

    @Test
    public void testRecoveryCoordinatorPerformsImport() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        // We disable local delivery so that we can stimulate a network partition by dropping
        // ACCORD_STABLE_THEN_READ_REQ messages
        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1).withConfig((config) ->
                                                                             config
                                                                             .set("accord.recover_txn", "100ms")
                                                                             .set("accord.permit_local_delivery", false)
                                                                             .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

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
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true));
            });

            // Wait until the recovery coordinator picks up the Import Txn
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    assertEquals(1, cfs.getLiveSSTables().size());
                });
            }

            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });
        }
    }

    @Test
    public void testRecoveryCoordinatorPerformsImport2() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1).withConfig((config) ->
                                                                             config
                                                                             .set("accord.recover_txn", "100ms")
                                                                             .set("accord.permit_local_delivery", false)
                                                                             .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

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
                Assertions.assertThatThrownBy(() -> cfs.importNewSSTables(paths, true, true, true, true, true, true, true));
            });

            // Wait until the recovery coordinator picks up the Import Txn
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

            Iterable<IInvokableInstance> up = cluster.stream()
                                                     .filter(instance -> instance != cluster.get(1))
                                                     .collect(Collectors.toList());

            assertSSTableCount(up, 1);

            assertLocalSelect(up, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });
        }
    }

    @Test
    public void testImportSSTablesWithZeroCopyStreaming() throws Throwable
    {

    }

    @Test
    public void testImportSSTablesCleanupWithMultipleDataDirectories() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder1 = CQLSSTableWriter.builder()
                                                            .forTable(TABLE_SCHEMA_CQL)
                                                            .inDirectory(file)
                                                            .using("INSERT INTO " + KEYSPACE_TABLE + "(k, v) " + "VALUES (?, ?)");

        try (CQLSSTableWriter writer = builder1.build())
        {
            writer.addRow(1, 1);
            writer.addRow(2, 1);
        }

        CQLSSTableWriter.Builder builder2 = CQLSSTableWriter.builder()
                                                            .forTable(TABLE_SCHEMA_CQL)
                                                            .inDirectory(file)
                                                            .using("INSERT INTO " + KEYSPACE_TABLE + "(k, v) " + "VALUES (?, ?)");


        try (CQLSSTableWriter writer = builder2.build())
        {
            writer.addRow(3, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3)
                                             .withoutVNodes()
                                             .withDataDirCount(3)
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            // Disable autocompaction so when we go to check the number of SSTables they correspond to the SSTables that we have imported
            cluster.forEach(instance -> {
                instance.runOnInstance(() -> {
                    ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).disableAutoCompaction();
                });
            });

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

            // Assert that each node has 2 SSTables
            assertSSTableCount(cluster, 2);

            // Assert that each node has the correct values
            assertLocalSelect(cluster, rows -> { assertRows(rows, row(1, 1), row(2, 1), row(3, 1)); });


            // Assert that SSTables are moved from the pending directories
            assertPendingDirs(cluster, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });
        }
    }

    /**
     * There is a bug with this case, because we are going to be marked as stable and then perform the read which has the
     * TxnImport logic contained within it. For ImportTxn's we prevent this replica from making any progress in the case,
     * where we can not properly execute a read import txn. We want to make sure it is marked as stale.
     */
    @Test
    public void testImportSSTableFailsActivation() throws Throwable
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();

        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)");

        // We import a 1 token SSTable so the txn read logic is not run multiple of times by different CommandStores
        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(1, 1);
        }

        try (Cluster cluster = init(builder().withNodes(3).withoutVNodes()
                                             .withDataDirCount(1)
                                             .withInstanceInitializer(ByteBuddyInjections.FailDiskMove.install(2))
                                             .withConfig((config) ->
                                                         config
                                                         .with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);

            Iterable<IInvokableInstance> up = cluster.stream()
                                                     .filter(instance -> instance != cluster.get(2))
                                                     .collect(Collectors.toList());

            assertSSTableCount(up, 1);

            assertLocalSelect(up, rows -> { assertRows(rows, row(1, 1)); });
        }
    }

    private static void bounce(Cluster cluster)
    {
        cluster.forEach(instance -> {
            try
            {
                instance.shutdown().get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
            instance.startup();
        });
    }

    private static void assertLocalTransferIsCleanup(Iterable<IInvokableInstance> validate)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                assertTrue(LocalTransfers.instance.local.isEmpty());
                assertTrue(LocalTransfers.instance.coordinating.isEmpty());
            });
        }
    }

    private static void assertPendingDirs(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<File> forPendingUuidDir)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                Set<File> allPendingDirs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).getDirectories().getPendingLocations();
                for (File pendingDir : allPendingDirs)
                {
                    File[] pendingUuidDirs = pendingDir.listUnchecked(File::isDirectory);
                    for (File pendingUuidDir : pendingUuidDirs)
                    {
                        forPendingUuidDir.accept(pendingUuidDir);
                    }
                }
            });
        }
    }

    private static void assertSSTableCount(Iterable<IInvokableInstance> validate, int count)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Assertions.assertThat(cfs.getLiveSSTables().size()).isEqualTo(count);
            });
        }
    }

    private static void assertLocalSelect(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<Object[][]> onRows)
    {
        for (IInvokableInstance instance : validate)
        {
            {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE));
                onRows.accept(rows);
            }
        }
    }

    @Shared
    public static class State
    {
        public static CountDownLatch waitForTopologyChange = new CountDownLatch(1);
        public static AtomicBoolean shouldFailDisk = new AtomicBoolean(true);
    }

    public static class ByteBuddyInjections
    {
        public static class StallImportTxn
        {
            public static IInstanceInitializer install(int... nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(CoordinatedTransfer.class)
                                           .method(named("performImportTxn"))
                                           .intercept(MethodDelegation.to(ByteBuddyInjections.StallImportTxn.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static void performImportTxn(@SuperCall Callable<Void> r) throws Exception
            {
                State.waitForTopologyChange.await();
                r.call();
            }
        }

        public static class FailIncomingStream
        {
            public static IInstanceInitializer install(int... nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(CassandraStreamReceiver.class)
                                           .method(named("finished").and(takesNoArguments()))
                                           .intercept(MethodDelegation.to(FailIncomingStream.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static void finished(@This CassandraStreamReceiver self)
            {
                throw new RuntimeException("Failing incoming stream for test");
            }
        }

        public static class FailDiskMove
        {
            public static IInstanceInitializer install(int... nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(SSTableReader.class)
                                           .method(named("moveAndOpenSSTable").and(takesArguments(5)))
                                           .intercept(MethodDelegation.to(FailDiskMove.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static SSTableReader moveAndOpenSSTable(ColumnFamilyStore cfs, Descriptor oldDescriptor, Descriptor newDescriptor, Set<Component> components, boolean copyData, @SuperCall Callable<SSTableReader> r) throws Exception
            {
                if (State.shouldFailDisk.get())
                {
                    State.shouldFailDisk.set(false);
                    throw new RuntimeException("Failing move and open SSTable");
                }
                return r.call();
            }
        }
    }
}
