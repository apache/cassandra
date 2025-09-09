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

package org.apache.cassandra.distributed.test.tracking;

import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.TransferActivation;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * TODO: Document why import with a node down is not currently supported.
 */
public class BulkTransfersTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(BulkTransfersTest.class);
    private static final String TABLE = "tbl";

    @Test
    public void importHappyPath() throws Throwable
    {
        Hooks hooks = new Hooks() {
            @Override
            public void afterImport(Cluster cluster)
            {
                // Sleep for a while to make sure import completes
                Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

                for (IInvokableInstance instance : cluster)
                {
                    logger.info("Checking propagation of imported SSTable to {}", instance.config().num());
                    // SinglePartition + PartitionRange
                    {
                        Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE k = 1"));
                        AssertUtils.assertRows(rows, AssertUtils.row(1, 1));
                    }
                    {
                        Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE));
                        AssertUtils.assertRows(rows, AssertUtils.row(1, 1));
                    }
                }
            }
        };
        testTrackedImport(hooks);
    }

    @Test
    @Ignore("Import currently requires all replicas up, see docstring on BulkTransfersTest")
    public void importReplicaDown() throws Throwable
    {
        Hooks hooks = new Hooks() {
            @Override
            public void beforeImport(Cluster cluster)
            {
                try
                {
                    cluster.get(3).shutdown().get();
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void afterImport(Cluster cluster)
            {
                cluster.get(3).startup();

                // Sleep for a while to make sure import completes
                Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

                for (IInvokableInstance instance : cluster)
                {
                    logger.info("Checking propagation of imported SSTable to {}", instance.config().num());
                    // SinglePartition + PartitionRange
                    {
                        Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE k = 1"));
                        AssertUtils.assertRows(rows, AssertUtils.row(1, 1));
                    }
                    {
                        Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE));
                        AssertUtils.assertRows(rows, AssertUtils.row(1, 1));
                    }
                }
            }
        };
        testTrackedImport(hooks);
    }

    @Test
    public void importMissedActivation() throws Throwable
    {
        Hooks hooks = new Hooks() {
            @Override
            public IInstanceInitializer getInstanceInitializer()
            {
                return ByteBuddyInjections.SkipActivation.install(2);
            }

            @Override
            public void afterImport(Cluster cluster)
            {
                cluster.get(1).runOnInstance(() -> {
                    DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
                    TableId tableId = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).metadata().id;
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(key, tableId, false);
                    Assertions.assertThat(summary).satisfies(s -> {
                        assert s.reconciledIds() == 0;
                        assert s.unreconciledIds() == 1;
                    });
                });

                cluster.get(2).runOnInstance(() -> {
                    DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
                    TableId tableId = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).metadata().id;
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(key, tableId, false);
                    Assertions.assertThat(summary).satisfies(s -> {
                        assert s.reconciledIds() == 0;
                        assert s.unreconciledIds() == 0;
                    });
                });

                cluster.forEach(() -> ByteBuddyInjections.SkipActivation.skip = false);

                logger.debug("Checking read at ALL");

                // Use coordinated query rather than executeInternal to confirm read reconciliation triggers activation
                String cql = "SELECT * FROM %s." + TABLE + " WHERE k = 1";
                Object[][] rows = cluster.get(1).coordinator().execute(withKeyspace(cql), ConsistencyLevel.ALL);
                AssertUtils.assertRows(rows, AssertUtils.row(1, 1));

                // Confirm instance2 gets activated
                rows = cluster.get(2).executeInternal(withKeyspace(cql));
                AssertUtils.assertRows(rows, AssertUtils.row(1, 1));
            }
        };
        testTrackedImport(hooks);
    }

    public static class ByteBuddyInjections
    {
        // Only skips direct transfer activation, not activation as part of read reconciliation
        public static class SkipActivation
        {
            public static volatile boolean skip = true;

            public static IInstanceInitializer install(int...nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(TransferActivation.VerbHandler.class)
                                           .method(named("doVerb"))
                                           .intercept(MethodDelegation.to(ByteBuddyInjections.SkipActivation.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static void doVerb(Message<TransferActivation> msg, @SuperCall Callable<?> zuper)
            {
                if (skip && !msg.payload.dryRun)
                {
                    logger.info("Skipping activation for test {}", msg.payload);
                    return;
                }

                logger.info("Test running activation as usual {}", msg.payload);

                try
                {
                    zuper.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Test
    public void importBounceAfterPending() throws Throwable
    {
        Hooks hooks = new Hooks() {
            @Override
            public IInstanceInitializer getInstanceInitializer()
            {
                // No activation, transfer stays pending everywhere
                return ByteBuddyInjections.SkipActivation.install(1, 2, 3);
            }

            @Override
            public void afterImport(Cluster cluster)
            {
                String cql = "SELECT * FROM %s." + TABLE + " WHERE k = 1";
                Object[][] EMPTY = new Object[0][0];

                for (IInvokableInstance instance : cluster)
                {
                    Object[][] rows = instance.coordinator().execute(withKeyspace(cql), ConsistencyLevel.ALL);
                    AssertUtils.assertRows(rows, EMPTY);
                }

                // When an import fails, bounce must not move the pending SSTables into the live set
                bounce(cluster);

                for (IInvokableInstance instance : cluster)
                {
                    Object[][] rows = instance.coordinator().execute(withKeyspace(cql), ConsistencyLevel.ALL);
                    AssertUtils.assertRows(rows, EMPTY);
                }
            }
        };
        testTrackedImport(hooks);
    }

    private interface Hooks
    {
        default IInstanceInitializer getInstanceInitializer()
        {
            return (classLoader, threadGroup, num, generation) -> {};
        }

        default void beforeImport(Cluster cluster) {};
        void afterImport(Cluster cluster);
    }

    private void testTrackedImport(Hooks hooks) throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .withInstanceInitializer(hooks.getInstanceInitializer())
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));
            String KEYSPACE_TABLE = String.format("%s.%s", KEYSPACE, TABLE);
            String schema = String.format(withKeyspace("CREATE TABLE %s." + TABLE + " (k int primary key, v int);"));
            cluster.schemaChange(schema);

            // Hack: need to bounce for KeyspaceShards to be created for new table, schema changes not yet supported
            bounce(cluster);

            // In unified reconciliation, we depend on ALR to execute read reconciliation
            /*
            cluster.forEach(instance -> instance.runOnInstance(() -> {
                MutationTrackingService.instance.pauseActiveReconciler();
            }));
            */

            // Needs to run outside of instance executor because creates schema
            String file = Files.createTempDirectory(MutationTrackingTest.class.getSimpleName()).toString();

            try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                           .forTable(schema)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)")
                                                           .build())
            {
                writer.addRow(1, 1);
            }

            for (IInvokableInstance instance : cluster)
            {
                logger.info("Checking instance {} empty before import", instance.config().num());
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE));
                AssertUtils.assertRows(rows); // empty
            }

            hooks.beforeImport(cluster);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                logger.info("Importing SSTables {}", paths);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            hooks.afterImport(cluster);

            /*
            cluster.forEach(instance -> instance.runOnInstance(MutationTrackingService.instance::resumeActiveReconciler));
            */
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
}
