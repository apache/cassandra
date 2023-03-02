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

package org.apache.cassandra.distributed.test;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.ActiveCompactions;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

public class UpgradeSSTablesTest extends TestBaseImpl
{
    @Test
    public void upgradeSSTablesInterruptsOngoingCompaction() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck));");
            cluster.get(1).acceptsOnInstance((String ks) -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                cfs.disableAutoCompaction();
                CompactionManager.instance.setMaximumCompactorThreads(1);
                CompactionManager.instance.setCoreCompactorThreads(1);
            }).accept(KEYSPACE);

            String blob = "blob";
            for (int i = 0; i < 6; i++)
                blob += blob;

            for (int cnt = 0; cnt < 5; cnt++)
            {
                for (int i = 0; i < 100; i++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?,?,?)",
                                                   ConsistencyLevel.QUORUM, (cnt * 1000) + i, i, blob);
                }
                cluster.get(1).nodetool("flush", KEYSPACE, "tbl");
            }

            LogAction logAction = cluster.get(1).logs();
            logAction.mark();
            Future<?> future = cluster.get(1).asyncAcceptsOnInstance((String ks) -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                CompactionManager.instance.submitMaximal(cfs, FBUtilities.nowInSeconds(), false, OperationType.COMPACTION);
            }).apply(KEYSPACE);
            Assert.assertEquals(0, cluster.get(1).nodetool("upgradesstables", "-a", KEYSPACE, "tbl"));
            future.get();
            Assert.assertFalse(logAction.grep("Compaction interrupted").getResult().isEmpty());
        }
    }

    @Test
    public void compactionDoesNotCancelUpgradeSSTables() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck));");
            cluster.get(1).acceptsOnInstance((String ks) -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                cfs.disableAutoCompaction();
                CompactionManager.instance.setMaximumCompactorThreads(1);
                CompactionManager.instance.setCoreCompactorThreads(1);
            }).accept(KEYSPACE);

            String blob = "blob";
            for (int i = 0; i < 6; i++)
                blob += blob;

            for (int cnt = 0; cnt < 5; cnt++)
            {
                for (int i = 0; i < 100; i++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?,?,?)",
                                                   ConsistencyLevel.QUORUM, (cnt * 1000) + i, i, blob);
                }
                cluster.get(1).nodetool("flush", KEYSPACE, "tbl");
            }

            LogAction logAction = cluster.get(1).logs();
            logAction.mark();
            Assert.assertEquals(0, cluster.get(1).nodetool("upgradesstables", "-a", KEYSPACE, "tbl"));
            Assert.assertFalse(logAction.watchFor("Compacting").getResult().isEmpty());

            cluster.get(1).acceptsOnInstance((String ks) -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                FBUtilities.allOf(CompactionManager.instance.submitMaximal(cfs, FBUtilities.nowInSeconds(), false, OperationType.COMPACTION))
                           .awaitUninterruptibly(1, TimeUnit.MINUTES);

            }).accept(KEYSPACE);
            Assert.assertTrue(logAction.grep("Compaction interrupted").getResult().isEmpty());
            Assert.assertFalse(logAction.grep("Finished Upgrade sstables").getResult().isEmpty());
            Assert.assertFalse(logAction.grep("Compacted (.*) 5 sstables to").getResult().isEmpty());
        }
    }

    @Test
    public void cleanupDoesNotInterruptUpgradeSSTables() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1).withInstanceInitializer(BB::install).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck));");

            cluster.get(1).acceptsOnInstance((String ks) -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                cfs.disableAutoCompaction();
            }).accept(KEYSPACE);

            String blob = "blob";
            for (int i = 0; i < 6; i++)
                blob += blob;

            for (int i = 0; i < 10000; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?,?,?)",
                                               ConsistencyLevel.QUORUM, i, i, blob);
            }

            cluster.get(1).nodetool("flush", KEYSPACE, "tbl");

            LogAction logAction = cluster.get(1).logs();
            logAction.mark();

            // Start upgradingsstables - use BB to pause once inside ActiveCompactions.beginCompaction
            Thread upgradeThread = new Thread(() -> {
                cluster.get(1).nodetool("upgradesstables", "-a", KEYSPACE, "tbl");
            });
            upgradeThread.start();
            Assert.assertTrue(cluster.get(1).callOnInstance(() -> BB.starting.awaitUninterruptibly(1, TimeUnit.MINUTES)));

            // Start a scrub and make sure that it fails, log check later to make sure it was
            // because it cannot cancel the active upgrade sstables
            Assert.assertNotEquals(0, cluster.get(1).nodetool("scrub", KEYSPACE, "tbl"));

            // Now resume the upgrade sstables so test can shut down
            cluster.get(1).runOnInstance(() -> {
                BB.start.decrement();
            });
            upgradeThread.join();

            Assert.assertFalse(logAction.grep("Unable to cancel in-progress compactions, since they're running with higher or same priority: Upgrade sstables").getResult().isEmpty());
            Assert.assertFalse(logAction.grep("Starting Scrub for ").getResult().isEmpty());
            Assert.assertFalse(logAction.grep("Finished Upgrade sstables for distributed_test_keyspace.tbl successfully").getResult().isEmpty());
        }
    }

    @Test
    public void truncateWhileUpgrading() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck)) "));
            cluster.get(1).acceptsOnInstance((String ks) -> {
                ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                cfs.disableAutoCompaction();
                CompactionManager.instance.setMaximumCompactorThreads(1);
                CompactionManager.instance.setCoreCompactorThreads(1);
            }).accept(KEYSPACE);

            String blob = "blob";
            for (int i = 0; i < 10; i++)
                blob += blob;

            for (int i = 0; i < 500; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"),
                                               ConsistencyLevel.QUORUM, i, i, blob);
                if (i > 0 && i % 100 == 0)
                    cluster.get(1).nodetool("flush", KEYSPACE, "tbl");
            }

            LogAction logAction = cluster.get(1).logs();
            logAction.mark();

            Future<?> upgrade = CompletableFuture.runAsync(() -> {
                cluster.get(1).nodetool("upgradesstables", "-a", KEYSPACE, "tbl");
            });

            cluster.schemaChange(withKeyspace("TRUNCATE %s.tbl"));
            upgrade.get();
            Assert.assertFalse(logAction.grep("Compaction interrupted").getResult().isEmpty());
        }
    }

    @Test
    public void rewriteSSTablesTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = builder().withNodes(1).withDataDirCount(1).start())
        {
            for (String compressionBefore : new String[]{ "{'class' : 'LZ4Compressor', 'chunk_length_in_kb' : 32}", "{'enabled': 'false'}" })
            {
                for (String command : new String[]{ "upgradesstables", "recompress_sstables" })
                {
                    cluster.schemaChange(withKeyspace("DROP KEYSPACE IF EXISTS %s"));
                    cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"));

                    cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                                                      "WITH compression = " + compressionBefore));
                    cluster.get(1).acceptsOnInstance((String ks) -> {
                        Keyspace.open(ks).getColumnFamilyStore("tbl").disableAutoCompaction();
                    }).accept(KEYSPACE);

                    String blob = "blob";
                    for (int i = 0; i < 6; i++)
                        blob += blob;

                    for (int i = 0; i < 100; i++)
                    {
                        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"),
                                                       ConsistencyLevel.QUORUM, i, i, blob);
                    }
                    cluster.get(1).nodetool("flush", KEYSPACE, "tbl");

                    Assert.assertEquals(0, cluster.get(1).nodetool("upgradesstables", "-a", KEYSPACE, "tbl"));
                    cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH compression = {'class' : 'LZ4Compressor', 'chunk_length_in_kb' : 128};"));

                    Thread.sleep(2000); // Make sure timestamp will be different even with 1-second resolution.

                    long maxSoFar = cluster.get(1).appliesOnInstance((String ks) -> {
                        long maxTs = -1;
                        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                        cfs.disableAutoCompaction();
                        for (SSTableReader tbl : cfs.getLiveSSTables())
                        {
                            maxTs = Math.max(maxTs, tbl.getDataCreationTime());
                        }
                        return maxTs;
                    }).apply(KEYSPACE);

                    for (int i = 100; i < 200; i++)
                    {
                        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"),
                                                       ConsistencyLevel.QUORUM, i, i, blob);
                    }
                    cluster.get(1).nodetool("flush", KEYSPACE, "tbl");

                    LogAction logAction = cluster.get(1).logs();
                    logAction.mark();

                    long expectedCount = cluster.get(1).appliesOnInstance((String ks, Long maxTs) -> {
                        long count = 0;
                        long skipped = 0;
                        Set<SSTableReader> liveSSTables = Keyspace.open(ks).getColumnFamilyStore("tbl").getLiveSSTables();
                        assert liveSSTables.size() == 2 : String.format("Expected 2 sstables, but got " + liveSSTables.size());
                        for (SSTableReader tbl : liveSSTables)
                        {
                            if (tbl.getDataCreationTime() <= maxTs)
                                count++;
                            else
                                skipped++;
                        }
                        assert skipped > 0;
                        return count;
                    }).apply(KEYSPACE, maxSoFar);

                    if (command.equals("upgradesstables"))
                        Assert.assertEquals(0, cluster.get(1).nodetool("upgradesstables", "-a", "-t", Long.toString(maxSoFar), KEYSPACE, "tbl"));
                    else
                        Assert.assertEquals(0, cluster.get(1).nodetool("recompress_sstables", KEYSPACE, "tbl"));

                    Assert.assertFalse(logAction.grep(String.format("%d sstables to", expectedCount)).getResult().isEmpty());
                }
            }
        }
    }

    public static class BB
    {
        // Will be initialized in the context of the instance class loader
        static CountDownLatch starting = newCountDownLatch(1);
        static CountDownLatch start = newCountDownLatch(1);

        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(ActiveCompactions.class)
                           .method(named("beginCompaction"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void beginCompaction(CompactionInfo.Holder ci, @SuperCall Callable<Void> zuperCall)
        {
            try
            {
                zuperCall.call();
                if (ci.getCompactionInfo().getTaskType() == OperationType.UPGRADE_SSTABLES)
                {
                    starting.decrement();
                    Assert.assertTrue(start.awaitUninterruptibly(1, TimeUnit.MINUTES));
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
