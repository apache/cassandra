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

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.durability.DurabilityService;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import com.google.common.collect.Iterables;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.IAccordService.DelegatingAccordService;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.READ_WRITE;
import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.accord.AccordTestBase.executeWithRetry;

public class AccordIncrementalRepairTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordIncrementalRepairTest.class);

    public static class BarrierRecordingService extends DelegatingAccordService
    {
        private volatile boolean executedBarriers = false;

        public BarrierRecordingService(IAccordService delegate)
        {
            super(delegate);
        }

        @Override
        public AsyncResult<Void> sync(Object requestedBy, @Nullable Timestamp onOrAfter, Ranges ranges, @Nullable Collection<Node.Id> include, DurabilityService.SyncLocal syncLocal, DurabilityService.SyncRemote syncRemote, long timeout, TimeUnit timeoutUnits)
        {
            return delegate.sync(requestedBy, onOrAfter, ranges, include, syncLocal, syncRemote, 10L, TimeUnit.MINUTES).map(v -> {
                executedBarriers = true;
                return v;
            }).beginAsResult();
        }

        @Override
        public AsyncResult<Void> sync(@Nullable Timestamp onOrAfter, Keys keys, DurabilityService.SyncLocal syncLocal, DurabilityService.SyncRemote syncRemote)
        {
            return delegate.sync(onOrAfter, keys, syncLocal, syncRemote).map(v -> {
                executedBarriers = true;
                return v;
            }).beginAsResult();
        }

        public void reset()
        {
            executedBarriers = false;
        }
    }

    static BarrierRecordingService barrierRecordingService()
    {
        return (BarrierRecordingService) AccordService.instance();
    }

    static IAccordService accordService()
    {
        return AccordService.instance();
    }

    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws Throwable
    {


    }

    private static Cluster createCluster() throws Throwable
    {
        Cluster cluster = AccordTestBase.createCluster(3, opt -> opt.withConfig(conf -> conf.with(Feature.NETWORK, Feature.GOSSIP)
                                                           .set("accord.recover_txn", "1s")
                                                           .set("accord.retry_syncpoint", "1s*attempts")
                                                           .set("accord.retry_durability", "1s*attempts")
                                                           .set("accord.shard_durability_target_splits", 4)));
        for (IInvokableInstance instance : cluster)
            instance.runOnInstance(() -> AccordService.unsafeSetNewAccordService(new BarrierRecordingService(AccordService.instance())));

        return cluster;
    }

    private static void withCluster(ModelChecker.ThrowingConsumer<Cluster> run) throws Throwable
    {
        try (Cluster cluster = createCluster())
        {
            try
            {
                run.accept(cluster);
            }
            catch (Throwable t)
            {
                cluster.filters().reset();
                for (IInvokableInstance instance : cluster)
                    instance.runOnInstance(() -> AccordService.instance().node().commandStores().forEachCommandStore(cs -> cs.unsafeProgressLog().start()));
            }
        }
    }


    private static void await(IInvokableInstance instance, IIsolatedExecutor.SerializableCallable<Boolean> check, long duration, TimeUnit unit)
    {
        instance.runOnInstance(() -> {
            long timeout = Clock.Global.currentTimeMillis() + unit.toMillis(duration);
            while (Clock.Global.currentTimeMillis() < timeout)
            {
                if (check.call())
                    return;

                try
                {
                    Thread.sleep(1);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
            throw new AssertionError("Timed out waiting for node 3 to become alive");
        });
    }

    private static void awaitEndpointUp(IInvokableInstance instance, IInvokableInstance waitOn)
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByAddress(waitOn.broadcastAddress());
        await(instance, () -> FailureDetector.instance.isAlive(endpoint), 1, TimeUnit.MINUTES);
    }

    private static void awaitEndpointDown(IInvokableInstance instance, IInvokableInstance waitOn)
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByAddress(waitOn.broadcastAddress());
        await(instance, () -> !FailureDetector.instance.isAlive(endpoint), 1, TimeUnit.MINUTES);
    }

    private static TxnId awaitLocalApplyOnKey(TableMetadata metadata, int k)
    {
        return awaitLocalApplyOnKey(new TokenKey(metadata.id, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(k)).getToken()));
    }

    private static TxnId awaitLocalApplyOnKey(TokenKey key)
    {
        Node node = accordService().node();
        AtomicReference<TxnId> waitFor = new AtomicReference<>(null);
        AsyncChains.awaitUninterruptibly(node.commandStores().ifLocal(PreLoadContext.contextFor(key, SYNC, READ_WRITE, "Test"), key.toUnseekable(), 0, Long.MAX_VALUE, safeStore -> {
            AccordSafeCommandStore store = (AccordSafeCommandStore) safeStore;
            SafeCommandsForKey safeCfk = store.ifLoadedAndInitialised(key);
            if (safeCfk == null)
                return;
            CommandsForKey cfk = safeCfk.current();
            int size = cfk.size();
            if (size < 1)
                return;
            // if txnId is an instance of CommandsForKey.TxnInfo, copying it into a
            // new txnId instance will prevent any issues related to TxnInfo#hashCode
            waitFor.set(new TxnId(cfk.txnId(size - 1)));
        }));
        Assert.assertNotNull(waitFor.get());
        TxnId txnId = waitFor.get();
        long start = Clock.Global.currentTimeMillis();
        AtomicBoolean applied = new AtomicBoolean(false);
        while (!applied.get())
        {
            long now = Clock.Global.currentTimeMillis();
            if (now - start > TimeUnit.MINUTES.toMillis(1))
                throw new AssertionError("Timeout");
            AsyncChains.awaitUninterruptibly(node.commandStores().ifLocal(PreLoadContext.contextFor(txnId, "Test"), key.toUnseekable(), 0, Long.MAX_VALUE, safeStore -> {
                SafeCommand command = safeStore.get(txnId, StoreParticipants.empty(txnId));
                Assert.assertNotNull(command.current());
                if (command.current().status().hasBeen(Status.Applied))
                    applied.set(true);
            }));
        }
        return txnId;
    }

    // TODO (required): After conversation with Ariel: it's a known issue that I am not sure we need to fix now.
    //  The problem is that we don't flush after Accord repair, but before data repair when running incremental
    //  repair so it doesn't see the repaired sstables it is checking for.
    //  This hard fails now that incremental repair Accord barriers are at all to account for the missing flushes
    @Ignore
    @Test
    public void txnRepairTest() throws Throwable
    {
        withCluster(this::txnRepairTest);
    }

    public void txnRepairTest(Cluster cluster) throws Throwable
    {
        final String keyspace = KEYSPACE;
        final String table = "accord_table";
        final String qualifiedTable = String.format("%s.%s", keyspace, table);
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='full' AND fast_path={'size':2};", keyspace, table));

        cluster.filters().allVerbs().to(3).drop();
        awaitEndpointDown(cluster.get(1), cluster.get(3));

        executeWithRetry(cluster, format("BEGIN TRANSACTION\n" +
                                                        "INSERT INTO %s (k, v) VALUES (1, 1);\n" +
                                                        "COMMIT TRANSACTION", qualifiedTable));

        cluster.get(1, 2).forEach(instance -> instance.runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            awaitLocalApplyOnKey(metadata, 1);
        }));

        cluster.forEach(instance -> instance.runOnInstance(() -> barrierRecordingService().reset()));

        cluster.get(1, 2).forEach(instance -> {
            instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
                cfs.getLiveSSTables().forEach(sstable -> {
                    Assert.assertFalse(sstable.isRepaired());
                    Assert.assertFalse(sstable.isPendingRepair());
                });
            });
        });
        cluster.get(3).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        });

        // heal partition and wait for node 1 to see node 3 again
        for (IInvokableInstance instance : cluster)
            instance.runOnInstance(() -> {
                AccordService.instance().node().commandStores().forEachCommandStore(cs -> cs.unsafeProgressLog().stop());
                Assert.assertFalse(barrierRecordingService().executedBarriers);
            });
        cluster.filters().reset();
        awaitEndpointUp(cluster.get(1), cluster.get(3));
        nodetool(cluster.get(1), "repair", keyspace);

        cluster.get(1).runOnInstance(() -> {
            Assert.assertTrue(barrierRecordingService().executedBarriers);
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
            cfs.getLiveSSTables().forEach(sstable -> {
                Assert.assertTrue(sstable.isRepaired() || sstable.isPendingRepair());
            });
        });
    }

    private void testSingleNodeWrite(Cluster cluster, TransactionalMode mode)
    {
        final String keyspace = KEYSPACE;
        final String table = "accord_table";
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='%s';", keyspace, table, mode));

        cluster.get(3).runOnInstance(() -> {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 2);", keyspace, table));
        });

        cluster.get(3).runOnInstance(() -> {
            UntypedResultSet result = QueryProcessor.executeInternal(format("SELECT * FROM %s.%s WHERE k=1", keyspace, table));
            Assert.assertFalse(result.isEmpty());
            UntypedResultSet.Row row = Iterables.getOnlyElement(result);
            Assert.assertEquals(1, row.getInt("k"));
            Assert.assertEquals(2, row.getInt("v"));



            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
            cfs.getLiveSSTables().forEach(sstable -> {
                Assert.assertFalse(sstable.isRepaired());
                Assert.assertFalse(sstable.isPendingRepair());
            });
        });
        cluster.get(1, 2).forEach(instance -> instance.runOnInstance(() -> {
            UntypedResultSet result = QueryProcessor.executeInternal(format("SELECT * FROM %s.%s WHERE k=1", keyspace, table));
            Assert.assertTrue(result.isEmpty());

            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        }));
        cluster.forEach(instance -> instance.runOnInstance(() -> {
            barrierRecordingService().reset();
        }));

        nodetool(cluster.get(1), "repair", KEYSPACE);
        cluster.get(1).runOnInstance(() -> {
            Assert.assertTrue(barrierRecordingService().executedBarriers);
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
            cfs.getLiveSSTables().forEach(sstable -> {
                Assert.assertTrue(sstable.isRepaired() || sstable.isPendingRepair());
            });

            UntypedResultSet result = QueryProcessor.executeInternal(format("SELECT * FROM %s.%s WHERE k=1", keyspace, table));
            Assert.assertFalse(result.isEmpty());
            UntypedResultSet.Row row = Iterables.getOnlyElement(result);
            Assert.assertEquals(1, row.getInt("k"));
            Assert.assertEquals(2, row.getInt("v"));
        });
    }

    /**
     * a failed write at txn mode unsafe should be made visible by repair
     */
    @Test
    public void unsafeRepairTest() throws Throwable
    {
        withCluster(cluster -> {
            testSingleNodeWrite(cluster, TransactionalMode.test_unsafe);
        });
    }

    /**
     * Repair should repair (fully replicate _some_ state) any divergent state between replicas
     */
    @Test
    public void fullRepairTest() throws Throwable
    {
        withCluster(cluster -> {
            testSingleNodeWrite(cluster, TransactionalMode.full);
        });
    }

    @Test
    public void onlyAccordTest() throws Throwable
    {
        withCluster(this::onlyAccordTest);
    }

    public void onlyAccordTest(Cluster cluster)
    {
        final String keyspace = KEYSPACE;
        final String table = "accord_table";
        final String qualifiedTable = String.format("%s.%s", keyspace, table);
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='full' AND fast_path={'size':2};", KEYSPACE, table));

        executeWithRetry(cluster, format("BEGIN TRANSACTION\n" +
                                                "INSERT INTO %s (k, v) VALUES (1, 1);\n" +
                                                "COMMIT TRANSACTION", qualifiedTable));

        cluster.get(1, 2).forEach(instance -> instance.runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            awaitLocalApplyOnKey(metadata, 1);
        }));

        cluster.forEach(instance -> instance.runOnInstance(() -> barrierRecordingService().reset()));

        cluster.filters().reset();
        awaitEndpointUp(cluster.get(1), cluster.get(3));
        nodetool(cluster.get(1), "repair", "--accord-only", KEYSPACE);

        cluster.get(1).runOnInstance(() -> {
            Assert.assertTrue(barrierRecordingService().executedBarriers);
        });
    }

    @Test
    public void onlyAccordWithForceTest() throws Throwable
    {
        withCluster(this::onlyAccordWithForceTest);
    }

    public void onlyAccordWithForceTest(Cluster cluster)
    {
        final String keyspace = KEYSPACE;
        final String table = "accord_table";
        final String qualifiedTable = String.format("%s.%s", keyspace, table);
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='full' AND fast_path={'size':2};", KEYSPACE, table));

        cluster.filters().allVerbs().to(3).drop();
        awaitEndpointDown(cluster.get(1), cluster.get(3));
        awaitEndpointDown(cluster.get(2), cluster.get(3));

        executeWithRetry(cluster, format("BEGIN TRANSACTION\n" +
                                                "INSERT INTO %s (k, v) VALUES (1, 1);\n" +
                                                "COMMIT TRANSACTION", qualifiedTable));

        cluster.get(1, 2).forEach(instance -> instance.runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            awaitLocalApplyOnKey(metadata, 1);
        }));

        cluster.forEach(instance -> instance.runOnInstance(() -> barrierRecordingService().reset()));

        cluster.filters().reset();
        awaitEndpointUp(cluster.get(1), cluster.get(3));
        nodetool(cluster.get(1), "repair", "--force", "--accord-only", KEYSPACE);

        cluster.get(1).runOnInstance(() -> {
            Assert.assertTrue(barrierRecordingService().executedBarriers);
        });
    }
}
