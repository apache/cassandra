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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.cfk.CommandsForKey;
import accord.impl.SimpleProgressLog;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.Status;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.lang.String.format;

public class AccordIncrementalRepairTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordIncrementalRepairTest.class);

    public static class BarrierRecordingAgent extends AccordAgent
    {
        static class ExecutedBarrier
        {
            final Seekables<?, ?> keysOrRanges;
            final @Nonnull Timestamp executeAt;

            public ExecutedBarrier(Seekables<?, ?> keysOrRanges, @Nonnull Timestamp executeAt)
            {
                this.keysOrRanges = keysOrRanges;
                this.executeAt = executeAt;
            }

            @Override
            public String toString()
            {
                return "ExecutedBarrier{" +
                       "keysOrRanges=" + keysOrRanges +
                       ", executeAt=" + executeAt +
                       '}';
            }
        }

        private final List<ExecutedBarrier> barriers = new ArrayList<>();

        @Override
        public void onLocalBarrier(@Nonnull Seekables<?, ?> keysOrRanges, @Nonnull TxnId txnId)
        {
            super.onLocalBarrier(keysOrRanges, txnId);
            synchronized (barriers)
            {
                barriers.add(new ExecutedBarrier(keysOrRanges, txnId));
            }
        }

        public List<ExecutedBarrier> executedBarriers()
        {
            synchronized (barriers)
            {
                return ImmutableList.copyOf(barriers);
            }
        }

        public void reset()
        {
            synchronized (barriers)
            {
                barriers.clear();
            }
        }

    }

    static BarrierRecordingAgent agent()
    {
        AccordService service = (AccordService) AccordService.instance();
        return (BarrierRecordingAgent) service.node().agent();
    }

    static AccordService accordService()
    {
        return (AccordService) AccordService.instance();
    }

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        CassandraRelevantProperties.ACCORD_AGENT_CLASS.setString(BarrierRecordingAgent.class.getName());
//        setupCluster(opt -> opt.withConfig(conf -> conf.with(Feature.NETWORK, Feature.GOSSIP)), 3);
        setupCluster(opt -> opt, 3);
    }

    @After
    public void tearDown()
    {
        SHARED_CLUSTER.filters().reset();
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

    private static <V> V getUninterruptibly(Future<V> future, long timeout, TimeUnit units)
    {
        try
        {
            return future.get(timeout, units);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <V> V getUninterruptibly(Future<V> future)
    {
        return getUninterruptibly(future, 1, TimeUnit.MINUTES);
    }

    private static TxnId awaitLocalApplyOnKey(PartitionKey key)
    {
        Node node = accordService().node();
        AtomicReference<TxnId> waitFor = new AtomicReference<>(null);
        AsyncChains.awaitUninterruptibly(node.commandStores().ifLocal(PreLoadContext.contextFor(key), key.toUnseekable(), 0, Long.MAX_VALUE, safeStore -> {
            AccordSafeCommandStore store = (AccordSafeCommandStore) safeStore;
            CommandsForKey commands = store.maybeCommandsForKey(key).current();
            int size = commands.size();
            if (size < 1)
                return;
            // if txnId is an instance of CommandsForKey.TxnInfo, copying it into a
            // new txnId instance will prevent any issues related to TxnInfo#hashCode
            waitFor.set(new TxnId(commands.txnId(size - 1)));
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
            AsyncChains.awaitUninterruptibly(node.commandStores().ifLocal(PreLoadContext.contextFor(txnId), key.toUnseekable(), 0, Long.MAX_VALUE, safeStore -> {
                SafeCommand command = safeStore.get(txnId, key.toUnseekable());
                Assert.assertNotNull(command.current());
                if (command.current().status().hasBeen(Status.Applied))
                    applied.set(true);
            }));
        }
        return txnId;
    }

    @Test
    public void txnRepairTest() throws Throwable
    {
        SHARED_CLUSTER.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='full' AND fast_path={'size':2};", KEYSPACE, tableName));
        final String keyspace = KEYSPACE;
        final String table = tableName;

        SHARED_CLUSTER.filters().allVerbs().to(3).drop();
        awaitEndpointDown(SHARED_CLUSTER.get(1), SHARED_CLUSTER.get(3));

        executeWithRetry(SHARED_CLUSTER, format("BEGIN TRANSACTION\n" +
                                                "INSERT INTO %s (k, v) VALUES (1, 1);\n" +
                                                "COMMIT TRANSACTION", qualifiedTableName));

        SHARED_CLUSTER.get(1, 2).forEach(instance -> instance.runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            awaitLocalApplyOnKey(new PartitionKey(metadata.id, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1))));
        }));

        SHARED_CLUSTER.forEach(instance -> instance.runOnInstance(() -> agent().reset()));

        SHARED_CLUSTER.get(1, 2).forEach(instance -> {
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
        SHARED_CLUSTER.get(3).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        });

        // heal partition and wait for node 1 to see node 3 again
        for (IInvokableInstance instance : SHARED_CLUSTER)
            instance.runOnInstance(() -> {
                SimpleProgressLog.PAUSE_FOR_TEST = true;
                Assert.assertTrue(agent().executedBarriers().isEmpty());
            });
        SHARED_CLUSTER.filters().reset();
        awaitEndpointUp(SHARED_CLUSTER.get(1), SHARED_CLUSTER.get(3));
        SHARED_CLUSTER.get(1).nodetool("repair", KEYSPACE);

        SHARED_CLUSTER.forEach(instance -> {
            instance.runOnInstance(() -> {
                Assert.assertFalse( agent().executedBarriers().isEmpty());
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
                cfs.getLiveSSTables().forEach(sstable -> {
                    Assert.assertTrue(sstable.isRepaired() || sstable.isPendingRepair());
                });
            });
        });
    }

    private void testSingleNodeWrite(TransactionalMode mode)
    {
        SHARED_CLUSTER.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='%s';", KEYSPACE, tableName, mode));
        final String keyspace = KEYSPACE;
        final String table = tableName;

        SHARED_CLUSTER.get(3).runOnInstance(() -> {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 2);", keyspace, table));
        });

        SHARED_CLUSTER.get(3).runOnInstance(() -> {
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
        SHARED_CLUSTER.get(1, 2).forEach(instance -> instance.runOnInstance(() -> {
            UntypedResultSet result = QueryProcessor.executeInternal(format("SELECT * FROM %s.%s WHERE k=1", keyspace, table));
            Assert.assertTrue(result.isEmpty());

            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        }));
        SHARED_CLUSTER.forEach(instance -> instance.runOnInstance(() -> {
            agent().reset();
        }));

        SHARED_CLUSTER.get(1).nodetool("repair", KEYSPACE);
        SHARED_CLUSTER.forEach(instance -> instance.runOnInstance(() -> {
            Assert.assertFalse( agent().executedBarriers().isEmpty());
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
        }));
    }

    /**
     * a failed write at txn mode unsafe should be made visible by repair
     */
    @Test
    public void unsafeRepairTest()
    {
        testSingleNodeWrite(TransactionalMode.unsafe);
    }

    /**
     * Repair should repair (fully replicate _some_ state) any divergent state between replicas
     */
    @Test
    public void fullRepairTest()
    {
        testSingleNodeWrite(TransactionalMode.full);
    }

    @Test
    public void onlyAccordTest()
    {
        SHARED_CLUSTER.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) WITH transactional_mode='full' AND fast_path={'size':2};", KEYSPACE, tableName));
        final String keyspace = KEYSPACE;
        final String table = tableName;

        SHARED_CLUSTER.filters().allVerbs().to(3).drop();
        awaitEndpointDown(SHARED_CLUSTER.get(1), SHARED_CLUSTER.get(3));
        awaitEndpointDown(SHARED_CLUSTER.get(2), SHARED_CLUSTER.get(3));

        executeWithRetry(SHARED_CLUSTER, format("BEGIN TRANSACTION\n" +
                                                "INSERT INTO %s (k, v) VALUES (1, 1);\n" +
                                                "COMMIT TRANSACTION", qualifiedTableName));

        SHARED_CLUSTER.get(1, 2).forEach(instance -> instance.runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            awaitLocalApplyOnKey(new PartitionKey(metadata.id, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1))));
        }));

        SHARED_CLUSTER.forEach(instance -> instance.runOnInstance(() -> agent().reset()));

        SHARED_CLUSTER.filters().reset();
        awaitEndpointUp(SHARED_CLUSTER.get(1), SHARED_CLUSTER.get(3));
        SHARED_CLUSTER.get(1).nodetool("repair", "--accord-only", KEYSPACE);

        SHARED_CLUSTER.forEach(instance -> {
            logger().info("checking instance {}", instance.broadcastAddress());
            instance.runOnInstance(() -> {
                Assert.assertFalse( agent().executedBarriers().isEmpty());
            });
        });
    }
}
