///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.distributed.test.repair;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Objects;
//import java.util.UUID;
//import java.util.concurrent.Callable;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.function.BooleanSupplier;
//
//import org.junit.Test;
//
//import net.bytebuddy.ByteBuddy;
//import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
//import net.bytebuddy.implementation.MethodDelegation;
//import net.bytebuddy.implementation.bind.annotation.SuperCall;
//import org.apache.cassandra.db.ColumnFamilyStore;
//import org.apache.cassandra.db.Keyspace;
//import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
//import org.apache.cassandra.db.compaction.CompactionManager;
//import org.apache.cassandra.db.compaction.CompactionStrategyManager;
//import org.apache.cassandra.db.compaction.PendingRepairManager;
//import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
//import org.apache.cassandra.dht.Murmur3Partitioner;
//import org.apache.cassandra.distributed.Cluster;
//import org.apache.cassandra.distributed.api.ConsistencyLevel;
//import org.apache.cassandra.distributed.api.Feature;
//import org.apache.cassandra.distributed.api.IInvokableInstance;
//import org.apache.cassandra.distributed.test.TestBaseImpl;
//import org.apache.cassandra.io.sstable.format.SSTableReader;
//import org.apache.cassandra.locator.RangesAtEndpoint;
//import org.apache.cassandra.repair.consistent.ConsistentSession;
//import org.apache.cassandra.repair.consistent.LocalSession;
//import org.apache.cassandra.schema.Schema;
//import org.apache.cassandra.service.ActiveRepairService;
//import org.apache.cassandra.utils.Shared;
//import org.apache.cassandra.utils.TimeUUID;
//import org.apache.cassandra.utils.concurrent.CountDownLatch;
//import org.apache.cassandra.utils.concurrent.Future;
//import org.apache.cassandra.utils.concurrent.Refs;
//import org.assertj.core.api.Assertions;
//
//import static net.bytebuddy.matcher.ElementMatchers.named;
//import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
//
//public class FailedIRDelinksSSTablesTest extends TestBaseImpl
//{
//    @Test
//    public void passing() throws IOException
//    {
//        try (Cluster cluster = init(Cluster.build(2)
//                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)
//                                                             // to protect against default changes
//                                                             .set("partitioner", Murmur3Partitioner.class.getSimpleName())
//                                                             .set("repair.retries.max_attempts", Integer.MAX_VALUE))
//                                           .withInstanceInitializer(RacyBB::install)
//                                           .start()))
//        {
//            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl1 (key BLOB PRIMARY KEY)"));
//
//            String cql = String.format("INSERT INTO %s.%s(key) VALUES (?)", KEYSPACE, "tbl1");
//            IInvokableInstance coordinator = cluster.get(2);
//            coordinator.executeInternal(cql, keyForToken(30));
//            coordinator.executeInternal(cql, keyForToken(42));
//            coordinator.executeInternal(cql, keyForToken(60));
//
//            cluster.get(1).nodetoolResult("repair", KEYSPACE, "--start-token", "40", "--end-token", "50").asserts().failure();
//            // did anything leak?
//            cluster.forEach(n -> {
//                String name = "node" + n.config().num();
//                n.forceCompact(KEYSPACE, "tbl1");
//                n.runOnInstance(() -> {
//                    Keyspace ks = Keyspace.open("distributed_test_keyspace");
//                    ColumnFamilyStore tbl1 = ks.getColumnFamilyStore("tbl1");
//                    for (SSTableReader sstable : tbl1.getTracker().getView().liveSSTables())
//                        Assertions.assertThat(sstable.getSSTableMetadata().pendingRepair).describedAs("%s had leak detected on sstable %s", name, sstable.descriptor).isNull();
//                });
//            });
//        }
//    }
//
//    public static class RacyBB
//    {
//        public static void install(ClassLoader classLoader, Integer num)
//        {
//            if (num != 1)
//                return;
//            new ByteBuddy().rebase(PendingRepairManager.class)
//                           .method(named("removeSessionIfEmpty"))
//                           .intercept(MethodDelegation.to(RacyBB.class))
//                           .make()
//                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
//            new ByteBuddy().rebase(CompactionStrategyManager.class)
//                           .method(named("compactionStrategyFor"))
//                           .intercept(MethodDelegation.to(RacyBB.class))
//                           .make()
//                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
//        }
//
//        public static AbstractCompactionStrategy compactionStrategyFor(SSTableReader sstable, @SuperCall Callable<AbstractCompactionStrategy> run)
//        {
//            AbstractCompactionStrategy strat;
//            try
//            {
//                strat = run.call();
//            }
//            catch (Exception e)
//            {
//                throw new AssertionError(e);
//            }
//            // don't block, its not the table we are looking for
//            if (!KEYSPACE.equals(sstable.metadata().keyspace))
//                return strat;
//            TimeUUID sessionID = Objects.requireNonNull(sstable.getSSTableMetadata().pendingRepair);
//            ActiveRepairService.instance().consistent.local.failSession(sessionID, false);
//            // this is the table... run a pending repair cleanup now that we have a strat registered...
//            List<Future<?>> background;
//            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(sstable.metadata().id);
//            do
//            {
//                background = CompactionManager.instance.submitBackground(cfs);
//            }
//            while (background.isEmpty());
//            try
//            {
//                latch.await();
//            }
//            catch (InterruptedException e)
//            {
//                throw new AssertionError(e);
//            }
//            return strat;
//        }
//
//        private static final CountDownLatch latch = CountDownLatch.newCountDownLatch(1);
//
//        public static void removeSessionIfEmpty(TimeUUID sessionID, @SuperCall Runnable run)
//        {
//            // Need to avoid calling this with the lock until the following condition is met:
//            // Streaming calls org.apache.cassandra.db.compaction.CompactionStrategyManager.handleFlushNotification
//            // that logic calls org.apache.cassandra.db.compaction.CompactionStrategyManager.compactionStrategyFor
//            // once the AbstractCompactionStrategy is held and BEFORE addSSTable is called, trigger this
//
////            if (mgr.hasDataForSession(sessionID))
////                true;
//            run.run();
//            latch.decrement();
//        }
//    }
//
//    @Test
//    public void failedDuringAntiCompation() throws IOException
//    {
//        try (Cluster cluster = init(Cluster.build(2)
//                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)
//                                                             // to protect against default changes
//                                                             .set("partitioner", Murmur3Partitioner.class.getSimpleName())
//                                                             .set("repair.retries.max_attempts", Integer.MAX_VALUE))
//                                           .withInstanceInitializer(BB::install)
//                                           .start()))
//        {
//            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl1 (key BLOB PRIMARY KEY)"));
//            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl2 (key BLOB PRIMARY KEY)"));
//            // make sure we know when compaction runs...
//            cluster.forEach(n -> n.nodetoolResult("disableautocompaction").asserts().success());
//
//            for (String table : Arrays.asList("tbl1", "tbl2"))
//            {
//                String cql = String.format("INSERT INTO %s.%s(key) VALUES (?)", KEYSPACE, table);
//                cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL, keyForToken(30));
//                cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL, keyForToken(42));
//                cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL, keyForToken(60));
//            }
//
//            cluster.get(1).nodetoolResult("repair", KEYSPACE, "--start-token", "40", "--end-token", "50").asserts().failure();
//            // make sure all nodes have failed
//            cluster.get(2).runOnInstance(() -> {
//                TimeUUID sessionID = TimeUUID.fromUuid(State.sessionId);
//                LocalSession session;
//                do
//                {
//                    session = ActiveRepairService.instance().consistent.local.getSession(sessionID);
//                }
//                while (session.getState() != ConsistentSession.State.FAILED);
//            });
//            // did anything leak?
//            cluster.forEach(n -> {
//                String name = "node" + n.config().num();
//                n.forceCompact(KEYSPACE, "tbl1");
//                n.forceCompact(KEYSPACE, "tbl2");
//                n.runOnInstance(() -> {
//                    Keyspace ks = Keyspace.open("distributed_test_keyspace");
//                    ColumnFamilyStore tbl1 = ks.getColumnFamilyStore("tbl1");
//                    for (SSTableReader sstable : tbl1.getTracker().getView().liveSSTables())
//                        Assertions.assertThat(sstable.getSSTableMetadata().pendingRepair).describedAs("%s had leak detected on sstable %s", name, sstable.descriptor).isNull();
//                    ColumnFamilyStore tbl2 = ks.getColumnFamilyStore("tbl2");
//                    for (SSTableReader sstable : tbl2.getTracker().getView().liveSSTables())
//                        Assertions.assertThat(sstable.getSSTableMetadata().pendingRepair).describedAs("%s had leak detected on sstable %s", name, sstable.descriptor).isNull();
//                });
//            });
//        }
//    }
//
//    public static class BB
//    {
//        public static void install(ClassLoader classLoader, Integer num)
//        {
//            new ByteBuddy().rebase(CompactionManager.class)
//                           .method(named("performAnticompaction"))
//                           .intercept(MethodDelegation.to(BB.class))
//                           .make()
//                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
//        }
//
//        private static final AtomicBoolean run = new AtomicBoolean(false);
//
//        public synchronized static void performAnticompaction(ColumnFamilyStore cfs,
//                                                              RangesAtEndpoint replicas,
//                                                              Refs<SSTableReader> validatedForRepair,
//                                                              LifecycleTransaction txn,
//                                                              TimeUUID sessionID,
//                                                              BooleanSupplier isCancelled,
//                                                              @SuperCall Runnable zuper) throws IOException
//        {
//            if (run.compareAndSet(false, true))
//            {
//                zuper.run();
//                return;
//            }
//            // this is the second attempt... lets make sure the repair gets failed
//            ActiveRepairService.instance().consistent.local.failSession(sessionID, true);
//            State.sessionId = sessionID.asUUID();
//            zuper.run();
//        }
//    }
//
//    @Shared
//    public static class State
//    {
//        public static volatile UUID sessionId = null;
//    }
//}
