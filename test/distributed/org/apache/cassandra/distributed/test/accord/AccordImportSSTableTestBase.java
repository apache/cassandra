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

import org.apache.cassandra.distributed.test.TestBaseImpl;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.SuperMethod;
import net.bytebuddy.implementation.bind.annotation.This;

import org.agrona.collections.Long2LongHashMap;
import org.assertj.core.api.Assertions;
import org.junit.After;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.CoordinatedTransfer;
import org.apache.cassandra.service.accord.LocalTransfers;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;
import static org.junit.Assert.assertTrue;

public class AccordImportSSTableTestBase extends TestBaseImpl
{
    public static final String TABLE = "tbl";
    public static final String KEYSPACE_TABLE = String.format("%s.%s", KEYSPACE, TABLE);
    public static final String TABLE_SCHEMA_CQL = String.format(withKeyspace("CREATE TABLE %s." + TABLE + " (k int primary key, v int);"));

    @After
    public void reset()
    {
        State.waitForTopologyChange = new CountDownLatch(1);
        State.diskMoveAttempts = new AtomicInteger(0);
        State.failOnDiskMoveAttempt = new AtomicInteger(0);
        State.waitForCrash = new CountDownLatch(1);
        State.crashed = new AtomicBoolean(false);
    }

    static void createSchema(Cluster cluster)
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k int PRIMARY KEY, v int) WITH transactional_mode='full'");

        // Disable autocompaction so when we go to check the number of SSTables they correspond to the SSTables that we have imported
        cluster.forEach(instance -> instance.runOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).disableAutoCompaction()));
    }

    static String writeSSTables(int[]... sstables) throws Exception
    {
        String file = Files.createTempDirectory(AccordImportSSTableTest.class.getSimpleName()).toString();
        for (int[] sstable : sstables)
        {
            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                               .forTable(TABLE_SCHEMA_CQL)
                                                               .inDirectory(file)
                                                               .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)");

            try (CQLSSTableWriter writer = builder.build())
            {
                for (int key : sstable)
                    writer.addRow(key, 1);
            }
        }

        return file;
    }

    static void bounce(Cluster cluster)
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

    static void assertLocalTransferIsCleanup(Iterable<IInvokableInstance> validate)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                assertTrue(LocalTransfers.instance.local.isEmpty());
                assertTrue(LocalTransfers.instance.coordinating.isEmpty());
            });
        }
    }

    static void assertPendingDirs(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<File> forPendingUuidDir)
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

    static void assertSSTableCount(Iterable<IInvokableInstance> validate, int count)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Assertions.assertThat(cfs.getLiveSSTables().size()).isEqualTo(count);
            });
        }
    }

    static void assertLocalSelect(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<Object[][]> onRows)
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
        public static AtomicInteger diskMoveAttempts = new AtomicInteger(0);
        public static AtomicInteger failOnDiskMoveAttempt = new AtomicInteger(0);
        public static CountDownLatch waitForCrash = new CountDownLatch(1);
        public static AtomicBoolean crashed = new AtomicBoolean(false);
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
                State.waitForTopologyChange.countDown();
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

        public static class FailNthDiskMove
        {
            public static IInstanceInitializer install(int... nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(SSTableReader.class)
                                           .method(named("moveAndOpenSSTable").and(takesArguments(5)))
                                           .intercept(MethodDelegation.to(FailNthDiskMove.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static SSTableReader moveAndOpenSSTable(ColumnFamilyStore cfs, Descriptor oldDescriptor, Descriptor newDescriptor, Set<Component> components, boolean copyData, @SuperCall Callable<SSTableReader> r) throws Exception
            {
                if (State.diskMoveAttempts.incrementAndGet() == State.failOnDiskMoveAttempt.get())
                    throw new RuntimeException("Failing move and open SSTable");
                return r.call();
            }
        }

        public static class AwaitNthDiskMove
        {
            public static IInstanceInitializer install(int... nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(SSTableReader.class)
                                           .method(named("moveAndOpenSSTable").and(takesArguments(5)))
                                           .intercept(MethodDelegation.to(AwaitNthDiskMove.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static SSTableReader moveAndOpenSSTable(ColumnFamilyStore cfs, Descriptor oldDescriptor, Descriptor newDescriptor, Set<Component> components, boolean copyData, @SuperCall Callable<SSTableReader> r) throws Exception
            {
                if (State.crashed.get())
                    return r.call();
                State.waitForCrash.countDown();
                throw new RuntimeException("Failing move and open SSTable");
            }
        }

        public static class ReplayJournalWithNullMinSegment
        {
            public static IInstanceInitializer install(int... nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(AccordService.class)
                                           .method(named("replayJournal").and(takesArguments(1)))
                                           .intercept(MethodDelegation.to(ReplayJournalWithNullMinSegment.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static boolean replayJournal(Long2LongHashMap minSegments, @This AccordService self, @SuperMethod Method method) throws Exception
            {
                return (boolean) method.invoke(self, new Long2LongHashMap(0L));
            }
        }
    }
}
