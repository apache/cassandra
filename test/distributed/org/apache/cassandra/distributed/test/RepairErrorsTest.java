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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraIncomingFile;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class RepairErrorsTest extends TestBaseImpl
{
    @Test
    public void testRemoteValidationFailure() throws IOException
    {
        Cluster.Builder builder = Cluster.build(2)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .withInstanceInitializer(ByteBuddyHelper::install);
        try (Cluster cluster = builder.createWithoutStarting())
        {
            cluster.setUncaughtExceptionsFilter((i, throwable) -> {
                if (i == 2)
                    return throwable.getMessage() != null && throwable.getMessage().contains("IGNORE");
                return false;
            });

            cluster.startup();
            init(cluster);

            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, x int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));
            long mark = cluster.get(1).logs().mark();
            cluster.forEach(i -> i.nodetoolResult("repair", "--full").asserts().failure());
            Assertions.assertThat(cluster.get(1).logs().grep(mark, "^ERROR").getResult()).isEmpty();
        }
    }

    @Test
    public void testRemoteSyncFailure() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK)
                                                                       .set("disk_failure_policy", "stop")
                                                                       .set("disk_access_mode", "mmap_index_only"))
                                           .withInstanceInitializer(ByteBuddyHelper::installStreamPlanExecutionFailure).start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, x int)");
            
            // On repair, this data layout will require two (local) syncs from node 1 and one remote sync from node 2:
            cluster.get(1).executeInternal("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", 1, 1);
            cluster.get(2).executeInternal("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", 2, 2);
            cluster.get(3).executeInternal("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", 3, 3);
            cluster.forEach(i -> i.flush(KEYSPACE));
            
            // Flush system.peers_v2, or there won't be any SSTables...
            cluster.forEach(i -> i.flush("system"));
            
            // The remote sync started from node 2 will fail on plan execution and propagate the error...
            NodeToolResult result = cluster.get(1).nodetoolResult("repair", KEYSPACE);
            result.asserts().failure().errorContains("Sync failed between /127.0.0.2:7012 and /127.0.0.3:7012");

            // Before CASSANDRA-17466 added an abort mechanism for local sync tasks and switched the repair task
            // executor to shut down without interrupting its threads, we could trigger the disk failure policy, as
            // interruption could accidentally close shared channels in the middle of a blocking operation. To see
            // this, simply revert those changes in RepairJob (aborting sync tasks) and RepairSession (shutdown()
            // rather than shutdownNow() on failure).
            assertTrue(cluster.get(1).logs().grep("Stopping transports as disk_failure_policy is stop").getResult().isEmpty());
            assertTrue(cluster.get(1).logs().grep("FSReadError").getResult().isEmpty());
            assertTrue(cluster.get(1).logs().grep("ClosedByInterruptException").getResult().isEmpty());

            // Make sync unnecessary, and repair should succeed:
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, 1, 1);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, 2, 2);
            cluster.coordinator(1).execute("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, 3, 3);
            cluster.forEach(i -> i.flush(KEYSPACE));
            result = cluster.get(1).nodetoolResult("repair", KEYSPACE);
            result.asserts().success();

            assertNoActiveRepairSessions(cluster.get(1));

            cluster.forEach(i -> Assertions.assertThat(i.logs().grep("SomeRepairFailedException").getResult())
                                           .describedAs("node%d logged hidden exception org.apache.cassandra.repair.SomeRepairFailedException", i.config().num())
                                           .isEmpty());
        }
    }

    @Test
    public void testRemoteStreamFailure() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK)
                                                                       .set("disk_failure_policy", "stop")
                                                                       .set("disk_access_mode", "mmap_index_only"))
                                           .withInstanceInitializer(ByteBuddyHelperStreamFailure::installStreamHandlingFailure).start()))
        {
            // Make sure we don't auto-compact the peers table. We'll need to try it manually later.
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("peers_v2");
                cfs.disableAutoCompaction();
            });

            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, x int)");

            // On repair, this data layout will require two (local) syncs from node 1 and one remote sync from node 2:
            cluster.get(1).executeInternal("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", 1, 1);
            cluster.get(2).executeInternal("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", 2, 2);
            cluster.get(3).executeInternal("insert into " + KEYSPACE + ".tbl (id, x) VALUES (?,?)", 3, 3);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // Flush system.peers_v2, or there won't be any SSTables...
            cluster.forEach(i -> i.flush("system"));

            // Stream reading will fail on node 3, and this will interrupt node 1 just as it starts to stream to node 2.
            NodeToolResult result = cluster.get(1).nodetoolResult("repair", KEYSPACE);
            result.asserts().failure();

            // Ensure that the peers table is compactable even after the file streaming task is interrupted.
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("peers_v2");
                cfs.forceMajorCompaction();
            });

            assertTrue(cluster.get(1).logs().grep("Stopping transports as disk_failure_policy is stop").getResult().isEmpty());
            assertTrue(cluster.get(1).logs().grep("FSReadError").getResult().isEmpty());

            assertNoActiveRepairSessions(cluster.get(1));
        }
    }

    @Test
    public void testNoSuchRepairSessionAnticompaction() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .withInstanceInitializer(ByteBuddyHelper::installACNoSuchRepairSession)
                                           .start()))
        {
            cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, x int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("insert into "+KEYSPACE+".tbl (id, x) VALUES (?,?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach(i -> i.flush(KEYSPACE));
            long mark = cluster.get(1).logs().mark();
            cluster.forEach(i -> i.nodetoolResult("repair", KEYSPACE).asserts().failure());
            assertTrue(cluster.get(1).logs().grep(mark, "^ERROR").getResult().isEmpty());
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    private void assertNoActiveRepairSessions(IInvokableInstance instance)
    {
        // Make sure we've cleaned up local sessions:
        Integer sessions = instance.callOnInstance(() -> ActiveRepairService.instance().sessionCount());
        assertEquals(0, sessions.intValue());
    }

    public static class ByteBuddyHelper
    {
        public static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(CompactionIterator.class)
                               .method(named("next"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void installACNoSuchRepairSession(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(CompactionManager.class)
                               .method(named("validateSSTableBoundsForAnticompaction"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }
        
        public static void installStreamPlanExecutionFailure(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().rebase(StreamSession.class)
                               .method(named("onInitializationComplete"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }

            if (nodeNumber == 1)
            {
                new ByteBuddy().rebase(SystemKeyspace.class)
                        .method(named("getPreferredIP"))
                        .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                        .make()
                        .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static UnfilteredRowIterator next()
        {
            throw new RuntimeException("IGNORE");
        }

        @SuppressWarnings("unused")
        public static void validateSSTableBoundsForAnticompaction(TimeUUID sessionID,
                                                                  Collection<SSTableReader> sstables,
                                                                  RangesAtEndpoint ranges)
        {
            throw new CompactionInterruptedException(String.valueOf(sessionID));
        }

        @SuppressWarnings("unused")
        public static void onInitializationComplete()
        {
            throw new RuntimeException("Failing stream session initialization from test!");
        }

        @SuppressWarnings("unused")
        public static InetAddressAndPort getPreferredIP(InetAddressAndPort ep, @SuperCall Callable<InetAddressAndPort> zuper) throws Exception
        {
            if (Thread.currentThread().getName().contains("RepairJobTask"))
            {
                try
                {
                    TimeUnit.SECONDS.sleep(10);
                }
                catch (InterruptedException e)
                {
                    // Leave the interrupt flag intact for the ChannelProxy downstream...
                    Thread.currentThread().interrupt();
                }
            }

            return zuper.call();
        }
    }

    public static class ByteBuddyHelperStreamFailure
    {
        public static void installStreamHandlingFailure(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 3)
            {
                new ByteBuddy().rebase(CassandraIncomingFile.class)
                               .method(named("read"))
                               .intercept(MethodDelegation.to(ByteBuddyHelperStreamFailure.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }

            if (nodeNumber == 1)
            {
                new ByteBuddy().rebase(SystemKeyspace.class)
                               .method(named("getPreferredIP"))
                               .intercept(MethodDelegation.to(ByteBuddyHelperStreamFailure.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void read(DataInputPlus in, int version) throws IOException
        {
            throw new IOException("Failing incoming file read from test!");
        }

        @SuppressWarnings("unused")
        public static InetAddressAndPort getPreferredIP(InetAddressAndPort ep, @SuperCall Callable<InetAddressAndPort> zuper) throws Exception
        {
            if (Thread.currentThread().getName().contains("NettyStreaming-Outbound") && ep.getAddress().toString().contains("127.0.0.2"))
            {
                try
                {
                    TimeUnit.SECONDS.sleep(10);
                }
                catch (InterruptedException e)
                {
                    // Leave the interrupt flag intact for the ChannelProxy downstream...
                    Thread.currentThread().interrupt();
                }
            }

            return zuper.call();
        }
    }
}
