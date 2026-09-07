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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.snapshot.ClearSnapshotTask;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.fail;

public class ClearSnapshotTest extends TestBaseImpl
{
    /**
     * This test has been authored entirely by Claude.
     *
     * Clearing the snapshots taken by a repair must not be done while holding the {@link ActiveRepairService} monitor.
     * That monitor is taken by every {@code synchronized} method of {@link ActiveRepairService}, including the
     * gossip-facing path ({@code FailureDetector} convict -&gt; {@code abort} -&gt; {@code removeParentRepairSession}),
     * so clearing snapshots inline - as {@code removeParentRepairSession} used to - stalls the gossip stage for as long
     * as the deletions take. See CASSANDRA-17168.
     * <p>
     * This replaces an earlier version of this test, which drove twenty concurrent repairs and waited for ten
     * simultaneous parent repair sessions before killing a node. That had become vacuous: the delay it injected was
     * into {@code Directories.snapshotExists}, deleted when snapshot management was consolidated into
     * {@code SnapshotManager}, and {@code repair -full} is {@code PARALLEL}, for which no snapshot is taken at all - so
     * the whole scenario reduced to a race on how many repairs happened to be in flight.
     */
    @Test
    public void clearSnapshotDoesNotHoldActiveRepairServiceLock() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        // two nodes, so that the keyspace has a neighbour to repair with - a single node has nothing to repair, and so
        // never takes (or clears) a snapshot
        try (Cluster cluster = init(Cluster.build(2)
                                          .withConfig(config -> config.with(GOSSIP)
                                                                      .with(NETWORK))
                                          .withInstanceInitializer(BB::install)
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, t int)"));
            cluster.get(1).executeInternal(withKeyspace("insert into %s.tbl (id, t) values (?, ?)"), 1, 1);
            cluster.forEach(node -> node.flush(KEYSPACE));

            // -seq, because only a non-PARALLEL repair snapshots its replicas, and therefore only a non-PARALLEL repair
            // has snapshots to clear when its parent session is removed
            cluster.get(1).nodetoolResult("repair", "-seq", "-full", KEYSPACE).asserts().success();

            // BB.clearSnapshot blocks the repair's clear until we release it, so from here until the finally below the
            // node is inside ClearSnapshotTask on behalf of ActiveRepairService
            cluster.get(1).runOnInstance(BB::awaitClearingRepairSnapshots);
            try
            {
                Future<?> monitor = cluster.get(1).asyncRunsOnInstance(() -> {
                    //noinspection EmptySynchronizedStatement,SynchronizationOnLocalVariableOrMethodParameter
                    synchronized (ActiveRepairService.instance()) { }
                }).call();

                try
                {
                    monitor.get(30, TimeUnit.SECONDS);
                }
                catch (TimeoutException e)
                {
                    fail("The ActiveRepairService monitor was held while clearing a repair's snapshots, which stalls " +
                         "the gossip stage - see CASSANDRA-17168");
                }
            }
            finally
            {
                cluster.get(1).runOnInstance(BB::releaseClearingRepairSnapshots);
            }

            // and the snapshot is still cleared, once we stop holding it up
            cluster.get(1).logs().watchFor("Cleared snapshots in");
        }
    }

    public static class BB
    {
        private static final CountDownLatch clearing = new CountDownLatch(1);
        private static final CountDownLatch release = new CountDownLatch(1);

        public static void install(ClassLoader classLoader, Integer num)
        {
            // node1 only: it is the repair coordinator and a replica, so it performs a clear of its own, and holding up
            // the other replica's clear would only delay its shutdown
            if (num != 1)
                return;

            new ByteBuddy().rebase(ClearSnapshotTask.class)
                           .method(named("call"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static Void call(@SuperCall Callable<Void> zuper) throws Exception
        {
            // only hold up the clear that a finished repair triggers; ephemeral/expired snapshot clearing runs on
            // startup and on a timer, and blocking those would simply prevent the node from starting.
            // NOTE: identified by the stack and not by the executing thread, because which thread runs it is precisely
            // what is under test
            if (isClearingRepairSnapshots())
            {
                clearing.countDown();
                Uninterruptibles.awaitUninterruptibly(release, 30, TimeUnit.SECONDS);
            }
            return zuper.call();
        }

        private static boolean isClearingRepairSnapshots()
        {
            for (StackTraceElement element : Thread.currentThread().getStackTrace())
            {
                if (element.getClassName().startsWith(ActiveRepairService.class.getName()))
                    return true;
            }
            return false;
        }

        public static void awaitClearingRepairSnapshots()
        {
            if (!Uninterruptibles.awaitUninterruptibly(clearing, 1, TimeUnit.MINUTES))
                throw new AssertionError("Repair did not clear its snapshots");
        }

        public static void releaseClearingRepairSnapshots()
        {
            release.countDown();
        }
    }

    @Test
    public void testSeqClearsSnapshot() throws IOException, TimeoutException
    {
        try(Cluster cluster = init(Cluster.build(3).withConfig(config ->
                                                               config.with(GOSSIP)
                                                                     .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int)"));
            for (int i = 0; i < 10; i++)
                cluster.get(1).executeInternal(withKeyspace("insert into %s.tbl (id, x) values (?, ?)"), i, i);
            cluster.get(1).nodetoolResult("repair", "-seq", "-full", KEYSPACE).asserts().success();
            cluster.get(1).logs().watchFor("Clearing snapshot");
        }
    }
}
