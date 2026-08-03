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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.concurrent.Future;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.db.SystemKeyspace.BootstrapState.COMPLETED;
import static org.apache.cassandra.db.SystemKeyspace.BootstrapState.DECOMMISSIONED;
import static org.apache.cassandra.db.SystemKeyspace.TRANSFERRED_RANGES_V2;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.apache.cassandra.service.StorageService.Mode.DECOMMISSION_FAILED;
import static org.apache.cassandra.service.StorageService.Mode.NORMAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DecommissionTest extends TestBaseImpl
{
    @Test
    public void testDecommission() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .withInstanceInitializer(DecommissionTest.BB::install)
                                           .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            instance.runOnInstance(() -> {

                assertEquals(COMPLETED.name(), StorageService.instance.getBootstrapState());

                // pretend that decommissioning has failed in the middle

                try
                {
                    StorageService.instance.decommission(true);
                    fail("the first attempt to decommission should fail");
                }
                catch (Throwable t)
                {
                    assertEquals("simulated error in prepareUnbootstrapStreaming", t.getMessage());
                }

                assertFalse(StorageService.instance.isDecommissioning());
                assertTrue(StorageService.instance.isDecommissionFailed());

                // still COMPLETED, nothing has changed
                assertEquals(COMPLETED.name(), StorageService.instance.getBootstrapState());

                String operationMode = StorageService.instance.getOperationMode();
                assertEquals(DECOMMISSION_FAILED.name(), operationMode);

                // try to decommission again, now successfully

                try
                {
                    StorageService.instance.decommission(true);

                    // decommission was successful, so we reset failed decommission mode
                    assertFalse(StorageService.instance.isDecommissionFailed());

                    assertEquals(DECOMMISSIONED.name(), StorageService.instance.getBootstrapState());
                    assertFalse(StorageService.instance.isDecommissioning());
                }
                catch (Throwable t)
                {
                    fail("the second decommission attempt should pass but it failed on: " + t.getMessage());
                }

                // check that decommissioning of already decommissioned node has no effect

                try
                {
                    assertEquals(DECOMMISSIONED.name(), StorageService.instance.getBootstrapState());
                    assertFalse(StorageService.instance.isDecommissionFailed());

                    StorageService.instance.decommission(true);

                    assertEquals(DECOMMISSIONED.name(), StorageService.instance.getBootstrapState());
                    assertFalse(StorageService.instance.isDecommissionFailed());
                    assertFalse(StorageService.instance.isDecommissioning());
                }
                catch (Throwable t)
                {
                    fail("Decommissioning already decommissioned node should be no-op operation.");
                }
            });
        }
    }

    @Test
    public void testDecommissionAfterNodeRestart() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .withInstanceInitializer((classLoader, threadGroup, num, generation) -> {
                                               // we do not want to install BB after restart of a node which
                                               // failed to decommission which is the second generation, here
                                               // as "1" as it is counted from 0.
                                               if (num == 1 && generation != 1)
                                                   BB.install(classLoader, num);
                                           })
                                           .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            instance.runOnInstance(() -> {
                assertEquals(COMPLETED.name(), StorageService.instance.getBootstrapState());

                // pretend that decommissioning has failed in the middle

                try
                {
                    StorageService.instance.decommission(true);
                    fail("the first attempt to decommission should fail");
                }
                catch (Throwable t)
                {
                    assertEquals("simulated error in prepareUnbootstrapStreaming", t.getMessage());
                }

                // node is in DECOMMISSION_FAILED mode
                String operationMode = StorageService.instance.getOperationMode();
                assertEquals(DECOMMISSION_FAILED.name(), operationMode);
            });

            // restart the node which we failed to decommission
            stopUnchecked(instance);
            instance.startup();

            // it is back to normal so let's decommission again

            String oprationMode = instance.callOnInstance(() -> StorageService.instance.getOperationMode());
            assertEquals(NORMAL.name(), oprationMode);

            instance.runOnInstance(() -> {
                try
                {
                    StorageService.instance.decommission(true);
                }
                catch (InterruptedException e)
                {
                    fail("Should decommission the node");
                }

                assertEquals(DECOMMISSIONED.name(), StorageService.instance.getBootstrapState());
                assertFalse(StorageService.instance.isDecommissionFailed());
                assertFalse(StorageService.instance.isDecommissioning());
            });
        }
    }


    @Test
    public void testAbortingDecommissionRestreams() throws Exception
    {
        // https://issues.apache.org/jira/browse/CASSANDRA-16290
        // We demonstrate here that decommissioning and then aborting decommission is unsafe
        // if we've persisted transferred ranges and then skip them for something which was delivered after we aborted the decommission but before we resumed
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    // disable hints to simplify test
                                                                    .set("hinted_handoff_enabled", false)
                                        )
                                        // only install on the first generation of node2: a restarted instance gets a
                                        // fresh classloader, so any static "already failed once" state would be reset
                                        // and the resumed decommission would fail again
                                        .withInstanceInitializer((cl, threadGroup, num, generation) -> {
                                            if (num == 2 && generation == 0)
                                                BB.streamHintsInstall(cl);
                                        })
                                        .start())
        {
            // We need blob columns here so later we can do Murmur3Partitioner.LongToken.keyForToken(token);
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM, "pk blob, ck blob, v blob");

            IInvokableInstance leavingNode = cluster.get(2);

            leavingNode.nodetoolResult("decommission").asserts().failure();
            leavingNode.runOnInstance(() -> assertEquals(DECOMMISSION_FAILED, StorageService.Mode.valueOf(StorageService.instance.getOperationMode())));

            // abort the decommission
            ClusterUtils.stopUnchecked(leavingNode);
            ClusterUtils.start(leavingNode, props -> {});
            ClusterUtils.awaitRingHealthy(leavingNode);

            // Stop the non leaving nodes so we can write at ONE and fail to stream that datum
            ClusterUtils.stopUnchecked(cluster.get(1));
            ClusterUtils.stopUnchecked(cluster.get(3));
            ClusterUtils.stopUnchecked(cluster.get(4));

            List<Murmur3Partitioner.LongToken> tokens = ClusterUtils.getLocalTokens(leavingNode).stream().map(t -> new Murmur3Partitioner.LongToken(Long.parseLong(t))).collect(Collectors.toList());
            for (Murmur3Partitioner.LongToken token : tokens)
            {
                ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);
                leavingNode.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ONE, key, key, key);
            }

            ClusterUtils.start(cluster.get(1), props -> {});
            ClusterUtils.start(cluster.get(3), props -> {});
            ClusterUtils.start(cluster.get(4), props -> {});

            ClusterUtils.awaitRingHealthy(leavingNode);

            Object[][] ranges = leavingNode.executeInternal("SELECT keyspace_name from system." + TRANSFERRED_RANGES_V2);

            assertTrue("transferred ranges missing entirely", ranges.length > 0);
            assertTrue("transferred ranges present for keyspace", Arrays.stream(ranges).anyMatch(x -> x[0].equals(KEYSPACE)));

            // Resume decomm
            leavingNode.nodetoolResult("decommission").asserts().success();

            // Try and read data we wrote at ONE at ALL
            for (Murmur3Partitioner.LongToken token : tokens)
            {
                ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);
                Object[][] resp = cluster.get(1).coordinator().execute("SELECT pk from " + KEYSPACE + ".tbl where pk=?", ConsistencyLevel.ALL, key);
                assertTrue("We should get a response for this key we wrote it at ONE", resp.length > 0);
                assertEquals(key, resp[0][0]);
            }
        }
    }

    public static class BB
    {
        private static int invocations = 0;

        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("prepareUnbootstrapStreaming"))
                           .intercept(MethodDelegation.to(DecommissionTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        static void streamHintsInstall(ClassLoader cl)
        {
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("streamHints"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }


        @SuppressWarnings("unused")
        public static Supplier<Future<StreamState>> prepareUnbootstrapStreaming(@SuperCall Callable<Supplier<Future<StreamState>>> zuper)
        {
            ++invocations;

            if (invocations == 1)
                throw new RuntimeException("simulated error in prepareUnbootstrapStreaming");

            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> streamHints(@SuperCall Callable<Future<?>> zuper)
        {
            // this is only installed on the first startup of the leaving node, so every invocation
            // here belongs to the decommission attempt we want to fail at the last possible moment
            return ImmediateFuture.failure(new IOException("failing hints so that decomm fails at last moment possible"));
        }
    }
}
