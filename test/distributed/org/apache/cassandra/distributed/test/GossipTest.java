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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.toCassandraInetAddressAndPort;
import static org.apache.cassandra.distributed.shared.ClusterUtils.runAndWaitForLogs;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GossipTest extends TestBaseImpl
{
    @Test
    public void nodeDownDuringMove() throws Throwable
    {
        int liveCount = 1;
        try (Cluster cluster = Cluster.build(2 + liveCount)
                                      .withConfig(config -> config.with(NETWORK).with(GOSSIP))
                                      .createWithoutStarting())
        {
            int fail = liveCount + 1;
            int late = fail + 1;
            for (int i = 1 ; i <= liveCount ; ++i)
                cluster.get(i).startup();
            cluster.get(fail).startup();
            Collection<String> expectTokens =
                cluster.get(fail)
                       .callsOnInstance(() -> StorageService.instance.getTokenMetadata()
                                                                     .getTokens(FBUtilities.getBroadcastAddressAndPort())
                                                                     .stream()
                                                                     .map(Object::toString)
                                                                     .collect(Collectors.toList()))
                       .call();

            InetSocketAddress failAddress = cluster.get(fail).broadcastAddress();
            // wait for NORMAL state
            for (int i = 1 ; i <= liveCount ; ++i)
            {
                cluster.get(i).acceptsOnInstance((InetSocketAddress address) -> {
                    EndpointState ep;
                    InetAddressAndPort endpoint = toCassandraInetAddressAndPort(address);
                    while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                           || ep.getApplicationState(ApplicationState.STATUS_WITH_PORT) == null
                           || !ep.getApplicationState(ApplicationState.STATUS_WITH_PORT).value.startsWith("NORMAL"))
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                }).accept(failAddress);
            }

            // set ourselves to MOVING, and wait for it to propagate
            cluster.get(fail).runOnInstance(() -> {
                Token token = Iterables.getFirst(StorageService.instance.getTokenMetadata().getTokens(FBUtilities.getBroadcastAddressAndPort()), null);
                Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS_WITH_PORT, StorageService.instance.valueFactory.moving(token));
            });
            for (int i = 1 ; i <= liveCount ; ++i)
            {
                cluster.get(i).acceptsOnInstance((InetSocketAddress address) -> {
                    EndpointState ep;
                    InetAddressAndPort endpoint = toCassandraInetAddressAndPort(address);
                    while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                           || (ep.getApplicationState(ApplicationState.STATUS_WITH_PORT) == null
                           || !ep.getApplicationState(ApplicationState.STATUS_WITH_PORT).value.startsWith("MOVING")))
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
                }).accept(failAddress);
            }

            ClusterUtils.stopAbrupt(cluster, cluster.get(fail));
            cluster.get(late).startup();
            cluster.get(late).acceptsOnInstance((InetSocketAddress address) -> {
                EndpointState ep;
                InetAddressAndPort endpoint = toCassandraInetAddressAndPort(address);
                while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                       || !ep.getApplicationState(ApplicationState.STATUS_WITH_PORT).value.startsWith("MOVING"))
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
            }).accept(failAddress);

            Collection<String> tokens =
                cluster.get(late)
                       .appliesOnInstance((InetSocketAddress address) ->
                                          StorageService.instance.getTokenMetadata()
                                                                 .getTokens(toCassandraInetAddressAndPort(address))
                                                                 .stream()
                                                                 .map(Object::toString)
                                                                 .collect(Collectors.toList()))
                       .apply(failAddress);

            Assert.assertEquals(expectTokens, tokens);
        }
    }

    public static class BBBootstrapInterceptor
    {
        final static CountDownLatch bootstrapReady = new CountDownLatch(1);
        final static CountDownLatch bootstrapStart = new CountDownLatch(1);
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 2)
                return;
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("bootstrap").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BBBootstrapInterceptor.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean bootstrap(Collection<Token> tokens, long bootstrapTimeoutMillis)
        {
            bootstrapStart.countDown();
            Uninterruptibles.awaitUninterruptibly(bootstrapReady);
            return false; // bootstrap fails
        }
    }

    @Test
    public void testPreventStoppingGossipDuringBootstrap() throws Exception
    {
        ExecutorService es = Executors.newFixedThreadPool(1);
        try (Cluster cluster = builder().withNodes(2)
                                        .withConfig(config -> config.with(GOSSIP)
                                                                    .with(NETWORK)
                                                                    .set("auto_bootstrap", true))
                                        .withInstanceInitializer(BBBootstrapInterceptor::install)
                                        .createWithoutStarting();
             Closeable ignored = es::shutdown)
        {
            Runnable test = () ->
            {
                try
                {
                    cluster.get(2).runOnInstance(() -> {
                        Uninterruptibles.awaitUninterruptibly(BBBootstrapInterceptor.bootstrapStart);
                        BBBootstrapInterceptor.bootstrapReady.countDown();
                        try
                        {
                            StorageService.instance.stopGossiping();

                            Assert.fail("stopGossiping did not fail!");
                        }
                        catch (Exception ex)
                        {
                            Assert.assertSame(ex.getClass(), IllegalStateException.class);
                            Assert.assertEquals(ex.getMessage(), "Unable to stop gossip because the node is not in the normal state. Try to stop the node instead.");
                        }
                    });
                }
                finally
                {
                    // shut down the node2 to speed up cluster startup.
                    cluster.get(2).shutdown();
                }
            };
            Future<?> testResult = es.submit(test);
            try
            {
                cluster.startup();
            }
            catch (Exception ex) {
                // ignore exceptions from startup process. More interested in the test result.
            }
            testResult.get();
        }

        es.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPreventEnablingGossipDuringMove() throws Exception
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withConfig(config -> config.with(GOSSIP)
                                                                    .with(NETWORK))
                                        .start())
        {
            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.stopGossiping();
                StorageService.instance.setMovingModeUnsafe();
                try
                {
                    StorageService.instance.startGossiping();

                    Assert.fail("startGossiping did not fail!");
                }
                catch (Exception ex)
                {
                    Assert.assertSame(ex.getClass(), IllegalStateException.class);
                    Assert.assertEquals(ex.getMessage(), "Unable to start gossip because the node is not in the normal state.");
                }
            });
        }
    }

    @Test
    public void gossipShutdownUpdatesTokenMetadata() throws Exception
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withInstanceInitializer(FailureHelper::installMoveFailure)
                                      .withoutVNodes()
                                      .start())
        {
            init(cluster, 2);
            populate(cluster);
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            // initiate a move for node2, which will not complete due to the
            // ByteBuddy interceptor we injected. Wait for the other two nodes
            // to mark node2 as moving before proceeding.
            long t2 = Long.parseLong(getLocalToken(node2));
            long t3 = Long.parseLong(getLocalToken(node3));
            long moveTo = t2 + ((t3 - t2)/2);
            String logMsg = "Node " + node2.broadcastAddress() + " state moving, new token " + moveTo;
            runAndWaitForLogs(() -> node2.nodetoolResult("move", "--", Long.toString(moveTo)).asserts().failure(),
                              logMsg,
                              cluster);

            InetSocketAddress movingAddress = node2.broadcastAddress();
            // node1 & node3 should now consider some ranges pending for node2
            assertPendingRangesForPeer(true, movingAddress, cluster);

            // A controlled shutdown causes peers to replace the MOVING status to be with SHUTDOWN, but prior to
            // CASSANDRA-16796 this doesn't update TokenMetadata, so they maintain pending ranges for the down node
            // indefinitely, even after it has been removed from the ring.
            logMsg = "Marked " + node2.broadcastAddress() + " as shutdown";
            runAndWaitForLogs(() -> Futures.getUnchecked(node2.shutdown()),
                              logMsg,
                              node1, node3);
            // node1 & node3 should not consider any ranges as still pending for node2
            assertPendingRangesForPeer(false, movingAddress, cluster);
        }
    }

    @Test
    public void restartGossipOnGossippingOnlyMember() throws Throwable
    {
        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance gossippingOnlyMember = cluster.bootstrap(config);
            withProperty(JOIN_RING, false, () -> gossippingOnlyMember.startup(cluster));

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isGossipRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.stopGossiping());

            assertFalse(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                            () -> StorageService.instance.isGossipRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.startGossiping());

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isGossipRunning()));
        }
    }

    private static String getLocalToken(IInvokableInstance node)
    {
        Collection<String> tokens = ClusterUtils.getLocalTokens(node);
        Assertions.assertThat(tokens).hasSize(1);
        return tokens.stream().findFirst().get();
    }

    void assertPendingRangesForPeer(final boolean expectPending, final InetSocketAddress movingAddress, final Cluster cluster)
    {
        for (IInvokableInstance inst : new IInvokableInstance[]{ cluster.get(1), cluster.get(3)})
        {
            boolean hasPending = inst.appliesOnInstance((InetSocketAddress address) -> {
                InetAddressAndPort peer = toCassandraInetAddressAndPort(address);

                PendingRangeCalculatorService.instance.blockUntilFinished();

                boolean isMoving = StorageService.instance.getTokenMetadata()
                                                          .getMovingEndpoints()
                                                          .stream()
                                                          .map(pair -> pair.right)
                                                          .anyMatch(peer::equals);

                return isMoving && !StorageService.instance.getTokenMetadata()
                                                           .getPendingRanges(KEYSPACE, peer)
                                                           .isEmpty();
            }).apply(movingAddress);
            assertEquals(String.format("%s should %shave PENDING RANGES for %s",
                                       inst.broadcastAddress().getHostString(),
                                       expectPending ? "" : "not ",
                                       movingAddress),
                         hasPending, expectPending);
        }
    }

    static void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)",
                                           ConsistencyLevel.ALL,
                                           i);
        }
    }

    public static class FailureHelper
    {
        static void installMoveFailure(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(StreamPlan.class)
                               .method(named("execute"))
                               .intercept(MethodDelegation.to(FailureHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static StreamResultFuture execute()
        {
            throw new RuntimeException("failing to execute move");
        }
    }
}
