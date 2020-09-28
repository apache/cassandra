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
import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class GossipTest extends TestBaseImpl
{

    @Test
    public void nodeDownDuringMove() throws Throwable
    {
        int liveCount = 1;
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default
        System.setProperty("cassandra.consistent.rangemovement", "false");
        System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
        try (Cluster cluster = Cluster.build(2 + liveCount)
                                      .withConfig(config -> config.with(NETWORK).with(GOSSIP))
                                      .createWithoutStarting())
        {
            int fail = liveCount + 1;
            int late = fail + 1;
            for (int i = 1 ; i <= liveCount ; ++i)
                cluster.get(i).startup();
            cluster.get(fail).startup();
            Collection<String> expectTokens = cluster.get(fail).callsOnInstance(() ->
                StorageService.instance.getTokenMetadata().getTokens(FBUtilities.getBroadcastAddress())
                                       .stream().map(Object::toString).collect(Collectors.toList())
            ).call();

            InetAddress failAddress = cluster.get(fail).broadcastAddress().getAddress();
            // wait for NORMAL state
            for (int i = 1 ; i <= liveCount ; ++i)
            {
                cluster.get(i).acceptsOnInstance((InetAddress endpoint) -> {
                    EndpointState ep;
                    while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                           || ep.getApplicationState(ApplicationState.STATUS) == null
                           || !ep.getApplicationState(ApplicationState.STATUS).value.startsWith("NORMAL"))
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                }).accept(failAddress);
            }

            // set ourselves to MOVING, and wait for it to propagate
            cluster.get(fail).runOnInstance(() -> {

                Token token = Iterables.getFirst(StorageService.instance.getTokenMetadata().getTokens(FBUtilities.getBroadcastAddress()), null);
                Gossiper.instance.addLocalApplicationState(ApplicationState.STATUS, StorageService.instance.valueFactory.moving(token));
            });

            for (int i = 1 ; i <= liveCount ; ++i)
            {
                cluster.get(i).acceptsOnInstance((InetAddress endpoint) -> {
                    EndpointState ep;
                    while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                           || (ep.getApplicationState(ApplicationState.STATUS) == null
                               || !ep.getApplicationState(ApplicationState.STATUS).value.startsWith("MOVING")))
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
                }).accept(failAddress);
            }

            cluster.get(fail).shutdown(false).get();
            cluster.get(late).startup();
            cluster.get(late).acceptsOnInstance((InetAddress endpoint) -> {
                EndpointState ep;
                while (null == (ep = Gossiper.instance.getEndpointStateForEndpoint(endpoint))
                       || !ep.getApplicationState(ApplicationState.STATUS).value.startsWith("MOVING"))
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            }).accept(failAddress);

            Collection<String> tokens = cluster.get(late).appliesOnInstance((InetAddress endpoint) ->
                StorageService.instance.getTokenMetadata().getTokens(failAddress)
                                       .stream().map(Object::toString).collect(Collectors.toList())
            ).apply(failAddress);

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
                           .method(named("bootstrap").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBBootstrapInterceptor.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean bootstrap(Collection<Token> tokens) throws Exception
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

}
