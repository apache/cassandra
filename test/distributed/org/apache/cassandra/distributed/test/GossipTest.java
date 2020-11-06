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
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.StorageService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class GossipTest extends TestBaseImpl
{
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

}
