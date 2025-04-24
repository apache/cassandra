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
import java.util.concurrent.ForkJoinPool;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/**
 * When bootstrap hangs it can hang forever, but this can be a problem in CI as the test reports as "timeout" and all logs and history is lost.  This test makes sure that JVM-DTest instances do shutdown properly even in this case
 */
public class HungBootstrapDoesNotHangTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = Cluster.build(1)
                                      .withTokenSupplier(tokenSupplier)
                                      .withConfig(c -> c.set("auto_bootstrap", true).with(Feature.values()))
                                      .withInstanceInitializer(BBHelper::install)
                                      .createWithoutStarting())
        {
            cluster.get(1).startup(cluster); // should work fine
            IInvokableInstance node2 = ClusterUtils.addInstance(cluster, c -> c.set(Constants.KEY_DTEST_STARTUP_TIMEOUT, "1m")
                                                                               .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false));
            ForkJoinPool.commonPool().execute(() -> {
                node2.startup(); // should hang and never reach the next line
                State.notBlocked();
            });
            State.awaitBlocked();

            Assertions.assertThat(State.wasBlocked()).describedAs("node2 was supposed to get blocked by ByteBuddy but didnt").isEqualTo(true);

            // node1 is up, node2 is blocked in bootstrap... now let the cluster close
        }
    }

    @Shared
    public static class State
    {
        private static final CountDownLatch blocked = CountDownLatch.newCountDownLatch(1);
        private static volatile boolean wasBlocked = true;

        public static void blocked()
        {
            blocked.decrement();
        }

        public static void notBlocked()
        {
            wasBlocked = false;
            blocked();
        }

        public static void awaitBlocked()
        {
            blocked.awaitThrowUncheckedOnInterrupt();
        }

        public static boolean wasBlocked()
        {
            return wasBlocked;
        }
    }

    public static class BBHelper
    {
        public static void install(ClassLoader cl, int id)
        {
            if (id != 2) return;
            new ByteBuddy().rebase(BootstrapAndJoin.class)
                           .method(named("bootstrap").and(takesArguments(6)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static boolean bootstrap(final Collection<Token> tokens,
                                        long bootstrapTimeoutMillis,
                                        ClusterMetadata metadata,
                                        InetAddressAndPort beingReplaced,
                                        MovementMap movements,
                                        MovementMap strictMovements)
        {
            try
            {
                State.blocked();
                Thread.currentThread().join();
                return false;
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
        }

    }
}
