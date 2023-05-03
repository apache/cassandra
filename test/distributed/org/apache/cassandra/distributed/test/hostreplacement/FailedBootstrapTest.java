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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.streaming.StreamException;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.setupCluster;

public class FailedBootstrapTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(FailedBootstrapTest.class);

    private static final int NODE_TO_REMOVE = 2;

    @Test
    public void roleSetupDoesNotProduceUnavailables() throws IOException
    {
        Cluster.Builder builder = Cluster.build(3)
                                         .withConfig(c -> c.with(Feature.values()))
                                         .withInstanceInitializer(BB::install);
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3, builder.getTokenCount());
        builder = builder.withTokenSupplier((TokenSupplier) node -> even.tokens(node == 4 ? NODE_TO_REMOVE : node));
        try (Cluster cluster = builder.start())
        {
            List<IInvokableInstance> alive = Arrays.asList(cluster.get(1), cluster.get(3));
            IInvokableInstance nodeToRemove = cluster.get(NODE_TO_REMOVE);

            setupCluster(cluster);

            stopUnchecked(nodeToRemove);

            // should fail to join, but should start up!
            IInvokableInstance added = replaceHostAndStart(cluster, nodeToRemove, p -> p.set(SUPERUSER_SETUP_DELAY_MS, "1"));
            // log gossip for debugging
            alive.forEach(i -> {
                NodeToolResult result = i.nodetoolResult("gossipinfo");
                result.asserts().success();
                logger.info("gossipinfo for node{}\n{}", i.config().num(), result.getStdout());
            });

            // CassandraRoleManager attempted to do distributed reads while bootstrap was still going (it failed, so still in bootstrap mode)
            // so need to validate that is no longer happening and we incrementing org.apache.cassandra.metrics.ClientRequestMetrics.unavailables
            // sleep larger than multiple retry attempts...
            Awaitility.await()
                      .atMost(1, TimeUnit.MINUTES)
                      .until(() -> added.callOnInstance(() -> BB.SETUP_SCHEDULE_COUNTER.get()) >= 42); // why 42?  just need something large enough to make sure multiple attempts happened

            // do we have any read metrics have unavailables?
            added.runOnInstance(() -> {
                Assertions.assertThat(ClientRequestsMetricsHolder.readMetrics.unavailables.getCount()).describedAs("read unavailables").isEqualTo(0);
                Assertions.assertThat(ClientRequestsMetricsHolder.casReadMetrics.unavailables.getCount()).describedAs("CAS read unavailables").isEqualTo(0);
            });
        }
    }

    public static class BB
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != 4)
                return;

            new ByteBuddy().rebase(StreamResultFuture.class)
                           .method(named("maybeComplete"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(CassandraRoleManager.class)
                           .method(named("scheduleSetupTask"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void maybeComplete(@This StreamResultFuture future) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
        {
            Method method = future.getClass().getSuperclass().getSuperclass().getDeclaredMethod("tryFailure", Throwable.class);
            method.setAccessible(true);
            method.invoke(future, new StreamException(future.getCurrentState(), "Stream failed"));
        }

        private static final AtomicInteger SETUP_SCHEDULE_COUNTER = new AtomicInteger(0);
        public static void scheduleSetupTask(final Callable<?> setupTask, @SuperCall Runnable fn)
        {
            SETUP_SCHEDULE_COUNTER.incrementAndGet();
            fn.run();
        }
    }
}
