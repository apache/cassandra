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
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.streaming.StreamException;
import org.apache.cassandra.streaming.StreamResultFuture;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.distributed.shared.ClusterUtils.startHostReplacement;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.setupCluster;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.getMetricGaugeValue;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.getMetricMeterRate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FailedBootstrapTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(FailedBootstrapTest.class);

    private static final int NODE_TO_REMOVE = 2;

    @Test
    public void test() throws IOException, InterruptedException, TimeoutException
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
            IInstanceConfig toReplaceConf = nodeToRemove.config();
            IInvokableInstance added = addInstance(cluster, toReplaceConf, c -> c.set("auto_bootstrap", true));
            startHostReplacement(nodeToRemove, added, (a_, b_) -> {});
            added.logs().watchFor("Node is not yet bootstrapped completely");
            alive.forEach(i -> {
                NodeToolResult result = i.nodetoolResult("gossipinfo");
                result.asserts().success();
                logger.info("gossipinfo for node{}\n{}", i.config().num(), result.getStdout());
            });

            assertTrue(getMetricGaugeValue(added, "BootstrapFilesTotal", Long.class) > 0L);
            assertTrue(getMetricGaugeValue(added, "BootstrapFilesReceived", Long.class) > 0L);
            assertEquals("Beginning bootstrap process", getMetricGaugeValue(added, "BootstrapLastSeenStatus", String.class));
            assertEquals("Stream failed", getMetricGaugeValue(added, "BootstrapLastSeenError", String.class));
            assertTrue(getMetricMeterRate(added, "BootstrapFilesThroughput") > 0);
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
        }

        public static void maybeComplete(@This StreamResultFuture future) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
        {
            Method method = future.getClass().getSuperclass().getSuperclass().getDeclaredMethod("tryFailure", Throwable.class);
            method.setAccessible(true);
            method.invoke(future, new StreamException(future.getCurrentState(), "Stream failed"));
        }
    }
}
