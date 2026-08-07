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

package org.apache.cassandra.distributed.test.log.mso;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.sequences.SequenceState;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class IPChangeWithMSOBase extends TestBaseImpl
{
    public void runTest(BiConsumer<ClassLoader, Integer> instanceInitializer,
                        Consumer<Cluster> beforeIPChange,
                        Consumer<Cluster> afterIPChange) throws Exception
    {
        int NUM_NODES = 6;
        TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(NUM_NODES);
        try (Cluster cluster = builder().withNodes(NUM_NODES)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withTokenSupplier((TokenSupplier)i -> i <= NUM_NODES ? ts.tokens(i) : ts.tokens(i - NUM_NODES))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(NUM_NODES * 2, "dc0", "rack0"))
                                        .withInstanceInitializer(instanceInitializer)
                                        .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            beforeIPChange.accept(cluster);

            // change all ips;
            for (int i = 1; i <= 6; i++)
            {
                cluster.get(i).shutdown();
                IInstanceConfig nodeConfig = cluster.newInstanceConfig();
                nodeConfig.set("data_file_directories", cluster.get(i).config().get("data_file_directories"));
                cluster.bootstrap(nodeConfig, AbstractCluster.CURRENT_VERSION).startup();
            }

            afterIPChange.accept(cluster);
        }
    }

    public static class BBHelper
    {
        public static AtomicBoolean enabled = new AtomicBoolean(true);
        public static AtomicInteger cnt = new AtomicInteger();
        // step 1 = after StartX, step 2 = after MidX
        private static final int failStep = new Random().nextInt(2) + 1;
        public static void install(int installOn, Class<?> c, ClassLoader cl, int i)
        {
            if (i != installOn)
                return;

            new ByteBuddy().rebase(c)
                           .method(named("executeNext"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static SequenceState executeNext(@SuperCall Callable<SequenceState> zuper) throws Exception
        {
            if (enabled.get())
            {
                if (cnt.incrementAndGet() > failStep)
                    throw new RuntimeException("EXPECTED");
            }
            return zuper.call();
        }
    }
}
