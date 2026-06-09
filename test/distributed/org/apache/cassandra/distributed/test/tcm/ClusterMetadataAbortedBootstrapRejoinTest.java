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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;


public class ClusterMetadataAbortedBootstrapRejoinTest extends TestBaseImpl
{
    @Test
    public void testFailedBootstrapNotAllowedToJoin() throws IOException, TimeoutException, ExecutionException, InterruptedException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(BBHelper::install)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                           .withTokenSupplier(even::token)
                                           .start()))
        {
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true);
            IInvokableInstance toBootstrap = cluster.bootstrap(config);
            toBootstrap.startup(cluster);
            toBootstrap.logs().watchFor(Duration.ofSeconds(60), BBHelper.FAILMESSAGE);
            toBootstrap.shutdown().get();
            cluster.get(1).runOnInstance(() -> {
                int i = 0;
                while (FailureDetector.instance.isAlive(InetAddressAndPort.getByNameUnchecked("127.0.0.3")) && i++ < 30)
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            });
            cluster.get(1).nodetoolResult("abortbootstrap", "--ip", "127.0.0.3").asserts().success();
            Assertions.assertThatThrownBy(toBootstrap::startup)
                      .isInstanceOf(IllegalStateException.class)
                      .hasMessageContaining("but is not present in cluster metadata");
        }
    }

    public static class BBHelper
    {
        public static String FAILMESSAGE = "ARTIFICIALLY FAILING BOOTSTRAP";
        public static AtomicBoolean enabled = new AtomicBoolean(true);
        public static void install(ClassLoader cl, int i)
        {
            if (i == 3)
            {
                new ByteBuddy().rebase(StorageService.class)
                               .method(named("repairPaxosForTopologyChange").and(takesArguments(1)))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void repairPaxosForTopologyChange(String reason)
        {
            if (enabled.get())
            {
                enabled.set(false);
                throw new RuntimeException(FAILMESSAGE);
            }
        }

    }
}
