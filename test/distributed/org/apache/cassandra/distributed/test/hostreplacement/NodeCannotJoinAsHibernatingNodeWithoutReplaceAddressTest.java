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
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.InstanceIDDefiner;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Shared;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class NodeCannotJoinAsHibernatingNodeWithoutReplaceAddressTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException, InterruptedException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(2);
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(Feature.values()).set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                           .withInstanceInitializer(BBHelper::install)
                                           .withTokenSupplier(node -> even.token((node == 3 || node == 4) ? 2 : node))
                                           .start()))
        {
            final IInvokableInstance toReplace = cluster.get(2);
            final String toReplaceAddress = toReplace.broadcastAddress().getAddress().getHostAddress();

            SharedState.cluster = cluster;
            cluster.setUncaughtExceptionsFilter((nodeId, cause) -> nodeId > 2); // ignore host replacement errors
            fixDistributedSchemas(cluster);

            ClusterUtils.stopUnchecked(toReplace);

            try
            {
                ClusterUtils.replaceHostAndStart(cluster, toReplace, (inst, ignore) -> ClusterUtils.updateAddress(inst, toReplaceAddress));
                Assert.fail("Host replacement should exit with an error");
            }
            catch (Exception e)
            {
                // the instance is expected to fail, but it may not have finished shutdown yet, so wait for it to shutdown
                SharedState.shutdownComplete.await(1, TimeUnit.MINUTES);
            }

            IInvokableInstance inst = ClusterUtils.addInstance(cluster, toReplace.config(), c -> c.set("auto_bootstrap", true));
            ClusterUtils.updateAddress(inst, toReplaceAddress);
            Assertions.assertThatThrownBy(() -> inst.startup())
                      .hasMessageContaining("A node with address")
                      .hasMessageContaining("already exists, cancelling join");
        }
    }

    public static class BBHelper
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 3)
                return;
            shutdownBeforeNormal(cl);
        }

        private static void shutdownBeforeNormal(ClassLoader cl)
        {
            new ByteBuddy().rebase(PendingRangeCalculatorService.class)
                           .method(named("blockUntilFinished"))
                           .intercept(MethodDelegation.to(ShutdownBeforeNormal.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
    }

    @Shared
    public static class SharedState
    {
        public static volatile ICluster cluster;
        // Instance.shutdown can only be called once so only the caller knows when its done (isShutdown looks at a field set BEFORE shutting down..)
        // since the test needs to know when shutdown completes, add this static state so the caller (bytebuddy rewrite) can update it
        public static final CountDownLatch shutdownComplete = new CountDownLatch(1);
    }

    public static class ShutdownBeforeNormal
    {
        public static void blockUntilFinished(@SuperCall Runnable fn)
        {
            fn.run();
            int id = Integer.parseInt(InstanceIDDefiner.getInstanceId().replace("node", ""));
            ICluster cluster = Objects.requireNonNull(SharedState.cluster);
            // can't stop here as the stop method and start method share a lock; and block gets called in start...
            ForkJoinPool.commonPool().execute(() -> {
                ClusterUtils.stopAbrupt(cluster, cluster.get(id));
                SharedState.shutdownComplete.countDown();
            });
            JVMStabilityInspector.killCurrentJVM(new RuntimeException("Attempting to stop the instance"), false);
        }
    }
}
