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

import java.util.concurrent.Callable;
import java.util.function.Supplier;

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
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
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


    public static class BB
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("prepareUnbootstrapStreaming"))
                           .intercept(MethodDelegation.to(DecommissionTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        private static int invocations = 0;

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
    }
}
