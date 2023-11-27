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
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.sequences.UnbootstrapStreams;

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
            IInvokableInstance instance = cluster.get(2);

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
                    assertTrue(t.getMessage().contains("simulated error in prepareUnbootstrapStreaming"));
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

                assertEquals(DECOMMISSIONED.name(), StorageService.instance.getBootstrapState());
                assertFalse(StorageService.instance.isDecommissionFailed());

                try
                {
                    StorageService.instance.decommission(true);
                    fail("Should have failed since the node is in decomissioned state");
                }
                catch (UnsupportedOperationException e)
                {
                    // ignore
                }
                assertEquals(DECOMMISSIONED.name(), StorageService.instance.getBootstrapState());
                assertFalse(StorageService.instance.isDecommissionFailed());
                assertFalse(StorageService.instance.isDecommissioning());
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
                                               if (num == 2 && generation != 1)
                                                   BB.install(classLoader, num);
                                           })
                                           .start()))
        {
            IInvokableInstance instance = cluster.get(2);

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
                    assertTrue(t.getMessage().contains("simulated error in prepareUnbootstrapStreaming"));
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
                StorageService.instance.decommission(true);
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
            if (num == 2)
            {
                new ByteBuddy().rebase(UnbootstrapStreams.class)
                               .method(named("execute"))
                               .intercept(MethodDelegation.to(DecommissionTest.BB.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void execute(NodeId leaving, PlacementDeltas startLeave, PlacementDeltas midLeave, PlacementDeltas finishLeave,
                                   @SuperCall Callable<?> zuper) throws ExecutionException, InterruptedException
        {
            if (!StorageService.instance.isDecommissionFailed())
                throw new ExecutionException(new RuntimeException("simulated error in prepareUnbootstrapStreaming"));

            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
