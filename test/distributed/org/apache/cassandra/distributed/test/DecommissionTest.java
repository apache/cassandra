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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.action.GossipHelper;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadataService;
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
            assertBootstrapState(instance, COMPLETED);
            instance.nodetoolResult("decommission", "--force")
                    .asserts()
                    .failure()
                    .stderrContains("simulated error in prepareUnbootstrapStreaming");
            instance.runOnInstance(() -> {
                                       assertFalse(StorageService.instance.isDecommissioning());
                                       assertTrue(StorageService.instance.isDecommissionFailed());
                                   });

            // still COMPLETED, nothing has changed
            assertBootstrapState(instance, COMPLETED);
            assertOperationMode(instance, DECOMMISSION_FAILED);
            instance.nodetoolResult("decommission", "--force").asserts().success();
            instance.runOnInstance(() -> {
                                       assertFalse(StorageService.instance.isDecommissionFailed());
                                       assertFalse(StorageService.instance.isDecommissioning());
                                   });
            assertBootstrapState(instance, DECOMMISSIONED);
            instance.nodetoolResult("decommission", "--force")
                    .asserts()
                    .success()
                    .stdoutContains("Node was already decommissioned");
            assertBootstrapState(instance, DECOMMISSIONED);
            instance.runOnInstance(() -> {
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
            assertBootstrapState(instance, COMPLETED);
            // pretend that decommissioning has failed in the middle
            instance.nodetoolResult("decommission", "--force")
                    .asserts()
                    .failure()
                    .stderrContains("simulated error in prepareUnbootstrapStreaming");
            assertOperationMode(instance, DECOMMISSION_FAILED);
            // restart the node which we failed to decommission
            stopUnchecked(instance);
            instance.startup();
            // it starts up as DECOMMISSION_FAILED so let's decommission again
            assertOperationMode(instance, DECOMMISSION_FAILED);
            instance.nodetoolResult("decommission", "--force").asserts().success();
            assertBootstrapState(instance, DECOMMISSIONED);
            instance.runOnInstance(() -> {
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

    @Test
    public void testRestartDecommedNode() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
            cluster.get(2).shutdown().get();
            try
            {
                cluster.get(2).startup();
                fail();
            }
            catch (Exception e)
            {
                cluster.get(2).runOnInstance(() -> ClusterMetadataService.unsetInstance());
                assertTrue(e.getMessage().contains("This node was decommissioned and will not rejoin the ring unless cassandra.override_decommission=true"));
            }

            GossipHelper.withProperty(CassandraRelevantProperties.OVERRIDE_DECOMMISSION, true, () -> cluster.get(2).startup());
            assertBootstrapState(cluster.get(2), COMPLETED);
        }
    }

    private static void assertBootstrapState(IInvokableInstance i, SystemKeyspace.BootstrapState expectedState)
    {
        String bootstrapState = expectedState.name();
        i.runOnInstance(() -> assertEquals(bootstrapState, SystemKeyspace.getBootstrapState().name()));
    }

    private static void assertOperationMode(IInvokableInstance i, StorageService.Mode mode)
    {
        String operationMode = mode.name();
        i.runOnInstance(() -> assertEquals(operationMode, StorageService.instance.operationMode().name()));
    }

}
