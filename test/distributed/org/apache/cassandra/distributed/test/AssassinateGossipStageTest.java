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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;

import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitGossipStatus;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingHealthy;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getBroadcastAddressHostWithPortString;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAbrupt;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the gossip stage blocking issue in {@code nodetool assassinate}.
 *
 * CASSANDRA-15059 wrapped assassinateEndpoint in runInGossipStageBlocking, which moved the
 * 30-second RING_DELAY sleep onto the single-threaded GOSSIP stage. This causes two bugs:
 *   1. The liveness check is ineffective — the target's heartbeat cannot be updated while the
 *      GOSSIP stage is blocked, so the check always passes, even for a live node.
 *   2. The executing node is marked as DOWN by peers — the GOSSIP stage is blocked for ~34s,
 *      so peers' failure detectors convict the executing node.
 */
public class AssassinateGossipStageTest extends TestBaseImpl
{
    /**
     * Assassinating a live, healthy node should be rejected with "Endpoint still alive".
     *
     * This test is expected to FAIL with the current broken code because the liveness check
     * cannot detect heartbeat changes while the GOSSIP stage is blocked by the sleep.
     */
    @Test
    public void assassinateLiveNodeShouldFail() throws IOException
    {
        try (Cluster cluster = buildCluster())
        {
            cluster.setUncaughtExceptionsFilter(t -> t.getMessage() != null
                                                     && t.getMessage().contains("Endpoint still alive"));

            awaitRingHealthy(cluster.get(1));

            IInvokableInstance executor = cluster.get(1);
            IInvokableInstance target = cluster.get(2);

            NodeToolResult result = executor.nodetoolResult("assassinate",
                                                           getBroadcastAddressHostWithPortString(target));

            // A live node assassination should fail
            result.asserts().failure();
            result.asserts().errorContains("Endpoint still alive");
        }
    }

    /**
     * The node running the assassinate command should not be marked as DOWN by peers.
     *
     * This test is expected to FAIL with the current broken code because the GOSSIP stage
     * is blocked for ~14 seconds (RING_DELAY=10s + 4s propagation wait in test config),
     * causing the failure detector on peers to convict the executing node.
     *
     * The test runs the assassination in a background thread and continuously polls the
     * observer's ring view to detect any transient "Down" status on the executor.
     */
    @Test
    public void executingNodeShouldNotBeMarkedDown() throws Exception
    {
        try (Cluster cluster = buildCluster())
        {
            awaitRingHealthy(cluster.get(1));

            IInvokableInstance executor = cluster.get(1);
            IInvokableInstance target = cluster.get(2);
            IInvokableInstance observer = cluster.get(3);

            stopAbrupt(cluster, target);

            AtomicBoolean sawDown = new AtomicBoolean(false);
            AtomicBoolean assassinationDone = new AtomicBoolean(false);

            ExecutorService es = Executors.newSingleThreadExecutor();
            Future<NodeToolResult> future = es.submit(() -> {
                NodeToolResult r = executor.nodetoolResult("assassinate",
                                                          getBroadcastAddressHostWithPortString(target));
                assassinationDone.set(true);
                return r;
            });

            // Poll the observer's view of the executor during the assassination
            String executorAddress = executor.config().broadcastAddress().getAddress().getHostAddress();
            while (!assassinationDone.get())
            {
                List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(observer);
                for (ClusterUtils.RingInstanceDetails details : ring)
                {
                    if (details.getAddress().equals(executorAddress) && details.getStatus().equals("Down"))
                    {
                        sawDown.set(true);
                    }
                }
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }

            NodeToolResult result = future.get();
            result.asserts().success();
            es.shutdown();

            assertFalse("Executing node was transiently marked Down by observer during assassination",
                        sawDown.get());
        }
    }

    /**
     * Assassinating a truly dead node should succeed.
     *
     * This test should PASS with both the current and fixed code.
     */
    @Test
    public void assassinateDeadNodeShouldSucceed() throws IOException
    {
        try (Cluster cluster = buildCluster())
        {
            awaitRingHealthy(cluster.get(1));

            IInvokableInstance executor = cluster.get(1);
            IInvokableInstance target = cluster.get(2);
            IInvokableInstance observer = cluster.get(3);

            stopAbrupt(cluster, target);

            NodeToolResult result = executor.nodetoolResult("assassinate",
                                                           getBroadcastAddressHostWithPortString(target));

            result.asserts().success();

            // The target should eventually be seen as LEFT by the observer
            awaitGossipStatus(observer, target, "LEFT");
        }
    }

    private static Cluster buildCluster() throws IOException
    {
        return Cluster.build(3).withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)).start();
    }
}
