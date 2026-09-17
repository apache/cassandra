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

package org.apache.cassandra.distributed.test.log;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.Iterables;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.startHostReplacement;
import static org.apache.cassandra.tcm.Transformation.Kind.ADVANCE_CMS_RECONFIGURATION;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrentCMSReconfigurationAndNodeOperationsTest extends TestBaseImpl
{
    @Test
    public void testConcurrentCMSReconfigurationAndReplacementOfJoiningMember() throws Exception
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        // configure a short cms_await_timeout as one of the CMS nodes will be paused, meaning
        // any commit requests sent to it will eventually timeout and be resubmitted to another
        // member
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                               .set("cms_await_timeout", "1s"))
                                             .withTokenSupplier(node -> even.token(node == 4 ? 3 : node))
                                             .start()))
        {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            IInvokableInstance firstCMS = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, firstCMS);
            firstCMS.nodetoolResult("cms", "reconfigure", "2").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, firstCMS);

            // nodes 1 & 2 are now CMS members, prepare another reconfiguration which will add node 3
            // but force it to halt before the first step is committed by pausing commits on the node
            // driving it (node1)
            Callable<Epoch> pending = ClusterUtils.pauseBeforeCommit(firstCMS, e -> e.kind() == ADVANCE_CMS_RECONFIGURATION);
            Future<NodeToolResult> inFlightReconfig = executor.submit(() -> firstCMS.nodetoolResult("cms", "reconfigure", "3"));
            pending.call();

            String status = cluster.get(1).nodetoolResult("cms", "reconfigure", "--status").getStdout();
            assertTrue(status.contains("ADDITIONS: [/127.0.0.3"));

            // Now the reconfiguration which is active in cluster metadata includes node3, try replacing it.
            // node1 is still paused so cannot receive commit requests itself, but it can still participate
            // in consensus decisions with node2
            IInvokableInstance nodeToRemove = cluster.get(3);
            nodeToRemove.shutdown().get();
            IInvokableInstance replacingNode = addInstance(cluster, nodeToRemove.config(),
                                                           c -> c.set("auto_bootstrap", true)
                                                                 .set("progress_barrier_min_consistency_level", ConsistencyLevel.ONE));
            startHostReplacement(nodeToRemove, replacingNode, (ignore1_, ignore2_) -> {});
            awaitRingJoin(cluster.get(1), replacingNode);
            awaitRingJoin(replacingNode, cluster.get(1));

            // now that node4 has replaced node3, unpause node1 to allow it to continue with the inflight CMS
            // reconfiguration, which will fail.
            ClusterUtils.unpauseCommits(firstCMS);
            inFlightReconfig.get().asserts().failure();
            // Cancel the inflight reconfiguration and resubmit another, which will now succeed
            firstCMS.nodetoolResult("cms", "reconfigure", "--cancel").asserts().success();
            firstCMS.nodetoolResult("cms", "reconfigure", "3").asserts().success();
        }
    }

    @Test
    public void testConcurrentCMSReconfigurationAndReplacementOfLeavingMember() throws Exception
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        // configure a short cms_await_timeout as one of the CMS nodes will be paused, meaning
        // any commit requests sent to it will eventually timeout and be resubmitted to another
        // member
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                               .set("cms_await_timeout", "1s"))
                                             .withTokenSupplier(node -> even.token(node == 4 ? 3 : node))
                                             .start()))
        {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            IInvokableInstance firstCMS = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, firstCMS);
            firstCMS.nodetoolResult("cms", "reconfigure", "3").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, firstCMS);

            // nodes 1, 2 & 3 are now CMS members, prepare another reconfiguration which will remove node 3
            // but force it to halt before the first step is committed by pausing commits on the node
            // driving it (node1)
            Callable<Epoch> pending = ClusterUtils.pauseBeforeCommit(firstCMS, e -> e.kind() == ADVANCE_CMS_RECONFIGURATION);
            Future<NodeToolResult> inFlightReconfig = executor.submit(() -> firstCMS.nodetoolResult("cms", "reconfigure", "2"));
            pending.call();

            String status = cluster.get(1).nodetoolResult("cms", "reconfigure", "--status").getStdout();
            assertTrue(status.contains("REMOVALS: [/127.0.0.3"));

            // Now the reconfiguration which is active in cluster metadata includes node3, try replacing it.
            // node1 is still paused so cannot receive commit requests itself, but it can still participate
            // in consensus decisions with node2
            IInvokableInstance nodeToRemove = cluster.get(3);
            nodeToRemove.shutdown().get();
            IInvokableInstance replacingNode = addInstance(cluster, nodeToRemove.config(),
                                                           c -> c.set("auto_bootstrap", true)
                                                                 .set("progress_barrier_min_consistency_level", ConsistencyLevel.ONE));
            try
            {
                startHostReplacement(nodeToRemove, replacingNode, (ignore1_, ignore2_) -> {});
                fail("Instance replacement should fail");
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("Can not commit transformation: \"INVALID\"(Can not add a new in-progress sequence for Reconfigure CMS"));
            }

            // the replacement has failed, unpause node1 to allow it to continue with the inflight CMS
            // reconfiguration, which will succeed.
            ClusterUtils.unpauseCommits(firstCMS);
            inFlightReconfig.get().asserts().success();
        }
    }

    @Test
    public void testConcurrentCMSReconfigurationAndMove() throws Exception
    {
        // configure a short cms_await_timeout as one of the CMS nodes will be paused, meaning
        // any commit requests sent to it will eventually timeout and be resubmitted to another
        // member
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                               .set("cms_await_timeout", "1s"))
                                             .withoutVNodes() // move not supported with vnodes
                                             .start()))
        {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            IInvokableInstance firstCMS = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, firstCMS);
            firstCMS.nodetoolResult("cms", "reconfigure", "2").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, firstCMS);

            // nodes 1 & 2 are now CMS members, prepare another reconfiguration which will add node 3
            // but force it to halt before the first step is committed by pausing commits on the node
            // driving it (node1)
            Callable<Epoch> pending = ClusterUtils.pauseBeforeCommit(firstCMS, e -> e.kind() == ADVANCE_CMS_RECONFIGURATION);
            Future<NodeToolResult> reconfigureResult = executor.submit(() -> firstCMS.nodetoolResult("cms", "reconfigure", "3"));
            pending.call();
            String status = cluster.get(1).nodetoolResult("cms", "reconfigure", "--status").getStdout();
            assertTrue(status.contains("ADDITIONS: [/127.0.0.3"));

            // Now the reconfiguration which is active in cluster metadata includes node3, try moving it to a new token.
            // node1 is still paused so cannot receive commit requests itself, but it can still participate in consensus
            // decisions with node2
            IInvokableInstance nodeToMove = cluster.get(3);
            String token = nodeToMove.callsOnInstance(() -> Iterables.getOnlyElement(StorageService.instance.getLocalTokens()).toString()).call();
            long moveTo = Long.parseLong(token) + 1;
            nodeToMove.nodetoolResult("move", Long.toString(moveTo)).asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, nodeToMove);

            // now that node4 has replaced node3, unpause node1 to allow it to continue with the inflight
            // CMS reconfiguration, which will succeed.
            ClusterUtils.unpauseCommits(firstCMS);
            reconfigureResult.get().asserts().success();
        }
    }
}