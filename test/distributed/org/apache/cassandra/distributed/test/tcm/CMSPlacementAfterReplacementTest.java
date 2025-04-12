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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.startHostReplacement;
import static org.junit.Assert.assertTrue;

public class CMSPlacementAfterReplacementTest extends TestBaseImpl
{
    @Test
    public void replaceSmallerRF() throws IOException, ExecutionException, InterruptedException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(4);
        try (Cluster cluster = init(Cluster.build(4)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 5 ? 2 : node))
                                      .start()))
        {
            replacementHelper(cluster);
        }
    }

    @Test
    public void replaceEqualRF() throws IOException, ExecutionException, InterruptedException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .withTokenSupplier(node -> even.token(node == 4 ? 2 : node))
                                           .start()))
        {
            replacementHelper(cluster);
        }
    }

    /**
     * 1. make the CMS contain 3 nodes
     * 2. make sure node2 is in the CMS
     * 3. replace node2
     * 4. make sure the replacement node appears as a member of the CMS
     */
    private static void replacementHelper(Cluster cluster) throws ExecutionException, InterruptedException
    {
        IInvokableInstance nodeToRemove = cluster.get(2);
        cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
        cluster.get(2).runOnInstance(() -> {
            assertTrue(ClusterMetadata.current().isCMSMember(FBUtilities.getBroadcastAddressAndPort()));
        });
        nodeToRemove.shutdown().get();
        IInvokableInstance replacingNode = addInstance(cluster, nodeToRemove.config(),
                                                       c -> c.set("auto_bootstrap", true)
                                                             .set("progress_barrier_min_consistency_level", ConsistencyLevel.ONE));
        startHostReplacement(nodeToRemove, replacingNode, (ignore1_, ignore2_) -> {});
        awaitRingJoin(cluster.get(1), replacingNode);
        awaitRingJoin(replacingNode, cluster.get(1));
        int replacementNodeId = replacingNode.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
        assertInCMS(cluster, replacementNodeId);
    }

    static void assertInCMS(Cluster cluster, int nodeId)
    {
        cluster.get(1).runOnInstance(() -> {
            InetAddressAndPort ep = ClusterMetadata.current().directory.endpoint(new NodeId(nodeId));
            int tries = 0;
            while (!ClusterMetadata.current().isCMSMember(ep))
            {
                if (tries > 10)
                    throw new AssertionError(ep + " did not become a CMS member after " + tries + " seconds");
                tries++;
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        });
    }
}
