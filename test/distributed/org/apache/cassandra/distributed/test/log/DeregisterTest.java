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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getNodeId;
import static org.junit.Assert.assertEquals;

public class DeregisterTest extends TestBaseImpl
{
    @Test
    public void testUnregister() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(5)
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                        .start()))
        {
            Map<Integer, String> nodeToNodeId = new HashMap<>();
            for (int i = 1; i <= 5; i++)
                nodeToNodeId.put(i, String.valueOf(getNodeId(cluster.get(i)).id()));

            cluster.get(5).nodetoolResult("decommission", "--force").asserts().success();
            cluster.get(4).nodetoolResult("decommission", "--force").asserts().success();
            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();
            // deregister a single node
            cluster.get(1).nodetoolResult("cms", "deregister", nodeToNodeId.get(5)).asserts().success();
            // deregister multiple nodes
            cluster.get(1).nodetoolResult("cms", "deregister", nodeToNodeId.get(4), nodeToNodeId.get(3)).asserts().success();
            // try to deregister a joined node, should fail
            cluster.get(1).nodetoolResult("cms", "deregister", nodeToNodeId.get(2)).asserts().failure();

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertEquals(2, metadata.directory.states.size());
                for (Map.Entry<NodeId, NodeState> entry : metadata.directory.states.entrySet())
                    assertEquals(NodeState.JOINED, entry.getValue());
            });
        }
    }
}
