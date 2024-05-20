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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getNodeId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnregisterTest extends TestBaseImpl
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
            verifyVirtualTable(cluster, nodeToNodeId, 5);
            cluster.get(5).nodetoolResult("decommission", "--force").asserts().success();

            verifyVirtualTable(cluster, nodeToNodeId,5, 5);
            cluster.get(4).nodetoolResult("decommission", "--force").asserts().success();
            verifyVirtualTable(cluster, nodeToNodeId, 5, 5, 4);
            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();
            verifyVirtualTable(cluster, nodeToNodeId, 5, 5, 4, 3);
            // unregister a single node
            cluster.get(1).nodetoolResult("cms", "unregister", nodeToNodeId.get(5)).asserts().success();
            verifyVirtualTable(cluster, nodeToNodeId, 4, 4, 3);
            // unregister multiple nodes
            cluster.get(1).nodetoolResult("cms", "unregister", nodeToNodeId.get(4), nodeToNodeId.get(3)).asserts().success();
            verifyVirtualTable(cluster, nodeToNodeId, 2);
            // try to unregister a joined node, should fail
            cluster.get(1).nodetoolResult("cms", "unregister", nodeToNodeId.get(2)).asserts().failure();
            verifyVirtualTable(cluster, nodeToNodeId,2);

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertEquals(2, metadata.directory.states.size());
                for (Map.Entry<NodeId, NodeState> entry : metadata.directory.states.entrySet())
                    assertEquals(NodeState.JOINED, entry.getValue());
            });
        }
    }

    private static void verifyVirtualTable(Cluster cluster, Map<Integer, String> nodeToNodeId, int expectedTotal, int ... expectedLeftNodes)
    {
        Set<Integer> leftNodeIds = new HashSet<>();
        for (int i : expectedLeftNodes)
        {
            NodeId nodeId = NodeId.fromString(nodeToNodeId.get(i));
            leftNodeIds.add(nodeId.id());
        }
        Object [][] res = cluster.get(1).executeInternal("select node_id, state from system_views.cluster_metadata_directory");
        assertEquals(expectedTotal, res.length);
        for (Object [] row : res)
        {
            int id = (int)row[0];
            if (row[1].equals("JOINED"))
                assertFalse(leftNodeIds.contains(id));
            else if (row[1].equals("LEFT"))
                assertTrue(leftNodeIds.remove(id));
            else
                throw new AssertionError("Unexpected state: " + row[1]);
        }
    }
}
