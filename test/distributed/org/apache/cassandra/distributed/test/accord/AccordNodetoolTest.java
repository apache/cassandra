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

package org.apache.cassandra.distributed.test.accord;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import accord.local.Node;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;


import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getNodeId;

public class AccordNodetoolTest extends TestBaseImpl
{
    @Test
    public void testMarkSingleNode() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.get(1).nodetoolResult("accord", "mark_stale", "1").asserts().success();
            cluster.get(1).runOnInstance(() -> assertEquals(ImmutableSet.of(new Node.Id(1)), ClusterMetadata.current().accordStaleReplicas.ids()));
            cluster.get(1).nodetoolResult("accord", "describe").asserts().stdoutContains("Stale Replicas: 1");

            // Reject the operation if the target node is already stale:
            cluster.get(1).nodetoolResult("accord", "mark_stale", "1").asserts().failure().errorContains("it already is");

            // Reject the operation if marking the node stale brings us below a quorum of non-stale nodes:
            cluster.get(1).nodetoolResult("accord", "mark_stale", "2").asserts().failure().errorContains("that would leave fewer than a quorum");

            // Reject the operation if the target node doesn't exist:
            cluster.get(1).nodetoolResult("accord", "mark_stale", "4").asserts().failure().errorContains("not present in the directory");
            
            cluster.get(1).nodetoolResult("accord", "mark_rejoining", "1").asserts().success();
            cluster.get(1).runOnInstance(() -> assertEquals(Collections.emptySet(), ClusterMetadata.current().accordStaleReplicas.ids()));

            cluster.get(1).nodetoolResult("accord", "mark_rejoining", "1").asserts().failure().errorContains("it is not stale");
            cluster.get(1).nodetoolResult("accord", "mark_rejoining", "4").asserts().failure().errorContains("not present in the directory");
        }
    }

    @Test
    public void testMarkMultipleNodes() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(5).withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            // Reject the operation if marking the node stale brings us below a quorum of non-stale nodes:
            cluster.get(1).nodetoolResult("accord", "mark_stale", "1", "2", "3").asserts().failure().errorContains("that would leave fewer than a quorum");

            cluster.get(1).nodetoolResult("accord", "mark_stale", "1", "2").asserts().success();
            cluster.get(1).runOnInstance(() -> assertEquals(ImmutableSet.of(new Node.Id(1), new Node.Id(2)), ClusterMetadata.current().accordStaleReplicas.ids()));
            cluster.get(1).nodetoolResult("accord", "describe").asserts().stdoutContains("Stale Replicas: 1,2");

            // Reject the operation if a target node is already stale:
            cluster.get(1).nodetoolResult("accord", "mark_stale", "1", "2").asserts().failure().errorContains("it already is");

            // Reject the operation if a target node doesn't exist:
            cluster.get(1).nodetoolResult("accord", "mark_stale", "4", "6").asserts().failure().errorContains("not present in the directory");

            Map<Integer, Integer> nodeIdToNode = new HashMap<>();
            for (int i = 1; i <= 5; i++)
                nodeIdToNode.put(getNodeId(cluster.get(i)).id(), i);

            // Remove the second stale node, and ensure the set of stale replicas is updated:
            cluster.get(nodeIdToNode.get(2)).shutdown().get();
            cluster.get(1).nodetoolResult("removenode", "2", "--force").asserts().success();
            cluster.get(1).nodetoolResult("cms", "unregister", "2").asserts().success();
            cluster.get(1).runOnInstance(() -> assertEquals(ImmutableSet.of(new Node.Id(1)), ClusterMetadata.current().accordStaleReplicas.ids()));

            cluster.get(1).nodetoolResult("accord", "mark_rejoining", "1", "3").asserts().failure().errorContains("it is not stale");
            cluster.get(1).nodetoolResult("accord", "mark_rejoining", "1", "6").asserts().failure().errorContains("not present in the directory");
            cluster.get(1).runOnInstance(() -> assertEquals(ImmutableSet.of(new Node.Id(1)), ClusterMetadata.current().accordStaleReplicas.ids()));
        }
    }
}
