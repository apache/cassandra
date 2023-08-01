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

package org.apache.cassandra.distributed.test.ring;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToDecommission;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;

public class CleanupFailureTest extends TestBaseImpl
{
    @Test
    public void cleanupDuringDecommissionTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withTokenSupplier(evenlyDistributedTokens(2))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            IInvokableInstance nodeToDecommission = cluster.get(1);
            IInvokableInstance nodeToRemainInCluster = cluster.get(2);

            // Start decomission on nodeToDecommission
            cluster.forEach(statusToDecommission(nodeToDecommission));

            // Add data to cluster while node is decomissioning
            int NUM_ROWS = 100;
            populate(cluster, 0, NUM_ROWS, 1, 1, ConsistencyLevel.ONE);
            cluster.forEach(c -> c.flush(KEYSPACE));

            // Check data before cleanup on nodeToRemainInCluster
            assertEquals(100, nodeToRemainInCluster.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);

            // Run cleanup on nodeToRemainInCluster
            NodeToolResult result = nodeToRemainInCluster.nodetoolResult("cleanup");
            result.asserts().failure();
            result.asserts().stderrContains("Node is involved in cluster membership changes. Not safe to run cleanup.");

            // Check data after cleanup on nodeToRemainInCluster
            assertEquals(100, nodeToRemainInCluster.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);
        }
    }

    @Test
    public void cleanupDuringBootstrapTest() throws Throwable
    {
        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance bootstrappingNode = cluster.bootstrap(config);
            withProperty(JOIN_RING, false,
                         () -> bootstrappingNode.startup(cluster));

            // Start decomission on bootstrappingNode
            cluster.forEach(statusToBootstrap(bootstrappingNode));

            // Add data to cluster while node is bootstrapping
            int NUM_ROWS = 100;
            populate(cluster, 0, NUM_ROWS, 1, 2, ConsistencyLevel.ONE);
            cluster.forEach(c -> c.flush(KEYSPACE));

            // Check data before cleanup on bootstrappingNode
            assertEquals(NUM_ROWS, bootstrappingNode.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);

            // Run cleanup on bootstrappingNode
            NodeToolResult result = bootstrappingNode.nodetoolResult("cleanup");
            result.asserts().stderrContains("Node is involved in cluster membership changes. Not safe to run cleanup.");

            // Check data after cleanup on bootstrappingNode
            assertEquals(NUM_ROWS, bootstrappingNode.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);
        }
    }
}