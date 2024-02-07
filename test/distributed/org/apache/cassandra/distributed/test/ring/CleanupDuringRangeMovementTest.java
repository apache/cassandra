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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.decommission;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CleanupDuringRangeMovementTest extends TestBaseImpl
{
    @Test
    public void cleanupDuringDecommissionTest() throws Throwable
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withTokenSupplier(evenlyDistributedTokens(2))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .start(), 1))
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance nodeToDecommission = cluster.get(2);
            IInvokableInstance nodeToRemainInCluster = cluster.get(1);  // CMS instance remains in the cluster

            // Create table before starting decommission as at the moment schema changes are not permitted
            // while range movements are in-flight. Additionally, pausing the CMS instance to block the
            // leave sequence from completing would also block the commit of the schema transformation
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Prime the CMS node to pause before the finish leave event is committed
            Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareLeave.FinishLeave);
            // Start decomission on nodeToDecommission
            Future<Boolean> decomFuture = executor.submit(() -> decommission(nodeToDecommission));
            pending.call();

            // Add data to cluster while node is decomissioning
            int numRows = 100;
            insertData(cluster, 2, numRows, ConsistencyLevel.ONE);
            cluster.forEach(c -> c.flush(KEYSPACE));

            // Check data before cleanup on nodeToRemainInCluster
            assertEquals(numRows, nodeToRemainInCluster.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);

            // Run cleanup on nodeToRemainInCluster
            NodeToolResult result = nodeToRemainInCluster.nodetoolResult("cleanup");
            result.asserts().success();

            // Check data after cleanup on nodeToRemainInCluster
            assertEquals(numRows, nodeToRemainInCluster.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);

            unpauseCommits(cmsInstance);
            assertTrue(decomFuture.get());
        }
    }

    @Test
    public void cleanupDuringBootstrapTest() throws Throwable
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = init(builder().withNodes(originalNodeCount)
                                             .withTokenSupplier(evenlyDistributedTokens(expandedNodeCount))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                             .withConfig(config -> config.with(NETWORK, GOSSIP))
                                             .start(), 2))
        {
            // Create table before starting bootstrap as at the moment schema changes are not permitted
            // while range movements are in-flight. Additionally, pausing the CMS instance to block the
            // leave sequence from completing would also block the commit of the schema transformation
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IInvokableInstance cmsInstance = cluster.get(1);
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance bootstrappingNode = cluster.bootstrap(config);

            // Prime the CMS node to pause before the finish join event is committed
            Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareJoin.FinishJoin);
            Future<?> bootstrapFuture = executor.submit(() -> bootstrappingNode.startup());
            pending.call();

            // Add data to cluster while node is bootstrapping
            int numRows = 100;
            insertData(cluster, 1, numRows, ConsistencyLevel.ONE);
            cluster.forEach(c -> c.flush(KEYSPACE));

            // Check data before cleanup on bootstrappingNode
            assertEquals(numRows, bootstrappingNode.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);

            // Run cleanup on bootstrappingNode
            NodeToolResult result = bootstrappingNode.nodetoolResult("cleanup");
            result.asserts().success();

            // Check data after cleanup on bootstrappingNode
            assertEquals(numRows, bootstrappingNode.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl").length);
            unpauseCommits(cmsInstance);
            bootstrapFuture.get();
        }
    }

    private void insertData(Cluster cluster, int node, int numberOfRows, ConsistencyLevel cl)
    {
        for (int i = 0; i < numberOfRows; i++)
        {
            cluster.coordinator(node).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", cl, i, i, i);
        }
        cluster.forEach(c -> c.flush(KEYSPACE));
    }
}
