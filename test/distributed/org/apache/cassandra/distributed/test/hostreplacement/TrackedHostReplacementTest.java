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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertMatchingSummaryForTable;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryForTable;
import static org.assertj.core.api.Assertions.assertThat;

public class TrackedHostReplacementTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedHostReplacementTest.class);

    private static final String KEYSPACE = "test_ks";
    private static final String TABLE = "test_table";
    private static final String QUALIFIED_TABLE_NAME = KEYSPACE + '.' + TABLE;

    private static void pauseLogBroadcasts(Cluster cluster, boolean pause)
    {
        cluster.stream()
               .filter(node -> !node.isShutdown())
               .forEach(node -> node.runOnInstance(() -> {
                   MutationTrackingService.instance.pauseOffsetBroadcast(pause);
               }));
    }

    private static void awaitFullReconciliation(Cluster cluster, int ids) throws InterruptedException
    {
        // await full reconciliation
        boolean fullyReconciled = false;
        for (int i = 0; i < 20; i++)
        {
            int attempt = i + 1;
            fullyReconciled = cluster.stream().filter(node -> !node.isShutdown()).allMatch(node -> {
                MutationSummary summary = summaryForTable(node, KEYSPACE, TABLE);
                if (summary.unreconciledIds() == 0)
                {
                    Assert.assertEquals(node.toString(), ids, summary.reconciledIds());
                    return true;
                }
                else
                {
                    logger.info("Not yet fully reconciled (reconciled: {}, unreconciled:{}) - attempt {} summary: {}", summary.reconciledIds(), summary.unreconciledIds(), attempt, summary);
                }
                return false;
            });

            if (!fullyReconciled)
                Thread.sleep(1000);
        }

        Assert.assertTrue(fullyReconciled);
    }
    @Test
    public void testBasicTrackedHostReplacement() throws Exception
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(3)
                                          .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                          .withTokenSupplier(node -> even.token(node == 4 ? 3 : node))
                                          .start()))
        {
            setupTrackedKeyspace(cluster);

            // write some initial data
            writeDataRange(cluster, 0, 10);
            awaitFullReconciliation(cluster, 10);
            verifyLocalDataContents(cluster, 0, 10);

            IInvokableInstance victimNode = cluster.get(3);
            Set<String> victimTokens = getNodeTokens(victimNode);

            // Stop the victim node
            stopUnchecked(victimNode);

            // Write more data while victim is down
            writeDataRange(cluster, 10, 20);

            // Verify remaining nodes have the new data
            verifyLocalDataContents(Arrays.asList(cluster.get(1), cluster.get(2)), 10, 20);

            pauseLogBroadcasts(cluster, true);
            MutationSummary expectedSummary = summaryForTable(cluster.get(1), KEYSPACE, TABLE);
            Assert.assertEquals(10, expectedSummary.reconciledIds());
            Assert.assertEquals(10, expectedSummary.unreconciledIds());

            // Replace the node
            IInvokableInstance replacementNode = replaceHostAndStart(cluster, victimNode);

            // Wait for replacement to complete
            awaitRingJoin(cluster.get(1), replacementNode);
            awaitRingJoin(cluster.get(2), replacementNode);
            awaitRingJoin(replacementNode, cluster.get(1));
            awaitRingJoin(replacementNode, cluster.get(2));

            // confirm replacement node took over victim's token ranges
            Set<String> replacementTokens = getNodeTokens(replacementNode);
            assertThat(replacementTokens).as("Replacement node should have same tokens as victim")
                                         .isEqualTo(victimTokens);

            assertMatchingSummaryForTable(replacementNode, KEYSPACE, TABLE, expectedSummary);

            List<IInvokableInstance> remainingNodes = List.of(cluster.get(1), cluster.get(2), replacementNode);
            // Verify all nodes have all writes
            verifyLocalDataContents(remainingNodes, 0, 20);


            // unpause id broadcast. all nodes should now reach full reconciliation, even though the replica set has changed
            pauseLogBroadcasts(cluster, false);
            awaitFullReconciliation(cluster, 20);

            // Write new data and verify replacement node handles writes for its ranges
            writeDataRange(cluster, 20, 25);

            awaitFullReconciliation(cluster, 25);
            verifyLocalDataContents(remainingNodes, 20, 25);
        }
    }

    /**
     * Test host replacement with writes to the cluster during replacement bootstrap.
     */
    @Test
    public void testTrackedHostReplacementWithOngoingWrites() throws Exception
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(3)
                                          .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                          .withTokenSupplier(node -> even.token(node == 4 ? 3 : node))
                                          .start()))
        {
            final int numInitialWrites = 10;
            setupTrackedKeyspace(cluster);

            // Phase 1: Establish baseline mutation tracking state
            writeDataRange(cluster, 0, numInitialWrites);
            awaitFullReconciliation(cluster, numInitialWrites);
            verifyLocalDataContents(cluster, 0, numInitialWrites);

            // Capture victim's exact mutation tracking state for streaming validation
            IInvokableInstance victim = cluster.get(3);
            Set<String> victimTokens = getNodeTokens(victim);

            // Phase 2: Stop victim and start replacement bootstrap
            AtomicBoolean replacementCompleted = new AtomicBoolean(false);
            AtomicInteger totalWrites = new AtomicInteger(numInitialWrites);


            Thread thread = new Thread(() -> {
                while (!replacementCompleted.get())
                {
                    int key = totalWrites.getAndIncrement();
                    writeDataToCluster(cluster.coordinator((key % 2) + 1), key, key * 10);
                }
            });
            thread.start();

            stopUnchecked(victim);
            IInvokableInstance replacementNode = replaceHostAndStart(cluster, victim);

            // Wait for replacement to complete
            awaitRingJoin(cluster.get(1), replacementNode);
            awaitRingJoin(cluster.get(2), replacementNode);
            awaitRingJoin(replacementNode, cluster.get(1));
            awaitRingJoin(replacementNode, cluster.get(2));

            // confirm replacement node took over victim's token ranges
            Set<String> replacementTokens = getNodeTokens(replacementNode);
            assertThat(replacementTokens).as("Replacement node should have same tokens as victim")
                                         .isEqualTo(victimTokens);

            // stop concurrent writes
            replacementCompleted.set(true);
            thread.join();

            if (totalWrites.get() == numInitialWrites)
                throw new AssertionError("No concurrent writes were performed during replacement");

            logger.info("Total writes performed: {} ", totalWrites.get());

            List<IInvokableInstance> remainingNodes = List.of(cluster.get(1), cluster.get(2), replacementNode);

            // wait for all nodes to reach full reconciliation and verify data
            awaitFullReconciliation(cluster, totalWrites.get());
            verifyLocalDataContents(remainingNodes, 0, totalWrites.get());
        }
    }

    @Test
    public void testTrackedHostReplacementWithLargeDataSet() throws Exception
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP))
                                            .withTokenSupplier(node -> even.token(node == 4 ? 3 : node))
                                           .start()))
        {
            setupTrackedKeyspace(cluster);

            // Phase 1: Create initial SSTable generation (Generation 1)
            writeDataRange(cluster, 0, 30);
            flushAllNodes(cluster); // Creates first SSTable generation
            advanceMutationLogSegment(cluster);

            // Phase 2: Add data and create overlapping SSTable generation (Generation 2)
            writeDataRange(cluster, 30, 60);
            flushAllNodes(cluster); // Creates second SSTable generation with overlapping keys
            advanceMutationLogSegment(cluster);

            // Phase 3: Create final SSTable generation with different key distribution (Generation 3)
            writeDataRange(cluster, 60, 90);
            flushAllNodes(cluster); // Creates third SSTable generation
            advanceMutationLogSegment(cluster);

            // Capture victim mutation tracking baseline before replacement
            IInvokableInstance nodeToReplace = cluster.get(3);
            Set<String> victimTokens = getNodeTokens(nodeToReplace);

            // Stop victim node and write additional data to create streaming complexity
            nodeToReplace.shutdown().get();

            // Write additional data while victim is down
            writeDataRange(cluster, 90, 120);
            // don't flush


            // Replace the node
            IInvokableInstance replacementNode = replaceHostAndStart(cluster, nodeToReplace);

            // Wait for replacement to complete
            awaitRingJoin(cluster.get(1), replacementNode);
            awaitRingJoin(cluster.get(2), replacementNode);
            awaitRingJoin(replacementNode, cluster.get(1));
            awaitRingJoin(replacementNode, cluster.get(2));

            // confirm replacement node took over victim's token ranges
            Set<String> replacementTokens = getNodeTokens(replacementNode);
            assertThat(replacementTokens).as("Replacement node should have same tokens as victim")
                                         .isEqualTo(victimTokens);

            List<IInvokableInstance> remainingNodes = List.of(cluster.get(1), cluster.get(2), replacementNode);

            // wait for all nodes to reach full reconciliation and verify data
            awaitFullReconciliation(cluster, 120);
            verifyLocalDataContents(remainingNodes, 0, 120);
        }
    }

    private void setupTrackedKeyspace(Cluster cluster)
    {
        cluster.schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'", KEYSPACE));
        cluster.schemaChange(String.format("CREATE TABLE IF NOT EXISTS %s (k int PRIMARY KEY, v int)", QUALIFIED_TABLE_NAME));
    }

    /**
     * Write data using proper coordinator pattern, not direct node access.
     * This ensures replication happens correctly.
     */
    private void writeDataToCluster(Cluster cluster, int key, int value)
    {
        writeDataToCluster(cluster.coordinator(1), key, value);
    }

    private void writeDataToCluster(ICoordinator coordinator, int key, int value)
    {
        // Use coordinator to write with QUORUM - this will replicate properly
        coordinator.execute(String.format("INSERT INTO %s (k, v) VALUES (?, ?)", QUALIFIED_TABLE_NAME), ConsistencyLevel.QUORUM, key, value);
    }

    /**
     * Write multiple keys to establish baseline mutation tracking state
     */
    private void writeDataRange(Cluster cluster, int startKey, int endKey)
    {
        for (int i = startKey; i < endKey; i++)
        {
            writeDataToCluster(cluster, i, i * 10);
        }
    }

    private void flushAllNodes(Cluster cluster)
    {
        for (int i = 1; i <= cluster.size(); i++)
        {
            cluster.get(i).flush(KEYSPACE);
        }
    }

    private void advanceMutationLogSegment(Cluster cluster)
    {
        cluster.stream().filter(node -> !node.isShutdown()).forEach( node -> {
            node.runOnInstance(() -> {
                MutationJournal.instance.advanceSegment();
            });
        });
    }

    private void verifyLocalDataContents(Cluster cluster, int startKey, int endKey)
    {
        // Inline simple iteration over cluster nodes instead of using getAllNodes() helper
        List<IInvokableInstance> nodes = new java.util.ArrayList<>();
        for (int i = 1; i <= cluster.size(); i++)
        {
            nodes.add(cluster.get(i));
        }
        verifyLocalDataContents(Lists.newArrayList(cluster), startKey, endKey);
    }

    private void verifyLocalDataContents(List<IInvokableInstance> nodes, int startKey, int endKey)
    {
        for (int key = startKey; key < endKey; key++)
        {
            int expectedValue = key * 10;
            for (IInvokableInstance node : nodes)
            {
                Object[][] result = node.executeInternal(String.format("SELECT k, v FROM %s WHERE k = ?", QUALIFIED_TABLE_NAME), key);
                assertRows(result, row(key, expectedValue));
            }
        }
    }

    private Set<String> getNodeTokens(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            return metadata.tokenMap.tokens(metadata.myNodeId()).stream()
                          .map(Object::toString)
                          .collect(java.util.stream.Collectors.toSet());
        });
    }
}