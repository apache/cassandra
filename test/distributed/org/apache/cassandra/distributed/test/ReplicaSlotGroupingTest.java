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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.ReplicaSlotGroup;
import org.apache.cassandra.locator.SlotGroupMaps;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToLeaving;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;

/**
 * Topology-related tests for the replica_slot_grouping_enabled feature.
 *
 * Tests bootstrap, replacement, decommission, move, constraint violation fallback,
 * and stale fallback scenarios. Each test compares behavior with feature ENABLED vs DISABLED:
 * - ENABLED: blockFor = slot count, slot needs ALL members to ack
 * - DISABLED: blockFor increases with pending replicas (old behavior)
 */
public class ReplicaSlotGroupingTest extends ReplicaSlotGroupingTestBase
{
    /**
     * Validate computed SlotGroupMaps structure on a given node.
     *
     * @param node              the node to run the validation on
     * @param pkTransitioning   a PK in a range expected to have at least one transitioning slot
     * @param pkStable          a PK in a range expected to have all stable slots
     * @param scenario          description for assertion messages (e.g. "bootstrap", "decommission")
     */
    private void assertSlotGroupStructure(IInvokableInstance node, int pkTransitioning, int pkStable, String scenario)
    {
        final int pkT = pkTransitioning;
        final int pkS = pkStable;
        node.runOnInstance(() -> {
            SlotGroupMaps slotGroupMaps = StorageService.instance.getTokenMetadata().
                getSlotGroupMaps(KEYSPACE);
            Assert.assertNotNull("SlotGroupMaps should not be null during " + scenario, slotGroupMaps);
            Assert.assertFalse("SlotGroupMaps should not be empty during " + scenario, slotGroupMaps.isEmpty());

            Token tTransitioning = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pkT));
            SlotGroupMaps.SlotGroupInfo infoTransitioning = slotGroupMaps.getSlotInfoForToken(tTransitioning);
            Assert.assertNotNull(
                "SlotGroupInfo should exist for transitioning range during " + scenario,
                infoTransitioning);

            boolean hasTransitioning = false;
            for (ReplicaSlotGroup slot : infoTransitioning.endpointToSlot.values())
            {
                if (slot.isTransitioning())
                {
                    hasTransitioning = true;
                    break;
                }
            }
            Assert.assertTrue("Should have at least one transitioning slot during " + scenario, hasTransitioning);

            Token tStable = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pkS));
            SlotGroupMaps.SlotGroupInfo infoStable = slotGroupMaps.getSlotInfoForToken(tStable);
            Assert.assertNotNull("SlotGroupInfo should exist for stable range during " + scenario, infoStable);

            for (ReplicaSlotGroup slot : infoStable.endpointToSlot.values())
            {
                Assert.assertFalse(
                    "Stable range should have no transitioning slots during " + scenario,
                    slot.isTransitioning());
                Assert.assertNotNull(slot.naturalEndpoint());
            }
        });
    }

    /**
     * Test: Bootstrap scenario - compares enabled vs disabled behavior.
     * 
     * Setup: 3-node cluster (RF=3), Node 4 bootstrapping
     * 
     * With evenlyDistributedTokens(4) and RF=3:
     * - When node 4 joins, it takes over a token range
     * - For that range: before={1,2,3}, after={4,1,2}
     * - Node 3 "loses" replica responsibility, node 4 "gains" it
     * - Transitioning slot: {3 (losing), 4 (gaining)}
     * 
     * Slots (enabled): {1}, {2}, {3+4 transitioning}
     * 
     * Tests multiple sub-scenarios:
     * 1. Both nodes 3,4 blocked
     * 2. Happy path: all respond, only node 4 blocked, only node 3 blocked
     * 3. Degraded path: node 2 DOWN, various combinations
     */
    @Test
    public void testSlotGroupingDuringBootstrap() throws Exception
    {
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes + 1);

        BootstrapBB.resetForNodes(4);

        try (Cluster cluster = Cluster.build(numStartNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set("paxos_variant", "v2")).
                withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(
                    numStartNodes + 1, "datacenter1", "rack1")).
                withInstanceInitializer(BootstrapBB::install).
                withTokenSupplier(node -> even.token(node)).
                start())
        {
            IInvokableInstance node1 = cluster.get(1);
            
            fixDistributedSchemas(cluster);
            init(cluster);

            // Create keyspace and table
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + 
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + 
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Enable slot grouping before bootstrap so gossip-triggered calculation includes slot groups
            setSlotGroupingEnabled(cluster, true);

            // Bootstrap node 4 (kept in pending state via ByteBuddy)
            IInvokableInstance node4 = cluster.bootstrap(cluster.newInstanceConfig().
                set("auto_bootstrap", true));
            node4.startup(cluster);

            // Wait for gossip-triggered slot group calculation to complete on coordinator
            node1.logs().watchFor("Slot group calculation for " + KEYSPACE + " completed");

            int pkTransitioning = pkInTransitioningRange(cluster);
            // Range (T4, T1] is stable: replicas={1,2,3} both before and after bootstrap
            int pkStable = pkInRange(cluster.get(4), cluster.get(1));
            assertSlotGroupStructure(node1, pkTransitioning, pkStable, "bootstrap");

            long staleBefore = getStaleFallbacks(node1);
            long cvBefore = getConstraintViolations(node1);

            // === Sub-test 1: Both nodes 3,4 blocked ===
            IMessageFilters.Filter blockNode3 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(3).drop();
            IMessageFilters.Filter blockNode4 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(4).drop();

            // ENABLED: blockFor=2 slots, slots {1} and {2} satisfied -> SUCCESS
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("ENABLED + Nodes 3,4 blocked: QUORUM write + LWT succeeded");

            // DISABLED with same PK: blockFor=3, only 2 acks -> TIMEOUT
            setSlotGroupingEnabled(cluster, false);
            executeWriteExpectingTimeout(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeLwtExpectingTimeout(node1, pkTransitioning);
            logger.info("DISABLED + Nodes 3,4 blocked: QUORUM + LWT timed out as expected");

            blockNode3.off();
            blockNode4.off();

            // === Sub-test 2: Happy path - all respond ===
            setSlotGroupingEnabled(cluster, true);
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("ENABLED + All respond: QUORUM write + LWT succeeded");

            setSlotGroupingEnabled(cluster, false);
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("DISABLED + All respond: QUORUM write + LWT succeeded");

            // === Sub-test 3: Happy path - only node 4 blocked, 1,2,3 respond ===
            blockNode4 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(4).drop();

            setSlotGroupingEnabled(cluster, true);
            // ENABLED: {1} + {2} = 2 slots >= blockFor=2; {3+4} not satisfied but not needed
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("ENABLED + Node 4 blocked: QUORUM write + LWT succeeded");

            setSlotGroupingEnabled(cluster, false);
            // DISABLED: 3 acks >= blockFor=3 -> SUCCESS
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("DISABLED + Node 4 blocked: QUORUM write + LWT succeeded");

            blockNode4.off();

            // === Sub-test 4: Happy path - only node 3 blocked, 1,2,4 respond ===
            blockNode3 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(3).drop();

            setSlotGroupingEnabled(cluster, true);
            // ENABLED: {1} + {2} = 2 slots >= blockFor=2; {3+4} not satisfied but not needed
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("ENABLED + Node 3 blocked: QUORUM write + LWT succeeded");

            setSlotGroupingEnabled(cluster, false);
            // DISABLED: 3 acks >= blockFor=3 -> SUCCESS
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("DISABLED + Node 3 blocked: QUORUM write + LWT succeeded");

            blockNode3.off();

            // === Sub-test 5: Degraded - node 2 DOWN, nodes 1+3+4 respond ===
            IMessageFilters.Filter blockNode2 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(2).drop();

            setSlotGroupingEnabled(cluster, true);
            // ENABLED: slots {1} and {3+4} both satisfied, 2 >= blockFor=2
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("ENABLED + Node 2 blocked: QUORUM write + LWT succeeded");

            setSlotGroupingEnabled(cluster, false);
            // DISABLED: 3 acks >= blockFor=3 -> SUCCESS
            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("DISABLED + Node 2 blocked: QUORUM write + LWT succeeded");

            blockNode2.off();

            // === Sub-test 6: Degraded - node 2 DOWN + node 4 blocked ===
            blockNode2 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(2).drop();
            blockNode4 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(4).drop();

            setSlotGroupingEnabled(cluster, true);
            // ENABLED: slot {3+4} needs both but only 3 acked -> 1 slot satisfied -> TIMEOUT
            executeWriteExpectingTimeout(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            // LWT ENABLED: only slot {1} satisfied (node 1), slot {2} and {3+4} not -> 1 < 2 -> TIMEOUT
            executeLwtExpectingTimeout(node1, pkTransitioning);
            logger.info("ENABLED + Nodes 2,4 blocked: write + LWT both timed out");

            setSlotGroupingEnabled(cluster, false);
            // DISABLED: 2 acks < blockFor=3 -> TIMEOUT
            executeWriteExpectingTimeout(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeLwtExpectingTimeout(node1, pkTransitioning);
            logger.info("DISABLED + Nodes 2,4 blocked: QUORUM + LWT timed out");

            blockNode2.off();
            blockNode4.off();

            // === Sub-test 7: Degraded - node 2 DOWN + node 3 blocked ===
            blockNode2 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(2).drop();
            blockNode3 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(3).drop();

            setSlotGroupingEnabled(cluster, true);
            // ENABLED: slot {3+4} needs both but only 4 acked -> 1 slot satisfied -> TIMEOUT
            executeWriteExpectingTimeout(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            // LWT ENABLED: only slot {1} satisfied, slot {2} and {3+4} not -> 1 < 2 -> TIMEOUT
            executeLwtExpectingTimeout(node1, pkTransitioning);
            logger.info("ENABLED + Nodes 2,3 blocked: write + LWT both timed out");

            setSlotGroupingEnabled(cluster, false);
            // DISABLED: 2 acks < blockFor=3 -> TIMEOUT
            executeWriteExpectingTimeout(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeLwtExpectingTimeout(node1, pkTransitioning);
            logger.info("DISABLED + Nodes 2,3 blocked: QUORUM + LWT timed out");

            blockNode2.off();
            blockNode3.off();

            // === Sub-test 8: Failure path - node 4 genuinely dead + node 3 blocked ===
            // Unlike Sub-tests 1-7 which DROP messages (causing timeouts),
            // this sub-test stops node 4 so that PaxosCommit.start() calls
            // onFailure(node4, NODE_DOWN) IMMEDIATELY via participants.allDown.
            // This exercises slotTracker.recordFailure() and the canSucceed()
            // fast-failure detection path, not just timeout-based failures.
            stopUnchecked(cluster.get(4));
            java.net.InetSocketAddress node4Addr = cluster.get(4).broadcastAddress();

            // Wait for node 1's failure detector to mark node 4 as dead
            for (int attempt = 0; attempt < 60; attempt++)
            {
                boolean alive = node1.callOnInstance(() ->
                    org.apache.cassandra.gms.FailureDetector.instance.isAlive(
                        org.apache.cassandra.locator.InetAddressAndPort.getByAddress(node4Addr)));
                if (!alive)
                    break;
                Thread.sleep(1000);
            }
            logger.info("Node 4 detected as dead by node 1");

            blockNode3 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(3).drop();

            // Use a fresh PK so IF NOT EXISTS triggers a full Paxos commit phase
            int pkFailure = nextPkInRange(cluster.get(3), cluster.get(4), pkTransitioning + 1000);

            // Set on living nodes only (node 4 is stopped)
            setSlotGroupingEnabled(node1, true);
            setSlotGroupingEnabled(cluster.get(2), true);
            setSlotGroupingEnabled(cluster.get(3), true);
            // ENABLED: node 4 dead -> PaxosCommit immediately fails slot {3+4}
            // via recordFailure(). Slots {1} and {2} satisfied -> 2 >= blockFor=2 -> SUCCESS
            executeSuccessfulWrite(node1, pkFailure, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkFailure);
            logger.info("ENABLED + Node 4 dead + Node 3 blocked: write + LWT succeeded (failure path)");

            setSlotGroupingEnabled(node1, false);
            setSlotGroupingEnabled(cluster.get(2), false);
            setSlotGroupingEnabled(cluster.get(3), false);
            // DISABLED: node 4 dead (1 immediate failure) + node 3 blocked (timeout)
            // -> only 2 acks from nodes 1,2, blockFor=3 -> TIMEOUT
            executeWriteExpectingTimeout(node1, pkFailure + 1, ConsistencyLevel.QUORUM);
            executeLwtExpectingTimeout(node1, pkFailure + 1);
            logger.info("DISABLED + Node 4 dead + Node 3 blocked: write + LWT timed out");

            blockNode3.off();

            assertMetricsUnchanged(
                node1, staleBefore, cvBefore, "bootstrap");

            // Clean up
            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }

    /**
     * Test: Replacement scenario - compares enabled vs disabled behavior.
     * 
     * Setup: 3-node cluster (RF=3), Node 2 DOWN, Node 4 replacing
     * Slots (enabled): {1}, {2 DOWN, 4 transitioning}, {3}
     * 
     * Key difference: With ENABLED, QUORUM succeeds regardless of Node 4's response
     * because stable slots 1 and 3 satisfy quorum.
     */
    @Test
    public void testSlotGroupingDuringReplacement() throws Exception
    {
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes);

        BootstrapBB.resetForNodes(4);

        try (Cluster cluster = Cluster.build(numStartNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set("paxos_variant", "v2")).
                withDynamicPortAllocation(false).
                withRacks(1, 3, numStartNodes).
                withInstanceInitializer(BootstrapBB::install).
                withTokenSupplier(node -> even.token(node == (numStartNodes + 1) ? 2 : node)).
                start())
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);
            
            fixDistributedSchemas(cluster);
            init(cluster);

            // Create keyspace and table
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + 
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + 
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Stop node 2
            stopUnchecked(nodeToRemove);

            // Enable slot grouping on alive nodes before replacement
            setSlotGroupingEnabled(node1, true);
            setSlotGroupingEnabled(node3, true);

            long staleBefore = getStaleFallbacks(node1);
            long cvBefore = getConstraintViolations(node1);

            // Start node 4 as replacement for node 2
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove, props -> {
                props.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
            });

            // Wait for gossip-triggered slot group calculation to complete on coordinator
            node1.logs().watchFor("Slot group calculation for " + KEYSPACE + " completed");

            // Find a pk in a range where node 4 (replacing node 2) is pending
            int pk = pkInRange(cluster.get(1), nodeToRemove);

            // Block messages to replacement node to simulate it being busy
            IMessageFilters.Filter blockReplacement = cluster.filters().
                inbound().
                verbs(BLOCK_NODE_VERBS).
                to(4).
                drop();

            // Replacement blocked → QUORUM should SUCCESS (stable slots 1, 3 satisfy)
            executeSuccessfulWrite(node1, pk, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pk);
            logger.info("ENABLED + Replacement blocked: QUORUM write + LWT succeeded");

            // TEST WITH FEATURE DISABLED
            setSlotGroupingEnabled(node1, false);
            setSlotGroupingEnabled(node3, false);

            // Replacement blocked → QUORUM should FAIL (blockFor=3, only 2 acks)
            executeWriteExpectingTimeout(node1, pk, ConsistencyLevel.QUORUM);
            // LWT DISABLED: node 2 dead + node 4 blocked -> only 2 Paxos responses, quorum(3) not met
            executeLwtExpectingTimeout(node1, pk);
            logger.info("DISABLED + Replacement blocked: write + LWT timed out");

            assertMetricsUnchanged(
                node1, staleBefore, cvBefore,
                "replacement");

            // Clean up
            blockReplacement.off();
            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }

    /**
     * Test: Decommission scenario - compares enabled vs disabled behavior.
     * 
     * Setup: 4-node cluster (RF=3), Node 2 decommissioning (in leaving state)
     * During decommission, Node 2's ranges are being transferred to Node 4.
     * Slots (enabled): {1}, {2 leaving + 4 gaining}, {3}
     * 
     * To test the difference, we block BOTH nodes in the transitioning slot (2 and 4).
     * - ENABLED: blockFor=2 slots, slots {1} and {3} satisfied → SUCCESS
     * - DISABLED: blockFor=3 (base quorum + pending), only 2 acks from nodes 1,3 → TIMEOUT
     */
    @Test
    public void testSlotGroupingDuringDecommission() throws Exception
    {
        int numNodes = 4;
        
        try (Cluster cluster = Cluster.build(numNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set("paxos_variant", "v2")).
                withRacks(1, 1, numNodes).
                start())
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);  // Will be set to "leaving" state
            
            fixDistributedSchemas(cluster);
            init(cluster);

            // Create keyspace and table
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + 
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + 
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Enable slot grouping before topology change
            setSlotGroupingEnabled(cluster, true);

            long staleBefore = getStaleFallbacks(node1);
            long cvBefore = getConstraintViolations(node1);

            // Set node 2 to "leaving" state via gossip
            cluster.forEach(statusToLeaving(node2));

            // Wait for gossip-triggered slot group calculation to complete on coordinator
            node1.logs().watchFor("Slot group calculation for " + KEYSPACE + " completed");

            // Use a key in range (T4, T1] where node 4 is the pending replica
            // gaining from node 2 leaving.  Before={1,2,3}, after={1,3,4}.
            // Blocking nodes 2 and 4 leaves nodes 1 and 3 to ack.
            int pkDecom = pkInRange(cluster.get(4), cluster.get(1));

            IMessageFilters.Filter blockNode2 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(2).drop();
            IMessageFilters.Filter blockNode4 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(4).drop();

            // Range (T2, T3] is stable: replicas={3,4,1}, node 2 is not a replica so leaving has no effect
            int pkStable = pkInRange(cluster.get(2), cluster.get(3));
            assertSlotGroupStructure(node1, pkDecom, pkStable, "decommission");

            executeSuccessfulWrite(node1, pkDecom, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkDecom);
            logger.info("ENABLED + Nodes 2,4 blocked: QUORUM write + LWT succeeded");

            // TEST WITH FEATURE DISABLED (same PK)
            setSlotGroupingEnabled(cluster, false);

            executeWriteExpectingTimeout(node1, pkDecom, ConsistencyLevel.QUORUM);
            // LWT DISABLED: only 2 Paxos responses from nodes 1,3. Quorum(3) not met
            executeLwtExpectingTimeout(node1, pkDecom);
            logger.info("DISABLED + Nodes 2,4 blocked: write + LWT timed out");

            // Clean up filters and test with all nodes responding
            blockNode2.off();
            blockNode4.off();

            // TEST WITH ALL RESPONDING - should succeed in both modes
            setSlotGroupingEnabled(cluster, true);
            executeSuccessfulWrite(node1, 300, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, 300);
            logger.info("ENABLED + All respond: QUORUM write + LWT succeeded");
            
            setSlotGroupingEnabled(cluster, false);
            executeSuccessfulWrite(node1, 400, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, 400);
            logger.info("DISABLED + All respond: QUORUM write + LWT succeeded");

            assertMetricsUnchanged(
                node1, staleBefore, cvBefore,
                "decommission");
        }
    }

    /**
     * Test: Move scenario - compares enabled vs disabled behavior.
     *
     * Setup: 4-node cluster (RF=3), Node 2 moving to a new token between T3 and T4.
     * During move, node 2 gains replica responsibility for range (T2, T3]:
     *   Before={3,4,1}, After={3,2,4}. Node 2 is pending (gaining), node 1 is losing.
     * Slots (enabled): {3}, {4}, {1 losing + 2 gaining}
     *
     * To test the difference, we block BOTH nodes in the transitioning slot (1 and 2).
     * Use node 3 as coordinator (natural replica, avoids local ack bypassing filters on blocked nodes).
     * - ENABLED: blockFor=2 slots, slots {3} and {4} satisfied → SUCCESS
     * - DISABLED: blockFor=3, only 2 acks from nodes 3,4 → TIMEOUT
     */
    @Test
    public void testSlotGroupingDuringMove() throws Exception
    {
        int numNodes = 4;

        try (Cluster cluster = Cluster.build(numNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set("paxos_variant", "v2")).
                withRacks(1, 1, numNodes).
                start())
        {
            IInvokableInstance node3 = cluster.get(3);

            fixDistributedSchemas(cluster);
            init(cluster);

            // Create keyspace and table
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE +
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Enable slot grouping before topology change
            setSlotGroupingEnabled(cluster, true);

            long staleBefore = getStaleFallbacks(node3);
            long cvBefore = getConstraintViolations(node3);

            // Set node 2 to MOVING state
            // We add the moving endpoint directly on the coordinator's (node 3) TokenMetadata
            // so it computes pending ranges and slot groups correctly.
            node3.runOnInstance(() -> {
                java.util.List<Token> sorted = new java.util.ArrayList<>(
                    StorageService.instance.getTokenMetadata().
                        sortedTokens());
                Token node2Token = sorted.get(1);
                org.apache.cassandra.locator.InetAddressAndPort node2Ep =
                    StorageService.instance.getTokenMetadata().getEndpoint(node2Token);

                long t3Val = Long.parseLong(sorted.get(2).toString());
                long t4Val = Long.parseLong(sorted.get(3).toString());
                long midVal = t3Val + (t4Val - t3Val) / 2;
                Token newToken = Murmur3Partitioner.instance.getTokenFactory().
                    fromString(Long.toString(midVal));

                StorageService.instance.getTokenMetadata().
                    addMovingEndpoint(newToken, node2Ep);
            });

            // Moving state is added directly to TokenMetadata (not via gossip),
            // so we must trigger pending range calculation manually.
            node3.runOnInstance(() -> {
                PendingRangeCalculatorService.instance.update();
                PendingRangeCalculatorService.instance.blockUntilFinished();
            });

            // Use key in range (T2, T3] where node 2 becomes pending (replacing node 1).
            // Before: replicas={3,4,1}. After (node 2 at new position): replicas={3,2,4}.
            int pkMove = pkInRange(cluster.get(2), cluster.get(3));

            // Block BOTH nodes in the transitioning slot: node 1 (losing) and node 2 (gaining).
            IMessageFilters.Filter blockNode1 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(1).drop();
            IMessageFilters.Filter blockNode2 = cluster.filters().
                inbound().verbs(BLOCK_NODE_VERBS).to(2).drop();

            // Range (T4, T1] is stable: replicas={1,2,3}, unaffected by node 2's move
            int pkStable = pkInRange(cluster.get(4), cluster.get(1));
            assertSlotGroupStructure(node3, pkMove, pkStable, "move");

            // Nodes 1,2 blocked -> QUORUM should SUCCESS
            // Slots {3} and {4} satisfied by coordinator (node 3) and node 4, meeting blockFor=2
            executeSuccessfulWrite(node3, pkMove, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node3, pkMove);
            logger.info("ENABLED + Nodes 1,2 blocked: QUORUM write + LWT succeeded");

            // TEST WITH FEATURE DISABLED (same PK)
            setSlotGroupingEnabled(cluster, false);

            // Nodes 1,2 blocked -> QUORUM should FAIL
            // blockFor=3 (base quorum 2 + 1 pending), only 2 acks from nodes 3,4
            executeWriteExpectingTimeout(node3, pkMove, ConsistencyLevel.QUORUM);
            // LWT DISABLED: only 2 Paxos responses from nodes 3,4. Quorum(3) not met
            executeLwtExpectingTimeout(node3, pkMove);
            logger.info("DISABLED + Nodes 1,2 blocked: write + LWT timed out");

            assertMetricsUnchanged(
                node3, staleBefore, cvBefore, "move");
            
            // Clean up
            blockNode1.off();
            blockNode2.off();
        }
    }

    /**
     * Test: Constraint violation fallback when two nodes bootstrap simultaneously.
     *
     * Setup: 3-node cluster (RF=3), Node 4 and Node 5 both bootstrapping (kept in pending
     * state via BootstrapBB with targetNodes={4,5}).
     *
     * With evenlyDistributedTokens(5) and RF=3, before = 3-node ring (all ranges → {1,2,3}):
     * - Node 4 bootstrap at T4: for token T2, replicas change from {1,2,3} to {3,4,1},
     *   so node 4 is pending (replacing node 2).
     * - Node 5 bootstrap at T5: for token T2, replicas change from {1,2,3} to {3,5,1},
     *   so node 5 is pending (also replacing node 2).
     * - Token T2 now has two pending operations → CONSTRAINT VIOLATION.
     *
     * Expected: Slot group calculation detects the violation, falls back to existing behavior
     * (no slot groups computed). Writes still succeed when all natural replicas respond.
     */
    @Test
    public void testSlotGroupingConstraintViolationFallback() throws Exception
    {
        int numStartNodes = 3;
        int totalNodes = 5;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(totalNodes);

        BootstrapBB.resetForNodes(4, 5);

        try (Cluster cluster = Cluster.build(numStartNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set(
                    "paxos_variant", "v2").set(
                    "skip_paxos_repair_on_topology_change", true)).
                withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(
                    totalNodes, "datacenter1", "rack1")).
                withInstanceInitializer(BootstrapBB::install).
                withTokenSupplier(node -> even.token(node)).
                start())
        {
            IInvokableInstance node1 = cluster.get(1);

            fixDistributedSchemas(cluster);
            init(cluster);

            // Create keyspace and table
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE +
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Enable slot grouping before bootstraps
            setSlotGroupingEnabled(cluster, true);

            long staleBefore = getStaleFallbacks(node1);
            long cvBefore = getConstraintViolations(node1);

            // Bootstrap node 4 (kept in pending state via BootstrapBB)
            IInvokableInstance node4 = cluster.bootstrap(cluster.newInstanceConfig().
                set("auto_bootstrap", true));
            node4.startup(cluster);

            // Bootstrap node 5 (also kept in pending state via BootstrapBB)
            // With both nodes 4 and 5 bootstrapping, their pending ranges overlap at
            // the same token boundary (e.g., T2), triggering the constraint violation.
            IInvokableInstance node5 = cluster.bootstrap(cluster.newInstanceConfig().
                set("auto_bootstrap", true));
            node5.startup(cluster);

            // Wait for constraint violation to be detected on coordinator
            node1.logs().watchFor("Slot group calculation for " + KEYSPACE + " failed due to constraint violation");

            // Verify: slot groups should be null due to constraint violation.
            // getSlotInfoForToken returns null when slotGroupMaps has no entry for
            // this keyspace (removed by the constraint violation fallback path).
            node1.runOnInstance(() -> {
                Token t = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(1));
                if (StorageService.instance.getTokenMetadata().
                        getSlotInfoForToken(t, "replica_slot_grouping_test") != null)
                {
                    throw new RuntimeException(
                        "Slot groups should be null due to constraint violation, but slot info was found");
                }
            });
            logger.info(
                "Constraint violation correctly detected:" +
                " slot groups cleared (fallback to standard behavior)");

            // Verify ConstraintViolations incremented and
            // StaleFallbacks unchanged
            long cvAfter = getConstraintViolations(node1);
            Assert.assertTrue(
                "ConstraintViolations should increment," +
                " before=" + cvBefore + " after=" + cvAfter,
                cvAfter > cvBefore);
            Assert.assertEquals(
                "StaleFallbacks should not change during" +
                " constraint violation fallback",
                staleBefore, getStaleFallbacks(node1));
            logger.info(
                "Metrics verified: ConstraintViolations" +
                " {}->{}; StaleFallbacks unchanged={}",
                cvBefore, cvAfter, staleBefore);

            // Sanity check: writes still succeed with all natural replicas
            executeSuccessfulWrite(node1, 1, ConsistencyLevel.QUORUM);
            logger.info(
                "Constraint violation fallback + all responding:" +
                " QUORUM succeeded");

            // Clean up
            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }

    /**
     * Test: Stale fallback when slot groups don't cover all contacts.
     *
     * Setup: 3-node cluster (RF=3), node 4 bootstrapping (kept in pending state).
     * After slot groups are computed for the 4-node topology, a fake 5th bootstrap
     * endpoint is added to TokenMetadata and pending ranges are recalculated (without
     * recalculating slot groups). This creates a stale condition: the write contacts
     * include the fake node 5 (as a pending replica), but the slot groups only know
     * about nodes {1,2,3,4}.
     *
     * Expected: The stale detection in getValidatedSlotInfo finds node 5 not in
     * endpointToSlot, increments StaleFallbacks, and falls back to standard write
     * behavior. The write succeeds because enough real nodes respond.
     */
    @Test
    public void testSlotGroupingStaleFallback() throws Exception
    {
        int numStartNodes = 3;
        TokenSupplier even =
            TokenSupplier.evenlyDistributedTokens(numStartNodes + 1);

        BootstrapBB.resetForNodes(4);

        try (Cluster cluster = Cluster.build(numStartNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set(
                    "paxos_variant", "v2").set(
                    "skip_paxos_repair_on_topology_change", true)).
                withNodeIdTopology(NetworkTopology.
                    singleDcNetworkTopology(
                        numStartNodes + 1,
                        "datacenter1", "rack1")).
                withInstanceInitializer(BootstrapBB::install).
                withTokenSupplier(node -> even.token(node)).
                start())
        {
            IInvokableInstance node1 = cluster.get(1);

            fixDistributedSchemas(cluster);
            init(cluster);

            cluster.schemaChange(
                "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class':" +
                " 'SimpleStrategy'," +
                " 'replication_factor': 3};");
            cluster.schemaChange(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE +
                ".tbl (pk int, ck int, v int," +
                " PRIMARY KEY (pk, ck))");

            setSlotGroupingEnabled(cluster, true);

            // Bootstrap node 4 (kept in pending state)
            IInvokableInstance node4 =
                cluster.bootstrap(cluster.newInstanceConfig().
                    set("auto_bootstrap", true));
            node4.startup(cluster);

            node1.logs().watchFor(
                "Slot group calculation for " +
                KEYSPACE + " completed");

            long staleBefore = getStaleFallbacks(node1);
            long cvBefore = getConstraintViolations(node1);

            // Add a fake 5th bootstrap endpoint to the coordinator's TokenMetadata, then recalculate
            // only pending ranges (NOT slot groups). We call TokenMetadata.calculatePendingRanges()
            // directly instead of PendingRangeCalculatorService.update(), because the latter also
            // recalculates slot groups — which would include the fake node and defeat the stale test.
            // Returns a pk guaranteed to be in the fake node's pending range.
            int stalePk = node1.callOnInstance(() -> {
                org.apache.cassandra.locator.TokenMetadata tm =
                    StorageService.instance.getTokenMetadata();
                java.util.List<Token> sorted = new java.util.ArrayList<>(tm.sortedTokens());

                long t1Val = Long.parseLong(sorted.get(0).toString());
                long t2Val = Long.parseLong(sorted.get(1).toString());
                long midVal = t1Val + (t2Val - t1Val) / 2;
                Token fakeToken = Murmur3Partitioner.instance.getTokenFactory().fromString(Long.toString(midVal));

                org.apache.cassandra.locator.InetAddressAndPort fakeEp;
                try
                {
                    fakeEp = org.apache.cassandra.locator.InetAddressAndPort.getByNameOverrideDefaults(
                        "127.0.0.100", 7012);
                }
                catch (java.net.UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                tm.addBootstrapToken(fakeToken, fakeEp);

                // Recalculate ONLY pending ranges, not slot groups
                org.apache.cassandra.locator.AbstractReplicationStrategy strategy =
                    org.apache.cassandra.db.Keyspace.open(KEYSPACE).getReplicationStrategy();
                tm.calculatePendingRanges(strategy, KEYSPACE);

                // Find a pk whose token falls in (sorted[0], fakeToken] — the fake node is the
                // primary replica for this range in the "after" topology, so it will be a pending
                // replica and trigger the stale detection in getValidatedSlotInfo.
                Token lbToken = sorted.get(0);
                for (int pk = 0; pk < 2_000_000; pk++)
                {
                    Token pkt = Murmur3Partitioner.instance.getToken(
                        org.apache.cassandra.db.marshal.Int32Type.instance.decompose(pk));
                    if (lbToken.compareTo(pkt) < 0 && fakeToken.compareTo(pkt) >= 0)
                        return pk;
                }
                throw new RuntimeException("Could not find pk in fake node's pending range");
            });

            // Write should succeed: stale detection falls back to standard behavior
            executeSuccessfulWrite(node1, stalePk, ConsistencyLevel.QUORUM);
            logger.info("ENABLED + Stale slot groups: write succeeded via fallback");

            // Verify StaleFallbacks incremented
            long staleAfter = getStaleFallbacks(node1);
            Assert.assertTrue(
                "StaleFallbacks should increment, before=" +
                staleBefore + " after=" + staleAfter,
                staleAfter > staleBefore);
            Assert.assertEquals(
                "ConstraintViolations should not change" +
                " during stale fallback",
                cvBefore, getConstraintViolations(node1));
            logger.info(
                "Metrics: StaleFallbacks {}->{};" +
                " ConstraintViolations unchanged={}",
                staleBefore, staleAfter, cvBefore);

            // Clean up
            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }
}
