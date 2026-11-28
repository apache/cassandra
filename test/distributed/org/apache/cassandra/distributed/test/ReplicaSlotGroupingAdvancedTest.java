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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.service.paxos.PaxosCommitAndPrepare;
import org.apache.cassandra.service.paxos.PaxosPrepare;
import org.apache.cassandra.service.paxos.PaxosPropose;
import org.apache.cassandra.utils.Clock;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Advanced slot grouping tests: ack permutations, failure reply handling, and Paxos-specific
 * slot tracker scenarios (reset during PREPARE, lowBound reclassification).
 */
public class ReplicaSlotGroupingAdvancedTest extends ReplicaSlotGroupingTestBase
{
    /**
     * ByteBuddy helper to inject failure responses from verb handlers.
     * When {@link #injectFailures} is true, intercepted handlers throw RuntimeException,
     * which InboundSink catches and converts to FAILURE_RSP (RequestFailureReason.UNKNOWN).
     * This exercises the failure-reply code paths (SlotResponseTracker.recordFailure,
     * canSucceed, canSucceedWithFailed) rather than the timeout-based paths.
     */
    public static class FailureInjectionBB
    {
        public static final AtomicBoolean injectFailures = new AtomicBoolean(false);
        public static final Set<Integer> targetNodes = new HashSet<>(Arrays.asList(3, 4));

        public static void resetForNodes(Integer... nodes)
        {
            targetNodes.clear();
            targetNodes.addAll(Arrays.asList(nodes));
            injectFailures.set(false);
        }

        /**
         * Set injectFailures on each target node via runOnInstance.
         * Each node has its own class loader and thus its own copy of static fields;
         * setting from the test context only affects the test's copy.
         */
        public static void setInjectFailures(Cluster cluster, boolean value)
        {
            for (int nodeNum : targetNodes)
            {
                IInvokableInstance inst = cluster.get(nodeNum);
                inst.runOnInstance(() -> injectFailures.set(value));
            }
        }

        public static void install(ClassLoader cl, Integer nodeNum)
        {
            if (!targetNodes.contains(nodeNum))
                return;

            Class<?>[] targets = {
                MutationVerbHandler.class,
                PaxosPrepare.RequestHandler.class,
                PaxosPropose.RequestHandler.class,
                PaxosCommit.RequestHandler.class,
                PaxosCommitAndPrepare.RequestHandler.class
            };
            for (Class<?> target : targets)
            {
                new ByteBuddy().rebase(target).method(named("doVerb")).
                    intercept(MethodDelegation.to(FailureInjectionBB.class)).
                    make().load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void doVerb(@SuperCall Callable<Void> zuper) throws Exception
        {
            if (injectFailures.get())
                throw new RuntimeException("Injected failure for slot grouping test");
            zuper.call();
        }
    }

    /**
     * Test case data for permutation testing.
     */
    private static class PermutationTestCase
    {
        final boolean aResponds;
        final boolean bResponds;
        final boolean cResponds;
        final boolean dResponds;
        final boolean expectEnabledSuccess;
        final boolean expectDisabledSuccess;

        PermutationTestCase(boolean aResponds, boolean bResponds, boolean cResponds, boolean dResponds,
                           boolean expectEnabledSuccess, boolean expectDisabledSuccess)
        {
            this.aResponds = aResponds;
            this.bResponds = bResponds;
            this.cResponds = cResponds;
            this.dResponds = dResponds;
            this.expectEnabledSuccess = expectEnabledSuccess;
            this.expectDisabledSuccess = expectDisabledSuccess;
        }
    }

    /**
     * Get all 10 permutation test cases.
     * 
     * Slots: {A}, {B}, {C, D transitioning}
     * blockFor = 2 for QUORUM with RF=3
     */
    private List<PermutationTestCase> getPermutationTestCases()
    {
        return Arrays.asList(
            //                           A      B      C      D      enabled  disabled
            new PermutationTestCase(true,  true,  true,  true,  true,    true),   // All respond
            new PermutationTestCase(true,  true,  true,  false, true,    true),   // D busy
            new PermutationTestCase(true,  true,  false, true,  true,    true),   // C busy
            new PermutationTestCase(true,  true,  false, false, true,    false),  // C,D busy - KEY DIFFERENCE!
            new PermutationTestCase(true,  false, true,  true,  true,    true),   // B busy
            new PermutationTestCase(false, true,  true,  true,  true,    true),   // A busy
            new PermutationTestCase(true,  false, true,  false, false,   false),  // B,D busy
            new PermutationTestCase(false, true,  true,  false, false,   false),  // A,D busy
            new PermutationTestCase(true,  false, false, true,  false,   false),  // B,C busy
            new PermutationTestCase(false, true,  false, true,  false,   false)   // A,C busy
        );
    }

    /**
     * Test all 10 meaningful ack permutations for 3 slots {A}, {B}, {C, D transitioning}.
     * For each permutation, block non-responding nodes via message filters, then verify that
     * the write + LWT succeeds or times out as expected with feature ENABLED and DISABLED.
     * Tests all permutations with BOTH feature enabled and disabled.
     */
    @Test
    public void testSlotGroupingAckPermutations() throws Exception
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
            IInvokableInstance nodeA = cluster.get(1);  // Slot A
            IInvokableInstance nodeB = cluster.get(2);  // Slot B
            IInvokableInstance nodeC = cluster.get(3);  // Slot C (natural in transitioning slot)
            
            fixDistributedSchemas(cluster);
            init(cluster);

            // Create keyspace and table
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + 
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + 
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            long staleBefore = getStaleFallbacks(nodeA);
            long cvBefore = getConstraintViolations(nodeA);

            // Bootstrap node D (kept in pending state)
            IInvokableInstance nodeD = cluster.bootstrap(cluster.newInstanceConfig().
                set("auto_bootstrap", true));
            nodeD.startup(cluster);

            // Find keys in the transitioning range (T3, T4] where node D is pending
            int basePk = pkInTransitioningRange(cluster);

            // Run all permutation test cases
            List<PermutationTestCase> testCases = getPermutationTestCases();
            int pkCounter = 0;

            for (PermutationTestCase tc : testCases)
            {
                // Pick a coordinator that IS responding AND is a natural replica (nodes 1,2,3).
                // The coordinator processes its local mutation directly (not through the network),
                // so inbound MUTATION_REQ filters do NOT block the coordinator's own ack.
                // The coordinator must NOT be a node we want to simulate as "blocked".
                IInvokableInstance coordinator;
                if (tc.aResponds)
                    coordinator = nodeA;
                else if (tc.bResponds)
                    coordinator = nodeB;
                else
                    coordinator = nodeC;

                logger.info(
                    "Testing permutation: A={} B={} C={} D={}, expectEnabled={}, expectDisabled={}, coordinator={}",
                    tc.aResponds, tc.bResponds, tc.cResponds, tc.dResponds,
                    tc.expectEnabledSuccess, tc.expectDisabledSuccess,
                    coordinator == nodeA ? "A" : (coordinator == nodeB ? "B" : "C"));

                // Set up message filters based on which nodes should NOT respond
                List<IMessageFilters.Filter> filters = new ArrayList<>();
                if (!tc.aResponds)
                    filters.add(cluster.filters().inbound().verbs(BLOCK_NODE_VERBS).to(1).drop());
                if (!tc.bResponds)
                    filters.add(cluster.filters().inbound().verbs(BLOCK_NODE_VERBS).to(2).drop());
                if (!tc.cResponds)
                    filters.add(cluster.filters().inbound().verbs(BLOCK_NODE_VERBS).to(3).drop());
                if (!tc.dResponds)
                    filters.add(cluster.filters().inbound().verbs(BLOCK_NODE_VERBS).to(4).drop());

                // Use the SAME PK for both enabled and disabled (A/B comparison)
                int pk = nextPkInRange(cluster.get(3), cluster.get(4), basePk + pkCounter);
                pkCounter++;

                // TEST WITH FEATURE ENABLED
                setSlotGroupingEnabled(cluster, true);
                coordinator.runOnInstance(() -> {
                    PendingRangeCalculatorService.instance.update();
                    PendingRangeCalculatorService.instance.blockUntilFinished();
                });

                if (tc.expectEnabledSuccess)
                {
                    executeSuccessfulWrite(coordinator, pk, ConsistencyLevel.QUORUM);
                    executeSuccessfulLwt(coordinator, pk);
                    logger.info("  ENABLED: write + LWT SUCCESS as expected");
                }
                else
                {
                    executeWriteExpectingTimeout(coordinator, pk, ConsistencyLevel.QUORUM);
                    executeLwtExpectingTimeout(coordinator, pk);
                    logger.info("  ENABLED: write + LWT TIMEOUT as expected");
                }

                // TEST WITH FEATURE DISABLED (same PK for direct A/B comparison)
                setSlotGroupingEnabled(cluster, false);

                if (tc.expectDisabledSuccess)
                {
                    executeSuccessfulWrite(coordinator, pk, ConsistencyLevel.QUORUM);
                    executeSuccessfulLwt(coordinator, pk);
                    logger.info("  DISABLED: write + LWT SUCCESS as expected");
                }
                else
                {
                    executeWriteExpectingTimeout(coordinator, pk, ConsistencyLevel.QUORUM);
                    executeLwtExpectingTimeout(coordinator, pk);
                    logger.info("  DISABLED: write + LWT TIMEOUT as expected");
                }

                // Clean up filters
                filters.forEach(IMessageFilters.Filter::off);
            }

            assertMetricsUnchanged(
                nodeA, staleBefore, cvBefore,
                "ack permutations");

            // Clean up
            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }

    /**
     * Test: Failure reply scenario - nodes reply with FAILURE_RSP instead of dropping messages.
     *
     * Unlike the timeout-based tests which DROP messages (causing the coordinator to wait until
     * RPC timeout), this test uses ByteBuddy to make verb handlers on nodes 3 and 4 throw
     * RuntimeException. InboundSink catches the exception and sends an immediate FAILURE_RSP
     * (with RequestFailureReason.UNKNOWN) back to the coordinator.
     *
     * This exercises the failure-specific code paths in SlotResponseTracker:
     * - recordFailure(from): marks a node as failed in its slot
     * - canSucceed(required): checks if enough slots can still be satisfied
     * - canSucceedWithFailed(failedCount, required): used in PaxosCommit
     *
     * Setup: 3-node cluster (RF=3), Node 4 bootstrapping (kept pending via BootstrapBB)
     * Slots: {1}, {2}, {3+4 transitioning}
     * Failure injection: nodes 3 and 4 (entire transitioning slot fails)
     *
     * - ENABLED: Slots {1} and {2} satisfied by nodes 1,2 → 2 >= blockFor=2 → SUCCESS
     * - DISABLED: Only 2 acks from nodes 1,2. blockFor=3 → FAILURE (WriteFailureException)
     */
    @Test
    public void testSlotGroupingWithFailureReplies() throws Exception
    {
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes + 1);

        BootstrapBB.resetForNodes(4);
        FailureInjectionBB.resetForNodes(3, 4);

        try (Cluster cluster = Cluster.build(numStartNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set(
                    "paxos_variant", "v2").set(
                    "skip_paxos_repair_on_topology_change", true)).
                withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(
                    numStartNodes + 1, "datacenter1", "rack1")).
                withInstanceInitializer((cl, nodeNum) -> {
                    BootstrapBB.install(cl, nodeNum);
                    FailureInjectionBB.install(cl, nodeNum);
                }).
                withTokenSupplier(node -> even.token(node)).
                start())
        {
            cluster.setUncaughtExceptionsFilter((nodeNum, throwable) ->
                throwable.getMessage() != null
                && throwable.getMessage().contains("Injected failure for slot grouping test"));

            IInvokableInstance node1 = cluster.get(1);

            fixDistributedSchemas(cluster);
            init(cluster);

            // Create keyspace and table
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE +
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Enable slot grouping before bootstrap
            setSlotGroupingEnabled(cluster, true);

            // Bootstrap node 4 (kept in pending state via BootstrapBB)
            IInvokableInstance node4 = cluster.bootstrap(cluster.newInstanceConfig().
                set("auto_bootstrap", true));
            node4.startup(cluster);

            // Wait for slot group calculation
            node1.logs().watchFor("Slot group calculation for " + KEYSPACE + " completed");

            int pkTransitioning = pkInTransitioningRange(cluster);

            // === Test 1: Failure injection + slot grouping ENABLED → SUCCESS ===
            // Nodes 3 and 4 reply with FAILURE_RSP (immediate, not timeout).
            // Slots {1} and {2} are satisfied by nodes 1 and 2 → 2 >= blockFor=2 → SUCCESS
            FailureInjectionBB.setInjectFailures(cluster, true);

            executeSuccessfulWrite(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkTransitioning);
            logger.info("ENABLED + Failure injection on nodes 3,4: write + LWT succeeded");

            // === Test 2: Failure injection + slot grouping DISABLED → FAILURE ===
            // With slot grouping disabled, blockFor=3 (inflated for pending replica).
            // Only 2 acks from nodes 1,2, with 2 failures → cannot reach quorum.
            setSlotGroupingEnabled(cluster, false);

            executeWriteExpectingFailureOrTimeout(node1, pkTransitioning, ConsistencyLevel.QUORUM);
            executeLwtExpectingFailureOrTimeout(node1, pkTransitioning);
            logger.info("DISABLED + Failure injection on nodes 3,4: write + LWT failed as expected");

            // === Test 3: Disable failure injection → recovery, both modes succeed ===
            FailureInjectionBB.setInjectFailures(cluster, false);

            setSlotGroupingEnabled(cluster, true);
            int pkRecovery = nextPkInRange(cluster.get(3), cluster.get(4), pkTransitioning + 1000);
            executeSuccessfulWrite(node1, pkRecovery, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkRecovery);
            logger.info("ENABLED + No injection: write + LWT succeeded after recovery");

            setSlotGroupingEnabled(cluster, false);
            executeSuccessfulWrite(node1, pkRecovery, ConsistencyLevel.QUORUM);
            executeSuccessfulLwt(node1, pkRecovery);
            logger.info("DISABLED + No injection: write + LWT succeeded after recovery");

            // Clean up
            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }

    /**
     * Test: Paxos PREPARE committed-state divergence triggers SlotResponseTracker.reset(),
     * and the refresh path applies the missed committed mutation to the stale node.
     *
     * When a PREPARE response carries a newer latestCommitted (AFTER case), all previous
     * "withLatest" acks are invalidated and withLatestSlots.reset() is called. With node 1
     * blocked, only 3 nodes participate. The slot layout is:
     *   Slot 1: Node 1 (stable, BLOCKED)
     *   Slot 2: Node 2 (stable)
     *   Slot 3: Node 3 + Node 4 (transitioning)
     *
     * With reset(): after AFTER fires, withLatestSlots has only node 4 (Slot 3: 1/2) and
     * node 2 (Slot 2: 1/1) → satisfiedCount=1 < quorum(2) → refreshStaleParticipants()
     * sends the missed commit to node 3 → state.commit() applies the mutation locally.
     *
     * Without reset(): node 3's stale ack remains → Slot 3 has 2/2 (stale node3 + node4)
     * → satisfiedCount=2 >= quorum → fast path, no refresh → node 3 never gets the missed
     * mutation applied to its local storage.
     *
     * The test asserts on node 3's local data via executeInternalWithResult (bypassing
     * coordinator/CL). The missed LWT wrote v2=200; if reset() works, node 3 has v2=200
     * (from refresh). If reset() is broken, node 3 still has v2=100 (stale).
     */
    @Test
    public void testSlotTrackerResetDuringPaxosPrepare() throws Exception
    {
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes + 1);

        BootstrapBB.resetForNodes(4);

        try (Cluster cluster = Cluster.build(numStartNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set(
                    "paxos_variant", "v2").set(
                    "skip_paxos_repair_on_topology_change", true)).
                withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(
                    numStartNodes + 1, "datacenter1", "rack1")).
                withInstanceInitializer(BootstrapBB::install).
                withTokenSupplier(node -> even.token(node)).
                start())
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node3 = cluster.get(3);

            fixDistributedSchemas(cluster);
            init(cluster);

            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE +
                ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            setSlotGroupingEnabled(cluster, true);

            IInvokableInstance node4 = cluster.bootstrap(cluster.newInstanceConfig().
                set("auto_bootstrap", true));
            node4.startup(cluster);

            node1.logs().watchFor("Slot group calculation for " + KEYSPACE + " completed");

            int pk = pkInTransitioningRange(cluster);

            // Seed the row
            node1.coordinator().execute(
                "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                ConsistencyLevel.QUORUM, pk, 1, 0, 0);

            // LWT1: set v2=100 on all nodes (establishes baseline committed state)
            node1.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v2 = 100 WHERE pk = ? AND ck = ? IF v1 = 0",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
            logger.info("LWT1 committed v2=100 on all nodes");

            // Drop commit messages inbound to node 3
            int[] commitVerbs = {
                Verb.PAXOS_COMMIT_REQ.id,
                Verb.PAXOS2_COMMIT_AND_PREPARE_REQ.id
            };
            IMessageFilters.Filter commitFilter = cluster.filters().inbound().to(3).
                verbs(commitVerbs).drop();

            // LWT2: set v2=200 — commits on 1,2,4 but node 3 misses it (still has v2=100)
            node1.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v2 = 200 WHERE pk = ? AND ck = ? IF v1 = 0",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
            logger.info("LWT2 committed v2=200; node 3 missed the commit (has v2=100)");

            commitFilter.off();

            // Block node 1 so only nodes 2, 3, 4 participate in the next LWT.
            // This makes the refresh path necessary: with reset(), only Slot 2 (node 2) is
            // satisfied → satisfiedCount=1 < 2 → refresh fires → node 3 gets v2=200.
            IMessageFilters.Filter blockNode1 = cluster.filters().inbound().
                verbs(BLOCK_NODE_VERBS).to(1).drop();

            // LWT3 from node 3 (stale coordinator): writes v1=1 (does NOT touch v2).
            // PREPARE triggers AFTER → reset() → refresh applies LWT2's commit (v2=200) on node 3.
            node3.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v1 = 1 WHERE pk = ? AND ck = ? IF v1 = 0",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
            logger.info("LWT3 from stale node 3 succeeded");

            blockNode1.off();

            // Read v2 directly from node 3's local storage (no coordinator, no read repair).
            // If reset() worked: refresh applied LWT2's commit → v2=200.
            // If reset() was missing: no refresh, v2 still 100 from LWT1.
            Object[][] localResult = node3.executeInternalWithResult(
                "SELECT v2 FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?", pk, 1).
                toObjectArrays();
            Assert.assertEquals("Node 3 should have v2=200 from the refresh during PREPARE",
                                200, localResult[0][0]);

            // Also verify v1 was updated by LWT3
            Object[][] result = node1.coordinator().execute(
                "SELECT v1, v2 FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?",
                ConsistencyLevel.QUORUM, pk, 1);
            Assert.assertEquals("v1 should be 1 from LWT3", 1, result[0][0]);
            Assert.assertEquals("v2 should be 200 from LWT2", 200, result[0][1]);

            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }

    /**
     * Exercises the lowBound path in PaxosPrepare.permitted() where needLatest nodes are reclassified as withLatest
     * and the withLatestSlots tracker must be updated for the reclassified nodes.
     *
     * The lowBound path fires when a PREPARE response carries a paxos repair lowBound higher than the coordinator's
     * current latestCommitted ballot. This invalidates latestCommitted (reset to none) and moves all needLatest
     * nodes into withLatest.
     *
     * Scenario (3 slots, quorum=2, node 1 blocked so only 3 nodes respond):
     *   Slot 1: Node 1 (stable, BLOCKED)
     *   Slot 2: Node 2 (stable, has high lowBound from paxos repair)
     *   Slot 3: Node 3 + Node 4 (transitioning)
     *
     * 1. LWT1 commits B1 on all nodes
     * 2. Drop commits to node 3; LWT2 commits B2 on 1,2,4 (node 3 has B1)
     * 3. Set paxos repair lowBound on node 2 above B2
     * 4. Block node 1 from receiving Paxos messages
     * 5. LWT from node 3 (coordinator): local response sets latestCommitted=B1. Node 4 (B2, no high lowBound)
     *    responds next: AFTER fires, node 3 moves to needLatest. Then node 2 (B2, high lowBound) fires the
     *    lowBound path, reclassifying node 3 as withLatest.
     *    Without the fix: withLatestSlots has Slot 2 (1 satisfied) but Slot 3 is missing node 3's ack (1/2) ->
     *    effectiveWithLatestCount=1 < 2 -> PREPARE hangs -> CAS timeout.
     *    With the fix: Slot 3 has both node 3 + node 4 acks (2/2) -> effectiveWithLatestCount=2 -> PREPARE succeeds.
     *
     * A message filter delays PREPARE_REQ delivery to node 2, ensuring deterministic response ordering (node 4 before
     * node 2) so the reclassification path is exercised on every run.
     */
    @Test
    public void testSlotTrackerLowBoundReclassification() throws Exception
    {
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes + 1);

        BootstrapBB.resetForNodes(4);

        try (Cluster cluster = Cluster.build(numStartNodes).
                withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set(
                    "paxos_variant", "v2").set(
                    "skip_paxos_repair_on_topology_change", true)).
                withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(
                    numStartNodes + 1, "datacenter1", "rack1")).
                withInstanceInitializer(BootstrapBB::install).
                withTokenSupplier(node -> even.token(node)).
                start())
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            fixDistributedSchemas(cluster);
            init(cluster);

            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE +
                ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            setSlotGroupingEnabled(cluster, true);

            IInvokableInstance node4 = cluster.bootstrap(cluster.newInstanceConfig().
                set("auto_bootstrap", true));
            node4.startup(cluster);

            node1.logs().watchFor("Slot group calculation for " + KEYSPACE + " completed");

            int pk = pkInTransitioningRange(cluster);

            // Seed the row so IF EXISTS conditions succeed
            node1.coordinator().execute(
                "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                ConsistencyLevel.QUORUM, pk, 1, 1);

            // LWT1: establish committed state B1 on all nodes
            node1.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v = 10 WHERE pk = ? AND ck = ? IF EXISTS",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
            logger.info("LWT1 committed on all nodes");

            // Drop commit messages to node 3
            int[] commitVerbs = { Verb.PAXOS_COMMIT_REQ.id, Verb.PAXOS2_COMMIT_AND_PREPARE_REQ.id };
            IMessageFilters.Filter commitFilter = cluster.filters().inbound().to(3).
                verbs(commitVerbs).drop();

            // LWT2: commits B2 on nodes 1,2,4 but node 3 still has B1
            node1.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v = 20 WHERE pk = ? AND ck = ? IF EXISTS",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
            logger.info("LWT2 committed; node 3 missed the commit");

            commitFilter.off();

            // Set paxos repair lowBound on node 2 above B2. This simulates a paxos repair having completed on
            // node 2 but not yet on other nodes; future PREPARE responses from node 2 carry this high lowBound.
            // The lowBound is placed on node 2 (the last responder) so that node 4's AFTER response (which
            // populates needLatest) is processed before node 2's lowBound triggers the reclassification path.
            node2.runOnInstance(() -> {
                long nowMicros = TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
                Ballot highBallot = Ballot.atUnixMicrosWithLsb(nowMicros + 1_000_000L, 0, Ballot.Flag.GLOBAL);
                Token minToken = Murmur3Partitioner.instance.getMinimumToken();
                Token maxToken = Murmur3Partitioner.instance.getMaximumToken();
                Collection<Range<Token>> ranges = Collections.singleton(new Range<>(minToken, maxToken));
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").onPaxosRepairComplete(ranges, highBallot);
            });

            // Block node 1 so only nodes 2, 3, 4 participate; Slot 3 (transitioning) becomes critical for quorum.
            IMessageFilters.Filter blockNode1 = cluster.filters().inbound().verbs(BLOCK_NODE_VERBS).to(1).drop();

            // Delay PREPARE_REQ delivery to node 2 so node 4 responds first. This ensures node 4's B2 triggers the
            // AFTER case (populating needLatest) before node 2's high lowBound fires the reclassification path.
            IMessageFilters.Filter delayNode2Prepare =
                cluster.filters().inbound().to(2).verbs(Verb.PAXOS2_PREPARE_REQ.id).messagesMatching(
                    (from, to, msg) ->
                    {
                        try
                        {
                            Thread.sleep(200);
                        }
                        catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                        }
                        return false;
                    }).drop();

            // Coordinate from node 3 (stale). Delay filter ensures deterministic ordering: node3 (local, B1) ->
            // node4 (B2, AFTER creates needLatest=[node3]) -> node2 (high lowBound fires reclassification).
            node3.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v = 30 WHERE pk = ? AND ck = ? IF EXISTS",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);

            delayNode2Prepare.off();
            logger.info("ENABLED: LWT from stale node 3 succeeded (lowBound reclassification path exercised)");

            blockNode1.off();

            // Verify the final value
            Object[][] result = node1.coordinator().execute(
                "SELECT v FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?",
                ConsistencyLevel.QUORUM, pk, 1);
            Assert.assertEquals("Final value should reflect the last LWT", 30, result[0][0]);

            // === DISABLED: same scenario, verify LWT still succeeds via count-based quorum ===
            // With slot grouping disabled, Paxos quorum = participants/2+1 = 4/2+1 = 3.
            // Node 1 blocked → 3 responses from nodes 2,3,4 → meets quorum → SUCCESS.
            setSlotGroupingEnabled(cluster, false);

            // Re-create committed-state divergence: drop commits to node 3, run LWT from node 1
            commitFilter = cluster.filters().inbound().to(3).verbs(commitVerbs).drop();
            node1.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v = 40 WHERE pk = ? AND ck = ? IF EXISTS",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
            logger.info("DISABLED: LWT from node 1 committed; node 3 missed the commit");
            commitFilter.off();

            // Block node 1 again
            blockNode1 = cluster.filters().inbound().verbs(BLOCK_NODE_VERBS).to(1).drop();

            // LWT from stale node 3 should succeed: 3 responses (nodes 2,3,4) >= quorum=3
            node3.coordinator().execute(
                "UPDATE " + KEYSPACE + ".tbl SET v = 50 WHERE pk = ? AND ck = ? IF EXISTS",
                ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM, pk, 1);
            logger.info("DISABLED: LWT from stale node 3 succeeded (count-based quorum: 3/4 >= 3)");

            blockNode1.off();

            result = node1.coordinator().execute(
                "SELECT v FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?",
                ConsistencyLevel.QUORUM, pk, 1);
            Assert.assertEquals("Final value should reflect the disabled LWT", 50, result[0][0]);

            BootstrapBB.keepNodeInPendingState.set(false);
        }
    }
}
