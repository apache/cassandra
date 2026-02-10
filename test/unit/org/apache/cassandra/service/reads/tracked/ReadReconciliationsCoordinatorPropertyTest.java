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

package org.apache.cassandra.service.reads.tracked;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

/**
 * Property-based tests for ReadReconciliations.Coordinator completion logic.
 * Tests that the three-counter system (mutations, summaries, syncAcks) correctly
 * determines when reconciliation is complete.
 */
public class ReadReconciliationsCoordinatorPropertyTest
{
    // LOCAL_NODE is initialized from ClusterMetadata to match production code
    private static int LOCAL_NODE;
    private static int REMOTE_NODE;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        // Get the actual local node ID that ReadReconciliations will use
        LOCAL_NODE = ClusterMetadata.current().myNodeId().id();
        REMOTE_NODE = LOCAL_NODE + 1;  // Use a distinct remote node ID
    }

    /**
     * Testable subclass that overrides complete() to skip messaging side effects,
     * and exposes test methods to directly manipulate counters.
     */
    static class TestableCoordinator extends ReadReconciliations.Coordinator
    {
        TestableCoordinator(int dataNode, int[] summaryNodes)
        {
            super(new TrackedRead.Id(LOCAL_NODE, 0), dataNode, summaryNodes);
        }

        @Override
        protected boolean complete()
        {
            // Skip messaging - just return true
            return true;
        }

        // Test methods that directly update counters without messaging side effects
        boolean testAcceptLocalSummary()
        {
            return updateRemainingAndMaybeComplete(0, -1, 0);
        }

        boolean testAcceptRemoteSummary(int missingCount)
        {
            return updateRemainingAndMaybeComplete(missingCount, -1, 0);
        }
    }

    /**
     * Configuration for a coordinator test case.
     */
    static class CoordinatorConfig
    {
        final int summaryNodeCount;  // 0-5 summary nodes
        final boolean isDataNode;    // Whether local node is the data node

        CoordinatorConfig(int summaryNodeCount, boolean isDataNode)
        {
            this.summaryNodeCount = summaryNodeCount;
            this.isDataNode = isDataNode;
        }

        @Override
        public String toString()
        {
            return String.format("summaryNodes=%d, isDataNode=%s", summaryNodeCount, isDataNode);
        }
    }

    /**
     * Response types for coordinator testing.
     */
    enum ResponseType
    {
        LOCAL_SUMMARY,
        REMOTE_SUMMARY,
        SYNC_ACK,
        MUTATION
    }

    /**
     * A single response event.
     */
    static class CoordinatorResponse
    {
        final ResponseType type;
        final int missingCount;  // Only used for REMOTE_SUMMARY
        final int nodeId;        // Used for SYNC_ACK

        CoordinatorResponse(ResponseType type)
        {
            this(type, 0, 0);
        }

        CoordinatorResponse(ResponseType type, int missingCount)
        {
            this(type, missingCount, 0);
        }

        CoordinatorResponse(ResponseType type, int missingCount, int nodeId)
        {
            this.type = type;
            this.missingCount = missingCount;
            this.nodeId = nodeId;
        }

        @Override
        public String toString()
        {
            if (type == ResponseType.REMOTE_SUMMARY)
                return String.format("%s(missing=%d)", type, missingCount);
            if (type == ResponseType.SYNC_ACK)
                return String.format("%s(node=%d)", type, nodeId);
            return type.toString();
        }
    }

    /**
     * A complete test case with configuration and response sequence.
     */
    static class TestCase
    {
        final CoordinatorConfig config;
        final List<CoordinatorResponse> responses;

        TestCase(CoordinatorConfig config, List<CoordinatorResponse> responses)
        {
            this.config = config;
            this.responses = responses;
        }

        @Override
        public String toString()
        {
            return String.format("config=%s, responses=%d", config, responses.size());
        }
    }

    /**
     * Expected state tracker for validating coordinator behavior.
     */
    static class ExpectedState
    {
        int remainingMutations;
        int remainingSummaries;
        int remainingSyncAcks;

        ExpectedState(int summaryNodeCount, boolean isDataNode)
        {
            this.remainingMutations = 0;
            this.remainingSummaries = 1 + summaryNodeCount;
            this.remainingSyncAcks = isDataNode ? summaryNodeCount : 0;
        }

        boolean apply(CoordinatorResponse response)
        {
            switch (response.type)
            {
                case LOCAL_SUMMARY:
                    remainingSummaries--;
                    break;
                case REMOTE_SUMMARY:
                    remainingMutations += response.missingCount;
                    remainingSummaries--;
                    break;
                case SYNC_ACK:
                    remainingSyncAcks--;
                    break;
                case MUTATION:
                    remainingMutations--;
                    break;
            }
            return isComplete();
        }

        boolean isComplete()
        {
            return remainingMutations == 0
                && remainingSummaries == 0
                && remainingSyncAcks == 0;
        }

        @Override
        public String toString()
        {
            return String.format("mutations=%d, summaries=%d, syncAcks=%d",
                                 remainingMutations, remainingSummaries, remainingSyncAcks);
        }
    }

    // ========================================
    // Generators
    // ========================================

    /**
     * Generate coordinator configurations.
     */
    Gen<CoordinatorConfig> configGen()
    {
        return integers().between(0, 5).flatMap(summaryNodes ->
            booleans().all().map(isDataNode ->
                new CoordinatorConfig(summaryNodes, isDataNode)
            )
        );
    }

    /**
     * Generate test cases with valid response sequences.
     * Responses are generated such that mutations always come after their parent remote summary.
     */
    Gen<TestCase> testCaseGen()
    {
        return configGen().flatMap(config -> responseSequenceGen(config).map(responses ->
            new TestCase(config, responses)
        ));
    }

    /**
     * Generate a valid response sequence for a given configuration.
     */
    Gen<List<CoordinatorResponse>> responseSequenceGen(CoordinatorConfig config)
    {
        // Generate missingCount for each remote summary (0-10)
        return lists().of(integers().between(0, 10)).ofSize(config.summaryNodeCount).flatMap(missingCounts -> {
            // Calculate total mutations needed
            int totalMutations = missingCounts.stream().mapToInt(Integer::intValue).sum();

            // Generate a random permutation using indices
            int totalEvents = 1 + config.summaryNodeCount + totalMutations + (config.isDataNode ? config.summaryNodeCount : 0);
            return permutationGen(totalEvents).map(permutation -> {
                // Build the response sequence respecting ordering constraints
                return buildResponseSequence(config, missingCounts, permutation);
            });
        });
    }

    /**
     * Build response sequence respecting the constraint that mutations must come after their parent remote summary.
     */
    private List<CoordinatorResponse> buildResponseSequence(CoordinatorConfig config,
                                                            List<Integer> missingCounts,
                                                            List<Integer> permutation)
    {
        // Create all events
        List<CoordinatorResponse> events = new ArrayList<>();

        // Add local summary
        events.add(new CoordinatorResponse(ResponseType.LOCAL_SUMMARY));

        // Add remote summaries with their mutations linked
        List<List<CoordinatorResponse>> remoteSummaryGroups = new ArrayList<>();
        for (int i = 0; i < config.summaryNodeCount; i++)
        {
            List<CoordinatorResponse> group = new ArrayList<>();
            group.add(new CoordinatorResponse(ResponseType.REMOTE_SUMMARY, missingCounts.get(i)));
            for (int j = 0; j < missingCounts.get(i); j++)
                group.add(new CoordinatorResponse(ResponseType.MUTATION));
            remoteSummaryGroups.add(group);
        }

        // Add sync acks (if data node)
        List<CoordinatorResponse> syncAcks = new ArrayList<>();
        if (config.isDataNode)
        {
            for (int i = 0; i < config.summaryNodeCount; i++)
                syncAcks.add(new CoordinatorResponse(ResponseType.SYNC_ACK, 0, REMOTE_NODE + i + 1));
        }

        // Shuffle the independent groups using the permutation
        // We shuffle: local summary, each remote summary group (as a unit), and sync acks
        List<Object> shuffleable = new ArrayList<>();
        shuffleable.add(events.get(0));  // local summary
        shuffleable.addAll(remoteSummaryGroups);
        shuffleable.addAll(syncAcks);

        // Use permutation to reorder (just use first N elements of permutation as indices)
        List<Object> shuffled = new ArrayList<>(shuffleable);
        Collections.shuffle(shuffled, new java.util.Random(permutation.hashCode()));

        // Flatten back to response list
        List<CoordinatorResponse> result = new ArrayList<>();
        for (Object item : shuffled)
        {
            if (item instanceof CoordinatorResponse)
                result.add((CoordinatorResponse) item);
            else if (item instanceof List)
            {
                @SuppressWarnings("unchecked")
                List<CoordinatorResponse> group = (List<CoordinatorResponse>) item;
                result.addAll(group);
            }
        }

        return result;
    }

    /**
     * Generate a random permutation of integers [0, n-1].
     */
    private static Gen<List<Integer>> permutationGen(int n)
    {
        if (n == 0)
            return lists().of(integers().all()).ofSize(0);
        if (n == 1)
        {
            return lists().of(integers().all()).ofSize(1).map(list -> {
                List<Integer> result = new ArrayList<>();
                result.add(0);
                return result;
            });
        }

        // Generate n random integers for shuffling
        return lists().of(integers().between(0, Integer.MAX_VALUE)).ofSize(n).map(randoms -> {
            List<Integer> result = new ArrayList<>();
            for (int i = 0; i < n; i++)
                result.add(i);

            // Fisher-Yates shuffle using generated random values
            for (int i = n - 1; i > 0; i--)
            {
                int j = Math.abs(randoms.get(i)) % (i + 1);
                Collections.swap(result, i, j);
            }

            return result;
        });
    }

    // ========================================
    // Test Methods
    // ========================================

    /**
     * Apply a response to the coordinator and return whether it completed.
     */
    private boolean applyResponse(TestableCoordinator coordinator, CoordinatorResponse response)
    {
        switch (response.type)
        {
            case LOCAL_SUMMARY:
                return coordinator.testAcceptLocalSummary();
            case REMOTE_SUMMARY:
                return coordinator.testAcceptRemoteSummary(response.missingCount);
            case SYNC_ACK:
                return coordinator.acceptSyncAck(response.nodeId);
            case MUTATION:
                return coordinator.acceptMutation(null);  // mutationId is ignored
            default:
                throw new IllegalArgumentException("Unknown response type: " + response.type);
        }
    }

    /**
     * Create a coordinator for testing.
     */
    private TestableCoordinator createCoordinator(CoordinatorConfig config)
    {
        int dataNode = config.isDataNode ? LOCAL_NODE : REMOTE_NODE;
        int[] summaryNodes = new int[config.summaryNodeCount];
        for (int i = 0; i < config.summaryNodeCount; i++)
            summaryNodes[i] = REMOTE_NODE + i + 1;  // Use distinct node IDs

        return new TestableCoordinator(dataNode, summaryNodes);
    }

    @Test
    public void coordinatorCompletionBehavior()
    {
        qt()
            .withExamples(500)
            .forAll(testCaseGen())
            .checkAssert(testCase -> {
                TestableCoordinator coordinator = createCoordinator(testCase.config);
                ExpectedState expected = new ExpectedState(
                    testCase.config.summaryNodeCount,
                    testCase.config.isDataNode
                );

                for (CoordinatorResponse response : testCase.responses)
                {
                    boolean actualComplete = applyResponse(coordinator, response);
                    boolean expectedComplete = expected.apply(response);

                    Assert.assertEquals(
                        String.format("Completion mismatch after %s: expected=%s, actual=%s, state=%s, config=%s",
                                      response, expectedComplete, actualComplete, expected, testCase.config),
                        expectedComplete, actualComplete
                    );

                    if (actualComplete)
                        break;
                }

                // Final state should be complete
                Assert.assertTrue(
                    String.format("Should be complete after all responses: state=%s, config=%s",
                                  expected, testCase.config),
                    expected.isComplete()
                );
            });
    }

    /**
     * Test edge case: minimal coordinator with 0 summary nodes as data node.
     * Should complete immediately after local summary.
     */
    @Test
    public void minimalDataNodeCompletes()
    {
        TestableCoordinator coordinator = new TestableCoordinator(LOCAL_NODE, new int[0]);

        // Should complete after just the local summary
        Assert.assertTrue("Should complete after local summary with 0 summary nodes",
                          coordinator.testAcceptLocalSummary());
    }

    /**
     * Test edge case: minimal coordinator with 0 summary nodes as summary node.
     * Should complete immediately after local summary.
     */
    @Test
    public void minimalSummaryNodeCompletes()
    {
        TestableCoordinator coordinator = new TestableCoordinator(REMOTE_NODE, new int[0]);

        // Should complete after just the local summary
        Assert.assertTrue("Should complete after local summary with 0 summary nodes",
                          coordinator.testAcceptLocalSummary());
    }

    /**
     * Test that data node requires sync acks from all summary nodes.
     */
    @Test
    public void dataNodeRequiresSyncAcks()
    {
        TestableCoordinator coordinator = new TestableCoordinator(
            LOCAL_NODE, new int[]{REMOTE_NODE, REMOTE_NODE + 1}
        );

        // Local summary
        Assert.assertFalse(coordinator.testAcceptLocalSummary());

        // Remote summaries with no missing mutations
        Assert.assertFalse(coordinator.testAcceptRemoteSummary(0));
        Assert.assertFalse(coordinator.testAcceptRemoteSummary(0));

        // First sync ack
        Assert.assertFalse(coordinator.acceptSyncAck(REMOTE_NODE));

        // Second sync ack should complete
        Assert.assertTrue(coordinator.acceptSyncAck(REMOTE_NODE + 1));
    }

    /**
     * Test that summary node does NOT require sync acks.
     */
    @Test
    public void summaryNodeDoesNotRequireSyncAcks()
    {
        TestableCoordinator coordinator = new TestableCoordinator(
            REMOTE_NODE, new int[]{REMOTE_NODE + 1, REMOTE_NODE + 2}
        );

        // Local summary
        Assert.assertFalse(coordinator.testAcceptLocalSummary());

        // Remote summaries with no missing mutations
        Assert.assertFalse(coordinator.testAcceptRemoteSummary(0));

        // Last remote summary should complete (no sync acks needed)
        Assert.assertTrue(coordinator.testAcceptRemoteSummary(0));
    }

    /**
     * Test that mutations must be received before completion.
     */
    @Test
    public void mutationsMustBeReceived()
    {
        TestableCoordinator coordinator = new TestableCoordinator(
            REMOTE_NODE, new int[]{REMOTE_NODE + 1}
        );

        // Local summary
        Assert.assertFalse(coordinator.testAcceptLocalSummary());

        // Remote summary with 2 missing mutations
        Assert.assertFalse(coordinator.testAcceptRemoteSummary(2));

        // First mutation
        Assert.assertFalse(coordinator.acceptMutation(null));

        // Second mutation should complete
        Assert.assertTrue(coordinator.acceptMutation(null));
    }
}
