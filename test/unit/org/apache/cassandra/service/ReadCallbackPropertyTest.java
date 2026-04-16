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

package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.CoordinationPlan;
import org.apache.cassandra.locator.CoordinationPlans;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.ResponseResolver;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;

import static org.quicktheories.QuickTheory.qt;

/**
 * Property-based tests for ReadCallback with dynamic topology generation.
 * Tests incremental response behavior: applies responses one at a time and validates
 * completion state after each response.
 */
public class ReadCallbackPropertyTest extends ResponseHandlerPropertyTestBase
{
    private static final ImmutableList<ConsistencyLevel> consistencyLevels = ImmutableList.of(
            ConsistencyLevel.ONE,
            ConsistencyLevel.TWO,
            ConsistencyLevel.THREE,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.ALL,
            ConsistencyLevel.LOCAL_ONE,
            ConsistencyLevel.LOCAL_QUORUM,
            ConsistencyLevel.EACH_QUORUM
    );

    @Override
    protected List<ConsistencyLevel> consistencyLevels()
    {
        return consistencyLevels;
    }

    /**
     * Calculate expected outcome based on responses so far.
     * Returns null if more responses needed (incomplete).
     *
     * Single-requirement model for reads (no pending replica complexity):
     * - Reads only count responses from full (non-pending) replicas
     * - blockFor does NOT include pending replicas
     * - Success when: full_replica_successes >= CL@RF
     * - For LOCAL_* CLs, only local DC replicas are contacted
     */
    private static ExpectedOutcome calculateExpectedOutcome(TopologyConfig topology,
                                                           ConsistencyLevel cl,
                                                           List<ResponseMessage> responses,
                                                           int blockFor,
                                                           int contactedReplicas)
    {
        // Count successes from full replicas only (pending can't serve reads)
        int fullReplicaSuccesses = 0;
        int fullReplicaResponses = 0;

        Map<String, Integer> successesPerDc = new HashMap<>();
        Map<String, Integer> responsesPerDc = new HashMap<>();

        for (ResponseMessage resp : responses)
        {
            // Skip pending replicas - they can't serve reads
            if (resp.replicaIdx >= topology.totalReplicas)
                continue;

            String dc = getReplicaDatacenter(resp.replicaIdx, topology);
            fullReplicaResponses++;
            responsesPerDc.merge(dc, 1, Integer::sum);

            if (resp.isSuccess())
            {
                fullReplicaSuccesses++;
                successesPerDc.merge(dc, 1, Integer::sum);
            }
        }

        // Check completion conditions based on CL type
        switch (cl)
        {
            case ONE:
            case TWO:
            case THREE:
            case QUORUM:
            case ALL:
            {
                // Global CLs: need blockFor successes from full replicas
                if (fullReplicaSuccesses >= blockFor)
                    return ExpectedOutcome.SUCCESS;

                // Early failure: can't satisfy requirement
                int remaining = contactedReplicas - fullReplicaResponses;
                if (fullReplicaSuccesses + remaining < blockFor)
                    return ExpectedOutcome.FAILURE;

                return null;
            }

            case LOCAL_ONE:
            case LOCAL_QUORUM:
            {
                // Local CLs: only count responses from local DC (datacenter1)
                String localDc = "datacenter1";
                int localSuccesses = successesPerDc.getOrDefault(localDc, 0);
                int localResponses = responsesPerDc.getOrDefault(localDc, 0);

                if (localSuccesses >= blockFor)
                    return ExpectedOutcome.SUCCESS;

                // Early failure: can't satisfy requirement from local DC
                int localRemaining = contactedReplicas - localResponses;
                if (localSuccesses + localRemaining < blockFor)
                    return ExpectedOutcome.FAILURE;

                return null;
            }

            case EACH_QUORUM:
            {
                // Note: ReadCallback doesn't have special per-DC tracking for EACH_QUORUM.
                // It just counts total successes against blockFor (sum of DC quorums).
                // This is different from DatacenterSyncWriteResponseHandler which tracks per-DC.
                if (fullReplicaSuccesses >= blockFor)
                    return ExpectedOutcome.SUCCESS;

                // Early failure: can't satisfy requirement
                int remaining = contactedReplicas - fullReplicaResponses;
                if (fullReplicaSuccesses + remaining < blockFor)
                    return ExpectedOutcome.FAILURE;

                return null;
            }

            default:
                throw new IllegalArgumentException("Unsupported CL: " + cl);
        }
    }

    /**
     * A minimal ResponseResolver for testing that only tracks response counts and data presence.
     * This allows testing ReadCallback's completion logic without full read infrastructure.
     */
    private static class TestResponseResolver extends ResponseResolver<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        private volatile boolean dataPresent = false;

        public TestResponseResolver(CoordinationPlan.ForTokenRead plan, Dispatcher.RequestTime requestTime)
        {
            super(null, null, plan, requestTime);
        }

        @Override
        public boolean isDataPresent()
        {
            return dataPresent;
        }

        @Override
        public void preprocess(Message<ReadResponse> message)
        {
            // Add to accumulator (parent class behavior)
            responses.add(message);
            // First full replica response makes data present
            Replica replica = replicaPlan().lookup(message.from());
            if (replica != null && replica.isFull())
                dataPresent = true;
        }

        public int responseCount()
        {
            return responses.size();
        }

        /** Expose replicaPlan for testing */
        public ReplicaPlan.ForTokenRead getReplicaPlan()
        {
            return replicaPlan();
        }
    }

    /**
     * Creates a ReplicaPlan for testing with the given parameters.
     */
    private static CoordinationPlan.ForTokenRead createReplicaPlan(Keyspace ks,
                                                                   ConsistencyLevel cl,
                                                                   EndpointsForToken contacts)
    {
        ReplicaPlan.ForTokenRead plan = new ReplicaPlan.ForTokenRead(
            ks,
            ks.getReplicationStrategy(),
            cl,
            contacts,  // candidates
            contacts,  // contacts
            contacts,  // liveAndDown
            (cm) -> null,  // recompute function
            (self) -> null, // repair plan function
            Epoch.EMPTY
        );
        return CoordinationPlans.create(plan);
    }

    /**
     * Gets the set of replica indices that belong to the local datacenter (datacenter1).
     */
    private static Set<Integer> getLocalDcReplicaIndices(TopologyConfig topology)
    {
        Set<Integer> localIndices = new HashSet<>();
        int idx = 0;
        for (Map.Entry<String, Integer> entry : topology.replicationFactors.entrySet())
        {
            String dc = entry.getKey();
            int rf = entry.getValue();
            if ("datacenter1".equals(dc))
            {
                for (int i = 0; i < rf; i++)
                    localIndices.add(idx + i);
            }
            idx += rf;
        }
        return localIndices;
    }

    /**
     * Filters replicas to only include those from the local datacenter.
     */
    private static EndpointsForToken filterToLocalDc(EndpointsForToken replicas, TopologyConfig topology)
    {
        Set<Integer> localIndices = getLocalDcReplicaIndices(topology);
        List<Replica> localReplicas = new ArrayList<>();
        for (int i = 0; i < replicas.size(); i++)
        {
            if (localIndices.contains(i))
                localReplicas.add(replicas.get(i));
        }
        return EndpointsForToken.of(replicas.token(), localReplicas.toArray(new Replica[0]));
    }

    /**
     * Bundles a handler with the set of contacted replica indices.
     */
    private static class HandlerWithContacts
    {
        final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler;
        final Set<Integer> contactedIndices;

        HandlerWithContacts(ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler, Set<Integer> contactedIndices)
        {
            this.handler = handler;
            this.contactedIndices = contactedIndices;
        }
    }

    /**
     * Creates a fresh ReadCallback for testing.
     * For LOCAL_* CLs, only local DC replicas are contacted.
     */
    private static HandlerWithContacts createHandler(
        Keyspace ks,
        ConsistencyLevel cl,
        EndpointsForToken fullReplicas,
        TopologyConfig topology)
    {
        EndpointsForToken contacts;
        Set<Integer> contactedIndices;

        if (cl == ConsistencyLevel.LOCAL_ONE || cl == ConsistencyLevel.LOCAL_QUORUM)
        {
            // For LOCAL_* CLs, only contact local DC replicas
            contacts = filterToLocalDc(fullReplicas, topology);
            contactedIndices = getLocalDcReplicaIndices(topology);
        }
        else
        {
            // For global CLs, contact all full replicas
            contacts = fullReplicas;
            contactedIndices = new HashSet<>();
            for (int i = 0; i < fullReplicas.size(); i++)
                contactedIndices.add(i);
        }

        CoordinationPlan.ForTokenRead plan = createReplicaPlan(ks, cl, contacts);
        Dispatcher.RequestTime requestTime = new Dispatcher.RequestTime(System.nanoTime(), System.nanoTime());

        TestResponseResolver resolver = new TestResponseResolver(plan, requestTime);
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler = new ReadCallback<>(resolver, null, plan, requestTime);

        return new HandlerWithContacts(handler, contactedIndices);
    }

    /**
     * Gets the endpoint for a replica index from the replica sets.
     */
    private static InetAddressAndPort getEndpoint(int replicaIdx, ReplicaSets replicaSets)
    {
        if (replicaIdx < replicaSets.fullReplicas.size())
            return replicaSets.fullReplicas.get(replicaIdx).endpoint();

        int pendingIdx = replicaIdx - replicaSets.fullReplicas.size();
        if (pendingIdx < replicaSets.pendingReplicas.size())
            return replicaSets.pendingReplicas.get(pendingIdx).endpoint();

        throw new IllegalArgumentException("Invalid replica index: " + replicaIdx);
    }

    /**
     * Creates a minimal ReadResponse message for testing.
     * Uses Message.synthetic() which is designed for testing and allows creating messages from arbitrary nodes.
     * The TestResponseResolver doesn't access the payload, so we pass null.
     */
    private static Message<ReadResponse> createResponseMessage(InetAddressAndPort from)
    {
        return Message.synthetic(from, org.apache.cassandra.net.Verb.READ_RSP, null);
    }

    /**
     * Applies a single response to the handler.
     * Returns true if the response was applied (i.e., from a replica in contacts).
     */
    private static boolean applyResponse(ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler,
                                         ResponseMessage response,
                                         ReplicaSets replicaSets,
                                         Set<Integer> contactedIndices)
    {
        // Skip pending replicas - they can't serve reads
        if (response.replicaIdx >= replicaSets.fullReplicas.size())
            return false;

        // Skip replicas not in the contact set (important for LOCAL_* CLs)
        if (!contactedIndices.contains(response.replicaIdx))
            return false;

        InetAddressAndPort endpoint = getEndpoint(response.replicaIdx, replicaSets);

        if (response.isSuccess())
        {
            Message<ReadResponse> msg = createResponseMessage(endpoint);
            handler.onResponse(msg);
        }
        else
        {
            handler.onFailure(endpoint, new RequestFailure(response.failureReason, null));
        }
        return true;
    }

    /**
     * Checks if the handler has signaled completion.
     */
    private static boolean isComplete(ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler)
    {
        // ReadCallback signals completion via its condition
        // We check by attempting a zero-timeout await
        return handler.await(0, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Applies a pre-generated response sequence to a handler, stopping when complete.
     * Returns the subset of responses that were actually applied.
     */
    private static List<ResponseMessage> applyResponseSequence(
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler,
        List<ResponseMessage> responses,
        ReplicaSets replicaSets,
        Set<Integer> contactedIndices)
    {
        List<ResponseMessage> applied = new ArrayList<>();

        for (ResponseMessage response : responses)
        {
            // Check if already complete before applying
            if (isComplete(handler))
                break;

            if (applyResponse(handler, response, replicaSets, contactedIndices))
                applied.add(response);
        }

        return applied;
    }

    /**
     * Validates handler completed with expected outcome.
     */
    private static void validateOutcome(ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> handler,
                                        List<ResponseMessage> appliedResponses,
                                        TopologyConfig topology,
                                        ConsistencyLevel cl,
                                        int contactedReplicas)
    {
        TestResponseResolver resolver = (TestResponseResolver) handler.resolver;
        int blockFor = resolver.getReplicaPlan().readQuorum();
        ExpectedOutcome expected = calculateExpectedOutcome(topology, cl, appliedResponses, blockFor, contactedReplicas);

        boolean complete = isComplete(handler);
        int successes = resolver.responseCount();
        int failures = contactedReplicas - successes;

        if (expected == null)
        {
            // Should not be complete yet
            Assert.assertFalse(
                String.format("Handler completed prematurely with %d successes, %d failures (blockFor=%d, CL=%s)",
                              successes, failures, blockFor, cl),
                complete
            );
        }
        else if (expected == ExpectedOutcome.SUCCESS)
        {
            Assert.assertTrue(
                String.format("Handler should have completed successfully with %d successes (blockFor=%d, CL=%s)",
                              successes, blockFor, cl),
                complete
            );
            Assert.assertTrue(
                String.format("Data should be present with %d successes", successes),
                resolver.isDataPresent()
            );
        }
        else
        {
            // Expected failure - handler should be complete due to too many failures
            Assert.assertTrue(
                String.format("Handler should have completed (failed) with %d failures (blockFor=%d, contacts=%d, CL=%s)",
                              failures, blockFor, contactedReplicas, cl),
                complete
            );
        }
    }

    // ========================================
    // Property Tests
    // ========================================

    @Test
    public void readCallbackBehavior()
    {
        qt()
            .withExamples(250)
            .forAll(testCaseGen())
            .assuming(testCase -> {
                // Filter out topologies where any scenario has insufficient replicas for its CL
                for (CLScenario scenario : testCase.scenarios)
                {
                    if (testCase.topology.totalReplicas < minReplicasForCL(scenario.cl))
                        return false;
                    if (!hasEnoughLocalReplicas(testCase.topology, scenario.cl))
                        return false;
                }
                return true;
            })
            .checkAssert(testCase -> {
                try
                {
                    Keyspace ks = getOrCreateKeyspace(testCase.topology);
                    ReplicaSets replicaSets = createReplicaSets(testCase.topology);

                    // Test all scenarios for this topology
                    for (CLScenario scenario : testCase.scenarios)
                    {
                        HandlerWithContacts hwc = createHandler(ks, scenario.cl, replicaSets.fullReplicas, testCase.topology);
                        List<ResponseMessage> appliedResponses = applyResponseSequence(
                            hwc.handler, scenario.responses, replicaSets, hwc.contactedIndices);
                        validateOutcome(hwc.handler, appliedResponses, testCase.topology, scenario.cl, hwc.contactedIndices.size());
                    }
                }
                catch (Throwable e)
                {
                    if (e instanceof AssertionError)
                        throw (AssertionError) e;
                    throw new AssertionError("Test setup failed: " + e.getMessage(), e);
                }
            });
    }

    /**
     * Minimum replicas needed for a consistency level to be valid.
     * For LOCAL_* CLs, this returns the minimum needed in the local DC.
     */
    private static int minReplicasForCL(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ONE:
            case LOCAL_ONE:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
            case LOCAL_QUORUM:
                return 3;  // Need at least 3 for quorum to be meaningful (3/2 + 1 = 2)
            case EACH_QUORUM:
                return 1;  // Just need at least 1 replica per DC (checked separately)
            case ALL:
                return 1;
            default:
                return 1;
        }
    }

    /**
     * Check if the local DC has enough replicas for the given CL.
     */
    private static boolean hasEnoughLocalReplicas(TopologyConfig topology, ConsistencyLevel cl)
    {
        if (cl == ConsistencyLevel.LOCAL_ONE || cl == ConsistencyLevel.LOCAL_QUORUM)
        {
            Integer localRf = topology.replicationFactors.get("datacenter1");
            if (localRf == null)
                return false;
            return localRf >= minReplicasForCL(cl);
        }

        if (cl == ConsistencyLevel.EACH_QUORUM)
        {
            // EACH_QUORUM requires at least 1 replica in every DC
            for (int rf : topology.replicationFactors.values())
            {
                if (rf < 1)
                    return false;
            }
        }

        return true;
    }
}
