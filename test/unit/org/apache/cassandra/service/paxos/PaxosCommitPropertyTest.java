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

package org.apache.cassandra.service.paxos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ResponseHandlerPropertyTestBase;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.service.paxos.Commit.Agreed;
import static org.quicktheories.QuickTheory.qt;

/**
 * Property-based tests for PaxosCommit response tracking logic.
 *
 * PaxosCommit uses a single-counter model: it counts accepts and failures in a
 * flat manner (no committed vs pending distinction). DC-local filtering applies
 * for datacenter-local consistency levels.
 *
 * Tests all write consistency levels: ANY, ONE, TWO, THREE, QUORUM, ALL,
 * LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM.
 */
public class PaxosCommitPropertyTest extends ResponseHandlerPropertyTestBase
{
    private static final ImmutableList<ConsistencyLevel> commitConsistencyLevels = ImmutableList.of(
        ConsistencyLevel.ANY,
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
        return commitConsistencyLevels;
    }

    // ========================================
    // Independent Model
    // ========================================

    /**
     * Independent model for PaxosCommit completion.
     *
     * PaxosCommit uses a single counter:
     * - DC-local CLs filter out non-local DC responses entirely
     * - SUCCESS: accepts == required (exact match for once-only signaling)
     * - FAILURE: replicas.size() - failures == required - 1 (impossible to reach required)
     *
     * Note: replicas.size() is ALL replicas across ALL DCs, even for local CLs.
     * This asymmetry (count only local, but use global size for failure) is
     * production behavior.
     */
    static ExpectedOutcome calculateCommitOutcome(TopologyConfig topology,
                                                  ConsistencyLevel cl,
                                                  List<ResponseMessage> responses,
                                                  int required,
                                                  int totalReplicaCount)
    {
        boolean dcLocalFilter = cl.isDatacenterLocal();
        int accepts = 0;
        int failures = 0;

        for (ResponseMessage resp : responses)
        {
            if (dcLocalFilter)
            {
                String dc = getReplicaDatacenter(resp.replicaIdx, topology);
                if (!"datacenter1".equals(dc))
                    continue;
            }

            if (resp.isSuccess())
                accepts++;
            else
                failures++;
        }

        // Success: reached required accepts
        if (accepts >= required)
            return ExpectedOutcome.SUCCESS;

        // Failure: impossible to reach required
        // (uses totalReplicaCount which includes all DCs, even for local CLs)
        if (totalReplicaCount - failures < required)
            return ExpectedOutcome.FAILURE;

        return null; // still in progress
    }

    // ========================================
    // Handler creation and response application
    // ========================================

    /**
     * Testable subclass that overrides the DC membership check using topology knowledge,
     * bypassing the InOurDc/Locator infrastructure which isn't fully initialized in unit tests.
     */
    static class TestableCommit<T extends Consumer<? super PaxosCommit.Status>>
        extends PaxosCommit<T>
    {
        private final Set<InetAddressAndPort> localEndpoints;

        TestableCommit(Agreed commit, EndpointsForToken replicas, int required,
                       ConsistencyLevel consistencyForCommit, T onDone,
                       Set<InetAddressAndPort> localEndpoints)
        {
            super(commit, false, consistencyForCommit, consistencyForCommit, replicas, required, onDone);
            this.localEndpoints = localEndpoints;
        }

        @Override
        protected boolean isFromLocalDc(InetAddressAndPort endpoint)
        {
            return localEndpoints.contains(endpoint);
        }
    }

    /**
     * Build the set of datacenter1 endpoints from the replica sets, used to override
     * DC filtering in TestableCommit.
     */
    private static Set<InetAddressAndPort> localEndpointSet(TopologyConfig topology,
                                                             ReplicaSets replicaSets)
    {
        Set<InetAddressAndPort> local = new HashSet<>();
        int dc1Full = topology.replicationFactors.getOrDefault("datacenter1", 0);

        for (int i = 0; i < dc1Full; i++)
            local.add(replicaSets.fullReplicas.get(i).endpoint());

        // pending replicas for DC1 are at indices [dc1Full, dc1Full + dc1Pending)
        // within replicaSets.pendingReplicas, but pendingReplicas is ordered per-DC too
        int pendingOffset = 0;
        for (Map.Entry<String, Integer> entry : topology.pendingReplicas.entrySet())
        {
            int count = entry.getValue();
            if ("datacenter1".equals(entry.getKey()))
            {
                for (int i = 0; i < count; i++)
                    local.add(replicaSets.pendingReplicas.get(pendingOffset + i).endpoint());
                break;
            }
            pendingOffset += count;
        }
        return local;
    }

    /**
     * Compute blockFor for PaxosCommit, matching Participants.requiredFor() behavior.
     */
    private static int computeBlockFor(Keyspace ks, ConsistencyLevel cl, EndpointsForToken pending)
    {
        return cl.blockForWrite(ks.getReplicationStrategy(), pending);
    }

    /**
     * Creates a PaxosCommit handler for testing.
     * Uses TestableCommit subclass to override DC filtering for LOCAL_* consistency levels.
     * Builds a synthetic Agreed with an untracked keyspace so mutation tracking is bypassed.
     */
    private static PaxosCommit<Consumer<PaxosCommit.Status>> createCommitHandler(
        EndpointsForToken allReplicas, int required, ConsistencyLevel commitCl,
        AtomicReference<PaxosCommit.Status> statusCapture,
        TopologyConfig topology, ReplicaSets replicaSets, Keyspace ks)
    {
        Set<InetAddressAndPort> localEndpoints = localEndpointSet(topology, replicaSets);
        TableMetadata table = ks.getColumnFamilyStores().iterator().next().metadata();
        DecoratedKey key = table.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        Agreed commit = new Agreed(Ballot.none(), PartitionUpdate.emptyUpdate(table, key));
        return new TestableCommit<>(commit, allReplicas, required, commitCl, statusCapture::set, localEndpoints);
    }

    /**
     * Applies a single response to the PaxosCommit handler.
     */
    private static void applyCommitResponse(PaxosCommit<?> handler,
                                            ResponseMessage response,
                                            ReplicaSets replicaSets)
    {
        EndpointsForToken targets;
        int adjustedIdx;

        if (response.replicaIdx < replicaSets.fullReplicas.size())
        {
            targets = replicaSets.fullReplicas;
            adjustedIdx = response.replicaIdx;
        }
        else
        {
            targets = replicaSets.pendingReplicas;
            adjustedIdx = response.replicaIdx - replicaSets.fullReplicas.size();
        }

        InetAddressAndPort endpoint = targets.get(adjustedIdx).endpoint();

        if (response.isSuccess())
        {
            Message<NoPayload> msg = Message.builder(Verb.ECHO_REQ, noPayload)
                                            .from(endpoint)
                                            .build();
            handler.onResponse(msg);
        }
        else
        {
            handler.onFailure(endpoint, RequestFailure.forReason(response.failureReason));
        }
    }

    /**
     * Applies responses to the handler, stopping when onDone fires.
     */
    private static List<ResponseMessage> applyCommitResponseSequence(
        PaxosCommit<?> handler,
        List<ResponseMessage> responses,
        ReplicaSets replicaSets,
        AtomicReference<PaxosCommit.Status> statusCapture)
    {
        List<ResponseMessage> appliedResponses = new ArrayList<>();
        for (ResponseMessage response : responses)
        {
            if (statusCapture.get() != null)
                break;

            applyCommitResponse(handler, response, replicaSets);
            appliedResponses.add(response);
        }
        return appliedResponses;
    }

    // ========================================
    // Property Test
    // ========================================

    @Test
    public void paxosCommitBehavior()
    {
        qt()
            .withExamples(250)
            .forAll(testCaseGen())
            .assuming(testCase -> {
                for (CLScenario scenario : testCase.scenarios)
                {
                    if (testCase.topology.totalReplicas < minReplicasForCL(scenario.cl))
                        return false;

                    if (scenario.cl == ConsistencyLevel.EACH_QUORUM)
                    {
                        for (Map.Entry<String, Integer> entry : testCase.topology.replicationFactors.entrySet())
                        {
                            int dcRf = entry.getValue();
                            int dcPending = testCase.topology.pendingReplicas.get(entry.getKey());
                            int dcQuorum = (dcRf / 2 + 1) + dcPending;
                            int dcTotal = dcRf + dcPending;

                            if (dcTotal < dcQuorum)
                                return false;
                        }
                    }
                }
                return true;
            })
            .checkAssert(testCase -> {
                try
                {
                    Keyspace ks = getOrCreateKeyspace(testCase.topology);
                    ReplicaSets replicaSets = createReplicaSets(testCase.topology);

                    // Build combined replica list (full + pending) as PaxosCommit sees it
                    List<Replica> allReplicaList = new ArrayList<>();
                    replicaSets.fullReplicas.forEach(allReplicaList::add);
                    replicaSets.pendingReplicas.forEach(allReplicaList::add);
                    EndpointsForToken allReplicas = EndpointsForToken.of(
                        replicaSets.fullReplicas.token(),
                        allReplicaList.toArray(new Replica[0]));

                    for (CLScenario scenario : testCase.scenarios)
                    {
                        int blockFor = computeBlockFor(ks, scenario.cl, replicaSets.pendingReplicas);

                        AtomicReference<PaxosCommit.Status> statusCapture = new AtomicReference<>();
                        PaxosCommit<Consumer<PaxosCommit.Status>> handler =
                            createCommitHandler(allReplicas, blockFor, scenario.cl, statusCapture,
                                                testCase.topology, replicaSets, ks);

                        List<ResponseMessage> appliedResponses =
                            applyCommitResponseSequence(handler, scenario.responses, replicaSets, statusCapture);

                        validateCommitOutcome(statusCapture.get(), appliedResponses,
                                              testCase.topology, scenario.cl, blockFor,
                                              allReplicas.size());
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
     * Validates the PaxosCommit outcome against the independent model.
     */
    private static void validateCommitOutcome(PaxosCommit.Status status,
                                              List<ResponseMessage> responses,
                                              TopologyConfig topology,
                                              ConsistencyLevel cl,
                                              int blockFor,
                                              int totalReplicaCount)
    {
        ExpectedOutcome expected = calculateCommitOutcome(topology, cl, responses, blockFor, totalReplicaCount);

        if (status == null)
        {
            if (expected != null)
            {
                throw new AssertionError(String.format(
                    "Model says %s but PaxosCommit handler didn't complete: " +
                    "topology=%s, CL=%s, blockFor=%d, responses=%d, totalReplicas=%d",
                    expected, topology, cl, blockFor, responses.size(), totalReplicaCount));
            }
            return;
        }

        if (expected == null)
        {
            throw new AssertionError(String.format(
                "PaxosCommit handler completed but model says incomplete: " +
                "topology=%s, CL=%s, blockFor=%d, responses=%d, totalReplicas=%d, status=%s",
                topology, cl, blockFor, responses.size(), totalReplicaCount, status));
        }

        if (expected == ExpectedOutcome.SUCCESS && !status.isSuccess())
        {
            throw new AssertionError(String.format(
                "Expected success but got failure: topology=%s, CL=%s, blockFor=%d, responses=%d",
                topology, cl, blockFor, responses.size()));
        }

        if (expected == ExpectedOutcome.FAILURE && status.isSuccess())
        {
            throw new AssertionError(String.format(
                "Expected failure but got success: topology=%s, CL=%s, blockFor=%d, responses=%d",
                topology, cl, blockFor, responses.size()));
        }
    }

    /**
     * Minimum replicas needed for a consistency level.
     */
    private static int minReplicasForCL(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY:
            case ONE:
            case LOCAL_ONE:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
            case LOCAL_QUORUM:
            case EACH_QUORUM:
                return 3;
            case ALL:
                return 1;
            default:
                throw new IllegalArgumentException("Unsupported CL: " + cl);
        }
    }
}
