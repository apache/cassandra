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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ResponseHandlerPropertyTestBase;
import org.apache.cassandra.service.paxos.Commit.Proposal;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.quicktheories.core.Gen;

import static org.quicktheories.QuickTheory.qt;

/**
 * Property-based tests for PaxosPropose response tracking logic.
 * Tests through actual PaxosPropose instances, calling onResponse/onFailure
 * and checking that the onDone callback fires at the correct point.
 *
 * Tests SERIAL and LOCAL_SERIAL only. Uses an independent model for the quorum math.
 */
public class PaxosProposePropertyTest extends ResponseHandlerPropertyTestBase
{
    private static final ImmutableList<ConsistencyLevel> CONSISTENCY_LEVELS = ImmutableList.of(
        ConsistencyLevel.SERIAL,
        ConsistencyLevel.LOCAL_SERIAL
    );

    @Override
    protected List<ConsistencyLevel> consistencyLevels()
    {
        return CONSISTENCY_LEVELS;
    }

    // ========================================
    // Data Structures
    // ========================================

    enum ProposeResponseType { ACCEPT, REFUSE, FAIL }

    static class ProposeResponseMessage
    {
        final int replicaIdx;
        final ProposeResponseType type;

        ProposeResponseMessage(int replicaIdx, ProposeResponseType type)
        {
            this.replicaIdx = replicaIdx;
            this.type = type;
        }

        @Override
        public String toString()
        {
            return String.format("replica=%d, %s", replicaIdx, type);
        }
    }

    static class ProposeScenario
    {
        final ConsistencyLevel cl;
        final Paxos.Participants participants;
        final List<ProposeResponseMessage> responses;

        ProposeScenario(ConsistencyLevel cl, Paxos.Participants participants,
                        List<ProposeResponseMessage> responses)
        {
            this.cl = cl;
            this.participants = participants;
            this.responses = responses;
        }
    }

    static class ProposeTestCase
    {
        final TopologyConfig topology;
        final List<ProposeScenario> scenarios;

        ProposeTestCase(TopologyConfig topology, List<ProposeScenario> scenarios)
        {
            this.topology = topology;
            this.scenarios = scenarios;
        }

        @Override
        public String toString()
        {
            return String.format("topology=%s, scenarios=%d", topology, scenarios.size());
        }
    }

    // ========================================
    // Independent Model
    // ========================================

    /**
     * Independent model for PaxosPropose completion.
     *
     * Rules:
     * - SUCCESS: accepts >= required
     * - Can still succeed: refusals == 0 AND required <= participants - failures
     * - FAILURE: cannot succeed (any refusal, or too many failures)
     */
    static ExpectedOutcome calculateProposeOutcome(int participants, int required,
                                                   List<ProposeResponseMessage> appliedResponses)
    {
        int accepts = 0, refusals = 0, failures = 0;
        for (ProposeResponseMessage r : appliedResponses)
        {
            switch (r.type)
            {
                case ACCEPT: accepts++; break;
                case REFUSE: refusals++; break;
                case FAIL: failures++; break;
            }
        }

        if (accepts >= required)
            return ExpectedOutcome.SUCCESS;

        boolean canSucceed = refusals == 0 && required <= participants - failures;
        if (canSucceed)
            return null; // still in progress

        return ExpectedOutcome.FAILURE;
    }

    // ========================================
    // PaxosPropose construction helpers
    // ========================================

    private static Paxos.Participants buildParticipants(ConsistencyLevel cl, Keyspace ks) throws Exception
    {
        var token = Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(0));
        TableMetadata table = ks.getColumnFamilyStores().iterator().next().metadata();
        Predicate<Replica> allAlive = r -> true;
        return Paxos.Participants.get(ClusterMetadata.current(), table, token, cl, allAlive);
    }

    private static Proposal buildEmptyProposal(Keyspace ks)
    {
        TableMetadata table = ks.getColumnFamilyStores().iterator().next().metadata();
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(0));
        return Proposal.empty(Ballot.none(), key, table);
    }

    // ========================================
    // Test Case Generator
    // ========================================

    /**
     * Generates test cases. Builds Participants from the actual replication strategy
     * to ensure quorum parameters match reality, then generates responses sized
     * to the actual electorate.
     */
    Gen<ProposeTestCase> proposeTestCaseGen()
    {
        return random -> {
            TopologyConfig topology = topologyGen().generate(random);

            Keyspace ks;
            try
            {
                ks = getOrCreateKeyspace(topology);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            List<ProposeScenario> scenarios = new ArrayList<>();

            for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
            {
                Paxos.Participants participants;
                try
                {
                    participants = buildParticipants(cl, ks);
                }
                catch (Exception e)
                {
                    continue;
                }

                int pollSize = participants.sizeOfPoll();
                if (pollSize <= 0 || participants.sizeOfConsensusQuorum > pollSize)
                    continue;

                Gen<List<ProposeResponseMessage>> responseGen =
                    typedResponseSequenceGen(pollSize, ProposeResponseType.values(), ProposeResponseMessage::new);

                for (int i = 0; i < 25; i++)
                    scenarios.add(new ProposeScenario(cl, participants, responseGen.generate(random)));

                for (ProposeResponseType type : ProposeResponseType.values())
                    scenarios.add(new ProposeScenario(cl, participants,
                        allSameTypeSequence(pollSize, type, ProposeResponseMessage::new)));
            }

            return new ProposeTestCase(topology, scenarios);
        };
    }

    // ========================================
    // Property Test
    // ========================================

    @Test
    public void paxosProposeSignaling()
    {
        qt()
            .withExamples(250)
            .forAll(proposeTestCaseGen())
            .assuming(testCase -> !testCase.scenarios.isEmpty())
            .checkAssert(testCase -> {
                try
                {
                    Keyspace ks = getOrCreateKeyspace(testCase.topology);
                    Proposal proposal = buildEmptyProposal(ks);

                    for (ProposeScenario scenario : testCase.scenarios)
                        verifyScenario(testCase.topology, scenario, proposal);
                }
                catch (Throwable e)
                {
                    if (e instanceof AssertionError)
                        throw (AssertionError) e;
                    throw new AssertionError("Test setup failed: " + e.getMessage(), e);
                }
            });
    }

    private void verifyScenario(TopologyConfig topology, ProposeScenario scenario, Proposal proposal)
    {
        Paxos.Participants participants = scenario.participants;
        int consensusQuorum = participants.sizeOfConsensusQuorum;
        int pollSize = participants.sizeOfPoll();

        AtomicReference<PaxosPropose.Status> statusCapture = new AtomicReference<>();
        PaxosPropose<Consumer<PaxosPropose.Status>> propose = new PaxosPropose<>(
            proposal, pollSize, consensusQuorum, statusCapture::set);

        List<ProposeResponseMessage> applied = new ArrayList<>();
        boolean completed = false;

        for (ProposeResponseMessage msg : scenario.responses)
        {
            if (statusCapture.get() != null)
            {
                completed = true;
                break;
            }

            InetAddressAndPort from = participants.voter(msg.replicaIdx);

            switch (msg.type)
            {
                case ACCEPT:
                    propose.onResponse(PaxosState.AcceptResult.SUCCESS, from);
                    break;
                case REFUSE:
                    propose.onResponse(new PaxosState.AcceptResult(Ballot.none()), from);
                    break;
                case FAIL:
                    propose.onFailure(from, RequestFailure.forReason(RequestFailureReason.TIMEOUT));
                    break;
            }

            applied.add(msg);

            if (statusCapture.get() != null)
                completed = true;

            ExpectedOutcome expected = calculateProposeOutcome(pollSize, consensusQuorum, applied);

            if (expected != null && !completed)
            {
                throw new AssertionError(String.format(
                    "Model says %s but PaxosPropose not complete: topology=%s, CL=%s, " +
                    "applied=%d, quorum=%d/%d, responses=%s",
                    expected, topology, scenario.cl,
                    applied.size(), consensusQuorum, pollSize, applied));
            }

            if (expected == null && completed)
            {
                throw new AssertionError(String.format(
                    "PaxosPropose completed but model says incomplete: topology=%s, CL=%s, " +
                    "applied=%d, quorum=%d/%d, status=%s, responses=%s",
                    topology, scenario.cl,
                    applied.size(), consensusQuorum, pollSize,
                    statusCapture.get(), applied));
            }
        }

        if (!completed)
        {
            ExpectedOutcome expected = calculateProposeOutcome(pollSize, consensusQuorum, applied);
            if (expected != null)
            {
                throw new AssertionError(String.format(
                    "After all %d responses, model says %s but PaxosPropose not complete: " +
                    "topology=%s, CL=%s, quorum=%d/%d",
                    applied.size(), expected, topology, scenario.cl,
                    consensusQuorum, pollSize));
            }
        }
    }
}
