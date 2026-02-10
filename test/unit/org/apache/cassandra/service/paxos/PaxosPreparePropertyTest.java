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
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.service.paxos.PaxosPrepare.Permitted;
import org.apache.cassandra.service.paxos.PaxosPrepare.Rejected;
import org.apache.cassandra.service.paxos.PaxosPrepare.Response;
import org.apache.cassandra.service.paxos.PaxosPrepare.Status;
import org.apache.cassandra.service.paxos.PaxosState.MaybePromise;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.quicktheories.core.Gen;

import static java.util.Collections.emptyMap;
import static org.quicktheories.QuickTheory.qt;

/**
 * Property-based tests for PaxosPrepare response tracking logic.
 * Tests the quorum counting aspects: when does a prepare achieve a quorum of
 * permissions, and when is failure due to too many failures.
 *
 * Simplified model: all responses agree on the same "latest commit" (no divergence).
 * Tests SERIAL and LOCAL_SERIAL only.
 */
public class PaxosPreparePropertyTest extends ResponseHandlerPropertyTestBase
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

    enum PrepareResponseType { PERMIT_WITH_LATEST, REJECT, FAIL }

    static class PrepareResponseMessage
    {
        final int replicaIdx;
        final PrepareResponseType type;

        PrepareResponseMessage(int replicaIdx, PrepareResponseType type)
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

    static class PrepareScenario
    {
        final ConsistencyLevel cl;
        final Paxos.Participants participants;
        final List<PrepareResponseMessage> responses;

        PrepareScenario(ConsistencyLevel cl, Paxos.Participants participants,
                        List<PrepareResponseMessage> responses)
        {
            this.cl = cl;
            this.participants = participants;
            this.responses = responses;
        }
    }

    static class PrepareTestCase
    {
        final TopologyConfig topology;
        final List<PrepareScenario> scenarios;

        PrepareTestCase(TopologyConfig topology, List<PrepareScenario> scenarios)
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
     * Simplified independent model for PaxosPrepare completion.
     *
     * All permitted responses are treated as having the latest commit.
     *
     * SUPERSEDED: any rejection received (immediate termination)
     * SUCCESS: withLatest >= sizeOfConsensusQuorum (and no rejection)
     * FAILURE: failures + sizeOfConsensusQuorum > sizeOfPoll
     * INCOMPLETE: otherwise
     */
    static ExpectedOutcome calculatePrepareOutcome(int sizeOfPoll, int sizeOfConsensusQuorum,
                                                   List<PrepareResponseMessage> appliedResponses)
    {
        int withLatest = 0;
        int failures = 0;

        for (PrepareResponseMessage r : appliedResponses)
        {
            switch (r.type)
            {
                case PERMIT_WITH_LATEST: withLatest++; break;
                case REJECT: return ExpectedOutcome.FAILURE;
                case FAIL: failures++; break;
            }
        }

        if (withLatest >= sizeOfConsensusQuorum)
            return ExpectedOutcome.SUCCESS;

        if (failures + sizeOfConsensusQuorum > sizeOfPoll)
            return ExpectedOutcome.FAILURE;

        return null;
    }

    // ========================================
    // PaxosPrepare construction helpers
    // ========================================

    private static Paxos.Participants buildParticipants(ConsistencyLevel cl, Keyspace ks) throws Exception
    {
        var token = Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(0));
        TableMetadata table = ks.getColumnFamilyStores().iterator().next().metadata();
        Predicate<Replica> allAlive = r -> true;
        return Paxos.Participants.get(ClusterMetadata.current(), table, token, cl, allAlive);
    }

    private static Committed buildCommittedNone(Keyspace ks)
    {
        TableMetadata table = ks.getColumnFamilyStores().iterator().next().metadata();
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(0));
        return Committed.none(key, table);
    }

    private static Response buildPermitWithLatest(Committed latestCommitted)
    {
        return new Permitted(
            MaybePromise.Outcome.PROMISE, 0L, null, latestCommitted,
            null, true, emptyMap(), Epoch.EMPTY, null
        );
    }

    private static Response buildRejected()
    {
        return new Rejected(Ballot.none());
    }

    // ========================================
    // Test Case Generator
    // ========================================

    /**
     * Generates test cases. Builds Participants from the actual replication strategy
     * to ensure quorum parameters match reality, then generates responses sized
     * to the actual electorate.
     */
    Gen<PrepareTestCase> prepareTestCaseGen()
    {
        return random -> {
            TopologyConfig topology = topologyGen().generate(random);

            Keyspace ks;
            try { ks = getOrCreateKeyspace(topology); }
            catch (Exception e) { throw new RuntimeException(e); }

            List<PrepareScenario> scenarios = new ArrayList<>();

            for (ConsistencyLevel cl : CONSISTENCY_LEVELS)
            {
                Paxos.Participants participants;
                try { participants = buildParticipants(cl, ks); }
                catch (Exception e) { continue; }

                int pollSize = participants.sizeOfPoll();
                if (pollSize <= 0 || participants.sizeOfConsensusQuorum > pollSize)
                    continue;

                Gen<List<PrepareResponseMessage>> responseGen =
                    typedResponseSequenceGen(pollSize, PrepareResponseType.values(), PrepareResponseMessage::new);

                for (int i = 0; i < 25; i++)
                    scenarios.add(new PrepareScenario(cl, participants, responseGen.generate(random)));

                for (PrepareResponseType type : PrepareResponseType.values())
                    scenarios.add(new PrepareScenario(cl, participants,
                        allSameTypeSequence(pollSize, type, PrepareResponseMessage::new)));
            }

            return new PrepareTestCase(topology, scenarios);
        };
    }

    // ========================================
    // Property Test
    // ========================================

    @Test
    public void paxosPrepareQuorumCounting()
    {
        qt()
            .withExamples(250)
            .forAll(prepareTestCaseGen())
            .assuming(testCase -> !testCase.scenarios.isEmpty())
            .checkAssert(testCase -> {
                try
                {
                    Keyspace ks = getOrCreateKeyspace(testCase.topology);
                    Committed committedNone = buildCommittedNone(ks);
                    TableMetadata table = ks.getColumnFamilyStores().iterator().next().metadata();
                    DecoratedKey key = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(0));

                    for (PrepareScenario scenario : testCase.scenarios)
                    {
                        Paxos.Participants participants = scenario.participants;
                        int consensusQuorum = participants.sizeOfConsensusQuorum;
                        int pollSize = participants.sizeOfPoll();

                        AtomicReference<Status> statusCapture = new AtomicReference<>();
                        PaxosPrepare prepare = new PaxosPrepare(participants,
                            new PaxosPrepare.Request(Ballot.none(), participants.electorate, key, table, true, true),
                            false, statusCapture::set);

                        List<PrepareResponseMessage> applied = new ArrayList<>();
                        boolean completed = false;

                        for (PrepareResponseMessage msg : scenario.responses)
                        {
                            if (statusCapture.get() != null)
                            {
                                completed = true;
                                break;
                            }

                            InetAddressAndPort from = participants.voter(msg.replicaIdx);

                            switch (msg.type)
                            {
                                case PERMIT_WITH_LATEST:
                                    prepare.onResponse(buildPermitWithLatest(committedNone), from);
                                    break;
                                case REJECT:
                                    prepare.onResponse(buildRejected(), from);
                                    break;
                                case FAIL:
                                    prepare.onFailure(from, RequestFailure.forReason(RequestFailureReason.TIMEOUT));
                                    break;
                            }

                            applied.add(msg);

                            if (statusCapture.get() != null)
                                completed = true;

                            ExpectedOutcome expected = calculatePrepareOutcome(pollSize, consensusQuorum, applied);

                            if (expected != null && !completed)
                            {
                                throw new AssertionError(String.format(
                                    "Model says %s but PaxosPrepare not complete: topology=%s, CL=%s, " +
                                    "applied=%d, quorum=%d/%d, responses=%s",
                                    expected, testCase.topology, scenario.cl,
                                    applied.size(), consensusQuorum, pollSize, applied));
                            }

                            if (expected == null && completed)
                            {
                                throw new AssertionError(String.format(
                                    "PaxosPrepare completed but model says incomplete: topology=%s, CL=%s, " +
                                    "applied=%d, quorum=%d/%d, status=%s, responses=%s",
                                    testCase.topology, scenario.cl,
                                    applied.size(), consensusQuorum, pollSize,
                                    statusCapture.get(), applied));
                            }
                        }

                        if (!completed)
                        {
                            ExpectedOutcome expected = calculatePrepareOutcome(pollSize, consensusQuorum, applied);
                            if (expected != null)
                            {
                                throw new AssertionError(String.format(
                                    "After all %d responses, model says %s but PaxosPrepare not complete: " +
                                    "topology=%s, CL=%s, quorum=%d/%d",
                                    applied.size(), expected, testCase.topology, scenario.cl,
                                    consensusQuorum, pollSize));
                            }
                        }
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
}
