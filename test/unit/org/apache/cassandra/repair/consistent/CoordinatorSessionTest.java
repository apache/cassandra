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

package org.apache.cassandra.repair.consistent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.CoordinatedRepairResult;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.FAILED;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.FINALIZE_PROMISED;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.PREPARED;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.PREPARING;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.REPAIRING;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class CoordinatorSessionTest extends AbstractRepairTest
{

    static CoordinatorSession.Builder createBuilder()
    {
        CoordinatorSession.Builder builder = CoordinatorSession.builder(SharedContext.Global.instance);
        builder.withState(PREPARING);
        builder.withSessionID(nextTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(Sets.newHashSet(UUID.randomUUID(), UUID.randomUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(Sets.newHashSet(RANGE1, RANGE2, RANGE3));
        builder.withParticipants(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));
        return builder;
    }

    static CoordinatorSession createSession()
    {
        return createBuilder().build();
    }

    static InstrumentedCoordinatorSession createInstrumentedSession()
    {
        MockMessaging msg = new MockMessaging();
        CoordinatorSession.Builder builder = createBuilder();
        builder.withContext(SharedContext.Global.instance.withMessaging(msg));
        return new InstrumentedCoordinatorSession(msg, builder);
    }

    private static RepairSessionResult createResult(CoordinatorSession coordinator)
    {
        return new RepairSessionResult(coordinator.sessionID, "ks", coordinator.ranges, null, false);
    }

    private static void assertMessageSent(InstrumentedCoordinatorSession coordinator, InetAddressAndPort participant, RepairMessage expected)
    {
        Assert.assertTrue(coordinator.sentMessages.containsKey(participant));
        Assert.assertEquals(1, coordinator.sentMessages.get(participant).size());
        Assert.assertEquals(expected, coordinator.sentMessages.get(participant).get(0));
    }

    private static class InstrumentedCoordinatorSession extends CoordinatorSession
    {
        private final Map<InetAddressAndPort, List<RepairMessage>> sentMessages;
        public InstrumentedCoordinatorSession(MockMessaging messaging, Builder builder)
        {
            super(builder);
            this.sentMessages = messaging.sentMessages;
        }

        Runnable onSetRepairing = null;
        boolean setRepairingCalled = false;
        public synchronized void setRepairing()
        {
            setRepairingCalled = true;
            if (onSetRepairing != null)
            {
                onSetRepairing.run();
            }
            super.setRepairing();
        }

        Runnable onFinalizeCommit = null;
        boolean finalizeCommitCalled = false;
        public synchronized void finalizeCommit()
        {
            finalizeCommitCalled = true;
            if (onFinalizeCommit != null)
            {
                onFinalizeCommit.run();
            }
            super.finalizeCommit();
        }

        Runnable onFail = null;
        boolean failCalled = false;
        public synchronized void fail()
        {
            failCalled = true;
            if (onFail != null)
            {
                onFail.run();
            }
            super.fail();
        }
    }

    /**
     * Coordinator state should only switch after all participants are set
     */
    @Test
    public void setPeerState()
    {
        CoordinatorSession session = createSession();
        Assert.assertEquals(PREPARING, session.getState());

        session.setParticipantState(PARTICIPANT1, PREPARED);
        Assert.assertEquals(PREPARING, session.getState());

        session.setParticipantState(PARTICIPANT2, PREPARED);
        Assert.assertEquals(PREPARING, session.getState());

        session.setParticipantState(PARTICIPANT3, PREPARED);
        Assert.assertEquals(PREPARED, session.getState());
    }

    @Test
    public void hasFailed()
    {
        CoordinatorSession session;

        // participant failure
        session = createSession();
        Assert.assertFalse(session.hasFailed());
        session.setParticipantState(PARTICIPANT1, FAILED);
        Assert.assertTrue(session.hasFailed());

        // coordinator failure
        session = createSession();
        Assert.assertFalse(session.hasFailed());
        session.setState(FAILED);
        Assert.assertTrue(session.hasFailed());
    }

    /**
     * Coordinator should only send out failures messages once
     */
    @Test
    public void multipleFailures()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();

        Assert.assertEquals(PREPARING, coordinator.getState());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());

        coordinator.fail();
        Assert.assertEquals(FAILED, coordinator.getState());
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            assertMessageSent(coordinator, participant, new FailSession(coordinator.sessionID));
        }

        coordinator.sentMessages.clear();
        coordinator.fail();
        Assert.assertEquals(FAILED, coordinator.getState());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
    }

    /**
     * Tests the complete coordinator side consistent repair cycle
     */
    @Test
    public void successCase()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        Promise<CoordinatedRepairResult> repairFuture = AsyncPromise.uncancellable();
        Supplier<Future<CoordinatedRepairResult>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        Future<CoordinatedRepairResult> sessionResult = coordinator.execute(sessionSupplier);

        for (InetAddressAndPort participant : PARTICIPANTS)
        {

            RepairMessage expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());
        
        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT1, true)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());
        
        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT2, true)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // set the setRepairing callback to verify the correct state when it's called
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = () -> Assert.assertEquals(PREPARED, coordinator.getState());
        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT3, true)));
        Assert.assertTrue(coordinator.setRepairingCalled);
        Assert.assertTrue(repairSubmitted.get());

        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        ArrayList<RepairSessionResult> results = Lists.newArrayList(createResult(coordinator),
                                                                    createResult(coordinator),
                                                                    createResult(coordinator));

        coordinator.sentMessages.clear();
        repairFuture.trySuccess(CoordinatedRepairResult.success(results));

        // propose messages should have been sent once all repair sessions completed successfully
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            RepairMessage expected = new FinalizePropose(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        // finalize commit messages will be sent once all participants respond with a promize to finalize
        coordinator.sentMessages.clear();
        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        coordinator.handleFinalizePromise(Message.out(Verb.FINALIZE_PROMISE_MSG, new FinalizePromise(coordinator.sessionID, PARTICIPANT1, true)));
        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        coordinator.handleFinalizePromise(Message.out(Verb.FINALIZE_PROMISE_MSG, new FinalizePromise(coordinator.sessionID, PARTICIPANT2, true)));
        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        // set the finalizeCommit callback so we can verify the state when it's called
        Assert.assertFalse(coordinator.finalizeCommitCalled);
        coordinator.onFinalizeCommit = () -> Assert.assertEquals(FINALIZE_PROMISED, coordinator.getState());
        coordinator.handleFinalizePromise(Message.out(Verb.FINALIZE_PROMISE_MSG, new FinalizePromise(coordinator.sessionID, PARTICIPANT3, true)));
        Assert.assertTrue(coordinator.finalizeCommitCalled);

        Assert.assertEquals(ConsistentSession.State.FINALIZED, coordinator.getState());
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            RepairMessage expected = new FinalizeCommit(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        Assert.assertTrue(sessionResult.isDone());
        sessionResult.syncUninterruptibly();
    }

    @Test
    public void failedRepairs()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        Promise<CoordinatedRepairResult> repairFuture = AsyncPromise.uncancellable();
        Supplier<Future<CoordinatedRepairResult>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        Future<CoordinatedRepairResult> sessionResult = coordinator.execute(sessionSupplier);
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            PrepareConsistentRequest expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT1, true)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT2, true)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // set the setRepairing callback to verify the correct state when it's called
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = () -> Assert.assertEquals(PREPARED, coordinator.getState());
        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT3, true)));
        Assert.assertTrue(coordinator.setRepairingCalled);
        Assert.assertTrue(repairSubmitted.get());

        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        List<Collection<Range<Token>>> ranges = Arrays.asList(coordinator.ranges, coordinator.ranges, coordinator.ranges);
        ArrayList<RepairSessionResult> results = Lists.newArrayList(createResult(coordinator),
                                                                    null,
                                                                    createResult(coordinator));

        coordinator.sentMessages.clear();
        Assert.assertFalse(coordinator.failCalled);
        coordinator.onFail = () -> Assert.assertEquals(REPAIRING, coordinator.getState());
        repairFuture.trySuccess(CoordinatedRepairResult.create(ranges, results));
        Assert.assertTrue(coordinator.failCalled);

        // all participants should have been notified of session failure
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            RepairMessage expected = new FailSession(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        Assert.assertTrue(sessionResult.isDone());
        Assert.assertNotNull(sessionResult.cause());
    }

    @Test
    public void failedPrepare()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        Promise<CoordinatedRepairResult> repairFuture = AsyncPromise.uncancellable();
        Supplier<Future<CoordinatedRepairResult>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        Future<CoordinatedRepairResult> sessionResult = coordinator.execute(sessionSupplier);
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            PrepareConsistentRequest expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        coordinator.sentMessages.clear();

        // participants respond to coordinator, and repair begins once all participants have responded
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT1, true)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());
        Assert.assertEquals(PREPARED, coordinator.getParticipantState(PARTICIPANT1));
        Assert.assertFalse(sessionResult.isDone());

        // participant 2 fails to prepare for consistent repair
        Assert.assertFalse(coordinator.failCalled);
        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT2, false)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());
        // we should have sent failure messages to the other participants, but not yet marked them failed internally
        assertMessageSent(coordinator, PARTICIPANT1, new FailSession(coordinator.sessionID));
        assertMessageSent(coordinator, PARTICIPANT2, new FailSession(coordinator.sessionID));
        assertMessageSent(coordinator, PARTICIPANT3, new FailSession(coordinator.sessionID));
        Assert.assertEquals(FAILED, coordinator.getParticipantState(PARTICIPANT2));
        Assert.assertEquals(PREPARED, coordinator.getParticipantState(PARTICIPANT1));
        Assert.assertEquals(PREPARING, coordinator.getParticipantState(PARTICIPANT3));
        Assert.assertFalse(sessionResult.isDone());
        Assert.assertFalse(coordinator.failCalled);
        coordinator.sentMessages.clear();

        // last outstanding response should cause repair to complete in failed state
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = Assert::fail;
        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT3, true)));
        Assert.assertTrue(coordinator.failCalled);
        Assert.assertFalse(coordinator.setRepairingCalled);
        Assert.assertFalse(repairSubmitted.get());

        // all participants that did not fail should have been notified of session failure
        RepairMessage expected = new FailSession(coordinator.sessionID);
        assertMessageSent(coordinator, PARTICIPANT1, expected);
        assertMessageSent(coordinator, PARTICIPANT3, expected);
        Assert.assertFalse(coordinator.sentMessages.containsKey(PARTICIPANT2));

        Assert.assertTrue(sessionResult.isDone());
        Assert.assertNotNull(sessionResult.cause());
    }

    @Test
    public void failedPropose()
    {
        InstrumentedCoordinatorSession coordinator = createInstrumentedSession();
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        Promise<CoordinatedRepairResult> repairFuture = AsyncPromise.uncancellable();
        Supplier<Future<CoordinatedRepairResult>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(coordinator.sentMessages.isEmpty());
        Future<CoordinatedRepairResult> sessionResult = coordinator.execute(sessionSupplier);

        for (InetAddressAndPort participant : PARTICIPANTS)
        {

            RepairMessage expected = new PrepareConsistentRequest(coordinator.sessionID, COORDINATOR, new HashSet<>(PARTICIPANTS));
            assertMessageSent(coordinator, participant, expected);
        }

        // participants respond to coordinator, and repair begins once all participants have responded with success
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT1, true)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT2, true)));
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());

        // set the setRepairing callback to verify the correct state when it's called
        Assert.assertFalse(coordinator.setRepairingCalled);
        coordinator.onSetRepairing = () -> Assert.assertEquals(PREPARED, coordinator.getState());
        coordinator.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP, new PrepareConsistentResponse(coordinator.sessionID, PARTICIPANT3, true)));
        Assert.assertTrue(coordinator.setRepairingCalled);
        Assert.assertTrue(repairSubmitted.get());

        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        ArrayList<RepairSessionResult> results = Lists.newArrayList(createResult(coordinator),
                                                                    createResult(coordinator),
                                                                    createResult(coordinator));

        coordinator.sentMessages.clear();
        repairFuture.trySuccess(CoordinatedRepairResult.success(results));

        // propose messages should have been sent once all repair sessions completed successfully
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            RepairMessage expected = new FinalizePropose(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        // finalize commit messages will be sent once all participants respond with a promize to finalize
        coordinator.sentMessages.clear();
        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        coordinator.handleFinalizePromise(Message.out(Verb.FINALIZE_PROMISE_MSG, new FinalizePromise(coordinator.sessionID, PARTICIPANT1, true)));
        Assert.assertEquals(ConsistentSession.State.REPAIRING, coordinator.getState());

        Assert.assertFalse(coordinator.failCalled);
        coordinator.handleFinalizePromise(Message.out(Verb.FINALIZE_PROMISE_MSG, new FinalizePromise(coordinator.sessionID, PARTICIPANT2, false)));
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());
        Assert.assertTrue(coordinator.failCalled);

        // additional success messages should be ignored
        Assert.assertFalse(coordinator.finalizeCommitCalled);
        coordinator.onFinalizeCommit = Assert::fail;
        coordinator.handleFinalizePromise(Message.out(Verb.FINALIZE_PROMISE_MSG, new FinalizePromise(coordinator.sessionID, PARTICIPANT3, true)));
        Assert.assertFalse(coordinator.finalizeCommitCalled);
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());

        // failure messages should have been sent to all participants
        for (InetAddressAndPort participant : PARTICIPANTS)
        {
            RepairMessage expected = new FailSession(coordinator.sessionID);
            assertMessageSent(coordinator, participant, expected);
        }

        Assert.assertTrue(sessionResult.isDone());
        Assert.assertNotNull(sessionResult.cause());
    }
}
