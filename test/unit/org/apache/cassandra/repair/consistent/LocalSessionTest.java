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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.KeyspaceRepairManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.*;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.psjava.util.AssertStatus.assertTrue;

public class LocalSessionTest extends AbstractRepairTest
{
    private static final UUID TID1 = UUID.randomUUID();
    private static final UUID TID2 = UUID.randomUUID();

    static LocalSession.Builder createBuilder()
    {
        LocalSession.Builder builder = LocalSession.builder(SharedContext.Global.instance);
        builder.withState(PREPARING);
        builder.withSessionID(nextTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(Sets.newHashSet(TID1, TID2));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(Sets.newHashSet(RANGE1, RANGE2, RANGE3));
        builder.withParticipants(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        long now = FBUtilities.nowInSeconds();
        builder.withStartedAt(now);
        builder.withLastUpdate(now);

        return builder;
    }

    static LocalSession createSession()
    {
        return createBuilder().build();
    }

    private static void assertValidationFailure(Consumer<LocalSession.Builder> consumer)
    {
        try
        {
            LocalSession.Builder builder = createBuilder();
            consumer.accept(builder);
            builder.build();
            Assert.fail("Expected assertion error");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private static void assertNoMessagesSent(InstrumentedLocalSessions sessions, InetAddressAndPort to)
    {
        Assert.assertNull(sessions.sentMessages.get(to));
    }

    private static void assertMessagesSent(InstrumentedLocalSessions sessions, InetAddressAndPort to, RepairMessage... expected)
    {
        Assert.assertEquals(Lists.newArrayList(expected), sessions.sentMessages.get(to));
    }

    static class InstrumentedLocalSessions extends LocalSessions
    {
        final Map<InetAddressAndPort, List<RepairMessage>> sentMessages;

        public InstrumentedLocalSessions()
        {
            this(new MockMessaging());
        }

        private InstrumentedLocalSessions(MockMessaging messaging)
        {
            super(SharedContext.Global.instance.withMessaging(messaging));
            sentMessages = messaging.sentMessages;
        }

        @Override
        protected void sendMessage(InetAddressAndPort destination, Message<? extends RepairMessage> message)
        {
            if (!sentMessages.containsKey(destination))
            {
                sentMessages.put(destination, new ArrayList<>());
            }
            sentMessages.get(destination).add(message.payload);
        }

        AsyncPromise<List<Void>> prepareSessionFuture = null;
        boolean prepareSessionCalled = false;

        @Override
        Future<List<Void>> prepareSession(KeyspaceRepairManager repairManager,
                                          TimeUUID sessionID,
                                          Collection<ColumnFamilyStore> tables,
                                          RangesAtEndpoint ranges,
                                          ExecutorService executor,
                                          BooleanSupplier isCancelled)
        {
            prepareSessionCalled = true;
            if (prepareSessionFuture != null)
            {
                return prepareSessionFuture;
            }
            else
            {
                return super.prepareSession(repairManager, sessionID, tables, ranges, executor, isCancelled);
            }
        }

        boolean failSessionCalled = false;
        public void failSession(TimeUUID sessionID, boolean sendMessage)
        {
            failSessionCalled = true;
            super.failSession(sessionID, sendMessage);
        }

        public LocalSession prepareForTest(TimeUUID sessionID)
        {
            prepareSessionFuture = new AsyncPromise<>();
            handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS)).from(PARTICIPANT1).build());
            prepareSessionFuture.trySuccess(null);
            sentMessages.clear();
            return getSession(sessionID);
        }

        @Override
        protected InetAddressAndPort getBroadcastAddressAndPort()
        {
            return PARTICIPANT1;
        }

        protected boolean isAlive(InetAddressAndPort address)
        {
            return true;
        }

        protected boolean isNodeInitialized()
        {
            return true;
        }

        public Map<TimeUUID, Integer> completedSessions = new HashMap<>();

        protected void sessionCompleted(LocalSession session)
        {
            TimeUUID sessionID = session.sessionID;
            int calls = completedSessions.getOrDefault(sessionID, 0);
            completedSessions.put(sessionID, calls + 1);
        }

        boolean sessionHasData = false;
        protected boolean sessionHasData(LocalSession session)
        {
            return sessionHasData;
        }
    }

    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "localsessiontest").build();
        SchemaLoader.createKeyspace("localsessiontest", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    @Before
    public void setup()
    {
        // clear out any data from previous test runs
        ColumnFamilyStore repairCfs = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.REPAIRS);
        repairCfs.truncateBlocking();
    }

    private static TimeUUID registerSession()
    {
        return registerSession(cfs, true, true);
    }

    @Test
    public void validation()
    {
        assertValidationFailure(b -> b.withState(null));
        assertValidationFailure(b -> b.withSessionID(null));
        assertValidationFailure(b -> b.withCoordinator(null));
        assertValidationFailure(b -> b.withTableIds(null));
        assertValidationFailure(b -> b.withTableIds(new HashSet<>()));
        assertValidationFailure(b -> b.withRepairedAt(-1));
        assertValidationFailure(b -> b.withRanges(null));
        assertValidationFailure(b -> b.withRanges(new HashSet<>()));
        assertValidationFailure(b -> b.withParticipants(null));
        assertValidationFailure(b -> b.withParticipants(new HashSet<>()));
        assertValidationFailure(b -> b.withStartedAt(0));
        assertValidationFailure(b -> b.withLastUpdate(0));
    }

    /**
     * Test that sessions are loaded and saved properly
     */
    @Test
    public void persistence()
    {
        LocalSessions sessions = new LocalSessions(SharedContext.Global.instance);
        LocalSession expected = createSession();
        sessions.save(expected);
        LocalSession actual = sessions.loadUnsafe(expected.sessionID);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void prepareSuccessCase()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // replacing future so we can inspect state before and after anti compaction callback
        sessions.prepareSessionFuture = new AsyncPromise<>();
        Assert.assertFalse(sessions.prepareSessionCalled);
        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS)).from(PARTICIPANT1).build());
        Assert.assertTrue(sessions.prepareSessionCalled);
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // anti compaction hasn't finished yet, so state in memory and on disk should be PREPARING
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // anti compaction has now finished, so state in memory and on disk should be PREPARED
        sessions.prepareSessionFuture.trySuccess(null);
        session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // ...and we should have sent a success message back to the coordinator
        assertMessagesSent(sessions, COORDINATOR, new PrepareConsistentResponse(sessionID, PARTICIPANT1, true));
    }

    /**
     * If anti compactionn fails, we should fail the session locally,
     * and send a failure message back to the coordinator
     */
    @Test
    public void prepareAntiCompactFailure()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // replacing future so we can inspect state before and after anti compaction callback
        sessions.prepareSessionFuture = new AsyncPromise<>();
        Assert.assertFalse(sessions.prepareSessionCalled);
        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS)).from(PARTICIPANT1).build());
        Assert.assertTrue(sessions.prepareSessionCalled);
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // anti compaction hasn't finished yet, so state in memory and on disk should be PREPARING
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // anti compaction has now finished, so state in memory and on disk should be PREPARED
        sessions.prepareSessionFuture.tryFailure(new RuntimeException());
        session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // ...and we should have sent a success message back to the coordinator
        assertMessagesSent(sessions, COORDINATOR, new PrepareConsistentResponse(sessionID, PARTICIPANT1, false));

    }

    /**
     * If a ParentRepairSession wasn't previously created, we shouldn't
     * create a session locally, but we should send a failure message to
     * the coordinator.
     */
    @Test
    public void prepareWithNonExistantParentSession()
    {
        TimeUUID sessionID = nextTimeUUID();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS)).from(PARTICIPANT1).build());
        Assert.assertNull(sessions.getSession(sessionID));
        assertMessagesSent(sessions, COORDINATOR, new PrepareConsistentResponse(sessionID, PARTICIPANT1, false));
    }

    /**
     * If the session is cancelled mid-prepare, the isCancelled boolean supplier should start returning true
     */
    @Test
    public void prepareCancellation()
    {
        TimeUUID sessionID = registerSession();
        AtomicReference<BooleanSupplier> isCancelledRef = new AtomicReference<>();
        Promise<List<Void>> future = new AsyncPromise<>();

        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions() {
            Future<List<Void>> prepareSession(KeyspaceRepairManager repairManager, TimeUUID sessionID, Collection<ColumnFamilyStore> tables, RangesAtEndpoint ranges, ExecutorService executor, BooleanSupplier isCancelled)
            {
                isCancelledRef.set(isCancelled);
                return future;
            }
        };
        sessions.start();

        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS)).from(PARTICIPANT1).build());

        BooleanSupplier isCancelled = isCancelledRef.get();
        Assert.assertNotNull(isCancelled);
        Assert.assertFalse(isCancelled.getAsBoolean());
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        sessions.failSession(sessionID, false);
        Assert.assertTrue(isCancelled.getAsBoolean());

        // now that the session has failed, it send a negative response to the coordinator (even if the anti-compaction completed successfully)
        future.trySuccess(null);
        assertMessagesSent(sessions, COORDINATOR, new PrepareConsistentResponse(sessionID, PARTICIPANT1, false));
    }

    @Test
    public void maybeSetRepairing()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        LocalSession session = sessions.prepareForTest(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        sessions.sentMessages.clear();
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    /**
     * Multiple calls to maybeSetRepairing shouldn't cause any problems
     */
    @Test
    public void maybeSetRepairingDuplicates()
    {

        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        LocalSession session = sessions.prepareForTest(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        // initial set
        sessions.sentMessages.clear();
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // repeated call 1
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // repeated call 2
        sessions.maybeSetRepairing(sessionID);
        Assert.assertEquals(REPAIRING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    /**
     * We shouldn't fail if we don't have a session for the given session id
     */
    @Test
    public void maybeSetRepairingNonExistantSession()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        TimeUUID fakeID = nextTimeUUID();
        sessions.maybeSetRepairing(fakeID);
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    /**
     * In the success case, session state should be set to FINALIZE_PROMISED and
     * persisted, and a FinalizePromise message should be sent back to the coordinator
     */
    @Test
    public void finalizeProposeSuccessCase()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to preparing
        LocalSession session = sessions.prepareForTest(sessionID);
        sessions.maybeSetRepairing(sessionID);

        //
        Assert.assertEquals(REPAIRING, session.getState());

        // should send a promised message to coordinator and set session state accordingly
        sessions.sentMessages.clear();
        sessions.handleFinalizeProposeMessage(Message.builder(Verb.FINALIZE_PROPOSE_MSG, new FinalizePropose(sessionID)).from(COORDINATOR).build());
        Assert.assertEquals(FINALIZE_PROMISED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        assertMessagesSent(sessions, COORDINATOR, new FinalizePromise(sessionID, PARTICIPANT1, true));
    }

    /**
     * Trying to propose finalization when the session isn't in the repaired
     * state should fail the session and send a failure message to the proposer
     */
    @Test
    public void finalizeProposeInvalidStateFailure()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        LocalSession session = sessions.prepareForTest(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        // should fail the session and send a failure message to the coordinator
        sessions.sentMessages.clear();
        sessions.handleFinalizeProposeMessage(Message.builder(Verb.FINALIZE_PROPOSE_MSG, new FinalizePropose(sessionID)).from(COORDINATOR).build());
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        assertMessagesSent(sessions, COORDINATOR, new FailSession(sessionID));
    }

    @Test
    public void finalizeProposeNonExistantSessionFailure()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        TimeUUID fakeID = nextTimeUUID();
        sessions.handleFinalizeProposeMessage(Message.builder(Verb.FINALIZE_PROPOSE_MSG, new FinalizePropose(fakeID)).from(COORDINATOR).build());
        Assert.assertNull(sessions.getSession(fakeID));
        assertMessagesSent(sessions, COORDINATOR, new FailSession(fakeID));
    }

    /**
     * Session state should be set to finalized, sstables should be promoted
     * to repaired. No messages should be sent to the coordinator
     */
    @Test
    public void finalizeCommitSuccessCase()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to finalized promised
        sessions.prepareForTest(sessionID);
        sessions.maybeSetRepairing(sessionID);
        sessions.handleFinalizeProposeMessage(Message.builder(Verb.FINALIZE_PROPOSE_MSG, new FinalizePropose(sessionID)).from(COORDINATOR).build());

        Assert.assertEquals(0, (int) sessions.completedSessions.getOrDefault(sessionID, 0));
        sessions.sentMessages.clear();
        LocalSession session = sessions.getSession(sessionID);
        sessions.handleFinalizeCommitMessage(Message.builder(Verb.FINALIZE_COMMIT_MSG, new FinalizeCommit(sessionID)).from(PARTICIPANT1).build());

        Assert.assertEquals(FINALIZED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
        Assert.assertEquals(1, (int) sessions.completedSessions.getOrDefault(sessionID, 0));
    }

    @Test
    public void finalizeCommitNonExistantSession()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        TimeUUID fakeID = nextTimeUUID();
        sessions.handleFinalizeCommitMessage(Message.builder(Verb.FINALIZE_COMMIT_MSG, new FinalizeCommit(fakeID)).from(PARTICIPANT1).build());
        Assert.assertNull(sessions.getSession(fakeID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    @Test
    public void failSession()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        LocalSession session = sessions.prepareForTest(sessionID);
        Assert.assertEquals(PREPARED, session.getState());
        sessions.sentMessages.clear();

        // fail session
        Assert.assertEquals(0, (int) sessions.completedSessions.getOrDefault(sessionID, 0));
        sessions.failSession(sessionID);
        Assert.assertEquals(FAILED, session.getState());
        assertMessagesSent(sessions, COORDINATOR, new FailSession(sessionID));
        Assert.assertEquals(1, (int) sessions.completedSessions.getOrDefault(sessionID, 0));
    }

    /**
     * Session should be failed, but no messages should be sent
     */
    @Test
    public void handleFailMessage()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        LocalSession session = sessions.prepareForTest(sessionID);
        Assert.assertEquals(PREPARED, session.getState());
        sessions.sentMessages.clear();

        sessions.handleFailSessionMessage(PARTICIPANT1, new FailSession(sessionID));
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    @Test
    public void sendStatusRequest() throws Exception
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        LocalSession session = sessions.prepareForTest(sessionID);

        sessions.sentMessages.clear();
        sessions.sendStatusRequest(session);

        assertNoMessagesSent(sessions, PARTICIPANT1);
        StatusRequest expected = new StatusRequest(sessionID);
        assertMessagesSent(sessions, PARTICIPANT2, expected);
        assertMessagesSent(sessions, PARTICIPANT3, expected);
    }

    @Test
    public void handleStatusRequest() throws Exception
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        LocalSession session = sessions.prepareForTest(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        sessions.sentMessages.clear();
        sessions.handleStatusRequest(PARTICIPANT2, new StatusRequest(sessionID));
        assertNoMessagesSent(sessions, PARTICIPANT1);
        assertMessagesSent(sessions, PARTICIPANT2, new StatusResponse(sessionID, PREPARED));
        assertNoMessagesSent(sessions, PARTICIPANT3);
    }

    @Test
    public void handleStatusRequestNoSession() throws Exception
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.sentMessages.clear();
        TimeUUID sessionID = nextTimeUUID();
        sessions.handleStatusRequest(PARTICIPANT2, new StatusRequest(sessionID));
        assertNoMessagesSent(sessions, PARTICIPANT1);
        assertMessagesSent(sessions, PARTICIPANT2, new StatusResponse(sessionID, FAILED));
        assertNoMessagesSent(sessions, PARTICIPANT3);
    }

    @Test
    public void handleStatusResponseFinalized() throws Exception
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        LocalSession session = sessions.prepareForTest(sessionID);
        session.setState(FINALIZE_PROMISED);

        sessions.handleStatusResponse(PARTICIPANT1, new StatusResponse(sessionID, FINALIZED));
        Assert.assertEquals(FINALIZED, session.getState());
    }

    @Test
    public void handleStatusResponseFinalizedRedundant() throws Exception
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        LocalSession session = sessions.prepareForTest(sessionID);
        session.setState(FINALIZED);

        sessions.handleStatusResponse(PARTICIPANT1, new StatusResponse(sessionID, FINALIZED));
        Assert.assertEquals(FINALIZED, session.getState());
    }

    @Test
    public void handleStatusResponseFailed() throws Exception
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        LocalSession session = sessions.prepareForTest(sessionID);
        session.setState(FINALIZE_PROMISED);

        sessions.handleStatusResponse(PARTICIPANT1, new StatusResponse(sessionID, FAILED));
        Assert.assertEquals(FAILED, session.getState());
    }

    @Test
    public void handleStatusResponseFailedRedundant() throws Exception
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        LocalSession session = sessions.prepareForTest(sessionID);
        session.setState(FAILED);

        sessions.handleStatusResponse(PARTICIPANT1, new StatusResponse(sessionID, FAILED));
        Assert.assertEquals(FAILED, session.getState());
    }

    @Test
    public void handleStatusResponseNoop() throws Exception
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        LocalSession session = sessions.prepareForTest(sessionID);
        session.setState(REPAIRING);

        sessions.handleStatusResponse(PARTICIPANT1, new StatusResponse(sessionID, FINALIZE_PROMISED));
        Assert.assertEquals(REPAIRING, session.getState());
    }

    @Test
    public void handleStatusResponseNoSession() throws Exception
    {
        TimeUUID sessionID = nextTimeUUID();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.handleStatusResponse(PARTICIPANT1, new StatusResponse(sessionID, FINALIZE_PROMISED));
        Assert.assertNull(sessions.getSession(sessionID));
    }

    /**
     * Check all states (except failed)
     */
    @Test
    public void isSessionInProgress()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.prepareSessionFuture = new AsyncPromise<>();  // prevent moving to prepared
        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS)).from(PARTICIPANT1).build());

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(PREPARED);
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(REPAIRING);
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(FINALIZE_PROMISED);
        Assert.assertTrue(sessions.isSessionInProgress(sessionID));

        session.setState(FINALIZED);
        Assert.assertFalse(sessions.isSessionInProgress(sessionID));
    }

    @Test
    public void isSessionInProgressFailed()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.prepareSessionFuture = new AsyncPromise<>();
        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS)).from(PARTICIPANT1).build());
        sessions.prepareSessionFuture.trySuccess(null);

        Assert.assertTrue(sessions.isSessionInProgress(sessionID));
        sessions.failSession(sessionID);
        Assert.assertFalse(sessions.isSessionInProgress(sessionID));
    }

    @Test
    public void isSessionInProgressNonExistantSession()
    {
        TimeUUID fakeID = nextTimeUUID();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        Assert.assertFalse(sessions.isSessionInProgress(fakeID));
    }

    @Test
    public void finalRepairedAtFinalized()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.prepareForTest(sessionID);
        sessions.maybeSetRepairing(sessionID);
        sessions.handleFinalizeProposeMessage(Message.builder(Verb.FINALIZE_PROPOSE_MSG, new FinalizePropose(sessionID)).from(COORDINATOR).build());
        sessions.handleFinalizeCommitMessage(Message.builder(Verb.FINALIZE_COMMIT_MSG, new FinalizeCommit(sessionID)).from(PARTICIPANT1).build());

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertTrue(session.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE);
        Assert.assertEquals(session.repairedAt, sessions.getFinalSessionRepairedAt(sessionID));
    }

    @Test
    public void finalRepairedAtFailed()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.prepareForTest(sessionID);
        sessions.failSession(sessionID);

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertTrue(session.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE);
        long repairedAt = sessions.getFinalSessionRepairedAt(sessionID);
        Assert.assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, repairedAt);
    }

    @Test
    public void finalRepairedAtNoSession()
    {
        TimeUUID fakeID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        long repairedAt = sessions.getFinalSessionRepairedAt(fakeID);
        Assert.assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, repairedAt);
    }

    @Test(expected = IllegalStateException.class)
    public void finalRepairedAtInProgress()
    {
        TimeUUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.prepareForTest(sessionID);

        sessions.getFinalSessionRepairedAt(sessionID);
    }

    /**
     * Startup happy path
     */
    @Test
    public void startup() throws Exception
    {
        InstrumentedLocalSessions initialSessions = new InstrumentedLocalSessions();
        initialSessions.start();
        Assert.assertEquals(0, initialSessions.getNumSessions());
        TimeUUID id1 = registerSession();
        TimeUUID id2 = registerSession();
        TimeUUID id3 = registerSession();

        initialSessions.prepareForTest(id1);
        initialSessions.prepareForTest(id2);
        initialSessions.prepareForTest(id3);

        Assert.assertEquals(3, initialSessions.getNumSessions());
        LocalSession session1 = initialSessions.getSession(id1);
        LocalSession session2 = initialSessions.getSession(id2);
        LocalSession session3 = initialSessions.getSession(id3);
        initialSessions.setStateAndSave(session2, PREPARED);
        initialSessions.setStateAndSave(session2, REPAIRING);
        initialSessions.setStateAndSave(session2, FINALIZE_PROMISED);
        initialSessions.setStateAndSave(session3, PREPARED);
        initialSessions.setStateAndSave(session3, REPAIRING);
        initialSessions.setStateAndSave(session3, FINALIZE_PROMISED);
        initialSessions.setStateAndSave(session3, FINALIZED);
        Assert.assertEquals(3, initialSessions.getNumSessions());

        // subsequent startups should load persisted sessions
        InstrumentedLocalSessions nextSessions = new InstrumentedLocalSessions();
        Assert.assertEquals(0, nextSessions.getNumSessions());
        nextSessions.start();
        Assert.assertEquals(3, nextSessions.getNumSessions());

        LocalSession session1next = nextSessions.getSession(id1);
        LocalSession session2next = nextSessions.getSession(id2);
        LocalSession session3next = nextSessions.getSession(id3);

        // non-finalized sessions should fail & notify coordinator after startup
        assertMessagesSent(nextSessions, session1next.coordinator, new FailSession(session1next.sessionID));
        Assert.assertEquals(session1.sessionID, session1next.sessionID);
        Assert.assertEquals(FAILED, session1next.getState());

        Assert.assertEquals(session2, session2next);
        Assert.assertEquals(session3, session3next);

    }

    /**
     * Stop happy path
     */
    @Test
    public void stop() throws Exception
    {
        InstrumentedLocalSessions initialSessions = new InstrumentedLocalSessions();
        initialSessions.start();
        Assert.assertEquals(0, initialSessions.getNumSessions());
        TimeUUID id1 = registerSession();
        TimeUUID id2 = registerSession();
        TimeUUID id3 = registerSession();

        initialSessions.prepareForTest(id1);
        initialSessions.prepareForTest(id2);
        initialSessions.prepareForTest(id3);

        Assert.assertEquals(3, initialSessions.getNumSessions());
        LocalSession session1 = initialSessions.getSession(id1);
        LocalSession session2 = initialSessions.getSession(id2);
        LocalSession session3 = initialSessions.getSession(id3);
        initialSessions.setStateAndSave(session2, PREPARED);
        initialSessions.setStateAndSave(session2, REPAIRING);
        initialSessions.setStateAndSave(session2, FINALIZE_PROMISED);
        initialSessions.setStateAndSave(session3, PREPARED);
        initialSessions.setStateAndSave(session3, REPAIRING);
        initialSessions.setStateAndSave(session3, FINALIZE_PROMISED);
        initialSessions.setStateAndSave(session3, FINALIZED);

        initialSessions.stop();
        // clean shutdown should fail session1 & notify coordinator
        assertMessagesSent(initialSessions, session1.coordinator, new FailSession(session1.sessionID));

        // subsequent startups should load persisted sessions
        InstrumentedLocalSessions nextSessions = new InstrumentedLocalSessions();
        Assert.assertEquals(0, nextSessions.getNumSessions());
        nextSessions.start();
        Assert.assertEquals(3, nextSessions.getNumSessions());

        LocalSession session1next = nextSessions.getSession(id1);
        LocalSession session2next = nextSessions.getSession(id2);
        LocalSession session3next = nextSessions.getSession(id3);

        Assert.assertEquals(session1, session1next);
        Assert.assertEquals(session2, session2next);
        Assert.assertEquals(session3, session3next);
        // clean shutdown above should make startup send no messages;
        assertNoMessagesSent(nextSessions, session1next.coordinator);
        assertNoMessagesSent(nextSessions, session2next.coordinator);
        assertNoMessagesSent(nextSessions, session3next.coordinator);
    }

    /**
     * If LocalSessions.start is called more than
     * once, an exception should be thrown
     */
    @Test (expected = IllegalArgumentException.class)
    public void multipleStartupFailure() throws Exception
    {
        InstrumentedLocalSessions initialSessions = new InstrumentedLocalSessions();
        initialSessions.start();
        initialSessions.start();
    }

    /**
     * If there are problems with the rows we're reading out of the repair table, we should
     * do the best we can to repair them, but not refuse to startup.
     */
    @Test
    public void loadCorruptRow() throws Exception
    {
        LocalSessions sessions = new LocalSessions(SharedContext.Global.instance);
        LocalSession session = createSession();
        sessions.save(session);

        sessions = new LocalSessions(SharedContext.Global.instance);
        sessions.start();
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        QueryProcessor.instance.executeInternal("DELETE participants, participants_wp FROM system.repairs WHERE parent_id=?", session.sessionID);

        sessions = new LocalSessions(SharedContext.Global.instance);
        sessions.start();
        Assert.assertNull(sessions.getSession(session.sessionID));
        UntypedResultSet res = QueryProcessor.executeInternal("SELECT * FROM system.repairs WHERE parent_id=?", session.sessionID);
        assertTrue(res.isEmpty());
    }

    private static LocalSession sessionWithTime(long started, long updated)
    {
        LocalSession.Builder builder = createBuilder();
        builder.withStartedAt(started);
        builder.withRepairedAt(started);
        builder.withLastUpdate(updated);
        return builder.build();
    }

    /**
     * Sessions that shouldn't be failed or deleted are left alone
     */
    @Test
    public void cleanupNoOp() throws Exception
    {
        LocalSessions sessions = new LocalSessions(SharedContext.Global.instance);
        sessions.start();

        long time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT + 60;
        LocalSession session = sessionWithTime(time - 1, time);

        sessions.putSessionUnsafe(session);
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        sessions.cleanup();

        Assert.assertNotNull(sessions.getSession(session.sessionID));
    }

    /**
     * Sessions past the auto fail cutoff should be failed
     */
    @Test
    public void cleanupFail() throws Exception
    {
        LocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        long time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
        LocalSession session = sessionWithTime(time - 1, time);
        session.setState(REPAIRING);

        sessions.putSessionUnsafe(session);
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        sessions.cleanup();

        Assert.assertNotNull(sessions.getSession(session.sessionID));
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(session.sessionID));
    }

    /**
     * Sessions past the auto delete cutoff with no sstables should be deleted
     */
    @Test
    public void cleanupDeleteNoSSTables() throws Exception
    {
        LocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        long time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
        LocalSession failed = sessionWithTime(time - 1, time);
        failed.setState(FAILED);

        LocalSession finalized = sessionWithTime(time - 1, time);
        finalized.setState(FINALIZED);

        sessions.putSessionUnsafe(failed);
        sessions.putSessionUnsafe(finalized);
        Assert.assertNotNull(sessions.getSession(failed.sessionID));
        Assert.assertNotNull(sessions.getSession(finalized.sessionID));

        sessions.cleanup();

        // failed session should be gone, but finalized should not, since it hasn't been superseded
        Assert.assertNull(sessions.getSession(failed.sessionID));
        Assert.assertNotNull(sessions.getSession(finalized.sessionID));

        Assert.assertNull(sessions.loadUnsafe(failed.sessionID));
        Assert.assertNotNull(sessions.loadUnsafe(finalized.sessionID));

        // add a finalized superseding session
        LocalSession superseding = sessionWithTime(time, time + 1);
        superseding.setState(FINALIZED);
        sessions.putSessionUnsafe(superseding);

        sessions.cleanup();

        // old finalized should be removed, superseding should still be there
        Assert.assertNull(sessions.getSession(finalized.sessionID));
        Assert.assertNotNull(sessions.getSession(superseding.sessionID));

        Assert.assertNull(sessions.loadUnsafe(finalized.sessionID));
        Assert.assertNotNull(sessions.loadUnsafe(superseding.sessionID));
    }

    /**
     * Sessions past the auto delete cutoff with no sstables should be deleted
     */
    @Test
    public void cleanupDeleteSSTablesRemaining() throws Exception
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        long time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
        LocalSession failed = sessionWithTime(time - 1, time);
        failed.setState(FAILED);

        LocalSession finalized = sessionWithTime(time - 1, time);
        finalized.setState(FINALIZED);

        sessions.putSessionUnsafe(failed);
        sessions.putSessionUnsafe(finalized);
        Assert.assertNotNull(sessions.getSession(failed.sessionID));
        Assert.assertNotNull(sessions.getSession(finalized.sessionID));

        sessions.sessionHasData = true;
        sessions.cleanup();

        Assert.assertNotNull(sessions.getSession(failed.sessionID));
        Assert.assertNotNull(sessions.getSession(finalized.sessionID));

        Assert.assertNotNull(sessions.loadUnsafe(failed.sessionID));
        Assert.assertNotNull(sessions.loadUnsafe(finalized.sessionID));
    }

    /**
     * Sessions should start checking the status of their participants if
     * there hasn't been activity for the CHECK_STATUS_TIMEOUT period
     */
    @Test
    public void cleanupStatusRequest() throws Exception
    {
        AtomicReference<LocalSession> checkedSession = new AtomicReference<>();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions() {
            public void sendStatusRequest(LocalSession session)
            {
                Assert.assertTrue(checkedSession.compareAndSet(null, session));
            }
        };
        sessions.start();

        long time = FBUtilities.nowInSeconds() - LocalSessions.CHECK_STATUS_TIMEOUT - 1;
        LocalSession session = sessionWithTime(time - 1, time);
        session.setState(REPAIRING);

        sessions.putSessionUnsafe(session);
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        sessions.cleanup();

        Assert.assertEquals(session, checkedSession.get());
    }
}

