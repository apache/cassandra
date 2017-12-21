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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
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
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.repair.consistent.ConsistentSession.State.*;

public class LocalSessionTest extends AbstractRepairTest
{

    static LocalSession.Builder createBuilder()
    {
        LocalSession.Builder builder = LocalSession.builder();
        builder.withState(PREPARING);
        builder.withSessionID(UUIDGen.getTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(Sets.newHashSet(RANGE1, RANGE2, RANGE3));
        builder.withParticipants(Sets.newHashSet(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        int now = FBUtilities.nowInSeconds();
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

    private static void assertNoMessagesSent(InstrumentedLocalSessions sessions, InetAddress to)
    {
        Assert.assertNull(sessions.sentMessages.get(to));
    }

    private static void assertMessagesSent(InstrumentedLocalSessions sessions, InetAddress to, RepairMessage... expected)
    {
        Assert.assertEquals(Lists.newArrayList(expected), sessions.sentMessages.get(to));
    }

    static class InstrumentedLocalSessions extends LocalSessions
    {
        Map<InetAddress, List<RepairMessage>> sentMessages = new HashMap<>();
        protected void sendMessage(InetAddress destination, RepairMessage message)
        {
            if (!sentMessages.containsKey(destination))
            {
                sentMessages.put(destination, new ArrayList<>());
            }
            sentMessages.get(destination).add(message);
        }

        SettableFuture<Object> pendingAntiCompactionFuture = null;
        boolean submitPendingAntiCompactionCalled = false;
        ListenableFuture submitPendingAntiCompaction(LocalSession session, ExecutorService executor)
        {
            submitPendingAntiCompactionCalled = true;
            if (pendingAntiCompactionFuture != null)
            {
                return pendingAntiCompactionFuture;
            }
            else
            {
                return super.submitPendingAntiCompaction(session, executor);
            }
        }

        boolean failSessionCalled = false;
        public void failSession(UUID sessionID, boolean sendMessage)
        {
            failSessionCalled = true;
            super.failSession(sessionID, sendMessage);
        }

        public LocalSession prepareForTest(UUID sessionID)
        {
            pendingAntiCompactionFuture = SettableFuture.create();
            handlePrepareMessage(PARTICIPANT1, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
            pendingAntiCompactionFuture.set(new Object());
            sentMessages.clear();
            return getSession(sessionID);
        }

        protected InetAddress getBroadcastAddress()
        {
            return PARTICIPANT1;
        }

        protected boolean isAlive(InetAddress address)
        {
            return true;
        }

        protected boolean isNodeInitialized()
        {
            return true;
        }

        public Map<UUID, Integer> completedSessions = new HashMap<>();

        protected void sessionCompleted(LocalSession session)
        {
            UUID sessionID = session.sessionID;
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

    private static UUID registerSession()
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
        LocalSessions sessions = new LocalSessions();
        LocalSession expected = createSession();
        sessions.save(expected);
        LocalSession actual = sessions.loadUnsafe(expected.sessionID);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void prepareSuccessCase()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // replacing future so we can inspect state before and after anti compaction callback
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        Assert.assertFalse(sessions.submitPendingAntiCompactionCalled);
        sessions.handlePrepareMessage(PARTICIPANT1, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        Assert.assertTrue(sessions.submitPendingAntiCompactionCalled);
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // anti compaction hasn't finished yet, so state in memory and on disk should be PREPARING
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // anti compaction has now finished, so state in memory and on disk should be PREPARED
        sessions.pendingAntiCompactionFuture.set(new Object());
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
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // replacing future so we can inspect state before and after anti compaction callback
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        Assert.assertFalse(sessions.submitPendingAntiCompactionCalled);
        sessions.handlePrepareMessage(PARTICIPANT1, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        Assert.assertTrue(sessions.submitPendingAntiCompactionCalled);
        Assert.assertTrue(sessions.sentMessages.isEmpty());

        // anti compaction hasn't finished yet, so state in memory and on disk should be PREPARING
        LocalSession session = sessions.getSession(sessionID);
        Assert.assertNotNull(session);
        Assert.assertEquals(PREPARING, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));

        // anti compaction has now finished, so state in memory and on disk should be PREPARED
        sessions.pendingAntiCompactionFuture.setException(new RuntimeException());
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
        UUID sessionID = UUIDGen.getTimeUUID();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.handlePrepareMessage(PARTICIPANT1, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        Assert.assertNull(sessions.getSession(sessionID));
        assertMessagesSent(sessions, COORDINATOR, new FailSession(sessionID));
    }

    @Test
    public void maybeSetRepairing()
    {
        UUID sessionID = registerSession();
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

        UUID sessionID = registerSession();
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
        UUID fakeID = UUIDGen.getTimeUUID();
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
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to preparing
        LocalSession session = sessions.prepareForTest(sessionID);
        sessions.maybeSetRepairing(sessionID);

        //
        Assert.assertEquals(REPAIRING, session.getState());

        // should send a promised message to coordinator and set session state accordingly
        sessions.sentMessages.clear();
        sessions.handleFinalizeProposeMessage(COORDINATOR, new FinalizePropose(sessionID));
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
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        LocalSession session = sessions.prepareForTest(sessionID);
        Assert.assertEquals(PREPARED, session.getState());

        // should fail the session and send a failure message to the coordinator
        sessions.sentMessages.clear();
        sessions.handleFinalizeProposeMessage(COORDINATOR, new FinalizePropose(sessionID));
        Assert.assertEquals(FAILED, session.getState());
        Assert.assertEquals(session, sessions.loadUnsafe(sessionID));
        assertMessagesSent(sessions, COORDINATOR, new FailSession(sessionID));
    }

    @Test
    public void finalizeProposeNonExistantSessionFailure()
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        UUID fakeID = UUIDGen.getTimeUUID();
        sessions.handleFinalizeProposeMessage(COORDINATOR, new FinalizePropose(fakeID));
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
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        // create session and move to finalized promised
        sessions.prepareForTest(sessionID);
        sessions.maybeSetRepairing(sessionID);
        sessions.handleFinalizeProposeMessage(COORDINATOR, new FinalizePropose(sessionID));

        Assert.assertEquals(0, (int) sessions.completedSessions.getOrDefault(sessionID, 0));
        sessions.sentMessages.clear();
        LocalSession session = sessions.getSession(sessionID);
        sessions.handleFinalizeCommitMessage(PARTICIPANT1, new FinalizeCommit(sessionID));

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
        UUID fakeID = UUIDGen.getTimeUUID();
        sessions.handleFinalizeCommitMessage(PARTICIPANT1, new FinalizeCommit(fakeID));
        Assert.assertNull(sessions.getSession(fakeID));
        Assert.assertTrue(sessions.sentMessages.isEmpty());
    }

    @Test
    public void failSession()
    {
        UUID sessionID = registerSession();
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
        UUID sessionID = registerSession();
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
        UUID sessionID = registerSession();
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
        UUID sessionID = registerSession();
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
        UUID sessionID = UUIDGen.getTimeUUID();
        sessions.handleStatusRequest(PARTICIPANT2, new StatusRequest(sessionID));
        assertNoMessagesSent(sessions, PARTICIPANT1);
        assertMessagesSent(sessions, PARTICIPANT2, new StatusResponse(sessionID, FAILED));
        assertNoMessagesSent(sessions, PARTICIPANT3);
    }

    @Test
    public void handleStatusResponseFinalized() throws Exception
    {
        UUID sessionID = registerSession();
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
        UUID sessionID = registerSession();
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
        UUID sessionID = registerSession();
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
        UUID sessionID = registerSession();
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
        UUID sessionID = registerSession();
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
        UUID sessionID = UUIDGen.getTimeUUID();
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
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.pendingAntiCompactionFuture = SettableFuture.create();  // prevent moving to prepared
        sessions.handlePrepareMessage(PARTICIPANT1, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));

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
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.pendingAntiCompactionFuture = SettableFuture.create();
        sessions.handlePrepareMessage(PARTICIPANT1, new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS));
        sessions.pendingAntiCompactionFuture.set(new Object());

        Assert.assertTrue(sessions.isSessionInProgress(sessionID));
        sessions.failSession(sessionID);
        Assert.assertFalse(sessions.isSessionInProgress(sessionID));
    }

    @Test
    public void isSessionInProgressNonExistantSession()
    {
        UUID fakeID = UUIDGen.getTimeUUID();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        Assert.assertFalse(sessions.isSessionInProgress(fakeID));
    }

    @Test
    public void finalRepairedAtFinalized()
    {
        UUID sessionID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.prepareForTest(sessionID);
        sessions.maybeSetRepairing(sessionID);
        sessions.handleFinalizeProposeMessage(COORDINATOR, new FinalizePropose(sessionID));
        sessions.handleFinalizeCommitMessage(PARTICIPANT1, new FinalizeCommit(sessionID));

        LocalSession session = sessions.getSession(sessionID);
        Assert.assertTrue(session.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE);
        Assert.assertEquals(session.repairedAt, sessions.getFinalSessionRepairedAt(sessionID));
    }

    @Test
    public void finalRepairedAtFailed()
    {
        UUID sessionID = registerSession();
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
        UUID fakeID = registerSession();
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        long repairedAt = sessions.getFinalSessionRepairedAt(fakeID);
        Assert.assertEquals(ActiveRepairService.UNREPAIRED_SSTABLE, repairedAt);
    }

    @Test(expected = IllegalStateException.class)
    public void finalRepairedAtInProgress()
    {
        UUID sessionID = registerSession();
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
        UUID id1 = registerSession();
        UUID id2 = registerSession();

        initialSessions.prepareForTest(id1);
        initialSessions.prepareForTest(id2);
        Assert.assertEquals(2, initialSessions.getNumSessions());
        LocalSession session1 = initialSessions.getSession(id1);
        LocalSession session2 = initialSessions.getSession(id2);


        // subsequent startups should load persisted sessions
        InstrumentedLocalSessions nextSessions = new InstrumentedLocalSessions();
        Assert.assertEquals(0, nextSessions.getNumSessions());
        nextSessions.start();
        Assert.assertEquals(2, nextSessions.getNumSessions());

        Assert.assertEquals(session1, nextSessions.getSession(id1));
        Assert.assertEquals(session2, nextSessions.getSession(id2));
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
        LocalSessions sessions = new LocalSessions();
        LocalSession session = createSession();
        sessions.save(session);

        sessions = new LocalSessions();
        sessions.start();
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        QueryProcessor.instance.executeInternal("DELETE participants FROM system.repairs WHERE parent_id=?", session.sessionID);

        sessions = new LocalSessions();
        sessions.start();
        Assert.assertNull(sessions.getSession(session.sessionID));
    }

    private static LocalSession sessionWithTime(int started, int updated)
    {
        LocalSession.Builder builder = createBuilder();
        builder.withStartedAt(started);
        builder.withLastUpdate(updated);
        return builder.build();
    }

    /**
     * Sessions that shouldn't be failed or deleted are left alone
     */
    @Test
    public void cleanupNoOp() throws Exception
    {
        LocalSessions sessions = new LocalSessions();
        sessions.start();

        int time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT + 60;
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

        int time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
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

        int time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
        LocalSession failed = sessionWithTime(time - 1, time);
        failed.setState(FAILED);

        LocalSession finalized = sessionWithTime(time - 1, time);
        finalized.setState(FINALIZED);

        sessions.putSessionUnsafe(failed);
        sessions.putSessionUnsafe(finalized);
        Assert.assertNotNull(sessions.getSession(failed.sessionID));
        Assert.assertNotNull(sessions.getSession(finalized.sessionID));

        sessions.cleanup();

        Assert.assertNull(sessions.getSession(failed.sessionID));
        Assert.assertNull(sessions.getSession(finalized.sessionID));

        Assert.assertNull(sessions.loadUnsafe(failed.sessionID));
        Assert.assertNull(sessions.loadUnsafe(finalized.sessionID));
    }

    /**
     * Sessions past the auto delete cutoff with no sstables should be deleted
     */
    @Test
    public void cleanupDeleteSSTablesRemaining() throws Exception
    {
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        int time = FBUtilities.nowInSeconds() - LocalSessions.AUTO_FAIL_TIMEOUT - 1;
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

        int time = FBUtilities.nowInSeconds() - LocalSessions.CHECK_STATUS_TIMEOUT - 1;
        LocalSession session = sessionWithTime(time - 1, time);
        session.setState(REPAIRING);

        sessions.putSessionUnsafe(session);
        Assert.assertNotNull(sessions.getSession(session.sessionID));

        sessions.cleanup();

        Assert.assertEquals(session, checkedSession.get());
    }
}

