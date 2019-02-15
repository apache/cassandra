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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;

import static org.apache.cassandra.net.MockMessagingService.all;
import static org.apache.cassandra.net.MockMessagingService.payload;
import static org.apache.cassandra.net.MockMessagingService.to;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.junit.Assert.fail;

public class CoordinatorMessagingTest extends AbstractRepairTest
{

    protected ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        LocalSessionAccessor.startup();
    }

    @Before
    public void setup()
    {
        String ks = "ks_" + System.currentTimeMillis();
        TableMetadata cfm = CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)", ks, "tbl"), ks).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
        cfs.disableAutoCompaction();
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Test
    public void testMockedMessagingHappyPath() throws InterruptedException, ExecutionException, TimeoutException
    {

        MockMessagingSpy spyPrepare = createPrepareSpy(Collections.emptySet(), Collections.emptySet());
        MockMessagingSpy spyFinalize = createFinalizeSpy(Collections.emptySet(), Collections.emptySet());
        MockMessagingSpy spyCommit = createCommitSpy();

        UUID uuid = registerSession(cfs, true, true);
        CoordinatorSession coordinator = ActiveRepairService.instance.consistent.coordinated.registerSession(uuid, PARTICIPANTS, false);
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());

        // execute repair and start prepare phase
        ListenableFuture<Boolean> sessionResult = coordinator.execute(sessionSupplier, hasFailures);
        Assert.assertFalse(sessionResult.isDone());
        Assert.assertFalse(hasFailures.get());
        // prepare completed
        spyPrepare.interceptMessageOut(3).get(1, TimeUnit.SECONDS);
        Assert.assertFalse(sessionResult.isDone());
        Assert.assertFalse(hasFailures.get());

        // set result from local repair session
        repairFuture.set(Lists.newArrayList(createResult(coordinator), createResult(coordinator), createResult(coordinator)));

        // finalize phase
        spyFinalize.interceptMessageOut(3).get(1, TimeUnit.SECONDS);
        Assert.assertFalse(sessionResult.isDone());
        Assert.assertFalse(hasFailures.get());

        // commit phase
        spyCommit.interceptMessageOut(3).get(1, TimeUnit.SECONDS);
        Assert.assertTrue(sessionResult.get());
        Assert.assertFalse(hasFailures.get());

        // expect no other messages except from intercepted so far
        spyPrepare.interceptNoMsg(100, TimeUnit.MILLISECONDS);
        spyFinalize.interceptNoMsg(100, TimeUnit.MILLISECONDS);
        spyCommit.interceptNoMsg(100, TimeUnit.MILLISECONDS);

        Assert.assertEquals(ConsistentSession.State.FINALIZED, coordinator.getState());
        Assert.assertFalse(ActiveRepairService.instance.consistent.local.isSessionInProgress(uuid));
    }


    @Test
    public void testMockedMessagingPrepareFailureP1() throws InterruptedException, ExecutionException, TimeoutException
    {
        createPrepareSpy(Collections.singleton(PARTICIPANT1), Collections.emptySet());
        testMockedMessagingPrepareFailure();
    }

    @Test
    public void testMockedMessagingPrepareFailureP12() throws InterruptedException, ExecutionException, TimeoutException
    {
        createPrepareSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2), Collections.emptySet());
        testMockedMessagingPrepareFailure();
    }

    @Test
    public void testMockedMessagingPrepareFailureP3() throws InterruptedException, ExecutionException, TimeoutException
    {
        createPrepareSpy(Collections.singleton(PARTICIPANT3), Collections.emptySet());
        testMockedMessagingPrepareFailure();
    }

    @Test
    public void testMockedMessagingPrepareFailureP123() throws InterruptedException, ExecutionException, TimeoutException
    {
        createPrepareSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3), Collections.emptySet());
        testMockedMessagingPrepareFailure();
    }

    @Test(expected = TimeoutException.class)
    public void testMockedMessagingPrepareFailureWrongSessionId() throws InterruptedException, ExecutionException, TimeoutException
    {
        createPrepareSpy(Collections.singleton(PARTICIPANT1), Collections.emptySet(), (msgOut) -> UUID.randomUUID());
        testMockedMessagingPrepareFailure();
    }

    private void testMockedMessagingPrepareFailure() throws InterruptedException, ExecutionException, TimeoutException
    {
        // we expect FailSession messages to all participants
        MockMessagingSpy sendFailSessionExpectedSpy = createFailSessionSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        UUID uuid = registerSession(cfs, true, true);
        CoordinatorSession coordinator = ActiveRepairService.instance.consistent.coordinated.registerSession(uuid, PARTICIPANTS, false);
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean proposeFailed = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());

        // execute repair and start prepare phase
        ListenableFuture<Boolean> sessionResult = coordinator.execute(sessionSupplier, proposeFailed);
        Assert.assertFalse(proposeFailed.get());
        // prepare completed
        try
        {
            sessionResult.get(1, TimeUnit.SECONDS);
            fail("Completed session without failure after prepare failed");
        }
        catch (ExecutionException e)
        {
        }
        sendFailSessionExpectedSpy.interceptMessageOut(3).get(1, TimeUnit.SECONDS);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertTrue(proposeFailed.get());
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());
        Assert.assertFalse(ActiveRepairService.instance.consistent.local.isSessionInProgress(uuid));
    }

    @Test
    public void testMockedMessagingPrepareTimeout() throws InterruptedException, ExecutionException, TimeoutException
    {
        MockMessagingSpy spyPrepare = createPrepareSpy(Collections.emptySet(), Collections.singleton(PARTICIPANT3));
        MockMessagingSpy sendFailSessionUnexpectedSpy = createFailSessionSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        UUID uuid = registerSession(cfs, true, true);
        CoordinatorSession coordinator = ActiveRepairService.instance.consistent.coordinated.registerSession(uuid, PARTICIPANTS, false);
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        SettableFuture<List<RepairSessionResult>> repairFuture = SettableFuture.create();
        Supplier<ListenableFuture<List<RepairSessionResult>>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        AtomicBoolean hasFailures = new AtomicBoolean(false);
        Assert.assertFalse(repairSubmitted.get());

        // execute repair and start prepare phase
        ListenableFuture<Boolean> sessionResult = coordinator.execute(sessionSupplier, hasFailures);
        try
        {
            sessionResult.get(1, TimeUnit.SECONDS);
            fail("Completed session without failure after prepare failed");
        }
        catch (ExecutionException e)
        {
            fail("Failed session in prepare failed during timeout from participant");
        }
        catch (TimeoutException e)
        {
            // expected
        }
        // we won't send out any fail session message in case of timeouts
        spyPrepare.expectMockedMessageIn(2).get(100, TimeUnit.MILLISECONDS);
        sendFailSessionUnexpectedSpy.interceptNoMsg(100, TimeUnit.MILLISECONDS);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertFalse(hasFailures.get());
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());
        Assert.assertFalse(ActiveRepairService.instance.consistent.local.isSessionInProgress(uuid));
    }

    private MockMessagingSpy createPrepareSpy(Collection<InetAddressAndPort> failed,
                                              Collection<InetAddressAndPort> timeout)
    {
        return createPrepareSpy(failed, timeout, (msgOut) -> msgOut.parentSession);
    }

    private MockMessagingSpy createPrepareSpy(Collection<InetAddressAndPort> failed,
                                              Collection<InetAddressAndPort> timeout,
                                              Function<PrepareConsistentRequest, UUID> sessionIdFunc)
    {
        return MockMessagingService.when(
        all(verb(MessagingService.Verb.REPAIR_MESSAGE),
            payload((p) -> p instanceof PrepareConsistentRequest))
        ).respond((msgOut, to) ->
                  {
                      if(timeout.contains(to)) return null;
                      else return MessageIn.create(to,
                                                   new PrepareConsistentResponse(sessionIdFunc.apply((PrepareConsistentRequest) msgOut.payload), to, !failed.contains(to)),
                                                   Collections.emptyMap(),
                                                   MessagingService.Verb.REPAIR_MESSAGE,
                                                   MessagingService.current_version);
                  });
    }

    private MockMessagingSpy createFinalizeSpy(Collection<InetAddressAndPort> failed,
                                               Collection<InetAddressAndPort> timeout)
    {
        return MockMessagingService.when(
        all(verb(MessagingService.Verb.REPAIR_MESSAGE),
            payload((p) -> p instanceof FinalizePropose))
        ).respond((msgOut, to) ->
                  {
                      if(timeout.contains(to)) return null;
                      else return MessageIn.create(to,
                                                   new FinalizePromise(((FinalizePropose) msgOut.payload).sessionID, to, !failed.contains(to)),
                                                   Collections.emptyMap(),
                                                   MessagingService.Verb.REPAIR_MESSAGE,
                                                   MessagingService.current_version);
                  });
    }

    private MockMessagingSpy createCommitSpy()
    {
        return MockMessagingService.when(
            all(verb(MessagingService.Verb.REPAIR_MESSAGE),
                payload((p) -> p instanceof FinalizeCommit))
        ).dontReply();
    }

    private MockMessagingSpy createFailSessionSpy(Collection<InetAddressAndPort> participants)
    {
        return MockMessagingService.when(
            all(verb(MessagingService.Verb.REPAIR_MESSAGE),
                payload((p) -> p instanceof FailSession),
                to(participants::contains))
        ).dontReply();
    }

    private static RepairSessionResult createResult(CoordinatorSession coordinator)
    {
        return new RepairSessionResult(coordinator.sessionID, "ks", coordinator.ranges, null, false);
    }
}
