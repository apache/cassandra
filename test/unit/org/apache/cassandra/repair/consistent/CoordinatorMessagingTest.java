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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.Lists;

import org.apache.cassandra.repair.CoordinatedRepairResult;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.repair.RepairSessionResult;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;

import static org.apache.cassandra.net.MockMessagingService.all;
import static org.apache.cassandra.net.MockMessagingService.to;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
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
    public void testMockedMessagingHappyPath() throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        CountDownLatch prepareLatch = createLatch();
        CountDownLatch finalizeLatch = createLatch();

        MockMessagingSpy spyPrepare = createPrepareSpy(Collections.emptySet(), Collections.emptySet(), prepareLatch);
        MockMessagingSpy spyFinalize = createFinalizeSpy(Collections.emptySet(), Collections.emptySet(), finalizeLatch);
        MockMessagingSpy spyCommit = createCommitSpy();

        TimeUUID uuid = registerSession(cfs, true, true);
        CoordinatorSession coordinator = ActiveRepairService.instance().consistent.coordinated.registerSession(uuid, PARTICIPANTS, false);
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        Promise<CoordinatedRepairResult> repairFuture = AsyncPromise.uncancellable();
        Supplier<Future<CoordinatedRepairResult>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        Assert.assertFalse(repairSubmitted.get());

        // execute repair and start prepare phase
        Future<CoordinatedRepairResult> sessionResult = coordinator.execute(sessionSupplier);
        Assert.assertFalse(sessionResult.isDone());

        // prepare completed
        prepareLatch.countDown();
        spyPrepare.interceptMessageOut(3).get(1, TimeUnit.SECONDS);
        Assert.assertFalse(sessionResult.isDone());

        // set result from local repair session
        repairFuture.trySuccess(CoordinatedRepairResult.success(Lists.newArrayList(createResult(coordinator), createResult(coordinator), createResult(coordinator))));

        // finalize phase
        finalizeLatch.countDown();
        spyFinalize.interceptMessageOut(3).get(1, TimeUnit.SECONDS);

        // commit phase
        spyCommit.interceptMessageOut(3).get(1, TimeUnit.SECONDS);
        Assert.assertFalse(sessionResult.get().hasFailed());

        // expect no other messages except from intercepted so far
        spyPrepare.interceptNoMsg(100, TimeUnit.MILLISECONDS);
        spyFinalize.interceptNoMsg(100, TimeUnit.MILLISECONDS);
        spyCommit.interceptNoMsg(100, TimeUnit.MILLISECONDS);

        Assert.assertEquals(ConsistentSession.State.FINALIZED, coordinator.getState());
        Assert.assertFalse(ActiveRepairService.instance().consistent.local.isSessionInProgress(uuid));
    }


    @Test
    public void testMockedMessagingPrepareFailureP1() throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        CountDownLatch latch = createLatch();
        createPrepareSpy(Collections.singleton(PARTICIPANT1), Collections.emptySet(), latch);
        testMockedMessagingPrepareFailure(latch);
    }

    @Test
    public void testMockedMessagingPrepareFailureP12() throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        CountDownLatch latch = createLatch();
        createPrepareSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2), Collections.emptySet(), latch);
        testMockedMessagingPrepareFailure(latch);
    }

    @Test
    public void testMockedMessagingPrepareFailureP3() throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        CountDownLatch latch = createLatch();
        createPrepareSpy(Collections.singleton(PARTICIPANT3), Collections.emptySet(), latch);
        testMockedMessagingPrepareFailure(latch);
    }

    @Test
    public void testMockedMessagingPrepareFailureP123() throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        CountDownLatch latch = createLatch();
        createPrepareSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3), Collections.emptySet(), latch);
        testMockedMessagingPrepareFailure(latch);
    }

    @Test(expected = TimeoutException.class)
    public void testMockedMessagingPrepareFailureWrongSessionId() throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        CountDownLatch latch = createLatch();
        createPrepareSpy(Collections.singleton(PARTICIPANT1), Collections.emptySet(), (msgOut) -> nextTimeUUID(), latch);
        testMockedMessagingPrepareFailure(latch);
    }

    private void testMockedMessagingPrepareFailure(CountDownLatch prepareLatch) throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        // we expect FailSession messages to all participants
        MockMessagingSpy sendFailSessionExpectedSpy = createFailSessionSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        TimeUUID uuid = registerSession(cfs, true, true);
        CoordinatorSession coordinator = ActiveRepairService.instance().consistent.coordinated.registerSession(uuid, PARTICIPANTS, false);
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        Promise<CoordinatedRepairResult> repairFuture = AsyncPromise.uncancellable();
        Supplier<Future<CoordinatedRepairResult>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        Assert.assertFalse(repairSubmitted.get());

        // execute repair and start prepare phase
        Future<CoordinatedRepairResult> sessionResult = coordinator.execute(sessionSupplier);
        prepareLatch.countDown();
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
        Assert.assertTrue(sessionResult.isDone());
        Assert.assertNotNull(sessionResult.cause());
        Assert.assertEquals(ConsistentSession.State.FAILED, coordinator.getState());
        Assert.assertFalse(ActiveRepairService.instance().consistent.local.isSessionInProgress(uuid));
    }

    @Test
    public void testMockedMessagingPrepareTimeout() throws InterruptedException, ExecutionException, TimeoutException, NoSuchRepairSessionException
    {
        MockMessagingSpy spyPrepare = createPrepareSpy(Collections.emptySet(), Collections.singleton(PARTICIPANT3), new CountDownLatch(0));
        MockMessagingSpy sendFailSessionUnexpectedSpy = createFailSessionSpy(Lists.newArrayList(PARTICIPANT1, PARTICIPANT2, PARTICIPANT3));

        TimeUUID uuid = registerSession(cfs, true, true);
        CoordinatorSession coordinator = ActiveRepairService.instance().consistent.coordinated.registerSession(uuid, PARTICIPANTS, false);
        AtomicBoolean repairSubmitted = new AtomicBoolean(false);
        Promise<CoordinatedRepairResult> repairFuture = AsyncPromise.uncancellable();
        Supplier<Future<CoordinatedRepairResult>> sessionSupplier = () ->
        {
            repairSubmitted.set(true);
            return repairFuture;
        };

        // coordinator sends prepare requests to create local session and perform anticompaction
        Assert.assertFalse(repairSubmitted.get());

        // execute repair and start prepare phase
        Future<CoordinatedRepairResult> sessionResult = coordinator.execute(sessionSupplier);
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
        spyPrepare.expectMockedMessage(2).get(100, TimeUnit.MILLISECONDS);
        sendFailSessionUnexpectedSpy.interceptNoMsg(100, TimeUnit.MILLISECONDS);
        Assert.assertFalse(repairSubmitted.get());
        Assert.assertEquals(ConsistentSession.State.PREPARING, coordinator.getState());
        Assert.assertFalse(ActiveRepairService.instance().consistent.local.isSessionInProgress(uuid));
    }

    private MockMessagingSpy createPrepareSpy(Collection<InetAddressAndPort> failed,
                                              Collection<InetAddressAndPort> timeout,
                                              CountDownLatch latch)
    {
        return createPrepareSpy(failed, timeout, (msgOut) -> msgOut.parentSession, latch);
    }

    private MockMessagingSpy createPrepareSpy(Collection<InetAddressAndPort> failed,
                                              Collection<InetAddressAndPort> timeout,
                                              Function<PrepareConsistentRequest, TimeUUID> sessionIdFunc,
                                              CountDownLatch latch)
    {
        return MockMessagingService.when(verb(Verb.PREPARE_CONSISTENT_REQ)).respond((msgOut, to) ->
        {
            try
            {
                latch.await();
            }
            catch (InterruptedException e) { }
            if (timeout.contains(to))
                return null;

            return Message.out(Verb.PREPARE_CONSISTENT_RSP,
                               new PrepareConsistentResponse(sessionIdFunc.apply((PrepareConsistentRequest) msgOut.payload), to, !failed.contains(to)));
        });
    }

    private MockMessagingSpy createFinalizeSpy(Collection<InetAddressAndPort> failed,
                                               Collection<InetAddressAndPort> timeout,
                                               CountDownLatch latch)
    {
        return MockMessagingService.when(verb(Verb.FINALIZE_PROPOSE_MSG)).respond((msgOut, to) ->
        {
            try
            {
                latch.await();
            }
            catch (InterruptedException e) { }
            if (timeout.contains(to))
                return null;

            return Message.out(Verb.FINALIZE_PROMISE_MSG, new FinalizePromise(((FinalizePropose) msgOut.payload).sessionID, to, !failed.contains(to)));
        });
    }

    private MockMessagingSpy createCommitSpy()
    {
        return MockMessagingService.when(verb(Verb.FINALIZE_COMMIT_MSG)).dontReply();
    }

    private MockMessagingSpy createFailSessionSpy(Collection<InetAddressAndPort> participants)
    {
        return MockMessagingService.when(all(verb(Verb.FAILED_SESSION_MSG), to(participants::contains))).dontReply();
    }

    private static RepairSessionResult createResult(CoordinatorSession coordinator)
    {
        return new RepairSessionResult(coordinator.sessionID, "ks", coordinator.ranges, null, false);
    }

    private CountDownLatch createLatch()
    {
        return new CountDownLatch(1);
    }
}
