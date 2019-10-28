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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.InstanceClassLoader;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.CasWriteResultUnknownException;
import org.apache.cassandra.net.Verb;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;

import static org.hamcrest.CoreMatchers.containsString;

public class CasWriteTest extends DistributedTestBase
{
    // Sharing the same cluster to boost test speed. Using a pkGen to make sure queries has distinct pk value for paxos instances.
    private static Cluster cluster;
    private static final AtomicInteger pkGen = new AtomicInteger(1_000); // preserve any pk values less than 1000 for manual queries.
    private static final Logger logger = LoggerFactory.getLogger(CasWriteTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupCluster() throws Throwable
    {
        cluster = init(Cluster.create(3));
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
    }

    @AfterClass
    public static void close()
    {
        cluster.close();
        cluster = null;
    }

    @Before @After
    public void resetFilters()
    {
        cluster.filters().reset();
    }

    @Test
    public void testCasWriteSuccessWithNoContention()
    {
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS",
                                       ConsistencyLevel.QUORUM);
        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.QUORUM),
                   row(1, 1, 1));

        cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 AND ck = 1 IF v = 1",
                                       ConsistencyLevel.QUORUM);
        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.QUORUM),
                   row(1, 1, 2));
    }

    @Test
    public void testCasWriteTimeoutAtPreparePhase_ReqLost()
    {
        expectCasWriteTimeout();
        cluster.verbs(Verb.PAXOS_PREPARE_REQ).from(1).to(2, 3).drop().activate(); // drop the internode messages to acceptors
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtPreparePhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.verbs(Verb.PAXOS_PREPARE_RSP).from(2, 3).to(1).drop().activate(); // drop the internode messages to acceptors
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtProposePhase_ReqLost()
    {
        expectCasWriteTimeout();
        cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(1).to(2, 3).drop().activate();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtProposePhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.verbs(Verb.PAXOS_PROPOSE_RSP).from(2, 3).to(1).drop().activate();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtCommitPhase_ReqLost()
    {
        expectCasWriteTimeout();
        cluster.verbs(Verb.PAXOS_COMMIT_REQ).from(1).to(2, 3).drop().activate();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtCommitPhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.verbs(Verb.PAXOS_COMMIT_RSP).from(2, 3).to(1).drop().activate();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    /**
     * The scneario being constructed in this test has 2 clients contending and one of them experiences network partition and timed out with CasWriteTimeout.
     * The timeline is,
     * 1. client2/node2 sent proposal to all nodes. The nodes made promise, but the messages sent back to node2 was delayed. So node2 did not proceed.
     * 2. client1/node1 sent proposal (newer) to all nodes and received promise from all of them.
     * 3. client2/node2 now received the message, thinking it has a quorum promise (but not, in fact). It sent proposal to nodes to accept and got rejected by all nodes.
     * 4. client2/node2 started a new round to prepare, and got the quorum promise.
     * 5. client2/node2 sent proposal to nodes, but experienced network parition and the messages (propose_req) were not delivered and timed out.
     * 6. client1/node1 started to send proposal to be accepted and completed the paxos instance.
     */
    @Test
    public void testCasWriteTimeoutDueToContention() throws InterruptedException
    {
        // 2 contended LWT queries. same pk but different v.
        int curPk = pkGen.getAndIncrement();
        final String query1 = mkCasInsertQuery(a -> curPk, 1);
        final String query2 = mkCasInsertQuery(a -> curPk, 2);
        final CountDownLatch client1Ready = new CountDownLatch(2);
        final CountDownLatch client1ProposeReady = new CountDownLatch(2);
        final CountDownLatch client2ProposeReady = new CountDownLatch(2);
        final AtomicReference<Exception> exRef = new AtomicReference<>(null);
        Runnable client1 = () ->
        {
            awaitSilently(client1Ready);
            cluster.coordinator(1).execute(query1, ConsistencyLevel.QUORUM);
        };
        Runnable client2 = () ->
        {
            try
            {
                cluster.coordinator(2).execute(query2, ConsistencyLevel.QUORUM);
            }
            catch (Exception ex)
            {
                exRef.set(ex);
            }
        };
        cluster.verbs(Verb.PAXOS_PREPARE_REQ).from(2).to(1, 3)
               .intercept(() ->
                          {
                              client1Ready.countDown();
                              if (client2ProposeReady.getCount() == 0) // indicating the new round of prepare after being contended.
                              {
                                  client1ProposeReady.countDown();
                                  if (client1ProposeReady.getCount() == 0)
                                  {   // remove the propose intercepter and simulate network partition between 2 to (1, 3) to get writetimeout.
                                      cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(2).to(1, 3).drop().activate();
                                  }
                              }
                          }).activate();
        cluster.verbs(Verb.PAXOS_PREPARE_RSP).from(1, 3).to(2).intercept(() -> awaitSilently(client2ProposeReady)).activate();
        cluster.verbs(Verb.PAXOS_PREPARE_RSP).from(2, 3).to(1).intercept(client2ProposeReady::countDown).activate();
        cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(1).to(2, 3).intercept(() -> awaitSilently(client1ProposeReady)).activate();
        ExecutorService es = Executors.newFixedThreadPool(2);
        es.submit(client1);
        es.submit(client2);
        es.shutdown();
        es.awaitTermination(4, TimeUnit.SECONDS);
        Exception ex = exRef.get();
        Assert.assertNotNull("Expecting exception but found null.", ex);
        logger.info("Received exception: {}", ex.getMessage());
        Assert.assertThat("Expecting RuntimeException", ex, CoreMatchers.instanceOf(RuntimeException.class));
        Assert.assertTrue("Expecting cause to be CasWriteTimeoutException", ex.getMessage().contains(CasWriteTimeoutException.class.getCanonicalName()));
        Assert.assertTrue("Expecting contention to be 1", ex.getMessage().contains("encountered contentions: 1"));
    }

    /**
     * Not an comprehensive test to covers all scenarios that a client gets CasWriteUncertainty and its query/proposal not eventually replayed.
     * The scneario being constructed in this test has 2 clients contending. The timeline is,
     * 1. client2 starts first with node2 as proposer, and it gets a quorum promise at the PREPARE phase.
     * 2. client2 now sends proposal/query2 to nodes to accept, but experiences network delay
     * 3. client1 starts with node1 as proposer, and it gets a quorum promise at the PREPARE phase. (newer proposal)
     *    But, the message from node1 to node2 was not dilivered due to network partition.
     *    Therefore, the quorum consists of node1 and node3. Node2 is unaware of the proposal/query2 from node1(proposer).
     * 4. the proposal/query2 from client2 finally arrives the nodes. Node1 and node3 reject. Node2 accepts. Not a quorum, but 1 node has accepted. Client2 gets CasWriteUncertainty
     * 5. client1 continue to propose the proposal/query1 to nodes and being accepted by all nodes. The state is updated by query1.
     */
    @Test
    public void testCasWriteUncertainty_ProposalNotReplayed() throws InterruptedException
    {
        // 2 contended LWT queries. same pk but different v.
        int curPk = pkGen.getAndIncrement();
        final String query1 = mkCasInsertQuery(a -> curPk, 1);
        final String query2 = mkCasInsertQuery(a -> curPk, 2);
        final CountDownLatch client1Ready = new CountDownLatch(2);
        final CountDownLatch client1ProposeReady = new CountDownLatch(1);
        final CountDownLatch client2ProposeReady = new CountDownLatch(1);
        final AtomicReference<Exception> exRef = new AtomicReference<>(null);
        Runnable client1 = () -> // expecting client1's query to succeed
        {
            awaitSilently(client1Ready);
            cluster.coordinator(1).execute(query1, ConsistencyLevel.QUORUM);
            client2ProposeReady.countDown();
        };
        Runnable client2 = () -> // expecting client2 to see CasWriteUncertainException
        {
            try
            {
                cluster.coordinator(2).execute(query2, ConsistencyLevel.QUORUM);
            }
            catch (Exception ex)
            {
                exRef.set(ex);
                client1ProposeReady.countDown();
            }
        };
        cluster.verbs(Verb.PAXOS_PREPARE_RSP).from(1, 3).to(2).intercept(client1Ready::countDown).activate(); // start client1 as soon as client2 gets a quorum at prepare phase
        cluster.verbs(Verb.PAXOS_PREPARE_REQ, Verb.PAXOS_PROPOSE_REQ).from(1).to(2).drop().activate(); // client1 can get quorum from 1 and 3, but not 2 (req to 2 is dropped).
        cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(1).to(3).intercept(() -> awaitSilently(client1ProposeReady)).activate(); // await for client1 to backoff
        cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(2).to(1, 3).intercept(() -> awaitSilently(client2ProposeReady)).activate(); // await for client1 to get a quorum at prepare phase.
        cluster.verbs(Verb.PAXOS_PREPARE_RSP).from(3).to(1).intercept(client2ProposeReady::countDown).activate(); // allow client2 to propose once client1 gets quorum promise
        ExecutorService es = Executors.newFixedThreadPool(2);
        es.submit(client1);
        es.submit(client2);
        es.shutdown();
        es.awaitTermination(4, TimeUnit.SECONDS);
        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = " + curPk,
                                                  ConsistencyLevel.SERIAL), // read with Paxos.
                   row(curPk, 1, 1));
        Exception ex = exRef.get();
        Assert.assertNotNull("Expecting exception but found null. Proposer get none acceptance?", ex);
        Assert.assertThat("Expecting RuntimeException", ex, CoreMatchers.instanceOf(RuntimeException.class));
        Assert.assertTrue("Expecting cause to be CasWriteUncertainException", ex.getMessage().contains(CasWriteResultUnknownException.class.getCanonicalName()));
    }

    /**
     * Not an comprehensive test to covers all scenarios that a client gets CasWriteUncertainty and its query/proposal gets replayed.
     * The scneario being constructed in this test has 3 clients involved. The timeline is,
     * 1. client2 starts first with node2 as proposer, and it gets a quorum promise at the PREPARE phase.
     * 2. client2 now sends proposal/query2 to nodes to accept, but experiences network delay
     * 3. client1 starts with node1 as proposer, and it gets a quorum promise at the PREPARE phase. (newer proposal)
     *    But, the message from node1 to node2 was not dilivered due to network partition.
     *    Therefore, the quorum consists of node1 and node3. Node2 is unaware of the proposal/query2 from node1(proposer).
     * 4. the proposal/query2 from client2 finally arrives the nodes. Node1 and node3 reject. Node2 accepts. Not a quorum, but 1 has accepted. Client2 gets CasWriteUncertainty
     * 5. client1 continue to propose the proposal/query1 to nodes, but the messages are not delivered to node2 and node3, so that they cannot accept.
     *    Note that the proposal is accepted by local, which is node1
     * 6. client3 starts as soon as client2 received unncertainty error. It get promises from quorum, which constists of node2 and node3.
     *    Therefore, it does not know the existence of query1, and it knows the accepted proposal/query2 from node2.
     * 7. client3 replay the proposal/query2. The state is updated by query2.
     */
    @Test
    public void testCasWriteUncertainty_ProposalReplayed() throws InterruptedException
    {
        // 2 contended LWT queries. same pk but different v.
        int curPk = pkGen.getAndIncrement();
        final String query1 = mkCasInsertQuery(a -> curPk, 1);
        final String query2 = mkCasInsertQuery(a -> curPk, 2);
        final String query3 = mkCasInsertQuery(a -> curPk, 3);
        final CountDownLatch client1Ready = new CountDownLatch(2);
        final CountDownLatch client2ProposeReady = new CountDownLatch(1);
        final CountDownLatch client3Ready = new CountDownLatch(1);
        final AtomicReference<Exception> exRef = new AtomicReference<>(null);
        Runnable client1 = () ->
        {
            awaitSilently(client1Ready);
            cluster.coordinator(1).execute(query1, ConsistencyLevel.QUORUM);
        };
        Runnable client2 = () -> // expecting client2 to see CasWriteUncertainException
        {
            try
            {
                cluster.coordinator(2).execute(query2, ConsistencyLevel.QUORUM);
            }
            catch (Exception ex)
            {
                exRef.set(ex);
                client3Ready.countDown(); // let client3 to start and replay the query from client2
            }
        };
        Runnable client3 = () -> // expecting it to repair query2
        {
            awaitSilently(client3Ready);
            cluster.coordinator(3).execute(query3, ConsistencyLevel.QUORUM);
        };
        cluster.verbs(Verb.PAXOS_PREPARE_RSP).from(1, 3).to(2).intercept(client1Ready::countDown).activate(); // start client1 as soon as client2 gets a quorum at prepare phase
        cluster.verbs(Verb.PAXOS_PREPARE_REQ, Verb.PAXOS_PROPOSE_REQ).from(1).to(2).drop().activate(); // client1 can get quorum from 1 and 3, but not 2 (req to 2 is dropped).
        cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(2).to(1, 3).intercept(() -> awaitSilently(client2ProposeReady)).activate(); // await for client1 getting quorum at prepare phase.
        cluster.verbs(Verb.PAXOS_PREPARE_RSP).from(3).to(1).intercept(client2ProposeReady::countDown).activate(); // allow client2 to propose once client1 gets quorum promise
        cluster.verbs(Verb.PAXOS_PROPOSE_REQ).from(1).to(2, 3).drop().activate(); // prevent client1 to get a quorum in prophse phase.
        cluster.verbs(Verb.PAXOS_PREPARE_REQ).from(3).to(1).drop().activate(); // drop the message from proposer3 to node1, so it is unaware of the existence of query1, which is more advanced than query2
        ExecutorService es = Executors.newFixedThreadPool(3);
        es.submit(client1);
        es.submit(client2);
        es.submit(client3);
        es.shutdown();
        es.awaitTermination(4, TimeUnit.SECONDS);
        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = " + curPk,
                                                  ConsistencyLevel.SERIAL),
                   row(curPk, 1, 2));
        Exception ex = exRef.get();
        Assert.assertNotNull("Expecting exception but found null. Proposer get none acceptance?", ex);
        Assert.assertThat("Expecting RuntimeException", ex, CoreMatchers.instanceOf(RuntimeException.class));
        Assert.assertTrue("Expecting cause to be CasWriteUncertainException", ex.getMessage().contains(CasWriteResultUnknownException.class.getCanonicalName()));
    }

    private void expectCasWriteTimeout()
    {
        thrown.expect(RuntimeException.class);
        thrown.expectCause(new BaseMatcher<Throwable>()
        {
            public boolean matches(Object item)
            {
                return InstanceClassLoader.wasLoadedByAnInstanceClassLoader(item.getClass());
            }

            public void describeTo(Description description)
            {
                description.appendText("Cause should be loaded by InstanceClassLoader");
            }
        });
        // unable to assert on class becuase the exception thrown was loaded by a differnet classloader, InstanceClassLoader
        // therefor asserts the FQCN name present in the message as a workaround
        thrown.expectMessage(containsString(CasWriteTimeoutException.class.getCanonicalName()));
        thrown.expectMessage(containsString("CAS operation timed out"));
    }

    // every invokation returns a query with an unique pk
    private String mkUniqueCasInsertQuery(int v)
    {
        return mkCasInsertQuery(AtomicInteger::getAndIncrement, v);
    }

    private String mkCasInsertQuery(Function<AtomicInteger, Integer> pkFunc, int v)
    {
        String query = String.format("INSERT INTO %s.tbl (pk, ck, v) VALUES (%d, 1, %d) IF NOT EXISTS", KEYSPACE, pkFunc.apply(pkGen), v);
        logger.info("Generated query: " + query);
        return query;
    }

    private void awaitSilently(CountDownLatch latch) {
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
