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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import static org.hamcrest.CoreMatchers.containsString;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class CasWriteTest extends TestBaseImpl
{
    // Sharing the same cluster to boost test speed. Using a pkGen to make sure queries has distinct pk value for paxos instances.
    private static ICluster cluster;
    private static final AtomicInteger pkGen = new AtomicInteger(1_000); // preserve any pk values less than 1000 for manual queries.
    private static final Logger logger = LoggerFactory.getLogger(CasWriteTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupCluster() throws Throwable
    {
        cluster = init(Cluster.build().withNodes(3).start());
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
    }

    @AfterClass
    public static void close() throws Exception
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
        cluster.filters().verbs(Verb.PAXOS_PREPARE_REQ.id).from(1).to(2, 3).drop().on(); // drop the internode messages to acceptors
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtPreparePhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS_PREPARE_RSP.id).from(2, 3).to(1).drop().on(); // drop the internode messages to acceptors
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtProposePhase_ReqLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS_PROPOSE_REQ.id).from(1).to(2, 3).drop().on();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtProposePhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS_PROPOSE_RSP.id).from(2, 3).to(1).drop().on();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtCommitPhase_ReqLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).from(1).to(2, 3).drop().on();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtCommitPhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS_COMMIT_RSP.id).from(2, 3).to(1).drop().on();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }



    @Test
    public void casWriteContentionTimeoutTest() throws InterruptedException
    {
        testWithContention(101,
                           Arrays.asList(1, 3),
                           c -> {
                               c.filters().reset();
                               c.filters().verbs(Verb.PAXOS_PREPARE_REQ.id).from(1).to(3).drop();
                               c.filters().verbs(Verb.PAXOS_PROPOSE_REQ.id).from(1).to(2).drop();
                           },
                           failure ->
                               failure.get() != null &&
                               failure.get()
                                      .getClass().getCanonicalName()
                                      .equals(CasWriteTimeoutException.class.getCanonicalName()),
                           "Expecting cause to be CasWriteTimeoutException");
    }

    private void testWithContention(int testUid,
                                    List<Integer> contendingNodes,
                                    Consumer<ICluster> setupForEachRound,
                                    Function<AtomicReference<Throwable>, Boolean> expectedException,
                                    String assertHintMessage) throws InterruptedException
    {
        assert contendingNodes.size() == 2;
        AtomicInteger curPk = new AtomicInteger(1);
        ExecutorService es = Executors.newFixedThreadPool(3);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Supplier<Boolean> hasExpectedException = () -> expectedException.apply(failure);
        while (!hasExpectedException.get())
        {
            failure.set(null);
            setupForEachRound.accept(cluster);

            List<Future<?>> futures = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(3);
            contendingNodes.forEach(nodeId -> {
                String query = mkCasInsertQuery((a) -> curPk.get(), testUid, nodeId);
                futures.add(es.submit(() -> {
                    try
                    {
                        latch.countDown();
                        latch.await(1, TimeUnit.SECONDS); // help threads start at approximately same time
                        cluster.coordinator(nodeId).execute(query, ConsistencyLevel.QUORUM);
                    }
                    catch (Throwable t)
                    {
                        failure.set(t);
                    }
                }));
            });

            FBUtilities.waitOnFutures(futures);
            curPk.incrementAndGet();
        }

        es.shutdownNow();
        es.awaitTermination(1, TimeUnit.MINUTES);
        Assert.assertTrue(assertHintMessage, hasExpectedException.get());
    }

    private void expectCasWriteTimeout()
    {
        thrown.expect(new BaseMatcher<Throwable>()
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
        thrown.expect(new BaseMatcher<Throwable>()
        {
            public boolean matches(Object item)
            {
                return item.getClass().getCanonicalName().equals(CasWriteTimeoutException.class.getCanonicalName());
            }

            public void describeTo(Description description)
            {
                description.appendText("Class was expected to be " + CasWriteTimeoutException.class.getCanonicalName() + " but was not");
            }
        });
        thrown.expectMessage(containsString("CAS operation timed out"));
    }

    @Test
    public void testWriteUnknownResult()
    {
        cluster.filters().reset();
        int pk = pkGen.getAndIncrement();
        CountDownLatch ready = new CountDownLatch(1);
        cluster.filters().verbs(Verb.PAXOS_PROPOSE_REQ.id).from(1).to(2, 3).messagesMatching((from, to, msg) -> {
            if (to == 2)
            {
                // Inject a single CAS request in-between prepare and propose phases
                cluster.coordinator(2).execute(mkCasInsertQuery((a) -> pk, 1, 2),
                                               ConsistencyLevel.QUORUM);
                ready.countDown();
            } else {
                Uninterruptibles.awaitUninterruptibly(ready);
            }
            return false;
        }).drop();

        try
        {
            cluster.coordinator(1).execute(mkCasInsertQuery((a) -> pk, 1, 1), ConsistencyLevel.QUORUM);
        }
        catch (Throwable t)
        {
            Assert.assertEquals("Expecting cause to be CasWriteUnknownResultException",
                                CasWriteUnknownResultException.class.getCanonicalName(), t.getClass().getCanonicalName());
            return;
        }
        Assert.fail("Expecting test to throw a CasWriteUnknownResultException");
    }

    // every invokation returns a query with an unique pk
    private String mkUniqueCasInsertQuery(int v)
    {
        return mkCasInsertQuery(AtomicInteger::getAndIncrement, 1, v);
    }

    private String mkCasInsertQuery(Function<AtomicInteger, Integer> pkFunc, int ck, int v)
    {
        String query = String.format("INSERT INTO %s.tbl (pk, ck, v) VALUES (%d, %d, %d) IF NOT EXISTS", KEYSPACE, pkFunc.apply(pkGen), ck, v);
        logger.info("Generated query: " + query);
        return query;
    }
}
