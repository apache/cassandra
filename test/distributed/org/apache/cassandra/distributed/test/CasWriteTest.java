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
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.notifications.SSTableMetadataChanged;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.INTERNALLY_FORCED;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.hamcrest.CoreMatchers.containsString;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class CasWriteTest extends TestBaseImpl
{
    // Sharing the same cluster to boost test speed. Using a pkGen to make sure queries has distinct pk value for paxos instances.
    private static ICluster cluster;
    private static final AtomicInteger pkGen = new AtomicInteger(1_000); // preserve any pk values less than 1000 for manual queries.
    private static final Logger logger = LoggerFactory.getLogger(CasWriteTest.class);
    private static final long GC_GRACE_SECONDS = 10;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupCluster() throws Throwable
    {
        cluster = init(Cluster.build().withNodes(3).withConfig(config -> config.set("paxos_state_purging", "repaired")
                                                                               .set("paxos_variant", "v2")
                                                                               .set("paxos_cache_size", "0MiB")).start());
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = " + GC_GRACE_SECONDS);
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
        cluster.filters().verbs(Verb.PAXOS2_PREPARE_REQ.id).from(1).to(2, 3).drop().on(); // drop the internode messages to acceptors
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtPreparePhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS2_PREPARE_RSP.id).from(2, 3).to(1).drop().on(); // drop the internode messages to acceptors
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtProposePhase_ReqLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS_PROPOSE_REQ.id).from(1).to(2, 3).drop().on();
        cluster.filters().verbs(Verb.PAXOS2_PROPOSE_REQ.id).from(1).to(2, 3).drop().on();
        cluster.coordinator(1).execute(mkUniqueCasInsertQuery(1), ConsistencyLevel.QUORUM);
    }

    @Test
    public void testCasWriteTimeoutAtProposePhase_RspLost()
    {
        expectCasWriteTimeout();
        cluster.filters().verbs(Verb.PAXOS_PROPOSE_RSP.id).from(2, 3).to(1).drop().on();
        cluster.filters().verbs(Verb.PAXOS2_PROPOSE_RSP.id).from(2, 3).to(1).drop().on();
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
        final IMessageFilters.Matcher matcher = (from, to, msg) -> {
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
        };
        cluster.filters().verbs(Verb.PAXOS_PROPOSE_REQ.id).from(1).to(2, 3).messagesMatching(matcher).drop();
        cluster.filters().verbs(Verb.PAXOS2_PROPOSE_REQ.id).from(1).to(2, 3).messagesMatching(matcher).drop();

        try
        {
            cluster.coordinator(1).execute(mkCasInsertQuery((a) -> pk, 1, 1), ConsistencyLevel.QUORUM);
        }
        catch (Throwable t)
        {
            final Class<?> exceptionClass = isPaxosVariant2() ? CasWriteTimeoutException.class : CasWriteUnknownResultException.class;
            Assert.assertEquals("Expecting cause to be " + exceptionClass.getSimpleName(),
                                exceptionClass.getCanonicalName(), t.getClass().getCanonicalName());
            return;
        }
        Assert.fail("Expecting test to throw a CasWriteUnknownResultException");
    }

    @Test
    public void testStaleCommitInSystemPaxos() throws InterruptedException
    {
        cluster.filters().reset();
        int extraKeys = 10;
        int pk = pkGen.getAndAdd(extraKeys + 1);

        cluster.coordinator(1).execute(mkCasInsertQuery((a) -> pk, 1, 1), ConsistencyLevel.ALL);
        for (int i = 1 ; i <= 3 ; ++i)
        {
            ((IInvokableInstance)cluster.get(i)).runOnInstance(() -> DatabaseDescriptor.setPaxosPurgeGrace(0));
        }

        long insertTimestamp = ((IInvokableInstance)cluster.get(3)).applyOnInstance(pk_ -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(pk_));
            return SystemKeyspace.loadPaxosState(key, cfs.metadata.get(), FBUtilities.nowInSeconds()).committed.ballot.uuidTimestamp();
        }, pk);

        ((IInvokableInstance)cluster.get(3)).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("paxos");
            cfs.forceFlush(INTERNALLY_FORCED).awaitUninterruptibly();
            cfs.getLiveSSTables().forEach(s -> {
                try
                {
                    StatsMetadata oldMetadata = s.getSSTableMetadata();
                    s.mutateLevelAndReload(3);
                    cfs.getCompactionStrategyManager().handleNotification(new SSTableMetadataChanged(s, oldMetadata), null);
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                }
            });
        });
        cluster.coordinator(1).execute(mkCasDeleteQuery((a) -> pk, 1, 1), ConsistencyLevel.ALL);

        long deleteTimestamp = ((IInvokableInstance)cluster.get(3)).applyOnInstance(pk_ -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(pk_));
            return SystemKeyspace.loadPaxosState(key, cfs.metadata.get(), FBUtilities.nowInSeconds()).committed.ballot.uuidTimestamp();
        }, pk);

        cluster.get(1).nodetool("repair", "--paxos-only", KEYSPACE, "tbl");

        // write and flush enough data to trigger purge of the deletion commit, without touching the earlier insertion commit that is in a higher level
        for (int i = 0 ; i < 10 ; ++i)
        {
            for (int j = 1; j <= extraKeys ; ++j)
            {
                final int pkj = pk + j;
                cluster.coordinator(1).execute(mkCasInsertQuery(a -> pkj, i, 1), ConsistencyLevel.ALL);
            }
            for (int k = 1 ; k <= 3 ; ++k)
            {
                ((IInvokableInstance)cluster.get(k)).runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("paxos");
                    cfs.forceFlush(INTERNALLY_FORCED).awaitUninterruptibly();
                    ColumnFamilyStore cfs2 = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                    cfs2.forceFlush(INTERNALLY_FORCED).awaitUninterruptibly();
                });
            }
        }

        for (int k = 1 ; k <= 3 ; ++k)
        {
            ((IInvokableInstance)cluster.get(k)).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("paxos");
                while (cfs.getCompactionStrategyManager().getEstimatedRemainingTasks() > 0)
                {
                    try { Thread.sleep(1000); }
                    catch (InterruptedException e) { throw new RuntimeException(e); }
                }
            });
        }

        long repairTimestamp = ((IInvokableInstance)cluster.get(3)).applyOnInstance(pk_ -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(pk_));
            return cfs.getPaxosRepairHistory().ballotForToken(key.getToken()).uuidTimestamp();
        }, pk);

        long afterRepairTimestampOn1 = ((IInvokableInstance)cluster.get(1)).applyOnInstance(pk_ -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(pk_));
            return SystemKeyspace.loadPaxosState(key, cfs.metadata.get(), FBUtilities.nowInSeconds()).committed.ballot.uuidTimestamp();
        }, pk);

        long afterRepairTimestampOn3 = ((IInvokableInstance)cluster.get(3)).applyOnInstance(pk_ -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(pk_));
            return SystemKeyspace.loadPaxosState(key, cfs.metadata.get(), FBUtilities.nowInSeconds()).committed.ballot.uuidTimestamp();
        }, pk);

        Assert.assertEquals(Ballot.none().uuidTimestamp(), afterRepairTimestampOn1);

        logger.info("Waiting for tombstone to be purgeable");
        Thread.sleep(GC_GRACE_SECONDS * 1000);
        while (FBUtilities.timestampMicros() - (GC_GRACE_SECONDS * 1000_000) < TimeUUID.rawTimestampToUnixMicros(deleteTimestamp))
            Thread.sleep(1000);

        cluster.get(1).nodetool("compact", KEYSPACE, "tbl");

        for (int i = 1 ; i <= 3 ; ++i)
        {
            int partitionCount = ((IInvokableInstance)cluster.get(3)).applyOnInstance(pk_ -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(pk_));
                return Util.getAllUnfiltered(SinglePartitionReadCommand.create(cfs.metadata.get(), FBUtilities.nowInSeconds(), key, cfs.metadata.get().comparator.make(Int32Type.instance.decompose(1)))).size();
            }, pk);
            Assert.assertEquals(0, partitionCount);
        }

        cluster.filters().allVerbs().from(1).to(2).drop();
        // we must first perform a write as the read has proposal stability and so responds async
        cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE ck = 1 AND pk = " + pk + " IF EXISTS", ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
        Assert.assertArrayEquals(new Object[0], cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = " + pk, ConsistencyLevel.SERIAL));
    }

    private static boolean isPaxosVariant2()
    {
        return Config.PaxosVariant.v2.name().equals(cluster.coordinator(1).instance().config().getString("paxos_variant"));
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

    private String mkCasDeleteQuery(Function<AtomicInteger, Integer> pkFunc, int ck, int v)
    {
        String query = String.format("DELETE FROM %s.tbl WHERE pk = %d AND ck = 1 IF EXISTS", KEYSPACE, pkFunc.apply(pkGen));
        logger.info("Generated query: " + query);
        return query;
    }
}
