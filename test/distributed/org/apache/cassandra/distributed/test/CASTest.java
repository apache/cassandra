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

import java.io.IOException;
import java.util.UUID;
import java.util.function.BiConsumer;


import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.fail;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PROPOSE_REQ;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CASTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(CASTest.class);

    /**
     * The {@code cas_contention_timeout_in_ms} used during the tests
     */
    private static final long CONTENTION_TIMEOUT = 1000L;

    /**
     * The {@code write_request_timeout_in_ms} used during the tests
     */
    private static final long REQUEST_TIMEOUT = 1000L;

    @Test
    public void simpleUpdate() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 1));
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 3 WHERE pk = 1 and ck = 1 IF v = 2", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 1));
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 2));
        }
    }

    @Test
    public void incompletePrepare() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                                                                      .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop = cluster.filters().verbs(PAXOS_PREPARE_REQ.id).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertEquals("CAS operation timed out - encountered contentions: 0", e.getMessage());
            }
            drop.off();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL));
        }
    }

    @Test
    public void incompletePropose() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                                                                      .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop1 = cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertEquals("CAS operation timed out - encountered contentions: 0", e.getMessage());
            }
            drop1.off();
            // make sure we encounter one of the in-progress proposals so we complete it
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id).from(1).to(2).drop();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 2));
        }
    }

    @Test
    public void incompleteCommit() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                                                                      .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT))))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            IMessageFilters.Filter drop1 = cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(1).to(2, 3).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail();
            }
            catch (RuntimeException e)
            {
                Assert.assertEquals("CAS operation timed out - encountered contentions: 0", e.getMessage());
            }
            drop1.off();
            // make sure we see one of the successful commits
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(2).drop();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", ConsistencyLevel.QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.SERIAL),
                    row(1, 1, 2));
        }
    }

    private int[] paxosAndReadVerbs() {
        return new int[] { PAXOS_PREPARE_REQ.id, PAXOS_PROPOSE_REQ.id, PAXOS_COMMIT_REQ.id, READ_REQ.id };
    }

    /**
     * Base test to ensure that if a write times out but with a proposal accepted by some nodes (less then quorum), and
     * a following SERIAL operation does not observe that write (the node having accepted it do not participate in that
     * following operation), then that write is never applied, even when the nodes having accepted the original proposal
     * participate.
     *
     * <p>In other words, if an operation timeout, it may or may not be applied, but that "fate" is persistently decided
     * by the very SERIAL operation that "succeed" (in the sense of 'not timing out or throwing some other exception').
     *
     * @param postTimeoutOperation1 a SERIAL operation executed after an initial write that inserts the row [0, 0] times
     *                              out. It is executed with a QUORUM of nodes that have _not_ see the timed out
     *                              proposal, and so that operation should expect that the [0, 0] write has not taken
     *                              place.
     * @param postTimeoutOperation2 a 2nd SERIAL operation executed _after_ {@code postTimeoutOperation1}, with no
     *                              write executed between the 2 operation. Contrarily to the 1st operation, the QORUM
     *                              for this operation _will_ include the node that got the proposal for the [0, 0]
     *                              insert but didn't participated to {@code postTimeoutOperation1}}. That operation
     *                              should also no witness that [0, 0] write (since {@code postTimeoutOperation1}
     *                              didn't).
     * @param loseCommitOfOperation1 if {@code true}, the test will also drop the "commits" messages for
     *                               {@code postTimeoutOperation1}. In general, the test should behave the same with or
     *                               without that flag since a value is decided as soon as it has been "accepted by
     *                               quorum" and the commits should always be properly replayed.
     */
    private void consistencyAfterWriteTimeoutTest(BiConsumer<String, ICoordinator> postTimeoutOperation1,
                                                  BiConsumer<String, ICoordinator> postTimeoutOperation2,
                                                  boolean loseCommitOfOperation1) throws IOException
    {
        // It's unclear why (haven't dug), but in some of the instance of this test method, there is a consistent 2+
        // seconds pauses between the prepare and propose phases during the execution of 'postTimeoutOperation2'. This
        // does not happen on 3.0 and there is no report of such long pauses otherwise, so an hypothesis is that this
        // is due to the in-jvm dtest framework. This is is why we use a 4 seconds timeout here. Given this test is
        // not about performance, this is probably ok, even if we ideally should dug into the underlying reason.
        try (Cluster cluster = init(Cluster.create(3, config -> config.set("write_request_timeout_in_ms", 4000L)
                                                                      .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT))))
        {
            String table = KEYSPACE + ".t";
            cluster.schemaChange("CREATE TABLE " + table + " (k int PRIMARY KEY, v int)");

            // We do a CAS insertion, but have with the PROPOSE message dropped on node 1 and 2. The CAS will not get
            // through and should timeout. Importantly, node 3 does receive and answer the PROPOSE.
            IMessageFilters.Filter dropProposeFilter = cluster.filters()
                                                              .inbound()
                                                              .verbs(PAXOS_PROPOSE_REQ.id)
                                                              .from(3)
                                                              .to(1, 2)
                                                              .drop();
            try
            {
                // NOTE: the consistency below is the "commit" one, so it doesn't matter at all here.
                // NOTE 2: we use node 3 as coordinator because message filters don't currently work for locally
                //   delivered messages and as we want to drop messages to 1 and 2, we can't use them.
                cluster.coordinator(3)
                       .execute("INSERT INTO " + table + "(k, v) VALUES (0, 0) IF NOT EXISTS", ConsistencyLevel.ONE);
                fail("The insertion should have timed-out");
            }
            catch (Exception e)
            {
                // We expect a write timeout. If we get one, the test can continue, otherwise, we rethrow. Note that we
                // look at the root cause because the dtest framework effectively wrap the exception in a RuntimeException
                // (we could just look at the immediate cause, but this feel a bit more resilient this way).
                // TODO: we can't use an instanceof below because the WriteTimeoutException we get is from a different class
                //  loader than the one the test run under, and that's our poor-man work-around. This kind of things should
                //  be improved at the dtest API level.
                if (!e.getClass().getSimpleName().equals("CasWriteTimeoutException"))
                    throw e;
            }
            finally
            {
                dropProposeFilter.off();
            }

            // Isolates node 3 and executes the SERIAL operation. As neither node 1 or 2 got the initial insert proposal,
            // there is nothing to "replay" and the operation should assert the table is still empty.
            IMessageFilters.Filter ignoreNode3Filter = cluster.filters().verbs(paxosAndReadVerbs()).to(3).drop();
            IMessageFilters.Filter dropCommitFilter = null;
            if (loseCommitOfOperation1)
            {
                dropCommitFilter = cluster.filters().verbs(PAXOS_COMMIT_REQ.id).to(1, 2).drop();
            }
            try
            {
                postTimeoutOperation1.accept(table, cluster.coordinator(1));
            }
            finally
            {
                ignoreNode3Filter.off();
                if (dropCommitFilter != null)
                    dropCommitFilter.off();
            }

            // Node 3 is now back and we isolate node 2 to ensure the next read hits node 1 and 3.
            // What we want to ensure is that despite node 3 having the initial insert in its paxos state in a position of
            // being replayed, that insert is _not_ replayed (it would contradict serializability since the previous
            // operation asserted nothing was inserted). It is this execution that failed before CASSANDRA-12126.
            IMessageFilters.Filter ignoreNode2Filter = cluster.filters().verbs(paxosAndReadVerbs()).to(2).drop();
            try
            {
                postTimeoutOperation2.accept(table, cluster.coordinator(1));
            }
            finally
            {
                ignoreNode2Filter.off();
            }
        }
    }

    /**
     * Tests that if a write timeouts and a following serial read does not see that write, then no following reads sees
     * it, even if some nodes still have the write in their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void readConsistencyAfterWriteTimeoutTest() throws IOException
    {
        BiConsumer<String, ICoordinator> operation =
            (table, coordinator) -> assertRows(coordinator.execute("SELECT * FROM " + table + " WHERE k=0",
                                                                   ConsistencyLevel.SERIAL));

        consistencyAfterWriteTimeoutTest(operation, operation, false);
        consistencyAfterWriteTimeoutTest(operation, operation, true);
    }

    /**
     * Tests that if a write timeouts, then a following CAS succeed but does not apply in a way that indicate the write
     * has not applied, then no following CAS can see that initial insert , even if some nodes still have the write in
     * their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void nonApplyingCasConsistencyAfterWriteTimeout() throws IOException
    {
        // Note: we use CL.ANY so that the operation don't timeout in the case where we "lost" the operation1 commits.
        // The commit CL shouldn't have impact on this test anyway, so this doesn't diminishes the test.
        BiConsumer<String, ICoordinator> operation =
            (table, coordinator) -> assertCasNotApplied(coordinator.execute("UPDATE " + table + " SET v = 1 WHERE k = 0 IF v = 0",
                                                                            ConsistencyLevel.ANY));
        consistencyAfterWriteTimeoutTest(operation, operation, false);
        consistencyAfterWriteTimeoutTest(operation, operation, true);
    }

    /**
     * Tests that if a write timeouts and a following serial read does not see that write, then no following CAS see
     * that initial insert, even if some nodes still have the write in their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void mixedReadAndNonApplyingCasConsistencyAfterWriteTimeout() throws IOException
    {
        BiConsumer<String, ICoordinator> operation1 =
            (table, coordinator) -> assertRows(coordinator.execute("SELECT * FROM " + table + " WHERE k=0",
                                                                   ConsistencyLevel.SERIAL));
        BiConsumer<String, ICoordinator> operation2 =
            (table, coordinator) -> assertCasNotApplied(coordinator.execute("UPDATE " + table + " SET v = 1 WHERE k = 0 IF v = 0",
                                                                            ConsistencyLevel.QUORUM));
        consistencyAfterWriteTimeoutTest(operation1, operation2, false);
        consistencyAfterWriteTimeoutTest(operation1, operation2, true);
    }

    /**
     * Tests that if a write timeouts and a following CAS succeed but does not apply in a way that indicate the write
     * has not applied, then following serial reads do no see that write, even if some nodes still have the write in
     * their paxos state.
     *
     * <p>This specifically test for the inconsistency described/fixed by CASSANDRA-12126.
     */
    @Test
    public void mixedNonApplyingCasAndReadConsistencyAfterWriteTimeout() throws IOException
    {
        // Note: we use CL.ANY so that the operation don't timeout in the case where we "lost" the operation1 commits.
        // The commit CL shouldn't have impact on this test anyway, so this doesn't diminishes the test.
        BiConsumer<String, ICoordinator> operation1 =
            (table, coordinator) -> assertCasNotApplied(coordinator.execute("UPDATE " + table + " SET v = 1 WHERE k = 0 IF v = 0",
                                                                            ConsistencyLevel.ANY));
        BiConsumer<String, ICoordinator> operation2 =
            (table, coordinator) -> assertRows(coordinator.execute("SELECT * FROM " + table + " WHERE k=0",
                                                                   ConsistencyLevel.SERIAL));
        consistencyAfterWriteTimeoutTest(operation1, operation2, false);
        consistencyAfterWriteTimeoutTest(operation1, operation2, true);
    }

    // TODO: this shoud probably be moved into the dtest API.
    private void assertCasNotApplied(Object[][] resultSet)
    {
        assertFalse("Expected a CAS resultSet (with at least application result) but got an empty one.",
                    resultSet.length == 0);
        assertFalse("Invalid empty first row in CAS resultSet.", resultSet[0].length == 0);
        Object wasApplied = resultSet[0][0];
        assertTrue("Expected 1st column of CAS resultSet to be a boolean, but got a " + wasApplied.getClass(),
                   wasApplied instanceof Boolean);
        assertFalse("Expected CAS to not be applied, but was applied.", (Boolean)wasApplied);
    }

    /**
     * Failed write (by node that did not yet witness a range movement via gossip) is witnessed later as successful
     * conflicting with another successful write performed by a node that did witness the range movement
     * Prepare, Propose and Commit A to {1, 2}
     * Range moves to {2, 3, 4}
     * Prepare and Propose B (=> !A) to {3, 4}
     */
    @Ignore
    @Test
    public void testSuccessfulWriteBeforeRangeMovement() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {1} is unaware (yet) that {4} is an owner of the token
            cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2,3} => {1}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            for (int i = 1 ; i <= 3 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {4} reads from !{2} => {3, 4}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(4).to(2).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(4).to(2).drop();
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(false, pk, 1, 1, null));
        }
    }

    /**
     * Failed write (by node that did not yet witness a range movement via gossip) is witnessed later as successful
     * conflicting with another successful write performed by a node that did witness the range movement
     *  - Range moves from {1, 2, 3} to {2, 3, 4}, witnessed by X (not by !X)
     *  -  X: Prepare, Propose and Commit A to {3, 4}
     *  - !X: Prepare and Propose B (=>!A) to {1, 2}
     */
    @Ignore
    @Test
    public void testConflictingWritesWithStaleRingInformation() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {1} is unaware (yet) that {4} is an owner of the token
            cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));

            // {4} promises, accepts and commits on !{2} => {3, 4}
            int pk = pk(cluster, 1, 2);
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(4).to(2).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(4).to(2).drop();
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(4).to(2).drop();
            assertRows(cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // {1} promises, accepts and commmits on !{3} => {1, 2}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(1).to(3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(false, pk, 1, 1, null));
        }
    }

    /**
     * Successful write during range movement, not witnessed by read after range movement.
     * Very similar to {@link #testConflictingWritesWithStaleRingInformation}.
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range movement witnessed by !X
     *  - Any: Prepare and Read from {3, 4}
     */
    @Ignore
    @Test
    public void testSucccessfulWriteDuringRangeMovementFollowedByRead() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has not propagated to other nodes yet
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            cluster.get(4).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {1} promises and accepts on !{3} => {1, 2}; commmits on !{2, 3} => {1}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", ConsistencyLevel.SERIAL, pk),
                    row(pk, 1, 1));
        }
    }

    /**
     * Successful write during range movement not witnessed by write after range movement
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range movement witnessed by !X
     *  - Any: Prepare and Propose to {3, 4}
     */
    @Ignore
    @Test
    public void testSuccessfulWriteDuringRangeMovementFollowedByConflicting() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has not propagated to other nodes yet
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            cluster.get(4).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(false, pk, 1, 1, null));

            // TODO: repair and verify base table state
        }
    }

    /**
     * During a range movement, a CAS may fail leaving side effects that are not witnessed by another operation
     * being performed with stale ring information.
     * This is a particular special case of stale ring information sequencing, which probably would be resolved
     * by fixing each of the more isolated cases (but is unique, so deserving of its own test case).
     * See CASSANDRA-15745
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -   X: Prepare to {2, 3, 4}
     *  -   X: Propose to {4}
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range move visible by !X
     *  - Any: Prepare and Read from {3, 4}
     */
    @Ignore
    @Test
    public void testIncompleteWriteFollowedBySuccessfulWriteWithStaleRingDuringRangeMovementFollowedByRead() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has not propagated to other nodes yet
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            cluster.get(4).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {4} promises and accepts on !{1} => {2, 3, 4}; commits on !{1, 2, 3} => {4}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(4).to(1).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(4).to(1, 2, 3).drop();
            try
            {
                cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM, pk);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(3).to(2).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", ConsistencyLevel.SERIAL, pk),
                    row(pk, 1, null, 2));
        }
    }

    /**
     * During a range movement, a CAS may fail leaving side effects that are not witnessed by another operation
     * being performed with stale ring information.
     * This is a particular special case of stale ring information sequencing, which probably would be resolved
     * by fixing each of the more isolated cases (but is unique, so deserving of its own test case).
     * See CASSANDRA-15745
     *
     *  - Range moves from {1, 2, 3} to {2, 3, 4}; witnessed by X (not by !X)
     *  -   X: Prepare to {2, 3, 4}
     *  -   X: Propose to {4}
     *  -  !X: Prepare and Propose to {1, 2}
     *  - Range move visible by !X
     *  - Any: Prepare and Propose to {3, 4}
     */
    @Ignore
    @Test
    public void testIncompleteWriteFollowedBySuccessfulWriteWithStaleRingDuringRangeMovementFollowedByWrite() throws Throwable
    {
        try (Cluster cluster = Cluster.create(4, config -> config
                .set("write_request_timeout_in_ms", REQUEST_TIMEOUT)
                .set("cas_contention_timeout_in_ms", CONTENTION_TIMEOUT)))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

            // make it so {4} is bootstrapping, and this has not propagated to other nodes yet
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(1).acceptsOnInstance(Instance::removeFromRing).accept(cluster.get(4));
            cluster.get(4).acceptsOnInstance(Instance::addToRingBootstrapping).accept(cluster.get(4));

            int pk = pk(cluster, 1, 2);

            // {4} promises and accepts on !{1} => {2, 3, 4}; commits on !{1, 2, 3} => {4}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(4).to(1).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(4).to(1, 2, 3).drop();
            try
            {
                cluster.coordinator(4).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (?, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM, pk);
                Assert.assertTrue(false);
            }
            catch (RuntimeException wrapped)
            {
                Assert.assertEquals("Operation timed out - received only 1 responses.", wrapped.getCause().getMessage());
            }

            // {1} promises and accepts on !{3} => {1, 2}; commits on !{2, 3} => {1}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(1).to(3).drop();
            cluster.filters().verbs(PAXOS_COMMIT_REQ.id).from(1).to(2, 3).drop();
            assertRows(cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(true));

            // finish topology change
            for (int i = 1 ; i <= 4 ; ++i)
                cluster.get(i).acceptsOnInstance(Instance::addToRingNormal).accept(cluster.get(4));

            // {3} reads from !{2} => {3, 4}
            cluster.filters().verbs(PAXOS_PREPARE_REQ.id, READ_REQ.id).from(3).to(2).drop();
            cluster.filters().verbs(PAXOS_PROPOSE_REQ.id).from(3).to(2).drop();
            assertRows(cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v2) VALUES (?, 1, 2) IF NOT EXISTS", ConsistencyLevel.ONE, pk),
                    row(false, 5, 1, null, 2));
        }
    }

    private static int pk(Cluster cluster, int lb, int ub)
    {
        return pk(cluster.get(lb), cluster.get(ub));
    }

    private static int pk(IInstance lb, IInstance ub)
    {
        return pk(Murmur3Partitioner.instance.getTokenFactory().fromString(lb.config().getString("initial_token")),
                Murmur3Partitioner.instance.getTokenFactory().fromString(ub.config().getString("initial_token")));
    }

    private static int pk(Token lb, Token ub)
    {
        int pk = 0;
        Token pkt;
        while (lb.compareTo(pkt = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(pk))) >= 0 || ub.compareTo(pkt) < 0)
            ++pk;
        return pk;
    }

    private static void debugOwnership(Cluster cluster, int pk)
    {
        for (int i = 1 ; i <= cluster.size() ; ++i)
            System.out.println(i + ": " + cluster.get(i).appliesOnInstance((Integer v) -> StorageService.instance.getNaturalEndpointsWithPort(KEYSPACE, Int32Type.instance.decompose(v)))
                    .apply(pk));
    }

    private static void debugPaxosState(Cluster cluster, int pk)
    {
        TableId tableId = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata.id);
        for (int i = 1 ; i <= cluster.size() ; ++i)
            for (Object[] row : cluster.get(i).executeInternal("select in_progress_ballot, proposal_ballot, most_recent_commit_at from system.paxos where row_key = ? and cf_id = ?", Int32Type.instance.decompose(pk), tableId))
                System.out.println(i + ": " + (row[0] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[0])) + ", " + (row[1] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[1])) + ", " + (row[2] == null ? 0L : UUIDGen.microsTimestamp((UUID)row[2])));
    }

}
