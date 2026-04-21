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

package org.apache.cassandra.distributed.test.tracking;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.MessageSpy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.paxos.Commit;

import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.alterReplicationType;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasApplied;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasNotApplied;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicaHasNoRow;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicasAreExactly;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicasHaveValue;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.buildPaxosCluster;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.casAsync;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.createKeyspace;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.messageHasMutationId;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.on;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.pauseHintsAndReconciler;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.respondWithTimeout;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PaxosMutationTrackingForwardingV2Test extends TestBaseImpl
{
    private static Cluster cluster;
    private static final int KEY = 5;

    @BeforeClass
    public static void setup() throws Throwable
    {
        CassandraRelevantProperties.DISABLE_CONSENSUS_REQUEST_FORWARDING.setBoolean(true);
        cluster = init(buildPaxosCluster(4, "v2").start());
        pauseHintsAndReconciler(cluster);
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
        CassandraRelevantProperties.DISABLE_CONSENSUS_REQUEST_FORWARDING.reset();
    }

    @After
    public void resetFilters()
    {
        cluster.filters().reset();
        ClusterUtils.awaitTCMCatchUp(cluster);
    }

    private String newKeyspace(String replicationType)
    {
        String ks = createKeyspace(cluster, "pmt_fwd", replicationType);
        ClusterUtils.awaitTCMCatchUp(cluster);
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });
        return ks;
    }

    @Test
    public void testPrepareRefreshForwardHandler() throws Throwable
    {
        String ks = newKeyspace("untracked");

        // Partial commit on node 1 only (commits to replicas 2 and 3 blocked) so they remain stale
        // when the keyspace flips to tracked. The subsequent CAS from node 4 will force a
        // prepare-refresh that exercises PrepareRefreshForwardHandler.
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).from(1).to(2, 3, 4).drop();

        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because commits to nodes 2,3,4 were blocked");
        }
        catch (Exception e)
        {
            // First CAS is expected to throw: the commit phase was intentionally blocked to 2,3,4.
        }

        cluster.filters().reset();

        alterReplicationType(cluster, ks, "tracked");
        ClusterUtils.awaitTCMCatchUp(cluster);

        // Count PAXOS_PREPARE_REFRESH_FORWARD_REQ arrivals at any replica.
        MessageSpy forwardSpy = on(cluster, Verb.PAXOS_PREPARE_REFRESH_FORWARD_REQ)
                                .inbound()
                                .start();

        // Custom spy: extract the mutation ID string from each PAXOS2_PREPARE_REFRESH_REQ payload so
        // we can assert that the forward handler produced a non-empty mutation ID. MessageSpy can
        // only count by ID presence — it cannot capture the string representation — so this filter
        // stays manual.
        AtomicInteger refreshWithMutationId = new AtomicInteger();
        Set<String> observedMutationIds = ConcurrentHashMap.newKeySet();
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_REFRESH_REQ.id)
               .messagesMatching((from, to, msg) -> {
                   String mutId = cluster.get(to).callsOnInstance(() -> {
                       try
                       {
                           Message<?> deserialized = Instance.deserializeMessage(msg);
                           Object payload = deserialized.payload;
                           Field f = payload.getClass().getDeclaredField("missingCommit");
                           f.setAccessible(true);
                           Commit commit = (Commit) f.get(payload);
                           if (!commit.mutation.id().isNone())
                               return commit.mutation.id().toString();
                           return null;
                       }
                       catch (Exception e)
                       {
                           throw new RuntimeException(e);
                       }
                   }).call();
                   if (mutId != null)
                   {
                       refreshWithMutationId.incrementAndGet();
                       observedMutationIds.add(mutId);
                   }
                   return false;
               }).drop();

        // Drop prepare responses from node 3 so quorum can only be formed by node 1 + node 2.
        // Node 1 has the latest committed ballot; when its response arrives (in any order relative
        // to node 2), the coordinator will see withLatest()=1 < quorum=2 with
        // haveReadResponseWithLatest=true, triggering refreshStaleParticipants rather than
        // FOUND_INCOMPLETE_COMMITTED.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_RSP.id)
               .from(3)
               .to(4)
               .drop();

        // The CAS may timeout during the commit phase due to migration state, but the handler's
        // effect is observable via the intercepted PAXOS2_PREPARE_REFRESH_REQ messages.
        boolean casThrew = false;
        Object[][] casResult = null;
        try
        {
            try
            {
                casResult = cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 99) IF NOT EXISTS",
                                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            }
            catch (Exception e)
            {
                casThrew = true;
            }

            // Node 2 is the sole stale participant (it responded with an older commit). Node 3 never
            // responded to prepare (its PAXOS2_PREPARE_RSP was dropped), so it is NOT in needLatest
            // and is not targeted for refresh. The forward sends refresh only to node 2.
            int forwards = forwardSpy.total();
            assertTrue("At least 1 forward should have been sent, got " + forwards,
                       forwards >= 1);

            // The stale participant (node 2) should receive a refresh with a mutation ID. This may
            // come via a network PAXOS2_PREPARE_REFRESH_REQ (counted here) or via local execution when
            // the forward handler is itself the target (shouldExecuteOnSelf).
            int refreshes = refreshWithMutationId.get();
            assertTrue("Expected 1 refresh with mutation ID, got " + refreshes,
                       refreshes >= 1);

            assertFalse("Observed mutation IDs should not be empty", observedMutationIds.isEmpty());

            // Two deterministic possibilities for the CAS outcome:
            //  - CAS succeeded: IF NOT EXISTS saw v=42 → not applied
            //  - CAS threw: migration state disrupted commit phase
            // In either case v=99 must not be committed: the IF NOT EXISTS precondition fails because
            // the first CAS's v=42 is present on node 1's system.paxos.
            if (!casThrew)
                assertCasNotApplied(casResult);
        }
        finally
        {
            cluster.filters().reset();
            forwardSpy.close();
        }

        // Prepare-refresh commits v=42 to at least a quorum of the 3 replicas as part of the
        // prepare phase. Whether the subsequent CAS commit succeeds or times out, the refresh
        // durability guarantee still holds.
        int nodesWithData = 0;
        for (int i : new int[]{ 1, 2, 3 })
        {
            Object[][] nodeResult = cluster.get(i).executeInternal("SELECT v FROM " + ks + ".tbl WHERE k = " + KEY);
            if (nodeResult.length == 1 && Integer.valueOf(42).equals(nodeResult[0][0]))
                nodesWithData++;
        }
        assertTrue("Quorum (2 or 3) of replicas should have v=42 after prepare-refresh, but " + nodesWithData + " had it",
                   nodesWithData >= 2 && nodesWithData <= 3);
    }

    /**
     * FAILED_SENTINEL path when quorum is NOT achievable: the sole refresh target (node 2) fails
     * (message intercepted with immediate failure), causing a FAILED_SENTINEL response. The CAS
     * must fail (not hang) because quorum cannot be met (only node 1 has the latest commit;
     * node 3 never responded to prepare so is not targeted for refresh).
     *
     * Verifies sentinel response on the wire, CAS fails with appropriate error, no data on stale
     * nodes, no deadlock.
     */
    @Test
    public void testPrepareRefreshForwardHandlerTargetFailure() throws Throwable
    {
        String ks = newKeyspace("untracked");

        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).from(1).to(2, 3, 4).drop();

        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because commits to nodes 2,3,4 were blocked");
        }
        catch (Exception e)
        {
            // Expected — commit phase intentionally blocked.
        }

        cluster.filters().reset();

        alterReplicationType(cluster, ks, "tracked");
        ClusterUtils.awaitTCMCatchUp(cluster);

        // Intercept PAXOS2_PREPARE_REFRESH_REQ at nodes 2 and 3, immediately respond with failure
        // (simulating timeout), and drop the original. In practice only node 2 will be targeted
        // (node 3's prepare response is dropped below so it won't be in needLatest). This triggers
        // FAILED_SENTINEL instantly without waiting for the real write_request_timeout.
        // respondWithTimeout actively produces a response, so this filter stays manual.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_REFRESH_REQ.id)
               .to(2, 3)
               .messagesMatching((from, to, msg) -> {
                   cluster.get(to).runOnInstance(() -> respondWithTimeout(msg));
                   return true;
               }).drop();

        MessageSpy forwardSpy = on(cluster, Verb.PAXOS_PREPARE_REFRESH_FORWARD_REQ)
                                .inbound()
                                .start();

        // Drop prepare responses from node 3 so quorum can only be formed by node 1 + node 2.
        // Node 1 has the latest committed ballot, triggering refreshStaleParticipants.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_RSP.id)
               .from(3)
               .to(4)
               .drop();

        // Node 2's refresh receives an immediate failure (FAILED_SENTINEL), quorum unreachable, CAS must throw.
        boolean casThrew = false;
        try
        {
            try
            {
                cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 99) IF NOT EXISTS",
                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            }
            catch (Exception e)
            {
                casThrew = true;
            }

            assertTrue("CAS should have thrown due to quorum unreachable from FAILED_SENTINEL responses", casThrew);

            int forwards = forwardSpy.total();
            assertTrue("At least 1 forward should have been sent, got " + forwards, forwards >= 1);

            // The sentinel response on the wire proves FAILED_SENTINEL was generated by
            // PrepareRefreshForwardHandler and delivered to ForwardCallback. The CAS throwing proves
            // the path: PrepareRefreshForwardHandler received an immediate failure response (injected
            // via respondWithTimeout), sent Ballot.none() as supersededBy, ForwardCallback routed to
            // onRefreshFailure, PaxosPrepare.onFailure incremented failures until quorum became
            // unreachable.
        }
        finally
        {
            cluster.filters().reset();
            forwardSpy.close();
        }

        // Node 1 always has the data (original committer); neither node 2 nor node 3 did.
        assertReplicasHaveValue(cluster, ks, KEY, 42, 1);
        assertReplicaHasNoRow(cluster, ks, KEY, 2);
        assertReplicaHasNoRow(cluster, ks, KEY, 3);
    }

    /**
     * When node 3's prepare response is dropped, only node 2 becomes a refresh target (it
     * responded with an older commit). The single refresh to node 2 succeeds, achieving quorum
     * (node 1 + node 2 = 2). Verifies that quorum is reached with a single successful refresh
     * when one electorate member is unresponsive during prepare.
     */
    @Test
    public void testPrepareRefreshForwardHandlerPartialFailureAchievesQuorum() throws Throwable
    {
        String ks = newKeyspace("untracked");

        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).from(1).to(2, 3, 4).drop();

        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because commits to nodes 2,3,4 were blocked");
        }
        catch (Exception e)
        {
            // Expected — commit phase intentionally blocked.
        }

        cluster.filters().reset();

        alterReplicationType(cluster, ks, "tracked");
        ClusterUtils.awaitTCMCatchUp(cluster);

        // Defensive filter: intercept PAXOS2_PREPARE_REFRESH_REQ at node 3 in case it somehow
        // ends up in needLatest. In practice, node 3's prepare response is dropped below so it
        // will not be targeted for refresh. Node 2's refresh proceeds unblocked, giving quorum
        // (node 1 + node 2 = 2 >= quorum).
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_REFRESH_REQ.id)
               .to(3)
               .messagesMatching((from, to, msg) -> {
                   cluster.get(to).runOnInstance(() -> respondWithTimeout(msg));
                   return true;
               }).drop();

        MessageSpy forwardSpy = on(cluster, Verb.PAXOS_PREPARE_REFRESH_FORWARD_REQ)
                                .inbound()
                                .start();

        // Drop prepare responses from node 3 so quorum can only be formed by node 1 + node 2.
        // Node 1 has the latest committed ballot, triggering refreshStaleParticipants.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_RSP.id)
               .from(3)
               .to(4)
               .drop();

        // Only node 2 is targeted for refresh (node 3 never responded to prepare). The single
        // refresh succeeds: quorum = 2 with node 1 (has data) + node 2 (refresh success).
        try
        {
            Object[][] casResult = cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 99) IF NOT EXISTS",
                                                                  ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            // CAS succeeded -- IF NOT EXISTS saw v=42 already present.
            assertCasNotApplied(casResult);

            int forwards = forwardSpy.total();
            assertTrue("At least 1 forward should have been sent, got " + forwards, forwards >= 1);
        }
        finally
        {
            cluster.filters().reset();
            forwardSpy.close();
        }

        // Node 2 received the refresh; node 3 has no data (it was never targeted for refresh
        // because it did not respond to prepare).
        assertReplicasHaveValue(cluster, ks, KEY, 42, 2);
        assertReplicaHasNoRow(cluster, ks, KEY, 3);
    }

    // V2 Commit Forwarding Tests (Paxos2CommitForwardHandler)

    @Test
    public void testV2CommitForwardingFromNonReplica() throws Throwable
    {
        String ks = newKeyspace("tracked");

        MessageSpy forwardSpy = on(cluster, Verb.PAXOS2_COMMIT_FORWARD_REQ)
                                .inbound()
                                .expect(1)
                                .start();

        Object[][] result = cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

        assertCasApplied(result);
        forwardSpy.await();

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
        forwardSpy.close();
    }

    @Test
    public void testV2CommitForwardingFallbackToUntracked() throws Throwable
    {
        String ks = newKeyspace("tracked");

        // Hold PAXOS2_COMMIT_FORWARD_REQ so we can ALTER the keyspace to untracked mid-flight;
        // the forward handler should then commit on the untracked path (no mutation IDs on
        // PAXOS_COMMIT_REQ).
        MessageSpy hold = on(cluster, Verb.PAXOS2_COMMIT_FORWARD_REQ)
                          .inbound()
                          .holdAll()
                          .start();

        // The forward handler on the target replica commits locally and sends PAXOS_COMMIT_REQ to
        // the other 2 replicas. Quorum = 2, so the 2nd remote commit may still be in-flight when
        // the CAS returns — expect(2) waits for both.
        MessageSpy commitSpy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                               .inbound()
                               .to(1, 2, 3)
                               .checkMutationId()
                               .expect(2)
                               .start();

        CompletableFuture<Object[][]> casFuture = casAsync(cluster, 4,
                                                           "INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 99) IF NOT EXISTS");

        try
        {
            hold.awaitFirstArrival();
            alterReplicationType(cluster, ks, "untracked");
        }
        finally
        {
            hold.release();
        }

        Object[][] result = casFuture.get(60, TimeUnit.SECONDS);
        assertCasApplied(result);

        commitSpy.await();
        cluster.filters().reset();

        assertReplicasHaveValue(cluster, ks, KEY, 99, 1, 2, 3);

        // After fallback to untracked no commit should carry a mutation ID.
        assertEquals("No PAXOS_COMMIT_REQ should carry a mutation ID after fallback to untracked",
                     0, commitSpy.withMutationId());

        hold.close();
        commitSpy.close();
    }

    @Test
    public void testV2CommitForwardingRetryAfterCoordinatorBehind() throws Throwable
    {
        String ks = newKeyspace("tracked");

        // Pre-insert so the CAS condition (IF v = 1) is met on first try.
        cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1)",
                                       ConsistencyLevel.ALL);

        AtomicInteger forwardTarget = new AtomicInteger(-1);
        AssertingLatch commitArrived = new AssertingLatch("PAXOS_COMMIT_REQ from forward target");
        AssertingLatch alterDone = new AssertingLatch("ALTER to untracked to complete");
        AtomicInteger commitsAtReplicas = new AtomicInteger();
        AtomicInteger retryCommitsWithId = new AtomicInteger();

        // Learn which replica the snitch chose for the forward — any of {1,2,3} is possible.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_COMMIT_FORWARD_REQ.id)
               .messagesMatching((from, to, msg) -> {
                   forwardTarget.set(to);
                   return false;
               }).drop();

        // Hold PAXOS_COMMIT_REQ from the forward target until ALTER completes. Count ALL inbound
        // PAXOS_COMMIT_REQ at replicas to verify the retry fires, and check retry messages (from
        // node 4) for mutation ID absence. Conditional behaviour inside one filter — stays manual.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS_COMMIT_REQ.id)
               .to(1, 2, 3)
               .messagesMatching((from, to, msg) -> {
                   commitsAtReplicas.incrementAndGet();
                   if (from == forwardTarget.get())
                   {
                       commitArrived.countDown();
                       alterDone.await();
                   }
                   else if (from == 4)
                   {
                       boolean hasId = cluster.get(to).callsOnInstance(() -> messageHasMutationId(msg)).call();
                       if (hasId)
                           retryCommitsWithId.incrementAndGet();
                   }
                   return false;
               }).drop();

        CompletableFuture<Object[][]> casFuture = casAsync(cluster, 4,
                                                           "UPDATE " + ks + ".tbl SET v = 100 WHERE k = " + KEY + " IF v = 1");

        try
        {
            commitArrived.await();
            alterReplicationType(cluster, ks, "untracked");
        }
        finally
        {
            alterDone.release();
            commitArrived.release();
        }

        // With the fix: node 4's retry loop catches CoordinatorBehindException and retries with
        // fresh metadata (untracked), succeeding. Without the fix, WriteTimeoutException escapes
        // the loop and the test fails.
        try
        {
            Object[][] result = casFuture.get(30, TimeUnit.SECONDS);
            assertCasApplied(result);
        }
        catch (Exception e)
        {
            throw new AssertionError("CAS should succeed via COORDINATOR_BEHIND retry but got: " + e.getMessage(), e);
        }

        assertReplicasHaveValue(cluster, ks, KEY, 100, 1, 2, 3);

        // Retry verification: forward target sends to 2 other replicas (initial attempt), node 4
        // retries directly on the untracked path sending to all 3 replicas. Total = 5.
        assertTrue("Forward target should have been identified", forwardTarget.get() > 0);
        assertEquals("Expected initial (2) + retry (3) = 5 PAXOS_COMMIT_REQ at replicas",
                     5, commitsAtReplicas.get());

        // Retry messages from node 4 (untracked path) must not carry mutation IDs.
        assertEquals("Retry PAXOS_COMMIT_REQ from node 4 should NOT carry mutation IDs (untracked path)",
                     0, retryCommitsWithId.get());
    }
}
