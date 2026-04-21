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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.MessageSpy;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.alterReplicationType;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasApplied;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicasAreExactly;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicasHaveValue;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.buildPaxosCluster;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.casAsync;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.createKeyspace;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.messageHasMutationId;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.on;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.pauseHintsAndReconciler;
import static org.apache.cassandra.schema.ReplicationType.tracked;
import static org.apache.cassandra.schema.ReplicationType.untracked;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for V1 Paxos handler-level forwarding paths with CAS-level forwarding disabled.
 *
 * Tests the V1 commitPaxos forwarding path (StorageProxy.forwardPaxosCommit) which is invoked when
 * the coordinator is not a replica for a tracked keyspace.
 *
 * KEY=5 maps to replicas on nodes 1,2,3 (node 4 excluded) with Murmur3Partitioner RF=3.
 */
public class PaxosMutationTrackingForwardingV1Test extends TestBaseImpl
{
    private static Cluster cluster;
    private static final int KEY = 5;

    @BeforeClass
    public static void setup() throws Throwable
    {
        CassandraRelevantProperties.DISABLE_CONSENSUS_REQUEST_FORWARDING.setBoolean(true);
        cluster = init(buildPaxosCluster(4, "v1").start());
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
        String ks = createKeyspace(cluster, "pmt_fwd_v1", replicationType);
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });
        return ks;
    }

    /*
     * V1 commitPaxos forwarding from non-replica coordinator
     *
     * Covers: StorageProxy.forwardPaxosCommit() via commitPaxos()
     *   forwardPaxosCommit(reconciled, consistencyLevel, replicaPlan);
     *
     * With CAS-level forwarding disabled, a CAS from node 4 (non-replica for KEY=5)
     * executes locally through V1 doPaxos -> commitPaxos. Inside commitPaxos,
     * shouldBeTracked=true and requiresPaxosCommitForwarding returns
     * true (node 4 not in replicas) -> forwardPaxosCommit fires, forwarding the commit to a replica
     * via PaxosCommitForwardHandler.
     */
    @Test
    public void testV1CommitForwardingFromNonReplica()
    {
        String ks = newKeyspace("tracked");

        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_FORWARD_REQ)
                              .expect(1)
                              .start();
             MessageSpy commitSpy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                    .to(1, 2, 3)
                                    .checkMutationId()
                                    .expect(2)
                                    .start())
        {
            Object[][] result = cluster.coordinator(4)
                                       .execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                ConsistencyLevel.SERIAL,
                                                ConsistencyLevel.QUORUM);

            assertCasApplied(result);
            spy.await();
            commitSpy.await();
            assertEquals("PAXOS_COMMIT_FORWARD_REQ should have been sent exactly once",
                         1, spy.total());
            // The forward target re-coordinates on the tracked keyspace via commitPaxosTracked:
            // it commits locally and sends PAXOS_COMMIT_REQ to the two other replicas, each
            // carrying the freshly assigned mutation ID. Verify that on the wire, not just the
            // forward count.
            assertEquals("Forward target should send 2 sub-commits to the other replicas",
                         2, commitSpy.total());
            assertEquals("Every forwarded sub-commit on a tracked keyspace must carry a mutation ID",
                         2, commitSpy.withMutationId());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
    }

    /*
     * V1 commit forwarding with migration to untracked during forward
     *
     * Covers: PaxosCommitForwardHandler.doVerb() untracked fallback path
     *
     * CAS from node 4 (non-replica) forwards the commit to a replica via
     * PAXOS_COMMIT_FORWARD_REQ. The forward is delayed at the replica. During the delay,
     * the keyspace migrates to untracked. When the handler processes the forward, it finds
     * the keyspace is untracked and commits via the untracked path. No mutation ID needs to
     * be stripped because the proposal arrives with MutationId.none() — the ID is only
     * assigned inside commitPaxosTracked, which forwardPaxosCommit bypasses.
     */
    @Test
    public void testV1CommitForwardingFallbackToUntracked() throws Throwable
    {
        String ks = newKeyspace("tracked");

        // Delay PAXOS_COMMIT_FORWARD_REQ at the receiving replica until ALTER completes.
        // After migration to untracked, the forward handler commits via the untracked path.
        // The proposal never had a mutation ID (it is only assigned inside commitPaxosTracked),
        // so we verify no PAXOS_COMMIT_REQ carries one.
        try (MessageSpy hold = on(cluster, Verb.PAXOS_COMMIT_FORWARD_REQ)
                               .holdAll()
                               .start();
             MessageSpy commitSpy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                    .to(1, 2, 3)
                                    .checkMutationId()
                                    .start())
        {
            CompletableFuture<Object[][]> casResult = casAsync(cluster, 4,
                                                               "INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 99) IF NOT EXISTS");

            hold.awaitFirstArrival();
            alterReplicationType(cluster, ks, untracked);
            hold.release();

            Object[][] result = casResult.get(60, TimeUnit.SECONDS);
            assertCasApplied(result);

            assertReplicasHaveValue(cluster, ks, KEY, 99, 1, 2, 3);

            assertEquals("No PAXOS_COMMIT_REQ should carry a mutation ID after fallback to untracked",
                         0, commitSpy.withMutationId());
        }
    }

    /*
     * V1 commit forwarding with COORDINATOR_BEHIND retry after migration
     *
     * Covers: StorageProxy.java forwardPaxosCommit callback + commitPaxos retry loop
     *
     * The forward handler (node 1) still thinks tracked, calls commitPaxosTracked, sends
     * PAXOS_COMMIT_REQ to nodes 2,3 which have already migrated to untracked. They reject
     * with COORDINATOR_BEHIND. The forward handler catches the CoordinatorBehindException
     * and sends COORDINATOR_BEHIND back to the original coordinator (node 4). The coordinator's
     * retry loop should catch this and retry with fresh metadata (untracked path).
     *
     * Without the fix: the callback wraps COORDINATOR_BEHIND as WriteTimeoutException, which
     * propagates out of the retry loop and the CAS fails with CasWriteTimeout.
     */
    @Test
    public void testV1CommitForwardingRetryAfterCoordinatorBehind()
    {
        String ks = newKeyspace("tracked");

        @SuppressWarnings("Convert2MethodRef")
        long retryBefore = cluster.get(4).callOnInstance(() -> ClientRequestsMetricsHolder.casWriteMetrics.retryCoordinatorBehind.getCount());

        // Pre-insert so the CAS condition is met on first try
        cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1)",
                                       ConsistencyLevel.ALL);

        // Strategy: Hold PAXOS_COMMIT_REQ at nodes 2 and 3 (the non-forwarding replicas).
        // While held, ALTER the keyspace to untracked. When released, nodes 2,3 see the
        // commit request from the stale forward handler (node 1) and reject with
        // COORDINATOR_BEHIND. This bubbles back to node 4's retry loop.
        AssertingLatch commitArrived = new AssertingLatch("PAXOS_COMMIT_REQ at nodes 2,3");
        AssertingLatch alterDone = new AssertingLatch("V1 commit forwarding retry - alter to untracked");

        // Count ALL inbound PAXOS_COMMIT_REQ at nodes 2,3 regardless of source. After the retry,
        // node 4 (now on untracked) calls commitPaxosUntracked directly and sends to replicas
        // {1,2,3}, yielding 2 more messages at {2,3}. Total = initial 2 + retry 2 = 4.
        // If the retry didn't happen (regression), total would be 2 and the CAS would fail.
        AtomicInteger commitsAtReplicas = new AtomicInteger();

        // Hold PAXOS_COMMIT_REQ at nodes 2 and 3 until ALTER completes.
        // Also check retry messages (from node 4) for mutation ID absence. One filter handles
        // both roles because the from-node distinguishes initial (node 1) from retry (node 4)
        // and two overlapping MessageSpy instances on the same verb+destination would not
        // compose cleanly — the cluster filter system applies ALL matching filters.
        AtomicInteger retryCommitsWithId = new AtomicInteger();
        cluster.filters()
               .verbs(Verb.PAXOS_COMMIT_REQ.id)
               .to(2, 3)
               .messagesMatching((from, to, msg) -> {
                   commitsAtReplicas.incrementAndGet();
                   // Only hold commits from node 1 (the forward handler acting as coordinator)
                   if (from == 1)
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

        // Start the CAS from node 4 (non-replica) async
        CompletableFuture<Object[][]> casResult = casAsync(cluster, 4,
                                                           "UPDATE " + ks + ".tbl SET v = 100 WHERE k = " + KEY + " IF v = 1");

        // Wait for the PAXOS_COMMIT_REQ to arrive (proves forward path was taken), then ALTER
        // to untracked while the commit is held. All nodes see this immediately. Release the
        // held commits in finally so the held filter thread never strands if a step throws.
        try
        {
            commitArrived.await();

            alterReplicationType(cluster, ks, untracked);
        }
        finally
        {
            // Release the held commits — nodes 2,3 now see migration mismatch -> COORDINATOR_BEHIND
            alterDone.countDown();
        }

        // With the fix: node 4's retry loop catches CoordinatorBehindException, retries
        // with fresh metadata (untracked), succeeds.
        // Without the fix: WriteTimeoutException escapes the loop -> test fails.
        try
        {
            Object[][] result = casResult.get(30, TimeUnit.SECONDS);
            assertCasApplied(result);
        }
        catch (Exception e)
        {
            throw new AssertionError("CAS should succeed via COORDINATOR_BEHIND retry but got: " + e.getMessage(), e);
        }

        // Verify the write took effect on each replica individually. The retry path uses
        // commitPaxosUntracked at QUORUM, so all 3 replicas (1,2,3) should have the data
        // (node 1 gets it via the tracked local write in the initial forward, nodes 2,3 via retry).
        assertReplicasHaveValue(cluster, ks, KEY, 100, 1, 2, 3);

        // Authenticate that the retry actually happened by counting inbound PAXOS_COMMIT_REQ at
        // the two non-forwarding replicas. Initial attempt (from node 1, held then released) = 2,
        // retry (from node 4 directly on untracked path) = 2. Total = 4. A result of 2 would mean
        // the CAS succeeded without retry (bug).
        assertEquals("Expected initial (2) + retry (2) = 4 PAXOS_COMMIT_REQ at nodes 2,3",
                     4, commitsAtReplicas.get());

        // Retry messages from node 4 (untracked path) must not carry mutation IDs
        assertEquals("Retry PAXOS_COMMIT_REQ from node 4 should NOT carry mutation IDs (untracked path)",
                     0, retryCommitsWithId.get());

        // Verify the retryCoordinatorBehind metric was incremented on the coordinator (node 4).
        // StorageProxy.commitPaxos marks this when CoordinatorBehindException is caught and retried.
        @SuppressWarnings("Convert2MethodRef")
        long retryAfter = cluster.get(4).callOnInstance(() -> ClientRequestsMetricsHolder.casWriteMetrics.retryCoordinatorBehind.getCount());
        assertTrue("casWriteMetrics.retryCoordinatorBehind should have been incremented on coordinator node 4",
                   retryAfter > retryBefore);
    }

    /*
     * V1 commit forwarding reached via an untracked -> tracked migration.
     * The keyspace is created untracked -- where a non-replica coordinator commits directly -- then migrated to
     * tracked. Once writes are tracked, a non-replica (node 4) must forward the commit to a replica to obtain a
     * mutation id (inverse of testV1CommitForwardingFallbackToUntracked).
     */
    @Test
    public void testV1CommitForwardingDuringMigrationToTracked()
    {
        String ks = newKeyspace("untracked");

        alterReplicationType(cluster, ks, tracked);
        ClusterUtils.awaitTCMCatchUp(cluster);

        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_FORWARD_REQ)
                              .expect(1)
                              .start();
             MessageSpy commitSpy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                    .to(1, 2, 3)
                                    .checkMutationId()
                                    .expect(2)
                                    .start())
        {
            Object[][] result = cluster.coordinator(4)
                                       .execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                ConsistencyLevel.SERIAL,
                                                ConsistencyLevel.QUORUM);

            assertCasApplied(result);
            spy.await();
            commitSpy.await();
            assertEquals("Commit must be forwarded once after migration to tracked",
                         1, spy.total());
            assertEquals("Forward target should send 2 sub-commits to the other replicas",
                         2, commitSpy.total());
            assertEquals("Every forwarded sub-commit on the migrated-to-tracked keyspace must carry a mutation ID",
                         2, commitSpy.withMutationId());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
    }
}
