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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.EpochPin;
import org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.MessageSpy;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.alterReplicationType;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.alterReplicationTypeFrom;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertAllNodesSee;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasApplied;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasException;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasNotApplied;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicaHasNoRow;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicasHaveValue;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.awaitReplicationType;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.buildPaxosCluster;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.casAsync;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.createKeyspace;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.epochPin;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.on;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.pauseHintsAndReconciler;
import static org.apache.cassandra.schema.ReplicationType.tracked;
import static org.apache.cassandra.schema.ReplicationType.untracked;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that Paxos V1 CAS operations work correctly during mutation tracking migration.
 *
 * Each test documents the exact message verb being verified and asserts that the
 * expected code path was taken (not just that the CAS "succeeds").
 *
 * Uses a shared 3-node cluster with paxos_variant=v1.
 */
public class PaxosMutationTrackingMigrationV1Test extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws Throwable
    {
        cluster = init(buildPaxosCluster(3, "v1").start());
        pauseHintsAndReconciler(cluster);
    }

    @AfterClass
    public static void teardown()
    {
        if (cluster != null)
            cluster.close();
    }

    @After
    public void resetFilters()
    {
        cluster.filters().reset();
        ClusterUtils.awaitTCMCatchUp(cluster);
    }

    /*
     * Message: PAXOS_COMMIT_REQ
     * Path: V1 -> StorageProxy.commitPaxosTracked
     * Verifies: MigrationRouter.shouldUseTrackedForWrites() returns true during migration
     */
    @Test
    public void testCasDuringMigrationToTracked()
    {
        String ks = createKeyspace(cluster, "pmt_v1", "untracked");

        cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (1, 0)",
                                       ConsistencyLevel.QUORUM);

        alterReplicationType(cluster, ks, tracked);

        // Precondition: migration is active and schema reads tracked on every node.
        for (int i = 1; i <= cluster.size(); i++)
        {
            final String keyspace = ks;
            int nodeId = i;
            cluster.get(i).runOnInstance(() -> {
                ClusterMetadata cm = ClusterMetadata.current();
                MutationTrackingMigrationState state = cm.mutationTrackingMigrationState;
                assertTrue("Node " + nodeId + ": keyspace should be migrating",
                           state.isMigrating(keyspace));
                assertTrue("Node " + nodeId + ": schema should show tracked",
                           cm.schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked());
            });
        }

        // Count PAXOS_COMMIT_REQ messages that carry a mutation ID. Counting raw messages is not
        // enough -- 2 untracked commits on RF=3 would also produce count==2. The tracked path
        // differs from the untracked path by carrying a non-none mutation ID on the Commit payload.
        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                              .checkMutationId()
                              .expect(2)
                              .start())
        {
            Object[][] result = cluster.coordinator(1).execute("UPDATE " + ks + ".tbl SET v = 1 WHERE k = 1 IF v = 0",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            spy.await();
            assertCasApplied(result);
            assertEquals("Tracked CAS should send exactly 2 PAXOS_COMMIT_REQ carrying a mutation ID (to 2 remote replicas)",
                         2, spy.withMutationId());
        }

        for (int i = 1; i <= cluster.size(); i++)
        {
            int nodeId = i;
            cluster.get(i).runOnInstance(() ->
                assertTrue("Node " + nodeId + ": MutationTrackingService should be enabled",
                           MutationTrackingService.isEnabled()));
        }

        assertReplicasHaveValue(cluster, ks, 1, 1, 1, 2, 3);
    }

    /*
     * Scenario: first CAS commits locally but remote commits are dropped. Migrate to untracked.
     * Second CAS discovers the stale ballot and recommits with the ID stripped.
     *
     * The coordinator strips the ID BEFORE sending, so the handler never sees a mismatch.
     * Zero mutation IDs on outbound commits proves stripping worked (no COORDINATOR_BEHIND rejection).
     *
     * V1 message: PAXOS_COMMIT_REQ via StorageProxy.sendCommit()
     */
    @Test
    public void testStaleIdStrippedOnRecommit()
    {
        String ks = createKeyspace(cluster, "pmt_v1", "tracked");

        // Drop remote commits to leave an uncommitted ballot with a mutation ID on node 1.
        // The tracked commit path writes locally synchronously, so node 1 has the commit.
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

        boolean threw = false;
        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (1, 1) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
        }
        catch (Exception e)
        {
            threw = true;
            assertCasException(e);
        }
        assertTrue("First CAS should have thrown (commits were blocked)", threw);

        cluster.filters().reset();

        // Migrate to untracked (instant). The stale ballot in system.paxos has a mutation ID.
        alterReplicationType(cluster, ks, untracked);
        assertAllNodesSee(cluster, ks, untracked);

        // Spy on PAXOS_COMMIT_REQ messages to remote replicas. The V1 repair path's
        // beginAndRepairPaxos loop may run 1+ iterations depending on timing (when a replica
        // acknowledges the previous repair before the next prepare). Counting total messages
        // is non-deterministic, but the INVARIANT is: every message after migration-to-untracked
        // must have mutation_id stripped (.id().isNone() == true). If stripping failed, the
        // handler would reject with CoordinatorBehindException -> CAS times out.
        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                              .to(2, 3)
                              .checkMutationId()
                              .start())
        {
            // Second CAS: INSERT IF NOT EXISTS. The first CAS's local commit left v=1 on node 1.
            // After beginAndRepairPaxos repairs, all replicas have v=1. The condition IF NOT EXISTS
            // therefore evaluates FALSE -> CAS NOT applied, result[0][0] == false.
            // If stripping failed, CAS would time out (no exception/result here).
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (1, 1) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            assertCasNotApplied(result);

            assertReplicasHaveValue(cluster, ks, 1, 1, 1, 2, 3);

            // Core invariant: every repair commit must have its mutation_id stripped after migration
            // to untracked. Otherwise handlers reject with CoordinatorBehindException and the CAS
            // would time out (it didn't -- see CAS success above).
            assertEquals("No PAXOS_COMMIT_REQ should carry a mutation ID after migration to untracked",
                         0, spy.withMutationId());
        }
    }

    /*
     * Scenario: tracked -> untracked migration completes, then a fresh CAS runs.
     * No stale ballots, no epoch mismatches. The commit goes through the untracked path
     * and all replicas accept on the first try.
     *
     * Message: PAXOS_COMMIT_REQ
     * V1 path: StorageProxy.commitPaxosUntracked()
     *
     * Exact inbound count == 2 proves no COORDINATOR_BEHIND retry.
     */
    @Test
    public void testCommitAfterMigrationToUntracked()
    {
        String ks = createKeyspace(cluster, "pmt_v1", "tracked");

        alterReplicationType(cluster, ks, untracked);
        assertAllNodesSee(cluster, ks, untracked);

        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                              .to(2, 3)
                              .checkMutationId()
                              .expect(2)
                              .start())
        {
            // Fresh CAS on untracked keyspace -- clean commit, no stale data, no race
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (1, 42) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            spy.await();

            assertCasApplied(result);

            // Exactly 2 PAXOS_COMMIT_REQ (one per remote replica, no retry)
            assertEquals("PAXOS_COMMIT_REQ should be sent to exactly 2 remote replicas (no retry)",
                         2, spy.total());

            assertEquals("PAXOS_COMMIT_REQ should NOT carry mutation IDs on untracked keyspace",
                         0, spy.withMutationId());
        }

        assertReplicasHaveValue(cluster, ks, 1, 42, 1, 2, 3);
    }

    /*
     * Scenario: CAS on tracked keyspace. Inbound filter delays PAXOS_COMMIT_REQ delivery
     * at nodes 2, 3 while we ALTER to untracked. When released, the handlers see the stale
     * epoch and reject with COORDINATOR_BEHIND. The coordinator retries with fresh routing.
     *
     * Message: PAXOS_COMMIT_REQ
     * V1 retry: StorageProxy.commitPaxos() catches CoordinatorBehindException (thrown by
     *           AbstractWriteResponseHandler.get()), re-evaluates routing, resends.
     *
     * Inbound count == 4 proves the retry occurred (initial batch 2 + retry batch 2).
     * Of those 4, only the initial 2 should carry mutation IDs (tracked era); the retry
     * batch (untracked era) should have IDs stripped.
     */
    @Test
    public void testCommitCoordinatorBehindRetry() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v1", "tracked");

        // holdFirst(2) blocks the initial batch at the destination until release() is called;
        // subsequent messages (the retry batch) pass through immediately. The initial batch
        // arrives with old epoch (tracked), but by the time the handler processes it, the
        // node is at new epoch (untracked) -> COORDINATOR_BEHIND -> coordinator retries.
        try (MessageSpy hold = on(cluster, Verb.PAXOS_COMMIT_REQ)
                               .from(1)
                               .to(2, 3)
                               .holdFirst(2)
                               .checkMutationId()
                               .expect(4)
                               .start())
        {
            CompletableFuture<Object[][]> casResult = casAsync(cluster, 1,
                                                               "INSERT INTO " + ks + ".tbl (k, v) VALUES (1, 42) IF NOT EXISTS");

            hold.awaitFirstArrival();

            try
            {
                // ALTER to untracked while commits are delayed at the destination
                alterReplicationType(cluster, ks, untracked);
                assertAllNodesSee(cluster, ks, untracked);
            }
            finally
            {
                hold.release();
            }

            Object[][] result = casResult.get(60, SECONDS);
            hold.await();

            assertCasApplied(result);

            // Initial batch: 2 inbound (to nodes 2, 3) -> both COORDINATOR_BEHIND -> retry.
            // Retry batch: 2 more inbound (to nodes 2, 3 again).
            // Total = 4 proves the COORDINATOR_BEHIND retry occurred.
            assertEquals("Expected initial (2) + retry (2) = 4 PAXOS_COMMIT_REQ",
                         4, hold.total());

            // Only the initial batch (tracked era) carries mutation IDs. The retry batch
            // (untracked era) must have IDs stripped. Total-with-id == 2 proves this:
            // if retry messages also had IDs, the count would be 4.
            assertEquals("Only the initial stale batch should carry mutation IDs (retry batch should not)",
                         2, hold.withMutationId());
        }

        assertReplicasHaveValue(cluster, ks, 1, 42, 1, 2, 3);
        assertAllNodesSee(cluster, ks, untracked);
    }

    /*
     * Covers: StorageProxy.commitPaxos() -- WriteTimeoutException thrown when the V1
     * commitPaxosTracked response handler times out because maybeFetchLogs blocks before
     * delivering the COORDINATOR_BEHIND failure callback.
     *
     * Node 2 as coordinator is blocked from receiving TCM updates, so it stays at the
     * old epoch (tracked). After ALTER to untracked, nodes 1,3 are at the new epoch.
     * Every commit from node 2 triggers COORDINATOR_BEHIND on nodes 1,3. Node 2 can't
     * catch up, so the first commit attempt's inner timeout fires, failing the CAS without retrying.
     */
    @Test
    public void testCommitRetryLoopTimeout()
    {
        String ks = createKeyspace(cluster, "pmt_v1", "tracked");

        // EpochPin strands node 2 at its current (tracked) epoch by holding TCM traffic in/out.
        // "Hold" rather than "drop": messages block on a latch with a safety-net timeout so they
        // eventually flow after the test, preventing an indefinite fetchLogFromPeerOrCMS stall
        // on node 2 that would bleed into later tests.
        try (EpochPin ignored = epochPin(cluster, 2))
        {
            // ALTER to untracked via node 1 at CL.ONE so we don't wait for schema agreement
            // on TCM-blocked node 2.
            alterReplicationTypeFrom(cluster, 1, ks, untracked, ConsistencyLevel.ONE);

            // Wait for nodes 1,3 to see untracked. Node 2 should still see tracked.
            awaitReplicationType(cluster, ks, untracked, 1, 3);
            final String keyspace = ks;
            assertTrue("Node 2 should still see tracked (TCM blocked)",
                       cluster.get(2).callOnInstance(() ->
                           ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace)
                                .params.replicationType.isTracked()));

            // Spy on PAXOS_COMMIT_REQ from node 2 to verify commit attempts were made.
            try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                  .from(2)
                                  .checkMutationId()
                                  .start())
            {
                // CAS from node 2. Node 2 thinks tracked -> commitPaxosTracked sends PAXOS_COMMIT_REQ
                // to nodes 1,3. They detect epoch mismatch -> COORDINATOR_BEHIND. CBE propagates back;
                // maybeFetchLogs on node 2 tries to fetch but the fetches are held by the EpochPin.
                // The CAS ultimately fails via the inner response-handler timeout at
                // write_request_timeout, before the held fetch is released.
                boolean threw = false;
                try
                {
                    cluster.coordinator(2).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (1, 42) IF NOT EXISTS",
                                                   ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
                }
                catch (Exception e)
                {
                    threw = true;
                    assertTrue("Expected CAS timeout but got: " + e.getMessage(),
                               e.getMessage().contains("CAS operation timed out"));
                }

                // Exactly one iteration fires: inner timeout expires before the held TCM fetch
                // releases and allows cb.onFailure to let commitPaxos catch the CBE and retry.
                assertEquals("Exactly 1 iteration runs (held fetch outlasts write_request_timeout); 2 commits = 1 iteration x 2 replicas",
                             2, spy.total());

                // All commits from stranded node 2 carry mutation IDs (tracked path),
                // proving COORDINATOR_BEHIND is the rejection reason (untracked handlers reject tracked commits)
                assertEquals("All commit attempts from stranded coordinator should carry mutation IDs (tracked path)",
                             spy.total(), spy.withMutationId());

                assertTrue("CAS should have failed", threw);

                // Node 2 is the coordinator AND a replica; its local commit (executeOnSelf) fires
                // before the quorum wait, so v=42 is locally durable. Nodes 1,3 rejected with
                // COORDINATOR_BEHIND — they never applied the commit.
                assertReplicasHaveValue(cluster, ks, 1, 42, 2);
                assertReplicaHasNoRow(cluster, ks, 1, 1);
                assertReplicaHasNoRow(cluster, ks, 1, 3);
            }
        }
    }
}
