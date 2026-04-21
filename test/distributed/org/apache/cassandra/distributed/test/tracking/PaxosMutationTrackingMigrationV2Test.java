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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.EpochPin;
import org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.MessageSpy;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.alterReplicationType;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.alterReplicationTypeFrom;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertAllNodesSee;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasApplied;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasException;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasNotApplied;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertNodeSees;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicaHasNoRow;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicasAreExactly;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertReplicasHaveValue;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.awaitReplicationType;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.buildPaxosCluster;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.casAsync;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.casAsyncExpectingFailure;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.createKeyspace;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.epochPin;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.on;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.pauseHintsAndReconciler;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that Paxos V2 CAS operations work correctly during mutation tracking migration.
 *
 * Each test documents the exact message verb being verified and asserts that the
 * expected code path was taken (not just that the CAS "succeeds").
 *
 * Uses a shared 4-node cluster with paxos_variant=v2 and RF=3. With RF=3 on 4 nodes,
 * some keys have node 1 as a replica (2 remote replicas) and some do not (3 remote
 * replicas). All tests handle both cases.
 */
public class PaxosMutationTrackingMigrationV2Test extends TestBaseImpl
{
    private static Cluster cluster;

    /*
     * With Murmur3Partitioner, 4 nodes, and SimpleStrategy RF=3:
     * Key 5 → replicas on nodes 1, 2, 3 (node 4 excluded)
     * This key is used by most tests because node 1 is a replica (avoids CAS forwarding)
     * and node 4 is not a replica (useful for testCommitAndPrepareViaIncompleteAccepted).
     */
    private static final int KEY = 5;

    @BeforeClass
    public static void setup() throws Throwable
    {
        cluster = init(buildPaxosCluster(4, "v2").start());
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
        cluster.forEach(instance -> instance.runOnInstance(() -> HintsService.setRejectHintsBeforeNanos(0)));
        ClusterUtils.awaitTCMCatchUp(cluster);
    }

    /*
     * CAS during active to-tracked migration
     *
     * Message: PAXOS_COMMIT_REQ
     * Path: V2 -> PaxosCommit.start() (tracked path)
     * Verifies: MigrationRouter.shouldUseTrackedForWrites() returns true during migration
     */
    @Test
    public void testCasDuringMigrationToTracked() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "untracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 0)",
                                       ConsistencyLevel.QUORUM);

        alterReplicationType(cluster, ks, "tracked");

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
        // enough — 2 untracked commits on RF=3 would also produce count==2. The tracked path
        // differs from the untracked path by carrying a non-none mutation ID on the Commit payload.
        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                              .checkMutationId()
                              .expect(2)
                              .start())
        {
            Object[][] result = cluster.coordinator(1).execute("UPDATE " + ks + ".tbl SET v = 1 WHERE k = " + KEY + " IF v = 0",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            spy.await();
            assertCasApplied(result);
            assertEquals("Tracked CAS should send exactly 2 PAXOS_COMMIT_REQ carrying a mutation ID (to 2 remote replicas)",
                         2, spy.withMutationId());
        }

        for (int i = 1; i <= cluster.size(); i++)
        {
            int nodeId = i;
            cluster.get(i).runOnInstance(() -> {
                assertTrue("Node " + nodeId + ": MutationTrackingService should be enabled",
                           MutationTrackingService.isEnabled());
            });
        }

        assertReplicasHaveValue(cluster, ks, KEY, 1, 1, 2, 3);
    }

    /*
     * In V2 Paxos, the read is embedded in the PAXOS2_PREPARE_REQ payload. Tracked reads use
     * TrackedRead.DataRequest/SummaryRequest; untracked uses SinglePartitionReadCommand.
     * The handler validates via checkPaxosPrepareReadMigration(). Since both types use the
     * same verb (PAXOS2_PREPARE_REQ), we verify routing by checking MigrationRouter.shouldUseTracked()
     * on the coordinator, and confirm handler acceptance by checking no COORDINATOR_BEHIND occurred
     * (proven by exact PAXOS_COMMIT_REQ counts -- retries would inflate the count).
     *
     * 2a: During to-tracked migration, reads should be UNTRACKED (safe default).
     * 2b: After migration to untracked, reads should be UNTRACKED.
     * 2c: On a fully-tracked keyspace (no migration), reads should be TRACKED.
     */

    /**
     * 2a: During to-tracked migration, the Paxos prepare read uses untracked routing.
     *
     * MigrationRouter.shouldUseTracked() returns false during migration because tracked
     * reads require ALL writes to be tracked for monotonicity.
     */
    @Test
    public void testPrepareReadDuringMigrationIsUntracked() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "untracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42)",
                                       ConsistencyLevel.QUORUM);

        alterReplicationType(cluster, ks, "tracked");

        // Verify migration is active and the prepare read routing is UNTRACKED
        for (int i = 1; i <= cluster.size(); i++)
        {
            final String keyspace = ks;
            int nodeId = i;
            cluster.get(i).runOnInstance(() -> {
                assertTrue("Node " + nodeId + ": migration should be active",
                           ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(keyspace));

                TableMetadata tbl = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).getTableOrViewNullable("tbl");
                SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(tbl, FBUtilities.nowInSeconds(), tbl.partitioner.decorateKey(tbl.partitionKeyType.fromString(String.valueOf(KEY))));
                assertFalse("Node " + nodeId + ": prepare read should be UNTRACKED during migration",
                            MigrationRouter.shouldUseTracked(cmd));
            });
        }

        // CAS works with untracked read (correct routing during migration)
        Object[][] result = cluster.coordinator(1).execute("SELECT * FROM " + ks + ".tbl WHERE k = " + KEY,
                                                           ConsistencyLevel.SERIAL);

        assertNotNull("CAS read should return result", result);
        assertEquals("Should have one row", 1, result.length);
        assertEquals("Value should be 42", 42, result[0][1]);
    }

    /**
     * 2b: After migration to untracked, the Paxos prepare read uses untracked routing.
     */
    @Test
    public void testPrepareReadAfterMigrationToUntrackedIsUntracked() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        alterReplicationType(cluster, ks, "untracked");

        // Verify all nodes see untracked and the prepare read routing is UNTRACKED
        for (int i = 1; i <= cluster.size(); i++)
        {
            final String keyspace = ks;
            int nodeId = i;
            cluster.get(i).runOnInstance(() -> {
                assertFalse("Node " + nodeId + ": should be untracked",
                            ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked());

                TableMetadata tbl = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).getTableOrViewNullable("tbl");
                SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(tbl, FBUtilities.nowInSeconds(), tbl.partitioner.decorateKey(tbl.partitionKeyType.fromString(String.valueOf(KEY))));
                assertFalse("Node " + nodeId + ": prepare read should be UNTRACKED after migration",
                            MigrationRouter.shouldUseTracked(cmd));
            });
        }

        // Spy on PAXOS_COMMIT_REQ to verify no COORDINATOR_BEHIND retry.
        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                              .to(2, 3, 4)
                              .expect(2)
                              .start())
        {
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            spy.await();
            assertCasApplied(result);
            assertEquals("PAXOS_COMMIT_REQ should match remote replica count (no retry)",
                         2, spy.total());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
    }

    /**
     * 2c: On a fully-tracked keyspace (created as tracked, no migration), the Paxos
     * prepare read uses tracked routing.
     */
    @Test
    public void testPrepareReadOnTrackedKeyspaceIsTracked() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Verify the prepare read routing is TRACKED on all nodes
        for (int i = 1; i <= cluster.size(); i++)
        {
            final String keyspace = ks;
            int nodeId = i;
            cluster.get(i).runOnInstance(() -> {
                assertTrue("Node " + nodeId + ": should be tracked",
                           ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).params.replicationType.isTracked());

                TableMetadata tbl = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).getTableOrViewNullable("tbl");
                SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(tbl, FBUtilities.nowInSeconds(), tbl.partitioner.decorateKey(tbl.partitionKeyType.fromString(String.valueOf(KEY))));
                assertTrue("Node " + nodeId + ": prepare read should be TRACKED on tracked keyspace",
                           MigrationRouter.shouldUseTracked(cmd));
            });
        }

        // Spy on PAXOS_COMMIT_REQ to verify no COORDINATOR_BEHIND retry from read mismatch.
        try (MessageSpy spy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                              .to(2, 3, 4)
                              .expect(2)
                              .start())
        {
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            spy.await();
            assertCasApplied(result);
            assertEquals("PAXOS_COMMIT_REQ should match remote replica count (no retry)",
                         2, spy.total());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
    }

    /*
     * Scenario: first CAS commits locally but remote commits are dropped. Migrate to untracked.
     * Second CAS discovers the stale ballot and recommits with the ID stripped.
     *
     * The coordinator strips the ID BEFORE sending, so the handler never sees a mismatch.
     * Exact inbound message count proves no COORDINATOR_BEHIND retry occurred.
     *
     * V2 message: PAXOS2_PREPARE_REFRESH_REQ via PaxosPrepareRefresh.refresh()
     */
    @Test
    public void testStaleIdStrippedOnRecommit() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Drop remote commits to leave an uncommitted ballot with a mutation ID on node 1.
        // The tracked commit path writes locally synchronously, so node 1 has the commit.
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because remote commits were dropped");
        }
        catch (Exception e)
        {
            assertCasException(e);
        }

        cluster.filters().reset();

        // Migrate to untracked (instant). The stale ballot in system.paxos has a mutation ID.
        alterReplicationType(cluster, ks, "untracked");
        assertNodeSees(cluster, 1, ks, ReplicationType.untracked);

        // Spy on the recommit verb AND on PAXOS_COMMIT_REQ to detect any retry overhead.
        // V2 recommit: PaxosPrepareRefresh -> PAXOS2_PREPARE_REFRESH_REQ to remote replicas.
        try (MessageSpy recommitSpy = on(cluster, Verb.PAXOS2_PREPARE_REFRESH_REQ)
                                      .to(2, 3, 4)
                                      .checkMutationId()
                                      .expect(2)
                                      .start();
             // Also count PAXOS_COMMIT_REQ separately to verify no unexpected commits.
             // (The second CAS's condition is not met so no new commit should be sent.)
             MessageSpy commitSpy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                    .to(2, 3, 4)
                                    .start())
        {
            // Second CAS discovers the stale ballot and recommits with the ID stripped.
            // If the ID is NOT stripped, the handler rejects (tracked mutation in untracked
            // keyspace) and the CAS fails.
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            recommitSpy.await();

            assertCasNotApplied(result);

            // Assert: the recommit verb was sent to the remote replicas (2).
            assertEquals("PAXOS2_PREPARE_REFRESH_REQ should match remote replica count",
                         2, recommitSpy.total());

            // After migration to untracked, refresh messages must NOT carry mutation IDs
            assertEquals("PAXOS2_PREPARE_REFRESH_REQ should NOT carry mutation IDs after migration to untracked",
                         0, recommitSpy.withMutationId());

            // No PAXOS_COMMIT_REQ should be sent (condition not met -> no new commit)
            assertEquals("No PAXOS_COMMIT_REQ expected for V2 (condition not met)", 0, commitSpy.total());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 1, 1, 2, 3);
    }

    /*
     * Scenario: tracked -> untracked migration completes, then a fresh CAS runs.
     * No stale ballots, no epoch mismatches. The commit goes through the untracked path
     * and all replicas accept on the first try.
     *
     * Message: PAXOS_COMMIT_REQ
     * V2 path: PaxosCommit.start() (untracked, no mutation ID)
     *
     * Inbound count == 2 proves no COORDINATOR_BEHIND retry (one batch to 2 remote replicas).
     */
    @Test
    public void testCommitAfterMigrationToUntracked() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        alterReplicationType(cluster, ks, "untracked");
        assertAllNodesSee(cluster, ks, ReplicationType.untracked);

        // Count inbound PAXOS_COMMIT_REQ at remote nodes.
        // Also count PAXOS2_PREPARE_REFRESH_REQ to verify no stale ballot refresh.
        try (MessageSpy commitSpy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                    .to(2, 3, 4)
                                    .expect(2)
                                    .start();
             MessageSpy refreshSpy = on(cluster, Verb.PAXOS2_PREPARE_REFRESH_REQ)
                                     .to(2, 3, 4)
                                     .start())
        {
            // Fresh CAS on untracked keyspace -- clean commit, no stale data, no race
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            commitSpy.await();

            assertCasApplied(result);

            // Assert: PAXOS_COMMIT_REQ sent to remote replicas (2), no retry
            assertEquals("PAXOS_COMMIT_REQ should match remote replica count (no retry)",
                         2, commitSpy.total());

            // Assert: no PAXOS2_PREPARE_REFRESH_REQ (no stale ballot to refresh)
            assertEquals("No PAXOS2_PREPARE_REFRESH_REQ expected (no stale data)",
                         0, refreshSpy.total());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
    }

    /*
     * Scenario: CAS on tracked keyspace. Inbound filter delays PAXOS_COMMIT_REQ delivery
     * at nodes 2, 3, 4 while we ALTER to untracked. When released, the handlers see the stale
     * epoch and reject with COORDINATOR_BEHIND. The coordinator retries with fresh routing.
     *
     * Message: PAXOS_COMMIT_REQ
     * V2 retry: Paxos.cas() checks failure.failures map for COORDINATOR_BEHIND,
     *           creates new PaxosCommit which re-evaluates MigrationRouter, resends.
     *
     * Inbound count == 4 proves the retry occurred (initial batch 2 + retry batch 2).
     */
    @Test
    public void testCommitCoordinatorBehindRetry() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Inbound filter at nodes 2, 3, 4: hold ALL PAXOS_COMMIT_REQ arrivals
        // until after the schema change. The first batch arrives with old epoch (tracked),
        // but by the time the handler processes it, the node is at new epoch (untracked).
        // checkPaxosCommitMigration detects the mismatch -> COORDINATOR_BEHIND.
        try (MessageSpy hold = on(cluster, Verb.PAXOS_COMMIT_REQ)
                               .from(1)
                               .to(2, 3, 4)
                               .holdAll()
                               .checkMutationId()
                               .expect(4)
                               .start())
        {
            CompletableFuture<Object[][]> casResult = casAsync(cluster, 1,
                                                               "INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS");

            hold.awaitFirstArrival();

            try
            {
                // ALTER to untracked while commits are delayed at the destination
                alterReplicationType(cluster, ks, "untracked");
                assertAllNodesSee(cluster, ks, ReplicationType.untracked);
            }
            finally
            {
                hold.release();
            }

            Object[][] result = casResult.get(60, TimeUnit.SECONDS);
            hold.await();

            assertCasApplied(result);

            // Initial batch: 2 inbound (to remote replicas) -> COORDINATOR_BEHIND -> retry.
            // Retry batch: 2 more inbound.
            // Total = 4 proves the COORDINATOR_BEHIND retry occurred.
            assertEquals("Expected initial (2) + retry (2) = 4 PAXOS_COMMIT_REQ",
                         4, hold.total());

            // Initial batch (2) carries mutation IDs (tracked era), retry batch (2) does not.
            // Total with ID == 2 proves the retry used the untracked path (no mutation IDs).
            assertEquals("Only the initial stale batch should carry mutation IDs (retry batch should not)",
                         2, hold.withMutationId());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
        assertAllNodesSee(cluster, ks, ReplicationType.untracked);
    }

    /*
     * Scenario: first CAS commits on untracked keyspace (no mutation ID in system.paxos),
     * then migrate to tracked. Second CAS discovers the stale ballot without a mutation ID.
     * PaxosPrepareRefresh must generate a mutation ID for the now-tracked keyspace.
     *
     * Message: PAXOS2_PREPARE_REFRESH_REQ
     * Path: PaxosPrepareRefresh.refresh() -> tracked=true, commit.mutation.id().isNone()=true
     *       -> generates mutation ID locally (calls generateMutationIdAndPersistLocally())
     */
    @Test
    public void testPrepareRefreshGeneratesMutationId() throws Throwable
    {
        // Create as UNTRACKED -- commits will have no mutation ID
        String ks = createKeyspace(cluster, "pmt_v2", "untracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Block remote commits to create an uncommitted ballot WITHOUT a mutation ID
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because remote commits were dropped");
        }
        catch (Exception e)
        {
            assertCasException(e);
        }

        cluster.filters().reset();

        // Migrate to TRACKED. The stale ballot in system.paxos has NO mutation ID.
        alterReplicationType(cluster, ks, "tracked");
        assertAllNodesSee(cluster, ks, ReplicationType.tracked);

        // Spy on PAXOS2_PREPARE_REFRESH_REQ to verify the refresh path fires.
        try (MessageSpy refreshSpy = on(cluster, Verb.PAXOS2_PREPARE_REFRESH_REQ)
                                     .to(2, 3, 4)
                                     .checkMutationId()
                                     .expect(2)
                                     .start())
        {
            // Second CAS discovers the stale ballot. PaxosPrepareRefresh.refresh() runs:
            // tracked=true, commit.mutation.id().isNone()=true -> generates ID locally
            Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            refreshSpy.await();

            // First CAS (untracked era) only committed locally on node 1. Refresh propagates that
            // commit to all replicas (with a mutation ID generated during migration). After refresh,
            // node 1 has v=1 and the condition IF NOT EXISTS evaluates FALSE → second CAS not applied.
            assertCasNotApplied(result);

            // Refresh should have been sent to the remote replicas (2)
            assertEquals("PAXOS2_PREPARE_REFRESH_REQ should match remote replica count",
                         2, refreshSpy.total());

            // All refresh messages should carry a mutation ID (generated for the now-tracked keyspace)
            assertEquals("All PAXOS2_PREPARE_REFRESH_REQ should carry a mutation ID (tracked keyspace)",
                         refreshSpy.total(), refreshSpy.withMutationId());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 1, 1, 2, 3);
    }

    /*
     * Scenario: CAS on tracked keyspace. Inbound filter delays PAXOS2_PREPARE_REQ delivery
     * at nodes 2, 3, 4 while we ALTER to untracked. When released, the handler's
     * checkPaxosPrepareReadMigration detects the mismatch -> COORDINATOR_BEHIND.
     * Unlike the commit path, the prepare path has no internal COORDINATOR_BEHIND retry;
     * the CAS fails. The coordinator catches up via the response epoch, and a client-level
     * retry succeeds.
     *
     * Covers: MigrationRouter.checkPaxosPrepareReadMigration() coordinator-behind branch
     */
    @Test
    public void testPrepareReadCoordinatorBehind() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Delay PAXOS2_PREPARE_REQ at nodes 2, 3, 4 until after migration.
        // The prepare arrives with old epoch (tracked read routing).
        // After migration, handler is at new epoch (untracked).
        // checkPaxosPrepareReadMigration detects mismatch -> COORDINATOR_BEHIND.
        try (MessageSpy hold = on(cluster, Verb.PAXOS2_PREPARE_REQ)
                               .from(1)
                               .to(2, 3, 4)
                               .holdAll()
                               .start())
        {
            CompletableFuture<Throwable> casResult = casAsyncExpectingFailure(cluster, 1,
                                                                               "INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS");

            hold.awaitFirstArrival();

            try
            {
                // ALTER to untracked while prepares are delayed
                alterReplicationType(cluster, ks, "untracked");
                assertAllNodesSee(cluster, ks, ReplicationType.untracked);
            }
            finally
            {
                hold.release();
            }

            // First CAS should fail (COORDINATOR_BEHIND from prepare, no internal retry)
            // Expected: COORDINATOR_BEHIND from prepare causes WriteFailureException.
            // The prepare path does not internally retry COORDINATOR_BEHIND (unlike commit).
            Throwable error = casResult.get(60, TimeUnit.SECONDS);
            assertNotNull("First CAS should fail with COORDINATOR_BEHIND", error);
            assertCasException((Exception) error);

            // Exactly one prepare round was attempted (2 remote replicas for KEY=5 with RF=3 from node 1)
            assertEquals("Exactly one prepare round should be attempted (2 messages to remote replicas)",
                         2, hold.total());
        }

        cluster.filters().reset();

        // The coordinator has caught up by now (ResponseVerbHandler.maybeFetchLogs triggered
        // by the COORDINATOR_BEHIND response). A client-level retry succeeds.
        Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

        assertCasApplied(result);

        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2, 3);
    }

    /*
     * Covers: Paxos.cas() -- the throw inside the COORDINATOR_BEHIND retry while-loop
     * when the failure is NOT COORDINATOR_BEHIND (coordinatorBehindCount == 0).
     *
     * The V2 commit while-loop was added to retry on COORDINATOR_BEHIND. For all other
     * failures, it throws immediately. This test blocks commit messages to trigger a timeout.
     */
    @Test
    public void testCommitFailureThrowsFromRetryLoop() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Block commit messages to remotes and respond with TIMEOUT immediately so the
        // callback fires without waiting for the real write_request_timeout.
        // Local commit succeeds (1/3) but quorum (2) is not met -> Paxos.cas() throws.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS_COMMIT_REQ.id)
               .from(1).to(2, 3, 4)
               .messagesMatching((from, to, msg) -> {
                   cluster.get(to).runOnInstance(() -> PaxosMigrationTestUtils.respondWithTimeout(msg));
                   return true;
               }).drop();

        boolean threw = false;
        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
        }
        catch (Exception e)
        {
            threw = true;
            assertCasException(e);
        }

        assertTrue("CAS should have failed due to commit timeout", threw);
    }

    /*
     * First CAS from node 2 with commits blocked to nodes 1,3 -> only node 2 commits.
     * Second CAS from node 1: coordinator = data node for TrackedRead. Node 2 (summary,
     * readResponse=null) has the committed ballot. Node 3 is also a summary node without
     * the committed ballot.
     *
     * Key subtlety: PaxosPrepare signals outcome as soon as quorum is reached. If self +
     * node 3 reach quorum before node 2's response, latestCommitted is still 'none' and
     * FOUND_INCOMPLETE_ACCEPTED fires instead. To ensure FOUND_INCOMPLETE_COMMITTED, we
     * delay node 3's prepare so node 2 responds first, making latestCommitted known before
     * quorum is reached. Then hasInProgressProposal() returns false (same ballot), and the
     * check falls through to FOUND_INCOMPLETE_COMMITTED.
     */
    @Test
    public void testCommitAndPrepareViaIncompleteCommitted() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });
        // Both node 1 and node 2 are replicas for KEY (5): node 2 coordinates the
        // first CAS (local commit), and node 1 coordinates the second CAS (TrackedRead data node).

        // First CAS from NODE 2. Block commits to nodes 1 and 3.
        // Node 2 commits locally (executeOnSelf), nodes 1,3 only have ACCEPTED state.
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).from(2).to(1, 3).drop();

        try
        {
            cluster.coordinator(2).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because commits to nodes 1,3 were blocked");
        }
        catch (Exception e)
        {
            assertCasException(e);
        }

        cluster.filters().reset();

        // Ensure node 2's prepare response arrives at node 1 before node 3's.
        // This guarantees FOUND_INCOMPLETE_COMMITTED (not FOUND_INCOMPLETE_ACCEPTED).
        AssertingLatch node2Responded = new AssertingLatch("testCommitAndPrepareViaIncompleteCommitted node2Responded");
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_RSP.id)
               .from(2)
               .to(1)
               .messagesMatching((from, to, msg) -> {
                   node2Responded.countDown();
                   return false;
               }).drop();

        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS2_PREPARE_RSP.id)
               .from(3)
               .to(1)
               .messagesMatching((from, to, msg) -> {
                   node2Responded.await();
                   return false;
               }).drop();

        // Spy on verbs to prove the TRACKED commitAndPrepare path:
        // - PAXOS_COMMIT_REQ: the tracked path sends separate commit (PaxosCommit.commit())
        // - PAXOS2_COMMIT_AND_PREPARE_REQ: the UNTRACKED combined path — should NOT appear
        // - PAXOS2_PREPARE_REQ: separate prepare after commit (prepareWithBallot)
        try (MessageSpy commitSpy = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                    .from(1)
                                    .checkMutationId()
                                    .expect(4)
                                    .start();
             MessageSpy combinedSpy = on(cluster, Verb.PAXOS2_COMMIT_AND_PREPARE_REQ)
                                      .from(1)
                                      .start();
             MessageSpy prepareSpy = on(cluster, Verb.PAXOS2_PREPARE_REQ)
                                     .from(1)
                                     .expect(4)
                                     .start())
        {
            // Second CAS from NODE 1 (coordinator = data node for TrackedRead).
            // Response order: self (node 1, no commit) + node 2 (summary, has commit) = quorum.
            // latestCommitted known, hasInProgressProposal() = false (same ballot).
            // withLatest = {node2} (1) < quorum (2), haveReadResponseWithLatest = false.
            // -> FOUND_INCOMPLETE_COMMITTED -> commitAndPrepare()
            Object[][] result = cluster.coordinator(1).execute("UPDATE " + ks + ".tbl SET v2 = 99 WHERE k = " + KEY + " IF EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            commitSpy.await();
            prepareSpy.await();

            assertNotNull("CAS should return result", result);
            assertEquals(1, result.length);
            assertTrue("Second CAS (UPDATE IF EXISTS) should apply — row was repaired to v=42",
                       (boolean) result[0][0]);

            // Verify TRACKED commitAndPrepare path was taken:
            // 1. Separate PAXOS_COMMIT_REQ sent: 2 from repair commit + 2 from CAS's own commit = 4
            assertEquals("Tracked path should send 4 PAXOS_COMMIT_REQ (2 repair + 2 CAS commit)",
                         4, commitSpy.total());
            // 2. All commits carry a mutation ID (tracked keyspace)
            assertEquals("All PAXOS_COMMIT_REQ should carry a mutation ID",
                         commitSpy.total(), commitSpy.withMutationId());
            // 3. Combined PAXOS2_COMMIT_AND_PREPARE_REQ NOT sent (that's the untracked path)
            assertEquals("Combined commit+prepare should NOT be used for tracked keyspace",
                         0, combinedSpy.total());
            // 4. PAXOS2_PREPARE_REQ messages: initial CAS prepare (2) + commitAndPrepare's prepareWithBallot (2) = 4
            assertEquals("Tracked path: initial prepare (2) + post-repair prepareWithBallot (2) = 4",
                         4, prepareSpy.total());
        }

        // Verify BOTH the old commit (v=42) and new commit (v2=99) applied on all REPLICAS.
        // KEY=5 maps to replicas [1,2,3] — node 4 is not a replica and holds no local data.
        for (int i : new int[]{ 1, 2, 3 })
        {
            Object[][] nodeResult = cluster.get(i).executeInternal("SELECT v, v2 FROM " + ks + ".tbl WHERE k = " + KEY);
            assertEquals("Node " + i + " should have the committed row", 1, nodeResult.length);
            assertEquals("Node " + i + " should have v=42 from first commit", 42, nodeResult[0][0]);
            assertEquals("Node " + i + " should have v2=99 from second commit", 99, nodeResult[0][1]);
        }
    }

    /*
     * Triggers the V2 commitAndPrepare() path by creating an ACCEPTED-but-not-COMMITTED
     * state on all replicas. Node 4 (not a replica for the key) is the CAS coordinator.
     * In V2 PaxosCommit.start(), executeOnSelf() skips the local commit (not in range).
     * Blocking PAXOS_COMMIT_REQ prevents remote commits. All 3 replicas end up in ACCEPTED
     * only. A second CAS discovers FOUND_INCOMPLETE_ACCEPTED -> re-propose -> commitAndPrepare().
     */
    @Test
    public void testCommitAndPrepareViaIncompleteAccepted() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "untracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

        try
        {
            cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because all commits were dropped");
        }
        catch (Exception e)
        {
            assertCasException(e);
        }

        cluster.filters().reset();

        try (MessageSpy spy = on(cluster, Verb.PAXOS2_COMMIT_AND_PREPARE_REQ)
                              .start())
        {
            Object[][] result = cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 1) IF NOT EXISTS",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            assertCasNotApplied(result);

            // Coordinator is node 4 (NOT a replica for KEY=5; replicas are [1,2,3]).
            // Non-replica coordinator sends commitAndPrepare to ALL 3 replicas (no local execute).
            assertEquals("commitAndPrepare should send PAXOS2_COMMIT_AND_PREPARE_REQ to all 3 replicas (coordinator is non-replica)",
                         3, spy.total());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 1, 1, 2, 3);
    }

    /*
     * Tests the handler-side PaxosPrepare.RequestHandler.doVerb() behavior: it calls
     * checkPaxosPrepareReadMigration() BEFORE execute() runs, so a coordinator that hasn't
     * yet enacted the ALTER is rejected at the prepare phase — commitAndPrepare is never
     * reached. The CAS fails via timeout at cas_contention_timeout (the EpochPin blocks
     * ResponseVerbHandler.maybeFetchLogs, preventing the failure callback from completing).
     *
     * NOTE: Despite the name's historical origin, this test does NOT exercise
     * PaxosCommitAndPrepare.RequestHandler — the prepare-level rejection fires first.
     * A separate test (testCommitAndPrepareHandlerRejectsAfterMigration) exercises the
     * commitAndPrepare handler by engineering a scenario where prepare succeeds first.
     */
    @Test
    public void testPrepareRejectsAfterMigration() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "untracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Create incomplete ACCEPTED state: CAS from node 4, block commits globally.
        // Node 4 is NOT a replica (KEY=5, RF=3 → replicas 1,2,3), so all 3 replicas
        // only reach ACCEPTED state (commit fails to reach them).
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

        try
        {
            cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because all commits were dropped");
        }
        catch (Exception e)
        {
            assertCasException(e);
        }

        cluster.filters().reset();

        try (EpochPin pin = epochPin(cluster, 2))
        {
            // ALTER to tracked from node 1 (CMS). Use CL.ONE to avoid waiting for node 2's agreement.
            alterReplicationTypeFrom(cluster, 1, ks, "tracked", ConsistencyLevel.ONE);

            // Wait for node 3 to see tracked. Node 2 should still see untracked.
            awaitReplicationType(cluster, ks, ReplicationType.tracked, 3);
            assertNodeSees(cluster, 2, ks, ReplicationType.untracked);

            // CAS from node 2. Node 2 sees untracked; handlers on nodes 1,3 see tracked.
            // PaxosPrepare.RequestHandler.doVerb() calls checkPaxosPrepareReadMigration() BEFORE
            // execute(), which detects the disagreement and throws CoordinatorBehindException.
            // The FAILURE_RSP propagates back to node 2 — its ResponseVerbHandler triggers
            // maybeFetchLogs which attempts a TCM fetch. The fetch is held by the EpochPin;
            // the CAS ultimately fails via the inner response-handler timeout at cas_contention_timeout.
            boolean threw = false;
            try
            {
                cluster.coordinator(2).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            }
            catch (Exception e)
            {
                threw = true;
                assertCasException(e);
            }

            assertTrue("CAS should have failed due to handler COORDINATOR_BEHIND", threw);

            // Verify data state: the first CAS's incomplete ACCEPTED ballot (v=42) was NEVER
            // successfully committed on any replica. The handler rejected the new CAS's commit,
            // and the prior round never reached a committed state either.
            assertReplicaHasNoRow(cluster, ks, KEY, 1);
        }
    }

    /*
     * Exercises the commitAndPrepare handler's checkPaxosCommitMigration disagree case
     * specifically. Unlike testPrepareRejectsAfterMigration (where prepare fails first),
     * this test engineers a scenario where:
     *   1. Prepare succeeds — all nodes agree on current migration state at prepare time
     *   2. commitAndPrepare is triggered (via FOUND_INCOMPLETE_ACCEPTED)
     *   3. PAXOS2_COMMIT_AND_PREPARE_REQ is held in flight via message filter
     *   4. While held: ALTER to tracked fires on nodes 1,3 (coordinator node 2 has TCM blocked
     *      so stays untracked)
     *   5. Held messages are released → handlers see tracked, coordinator said untracked →
     *      checkPaxosCommitMigration throws CoordinatorBehindException
     *   6. CAS fails with CAS exception
     */
    @Test
    public void testCommitAndPrepareHandlerRejectsAfterMigration() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "untracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Create incomplete ACCEPTED state. CAS from node 4 (non-replica), block commits.
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

        try
        {
            cluster.coordinator(4).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because all commits were dropped");
        }
        catch (Exception e)
        {
            assertCasException(e);
        }

        cluster.filters().reset();

        // Hold inbound PAXOS2_COMMIT_AND_PREPARE_REQ at nodes 1,3 until released. Node 2 is the
        // coordinator (NOT one of the handlers) — messages flow OUT of node 2 TO nodes 1,3.
        try (MessageSpy hold = on(cluster, Verb.PAXOS2_COMMIT_AND_PREPARE_REQ)
                               .from(2)
                               .to(1, 3)
                               .holdAll()
                               .expect(2)
                               .start())
        {
            // Start the second CAS from node 2 asynchronously. All nodes currently see untracked,
            // so prepare succeeds. FOUND_INCOMPLETE_ACCEPTED triggers commitAndPrepare.
            CompletableFuture<Throwable> casResult = casAsyncExpectingFailure(cluster, 2,
                                                                               "INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS");

            EpochPin pin = null;
            try
            {
                hold.awaitFirstArrival();

                // Pin node 2 at its current epoch so it stays untracked while we ALTER via node 1.
                pin = epochPin(cluster, 2);

                // ALTER to tracked from node 1 (CMS). CL.ONE so we don't wait for node 2.
                alterReplicationTypeFrom(cluster, 1, ks, "tracked", ConsistencyLevel.ONE);

                // Wait for node 3 to see tracked. Node 2 stays untracked.
                awaitReplicationType(cluster, ks, ReplicationType.tracked, 3);
                assertNodeSees(cluster, 2, ks, ReplicationType.untracked);
            }
            finally
            {
                hold.release();
            }

            try
            {
                // Held messages are now released. Handlers see tracked; coordinator's message carries
                // untracked-era payload (no mutation ID, coordinatorSaysTracked=false). The handler's
                // checkPaxosCommitMigration detects the disagreement → CoordinatorBehindException.
                Throwable error = casResult.get(30, TimeUnit.SECONDS);
                assertNotNull("Second CAS must fail because commitAndPrepare handler rejects", error);
                assertCasException((Exception) error);

                // Verify the commitAndPrepare handler was actually invoked (otherwise we didn't test it).
                assertEquals("PAXOS2_COMMIT_AND_PREPARE_REQ must arrive at both tracked handlers",
                             2, hold.total());

                // Verify data state: no replica should have a committed v=42 row.
                assertReplicaHasNoRow(cluster, ks, KEY, 1);
            }
            finally
            {
                if (pin != null)
                    pin.close();
            }
        }
    }

    /*
     * Covers: PaxosCommitAndPrepare.java
     *   commit = commit.withMutationId(MutationId.none());
     *
     * First CAS while tracked stores committed ballot WITH mutation ID in system.paxos.
     * Migrate to untracked. Second CAS triggers FOUND_INCOMPLETE_COMMITTED which calls
     * commitAndPrepare(). The loaded commit has an ID but shouldBeTracked is now false,
     * so the code strips the ID before committing via the untracked path.
     */
    @Test
    public void testCommitAndPrepareStripsIdAfterMigrationToUntracked() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // First CAS from node 2. Block commits to nodes 1,3,4 so only node 2 commits.
        cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).from(2).to(1, 3, 4).drop();

        try
        {
            cluster.coordinator(2).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            fail("CAS should have thrown because commits to nodes 1,3,4 were blocked");
        }
        catch (Exception e)
        {
            assertCasException(e);
        }

        cluster.filters().reset();

        // Migrate to untracked. The committed ballot in system.paxos on node 2 still has
        // its mutation ID from the tracked era.
        alterReplicationType(cluster, ks, "untracked");
        assertAllNodesSee(cluster, ks, ReplicationType.untracked);

        // Spy on commit verbs AND refresh verbs. Count messages that still carry a mutation ID
        // after migration to untracked — any non-zero count indicates stripping failed at one
        // of these sites:
        //   - PaxosCommitAndPrepare.java (commitAndPrepare path)
        //   - PaxosPrepareRefresh.java (refresh path)
        //   - StorageProxy.java sendCommit (V1 repair path)
        //   - StorageProxy.java commitPaxos (reconcile path)
        // Which path fires depends on PaxosPrepare outcome (FOUND_INCOMPLETE_COMMITTED vs refresh
        // via haveReadResponseWithLatest), which varies with prepare response timing. We verify
        // the invariant that applies to ALL paths: after migration to untracked, no commit/refresh
        // message sent to a remote replica should carry a mutation ID.
        try (MessageSpy spy = on(cluster,
                                 Verb.PAXOS_COMMIT_REQ,
                                 Verb.PAXOS2_COMMIT_AND_PREPARE_REQ,
                                 Verb.PAXOS2_PREPARE_REFRESH_REQ)
                              .to(2, 3, 4)
                              .checkMutationId()
                              .expect(2)
                              .start())
        {
            // Second CAS from node 1 (data node for TrackedRead). Prepare discovers
            // FOUND_INCOMPLETE_COMMITTED from node 2's committed ballot (which has a mutation ID).
            // PaxosCommitAndPrepare.commitAndPrepare() loads the commit with ID.
            // shouldBeTracked=false -> the code strips the mutation ID.
            // If the stripping didn't fire, the untracked commit path would reject the mutation with an ID.
            Object[][] result = cluster.coordinator(1).execute("UPDATE " + ks + ".tbl SET v = 99 WHERE k = " + KEY + " IF v = 42",
                                                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            spy.await();

            assertCasApplied(result);

            // Core invariant: no PAXOS_COMMIT_REQ / PAXOS2_COMMIT_AND_PREPARE_REQ / PAXOS2_PREPARE_REFRESH_REQ
            // should carry a mutation ID after migration to untracked. If stripping failed anywhere, the
            // handler would reject with CoordinatorBehindException → CAS fails.
            assertEquals("No commit/refresh message should carry a mutation ID after migration to untracked",
                         0, spy.withMutationId());
        }

        assertReplicasHaveValue(cluster, ks, KEY, 99, 1, 2, 3);
    }

    /*
     * Verifies: PaxosCommit.onFailure() does NOT call submitHint() for tracked mutations.
     * Tracked mutations use MutationTrackingService for retries, not the hint system.
     * Writing hints with mutation IDs causes IllegalStateException on replay after migration.
     */
    @Test
    public void testTrackedPaxosCommitDoesNotWriteHints() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        // Reject hints for mutations created before now — prevents prior tests'
        // delayed PaxosCommit.onFailure callbacks from contaminating the hint count.
        // Current test's mutations are created after this threshold and pass through.
        // Capture inside node 1 (the coordinator) so both sides of the comparison use the same clock.
        long now = cluster.get(1).callOnInstance(() -> System.nanoTime());
        cluster.forEach(instance -> instance.runOnInstance(() -> HintsService.setRejectHintsBeforeNanos(now)));

        long hintsBefore = cluster.get(1).callOnInstance(() ->
            StorageMetrics.totalHints.getCount());

        // Drop PAXOS_COMMIT_REQ from node 1 to node 3 and respond with TIMEOUT immediately
        // so PaxosCommit.onFailure() fires without waiting for the real write_request_timeout.
        // shouldHint() returns true for a live node; with the fix, isTracked()=true prevents the hint write.
        cluster.filters()
               .inbound(true)
               .verbs(Verb.PAXOS_COMMIT_REQ.id)
               .from(1).to(3)
               .messagesMatching((from, to, msg) -> {
                   cluster.get(to).runOnInstance(() -> PaxosMigrationTestUtils.respondWithTimeout(msg));
                   return true;
               }).drop();

        // CAS from node 1 at QUORUM. Nodes 1,2 succeed (quorum met), node 3 times out.
        // PaxosCommit.onFailure fires for node 3. With the fix, no hint is written.
        Object[][] result = cluster.coordinator(1).execute("INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS",
                                                           ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

        assertCasApplied(result);

        // Synthetic TIMEOUT fires the callback almost immediately; 1s is a generous safety margin.
        // With isTracked()=true, submitHint should NOT be called.
        Thread.sleep(1000);

        long hintsAfter = cluster.get(1).callOnInstance(() ->
            StorageMetrics.totalHints.getCount());
        assertEquals("No hints should be written for tracked Paxos commits", hintsBefore, hintsAfter);

        // Verify data state: quorum committed on nodes 1,2 (node 3 was blocked).
        cluster.filters().reset();
        assertReplicasHaveValue(cluster, ks, KEY, 42, 1, 2);
    }

    /*
     * Exercises the commit-retry loop's deadline-exit path in Paxos.cas(). V2 checks migration
     * state at PREPARE (via checkPaxosPrepareReadMigration), so to ever reach the commit retry
     * loop we must let prepare succeed at the initial agreement, then create a per-replica
     * migration disagreement that only surfaces at commit time. We do that by TCM-stranding
     * node 2 as the coordinator, holding PAXOS_COMMIT_REQ at nodes 1,3 until after an ALTER
     * that nodes 1,3 enact but node 2 cannot, then releasing. The handlers then reject with
     * COORDINATOR_BEHIND, and node 2 — unable to catch up via TCM — exhausts commitDeadline
     * before a retry iteration can fire.
     *
     * Known ambiguity: the CAS could also fail via ConditionAsConsumer.awaitUntil(commitDeadline)
     * expiring before cb.onFailure signals (if COORDINATOR_BEHIND responses arrive at the
     * boundary). The two paths produce different exceptions (CasWriteTimeoutException vs
     * WriteFailureException); the test accepts both via assertCasException. The invariant under
     * test is the user-visible one: V2 CAS fails with a CAS exception when the coordinator is
     * persistently behind after prepare succeeds. Exact count==2 asserts only the initial
     * commit batch landed (no retry iteration).
     */
    @Test
    public void testCommitRetryLoopTimeout() throws Throwable
    {
        String ks = createKeyspace(cluster, "pmt_v2", "tracked");
        assertReplicasAreExactly(cluster, ks, KEY, new int[]{ 1, 2, 3 });

        try (EpochPin pin = epochPin(cluster, 2))
        {
            // Hold PAXOS_COMMIT_REQ from node 2 at nodes 1,3 until after the ALTER. Prepare
            // is not filtered so it proceeds to completion.
            try (MessageSpy hold = on(cluster, Verb.PAXOS_COMMIT_REQ)
                                   .from(2)
                                   .to(1, 3)
                                   .holdAll()
                                   .checkMutationId()
                                   .start())
            {
                CompletableFuture<Throwable> casResult = casAsyncExpectingFailure(cluster, 2,
                                                                                   "INSERT INTO " + ks + ".tbl (k, v) VALUES (" + KEY + ", 42) IF NOT EXISTS");

                try
                {
                    hold.awaitFirstArrival();

                    // ALTER to untracked via node 1 at CL.ONE so we don't wait for schema agreement
                    // on TCM-blocked node 2.
                    alterReplicationTypeFrom(cluster, 1, ks, "untracked", ConsistencyLevel.ONE);

                    // Wait for nodes 1 and 3 to observe untracked. Node 2 must still see tracked.
                    awaitReplicationType(cluster, ks, ReplicationType.untracked, 1, 3);
                    assertNodeSees(cluster, 2, ks, ReplicationType.tracked);
                }
                finally
                {
                    hold.release();
                }

                Throwable error = casResult.get(60, TimeUnit.SECONDS);
                assertNotNull("CAS from stranded coordinator must fail", error);
                assertTrue("Expected an Exception but got " + error.getClass().getName() + ": " + error.getMessage(),
                           error instanceof Exception);
                assertCasException((Exception) error);

                // Prepare succeeded (no filter on it), so the commit phase ran. The initial commit
                // batch to replicas 1,3 is 2 messages; the inner response-handler timeout fires
                // before the held TCM fetch releases and lets cb.onFailure signal the retry loop.
                assertEquals("Expected exactly 2 inbound PAXOS_COMMIT_REQ (initial batch only; retry iteration never fires because deadline expires before held TCM fetch releases)",
                             2, hold.total());

                // All commits from stranded node 2 carry mutation IDs (tracked path),
                // proving COORDINATOR_BEHIND is the rejection reason
                assertEquals("All commit attempts from stranded coordinator should carry mutation IDs (tracked path)",
                             2, hold.withMutationId());
            }
        }
    }
}
