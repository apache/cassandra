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
package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.repair.MutationTrackingIncrementalRepairTask;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.ClusterUtils.decode;
import static org.apache.cassandra.distributed.shared.ClusterUtils.encode;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getNextEpoch;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;
import static org.apache.cassandra.schema.ReplicationType.tracked;
import static org.apache.cassandra.schema.ReplicationType.untracked;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests that blocking read repair works correctly before, during, and after migration
 * between untracked and tracked replication types (in both directions).
 *
 * <p>
 * When a keyspace uses untracked replication, reads detect inconsistencies and use
 * blocking read repair (BRR) via {@code READ_REPAIR_REQ} messages (the classic path).
 * When a keyspace uses tracked replication, reads use reconciliation (mutation summaries)
 * instead of BRR. During migration, reads for pending ranges use the untracked path
 * with BRR, but BRR writes go through the tracked write path so they get proper
 * MutationIds and are recorded in the mutation journal.
 * <p>
 * Each test creates a unique keyspace to avoid interference. Keyspaces are not dropped
 * between tests to avoid background broadcast races (see MutationTrackingRepairTest).
 */
public class MutationTrackingReadRepairTest extends TestBaseImpl
{
    private static final int NUM_NODES = 3;
    private static final List<Integer> ALL_NODES = List.of(1, 2, 3);
    private static final String REPLICATION = "{'class': 'SimpleStrategy', 'replication_factor': 3}";

    private static Cluster CLUSTER;
    private static ExecutorService executor;
    private static final AtomicInteger ksCounter = new AtomicInteger();

    private String ksName;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).build());
        CLUSTER = Cluster.build()
                         .withNodes(NUM_NODES)
                         .withConfig(cfg -> cfg.set("hinted_handoff_enabled", false)
                                               .set("mutation_tracking_sync_timeout", "10s")
                                               .set("request_timeout", "10000ms")
                                               .set("repair.retries.max_attempts", 10)
                                               .set("repair.retries.base_sleep_time", "100ms")
                                               .set("repair.retries.max_sleep_time", "500ms")
                                               .with(Feature.GOSSIP))
                         .start();
    }

    @AfterClass
    public static void teardownCluster()
    {
        executor.shutdownNow();
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void setUp()
    {
        ksName = "mt_rr_" + ksCounter.incrementAndGet();
        // Pause regular-priority (background write retry) delivery on all nodes so the
        // reconciler cannot proactively fix inconsistencies before BRR gets a chance to
        // fire. High-priority tasks (needed by tracked read reconciliation) still run,
        // so tracked reads work normally.
        CLUSTER.forEach(() -> MutationTrackingService.instance().pauseActiveReconcilerRegularPriority());
    }

    @After
    public void tearDown()
    {
        CLUSTER.filters().reset();
        CLUSTER.forEach(() -> MutationTrackingService.instance().resumeActiveReconciler());
        CLUSTER.forEach(() -> MutationTrackingService.instance().resumeActiveReconcilerRegularPriority());
        // Unpause any paused epoch enactment
        for (int i = 1; i <= CLUSTER.size(); i++)
            unpauseEnactment(CLUSTER.get(i));
        // Re-mark all nodes as alive in case a test isolated one
        CLUSTER.forEach(() ->
            Gossiper.runInGossipStageBlocking(() -> {
                for (var entry : Gossiper.instance.endpointStateMap.entrySet())
                {
                    InetAddressAndPort ep = entry.getKey();
                    EndpointState state = entry.getValue();
                    if (!ep.equals(FBUtilities.getBroadcastAddressAndPort()) && !state.isAlive())
                    {
                        FailureDetector.instance.report(ep);
                        Gossiper.instance.realMarkAlive(ep, state);
                    }
                }
            }));
    }

    private void createKeyspace(ReplicationType replicationType)
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                REPLICATION + " AND replication_type='" + replicationType + "'");
        CLUSTER.schemaChange("CREATE TABLE " + ksName + ".tbl (k int PRIMARY KEY, v int)");
    }

    private void alterKeyspace(ReplicationType replicationType)
    {
        CLUSTER.schemaChange("ALTER KEYSPACE " + ksName + " WITH replication = " +
                REPLICATION + " AND replication_type='" + replicationType + "'");
    }

    private void insertConsistent(int start, int count)
    {
        for (int i = start; i < start + count; i++)
        {
            CLUSTER.coordinator(1).execute(
                    "INSERT INTO " + ksName + ".tbl (k, v) VALUES (?, ?)",
                    ConsistencyLevel.ALL, i, i);
        }
    }

    /**
     * Create inconsistency by isolating a node during QUORUM writes.
     * After this call, the isolated node is missing the written data.
     */
    private void insertWithInconsistency(int isolatedNode, int start, int count)
    {
        CLUSTER.filters().allVerbs().to(isolatedNode).drop();
        CLUSTER.filters().allVerbs().from(isolatedNode).drop();

        for (int i = start; i < start + count; i++)
        {
            int coordinatorNode = isolatedNode == 1 ? 2 : 1;
            CLUSTER.coordinator(coordinatorNode).execute(
                    "INSERT INTO " + ksName + ".tbl (k, v) VALUES (?, ?)",
                    ConsistencyLevel.QUORUM, i, i);
        }

        // Verify the isolated node actually missed the data
        Object[][] results = CLUSTER.get(isolatedNode).executeInternal(
                "SELECT k FROM " + ksName + ".tbl WHERE k >= ? AND k < ? ALLOW FILTERING",
                start, start + count);
        assertEquals("Node " + isolatedNode + " should not have data written while isolated",
                     0, results.length);
        CLUSTER.filters().reset();
    }

    private void assertNodeMissingData(int node, int start, int count)
    {
        Object[][] results = CLUSTER.get(node).executeInternal(
                "SELECT k FROM " + ksName + ".tbl WHERE k >= ? AND k < ? ALLOW FILTERING",
                start, start + count);
        assertEquals("Node " + node + " should still be missing data for keys [" + start + ", " + (start + count)
                     + ") after migration started — if present, data was delivered by an unexpected mechanism",
                     0, results.length);
    }

    private void assertDataOnAllNodes(int start, int count)
    {
        for (int node = 1; node <= CLUSTER.size(); node++)
        {
            for (int i = start; i < start + count; i++)
            {
                Object[][] results = CLUSTER.get(node).executeInternal(
                        "SELECT k, v FROM " + ksName + ".tbl WHERE k = ?", i);
                assertEquals("Node " + node + " missing row k=" + i, 1, results.length);
                assertEquals(i, results[0][0]);
                assertEquals(i, results[0][1]);
            }
        }
    }

    private long getRepairedBlocking(int node)
    {
        //noinspection Convert2MethodRef
        return CLUSTER.get(node).callOnInstance(() -> ReadRepairMetrics.repairedBlocking.getCount());
    }

    private long getRepairedBlockingViaTrackedWrite(int node)
    {
        //noinspection Convert2MethodRef
        return CLUSTER.get(node).callOnInstance(() -> ReadRepairMetrics.repairedBlockingViaTrackedWrite.getCount());
    }

    private long getTrackedReconcile(int node)
    {
        //noinspection Convert2MethodRef
        return CLUSTER.get(node).callOnInstance(() -> ReadRepairMetrics.trackedReconcile.getCount());
    }

    private boolean isMigrationInProgress()
    {
        String ks = ksName;
        return CLUSTER.get(1).callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            return MutationTrackingIncrementalRepairTask.isMutationTrackingMigrationInProgress(metadata, ks);
        });
    }

    private NodeToolResult nodetoolRepair(int node, String... args)
    {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "repair";
        System.arraycopy(args, 0, cmd, 1, args.length);
        return CLUSTER.get(node).nodetoolResult(cmd);
    }

    private List<NodeToolResult> repairConcurrently(List<Integer> nodes, String... args)
    {
        List<Future<NodeToolResult>> futures = new ArrayList<>();
        for (int node : nodes)
            futures.add(executor.submit(() -> nodetoolRepair(node, args)));
        List<NodeToolResult> results = new ArrayList<>();
        for (Future<NodeToolResult> f : futures)
        {
            try
            {
                results.add(f.get(60, TimeUnit.SECONDS));
            }
            catch (Exception e)
            {
                throw new RuntimeException("Repair future failed", e);
            }
        }
        return results;
    }

    private void completeMigrationViaRepair()
    {
        assertTrue("Migration should be in progress before repair", isMigrationInProgress());
        // The mutation tracking sync coordinator needs the regular-priority reconciler to
        // deliver mutations between replicas. Temporarily resume it for the duration of repair.
        CLUSTER.forEach(() -> MutationTrackingService.instance().resumeActiveReconcilerRegularPriority());
        try
        {
            List<NodeToolResult> results = repairConcurrently(ALL_NODES, ksName, "-pr");
            for (NodeToolResult r : results)
                r.asserts().success();
            assertFalse("Migration should complete after repair", isMigrationInProgress());
        }
        finally
        {
            CLUSTER.forEach(() -> MutationTrackingService.instance().pauseActiveReconcilerRegularPriority());
        }
    }

    /**
     * Read all specified keys using a mix of point reads and a range scan to exercise
     * both single-partition and multi-partition read repair code paths.
     *
     * <p>The first half of keys are read via individual point reads ({@code WHERE k = ?}).
     * Then a full table scan exercises the range read path, repairing any remaining
     * inconsistent partitions (the second half).
     *
     * <p>Per-read-command metrics (repairedBlocking) increment by the returned count.
     * Per-partition metrics (repairedBlockingViaTrackedWrite) increment by the total
     * number of inconsistent partitions regardless of how they were read.
     *
     * @return the number of CQL read commands issued
     */
    private int readAllKeys(int coordinator, int start, int count)
    {
        int mid = start + count / 2;

        // First half: individual point reads (single-partition read path)
        for (int i = start; i < mid; i++)
        {
            CLUSTER.coordinator(coordinator).execute(
                    "SELECT k, v FROM " + ksName + ".tbl WHERE k = ?",
                    ConsistencyLevel.ALL, i);
        }

        // Full table scan (multi-partition range read path).
        // Point reads above already repaired keys [start, mid).
        // The range scan repairs remaining inconsistent partitions.
        CLUSTER.coordinator(coordinator).execute(
                "SELECT k, v FROM " + ksName + ".tbl",
                ConsistencyLevel.ALL);

        return (mid - start) + 1;
    }

    /**
     * Baseline: BRR in a pure untracked keyspace uses the classic READ_REPAIR_REQ path.
     * Verify that repairedBlocking increments and repairedBlockingViaTrackedWrite does NOT.
     */
    @Test
    public void testBlockingReadRepairUntracked()
    {
        createKeyspace(untracked);
        insertWithInconsistency(3, 0, 10);

        long blockingBefore = getRepairedBlocking(1);
        long trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);

        int readOps = readAllKeys(1, 0, 10);

        long blockingAfter = getRepairedBlocking(1);
        long trackedWriteAfter = getRepairedBlockingViaTrackedWrite(1);
        assertEquals("repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, blockingAfter);
        assertEquals("repairedBlockingViaTrackedWrite should NOT increment for untracked BRR",
                     trackedWriteBefore, trackedWriteAfter);
        assertDataOnAllNodes(0, 10);
    }

    /**
     * During migration from untracked → tracked, reads for pending ranges use the untracked
     * read executor with BRR. BRR detects that writes should go through the tracked path
     * and uses repairViaTrackedWrite, incrementing repairedBlockingViaTrackedWrite.
     */
    @Test
    public void testBlockingReadRepairDuringMigrationUntrackedToTracked()
    {
        createKeyspace(untracked);
        insertWithInconsistency(3, 0, 10);

        // Start migration — all ranges are pending
        alterKeyspace(tracked);
        assertTrue("Migration should be in progress", isMigrationInProgress());

        long blockingBefore = getRepairedBlocking(1);
        long trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);

        // Pending ranges use untracked reads, BRR fires, writes go through tracked path
        int readOps = readAllKeys(1, 0, 10);

        long blockingAfter = getRepairedBlocking(1);
        long trackedWriteAfter = getRepairedBlockingViaTrackedWrite(1);
        assertEquals("repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, blockingAfter);
        assertEquals("repairedBlockingViaTrackedWrite should increment for all repaired partitions",
                     trackedWriteBefore + 10, trackedWriteAfter);
        assertDataOnAllNodes(0, 10);

        completeMigrationViaRepair();
        assertDataOnAllNodes(0, 10);
    }

    /**
     * After migration to tracked completes, reads use TrackedReadExecutor with
     * reconciliation — not BRR. Verify trackedReconcile increments and repairedBlocking does not.
     */
    @Test
    public void testReconciliationAfterMigrationToTracked()
    {
        createKeyspace(untracked);
        insertConsistent(0, 5);

        alterKeyspace(tracked);
        completeMigrationViaRepair();

        // Inconsistency goes through tracked coordination (gets MutationId) but node 3 misses the write
        insertWithInconsistency(3, 100, 10);

        long blockingBefore = getRepairedBlocking(1);
        long reconcileBefore = getTrackedReconcile(1);

        // Fully tracked — uses TrackedReadExecutor + reconciliation, not BRR
        int readOps = readAllKeys(1, 100, 10);

        long blockingAfter = getRepairedBlocking(1);
        long reconcileAfter = getTrackedReconcile(1);

        assertEquals("repairedBlocking should NOT increment for fully tracked reads",
                     blockingBefore, blockingAfter);
        assertEquals("trackedReconcile should increment for each tracked read",
                     reconcileBefore + readOps, reconcileAfter);

        assertDataOnAllNodes(0, 5);
        assertDataOnAllNodes(100, 10);
    }

    /**
     * After switching from tracked → untracked (instant, no migration), reads use the
     * untracked executor with classic BRR via {@code READ_REPAIR_REQ} messages.
     * Since the keyspace is fully untracked, repairedBlockingViaTrackedWrite should NOT increment.
     * Tests both inconsistencies created while tracked (with MutationIds) and inconsistencies
     * created after switching to untracked (without MutationIds).
     */
    @Test
    public void testBlockingReadRepairAfterSwitchToUntracked()
    {
        createKeyspace(tracked);
        insertConsistent(0, 5);

        // Inconsistency created while tracked — mutations have MutationIds
        insertWithInconsistency(3, 100, 10);

        // Switch to untracked — instant, no migration
        alterKeyspace(untracked);
        assertFalse("Migration should NOT be in progress (tracked->untracked is instant)",
                    isMigrationInProgress());

        // Inconsistency created after switch — mutations have no MutationIds
        insertWithInconsistency(3, 200, 10);

        long blockingBefore = getRepairedBlocking(1);
        long trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);

        // Read first range with point reads only — a table scan would cross-repair the
        // second range, preventing us from verifying BRR fires for both ranges independently.
        int readOps = 0;
        for (int i = 100; i < 110; i++)
        {
            CLUSTER.coordinator(1).execute(
                    "SELECT k, v FROM " + ksName + ".tbl WHERE k = ?",
                    ConsistencyLevel.ALL, i);
            readOps++;
        }

        // Second range via readAllKeys (includes point reads + table scan)
        readOps += readAllKeys(1, 200, 10);

        long blockingAfter = getRepairedBlocking(1);
        long trackedWriteAfter = getRepairedBlockingViaTrackedWrite(1);

        assertEquals("repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, blockingAfter);
        assertEquals("repairedBlockingViaTrackedWrite should NOT increment (fully untracked)",
                     trackedWriteBefore, trackedWriteAfter);
        assertDataOnAllNodes(100, 10);
        assertDataOnAllNodes(200, 10);
    }

    /**
     * Round trip: untracked → tracked → untracked.
     * Verify BRR uses the correct path at each stage.
     */
    @Test
    public void testRoundTripUntrackedToTrackedToUntracked()
    {
        createKeyspace(untracked);

        // Phase 1: BRR while untracked (classic path)
        insertWithInconsistency(3, 0, 5);
        long blockingBefore = getRepairedBlocking(1);
        long trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);
        int readOps = readAllKeys(1, 0, 5);
        assertEquals("Phase 1: repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, getRepairedBlocking(1));
        assertEquals("Phase 1: trackedWrite should NOT increment",
                     trackedWriteBefore, getRepairedBlockingViaTrackedWrite(1));
        assertDataOnAllNodes(0, 5);

        // Phase 2: Migrate to tracked, BRR during migration uses tracked writes
        insertWithInconsistency(3, 100, 5);
        alterKeyspace(tracked);
        assertTrue("Migration to tracked should be in progress", isMigrationInProgress());

        blockingBefore = getRepairedBlocking(1);
        trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);
        readOps = readAllKeys(1, 100, 5);
        assertEquals("Phase 2: repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, getRepairedBlocking(1));
        assertEquals("Phase 2: trackedWrite should increment for all repaired partitions",
                     trackedWriteBefore + 5, getRepairedBlockingViaTrackedWrite(1));
        assertDataOnAllNodes(100, 5);

        completeMigrationViaRepair();

        // Phase 3: Fully tracked — uses reconciliation, not BRR
        insertWithInconsistency(3, 200, 5);
        blockingBefore = getRepairedBlocking(1);
        long reconcileBefore = getTrackedReconcile(1);
        readOps = readAllKeys(1, 200, 5);
        assertEquals("Phase 3: repairedBlocking should NOT increment for tracked reads",
                     blockingBefore, getRepairedBlocking(1));
        assertEquals("Phase 3: trackedReconcile should increment for each tracked read",
                     reconcileBefore + readOps, getTrackedReconcile(1));

        // Phase 4: Migrate back to untracked — instant, no migration needed
        insertWithInconsistency(3, 300, 5);
        alterKeyspace(untracked);
        assertFalse("Migration to untracked should NOT be in progress (instant)", isMigrationInProgress());

        blockingBefore = getRepairedBlocking(1);
        trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);
        readOps = readAllKeys(1, 300, 5);
        assertEquals("Phase 4: repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, getRepairedBlocking(1));
        assertEquals("Phase 4: trackedWrite should NOT increment (fully untracked)",
                     trackedWriteBefore, getRepairedBlockingViaTrackedWrite(1));

        // Phase 5: Fully untracked again — classic BRR
        insertWithInconsistency(3, 400, 5);
        blockingBefore = getRepairedBlocking(1);
        trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);
        readOps = readAllKeys(1, 400, 5);
        assertEquals("Phase 5: repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, getRepairedBlocking(1));
        assertEquals("Phase 5: trackedWrite should NOT increment after migration complete",
                     trackedWriteBefore, getRepairedBlockingViaTrackedWrite(1));
        assertDataOnAllNodes(400, 5);
    }

    /**
     * Round trip: tracked → untracked → tracked.
     * Verify BRR uses the correct path at each stage.
     *
     * NOTE: The second completeMigrationViaRepair (untracked→tracked) is known to hit
     * "Could not find shard for logId" during SSTable flush when shards from the first
     * tracked phase were cleaned up. This is a pre-existing migration infrastructure issue.
     * This test therefore only verifies the BRR behavior up through the untracked phase
     * and does not attempt the second migration completion.
     */
    @Test
    public void testRoundTripTrackedToUntrackedToTracked()
    {
        createKeyspace(tracked);

        // Phase 1: Fully tracked — reconciliation, not BRR
        insertWithInconsistency(3, 0, 5);
        long blockingBefore = getRepairedBlocking(1);
        long reconcileBefore = getTrackedReconcile(1);
        int readOps = readAllKeys(1, 0, 5);
        assertEquals("Phase 1: repairedBlocking should NOT increment for tracked reads",
                     blockingBefore, getRepairedBlocking(1));
        assertEquals("Phase 1: trackedReconcile should increment for each tracked read",
                     reconcileBefore + readOps, getTrackedReconcile(1));

        // Phase 2: Switch to untracked — instant, no migration
        insertWithInconsistency(3, 100, 5);
        alterKeyspace(untracked);
        assertFalse("Migration to untracked should NOT be in progress (instant)", isMigrationInProgress());

        blockingBefore = getRepairedBlocking(1);
        long trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);
        readOps = readAllKeys(1, 100, 5);
        assertEquals("Phase 2: repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, getRepairedBlocking(1));
        assertEquals("Phase 2: trackedWrite should NOT increment (fully untracked)",
                     trackedWriteBefore, getRepairedBlockingViaTrackedWrite(1));

        // Phase 3: Fully untracked — classic BRR
        insertWithInconsistency(3, 200, 5);
        blockingBefore = getRepairedBlocking(1);
        trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);
        readOps = readAllKeys(1, 200, 5);
        assertEquals("Phase 3: repairedBlocking should increment for each BRR read command",
                     blockingBefore + readOps, getRepairedBlocking(1));
        assertEquals("Phase 3: trackedWrite should NOT increment for fully untracked",
                     trackedWriteBefore, getRepairedBlockingViaTrackedWrite(1));
        assertDataOnAllNodes(200, 5);

        // Phase 4: Start migration back to tracked, BRR uses tracked writes
        insertWithInconsistency(3, 300, 5);
        alterKeyspace(tracked);
        assertTrue("Migration to tracked should be in progress", isMigrationInProgress());

        // Verify node 3 is still missing the data after migration starts.
        // If this fails, something delivered the data between insertWithInconsistency
        // and the reads below (e.g. reconciler, delayed message, epoch-triggered sync).
        assertNodeMissingData(3, 300, 5);

        blockingBefore = getRepairedBlocking(1);
        trackedWriteBefore = getRepairedBlockingViaTrackedWrite(1);
        long reconcileBefore4 = getTrackedReconcile(1);
        readOps = readAllKeys(1, 300, 5);
        long blockingAfter = getRepairedBlocking(1);
        long trackedWriteAfter = getRepairedBlockingViaTrackedWrite(1);
        long reconcileAfter4 = getTrackedReconcile(1);
        assertEquals("Phase 4: repairedBlocking should increment for each BRR read command"
                     + " (trackedReconcile delta=" + (reconcileAfter4 - reconcileBefore4)
                     + ", trackedWrite delta=" + (trackedWriteAfter - trackedWriteBefore) + ")",
                     blockingBefore + readOps, blockingAfter);
        assertEquals("Phase 4: trackedWrite should increment for all repaired partitions",
                     trackedWriteBefore + 5, trackedWriteAfter);
        assertDataOnAllNodes(300, 5);

        // TODO: The second completeMigrationViaRepair (untracked→tracked) hits
        // "Could not find shard for logId" in MutationTrackingService.isDurablyReconciled()
        // during SSTable flush when shards from the first tracked phase were cleaned up
        // during the tracked→untracked migration. This is a pre-existing migration
        // infrastructure issue. Uncomment once fixed.
        //
        // Stack trace:
        //   java.lang.IllegalStateException: Could not find shard for logId 4294967300
        //     at o.a.c.replication.MutationTrackingService.isDurablyReconciled(MutationTrackingService.java:827)
        //     at o.a.c.io.sstable.format.SSTableWriter.finalizeMetadata(SSTableWriter.java:361)
        //     at o.a.c.io.sstable.format.SSTableWriter$TransactionalProxy.doPrepare(SSTableWriter.java:418)
        //     at o.a.c.db.ColumnFamilyStore$Flush.flushMemtable(ColumnFamilyStore.java:1348)
        //
//         // Complete migration to tracked
//         completeMigrationViaRepair();
//
//         // Phase 5: Fully tracked again — reconciliation, not BRR
//         insertWithInconsistency(3, 400, 5);
//         blockingBefore = getRepairedBlocking(1);
//         reconcileBefore = getTrackedReconcile(1);
//         readOps = readAllKeys(1, 400, 5);
//         assertEquals("Phase 5: repairedBlocking should NOT increment for tracked reads",
//                      blockingBefore, getRepairedBlocking(1));
//         assertEquals("Phase 5: trackedReconcile should increment for each tracked read",
//                      reconcileBefore + readOps + 1, getTrackedReconcile(1));
    }

    /**
     * Test that the CoordinatorBehindException retry path in BlockingReadRepair.startTrackedWriteAttempt
     * works correctly. When the coordinator is behind on epoch (still in migration state while
     * replicas have reverted to untracked), the tracked write read repair gets a CoordinatorBehindException
     * from the receiving node via checkReplicationMigration. The retry logic refetches ClusterMetadata,
     * sees that writes are no longer tracked, and falls back to classic READ_REPAIR_REQ.
     *
     * Setup:
     * 1. Create untracked keyspace, write consistent data
     * 2. Start migration to tracked — all nodes see migration in progress
     * 3. Pause node 3 (non-CMS-leader) before the next epoch
     * 4. ALTER KEYSPACE back to untracked from node 1 (CMS leader) — nodes 1, 2 advance
     * 5. Create inconsistency via executeInternal
     * 6. Unpause node 3, read from node 3 — BRR fires, node 3 still thinks migration in progress
     * 7. Tracked write carries old epoch; receiving node is now untracked → CoordinatorBehindException
     * 8. Retry logic refetches metadata, sees untracked, falls back to classic READ_REPAIR_REQ
     */
    @Ignore("https://issues.apache.org/jira/browse/CASSANDRA-21310")
    @Test
    public void testTrackedWriteReadRepairRetryOnCoordinatorBehind() throws Throwable
    {
        createKeyspace(untracked);
        insertConsistent(0, 5);

        // Start migration to tracked — all nodes see this epoch
        alterKeyspace(tracked);
        assertTrue("Migration should be in progress", isMigrationInProgress());

        // Pause node 3 before the next epoch — it will remain in migration-in-progress state.
        // Node 1 is the CMS leader and must stay operational for the ALTER to succeed.
        IInvokableInstance behindNode = CLUSTER.get(3);
        Epoch nextEpoch = getNextEpoch(behindNode);
        Callable<Void> paused = pauseBeforeEnacting(behindNode, nextEpoch);

        // ALTER KEYSPACE back to untracked from node 1 (CMS leader). Uses schemaChange(query, node)
        // which calls schemaChangeInternal directly, avoiding the schema agreement wait that would
        // hang because node 3 is paused.
        CLUSTER.schemaChange("ALTER KEYSPACE " + ksName + " WITH replication = " +
                REPLICATION + " AND replication_type='untracked'", 1);

        // Wait for the pause to trigger — node 3 has received the log entry but hasn't enacted
        paused.call();

        // Verify: node 3 is behind nodes 1, 2
        assertTrue("Node 3 should be behind node 1",
                   decode(CLUSTER.get(3).callOnInstance(() -> encode(ClusterMetadata.current().epoch)))
                       .isBefore(decode(CLUSTER.get(1).callOnInstance(() -> encode(ClusterMetadata.current().epoch)))));

        // Create inconsistency: write new values to nodes 1 and 3, leaving node 2 stale.
        String table = ksName + ".tbl";
        for (int i = 0; i < 5; i++)
        {
            int newValue = i + 100;
            CLUSTER.get(1).executeInternal("INSERT INTO " + table + " (k, v) VALUES (?, ?)", i, newValue);
            CLUSTER.get(3).executeInternal("INSERT INTO " + table + " (k, v) VALUES (?, ?)", i, newValue);
        }

        // Unpause node 3 so it can catch up during the retry after CoordinatorBehindException.
        unpauseEnactment(behindNode);

        // Read from node 3 (coordinator). Node 3 still thinks migration is in progress,
        // so BRR uses tracked writes. The tracked write carries node 3's old epoch;
        // nodes 1, 2 (at new epoch, untracked) see routing mismatch → CoordinatorBehindException.
        // Retry: node 3 catches up, sees untracked → falls back to classic READ_REPAIR_REQ.
        for (int i = 0; i < 5; i++)
        {
            CLUSTER.coordinator(3).execute(
                    "SELECT k, v FROM " + ksName + ".tbl WHERE k = ?",
                    ConsistencyLevel.ALL, i);
        }

        // TODO This doesn't actually validate the retry path was taken
        // Verify all nodes have the updated values
        for (int i = 0; i < 5; i++)
        {
            for (int node = 1; node <= CLUSTER.size(); node++)
            {
                Object[][] results = CLUSTER.get(node).executeInternal(
                        "SELECT v FROM " + table + " WHERE k = ?", i);
                assertEquals("Node " + node + " should have updated value for k=" + i,
                             i + 100, results[0][0]);
            }
        }
    }
}
