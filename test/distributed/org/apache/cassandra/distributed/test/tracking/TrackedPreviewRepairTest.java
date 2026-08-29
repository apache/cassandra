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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.Notification;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.journal.DeserializedRecordConsumer;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.MutationTrackingPreviewRepairTask;
import org.apache.cassandra.repair.RepairMessageVerbHandler;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TrackedPreviewRepairTest extends TestBaseImpl
{
    private static final int NODES = 3;

    private static final Consumer<IInstanceConfig> CONFIG = cfg -> cfg.with(Feature.NETWORK)
                                                                                    .with(Feature.GOSSIP)
                                                                                    .set("snapshot_on_repaired_data_mismatch", true);

    private static Cluster CLUSTER;
    private static final AtomicInteger UNIQUE_SUFFIX = new AtomicInteger();

    private String keyspace;
    private String table;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CLUSTER = Cluster.build(NODES).withConfig(CONFIG).withDataDirCount(1).withInstanceInitializer((cl, num) -> {
            PauseValidationRequest.install(cl, num);
            PauseEpochCheck.install(cl, num);
        }).start();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
        {
            CLUSTER.close();
            CLUSTER = null;
        }
    }

    @Before
    public void assignUniqueNames()
    {
        int suffix = UNIQUE_SUFFIX.incrementAndGet();
        keyspace = "preview_ks_" + suffix;
        table = "tbl_" + suffix;

        // The mutation journal is shared across every keyspace/table on a node.
        for (IInvokableInstance instance : CLUSTER)
            instance.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());
    }

    @Test
    public void routesToTrackedPathAndReportsInSync()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

        assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);

        assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);
    }

    @Test
    public void reportsInSyncWithFlushedReconciledData()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        // Everything reconciled before the offset ends up in SSTables, not the journal.
        flushAll(CLUSTER, keyspace, table);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

        assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
        assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);
    }

    @Test
    public void reportsInSyncWithMixedFlushedAndJournalData()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        // First batch: flushed, lives in SSTables.
        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);
        flushAll(CLUSTER, keyspace, table);

        // Second batch: left in journal, not flushed.
        writeRows(CLUSTER, keyspace, table, 10, 20);

        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

        assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
        assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);
    }

    @Test
    public void validatesJournalOnlyDataAndReportsInSync()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        for (IInvokableInstance instance : CLUSTER)
            assertThat(countLiveSSTables(instance, keyspace, table)).as("node %d should have no live SSTables -- everything must stay journal-resident", instance.config().num()).isZero();

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        IInvokableInstance coordinator = CLUSTER.get(1);
        NodeToolResult result = coordinator.nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);
        assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
        assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);

        assertThat(totalValidatedSSTables(coordinator, keyspace, table)).as("there are no SSTables in this scenario, so none should have been validated").isZero();
        assertThat(totalValidatedJournalPartitions(coordinator, keyspace, table)).as("the journal stream should have validated the reconciled batch").isGreaterThan(0);
        assertThat(bytesPreviewedOnInstance(coordinator, keyspace, table)).as("bytesPreviewed must reflect the journal-only data even though there's no SSTable byte range to measure").isGreaterThan(0);
    }

    @Test
    public void excludesJournalWritesIssuedAfterOffsetIsEstablished() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        IInvokableInstance coordinator = CLUSTER.get(1);
        IInvokableInstance node2 = CLUSTER.get(2);
        node2.runOnInstance(PauseValidationRequest::arm);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            node2.runOnInstance(PauseValidationRequest::awaitArrival);

            // Writes issued at CL.ALL after the offset is established land in every replica's
            // journal, including node 2's, while node 2 is paused before it reads anything for
            // the validation.
            writeRows(CLUSTER, keyspace, table, 10, 20);

            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertNotificationContains(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

            assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
            assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);

            // Node 2's journal has both batches by the time it reads (release happened
            // after the second batch was written), but only batch1's 10 partitions should have
            // been validated.
            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).as("the batch written after the offset must be excluded despite existing at read time").isEqualTo(10);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void detectsDivergenceInReconciledSSTable()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        // Force everything to disk so the diverged data lives in an SSTable, not the journal.
        flushAll(CLUSTER, keyspace, table);

        // Drop one SSTable on node 2 so its merkle tree diverges from nodes 1 and 3.
        dropOneSSTableOnReplica(CLUSTER.get(2), keyspace, table);

        // The journal isn't pruned just because its data was flushed. Node 2's journal
        // still has an intact copy of the dropped SSTable's content, which would otherwise
        // mask the divergence via the validation's journal stream. Wipe it so the drop above is
        // the only surviving copy of that data on node 2.
        CLUSTER.get(2).runOnInstance(() -> MutationJournal.instance().truncateForTesting());

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);
        long desyncBefore = desyncRangesOnCoordinator(CLUSTER, 1, keyspace, table);
        long desyncBytesBefore = desyncBytesOnCoordinator(CLUSTER, 1, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);

        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);

        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDesyncBytesBumped(CLUSTER, 1, keyspace, table, desyncBytesBefore);

        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 2, 3);
    }

    @Test
    public void detectsDivergenceInReconciledSSTableOnCoordinator()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        // Force everything to disk so the diverged data lives in an SSTable, not the journal.
        flushAll(CLUSTER, keyspace, table);

        // Drop one SSTable on node 1 (the coordinator) so its merkle tree diverges from nodes 2 and 3.
        dropOneSSTableOnReplica(CLUSTER.get(1), keyspace, table);

        // The journal isn't pruned just because its data was flushed, so wipe it to make the drop above the only
        // surviving copy of that data on node 1.
        CLUSTER.get(1).runOnInstance(() -> MutationJournal.instance().truncateForTesting());

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);
        long desyncBefore = desyncRangesOnCoordinator(CLUSTER, 1, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);
        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 2, 3);
    }

    @Test
    public void detectsCellDivergenceInReconciledSSTable()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);
        flushAll(CLUSTER, keyspace, table);

        injectSSTableCellDivergence(CLUSTER.get(2), keyspace, table, 3, 999);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);
        long desyncBefore = desyncRangesOnCoordinator(CLUSTER, 1, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);
        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 2, 3);
    }

    @Test
    public void validatesMultipleTablesIndependently()
    {
        String tableA = table;
        String tableB = table + "_b";

        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + tableA + " (k int PRIMARY KEY, v int)");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + tableB + " (k int PRIMARY KEY, v int)");

        writeRows(CLUSTER, keyspace, tableA, 0, 10);
        writeRows(CLUSTER, keyspace, tableB, 0, 10);

        settleReconciliation(CLUSTER);
        flushAll(CLUSTER, keyspace, tableA);
        flushAll(CLUSTER, keyspace, tableB);

        // Only table A diverges; table B stays identical on every replica.
        injectSSTableCellDivergence(CLUSTER.get(2), keyspace, tableA, 3, 999);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);
        long desyncBeforeA = desyncRangesOnCoordinator(CLUSTER, 1, keyspace, tableA);
        long desyncBeforeB = desyncRangesOnCoordinator(CLUSTER, 1, keyspace, tableB);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);

        assertDesyncRangesBumped(CLUSTER, 1, keyspace, tableA, desyncBeforeA);
        assertThat(desyncRangesOnCoordinator(CLUSTER, 1, keyspace, tableB)).isEqualTo(desyncBeforeB);

        assertDiagnosticSnapshotExists(CLUSTER, keyspace, tableA, 1, 2, 3);
        assertNoDiagnosticSnapshot(CLUSTER, keyspace, tableB);
    }

    @Test
    public void detectsDivergenceInReconciledJournalEntry()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        // Reconciled mutations must stay in the journal so the injection can rewrite an existing journal entry's 
        // payload rather than touching an SSTable.
        injectJournalPayloadDivergence(CLUSTER.get(2), keyspace, table, 3, 999);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);
        long desyncBefore = desyncRangesOnCoordinator(CLUSTER, 1, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);
        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 2, 3);
    }

    @Test
    public void excludesSSTablesFlushedAfterOffsetEstablishment() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        // Initial reconciled writes, flushed to SSTables before validation runs so the
        // validation has non-empty state to compare across replicas. Without this pre-flush
        // the validation would run against empty SSTable sets and the test would be a
        // trivial pass regardless of whether the SSTable created after the offset was included.
        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);
        flushAll(CLUSTER, keyspace, table);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        IInvokableInstance coordinator = CLUSTER.get(1);
        IInvokableInstance node2 = CLUSTER.get(2);
        node2.runOnInstance(PauseValidationRequest::arm);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            // Block until node 2 has received the validation's MT_VALIDATION_REQ and is
            // paused before reading its SSTable set. By this point the offset is necessarily
            // established (the validation only dispatches after OFFSETS_ESTABLISHED).
            node2.runOnInstance(PauseValidationRequest::awaitArrival);

            // Writes issued at CL.ALL after the offset is established land in every replica's memtable.
            writeRows(CLUSTER, keyspace, table, 10, 20);

            // Flush only on node 2, while its validation request handling is paused. Nodes 1
            // and 3 keep everything in memtable/journal, so node 2 is the only replica with
            // an SSTable created after the offset. Its coordinatorLogOffsets are not covered
            // by the offset, so the validation must exclude it once released. If it doesn't,
            // node 2's tree diverges from the empty trees on nodes 1 and 3 and the validation
            // falsely reports inconsistent.
            node2.nodetoolResult("flush", keyspace, table).asserts().success();

            // Release node 2 -- it will now read its SSTable set, which includes the
            // SSTable it just flushed after the offset.
            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertNotificationContains(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

            assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
            assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);

            // Directly verify what the validation actually validated on node 2, not just its
            // final verdict. If the offset filter were broken in a way that excludes
            // everything (not just data written after the offset), the notification checks above would
            // still say "in sync" vacuously.
            int baselineValidated = totalValidatedSSTables(coordinator, keyspace, table);
            int node2Validated = totalValidatedSSTables(node2, keyspace, table);
            assertThat(node2Validated).as("node 2 should have validated the same SSTable count as node 1, despite extra unreconciled SSTables on disk").isEqualTo(baselineValidated);
            assertThat(baselineValidated).as("sanity: the validation should have validated a non-zero number of SSTables").isGreaterThan(0);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void validatesReconciledDataViaJournalWhenSSTableStraddlesOffsetBoundary() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);
        
        // First batch stays reconciled but journal-resident, so it's still around to be caught up
        // in the same flush as batch2 below.

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        IInvokableInstance coordinator = CLUSTER.get(1);
        IInvokableInstance node2 = CLUSTER.get(2);
        node2.runOnInstance(PauseValidationRequest::arm);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            node2.runOnInstance(PauseValidationRequest::awaitArrival);

            writeRows(CLUSTER, keyspace, table, 10, 20);

            // Combines reconciled first batch and unreconciled batch2 into one straddling SSTable.
            node2.nodetoolResult("flush", keyspace, table).asserts().success();

            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertNotificationContains(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);
            assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
            assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);

            // The straddling SSTable is live on disk but excluded wholesale from the
            // SSTable stream; the reconciled first batch portion is still validated, via the
            // journal stream instead.
            assertThat(countLiveSSTables(node2, keyspace, table)).as("node 2 should have flushed the straddling SSTable").isGreaterThan(0);
            assertThat(totalValidatedSSTables(node2, keyspace, table)).as("the straddling SSTable is excluded wholesale from the SSTable stream").isZero();
            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).as("the reconciled batch1 portion should still be validated via the journal stream").isGreaterThan(0);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void falselyFlagsDivergenceWhenCompactionStraddlesReconciledData() throws Exception
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}");

        // Autocompaction must not run here: it could re-trigger finalizeMetadata() on its own schedule.
        for (int i = 1; i <= NODES; i++)
            CLUSTER.get(i).nodetoolResult("disableautocompaction", keyspace, table).asserts().success();

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);
        flushAll(CLUSTER, keyspace, table);

        IInvokableInstance coordinator = CLUSTER.get(1);
        IInvokableInstance node2 = CLUSTER.get(2);

        // Force the SSTable from the batch written before the offset back to unrepaired.
        String ks = keyspace;
        String tbl = table;
        node2.runOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(ks, tbl);
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try
                {
                    sstable.mutateRepairedAndReload(ActiveRepairService.UNREPAIRED_SSTABLE, ActiveRepairService.NO_PENDING_REPAIR);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            // mutateRepairedAndReload only rewrites the SSTable's own metadata; without this,
            // CompactionStrategyManager's repaired/unrepaired holder assignment (cached from
            // when the sstable was first tracked, already repaired) stays stale.
            cfs.getTracker().notifySSTableRepairedStatusChanged(cfs.getLiveSSTables());
        });
        node2.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());
        int staticSegmentsBeforeDrop = node2.callOnInstance(() -> MutationJournal.instance().countStaticSegmentsForTesting());
        node2.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting(true));
        int staticSegmentsAfterDrop = node2.callOnInstance(() -> MutationJournal.instance().countStaticSegmentsForTesting());
        assertThat(staticSegmentsAfterDrop).as("static journal segments on node 2 before/after the real drop attempt: %d -> %d", staticSegmentsBeforeDrop, staticSegmentsAfterDrop).isLessThan(staticSegmentsBeforeDrop);

        node2.runOnInstance(PauseValidationRequest::arm);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            node2.runOnInstance(PauseValidationRequest::awaitArrival);

            writeRows(CLUSTER, keyspace, table, 10, 20);

            // Only on node 2, flush the batch written after the offset to its own SSTable,
            // then major-compact so it merges with the clean SSTable from the batch written
            // before the offset, so the result straddles the offset.
            node2.nodetoolResult("flush", keyspace, table).asserts().success();
            node2.nodetoolResult("compact", keyspace, table).asserts().success();
            assertThat(countLiveSSTables(node2, keyspace, table)).isEqualTo(1);

            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            // Currently fails here: the real drop above already succeeded, so the journal
            // has nothing either, and this comes back "Repaired data is inconsistent".
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

            // The straddling SSTable stays correctly excluded either way.
            assertThat(totalValidatedSSTables(node2, keyspace, table)).isZero();
            // But the batch written before the offset should still be found via the journal,
            // once the pending patch refuses the drop above.
            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).isEqualTo(10);

            // Node 1 still sees the batch written before the offset via its own untouched, clean SSTable.
            assertThat(totalValidatedSSTables(coordinator, keyspace, table)).isEqualTo(1);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void excludesCorruptedJournalEntriesWrittenAfterOffsetEstablishment() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        IInvokableInstance coordinator = CLUSTER.get(1);
        IInvokableInstance node2 = CLUSTER.get(2);
        node2.runOnInstance(PauseValidationRequest::arm);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            node2.runOnInstance(PauseValidationRequest::awaitArrival);

            // Writes issued at CL.ALL after the offset is established: new mutations land in
            // every replica's journal, past the offset.
            writeRows(CLUSTER, keyspace, table, 10, 20);

            // No flush; stays journal-resident. Corrupt it while node 2 is still paused,
            // guaranteeing it exists by the time node 2 actually reads its journal.
            injectJournalPayloadDivergence(node2, keyspace, table, 15, 999);

            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertNotificationContains(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

            assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
            assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);

            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).as("the corrupted entry written after the offset must be excluded despite existing at read time").isEqualTo(10);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDisarm);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void rejectsWhenKeyspaceIsMigrating()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='untracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int)");
        CLUSTER.schemaChange("ALTER KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");

        String ks = keyspace;
        boolean isMigrating = CLUSTER.get(1).callOnInstance(() -> ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(ks));
        assertTrue("Expected keyspace " + keyspace + " to be in mutation tracking migration state", isMigrating);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().failure().errorContains("migration");

        assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
    }

    @Test
    public void failsFastWhenParticipantIsAlreadyDown() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        CLUSTER.get(2).shutdown().get();

        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
            result.asserts().failure().errorContains("not alive");

            assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
            assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
            assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.DISPATCHING_VALIDATION);
        }
        finally
        {
            CLUSTER.get(2).startup();
        }
    }

    @Test
    public void failsWhenTopologyChangesBetweenOffsetEstablishmentAndDispatch() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        IInvokableInstance coordinator = CLUSTER.get(1);
        coordinator.runOnInstance(PauseEpochCheck::arm);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            coordinator.runOnInstance(PauseEpochCheck::awaitArrival);

            CLUSTER.schemaChange("CREATE TABLE " + keyspace + ".epoch_bump (k int PRIMARY KEY)");

            coordinator.runOnInstance(PauseEpochCheck::releaseAndDisarm);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().failure().errorContains("topology changed");

            assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
            assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.DISPATCHING_VALIDATION);
            assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
            assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);
        }
        finally
        {
            coordinator.runOnInstance(PauseEpochCheck::releaseAndDisarm);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void rejectsUnrepairedOnTrackedKeyspace()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--preview", keyspace);
        result.asserts().failure().errorContains("tracked");

        assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
    }

    @Test
    public void rejectsAllOnTrackedKeyspace()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--full", "--preview", keyspace);
        result.asserts().failure().errorContains("tracked");

        assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
    }

    @Test
    public void rejectsFullValidateOnTrackedKeyspace()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--full", "--validate", keyspace);
        result.asserts().failure().errorContains("tracked");

        assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.ESTABLISHING_OFFSETS);
    }

    private static void createTrackedSchema(Cluster cluster, String keyspace, String table)
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        cluster.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int)");
    }

    private static void writeRows(Cluster cluster, String keyspace, String table, int fromInclusive, int toExclusive)
    {
        for (int i = fromInclusive; i < toExclusive; i++)
            cluster.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, i, i);
    }

    private static void settleReconciliation(Cluster cluster)
    {
        cluster.forEach(i -> i.runOnInstance(() ->{
            MutationTrackingService.instance().persistLogStateForTesting();
            MutationTrackingService.instance().broadcastOffsetsForTesting();
        }));

        // Each replica's own unreconciled-mutation count only drops to zero once it has
        // received (via broadcastOffsetsForTesting above) confirmation that every other live
        // participant has also witnessed everything it wrote.
        for (IInvokableInstance instance : cluster)
            await().atMost(10, TimeUnit.SECONDS)
                   .pollInterval(50, TimeUnit.MILLISECONDS)
                   .until(() -> instance.callOnInstance(() -> MutationTrackingService.instance().getUnreconciledMutationCount()) == 0);
    }

    private static void flushAll(Cluster cluster, String keyspace, String table)
    {
        cluster.forEach(i -> i.nodetoolResult("flush", keyspace, table).asserts().success());
    }

    private static Future<NodeToolResult> submitValidate(ExecutorService executor, IInvokableInstance coordinator, String keyspace)
    {
        return executor.submit(() -> coordinator.nodetoolResult(true, "repair", "--validate", keyspace));
    }

    private static void dropOneSSTableOnReplica(IInvokableInstance instance, String keyspace, String table)
    {
        instance.runOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            if (cfs == null)
                throw new AssertionError("No CFS for " + keyspace + '.' + table + " on this instance");
            Set<SSTableReader> live = cfs.getLiveSSTables();
            if (live.isEmpty())
                throw new AssertionError("No live SSTables for " + keyspace + '.' + table + " on this instance");
            SSTableReader victim = live.iterator().next();
            cfs.markObsolete(java.util.Collections.singleton(victim), OperationType.UNKNOWN);
        });
    }

    private static void injectSSTableCellDivergence(IInvokableInstance instance, String keyspace, String table, int key, int divergentValue)
    {
        Set<String> liveBefore = instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            Set<String> paths = new java.util.HashSet<>();
            if (cfs != null)
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    paths.add(sstable.descriptor.baseFile().toString());
            return paths;
        });

        instance.runOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            PartitionUpdate update = new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), key)
                                          .add("v", divergentValue)
                                          .buildUpdate();
            try (OpOrder.Group group = Keyspace.writeOrder.start())
            {
                cfs.apply(MutationId.none(), update, new CassandraWriteContext(group, null), true);
            }
        });
        instance.nodetoolResult("flush", keyspace, table).asserts().success();

        instance.runOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            SSTableReader original = null;
            SSTableReader divergent = null;
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                if (liveBefore.contains(sstable.descriptor.baseFile().toString()))
                    original = sstable;
                else if (divergent == null)
                    divergent = sstable;
                else
                    throw new AssertionError("Expected exactly one newly-flushed SSTable for " + keyspace + '.' + table);
            }
            if (original == null || divergent == null)
                throw new AssertionError("Expected both a pre-existing and a newly-flushed SSTable for " + keyspace + '.' + table);
            try
            {
                divergent.mutateCoordinatorLogOffsetsAndReload(original.getCoordinatorLogOffsets());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private static int countLiveSSTables(IInvokableInstance instance, String keyspace, String table)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            return cfs == null ? 0 : cfs.getLiveSSTables().size();
        });
    }

    private static int totalValidatedSSTables(IInvokableInstance instance, String keyspace, String table)
    {
        Pattern pattern = Pattern.compile("Performing validation compaction on (\\d+) sstables in " + Pattern.quote(keyspace + '.' + table));
        List<String> lines = instance.logs().grep(pattern).getResult();
        int total = 0;
        for (String line : lines)
        {
            Matcher m = pattern.matcher(line);
            if (m.find())
                total += Integer.parseInt(m.group(1));
        }
        return total;
    }

    private static int totalValidatedJournalPartitions(IInvokableInstance instance, String keyspace, String table)
    {
        Pattern pattern = Pattern.compile("Performing journal validation on (\\d+) partitions \\(\\d+ bytes\\) in " + Pattern.quote(keyspace + '.' + table));
        List<String> lines = instance.logs().grep(pattern).getResult();
        int total = 0;
        for (String line : lines)
        {
            Matcher m = pattern.matcher(line);
            if (m.find())
                total += Integer.parseInt(m.group(1));
        }
        return total;
    }

    private static long bytesPreviewedOnInstance(IInvokableInstance instance, String keyspace, String table)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            return cfs == null ? 0L : cfs.metric.bytesPreviewed.table.getCount();
        });
    }

    private static void injectJournalPayloadDivergence(IInvokableInstance instance, String keyspace, String table, int key, int divergentValue)
    {
        instance.runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
            TableId tableId = metadata.id;
            DecoratedKey targetKey = metadata.partitioner.decorateKey(Int32Type.instance.decompose(key));

            ShortMutationId[] found = new ShortMutationId[1];
            try (MutationJournal.Snapshot snapshot = MutationJournal.instance().snapshot())
            {
                snapshot.readAll(new DeserializedRecordConsumer<ShortMutationId, Mutation>(MutationJournal.MutationSerializer.INSTANCE)
                {
                    @Override
                    protected void accept(long segment, int position, ShortMutationId id, Mutation mutation)
                    {
                        PartitionUpdate update = mutation.modifications().get(tableId);
                        if (update != null && update.partitionKey().equals(targetKey))
                            found[0] = id;
                    }
                });
            }

            if (found[0] == null)
                throw new AssertionError("No journal entry found for " + keyspace + '.' + table + " key=" + key);

            Mutation divergent = new RowUpdateBuilder(metadata, FBUtilities.timestampMicros(), key).add("v", divergentValue).build();

            MutationJournal.instance().advanceSegment();
            MutationJournal.instance().write(found[0], divergent);
        });
    }

    private static void assertNotificationContains(NodeToolResult result, String expectedFragment)
    {
        List<Notification> notifications = result.getNotifications();
        for (Notification n : notifications)
        {
            String message = n.getMessage();
            if (message != null && message.contains(expectedFragment))
                return;
        }

        StringBuilder found = new StringBuilder();
        for (Notification n : notifications)
        {
            if (n.getMessage() != null)
                found.append("\n  - ").append(n.getMessage());
        }
        fail("Expected notification containing \"" + expectedFragment + "\" but found:" + found);
    }

    private static void assertNotificationAbsent(NodeToolResult result, String unexpectedFragment)
    {
        for (Notification n : result.getNotifications())
        {
            String message = n.getMessage();
            if (message != null && message.contains(unexpectedFragment))
                fail("Did not expect notification containing \"" + unexpectedFragment + "\" but found: " + message);
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    private static long[] previewFailuresPerNode(Cluster cluster)
    {
        long[] counts = new long[cluster.size()];
        for (int i = 0; i < cluster.size(); i++)
        {
            counts[i] = cluster.get(i + 1).callOnInstance(() -> RepairMetrics.previewFailures.getCount());
        }
        return counts;
    }

    private static void assertPreviewFailuresUnchanged(Cluster cluster, long[] baseline)
    {
        long[] after = previewFailuresPerNode(cluster);
        for (int i = 0; i < after.length; i++)
            assertThat(after[i]).as("RepairMetrics.previewFailures on node %d", i + 1).isEqualTo(baseline[i]);
    }

    private static void assertPreviewFailuresBumpedOnCoordinator(Cluster cluster, long[] baseline, int coordinatorNode)
    {
        long[] after = previewFailuresPerNode(cluster);
        for (int i = 0; i < after.length; i++)
        {
            long expectedDelta = (i + 1 == coordinatorNode) ? 1 : 0;
            assertThat(after[i]).as("RepairMetrics.previewFailures on node %d", i + 1).isEqualTo(baseline[i] + expectedDelta);
        }
    }

    private static long desyncRangesOnCoordinator(Cluster cluster, int coordinatorNode, String keyspace, String table)
    {
        return cluster.get(coordinatorNode).callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            TableMetrics metric = cfs.metric;
            return metric.tokenRangesPreviewedDesynchronized.table.getCount();
        });
    }

    private static void assertDesyncRangesBumped(Cluster cluster, int coordinatorNode, String keyspace, String table, long baseline)
    {
        long after = desyncRangesOnCoordinator(cluster, coordinatorNode, keyspace, table);
        assertThat(after).as("TableMetrics.tokenRangesPreviewedDesynchronized on node %d", coordinatorNode).isGreaterThan(baseline);
    }

    private static long desyncBytesOnCoordinator(Cluster cluster, int coordinatorNode, String keyspace, String table)
    {
        return cluster.get(coordinatorNode).callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
            TableMetrics metric = cfs.metric;
            return metric.bytesPreviewedDesynchronized.table.getCount();
        });
    }

    private static void assertDesyncBytesBumped(Cluster cluster, int coordinatorNode, String keyspace, String table, long baseline)
    {
        long after = desyncBytesOnCoordinator(cluster, coordinatorNode, keyspace, table);
        assertThat(after).as("TableMetrics.bytesPreviewedDesynchronized on node %d", coordinatorNode).isGreaterThan(baseline);
    }

    private static int countDiagnosticSnapshots(IInvokableInstance instance, String keyspace, String table)
    {
        return instance.callOnInstance(() -> SnapshotManager.instance.getSnapshots(snapshot ->
            snapshot.getKeyspaceName().equals(keyspace)
            && snapshot.getTableName().equals(table)
            && snapshot.getTag().startsWith(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX)).size());
    }

    private static void assertNoDiagnosticSnapshot(Cluster cluster, String keyspace, String table)
    {
        for (IInvokableInstance instance : cluster)
        {
            int count = countDiagnosticSnapshots(instance, keyspace, table);
            assertThat(count).as("Unexpected diagnostic snapshot(s) for %s.%s on node %d", keyspace, table, instance.config().num()).isZero();
        }
    }

    private static void assertDiagnosticSnapshotExists(Cluster cluster, String keyspace, String table, int... expectedNodes)
    {
        for (int nodeNum : expectedNodes)
        {
            IInvokableInstance instance = cluster.get(nodeNum);
            // DiagnosticSnapshotService fans out SNAPSHOT_REQ messages that are handled
            // asynchronously on each recipient; snapshot creation queues on an executor.
            // Poll until the snapshot appears rather than reading once.
            await().atMost(30, TimeUnit.SECONDS)
                   .pollInterval(200, TimeUnit.MILLISECONDS)
                   .untilAsserted(() -> assertThat(countDiagnosticSnapshots(instance, keyspace, table)).as("Expected diagnostic snapshot for %s.%s on node %d", keyspace, table, nodeNum)
                                                                                                       .isGreaterThan(0));
        }
    }

    public static class PauseValidationRequest
    {
        private static volatile CountDownLatch arrived;
        private static volatile CountDownLatch release;

        @SuppressWarnings("resource")
        public static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(RepairMessageVerbHandler.class)
                           .method(named("doVerb"))
                           .intercept(MethodDelegation.to(PauseValidationRequest.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void arm()
        {
            arrived = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        public static void awaitArrival()
        {
            try
            {
                if (!arrived.await(60, TimeUnit.SECONDS))
                    throw new AssertionError("Timed out waiting for MT_VALIDATION_REQ to arrive");
            }
            catch (InterruptedException e)
            {
                throw new AssertionError("Interrupted while waiting for MT_VALIDATION_REQ to arrive");
            }
        }

        public static void releaseAndDisarm()
        {
            if (release != null)
                release.countDown();
            arrived = null;
            release = null;
        }

        @SuppressWarnings("unused")
        public static void doVerb(Message<RepairMessage> message, @SuperCall Callable<?> zuper)
        {
            CountDownLatch localArrived = arrived;
            CountDownLatch localRelease = release;
            if (localArrived != null && message.verb() == Verb.MT_VALIDATION_REQ)
            {
                localArrived.countDown();
                try
                {
                    if (!localRelease.await(60, TimeUnit.SECONDS))
                        throw new AssertionError("Timed out waiting to be released after pausing MT_VALIDATION_REQ");
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError("Interrupted while paused on MT_VALIDATION_REQ", e);
                }
            }
            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class PauseEpochCheck
    {
        private static volatile CountDownLatch arrived;
        private static volatile CountDownLatch release;

        @SuppressWarnings("resource")
        public static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(MutationTrackingPreviewRepairTask.class)
                           .method(named("epochChanged"))
                           .intercept(MethodDelegation.to(PauseEpochCheck.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void arm()
        {
            arrived = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        public static void awaitArrival()
        {
            try
            {
                if (!arrived.await(60, TimeUnit.SECONDS))
                    throw new AssertionError("Timed out waiting for epochChanged to be called");
            }
            catch (InterruptedException e)
            {
                throw new AssertionError("Interrupted while waiting for epochChanged to be called");
            }
        }

        public static void releaseAndDisarm()
        {
            if (release != null)
                release.countDown();
            arrived = null;
            release = null;
        }

        @SuppressWarnings("unused")
        public static boolean epochChanged(Epoch epochAtStart, @SuperCall Callable<Boolean> zuper)
        {
            CountDownLatch localArrived = arrived;
            CountDownLatch localRelease = release;
            if (localArrived != null)
            {
                localArrived.countDown();
                try
                {
                    if (!localRelease.await(60, TimeUnit.SECONDS))
                        throw new AssertionError("Timed out waiting to be released after pausing epochChanged");
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError("Interrupted while paused on epochChanged", e);
                }
            }
            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
