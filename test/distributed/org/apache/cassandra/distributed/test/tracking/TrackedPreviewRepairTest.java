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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
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
import net.bytebuddy.implementation.bind.annotation.This;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LogDomain;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.journal.DeserializedRecordConsumer;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.MutationTrackingPreviewRepairTask;
import org.apache.cassandra.repair.MutationTrackingValidationCoordinator;
import org.apache.cassandra.repair.RepairMessageVerbHandler;
import org.apache.cassandra.repair.SharedContext;
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
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.settleReconciliation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TrackedPreviewRepairTest extends TestBaseImpl
{
    private static final int NODES = 3;

    private static final Consumer<IInstanceConfig> CONFIG = cfg -> cfg.with(Feature.NETWORK)
                                                                                    .with(Feature.GOSSIP)
                                                                                    .set("snapshot_on_repaired_data_mismatch", true)
                                                                                    .set("transient_replication_enabled", true);

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
            RaceValidationFailures.install(cl, num);
            ThrowOnTreeDifference.install(cl, num);
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

        assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);
    }

    @Test
    public void reportsInSyncWithWitnessReplicas()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int)");

        writeRows(CLUSTER, keyspace, table, 0, 10);

        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", "-pr", keyspace);
        result.asserts().success();

        assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);

        // Verify that the witness replica was excluded from validation and validated no data.
        assertThat(totalValidatedSSTables(CLUSTER.get(3), keyspace, table)).as("Witness replica must not validate SSTables").isZero();
        assertThat(totalValidatedJournalPartitions(CLUSTER.get(3), keyspace, table)).as("Witness replica must not validate journal partitions").isZero();
    }

    @Test
    public void testFullRepairWithWitnessReplicas()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int)");

        int witnessNode = 3;
        int fullNode1 = 1;
        int fullNode2 = 2;

        List<Integer> keys = findKeysWithWitness(CLUSTER, keyspace, table, witnessNode, 3);
        try
        {
            // Drop writes and reconciler messages to ensure mutations only exist in the witness journal.
            CLUSTER.filters().verbs(Verb.MUTATION_REQ.id).from(witnessNode).to(fullNode1, fullNode2).drop();
            CLUSTER.filters().verbs(Verb.MT_BROADCAST_LOG_OFFSETS.id).from(witnessNode).to(fullNode1, fullNode2).drop();
            CLUSTER.filters().verbs(Verb.MT_PUSH_MUTATION_REQ.id).drop();

            for (int key : keys)
            {
                try
                {
                    CLUSTER.coordinator(witnessNode).execute("INSERT INTO " + keyspace + '.' + table + " (k, v) VALUES (?, ?)",
                                                            ConsistencyLevel.ALL, key, key * 10);
                    fail("Expected write to time out because full replicas are unreachable");
                }
                catch (Throwable t)
                {
                    // Expected write timeout because full replicas dropped the mutation
                }
            }

            // Verify full replicas and witness have no table data before repair.
            for (int key : keys)
            {
                assertThat(CLUSTER.get(fullNode1).executeInternal("SELECT v FROM " + keyspace + '.' + table + " WHERE k = ?", key)).isEmpty();
                assertThat(CLUSTER.get(fullNode2).executeInternal("SELECT v FROM " + keyspace + '.' + table + " WHERE k = ?", key)).isEmpty();
                assertThat(CLUSTER.get(witnessNode).executeInternal("SELECT v FROM " + keyspace + '.' + table + " WHERE k = ?", key)).isEmpty();
            }
        }
        finally
        {
            CLUSTER.filters().reset();
        }

        // Full repair must synchronize witness journal entries to full replicas before anti-entropy validation.
        NodeToolResult result = CLUSTER.get(fullNode1).nodetoolResult(true, "repair", "--full", keyspace);
        result.asserts().success();
        assertNotificationContains(result, "Started mutation tracking sync before full repair");
        assertNotificationContains(result, "Mutation tracking sync completed; running full repair");

        for (int key : keys)
        {
            int expectedVal = key * 10;
            assertThat(CLUSTER.get(fullNode1).executeInternal("SELECT v FROM " + keyspace + '.' + table + " WHERE k = ?", key))
                .isEqualTo(new Object[][] { new Object[] { expectedVal } });
            assertThat(CLUSTER.get(fullNode2).executeInternal("SELECT v FROM " + keyspace + '.' + table + " WHERE k = ?", key))
                .isEqualTo(new Object[][] { new Object[] { expectedVal } });
            assertThat(CLUSTER.get(witnessNode).executeInternal("SELECT v FROM " + keyspace + '.' + table + " WHERE k = ?", key)).isEmpty();

            Object[][] coordRows = CLUSTER.coordinator(fullNode1).execute("SELECT v FROM " + keyspace + '.' + table + " WHERE k = ?",
                                                                          ConsistencyLevel.ALL, key);
            assertThat(coordRows).isEqualTo(new Object[][] { new Object[] { expectedVal } });
        }
    }

    private static List<Integer> findKeysWithWitness(Cluster cluster, String keyspace, String table, int witnessNode, int count)
    {
        List<Integer> keys = new ArrayList<>();
        for (int k = 0; k < 1000 && keys.size() < count; k++)
        {
            final int candidateKey = k;
            boolean isTransient = cluster.get(witnessNode).callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                Token token = Schema.instance.getTableMetadata(keyspace, table)
                                             .partitioner.getToken(Int32Type.instance.decompose(candidateKey));
                EndpointsForToken endpoints = metadata.placements.get(Schema.instance.getKeyspaceMetadata(keyspace).params.replication)
                                                                 .reads.forToken(token).get();
                boolean transientMatch = false;
                int fullCount = 0;
                for (Replica replica : endpoints)
                {
                    if (replica.isTransient() && replica.endpoint().equals(FBUtilities.getBroadcastAddressAndPort()))
                        transientMatch = true;
                    if (replica.isFull())
                        fullCount++;
                }
                return transientMatch && fullCount == 2;
            });
            if (isTransient)
                keys.add(candidateKey);
        }
        assertThat(keys).as("Could not find enough keys with witness on node %d", witnessNode).hasSize(count);
        return keys;
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

        assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);
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

        assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);
    }

    @Test
    public void validatesJournalOnlyBatchWhenMultipleSSTablesCoverEarlierOffsets()
    {
        createTrackedSchema(CLUSTER, keyspace, table);
        disableAutocompaction(CLUSTER, keyspace, table);

        // flushed to its own SSTable covering an earlier sub-range of this node's log
        writeRows(CLUSTER, keyspace, table, 0, 10);
        flushAll(CLUSTER, keyspace, table);
        settleReconciliation(CLUSTER);

        // flushed to a separate SSTable covering a later sub-range of the same log
        writeRows(CLUSTER, keyspace, table, 10, 20);
        flushAll(CLUSTER, keyspace, table);
        settleReconciliation(CLUSTER);

        // journal-resident batch not covered by either SSTable
        writeRows(CLUSTER, keyspace, table, 20, 30);
        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);
        assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
        assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);

        assertThat(totalValidatedSSTables(CLUSTER.get(1), keyspace, table)).isEqualTo(2);
        assertThat(totalValidatedJournalPartitions(CLUSTER.get(1), keyspace, table)).isEqualTo(10);
    }

    @Test
    public void reportsInSyncForPartialColumnUpdatesAcrossSSTableAndJournalSources()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int, w int)");

        // Disjoint key ranges so a digest mismatch in one combination can't be masked by another.
        writeColumns(CLUSTER, keyspace, table, 0, 10, "v");
        writeColumns(CLUSTER, keyspace, table, 10, 20, "w");
        writeColumns(CLUSTER, keyspace, table, 20, 30, "v", "w");

        for (int k = 30; k < 40; k++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k) VALUES (?)", ConsistencyLevel.ALL, k);

        settleAndAssertInSyncWithCoordinatorFlushed(CLUSTER, keyspace, table);
    }

    @Test
    public void reportsInSyncWhenPartitionHasMultipleUnflushedJournalEntries()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int, w int)");

        // Two separate mutations touch the same partition, each setting a different column, so a replica that hasn't 
        // flushed must merge more than one journal entry for this key to produce the correct row.
        CLUSTER.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, 0, 0);
        CLUSTER.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, w) VALUES (?, ?)", ConsistencyLevel.ALL, 0, 0);

        settleAndAssertInSyncWithCoordinatorFlushed(CLUSTER, keyspace, table);
    }

    @Test
    public void reportsInSyncForPartialStaticColumnUpdatesAcrossSSTableAndJournalSources()
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int, c int, s1 int static, s2 int static, v int, PRIMARY KEY (k, c))");

        // Disjoint key ranges so a digest mismatch in one combination can't be masked by another.
        for (int k = 0; k < 10; k++)
            writeClusteredRow(CLUSTER, keyspace, table, k, "s1", "v");
        for (int k = 10; k < 20; k++)
            writeClusteredRow(CLUSTER, keyspace, table, k, "s2", "v");
        for (int k = 20; k < 30; k++)
            writeClusteredRow(CLUSTER, keyspace, table, k, "s1", "s2", "v");

        for (int k = 30; k < 40; k++)
            writeStaticOnly(CLUSTER, keyspace, table, k, "s1");
        for (int k = 40; k < 50; k++)
            writeStaticOnly(CLUSTER, keyspace, table, k, "s1", "s2");

        // Clustering row present but otherwise empty, so columns() is completely empty on the journal side.
        for (int k = 50; k < 60; k++)
            CLUSTER.coordinator(1).execute("INSERT INTO " + keyspace + '.' + table + " (k, c) VALUES (?, 0)", ConsistencyLevel.ALL, k);

        settleAndAssertInSyncWithCoordinatorFlushed(CLUSTER, keyspace, table);
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

        assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);

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
        node2.runOnInstance(PauseValidationRequest::activate);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            node2.runOnInstance(PauseValidationRequest::awaitArrival);

            // Writes issued at CL.ALL after the offset is established land in every replica's
            // journal, including node 2's, while node 2 is paused before it reads anything for
            // the validation.
            writeRows(CLUSTER, keyspace, table, 10, 20);

            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);

            // Node 2's journal has both batches by the time it reads (release happened
            // after the second batch was written), but only batch1's 10 partitions should have
            // been validated.
            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).as("the batch written after the offset must be excluded despite existing at read time").isEqualTo(10);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);
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
        assertNotificationContains(result, table + " -");

        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);

        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDesyncBytesBumped(CLUSTER, 1, keyspace, table, desyncBytesBefore);

        assertDiagnosticSnapshotExistsPossiblyEmpty(CLUSTER, keyspace, table, 2);
        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 3);
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
        assertNotificationContains(result, table + " -");
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);
        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDiagnosticSnapshotExistsPossiblyEmpty(CLUSTER, keyspace, table, 1);
        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 2, 3);
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
        assertNotificationContains(result, table + " -");
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
        assertNotificationContains(result, tableA + " -");
        assertNotificationAbsent(result, tableB + " -");
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);

        assertDesyncRangesBumped(CLUSTER, 1, keyspace, tableA, desyncBeforeA);
        assertThat(desyncRangesOnCoordinator(CLUSTER, 1, keyspace, tableB)).isEqualTo(desyncBeforeB);

        assertDiagnosticSnapshotExists(CLUSTER, keyspace, tableA, 1, 2, 3);
        assertNoDiagnosticSnapshot(CLUSTER, keyspace, tableB);
    }

    @Test
    public void notifiesWithEveryTableWhenMultipleTablesDiverge()
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

        // Both tables diverge, so the notification must list both rather than only the first one found.
        injectSSTableCellDivergence(CLUSTER.get(2), keyspace, tableA, 3, 999);
        injectSSTableCellDivergence(CLUSTER.get(2), keyspace, tableB, 3, 999);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);
        assertNotificationContains(result, tableA + " -");
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);
        assertNotificationContains(result, tableB + " -");
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
        assertNotificationContains(result, table + " -");
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);
        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 2, 3);
    }

    @Test
    public void diagnosticSnapshotCapturesUnflushedJournalData()
    {
        createTrackedSchema(CLUSTER, keyspace, table);
        writeRows(CLUSTER, keyspace, table, 0, 10);
        settleReconciliation(CLUSTER);

        // Full replicas apply mutations to both the journal and the live Memtable, so a snapshot should include it.
        injectJournalPayloadDivergence(CLUSTER.get(2), keyspace, table, 3, 999);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 2, 3);
    }

    @Test
    public void detectsDivergenceInJournalOnlyWritesAlongsideSSTableCoveredWrites()
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        // flushed and fully covered by the resulting SSTable
        writeRows(CLUSTER, keyspace, table, 0, 10);
        settleReconciliation(CLUSTER);
        flushAll(CLUSTER, keyspace, table);

        // reconciled but journal-resident (not covered by any SSTable)
        writeRows(CLUSTER, keyspace, table, 10, 20);
        settleReconciliation(CLUSTER);

        // Diverge a row specifically from the journal-only batch
        injectJournalPayloadDivergence(CLUSTER.get(2), keyspace, table, 15, 999);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);
        long desyncBefore = desyncRangesOnCoordinator(CLUSTER, 1, keyspace, table);

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.INCONSISTENT_MESSAGE);
        assertNotificationContains(result, table + " -");
        assertPreviewFailuresBumpedOnCoordinator(CLUSTER, previewFailuresBefore, 1);
        assertDesyncRangesBumped(CLUSTER, 1, keyspace, table, desyncBefore);
        assertDiagnosticSnapshotExists(CLUSTER, keyspace, table, 1, 2, 3);

        // The divergence being caught is not just a coincidence of the SSTable path alone finding it:
        assertThat(totalValidatedSSTables(CLUSTER.get(1), keyspace, table)).isEqualTo(1);
        assertThat(totalValidatedJournalPartitions(CLUSTER.get(1), keyspace, table)).isGreaterThanOrEqualTo(10);
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
        node2.runOnInstance(PauseValidationRequest::activate);

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
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);

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
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);
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
        node2.runOnInstance(PauseValidationRequest::activate);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            node2.runOnInstance(PauseValidationRequest::awaitArrival);

            writeRows(CLUSTER, keyspace, table, 10, 20);

            // Combines reconciled first batch and unreconciled batch2 into one straddling SSTable.
            node2.nodetoolResult("flush", keyspace, table).asserts().success();

            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);

            // The straddling SSTable is live on disk but excluded wholesale from the
            // SSTable stream; the reconciled first batch portion is still validated, via the
            // journal stream instead.
            assertThat(countLiveSSTables(node2, keyspace, table)).as("node 2 should have flushed the straddling SSTable").isGreaterThan(0);
            assertThat(totalValidatedSSTables(node2, keyspace, table)).as("the straddling SSTable is excluded wholesale from the SSTable stream").isZero();
            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).as("the reconciled batch1 portion should still be validated via the journal stream").isGreaterThan(0);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void validatesReconciledDataViaJournalWhenCompactionStraddlesOffset() throws Exception
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + table + " (k int PRIMARY KEY, v int) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}");

        // Autocompaction could re-trigger finalizeMetadata() on its own schedule.
        disableAutocompaction(CLUSTER, keyspace, table);

        writeRows(CLUSTER, keyspace, table, 0, 10);

        // Flush before settling reconciliation. SSTableWriter marks an SSTable repaired and clears its offsets on write
        // if the data is already durably reconciled, so flushing first keeps it unrepaired with real offsets.
        flushAll(CLUSTER, keyspace, table);
        settleReconciliation(CLUSTER);

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

            // mutateRepairedAndReload() only rewrites the SSTable's own metadata. Without this,
            // CompactionStrategyManager's repaired/unrepaired holder assignment stays stale.
            cfs.getTracker().notifySSTableRepairedStatusChanged(cfs.getLiveSSTables());
        });

        node2.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());
        int staticSegmentsBeforeDrop = node2.callOnInstance(() -> MutationJournal.instance().countStaticSegmentsForTesting());
        node2.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting(true));
        int staticSegmentsAfterDrop = node2.callOnInstance(() -> MutationJournal.instance().countStaticSegmentsForTesting());

        // Segments unrelated to this SSTable's coordinator log offsets may still be reclaimed by the drop attempt.
        assertThat(staticSegmentsAfterDrop).as("static journal segments on node 2 before/after the drop attempt: %d -> %d", staticSegmentsBeforeDrop, staticSegmentsAfterDrop)
                                           .isLessThanOrEqualTo(staticSegmentsBeforeDrop).isGreaterThan(0);

        node2.runOnInstance(PauseValidationRequest::activate);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);
            node2.runOnInstance(PauseValidationRequest::awaitArrival);
            writeRows(CLUSTER, keyspace, table, 10, 20);

            // Only on node 2, flush the batch written after the offset to its own SSTable. Compact the two
            // live SSTables together via --user-defined, which skips TrackedCompactionManager's automatic
            // promotion of durably reconciled data so the result straddles the offset.
            node2.nodetoolResult("flush", keyspace, table).asserts().success();
            List<String> sstableFiles = node2.callOnInstance(() -> {
                ColumnFamilyStore filesCfs = ColumnFamilyStore.getIfExists(ks, tbl);
                List<String> files = new ArrayList<>();
                for (SSTableReader sstable : filesCfs.getLiveSSTables())
                    files.add(sstable.getFilename());
                return files;
            });
            List<String> compactArgs = new ArrayList<>(List.of("compact", "--user-defined"));
            compactArgs.addAll(sstableFiles);
            node2.nodetoolResult(compactArgs.toArray(new String[0])).asserts().success();
            assertThat(countLiveSSTables(node2, keyspace, table)).isEqualTo(1);

            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            // The segment holding the batch written before the offset survived the drop attempt
            // above, so it's still there for the journal stream to fill in during validation.
            assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);

            // The straddling SSTable stays correctly excluded either way.
            assertThat(totalValidatedSSTables(node2, keyspace, table)).isZero();

            // The batch written before the offset is found via the journal instead, since its segment survived.
            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).isEqualTo(10);

            // Node 1 still sees the batch written before the offset via its own untouched SSTable.
            assertThat(totalValidatedSSTables(coordinator, keyspace, table)).isEqualTo(1);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void validatesSSTableSpanningShardsOutsideValidationRange()
    {
        createTrackedSchema(CLUSTER, keyspace, table);
        disableAutocompaction(CLUSTER, keyspace, table);

        String ks = keyspace;

        // 50 keys hashed across the ring should be enough to land in more than one shard.
        writeRows(CLUSTER, keyspace, table, 0, 50);

        // Flush before settling reconciliation, so the SSTable is written unrepaired with real
        // offsets instead of being auto-promoted and cleared on write.
        flushAll(CLUSTER, keyspace, table);
        settleReconciliation(CLUSTER);

        // Validate only one shard's range, deliberately excluding the other(s) this SSTable also spans.
        List<String> shardBoundaries = CLUSTER.get(1).callOnInstance(() -> {
            List<String> boundaries = new ArrayList<>();
            MutationTrackingService.instance().forEachShardInKeyspace(ks, shard ->
                boundaries.add(shard.range.left + "," + shard.range.right));
            return boundaries;
        });
        assertThat(shardBoundaries.size()).as("expected more than one shard to exist").isGreaterThan(1);
        String[] bounds = shardBoundaries.get(0).split(",");

        NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace, table, "-st", bounds[0], "-et", bounds[1]);
        result.asserts().success();

        // The SSTable spans all 3 shards, but only logs belonging to the validated shard are
        // required to be covered, so it's still validated instead of being wrongly excluded.
        for (IInvokableInstance instance : CLUSTER)
            assertThat(totalValidatedSSTables(instance, keyspace, table)).as("expected the SSTable to be validated on %s", instance.config().num()).isEqualTo(1);
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
        node2.runOnInstance(PauseValidationRequest::activate);

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

            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().success();

            assertInSyncValidation(result, CLUSTER, keyspace, table, previewFailuresBefore);

            assertThat(totalValidatedJournalPartitions(node2, keyspace, table)).as("the corrupted entry written after the offset must be excluded despite existing at read time").isEqualTo(10);
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);
            ExecutorUtils.shutdownAndWait(60, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void reportsInSyncDespiteCorruptedJournalEntryOutsideOffsets()
    {
        createTrackedSchema(CLUSTER, keyspace, table);
        writeRows(CLUSTER, keyspace, table, 0, 10);
        settleReconciliation(CLUSTER);

        long[] previewFailuresBefore = previewFailuresPerNode(CLUSTER);

        byte[] corruptBytes = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        CLUSTER.get(2).runOnInstance(() -> MutationJournal.instance().writeUnsafe(new ShortMutationId(Long.MAX_VALUE, 0), corruptBytes));

        try
        {
            NodeToolResult result = CLUSTER.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
            result.asserts().success();

            assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);
            assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
            assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);
        }
        finally
        {
            // Clear the corrupt record so it doesn't interfere with tests run after:
            CLUSTER.get(2).runOnInstance(() -> MutationJournal.instance().truncateForTesting());
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
        coordinator.runOnInstance(PauseEpochCheck::activate);

        ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            coordinator.runOnInstance(PauseEpochCheck::awaitArrival);

            CLUSTER.schemaChange("CREATE TABLE " + keyspace + ".epoch_bump (k int PRIMARY KEY)");

            coordinator.runOnInstance(PauseEpochCheck::releaseAndDeactivate);

            NodeToolResult result = resultFuture.get(120, TimeUnit.SECONDS);
            result.asserts().failure().errorContains("topology changed");

            assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
            assertNotificationAbsent(result, MutationTrackingPreviewRepairTask.DISPATCHING_VALIDATION);
            assertPreviewFailuresUnchanged(CLUSTER, previewFailuresBefore);
            assertNoDiagnosticSnapshot(CLUSTER, keyspace, table);
        }
        finally
        {
            coordinator.runOnInstance(PauseEpochCheck::releaseAndDeactivate);
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

    @Test
    public void cancelsSiblingCoordinatorsConcurrentlyWithoutDeadlock() throws Exception
    {
        CLUSTER.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + NODES + "} AND replication_type='tracked'");
        String tableA = table + "_a";
        String tableB = table + "_b";
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + tableA + " (k int PRIMARY KEY, v int)");
        CLUSTER.schemaChange("CREATE TABLE " + keyspace + '.' + tableB + " (k int PRIMARY KEY, v int)");

        IInvokableInstance coordinator = CLUSTER.get(1);
        IInvokableInstance node2 = CLUSTER.get(2);

        // Keep both coordinators pending indefinitely, so we can force the race ourselves rather than depend on their validation timeout.
        node2.runOnInstance(PauseValidationRequest::activate);

        coordinator.runOnInstance(RaceValidationFailures::reset);
        coordinator.runOnInstance(RaceValidationFailures::arm);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            submitValidate(executor, coordinator, keyspace);

            // Wait for both coordinators to be created and started.
            await().atMost(30, TimeUnit.SECONDS)
                   .pollInterval(100, TimeUnit.MILLISECONDS)
                   .until(() -> coordinator.callOnInstance(RaceValidationFailures::capturedCount) == 2);

            // Cancel both coordinators from different threads at once. Each cancel() wins its own future first, then 
            // tries to cancel the other. This must complete without deadlocking.
            coordinator.runOnInstance(() -> {
                try
                {
                    RaceValidationFailures.cancelCapturedConcurrently();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
        finally
        {
            coordinator.runOnInstance(RaceValidationFailures::disarm);
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);
            ExecutorUtils.shutdownAndWait(10, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void failsFastWhenTreeDifferenceThrows() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        IInvokableInstance coordinator = CLUSTER.get(1);
        long originalTimeoutMillis = coordinator.callOnInstance(() -> DatabaseDescriptor.getMutationTrackingValidationTimeout(TimeUnit.MILLISECONDS));

        // Make the validation timeout essentially infinite, so the only path to resolution is an explicit failure.
        long longTimeoutMillis = TimeUnit.MINUTES.toMillis(10);
        coordinator.runOnInstance(() -> DatabaseDescriptor.setMutationTrackingValidationTimeout(longTimeoutMillis));
        coordinator.runOnInstance(ThrowOnTreeDifference::activate);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            // This should terminate immediately...
            NodeToolResult result = resultFuture.get(longTimeoutMillis / 2, TimeUnit.MILLISECONDS);
            result.asserts().failure();
        }
        finally
        {
            coordinator.runOnInstance(ThrowOnTreeDifference::deactivate);
            coordinator.runOnInstance(() -> DatabaseDescriptor.setMutationTrackingValidationTimeout(originalTimeoutMillis));
            ExecutorUtils.shutdownAndWait(10, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    public void failsFastWhenParticipantReturnsFailure() throws Exception
    {
        createTrackedSchema(CLUSTER, keyspace, table);

        IInvokableInstance coordinator = CLUSTER.get(1);
        IInvokableInstance node2 = CLUSTER.get(2);
        long originalTimeoutMillis = coordinator.callOnInstance(() -> DatabaseDescriptor.getMutationTrackingValidationTimeout(TimeUnit.MILLISECONDS));

        // Make the validation timeout essentially infinite, so the only path to resolution is an explicit failure.
        long longTimeoutMillis = TimeUnit.MINUTES.toMillis(10);
        coordinator.runOnInstance(() -> DatabaseDescriptor.setMutationTrackingValidationTimeout(longTimeoutMillis));
        node2.runOnInstance(PauseValidationRequest::activateToFail);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            Future<NodeToolResult> resultFuture = submitValidate(executor, coordinator, keyspace);

            // This should terminate immediately...
            NodeToolResult result = resultFuture.get(longTimeoutMillis / 2, TimeUnit.MILLISECONDS);
            result.asserts().failure();
        }
        finally
        {
            node2.runOnInstance(PauseValidationRequest::releaseAndDeactivate);
            coordinator.runOnInstance(() -> DatabaseDescriptor.setMutationTrackingValidationTimeout(originalTimeoutMillis));
            ExecutorUtils.shutdownAndWait(10, TimeUnit.SECONDS, executor);
        }
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

    private static void writeColumns(Cluster cluster, String keyspace, String table, int fromInclusive, int toExclusive, String... columns)
    {
        String cql = "INSERT INTO " + keyspace + '.' + table + " (k, " + String.join(", ", columns) + ") VALUES (?" + ", ?".repeat(columns.length) + ")";
        for (int i = fromInclusive; i < toExclusive; i++)
        {
            Object[] values = new Object[columns.length + 1];
            values[0] = i;
            Arrays.fill(values, 1, values.length, i);
            cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL, values);
        }
    }

    private static void writeClusteredRow(Cluster cluster, String keyspace, String table, int key, String... columns)
    {
        String cql = "INSERT INTO " + keyspace + '.' + table + " (k, c, " + String.join(", ", columns) + ") VALUES (?, 0" + ", ?".repeat(columns.length) + ")";
        Object[] values = new Object[columns.length + 1];
        Arrays.fill(values, key);
        cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL, values);
    }

    private static void writeStaticOnly(Cluster cluster, String keyspace, String table, int key, String... staticColumns)
    {
        String cql = "UPDATE " + keyspace + '.' + table + " SET " + String.join(" = ?, ", staticColumns) + " = ? WHERE k = ?";
        Object[] values = new Object[staticColumns.length + 1];
        Arrays.fill(values, 0, staticColumns.length, key);
        values[staticColumns.length] = key;
        cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL, values);
    }

    private static void flushAll(Cluster cluster, String keyspace, String table)
    {
        cluster.forEach(i -> i.nodetoolResult("flush", keyspace, table).asserts().success());
    }

    private static void settleAndAssertInSyncWithCoordinatorFlushed(Cluster cluster, String keyspace, String table)
    {
        settleReconciliation(cluster);

        cluster.get(1).nodetoolResult("flush", keyspace, table).asserts().success();

        long[] previewFailuresBefore = previewFailuresPerNode(cluster);

        NodeToolResult result = cluster.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
        result.asserts().success();

        assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);
        assertPreviewFailuresUnchanged(cluster, previewFailuresBefore);
        assertNoDiagnosticSnapshot(cluster, keyspace, table);
    }

    private static void assertInSyncValidation(NodeToolResult result, Cluster cluster, String keyspace, String table, long[] previewFailuresBefore)
    {
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.OFFSETS_ESTABLISHED);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.VALIDATION_COMPLETE);
        assertNotificationContains(result, MutationTrackingPreviewRepairTask.IN_SYNC_MESSAGE);
        assertPreviewFailuresUnchanged(cluster, previewFailuresBefore);
        assertNoDiagnosticSnapshot(cluster, keyspace, table);
    }

    private static void disableAutocompaction(Cluster cluster, String keyspace, String table)
    {
        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", keyspace, table).asserts().success());
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
                cfs.apply(MutationId.none(), update, new CassandraWriteContext(group, null, LogDomain.COMMIT_LOG), true);
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
            awaitDiagnosticSnapshot(instance, keyspace, table, nodeNum);
            assertThat(diagnosticSnapshotHasSSTableData(instance, keyspace, table)).isTrue();
        }
    }

    private static void assertDiagnosticSnapshotExistsPossiblyEmpty(Cluster cluster, String keyspace, String table, int... expectedNodes)
    {
        for (int nodeNum : expectedNodes)
            awaitDiagnosticSnapshot(cluster.get(nodeNum), keyspace, table, nodeNum);
    }

    private static void awaitDiagnosticSnapshot(IInvokableInstance instance, String keyspace, String table, int nodeNum)
    {
        // DiagnosticSnapshotService fans out SNAPSHOT_REQ messages that are handled asynchronously on each recipient.
        await().atMost(30, TimeUnit.SECONDS)
               .pollInterval(200, TimeUnit.MILLISECONDS)
               .untilAsserted(() -> assertThat(countDiagnosticSnapshots(instance, keyspace, table)).as("Expected diagnostic snapshot for %s.%s on node %d", keyspace, table, nodeNum)
                                                                                                   .isGreaterThan(0));
    }

    private static boolean diagnosticSnapshotHasSSTableData(IInvokableInstance instance, String keyspace, String table)
    {
        return instance.callOnInstance(() -> {
            List<TableSnapshot> snapshots = SnapshotManager.instance.getSnapshots(snapshot ->
                snapshot.getKeyspaceName().equals(keyspace)
                && snapshot.getTableName().equals(table)
                && snapshot.getTag().startsWith(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX));
            for (TableSnapshot snapshot : snapshots)
                if (!TableSnapshot.getSnapshotDescriptors(keyspace, table, snapshot.getTag()).isEmpty())
                    return true;
            return false;
        });
    }

    public static class PauseValidationRequest
    {
        private static volatile CountDownLatch arrived;
        private static volatile CountDownLatch release;
        private static volatile boolean failFast;

        @SuppressWarnings("resource")
        public static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(RepairMessageVerbHandler.class)
                           .method(named("doVerb"))
                           .intercept(MethodDelegation.to(PauseValidationRequest.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void activate()
        {
            arrived = new CountDownLatch(1);
            release = new CountDownLatch(1);
            failFast = false;
        }

        public static void activateToFail()
        {
            arrived = new CountDownLatch(1);
            release = new CountDownLatch(1);
            failFast = true;
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

        public static void releaseAndDeactivate()
        {
            if (release != null)
                release.countDown();
            arrived = null;
            release = null;
            failFast = false;
        }

        @SuppressWarnings("unused")
        public static void doVerb(Message<RepairMessage> message, @SuperCall Callable<?> zuper)
        {
            CountDownLatch localArrived = arrived;
            CountDownLatch localRelease = release;
            if (localArrived != null && message.verb() == Verb.MT_VALIDATION_REQ)
            {
                localArrived.countDown();

                if (failFast)
                {
                    RepairMessage.sendFailureResponse(SharedContext.Global.instance, message);
                    return;
                }

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

        public static void activate()
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

        public static void releaseAndDeactivate()
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

    public static class RaceValidationFailures
    {
        private static volatile CyclicBarrier barrier;
        private static final ThreadLocal<Object> home = new ThreadLocal<>();
        private static final List<Object> captured = new CopyOnWriteArrayList<>();

        @SuppressWarnings("resource")
        public static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(MutationTrackingValidationCoordinator.class)
                           .method(named("fail").or(named("start")))
                           .intercept(MethodDelegation.to(RaceValidationFailures.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void arm()
        {
            barrier = new CyclicBarrier(2);
        }

        public static void disarm()
        {
            barrier = null;
        }

        public static void reset()
        {
            captured.clear();
        }

        public static int capturedCount()
        {
            return captured.size();
        }

        @SuppressWarnings("unused")
        public static org.apache.cassandra.utils.concurrent.Future<?> start(@This Object self, @SuperCall Callable<?> zuper) throws Exception
        {
            captured.add(self);
            return (org.apache.cassandra.utils.concurrent.Future<?>) zuper.call();
        }

        @SuppressWarnings("unused")
        public static void fail(Throwable cause, @This Object self, @SuperCall Callable<?> zuper) throws Exception
        {
            CyclicBarrier local = barrier;
            if (local != null)
            {
                Object h = home.get();
                if (h == null)
                    home.set(self);
                else if (h != self)
                {
                    try
                    {
                        local.await(60, TimeUnit.SECONDS);
                    }
                    catch (Exception ignored)
                    {
                    }
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

        /**
         * Cancels every captured coordinator from its own independent thread, all at once, so that
         * if two coordinators deadlock on each other's monitor, this call itself hangs and throws.
         */
        public static void cancelCapturedConcurrently() throws Exception
        {
            List<Object> toCancel = new ArrayList<>(captured);
            if (toCancel.size() != 2)
                throw new AssertionError("expected exactly 2 captured coordinators, found " + toCancel.size());

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try
            {
                List<Future<?>> futures = new ArrayList<>();
                for (Object coord : toCancel)
                {
                    MutationTrackingValidationCoordinator typed = (MutationTrackingValidationCoordinator) coord;
                    futures.add(pool.submit(() -> {
                        typed.cancel();
                        return null;
                    }));
                }
                for (Future<?> f : futures)
                    f.get(20, TimeUnit.SECONDS);
            }
            finally
            {
                pool.shutdownNow();
            }
        }
    }

    public static class ThrowOnTreeDifference
    {
        private static volatile boolean active;

        @SuppressWarnings("resource")
        public static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(MerkleTrees.class)
                           .method(named("difference"))
                           .intercept(MethodDelegation.to(ThrowOnTreeDifference.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void activate()
        {
            active = true;
        }

        public static void deactivate()
        {
            active = false;
        }

        @SuppressWarnings("unused")
        public static List<Range<Token>> difference(MerkleTrees ltrees, MerkleTrees rtrees, @SuperCall Callable<List<Range<Token>>> zuper) throws Exception
        {
            if (active)
                throw new RuntimeException("injected failure for testing");
            return zuper.call();
        }
    }
}
