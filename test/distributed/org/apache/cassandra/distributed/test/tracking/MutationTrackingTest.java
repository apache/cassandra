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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.MutationTrackingMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.test.tracking.MutationTrackingReadReconciliationTest.awaitNodeAlive;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingReadReconciliationTest.awaitNodeDead;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertMatchingSummaryIdSpaceForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.numLogReconciliations;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

// TODO This test would be a lot faster if it had a shared cluster
public class MutationTrackingTest extends TestBaseImpl
{
    private static final String INSERT_FMT = "INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (%d, %d)";
    private static final String INSERT_CQL = String.format(INSERT_FMT, 1, 1);
    private static final String CONDITIONAL_INSERT_CQL = INSERT_CQL + " IF NOT EXISTS";

    private static final String BATCH_INSERT_FMT = "BEGIN %s BATCH%n"
                                                 + "  %s%n"
                                                 + "  %s%n"
                                                 + "APPLY BATCH";

    @Test
    public void testBasicWritePath() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);
                Offsets summaryIds = summaryIdSpace(summary.get(logId));
                assertEquals(1, summaryIds.offsetCount());
            });
        }
    }

    private static int getOffsetCount(IInvokableInstance node, String keyspaceName, String tableName, int key)
    {
        return node.callOnInstance(() -> {
            TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, tableName);
            DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(key));
            MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
            if (summary.isEmpty())
                return 0;
            CoordinatorLogId logId = getOnlyLogId(summary);
            return summaryIdSpace(summary.get(logId)).offsetCount();
        });

    }

    private static int getOffsetCount(IInvokableInstance node, String keyspaceName, int key)
    {
        return getOffsetCount(node, keyspaceName, "tbl", key);
    }

    /**
     * Writes tracked mutations, deliberately doesn't flush so the writes live only in the
     * commit log the node, and asserts MTS witness state on boot reflects the unflushed writes. Confirm they're
     * reconstructed on journal playback
     */
    @Test
    public void testWitnessSurvivesBounceWithoutFlush() throws Throwable
    {
        final int key = 1;
        final int writes = 10;

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 1} " +
                                              "AND replication_type='tracked';"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int);"));

            // Pause the persister so writes never reach system.coordinator_logs SSTables —
            // the only durable record of the witnesses on disk lives in the commit log.
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseOffsetsPersisterForTesting());

            for (int i = 0; i < writes; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (?, ?)"),
                                                ConsistencyLevel.QUORUM, key, i);

            String keyspaceName = KEYSPACE;
            int preBounceOffsetCount = getOffsetCount(cluster.get(1), keyspaceName, key);
            assertEquals("Pre-bounce witness count must equal write count", writes, preBounceOffsetCount);

            // Bounce without flushing. Witnesses live only in the commit log + journal segments
            // (still active, needsReplay=true). On the way back up, MTS.start must run after
            // CommitLog.recoverSegmentsOnDisk() so the journal replay path repopulates witnesses
            // before any consumer queries MTS state.
            ClusterUtils.stopUnchecked(cluster.get(1));
            cluster.get(1).startup();

            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseOffsetsPersisterForTesting());

            int postBounceOffsetCount = getOffsetCount(cluster.get(1), keyspaceName, key);

            assertEquals("Witness state must survive bounce-without-flush: post-bounce offsets must match pre-bounce",
                         preBounceOffsetCount, postBounceOffsetCount);
        }
    }

    /**
     * Regression test for the lost-witness-marker race (CASSANDRA-21443).
     *
     * When a memtable flush + segment close fires before the periodic LogStatePersister
     * has written witnessed offsets to system.coordinator_logs, the segment metadata can be
     * durably marked needsReplay=false while the witnesses for its mutations are still only
     * in memory. A crash in this window leaves the node with data in SSTables but with
     * witness state missing on restart, breaking mutation summaries and journal sync barrier
     * guarantees.
     *
     * The test pauses the persister, writes a known set of mutations, forces flush and
     * segment close (triggering maybeCleanupStaticSegment), bounces the node, and asserts
     * that the post-restart witness state matches the pre-bounce snapshot.
     */
    @Test
    public void testWitnessSurvivesCrashAfterFlushAndSegmentClose() throws Throwable
    {
        final int key = 1;
        final int writes = 10;

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 1} " +
                                              "AND replication_type='tracked';"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int);"));

            // Pause the persister so no witness state escapes to system.coordinator_logs
            // for the duration of the test window. This mirrors the in-production hazard
            // between persister ticks (currently 1s, planned 60s).
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseOffsetsPersisterForTesting());

            for (int i = 0; i < writes; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (?, ?)"),
                                                ConsistencyLevel.QUORUM, key, i);

            String keyspaceName = KEYSPACE;
            int preBounceOffsetCount = getOffsetCount(cluster.get(1), keyspaceName, key);
            assertEquals("Pre-bounce witness count must equal write count", writes, preBounceOffsetCount);

            // Flush so notifyFlushed marks the active segment's interval clean.
            cluster.get(1).nodetoolResult("flush", KEYSPACE).asserts().success();

            // Roll the active segment to static. The cleanup callback fires
            // (maybeCleanupStaticSegment) and — in the broken code — durably clears
            // needsReplay=false even though witnesses are not persisted.
            cluster.get(1).runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());

            // Bounce without running the persister.
            ClusterUtils.stopUnchecked(cluster.get(1));
            cluster.get(1).startup();

            // Re-pause on the freshly-restarted instance so any first persister tick
            // cannot accidentally normalize state before we sample it.
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseOffsetsPersisterForTesting());

            int postBounceOffsetCount = getOffsetCount(cluster.get(1), keyspaceName, key);

            assertEquals("Witness state must survive crash: post-bounce offsets must match pre-bounce",
                         preBounceOffsetCount, postBounceOffsetCount);
        }
    }

    /**
     * Companion to {@link #testWitnessSurvivesCrashAfterFlushAndSegmentClose}: confirms
     * that the deferred-cleanup fix did not break segment cleanup itself. After a flush
     * and segment close, the segment should sit in {@code pendingCleanup} (not yet
     * needsReplay=false on disk), and the next persister tick should drain it.
     *
     * If this test fails while the witness-survival test passes, it means we have
     * accidentally turned segment cleanup into a no-op - segments would never be eligible
     * for journal compaction, and disk would grow unbounded.
     */
    @Test
    public void testPersisterDrainsPendingSegmentCleanup() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 1} " +
                                              "AND replication_type='tracked';"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int);"));

            // Pause the scheduled persister so the only persister run in this test is the
            // explicit one below — otherwise the periodic tick could drain mid-assert.
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseOffsetsPersisterForTesting());

            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (?, ?)"),
                                                ConsistencyLevel.QUORUM, 1, i);

            cluster.get(1).nodetoolResult("flush", KEYSPACE).asserts().success();
            cluster.get(1).runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());

            // After flush + close, the segment should be queued for cleanup but not yet
            // marked needsReplay=false on disk.
            cluster.get(1).runOnInstance(() -> {
                assertTrue("Expected at least one segment queued for cleanup after flush + close",
                           !MutationJournal.instance().pendingCleanupForTesting().isEmpty());
            });

            // Run an explicit persister tick: writes coordinator_logs, drains the snapshot.
            // Use the boolean variant to bypass the isPaused check.
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting(true));

            // After the persister tick, the queue should be drained.
            cluster.get(1).runOnInstance(() -> {
                assertTrue("Expected pendingCleanup to be empty after persister tick",
                           MutationJournal.instance().pendingCleanupForTesting().isEmpty());
            });
        }
    }

    /**
     * Validates the clean-shutdown drain path (CASSANDRA-21443).
     *
     * Test setup pauses the periodic persister so that the only persister run is the
     * shutdown-drain one. After a clean bounce, witnesses must survive and there must be no
     * static segments on disk, (they're truncated on drain).
     */
    @Test
    public void testCleanShutdownDrainsPendingCleanup() throws Throwable
    {
        final int key = 1;
        final int writes = 10;

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 1} " +
                                              "AND replication_type='tracked';"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int);"));

            // Pause the periodic persister so the only run is the shutdown final tick.
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseOffsetsPersisterForTesting());

            for (int i = 0; i < writes; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (?, ?)"),
                                                ConsistencyLevel.QUORUM, key, i);

            String keyspaceName = KEYSPACE;
            int preBounceOffsetCount = getOffsetCount(cluster.get(1), keyspaceName, key);
            assertEquals("Pre-bounce witness count must equal write count", writes, preBounceOffsetCount);

            // Flush and close the active segment so it becomes static and enters pendingCleanup.
            cluster.get(1).nodetoolResult("flush", KEYSPACE).asserts().success();
            cluster.get(1).runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());

            // Confirm pendingCleanup is non-empty before shutdown — this is the state the
            // final tick is supposed to drain.
            cluster.get(1).runOnInstance(() -> {
                assertTrue("Expected at least one segment queued for cleanup before shutdown",
                           !MutationJournal.instance().pendingCleanupForTesting().isEmpty());
            });

            // Clean (graceful) shutdown — runs MutationTrackingService.shutdownBlocking which
            // performs the final persister tick.
            ClusterUtils.stopUnchecked(cluster.get(1));
            cluster.get(1).startup();

            // Re-pause so any first periodic tick post-restart cannot rewrite state before
            // we sample.
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseOffsetsPersisterForTesting());

            // Assertion 1: witness count survived. The final tick wrote coordinator_logs,
            // so MTS.start's loadFromSystemTables on the next boot saw the witnesses.
            int postBounceOffsetCount = getOffsetCount(cluster.get(1), keyspaceName, key);
            assertEquals("Witness state must survive clean shutdown via the final-tick path",
                         preBounceOffsetCount, postBounceOffsetCount);

            // Assertion 2: no static segments on disk. The final tick's truncation step
            // dropped the fully-reconciled segments. Without the final tick, the segment would
            // still be present (replay reconstitutes witnesses but doesn't drop the segment).
            cluster.get(1).runOnInstance(() -> {
                int staticSegments = MutationJournal.instance().countStaticSegmentsForTesting();
                assertEquals("Expected zero static segments after clean shutdown final tick",
                             0, staticSegments);
            });
        }
    }

    @Test
    public void testWitnessPaxosV1Reads() throws Throwable
    {
        testWitnessPaxosReads("v1");
    }

    @Test
    public void testWitnessPaxosV2Reads() throws Throwable
    {
        testWitnessPaxosReads("v2");
    }

    private void testWitnessPaxosReads(String paxosVariant) throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                            .set("transient_replication_enabled", "true")
                                                            .set("paxos_variant", paxosVariant))
                                      .start())
        {
            String keyspaceName = KEYSPACE;
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // TODO shouldn't be necessary to mess with marking things in Gossip but there is no read speculation
            // so the read fails because it routes to a node that is blocked
            cluster.filters().allVerbs().to(3).drop().on();
            cluster.filters().allVerbs().from(3).drop().on();
            for (int i = 1; i < 3; i++)
                cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked("127.0.0.3"), Double.MAX_VALUE));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            // Two nodes should know about the mutation
            for (int i = 1; i <= 2; i++)
                cluster.get(i).runOnInstance(() -> {
                    MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(Util.dk(1), ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, true);
                    assertEquals(1, summary.size());
                });

            // Filter should stop the witness from getting the mutation so we can test pushing the mutation summary to the witness
            cluster.get(3).runOnInstance(() -> {
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(Util.dk(1), ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, true);
                assertEquals(0, summary.size());
            });

            int rowsFound = 0;
            String singlePartitionSelectCQL = withKeyspace("SELECT * FROM %s.tbl WHERE k = 1");
            for (IInvokableInstance instance : cluster)
            {
                Object[][] result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0-1 rows", result.length == 0 || result.length == 1);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 2, rowsFound);

            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop().on();
            cluster.filters().allVerbs().from(2).drop().on();
            cluster.get(1).runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
                InetAddressAndPort endpoint = InetAddressAndPort.getByNameUnchecked("127.0.0.3");
                Gossiper.instance.realMarkAlive(endpoint, Gossiper.instance.getEndpointStateForEndpoint(endpoint));
            }));
            for (int i = 1; i < 4; i++)
                if (i != 2)
                    cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked("127.0.0.2"), Double.MAX_VALUE));

            Object[][] result = cluster.coordinator(1).execute(singlePartitionSelectCQL, ConsistencyLevel.SERIAL);
            assertEquals(1, result.length);
            assertEquals(1, result[0][0]);
            assertEquals(1, result[0][1]);

            // The read at SERIAL should propagate the mutation to the witness
            cluster.get(3).runOnInstance(() -> {
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(Util.dk(1), ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, true);
                assertEquals(1, summary.size());
            });
        }
    }

    @Ignore("Unlogged batches not supported with mutation tracking yet")
    @Test
    public void testWitnessUnloggedBatchSkippedPath() throws Throwable
    {
        testWitnessBatchWrites(false);
    }

    @Ignore("Logged batches not supported with mutation tracking yet")
    @Test
    public void testWitnessLoggedBatchSkippedPath() throws Throwable
    {
        testWitnessBatchWrites(true);
    }

    private void testWitnessBatchWrites(boolean logged) throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                                          .set("transient_replication_enabled", "true"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            String insertCql = String.format(BATCH_INSERT_FMT, logged ? "" : "UNLOGGED", String.format(INSERT_FMT, KEYSPACE, 1, 1), String.format(INSERT_FMT, KEYSPACE, 2, 2));
            cluster.coordinator(1).execute(insertCql, ConsistencyLevel.ALL);

            // Only two instances should have the row
            int rowsFound = 0;
            String singlePartitionSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            for (IInvokableInstance instance : cluster)
            {
                Object[][] result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0 or 2 rows", result.length == 0 || result.length == 2);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 4, rowsFound);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);

                Offsets summaryIds = summaryIdSpace(summary.get(logId));
                assertEquals(1, summaryIds.offsetCount());
            });

            Object[][] result = cluster.coordinator(1).execute(singlePartitionSelectCQL, ConsistencyLevel.ALL);
            assertEquals(2, result.length);
            String partitionRangeSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            result = cluster.coordinator(1).execute(partitionRangeSelectCQL, ConsistencyLevel.ALL);
            assertEquals(2, result.length);

            // Read time reconciliation should not propagate the row to the witness node
            rowsFound = 0;
            for (IInvokableInstance instance : cluster)
            {
                result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0 or 2 rows", result.length == 0 || result.length == 2);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 4, rowsFound);
        }
    }

    @Test
    public void testWitnessHintSkippedPath() throws Throwable
    {

    }

    @Test
    public void testWitnessSerialPaxosV1WritesSkipped() throws Throwable
    {
        testWitnessWrites(CONDITIONAL_INSERT_CQL, ConsistencyLevel.SERIAL, "v1");
    }

    @Test
    public void testWitnessSerialPaxosV2WritesSkipped() throws Throwable
    {
        testWitnessWrites(CONDITIONAL_INSERT_CQL, ConsistencyLevel.SERIAL, "v2");
    }

    @Test
    public void testNonSerialWitnessWrites() throws Throwable
    {
        testWitnessWrites(INSERT_CQL, ConsistencyLevel.ALL, null);
    }

    private void testWitnessWrites(String insertCql, ConsistencyLevel cl, String paxosVariant) throws Throwable
    {
        String paxosVariantFinal = paxosVariant == null ? "v1" : paxosVariant;
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                                          .set("transient_replication_enabled", "true")
                                                                          .set("paxos_variant", paxosVariantFinal))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            cluster.coordinator(1).execute(insertCql, cl, ConsistencyLevel.QUORUM);

            // Only two instances should have the row
            int rowsFound = 0;
            String singlePartitionSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            for (IInvokableInstance instance : cluster)
            {
                Object[][] result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0-1 rows", result.length == 0 || result.length == 1);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 2, rowsFound);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);

                Offsets summaryIds = summaryIdSpace(summary.get(logId));
                assertEquals(1, summaryIds.offsetCount());
            });

            Object[][] result = cluster.coordinator(1).execute(singlePartitionSelectCQL, ConsistencyLevel.ALL);
            assertEquals(1, result.length);
            String partitionRangeSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            result = cluster.coordinator(1).execute(partitionRangeSelectCQL, ConsistencyLevel.ALL);
            assertEquals(1, result.length);

            // Read time reconciliation should not propagate the row to the witness node
            rowsFound = 0;
            for (IInvokableInstance instance : cluster)
            {
                result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0-1 rows", result.length == 0 || result.length == 1);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 2, rowsFound);
        }
    }

    @Test
    public void testHintsNotWrittenOnFailedWrite() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // block messages to node 3
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            UUID node3HostId = cluster.get(3).callOnInstance(() -> StorageService.instance.getLocalHostUUID());
            long hints = cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount());

            // confirm no hints for node 3
            cluster.get(1).runOnInstance(() -> assertEquals(0, HintsService.instance.getTotalHintsSize(node3HostId)));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            // wait for write timeout
            Thread.sleep(5000);

            // TODO: confirm hints aren't written
            cluster.get(1).runOnInstance(() -> {
                assertEquals(hints, StorageMetrics.totalHints.getCount());
            });
        }
    }

    @Test
    public void testFailedMutationRedelivery() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = KEYSPACE;

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // block writes to node 3
            cluster.filters().verbs(Verb.MUTATION_REQ.id).to(3).drop();

            // pause reconciler temporarily
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseActiveReconciler());

            // issue a write - should fail on node 3
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            Thread.sleep(1000); // wait for write timeout

            cluster.get(1).runOnInstance(() ->
            {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);
                Assert.assertEquals(1, summary.get(logId).unreconciled.offsetCount());
                Assert.assertEquals(0, summary.get(logId).reconciled.offsetCount());
            });

            // resume the reconciler and spin until reconciliation completes.
            // The reconciler retries with PUSH_MUTATION_REQ whose response inherits the
            // request's expiry (write_request_timeout). Under load the response can arrive
            // after that expiry and be silently dropped by InboundMessageHandler, requiring
            // a retry cycle. Spinning accommodates multiple retry rounds.
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().resumeActiveReconciler());

            Util.spinUntilTrue(() ->
                cluster.get(1).callOnInstance(() -> {
                    TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                    DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                    MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                    CoordinatorLogId logId = getOnlyLogId(summary);
                    return summary.get(logId).unreconciled.offsetCount() == 0
                           && summary.get(logId).reconciled.offsetCount() == 1;
                }), 10);
        }
    }

    @Test
    public void testBackgroundPullReconciliation() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));

            // 1. Partition node 3 and pause push-side retries on all nodes
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            for (int i = 1; i <= 2; i++)
                cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked("127.0.0.3"), Double.MAX_VALUE));

            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> {
                    MutationTrackingService.instance().pauseActiveReconciler();
                    MutationTrackingService.instance().pauseBackgroundReconciler();
                });

            // wait until node 1 marks node 3 as dead
            awaitNodeDead(cluster.get(1), cluster.get(3));

            // 2. Write at QUORUM - succeeds on nodes 1, 2 but node 3 won't get the write
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"),
                                           ConsistencyLevel.QUORUM);

            // Sleep one second until write timeout elapses
            TimeUnit.SECONDS.sleep(1);

            // 3. Capture expected state from node 1
            MutationSummary expected = summaryForKey(cluster.get(1), KEYSPACE, "tbl", /* key */1);

            // 4. Ensure node 3 does NOT have the mutation yet
            cluster.get(3).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, "tbl");
                assertNotNull(table);
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance()
                                                                 .createSummaryForKey(dk, table.id, false);
                assertTrue("Node 3 should have no mutations yet", summary.isEmpty());
            });

            // 5. Reset state for node 3, let broadcasts propagate
            cluster.filters().reset();
            awaitNodeAlive(cluster.get(1), cluster.get(3));
            awaitNodeAlive(cluster.get(3), cluster.get(1));

            // Now broadcast offsets so node 3 learns what nodes 1 and 2 have
            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().resumeActiveReconciler());

            // 6. Trigger the background reconciler on node 3 ONLY (no reads, no push retries)
            cluster.get(3).runOnInstance(() -> {
                MutationTrackingService.instance().resumeBackgroundReconciler();
                MutationTrackingService.instance().reconcileForTesting();
            });

            // 7. Wait for the pull request to be processed and mutation to arrive
            TimeUnit.SECONDS.sleep(2);

            // Broadcast again so reconciliation state converges
            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            // 8. Verify node 3 now has the mutation (pulled via background reconciler)
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), KEYSPACE, "tbl", /* key */1, expected);

            // 9. Verify no read reconciliation was triggered
            assertEquals(0, numLogReconciliations(cluster.get(3)));
        }
    }

    @Test
    public void testBackgroundPullReconciliationWhenCoordinatorDown() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3).withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                     .with(Feature.GOSSIP)
                                                                     .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));

            // 1. Pause push-side retires and background reconciler on all nodes
            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> {
                    MutationTrackingService.instance().pauseActiveReconciler();
                    MutationTrackingService.instance().pauseBackgroundReconciler();
                });

            // 2. Partition node 3, then write at QUORUM from coordinator (node 1)
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            awaitNodeDead(cluster.get(1), cluster.get(3));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"),
                                           ConsistencyLevel.QUORUM);

            // Sleep one second until write timeout elapses
            TimeUnit.SECONDS.sleep(1);

            // 3. Capture expected state from node 1 (before we partition it)
            MutationSummary expected = summaryForKey(cluster.get(1), KEYSPACE, "tbl", 1);

            // 4. Heal node 3, then partition node 1 (the coordinator)
            cluster.filters().reset();
            awaitNodeAlive(cluster.get(2), cluster.get(3));
            awaitNodeAlive(cluster.get(3), cluster.get(2));

            cluster.filters().allVerbs().to(1).drop();
            cluster.filters().allVerbs().from(1).drop();
            for (int i = 2; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked("127.0.0.1"), Double.MAX_VALUE));
            awaitNodeDead(cluster.get(3), cluster.get(1));
            awaitNodeDead(cluster.get(2), cluster.get(1));

            // 5. Broadcast offsets between nodes 2 and 3 only (Node 3 learns what node 2 has witnessed
            // should discover the gap)
            for (int i = 2; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            // 6. Trigger background reconciler on node 3. Coordinator is down, so it is expected to fallback to node 2
            //    (the remaining alive replica)
            cluster.get(3).runOnInstance(() -> {
                MutationTrackingService.instance().resumeBackgroundReconciler();
                MutationTrackingService.instance().reconcileForTesting();
            });

            // 7. Resume the active reconciler on instance 2, so whenever instance 2 receives the request
            //    it will process the PULL_MUTATIONS_REQ verb
            cluster.get(2).runOnInstance(() -> MutationTrackingService.instance().resumeActiveReconciler());

            // Wait for the pull request to be processed and mutation to arrive
            TimeUnit.SECONDS.sleep(2);

            // Broadcast again so reconciliation state converges
            for (int i = 2; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            // 8. Verify node 3 pulled the mutation from node 2 (the fallback replica)
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), KEYSPACE, "tbl", 1, expected);

            // 9. Verify no read reconciliation was involved
            assertEquals(0, numLogReconciliations(cluster.get(3)));
        }
    }

    @Test
    public void testBackgroundReconciliationCooldown() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));

            // 1. Create a missing offset on node 3 using the same recipe as
            //    testBackgroundPullReconciliation: partition node 3, write at QUORUM, then
            //    reconnect and broadcast offsets so node 3 learns about the gap.
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            for (int i = 1; i <= 2; i++)
                cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked("127.0.0.3"), Double.MAX_VALUE));

            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> {
                    MutationTrackingService.instance().pauseActiveReconciler();
                    MutationTrackingService.instance().pauseBackgroundReconciler();
                });

            awaitNodeDead(cluster.get(1), cluster.get(3));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"),
                                           ConsistencyLevel.QUORUM);
            TimeUnit.SECONDS.sleep(1);

            cluster.filters().reset();
            awaitNodeAlive(cluster.get(1), cluster.get(3));
            awaitNodeAlive(cluster.get(3), cluster.get(1));

            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            // 2. Use a short request cooldown on node 3 so the test is fast, and keep it
            //    distinct from the schedule interval so the cooldown is actually exercised
            //    (not aliased onto the scheduling cadence).
            //
            //    NOTE: we deliberately leave the active reconciler PAUSED on node 1 (and 2)
            //    so the pull requests we send from node 3 are never served — the missing
            //    offsets stay missing across phases, and each reconcileForTesting() that
            //    isn't suppressed by the cooldown produces a fresh outbound pull request.
            long cooldownMs = 500;
            cluster.get(3).runOnInstance(() ->
                                         MutationTrackingService.instance().setMutationTrackingBackgroundReconciliationRequestCooldownMilliseconds(cooldownMs));

            // 3. Use the BackgroundPullRequestsSent metric on node 3 as the source of truth
            //    for outbound pull request counts — it's incremented in lock-step with each
            //    actual send and avoids needing a separate counting message filter.
            IInvokableInstance node3 = cluster.get(3);
            LongSupplier sentCount = () ->
                                     node3.callOnInstance(() -> MutationTrackingMetrics.instance().backgroundPullRequestsSent.getCount());

            // === Phase 1: rapid-fire dedup ===
            // Two reconcileForTesting calls back-to-back within the cooldown window must
            // produce only ONE outbound pull request.
            node3.runOnInstance(() -> {
                MutationTrackingService.instance().resumeBackgroundReconciler();
                MutationTrackingService.instance().reconcileForTesting();
                MutationTrackingService.instance().reconcileForTesting();
                MutationTrackingService.instance().pauseBackgroundReconciler();
            });
            TimeUnit.MILLISECONDS.sleep(200);
            assertEquals("Rapid-fire dedup: only the first reconcile should send a request",
                         1L, sentCount.getAsLong());
            // Verify the suppression code path was actually taken on the second call.
            long suppressedAfterPhase1 = node3.callOnInstance(() ->
                                                              MutationTrackingMetrics.instance().backgroundPullRequestsSuppressed.getCount());
            assertTrue("Cooldown suppression metric should advance when a duplicate reconcile is suppressed",
                       suppressedAfterPhase1 >= 1);

            // === Phase 2: cooldown expires, allowing a fresh request ===
            // Sleep longer than the configured cooldown. While paused, the scheduled task
            // fires and exercises the disable-clear branch (and the time-based removeIf),
            // so the previous entry is gone by the time we manually reconcile again.
            TimeUnit.MILLISECONDS.sleep(cooldownMs + 200);
            node3.runOnInstance(() -> {
                MutationTrackingService.instance().resumeBackgroundReconciler();
                MutationTrackingService.instance().reconcileForTesting();
                MutationTrackingService.instance().pauseBackgroundReconciler();
            });
            assertEquals("Post-cooldown: a fresh reconcile should send a request once the cooldown elapses",
                         2L, sentCount.getAsLong());

            // === Phase 3: disabling reconciliation clears tracked state ===
            // Phase 2 left a fresh entry in lastRequestedAt. Without sleeping past the
            // cooldown, invoking run() while disabled takes the disable-clear branch and
            // wipes the map. The next reconcile (after re-enabling) should then send a
            // request even though we are still inside Phase 2's cooldown window.
            node3.runOnInstance(() -> {
                // Already paused at the end of Phase 2 — explicitly run() to exercise the
                // disable-clear branch with the entry still inside the cooldown window.
                MutationTrackingService.instance().reconcileForTesting();
                MutationTrackingService.instance().resumeBackgroundReconciler();
                MutationTrackingService.instance().reconcileForTesting();
                MutationTrackingService.instance().pauseBackgroundReconciler();
            });
            TimeUnit.MILLISECONDS.sleep(200);
            // Without the disable-clear branch, the Phase 2 entry would still be within
            // its cooldown and would suppress this reconcile.
            assertEquals("Disabling should clear tracked-request state and allow a fresh send within the cooldown window",
                         3L, sentCount.getAsLong());

            // Sanity: the happy path should never count a send-layer failure. The callback
            // we install on each pull request only counts non-TIMEOUT failures (queue overload,
            // serialization, closed connection); the eventual TIMEOUT for an unanswered one-way
            // request is benign and must not advance this counter.
            long failedAtEnd = node3.callOnInstance(() ->
                                                    MutationTrackingMetrics.instance().backgroundPullRequestsFailed.getCount());
            assertEquals("Happy-path test should not record any send-layer failures",
                         0, failedAtEnd);
        }
    }
}
