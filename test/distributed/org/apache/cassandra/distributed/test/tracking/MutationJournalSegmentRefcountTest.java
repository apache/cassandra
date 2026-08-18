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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end coverage of CASSANDRA-21406: a static journal segment must be retained while any
 * unrepaired sstable references it, and must be droppable once every referencing sstable has
 * been promoted to repaired (e.g. by compaction once mutations are durably reconciled).
 */
public class MutationJournalSegmentRefcountTest extends TestBaseImpl
{
    private static final String CREATE_KEYSPACE =
    "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} " +
    "AND replication_type = 'tracked'";

    private static final String CREATE_TABLE = "CREATE TABLE %s.tbl (pk int PRIMARY KEY, val text)";

    @Test(timeout = 120_000)
    public void testSegmentRetainedUntilSSTableRepaired() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Disable autocompaction so flushed sstables stay until we explicitly compact.
            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, "tbl").asserts().success());

            // Block offset broadcasts: each node only sees its own witnesses, so isDurablyReconciled is
            // false everywhere and SSTableWriter cannot auto-mark the flushed sstables as repaired.
            cluster.filters().verbs(Verb.MT_BROADCAST_LOG_OFFSETS.id).drop();

            for (int i = 0; i < 50; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                ConsistencyLevel.QUORUM, i, "v" + i);
            }

            // Flush and force the active journal segment to roll so we have a static segment to inspect.
            cluster.forEach(i -> i.nodetoolResult("flush", KEYSPACE).asserts().success());
            cluster.forEach(i -> i.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty()));

            // Confirm there is a static segment that the new refcount is keeping alive, then try to drop:
            // the dropping pass must be a no-op because every flushed sstable is unrepaired.
            cluster.forEach(i -> i.runOnInstance(() -> {
                int before = MutationJournal.instance().countStaticSegmentsForTesting();
                assertTrue("Expected at least one static segment after flush+segment close, got " + before, before > 0);
                MutationTrackingService.instance().persistLogStateForTesting();
                int after = MutationJournal.instance().countStaticSegmentsForTesting();
                assertEquals("Static segments must not be dropped while unrepaired sstables reference them",
                             before, after);
            }));

            // Restore broadcast, exchange witnesses, and persist so isDurablyReconciled is now true everywhere.
            cluster.filters().reset();
            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting()));
            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));

            // Major-compact the table. Compaction rewrites the sstable through SSTableWriter.finalizeMetadata,
            // which detects that all mutations are durably reconciled and stamps repairedAt on the output.
            // SSTableListChangedNotification then releases refs from the (unrepaired) inputs without acquiring
            // any from the (repaired) output -> refcount drops to zero.
            cluster.forEach(i -> i.nodetoolResult("compact", KEYSPACE, "tbl").asserts().success());

            // Now the persister can drop the static segments.
            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));
            cluster.forEach(i -> i.runOnInstance(() -> {
                int remaining = MutationJournal.instance().countStaticSegmentsForTesting();
                assertEquals("Static segments must be dropped once their sstables are promoted to repaired",
                             0, remaining);
            }));
        }
    }

    /**
     * CASSANDRA-21406 (item 1, tracked -> untracked migration): a keyspace's unrepaired sstables hold journal
     * segments while it is tracked, but once it migrates away from tracked those sstables are never promoted to
     * repaired (that path is gated on the table being tracked). Their references must therefore be evicted on
     * MIGRATE_FROM so the segments can be reclaimed rather than pinned forever.
     */
    @Test(timeout = 120_000)
    public void testSegmentsReleasedWhenKeyspaceMigratesFromTracked() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, "tbl").asserts().success());

            for (int i = 0; i < 50; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                ConsistencyLevel.QUORUM, i, "v" + i);
            }

            cluster.forEach(i -> i.nodetoolResult("flush", KEYSPACE).asserts().success());
            cluster.forEach(i -> i.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty()));

            // Durably reconcile every offset (but keep the flushed sstables unrepaired, since we never compact),
            // driving the cross-node offset exchange to convergence. This satisfies the reconciliation gate (W) so
            // the *only* thing still pinning the segments is the unrepaired sstable references (R) -- making the
            // eviction below the sole reason they can finally drop.
            boolean reconciled = false;
            for (int round = 0; round < 30 && !reconciled; round++)
            {
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting());
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting());

                reconciled = true;
                for (int n = 1; n <= cluster.size(); n++)
                    reconciled &= cluster.get(n).callOnInstance(() ->
                        MutationTrackingService.instance().allStaticSegmentsDurablyReconciledForTesting());
            }
            assertTrue("Reconciliation must converge so only the sstable references still pin the segments", reconciled);

            // While tracked, the (fully reconciled but) unrepaired sstables still hold their segments: a drop pass
            // is a no-op.
            cluster.forEach(i -> i.runOnInstance(() -> {
                int before = MutationJournal.instance().countStaticSegmentsForTesting();
                assertTrue("Expected a static segment held by the unrepaired sstables, got " + before, before > 0);
                MutationTrackingService.instance().persistLogStateForTesting();
                assertEquals("Segments must be retained while unrepaired tracked sstables reference them",
                             before, MutationJournal.instance().countStaticSegmentsForTesting());
            }));

            // Migrate the keyspace away from tracked. tracked->untracked is instant; MIGRATE_FROM evicts the
            // keyspace's sstable references from the segment tracker on every node.
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication_type = 'untracked'"));

            // The eviction released the references; a drop pass now reclaims the segments.
            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));
            cluster.forEach(i -> i.runOnInstance(() -> {
                int remaining = MutationJournal.instance().countStaticSegmentsForTesting();
                assertEquals("Static segments must be dropped after the keyspace migrates away from tracked",
                             0, remaining);
            }));
        }
    }

    /**
     * CASSANDRA-21406 (item 4): under normal operation an unrepaired sstable can hold its journal segments for an
     * unbounded time (compaction, where reconciled sstables are born repaired, may not run for cold tables). When
     * the journal grows past mutation_tracking.journal_promotion_threshold, already durably-reconciled unrepaired
     * sstables are promoted to repaired out of band, releasing their segments even without a compaction.
     */
    @Test(timeout = 120_000)
    public void testSegmentsReleasedBySizeTriggeredPromotion() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                            .set("mutation_tracking.journal_promotion_threshold", "1KiB"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Never let compaction promote the sstables to repaired; only the size-triggered promotion can.
            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, "tbl").asserts().success());

            for (int i = 0; i < 50; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                ConsistencyLevel.QUORUM, i, "v" + i);
            }

            cluster.forEach(i -> i.nodetoolResult("flush", KEYSPACE).asserts().success());
            cluster.forEach(i -> i.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty()));

            // The journal exceeds the tiny threshold and the (unrepaired) flushed sstables are holding segments.
            cluster.forEach(i -> i.runOnInstance(() -> {
                long threshold = DatabaseDescriptor.getMutationTrackingConfig().getJournalPromotionThresholdBytes();
                assertTrue("promotion threshold must be configured (>0), was " + threshold, threshold > 0);
                assertTrue("journal size must exceed the threshold",
                           MutationJournal.instance().getDiskSpaceUsed() > threshold);
                assertTrue("segments must be held by the unrepaired sstables",
                           MutationJournal.instance().countStaticSegmentsForTesting() > 0);
            }));

            // Reconcile (persist then exchange offsets) and trigger size-based promotion until the reconciled
            // sstables are flipped to repaired and their segments reclaimed. Promotion only flips sstables that are
            // already durably reconciled, which requires the offset exchange to have propagated.
            boolean converged = false;
            for (int round = 0; round < 30 && !converged; round++)
            {
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting());
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> {
                        MutationTrackingService.instance().maybePromoteReconciledSSTablesForTesting();
                        MutationTrackingService.instance().persistLogStateForTesting();
                    });

                converged = true;
                for (int n = 1; n <= cluster.size(); n++)
                    converged &= cluster.get(n).callOnInstance(() -> MutationJournal.instance().countStaticSegmentsForTesting() == 0);
            }

            cluster.forEach(i -> i.runOnInstance(() -> {
                int remaining = MutationJournal.instance().countStaticSegmentsForTesting();
                assertEquals("Static segments must be dropped after size-triggered promotion of reconciled sstables",
                             0, remaining);
            }));
        }
    }

    /**
     * A transient (witness) replica journals witnessed writes but never applies them to a memtable, so it never
     * flushes an sstable for them. Such witness-only journal data must not pin {@code needsReplay} (only a flush
     * clears it, so it would otherwise stay set forever) and cannot be reclaimed by the reference path (there
     * is no sstable to promote). Its journal segments are instead governed purely by the reconciliation gate.
     * This exercises the full transient-replication path end to end on a {@code '3/1'} keyspace where every
     * node is a full replica for some ranges and a witness for others: once every offset is durably
     * reconciled and the full-replica sstables are compacted to repaired, every node reclaims all of its static
     * segments, including those holding only witnessed data.
     */
    @Test(timeout = 120_000)
    public void testWitnessSegmentsReleasedOnceReconciled() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                            .set("transient_replication_enabled", "true"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace(
                "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                "AND replication_type = 'tracked'"));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, "tbl").asserts().success());

            for (int i = 0; i < 50; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                ConsistencyLevel.QUORUM, i, "v" + i);
            }

            cluster.forEach(i -> i.nodetoolResult("flush", KEYSPACE).asserts().success());
            cluster.forEach(i -> i.runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty()));

            cluster.forEach(i -> i.runOnInstance(() ->
                assertTrue("Expected static segments after flush+close",
                           MutationJournal.instance().countStaticSegmentsForTesting() > 0)));

            // Reconcile to convergence across full replicas and witnesses.
            boolean reconciled = false;
            for (int round = 0; round < 30 && !reconciled; round++)
            {
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting());
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting());

                reconciled = true;
                for (int n = 1; n <= cluster.size(); n++)
                    reconciled &= cluster.get(n).callOnInstance(() ->
                        MutationTrackingService.instance().allStaticSegmentsDurablyReconciledForTesting());
            }
            assertTrue("Reconciliation must converge across all replicas and witnesses", reconciled);

            // Compact so the full-replica sstables are promoted to repaired and release their segment references.
            // Witness-only segments have no sstable and no pinned needsReplay, so the reconciliation above already
            // made them reclaimable.
            cluster.forEach(i -> i.nodetoolResult("compact", KEYSPACE, "tbl").asserts().success());

            cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));
            cluster.forEach(i -> i.runOnInstance(() ->
                assertEquals("All static segments (including witness-only ranges) must be reclaimed once reconciled",
                             0, MutationJournal.instance().countStaticSegmentsForTesting())));
        }
    }
}
