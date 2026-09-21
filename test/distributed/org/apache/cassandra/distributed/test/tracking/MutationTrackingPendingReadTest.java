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

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tracked.TrackedKeyspaceWriteHandler;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.PartialTrackedIndexRead;
import org.apache.cassandra.service.reads.tracked.PartialTrackedRead;
import org.apache.cassandra.service.reads.tracked.TrackedLocalReads;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertIdsForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertMatchingSummaryIdSpaceForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class MutationTrackingPendingReadTest
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingReadReconciliationTest.class);

    private static void assertKcvRow(ImmutableBTreePartition partition, ColumnFamilyStore cfs, int c, int v)
    {
        Row row = partition.getRow(Clustering.make(bytes(c)));
        Assert.assertNotNull(row);
        Cell<?> cell = Util.cell(cfs, row, "v");
        Assert.assertEquals(bytes(v), cell.buffer());
    }

    private static void assertKcvRow(Row row, ColumnFamilyStore cfs, int c, int v)
    {
        Assert.assertNotNull(row);
        Assert.assertEquals(bytes(c), row.clustering().bufferAt(0));
        Cell<?> cell = Util.cell(cfs, row, "v");
        Assert.assertEquals(bytes(v), cell.buffer());
    }

    private static void assertNoKcvRow(ImmutableBTreePartition partition, int c)
    {
        Row row = partition.getRow(Clustering.make(bytes(c)));
        Assert.assertNull(row);
    }

    /**
     * Tests that pending writes are included in read responses
     */
    @Test
    public void testPendingWriteInclusion() throws Throwable
    {

        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "pending_write_inclusion_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));


            // insert a row at all, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);

            MutationSummary firstSummary = summaryForKey(cluster.get(1), keyspaceName, "tbl", 1);
            CoordinatorLogId logId = firstSummary.get(0).logId();
            Offsets firstIds = summaryIdSpace(firstSummary.get(logId));
            Assert.assertEquals(1, firstIds.offsetCount());

            cluster.forEach(node -> assertMatchingSummaryIdSpaceForKey(node, keyspaceName, "tbl", 1, firstSummary));

            cluster.get(1).runOnInstance(() -> {

                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                DecoratedKey dk = metadata.partitioner.decorateKey(bytes(1));

                MutationSummary secondSummary = summaryForKey(keyspaceName, tableName, dk);
                Offsets secondIds = summaryIdSpace(secondSummary.get(logId));
                Assert.assertEquals(1, secondIds.offsetCount());

                // create a mutation
                MutationId id = MutationTrackingService.instance().nextMutationId(keyspaceName, dk.getToken());
                SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, keyspaceName, dk);
                PartitionUpdate.SimpleBuilder tableBuilder = builder.update(metadata);
                tableBuilder.row(bytes(1)).add("v", 1);
                Mutation mutation = builder.build();
                MutationId secondId = mutation.id();
                Assert.assertFalse(secondId.isNone());

                int nowInSeconds = (int) FBUtilities.nowInSeconds();
                // apply it to the journal and open a pending write
                PartialTrackedRead read;
                MutationSummary initialSummary;
                MutationSummary secondarySummary;

                SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, nowInSeconds, dk);
                TrackedKeyspaceWriteHandler trackedWriteHandler = new TrackedKeyspaceWriteHandler();
                try (WriteContext ctx = trackedWriteHandler.beginWrite(mutation, true))
                {


                    initialSummary = command.createMutationSummary(false);
                    ReadExecutionController controller = command.executionController(false);

                    MutationTrackingService.instance().startWriting(mutation);

                    read = command.beginTrackedRead(controller);
                    // Create another summary once initial data has been read fully. We do this to catch
                    // any mutations that may have arrived during initial read execution.
                    secondarySummary = command.createMutationSummary(true);
                    TrackedLocalReads.processDelta(read, initialSummary, secondarySummary);
                }

                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                // check that the memtable doesn't somehow contain the unapplied mutation
                ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, dk));
                Assert.assertTrue(view.sstables.isEmpty());
                try (UnfilteredRowIterator rowIterator = Iterables.getOnlyElement(view.memtables).rowIterator(dk))
                {
                    ImmutableBTreePartition partition = ImmutableBTreePartition.create(rowIterator);
                    assertKcvRow(partition, cfs, 0, 0);
                    assertNoKcvRow(partition, 1);
                }

                // confirm that the initial summary was not aware of the unapplied mutation
                Offsets initialIds = summaryIdSpace(initialSummary.get(logId));
                Assert.assertEquals(1, initialIds.offsetCount());
                Assert.assertFalse(initialIds.contains(secondId.offset()));

                // check that the summary is aware of the unapplied mutation
                Offsets summaryIds = summaryIdSpace(secondarySummary.get(logId));
                Assert.assertEquals(2, summaryIds.offsetCount());
                Assert.assertTrue(summaryIds.contains(secondId.offset()));

                // check that the returned data contains the unapplied mutation
                try (PartialTrackedRead.CompletedRead completedRead = read.complete();
                     PartitionIterator partitions = completedRead.response().makeIterator(command))
                {
                    Assert.assertTrue(partitions.hasNext());
                    try (RowIterator rowIterator = partitions.next())
                    {
                        Assert.assertTrue(rowIterator.hasNext());
                        assertKcvRow(rowIterator.next(), cfs, 0, 0);

                        Assert.assertTrue(rowIterator.hasNext());
                        assertKcvRow(rowIterator.next(), cfs, 1, 1);

                        Assert.assertFalse(rowIterator.hasNext());
                    }
                    Assert.assertFalse(partitions.hasNext());
                }
            });
        }
    }

    /**
     * Confirms that reads are notified of writes that come in while a read is inflight
     */
    @Test
    public void testPendingReadInclusion() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "pending_read_inclusion_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // check that there aren't any mutations for the given key
            cluster.forEach(node -> {
                assertIdsForKey(node, keyspaceName, tableName, 1, Collections.emptySet());
            });


            cluster.get(1).runOnInstance(() -> {
                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                DecoratedKey dk = metadata.partitioner.decorateKey(bytes(1));


                int nowInSeconds = (int) FBUtilities.nowInSeconds();
                SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, nowInSeconds, dk);
//                try (ListeningPendingRead pendingRead = (ListeningPendingRead) MutationTrackingService.instance().startReading(command))
//                {
//                    Assert.assertTrue(pendingRead.mutationIds().isEmpty());
//
//                    // create and apply a mutation
//                    MutationId id = MutationTrackingService.instance().nextMutationId(keyspaceName, dk.getToken());
//                    SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, keyspaceName, dk);
//                    PartitionUpdate.SimpleBuilder tableBuilder = builder.update(metadata);
//                    tableBuilder.row(bytes(1)).add("v", 1);
//                    Mutation mutation = builder.build();
//                    mutation.apply();
//
//                    // the in flight read should be aware of the racing write
//                    Assert.assertEquals(Set.of(mutation.id()), pendingRead.mutationIds());
//                }
            });
        }
    }

    /**
     * A tracked index range read snapshots a partition per key its own index scan reached, so a key reconciliation
     * hands it afterwards has no read behind it: it starts a follow up read of that key, which opens a
     * {@link ReadExecutionController} - and so an {@link OpOrder.Group} on the base table and its index table - of its
     * own. Abandoning the range read rather than completing it (TrackedLocalReads purging it once past its deadline,
     * or beginReadInternal failing after the delta was processed) closes the range read, and that close has to close
     * the follow up reads with it. A follow up read whose future succeeded is exactly the one that has a read to
     * close; a failed one never built one. Closing the failed ones instead leaves the controller open forever, and an
     * OpOrder.Group that never closes blocks every subsequent memtable flush and compaction of the table for the life
     * of the node.
     */
    @Test
    public void testAbandonedIndexRangeReadClosesItsFollowUpReads() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true))
                                      .start())
        {
            String keyspaceName = "follow_up_read_close_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));
            cluster.schemaChange(format("CREATE INDEX tbl_v ON %s.%s(v) USING 'legacy_local_table';", keyspaceName, tableName));

            // a row for the range read's own index scan to reach, so the read starts from the state it would have in
            // production rather than from one that scanned nothing
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 1)", keyspaceName, tableName), ConsistencyLevel.ALL);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                awaitCondition(() -> cfs.getBuiltIndexes().contains("tbl_v"), "Index tbl_v was never built");

                RowFilter rowFilter = RowFilter.create(false);
                rowFilter.add(metadata.getColumn(bytes("v")), Operator.EQ, bytes(1));
                PartitionRangeReadCommand command = PartitionRangeReadCommand.create(metadata,
                                                                                     FBUtilities.nowInSeconds(),
                                                                                     ColumnFilter.all(metadata),
                                                                                     rowFilter,
                                                                                     DataLimits.NONE,
                                                                                     DataRange.allData(metadata.partitioner));
                Assert.assertNotNull("v is indexed, so this range read has to have picked an index", command.indexQueryPlan());

                ReadExecutionController controller = command.executionController(false);
                PartialTrackedRead read = command.beginTrackedRead(controller);
                Assert.assertTrue("Expected an index read, got " + read.getClass().getName(),
                                  read instanceof PartialTrackedIndexRead);
                ((PartialTrackedIndexRead<?, ?>) read).setFollowUpReadContext(org.apache.cassandra.db.ConsistencyLevel.ALL,
                                                                             Dispatcher.RequestTime.forImmediateExecution());

                // a key the scan above cannot have reached, because it is only written now
                DecoratedKey unscanned = metadata.partitioner.decorateKey(bytes(2));
                MutationId id = MutationTrackingService.instance().nextMutationId(keyspaceName, unscanned.getToken());
                SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, keyspaceName, unscanned);
                builder.update(metadata).row(bytes(1)).add("v", 1);
                Mutation mutation = builder.build();
                mutation.apply();

                // augmenting with a matching mutation for a key that has no read behind it is what starts the follow
                // up read, and TrackedLocalReads marks a reconcile once that read has a controller of its own
                long reconcilesBefore = ReadRepairMetrics.trackedReconcile.getCount();
                read.augment(mutation);
                awaitCondition(() -> ReadRepairMetrics.trackedReconcile.getCount() > reconcilesBefore,
                               "The follow up read never began, so there was no second controller to leak and this " +
                               "test would have passed without asserting anything");

                // abandon the range read instead of completing it
                read.close();

                // the range read's own controller closed above, and the follow up read's is either closed already or
                // has a callback registered to close it as soon as its future completes, so the barrier has to drain
                OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
                barrier.issue();
                awaitCondition(barrier.getSyncPoint()::isFinished,
                               "Closing the range read left the follow up read's ReadExecutionController open, so its " +
                               "OpOrder.Group blocks every flush and compaction of " + tableName + " from here on");
            });
        }
    }

    private static void awaitCondition(BooleanSupplier condition, String message)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean() && System.nanoTime() - deadline < 0)
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        Assert.assertTrue(message, condition.getAsBoolean());
    }
}
