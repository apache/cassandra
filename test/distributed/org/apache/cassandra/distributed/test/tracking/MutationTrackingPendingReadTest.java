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
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

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
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.ActiveLogReconciler;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.PartialTrackedIndexRead;
import org.apache.cassandra.service.reads.tracked.PartialTrackedRead;
import org.apache.cassandra.service.reads.tracked.TrackedDataResponse;
import org.apache.cassandra.service.reads.tracked.TrackedLocalReads;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertMatchingSummaryIdSpaceForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.decodeSummary;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.encodeSummary;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.row;
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

                DecoratedKey unscanned = metadata.partitioner.decorateKey(bytes(2));
                MutationId id = MutationTrackingService.instance().nextMutationId(keyspaceName, unscanned.getToken());
                SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, keyspaceName, unscanned);
                builder.update(metadata).row(bytes(1)).add("v", 1);
                Mutation mutation = builder.build();
                mutation.apply();

                // augment starts a FollowUpRead for key 2, written after beginTrackedRead; beginReadInternal marks
                // trackedReconcile once that read has its own controller
                long reconcilesBefore = ReadRepairMetrics.trackedReconcile.getCount();
                read.augment(mutation);
                awaitCondition(() -> ReadRepairMetrics.trackedReconcile.getCount() > reconcilesBefore,
                               "The follow up read never began, so there was no second controller to leak and this " +
                               "test would have passed without asserting anything");

                read.close();

                // read.close() closes the FollowUpRead only once its future succeeds, so this barrier also waits for
                // that read to finish
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

    /**
     * A mutation can be applied after ReadReconciliations.acceptRemoteSummary finds it missing but before pull
     * registers its listener; only the rejected duplicate that pull fetches can then notify that listener.
     */
    @Test
    public void testDuplicateWriteNotifiesMutationListeners() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "duplicate_write_notification_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            cluster.get(1).runOnInstance(() -> {
                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                DecoratedKey dk = metadata.partitioner.decorateKey(bytes(1));

                MutationId id = MutationTrackingService.instance().nextMutationId(keyspaceName, dk.getToken());
                SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, keyspaceName, dk);
                builder.update(metadata).row(bytes(1)).add("v", 1);
                Mutation mutation = builder.build();

                mutation.apply();

                Assert.assertFalse("An already witnessed mutation was not recognized as a duplicate",
                                   MutationTrackingService.instance().startWriting(mutation));

                AtomicInteger notified = new AtomicInteger();
                Assert.assertTrue("Expected to be the first listener registered for this id",
                                  MutationTrackingService.instance().registerMutationCallback(mutation.id(), notifiedId -> notified.incrementAndGet()));

                mutation.apply();

                Assert.assertEquals("A duplicate mutation left a read reconciliation listener waiting",
                                    1, notified.get());
            });
        }
    }

    /**
     * The two mutations are in two coordinator logs, so one {@code ReadReconciliations.Coordinator.acceptRemoteSummary}
     * call makes two {@code pull} calls. The first MT_PULL_MUTATIONS_REQ is held in an outbound filter, which runs on
     * the sending thread and so parks that call between its two pulls, until node 1 has witnessed both mutations.
     */
    @Test
    public void testReadCompletesWhenThePulledMutationsArriveAsDuplicates() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true)
                                                            // background pulls from node 1 use MT_PULL_MUTATIONS_REQ,
                                                            // which the filter below counts and holds
                                                            .set("mutation_tracking.background_reconciliation_enabled", false)
                                                            .set("hinted_handoff_enabled", false)
                                                            // the read blocks during delivery; IncomingMutations
                                                            // listeners expire after write_request_timeout
                                                            .set("read_request_timeout", "10000ms")
                                                            .set("write_request_timeout", "10000ms"))
                                      .start())
        {
            // MutationTrackingService.retryFailedWrite would deliver the dropped writes to node 1 at Priority.REGULAR
            // even with background_reconciliation_enabled false; the read's pulls and the requests below use HIGH
            cluster.forEach(() -> MutationTrackingService.instance().pauseActiveReconcilerRegularPriority());

            String keyspaceName = "duplicate_pull_notification_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            String select = format("SELECT k, c, v FROM %s.%s WHERE k = 1", keyspaceName, tableName);

            IMessageFilters.Filter writesToNode1 = cluster.filters().verbs(Verb.MUTATION_REQ.id).to(1).drop();
            cluster.coordinator(2).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            cluster.coordinator(3).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 2, 2)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            writesToNode1.off();

            assertRows(cluster.get(1).executeInternal(select));
            assertRows(cluster.get(2).executeInternal(select), row(1, 1, 1), row(1, 2, 2));
            assertRows(cluster.get(3).executeInternal(select), row(1, 1, 1), row(1, 2, 2));

            byte[] remoteSummary = cluster.get(2).callOnInstance(() -> encodeSummary(summaryForKey(keyspaceName, tableName, 1)));

            Set<Integer> logOwners = new HashSet<>();
            summaryIdSpace(decodeSummary(remoteSummary)).forEach((logId, offsets) -> {
                if (offsets.isEmpty())
                    return;
                Assert.assertEquals("Expected one mutation in each coordinator log", 1, offsets.offsetCount());
                logOwners.add(logId.hostId());
            });
            Assert.assertEquals("Both writes are in one coordinator log, so acceptRemoteSummary would make a single " +
                                "pull call and there would be no window between deciding a mutation is missing and " +
                                "registering the listener for it",
                                ImmutableSet.of(2, 3), logOwners);

            AtomicInteger summaryRequests = new AtomicInteger();
            cluster.filters().verbs(Verb.MT_SUMMARY_REQ.id).from(1).outbound().messagesMatching((from, to, message) -> {
                summaryRequests.incrementAndGet();
                return false;
            }).drop();

            AtomicInteger pullRequests = new AtomicInteger();
            AtomicBoolean held = new AtomicBoolean();
            CountDownLatch parked = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            cluster.filters().verbs(Verb.MT_PULL_MUTATIONS_REQ.id).from(1).outbound().messagesMatching((from, to, message) -> {
                pullRequests.incrementAndGet();
                if (held.compareAndSet(false, true))
                {
                    parked.countDown();
                    Uninterruptibles.awaitUninterruptibly(release);
                }
                return false;
            }).drop();

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try
            {
                Future<Object[][]> read = executor.submit(() -> cluster.coordinator(1).execute(select, ConsistencyLevel.QUORUM));

                long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
                while (!parked.await(50, TimeUnit.MILLISECONDS))
                {
                    if (read.isDone())
                    {
                        read.get();
                        throw new AssertionError("The read completed without ever sending a pull request, so it found " +
                                                 "nothing to reconcile and could not have been left waiting on one");
                    }
                    Assert.assertTrue("The read neither sent a pull request nor completed", System.nanoTime() - deadline < 0);
                }

                try
                {
                    // Deliver to node 1 as PullMutationsRequest's verb handler does. Not retryFailedWrite: it waits for
                    // write_request_timeout, which is also how long an IncomingMutations listener lives.
                    for (int node : new int[]{ 2, 3 })
                        cluster.get(node).runOnInstance(() -> {
                            ClusterMetadata metadata = ClusterMetadata.current();
                            InetAddressAndPort dataReplica = metadata.directory.endpoint(new NodeId(1));
                            int localNode = metadata.directory.peerId(FBUtilities.getBroadcastAddressAndPort()).id();
                            MutationTrackingService.instance().forEachShardInKeyspace(keyspaceName, shard ->
                                shard.collectUnionOfWitnessedOffsetsPerLog().forEach((logId, offsets) -> {
                                    if (logId.hostId() == localNode && !offsets.isEmpty())
                                        MutationTrackingService.instance().requestMissingMutations(offsets, dataReplica, ActiveLogReconciler.Priority.HIGH);
                                }));
                        });

                    // Wait on what node 1 has witnessed, which is what startWriting checks, not on a read of node 1:
                    // updates are applied before finishWriting records the offset.
                    awaitCondition(() -> cluster.get(1).callOnInstance(() -> {
                                       Log2OffsetsMap.Mutable stillMissing = new Log2OffsetsMap.Mutable();
                                       MutationTrackingService.instance().collectLocallyMissingMutations(decodeSummary(remoteSummary), stillMissing);
                                       return stillMissing.idCount() == 0;
                                   }),
                                   "Node 1 never witnessed the mutations, so the copies the read pulls would not have " +
                                   "been duplicates and this test would prove nothing");
                    assertRows(cluster.get(1).executeInternal(select), row(1, 1, 1), row(1, 2, 2));
                }
                finally
                {
                    release.countDown();
                }

                Object[][] result;
                try
                {
                    result = read.get(1, TimeUnit.MINUTES);
                }
                catch (ExecutionException e)
                {
                    throw new AssertionError("The read failed instead of returning the partition: both copies it " +
                                             "pulled arrived as duplicates of copies already in the table, and a " +
                                             "duplicate that notifies no listener leaves the read's reconciliation " +
                                             "one mutation short for good", e.getCause());
                }
                catch (TimeoutException e)
                {
                    throw new AssertionError("The read never returned at all, so it was left waiting on a mutation " +
                                             "whose only remaining delivery was a duplicate", e);
                }
                assertRows(result, row(1, 1, 1), row(1, 2, 2));

                Assert.assertEquals("Expected one pull request per coordinator log", 2, pullRequests.get());
                // a second summary replica's acceptRemoteSummary call could register the second log's listener before
                // nodes 2 and 3 deliver its mutation, so no duplicate would notify it
                Assert.assertEquals("Expected exactly one summary replica", 1, summaryRequests.get());
            }
            finally
            {
                executor.shutdownNow();
            }
        }
    }

    /**
     * {@link TrackedLocalReads#acknowledgeReconcile} removes the read's coordinator before augmenting, so when
     * augmenting fails, the failure handler must close the read.
     */
    @Test
    public void testFailedAugmentClosesTheReconciledRead() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true))
                                      .start())
        {
            String keyspaceName = "failed_augment_close_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 2} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // the read stage rethrows the augment failure after failing the read's promise
            cluster.setUncaughtExceptionsFilter(t -> t.getMessage() != null && t.getMessage().startsWith("Missing mutation"));

            // node 2 is the read's only summary node; while it is down the reconciliation stays pending until the
            // test acknowledges it by hand
            cluster.get(2).shutdown().get();

            cluster.get(1).runOnInstance(() -> {
                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                ClusterMetadata clusterMetadata = ClusterMetadata.current();
                NodeId summaryNode = Iterables.getOnlyElement(Sets.difference(clusterMetadata.directory.peerIds(),
                                                                             Collections.singleton(clusterMetadata.myNodeId())));

                TrackedRead.Id readId = TrackedRead.Id.nextId();
                PartitionRangeReadCommand command = PartitionRangeReadCommand.allDataRead(metadata, FBUtilities.nowInSeconds());
                AsyncPromise<TrackedDataResponse> promise =
                    MutationTrackingService.instance().localReads().beginRead(readId,
                                                                             clusterMetadata,
                                                                             command,
                                                                             org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                             new int[]{ summaryNode.id() },
                                                                             Dispatcher.RequestTime.forImmediateExecution(),
                                                                             TrackedLocalReads.Completer.DEFAULT);

                Assert.assertFalse("The read reconciled without a summary from its summary node, so the " +
                                   "acknowledgement below is a no-op and this test would have passed without " +
                                   "asserting anything",
                                   promise.isDone());

                DecoratedKey dk = metadata.partitioner.decorateKey(bytes(1));
                MutationId unwritten = MutationTrackingService.instance().nextMutationId(keyspaceName, dk.getToken());
                Log2OffsetsMap.Mutable augmenting = new Log2OffsetsMap.Mutable();
                augmenting.add(new ShortMutationId(unwritten));
                MutationTrackingService.instance().localReads().acknowledgeReconcile(readId, augmenting);

                awaitCondition(promise::isDone, "Acknowledging the reconciliation neither completed nor failed the read");
                Assert.assertNotNull("Expected the read to fail on the mutation missing from the journal", promise.cause());
                Assert.assertTrue("Expected the read to fail on the mutation missing from the journal, got " + promise.cause(),
                                  promise.cause().getMessage().contains("Missing mutation"));

                OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
                barrier.issue();
                awaitCondition(barrier.getSyncPoint()::isFinished,
                               "Failing to augment the read left its ReadExecutionController open, so its " +
                               "OpOrder.Group blocks every flush and compaction of " + tableName + " from here on");
            });
        }
    }

    /**
     * A tracked index range read whose follow up read fails is never completed and never closed, and keeps the base
     * table's read ordering open for the life of the node.
     * <p>
     * <b>How a follow up read starts.</b> An index range read only snapshots the partitions its own index scan reached
     * in the prepare phase. When reconciliation later hands it a mutation for a key the scan never reached and that
     * mutation matches the index expression, {@code PartialTrackedIndexRead.IndexPrepared#augment} has no partition
     * read to apply it to. It calls {@code FollowUpRead.start} instead, which starts a {@link TrackedRead.Partition} of
     * that key through {@code TrackedRead#startLocal} and stores the returned {@code followUpPromise} in the range
     * read's {@code followUpReads}. The follow up read is a tracked read in its own right. It has its own id, its own
     * {@link ReadExecutionController} (so its own {@link OpOrder.Group} on the base table and index table), its own
     * entry in {@link TrackedLocalReads}, and its own summary nodes to reconcile with.
     * <p>
     * <b>How the range read waits on it.</b> {@link TrackedLocalReads#acknowledgeReconcile} takes the range read's
     * coordinator out of the map, augments the read (which is what starts the follow up read), and then runs the
     * completer. {@code PartialTrackedIndexRead#complete} finds a follow up future that is not done. It moves the
     * read to {@code IndexPreComplete} and registers a listener on {@code FutureCombiner.allOf(followUpReads)}, and that
     * listener is the only thing that will complete the range read's promise and close the read. The range read's
     * coordinator is already out of {@link TrackedLocalReads}, so {@code expire()} can no longer abort it.
     * <p>
     * <b>The bug.</b> {@code followUpPromise} is completed only from inside the custom completer that
     * {@code FollowUpRead.start} hands to {@code startLocal}. A completer only runs once the follow up read has
     * reconciled and augmented successfully, so any follow up read that fails before then leaves {@code followUpPromise}
     * pending forever:
     * <ul>
     *     <li>its augment throws, for example "Missing mutation" for an id absent from the local mutation journal,
     *     which {@code PartialTrackedRead#augment(ShortMutationId)} documents as reachable through a newly activated
     *     transfer. The failure handler in {@code TrackedLocalReads.Coordinator#acknowledgeReconcile} closes the follow
     *     up read and fails its promise. That promise is only watched by the callback in {@code TrackedRead#start},
     *     which logs the error and does nothing else ("TODO: notify coordinator that read has failed"), so neither
     *     {@code TrackedRead#future} nor {@code followUpPromise} ever hears about it;</li>
     *     <li>it expires because a summary node never answers in time. {@code TrackedLocalReads#expire} calls
     *     {@code Coordinator#abort}, which closes the follow up read but completes nothing at all;</li>
     *     <li>{@code beginRead} throws inside the READ stage task that {@code TrackedRead#start} submits, before any
     *     promise exists.</li>
     * </ul>
     * <b>The consequence.</b> The range read's listener never fires, so its promise never completes and the client
     * waits out its timeout. Worse, the range read is never closed, and nothing else can close it, so its
     * {@link ReadExecutionController} and the {@link OpOrder.Group} it holds on the base table's {@code readOrdering}
     * stay open for the life of the node. Every later barrier on that ordering waits forever, and with it every
     * memtable flush and compaction of the table.
     * <p>
     * A related gap, not exercised here: if the {@code FollowUpRead} constructor throws inside that completer (from
     * {@code read.complete()} or the {@code partitionRead(key)} null check), the completer fails {@code followUpPromise}
     * but never closes the follow up read. It swallows the exception, so the failure handler in
     * {@link TrackedLocalReads} does not see it either, and the follow up read's controller leaks.
     * <p>
     * <b>What the test does.</b> Two nodes, RF=2, a legacy index on {@code v}. {@code MT_SUMMARY_REQ} from node 1 to
     * node 2 is dropped, so the follow up read, which takes node 2 as its summary node, stays pending until the test
     * acts. The test begins an index range read directly through {@link TrackedLocalReads#beginRead}, writes a matching
     * row to a key that read's scan cannot have reached, and acknowledges the range read's reconciliation with that
     * write's id, which starts the follow up read. It then fails the follow up read, either by acknowledging it with an
     * id that was never written or by waiting for the purger to expire it. It expects the range read's promise to
     * fail and a fresh barrier on the table's read ordering to drain. If the barrier does not drain, the test closes
     * the range read by hand before failing. That shows the range read is the only thing still holding the ordering,
     * and lets the cluster shut down.
     */
    @Test
    public void testFollowUpReadAugmentFailureCompletesItsIndexRangeRead() throws Throwable
    {
        runFailedFollowUpRead("follow_up_augment_failure_test", false);
    }

    /**
     * Same as {@link #testFollowUpReadAugmentFailureCompletesItsIndexRangeRead}, but the follow up read fails by
     * expiring: its summary node never answers, and {@code TrackedLocalReads#expire} aborts it. {@code abort()} closes
     * the follow up read without completing any promise, so {@code followUpPromise} stays pending and the range read
     * waiting on it leaks as above. All it takes is a summary node that answers late, with no inconsistency in the
     * journal.
     */
    @Test
    public void testFollowUpReadExpiryCompletesItsIndexRangeRead() throws Throwable
    {
        runFailedFollowUpRead("follow_up_expiry_test", true);
    }

    private static void runFailedFollowUpRead(String keyspaceName, boolean expireFollowUp) throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true)
                                                            .set("read_request_timeout", "2000ms")
                                                            .set("range_request_timeout", "2000ms"))
                                      .start())
        {
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 2} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));
            cluster.schemaChange(format("CREATE INDEX tbl_v ON %s.%s(v) USING 'legacy_local_table';", keyspaceName, tableName));
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 1)", keyspaceName, tableName), ConsistencyLevel.ALL);

            cluster.setUncaughtExceptionsFilter(MutationTrackingPendingReadTest::isFailedFollowUpRead);

            cluster.filters().verbs(Verb.MT_SUMMARY_REQ.id).from(1).to(2).drop();

            cluster.get(1).runOnInstance(() -> {
                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                awaitCondition(() -> cfs.getBuiltIndexes().contains("tbl_v"), "Index tbl_v was never built");
                ClusterMetadata clusterMetadata = ClusterMetadata.current();
                NodeId summaryNode = Iterables.getOnlyElement(Sets.difference(clusterMetadata.directory.peerIds(),
                                                                             Collections.singleton(clusterMetadata.myNodeId())));
                TrackedLocalReads localReads = MutationTrackingService.instance().localReads();
                Map<TrackedRead.Id, ?> pendingReads = pendingLocalReads(localReads);

                RowFilter rowFilter = RowFilter.create(false);
                rowFilter.add(metadata.getColumn(bytes("v")), Operator.EQ, bytes(1));
                PartitionRangeReadCommand command = PartitionRangeReadCommand.create(metadata,
                                                                                     FBUtilities.nowInSeconds(),
                                                                                     ColumnFilter.all(metadata),
                                                                                     rowFilter,
                                                                                     DataLimits.NONE,
                                                                                     DataRange.allData(metadata.partitioner));

                TrackedRead.Id rangeReadId = TrackedRead.Id.nextId();
                AtomicReference<PartialTrackedRead> rangeRead = new AtomicReference<>();
                AsyncPromise<TrackedDataResponse> promise =
                    localReads.beginRead(rangeReadId,
                                         clusterMetadata,
                                         command,
                                         org.apache.cassandra.db.ConsistencyLevel.ALL,
                                         new int[]{ summaryNode.id() },
                                         Dispatcher.RequestTime.forImmediateExecution(),
                                         rangeRead::set,
                                         TrackedLocalReads.Completer.DEFAULT);
                Assert.assertFalse("The range read reconciled before its summary node answered", promise.isDone());
                Assert.assertTrue("Expected an index read, got " + rangeRead.get(), rangeRead.get() instanceof PartialTrackedIndexRead);

                DecoratedKey unscanned = metadata.partitioner.decorateKey(bytes(2));
                MutationId written = MutationTrackingService.instance().nextMutationId(keyspaceName, unscanned.getToken());
                SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(written, keyspaceName, unscanned);
                builder.update(metadata).row(bytes(1)).add("v", 1);
                builder.build().apply();
                Assert.assertNotNull("The write never reached the mutation journal",
                                     MutationJournal.instance().read(new ShortMutationId(written)));

                Log2OffsetsMap.Mutable rangeAugmenting = new Log2OffsetsMap.Mutable();
                rangeAugmenting.add(new ShortMutationId(written));
                localReads.acknowledgeReconcile(rangeReadId, rangeAugmenting);

                awaitCondition(() -> !pendingReads.isEmpty(), "The follow up read never began");
                TrackedRead.Id followUpId = Iterables.getOnlyElement(pendingReads.keySet());
                Assert.assertNotEquals(rangeReadId, followUpId);
                Assert.assertFalse("The range read completed before its follow up read did", promise.isDone());

                if (expireFollowUp)
                {
                    awaitCondition(() -> !pendingReads.containsKey(followUpId), "The follow up read never expired");
                }
                else
                {
                    MutationId unwritten = MutationTrackingService.instance().nextMutationId(keyspaceName, unscanned.getToken());
                    Log2OffsetsMap.Mutable followUpAugmenting = new Log2OffsetsMap.Mutable();
                    followUpAugmenting.add(new ShortMutationId(unwritten));
                    localReads.acknowledgeReconcile(followUpId, followUpAugmenting);
                }

                boolean rangeReadCompleted = await(promise::isDone, 10);
                OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
                barrier.issue();
                boolean drained = await(barrier.getSyncPoint()::isFinished, 10);

                boolean drainedOnceRangeReadClosed = drained;
                if (!drained)
                {
                    rangeRead.get().close();
                    drainedOnceRangeReadClosed = await(barrier.getSyncPoint()::isFinished, 10);
                }

                Assert.assertTrue(format("The failed follow up read left its range read pending: range read completed=%s, " +
                                         "barrier drained=%s, barrier drained once the range read was closed by hand=%s",
                                         rangeReadCompleted, drained, drainedOnceRangeReadClosed),
                                  rangeReadCompleted && drained);
                Assert.assertNotNull("The range read succeeded without the follow up read it depended on", promise.cause());
            });
        }
    }

    private static boolean isFailedFollowUpRead(Throwable t)
    {
        for (Throwable cause = t; cause != null; cause = cause.getCause())
        {
            if (cause instanceof TimeoutException)
                return true;
            if (cause.getMessage() != null && cause.getMessage().startsWith("Missing mutation"))
                return true;
        }
        return false;
    }

    private static boolean await(BooleanSupplier condition, long seconds)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean() && System.nanoTime() - deadline < 0)
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        return condition.getAsBoolean();
    }

    @SuppressWarnings("unchecked")
    private static Map<TrackedRead.Id, ?> pendingLocalReads(TrackedLocalReads localReads)
    {
        try
        {
            Field coordinators = TrackedLocalReads.class.getDeclaredField("coordinators");
            coordinators.setAccessible(true);
            return (Map<TrackedRead.Id, ?>) coordinators.get(localReads);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * {@link ReadExecutionController#close()} is not idempotent, and a second close releases the table's read
     * {@link OpOrder.Group} again, so a flush barrier finishes while another read on that group is still running.
     */
    @Test
    public void testAbortedReadReleasesItsExecutionControllerOnce() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true))
                                      .withInstanceInitializer(FailSecondarySummary.install(1))
                                      .start())
        {
            String keyspaceName = "aborted_read_controller_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 2} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            cluster.get(1).runOnInstance(() -> {
                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);

                OpOrder.Barrier drained = cfs.readOrdering.newBarrier();
                drained.issue();
                awaitCondition(drained.getSyncPoint()::isFinished,
                               "An operation on " + tableName + "'s read ordering outlived the read that started it, " +
                               "so the assertions below cannot tell one release too many from one live read");

                OpOrder.Group concurrentRead = cfs.readOrdering.start();

                FailSecondarySummary.failingTable = tableName;
                try
                {
                    MutationTrackingService.instance().localReads().beginRead(TrackedRead.Id.nextId(),
                                                                             ClusterMetadata.current(),
                                                                             PartitionRangeReadCommand.allDataRead(metadata, FBUtilities.nowInSeconds()),
                                                                             org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                             new int[0],
                                                                             Dispatcher.RequestTime.forImmediateExecution(),
                                                                             TrackedLocalReads.Completer.DEFAULT);
                    Assert.fail("The read began without its secondary summary failing, so it never took the abort " +
                                "path and this test would have passed without asserting anything");
                }
                catch (RuntimeException e)
                {
                    Assert.assertEquals(FailSecondarySummary.MESSAGE, e.getMessage());
                }
                finally
                {
                    FailSecondarySummary.failingTable = null;
                }

                OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
                barrier.issue();
                Assert.assertFalse("Aborting the read released the read ordering group it shares with a running read " +
                                   "twice, so a flush of " + tableName + " would proceed underneath that read",
                                   barrier.getSyncPoint().isFinished());

                concurrentRead.close();
                Assert.assertTrue("The barrier did not finish once the only read left on its group closed, so it was " +
                                  "never waiting on that read and the assertion above held for the wrong reason",
                                  barrier.getSyncPoint().isFinished());
            });
        }
    }

    /**
     * PartialTrackedRangeRead.create reads the initial data before beginTrackedRead returns the read to
     * beginReadInternal. A failure there reaches beginReadInternal's abort with no read to close, so the abort closes
     * the controller itself, and create must not close it as well.
     */
    @Test
    public void testReadFailingOnACorruptSSTableReleasesItsExecutionControllerOnce() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true))
                                      .start())
        {
            String keyspaceName = "corrupt_read_controller_test";
            String tableName = "tbl";
            createTrackedTableWithCorruptSSTable(cluster, keyspaceName, tableName);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                drainReadOrdering(cfs);

                OpOrder.Group concurrentRead = cfs.readOrdering.start();
                beginReadOfCorruptSSTable(cfs);

                OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
                barrier.issue();
                Assert.assertFalse("The read that failed on the corrupt sstable released the read ordering group it " +
                                   "shares with a running read twice, so a flush of " + tableName +
                                   " would proceed underneath that read",
                                   barrier.getSyncPoint().isFinished());

                concurrentRead.close();
                Assert.assertTrue("The barrier did not finish once the only read left on its group closed, so it was " +
                                  "never waiting on that read and the assertion above held for the wrong reason",
                                  barrier.getSyncPoint().isFinished());
            });
        }
    }

    /**
     * With no other read on the table, releasing the failed read's {@link OpOrder.Group} twice takes its count below
     * zero while it is still the group new reads join. OpOrder reads a negative count as expired, so every later
     * readOrdering.start() on the table spins until a barrier replaces the group, and that barrier's issue() throws
     * because the group it expires is already marked expired.
     */
    @Test
    public void testReadFailingOnACorruptSSTableLeavesLaterReadsAbleToStart() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true))
                                      .start())
        {
            String keyspaceName = "corrupt_read_ordering_test";
            String tableName = "tbl";
            createTrackedTableWithCorruptSSTable(cluster, keyspaceName, tableName);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                drainReadOrdering(cfs);
                beginReadOfCorruptSSTable(cfs);

                Thread laterRead = new Thread(() -> cfs.readOrdering.start().close(), "later-read-of-" + tableName);
                laterRead.setDaemon(true);
                laterRead.start();
                Uninterruptibles.joinUninterruptibly(laterRead, 10, TimeUnit.SECONDS);
                boolean laterReadStarted = !laterRead.isAlive();

                // issue() replaces the group before it expires the old one, so it releases a read spinning on the old
                // group even when expiring that group throws
                OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
                IllegalStateException issueFailure = null;
                try
                {
                    barrier.issue();
                }
                catch (IllegalStateException e)
                {
                    issueFailure = e;
                }
                Uninterruptibles.joinUninterruptibly(laterRead, 60, TimeUnit.SECONDS);

                String afterBarrier = format("issuing a barrier %s, and the later read %s once it had",
                                             issueFailure == null ? "succeeded" : "threw " + issueFailure,
                                             laterRead.isAlive() ? "was still waiting" : "started");
                Assert.assertTrue("The read that failed on the corrupt sstable released its read ordering group twice, " +
                                  "leaving the group current but looking expired, so a later read of " + tableName +
                                  " could not start: " + afterBarrier,
                                  laterReadStarted);
                Assert.assertNull(afterBarrier, issueFailure);
            });
        }
    }

    private static void createTrackedTableWithCorruptSSTable(Cluster cluster, String keyspaceName, String tableName)
    {
        cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                    "{'class': 'SimpleStrategy', 'replication_factor': 2} " +
                                    "AND replication_type='tracked';", keyspaceName));
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c)) " +
                                    "WITH compression = {'class': 'LZ4Compressor'} AND crc_check_chance = 1.0;",
                                    keyspaceName, tableName));
        cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName),
                                       ConsistencyLevel.ALL);
        cluster.get(1).flush(keyspaceName);
        cluster.get(1).runOnInstance(() -> {
            SSTableReader sstable = Iterables.getOnlyElement(Keyspace.open(keyspaceName).getColumnFamilyStore(tableName).getLiveSSTables());
            try (FileChannel data = sstable.descriptor.fileFor(SSTableFormat.Components.DATA).newReadWriteChannel())
            {
                ByteBuffer firstByte = ByteBuffer.allocate(1);
                data.read(firstByte, 0);
                firstByte.flip();
                firstByte.put(0, (byte) ~firstByte.get(0));
                data.write(firstByte, 0);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static void drainReadOrdering(ColumnFamilyStore cfs)
    {
        OpOrder.Barrier drained = cfs.readOrdering.newBarrier();
        drained.issue();
        awaitCondition(drained.getSyncPoint()::isFinished,
                       "An operation on " + cfs.name + "'s read ordering outlived the read that started it, " +
                       "so the assertions that follow cannot tell one release too many from one live read");
    }

    private static void beginReadOfCorruptSSTable(ColumnFamilyStore cfs)
    {
        try
        {
            MutationTrackingService.instance().localReads().beginRead(TrackedRead.Id.nextId(),
                                                                     ClusterMetadata.current(),
                                                                     PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds()),
                                                                     org.apache.cassandra.db.ConsistencyLevel.ONE,
                                                                     new int[0],
                                                                     Dispatcher.RequestTime.forImmediateExecution(),
                                                                     TrackedLocalReads.Completer.DEFAULT);
            Assert.fail("The read began without reading the corrupt sstable, so it never failed and this test would " +
                        "have passed without asserting anything");
        }
        catch (CorruptSSTableException e)
        {
            for (StackTraceElement frame : e.getStackTrace())
                if (frame.getClassName().equals(PartialTrackedRead.class.getName()) && frame.getMethodName().equals("prepare"))
                    return;
            throw new AssertionError("The corrupt sstable failed the read before the read was built, so no read owned " +
                                     "its controller and this test would have passed without asserting anything", e);
        }
    }

    public static class FailSecondarySummary
    {
        public static final String MESSAGE = "Failing the secondary summary for test";

        // assigned inside each instance's classloader, since that is the copy of this class the injected body reads
        public static volatile String failingTable = null;

        @SuppressWarnings("resource")
        public static IInstanceInitializer install(int... nodes)
        {
            return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                for (int node : nodes)
                    if (node == num)
                        new ByteBuddy().rebase(PartitionRangeReadCommand.class)
                                       .method(named("createMutationSummaryInternal"))
                                       .intercept(MethodDelegation.to(FailSecondarySummary.class))
                                       .make()
                                       .load(cl, ClassLoadingStrategy.Default.INJECTION);
            };
        }

        @SuppressWarnings("unused")
        public static MutationSummary createMutationSummaryInternal(boolean includePending,
                                                                    @This PartitionRangeReadCommand command,
                                                                    @SuperCall Callable<MutationSummary> zuper) throws Exception
        {
            if (includePending && command.metadata().name.equals(failingTable))
                throw new RuntimeException(MESSAGE);

            return zuper.call();
        }
    }
}
