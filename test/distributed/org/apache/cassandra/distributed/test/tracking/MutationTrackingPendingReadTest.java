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
import java.util.HashSet;
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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.ActiveLogReconciler;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationId;
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

    /**
     * Read reconciliation pulls the mutations a remote summary reported and this node is missing, and completes once a
     * listener has fired for each of them. The write path can land one of those mutations in the window between
     * ReadReconciliations.acceptRemoteSummary deciding it is missing and pull registering the listener for it: that
     * copy's finishWriting invoked the listeners for the id while there were none, and the pulled copy then arrives as
     * a duplicate. Keyspace.applyInternalTracked skips the apply and finishWriting for a duplicate, since the data is
     * already there, so nothing invoked the listener registered in between. ReadReconciliations.Coordinator.remaining
     * never reached zero, and the read waited out its timeout instead - the callback ReadReconciliations registers
     * implements only onSuccess, so listener expiry does not fail the read either.
     *
     * A duplicate has to notify the listeners that the copy landing first could not have notified.
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

                // the copy that lands first witnesses the id, and finds no listener to notify
                mutation.apply();

                // so a re-delivery of it is a duplicate, which is what makes the rest of this test mean anything:
                // were it applied as a new mutation below, its own finishWriting would notify and prove nothing.
                // No listener is registered yet, exactly as when the first copy landed, so this probe notifies nobody.
                Assert.assertFalse("An already witnessed mutation was not recognized as a duplicate",
                                   MutationTrackingService.instance().startWriting(mutation));

                // a read that decided it was missing this id before the write above landed registers its listener now
                AtomicInteger notified = new AtomicInteger();
                Assert.assertTrue("Expected to be the first listener registered for this id",
                                  MutationTrackingService.instance().registerMutationCallback(mutation.id(), notifiedId -> notified.incrementAndGet()));

                // the pulled copy arrives, and is a duplicate of what already landed
                mutation.apply();

                Assert.assertEquals("A duplicate mutation left a read reconciliation listener waiting",
                                    1, notified.get());
            });
        }
    }

    /**
     * The same hole, from the outside: a client read that has to reconcile, whose every pulled copy arrives as a
     * duplicate. What a duplicate notifying nobody costs is not an internal counter, it is this read.
     *
     * {@code ReadReconciliations.Coordinator.acceptRemoteSummary} calls {@code pull} once per coordinator log the
     * missing mutations belong to, so the window {@link #testDuplicateWriteNotifiesMutationListeners} describes is
     * open once per log: for a log a call has not reached yet, the mutations are known to be missing with nobody yet
     * listening for them.
     *
     * The interleaving is built, not raced. The two mutations go into two different coordinator logs, so one
     * {@code acceptRemoteSummary} call makes two {@code pull} calls; the first call's MT_PULL_MUTATIONS_REQ is held in
     * an outbound filter, which parks that call between its two pulls, because the filter runs on the thread inside it;
     * both mutations are then delivered to the data replica while it is parked, and the held request is not released
     * until that replica has witnessed both of them, which is the condition under which {@code startWriting} calls a
     * further copy a duplicate. By then the first log's listener has fired on delivery and the second log's listener
     * does not exist yet, so the copy that listener is registered for is guaranteed to arrive as a duplicate.
     */
    @Test
    public void testReadCompletesWhenThePulledMutationsArriveAsDuplicates() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking.enabled", true)
                                                            // a healer, and a source of the message this test holds:
                                                            // it pulls what one replica's log has and another's lacks
                                                            // on its own schedule, so it would land both mutations on
                                                            // node 1 before the read below computes a summary, leaving
                                                            // the read nothing to reconcile - and it pulls with the
                                                            // same verb, so its requests would consume the hold
                                                            .set("mutation_tracking.background_reconciliation_enabled", false)
                                                            // a healer: the writes below are real coordinated writes
                                                            // whose delivery to node 1 is dropped, so each coordinator
                                                            // stores node 1 a hint, and hint delivery would repair it
                                                            .set("hinted_handoff_enabled", false)
                                                            // the read is parked mid reconciliation while this test
                                                            // lands the mutations it is pulling, and an
                                                            // IncomingMutations listener lives for the write rpc
                                                            // timeout, so neither timeout may be tight
                                                            .set("read_request_timeout", "10000ms")
                                                            .set("write_request_timeout", "10000ms"))
                                      .start())
        {
            // a healer: a write whose delivery to a replica failed is retried onto that replica through
            // MutationTrackingService.retryFailedWrite, which schedules at ActiveLogReconciler.Priority.REGULAR and is
            // not gated by background_reconciliation_enabled. Only the REGULAR queue pauses here; HIGH always drains,
            // and HIGH is what the read's own pull and this test's delivery below both use
            cluster.forEach(() -> MutationTrackingService.instance().pauseActiveReconcilerRegularPriority());

            String keyspaceName = "duplicate_pull_notification_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            String select = format("SELECT k, c, v FROM %s.%s WHERE k = 1", keyspaceName, tableName);

            // two rows of one partition, each written through a different coordinator so each mutation is minted into
            // that coordinator's own log, and neither reaches node 1. QUORUM rather than ALL because a tracked write
            // acks on ordinary consistency terms, so 2 of 3 is a write the client was told succeeded rather than one
            // the harness had to swallow an exception for
            IMessageFilters.Filter writesToNode1 = cluster.filters().verbs(Verb.MUTATION_REQ.id).to(1).drop();
            cluster.coordinator(2).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            cluster.coordinator(3).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 2, 2)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            writesToNode1.off();

            assertRows(cluster.get(1).executeInternal(select));
            assertRows(cluster.get(2).executeInternal(select), row(1, 1, 1), row(1, 2, 2));
            assertRows(cluster.get(3).executeInternal(select), row(1, 1, 1), row(1, 2, 2));

            // the ids a summary replica reports for this partition, which is what the read will diff against node 1
            byte[] remoteSummary = cluster.get(2).callOnInstance(() -> encodeSummary(summaryForKey(keyspaceName, tableName, 1)));

            // acceptRemoteSummary calls pull once per coordinator log, and the window is between two of those calls,
            // so the two mutations have to have landed in two different logs
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
                return false; // returning false permits the message: this filter only counts
            }).drop();

            AtomicInteger pullRequests = new AtomicInteger();
            AtomicBoolean held = new AtomicBoolean();
            CountDownLatch parked = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            cluster.filters().verbs(Verb.MT_PULL_MUTATIONS_REQ.id).from(1).outbound().messagesMatching((from, to, message) -> {
                pullRequests.incrementAndGet();
                // an outbound matcher runs on the thread that sent the message, which for the read's pull is the
                // thread inside acceptRemoteSummary, so blocking here holds that call between the pull it is sending
                // now and the pull for the coordinator log it has not reached yet
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
                        read.get(); // surfaces a read failure rather than reporting it as a missing pull request
                        throw new AssertionError("The read completed without ever sending a pull request, so it found " +
                                                 "nothing to reconcile and could not have been left waiting on one");
                    }
                    Assert.assertTrue("The read neither sent a pull request nor completed", System.nanoTime() - deadline < 0);
                }

                try
                {
                    // Deliver both mutations to node 1 now, while the read is parked between its two pulls. This is
                    // the same call PullMutationsRequest's verb handler makes when it serves a pull, so what node 1
                    // receives is the delivery a pull would have produced. The failed write retry would have delivered
                    // them too, but only once the write rpc timeout had expired, and that timeout is also how long an
                    // IncomingMutations listener lives, so waiting for it would race the expiry of the listener that
                    // has to survive the park.
                    for (int node : new int[]{ 2, 3 })
                        cluster.get(node).runOnInstance(() -> {
                            ClusterMetadata metadata = ClusterMetadata.current();
                            InetAddressAndPort dataReplica = metadata.directory.endpoint(new NodeId(1));
                            int localNode = metadata.directory.peerId(FBUtilities.getBroadcastAddressAndPort()).id();
                            MutationTrackingService.instance().forEachShardInKeyspace(keyspaceName, shard ->
                                shard.collectUnionOfWitnessedOffsetsPerLog().forEach((logId, offsets) -> {
                                    // the log this node coordinates, which is where the read's own pull for those
                                    // offsets goes as well
                                    if (logId.hostId() == localNode && !offsets.isEmpty())
                                        MutationTrackingService.instance().requestMissingMutations(offsets, dataReplica, ActiveLogReconciler.Priority.HIGH);
                                }));
                        });

                    // Both copies are witnessed by node 1 before the read is allowed to send a single pull request, so
                    // every copy it pulls from here on is a duplicate. This is what the test rests on: without it the
                    // second log's copy could arrive as a first copy and notify on its own account, and the read would
                    // complete whether duplicates notify anything or not. The predicate is the one startWriting itself
                    // uses - what node 1 has locally witnessed - rather than what a read of node 1 returns, because a
                    // mutation's updates are applied before finishWriting records the offset, so rows can be visible
                    // for a moment while a further copy would still be treated as an original.
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
                // one summary replica, so a single acceptRemoteSummary call decided both logs were missing. Two would
                // let a second call register the second log's listener before the copies landed, and the read would
                // complete without a duplicate ever having to notify anything
                Assert.assertEquals("Expected exactly one summary replica", 1, summaryRequests.get());
            }
            finally
            {
                executor.shutdownNow();
            }
        }
    }

    /**
     * Reconciliation hands a read the mutation ids other replicas reported and this node lacks, and
     * {@link TrackedLocalReads#acknowledgeReconcile} takes the read's coordinator out of its map before handing off,
     * so neither expiry nor abort can reach that read afterwards. Augmenting with an id the local mutation journal
     * holds no record of throws, and {@link PartialTrackedRead}'s own javadoc for that case names a newly activated
     * transfer - an ordinary production event - alongside a bug, so the failure handler is the only thing left that
     * can close the read. A read that is never closed never closes its {@link ReadExecutionController}, and an
     * {@link OpOrder.Group} that never closes blocks every subsequent memtable flush and compaction of the table for
     * the life of the node.
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

            // the read stage rethrows the augment failure the assertions below are about once it has failed the read's
            // promise, and that rethrow reaches the uncaught exception handler
            cluster.setUncaughtExceptionsFilter(t -> t.getMessage() != null && t.getMessage().startsWith("Missing mutation"));

            // the read below takes node 2 as its only summary node, and a summary from a node that is down never
            // arrives, so its reconciliation stays pending until this test acknowledges it by hand
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

                // a reconciliation waits for one summary per summary node plus this node's own, so an empty
                // summaryNodes above would have reconciled and completed the read inside beginRead, leaving the
                // acknowledgement below no coordinator to find
                Assert.assertFalse("The read reconciled without a summary from its summary node, so the " +
                                   "acknowledgement below is a no-op and this test would have passed without " +
                                   "asserting anything",
                                   promise.isDone());

                // an id the journal cannot hold a record for, since it is allocated here and never written
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
     * beginReadInternal opens a {@link ReadExecutionController} and hands it to the read it begins, and the read owns
     * it from there on: {@link PartialTrackedRead#close()} closes it. {@link ReadExecutionController#close()} is not
     * idempotent - it closes the base table's {@link OpOrder.Group}, the index controller and the index write context
     * unconditionally - so an abort that closes the controller as well as the read releases that group twice. One
     * release too many takes a group holding a running operation to the count that means finished, and the next flush
     * barrier then reports drained while that operation is still reading the memtable being flushed.
     *
     * Everything thrown between beginTrackedRead returning and the read's coordinator being registered reaches that
     * abort path - the secondary summary, the delta against it and the transfer id merge are all inside it - and the
     * secondary summary is the one made to throw here.
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

                // drain the table's read ordering first, so the operations the assertions below count are this test's
                // own and the group they land on is empty to begin with
                OpOrder.Barrier drained = cfs.readOrdering.newBarrier();
                drained.issue();
                awaitCondition(drained.getSyncPoint()::isFinished,
                               "An operation on " + tableName + "'s read ordering outlived the read that started it, " +
                               "so the assertions below cannot tell one release too many from one live read");

                // one read of this test's own, so a correctly aborted read leaves the group holding exactly it, while
                // a doubly released one empties the group with this read still running
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
     * Fails the secondary summary beginReadInternal takes after its read has begun, which is the summary taken with
     * pending mutations included.
     */
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
