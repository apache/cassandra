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

import com.google.common.collect.Iterables;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.tracked.TrackedKeyspaceWriteHandler;
import org.apache.cassandra.replication.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.TrackedReadResponse;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.*;
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
                                                            .set("mutation_tracking_enabled", "true")
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
                MutationId id = MutationTrackingService.instance.nextMutationId(keyspaceName, dk.getToken());
                SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(id, keyspaceName, dk);
                PartitionUpdate.SimpleBuilder tableBuilder = builder.update(metadata);
                tableBuilder.row(bytes(1)).add("v", 1);
                Mutation mutation = builder.build();
                MutationId secondId = mutation.id();
                Assert.assertFalse(secondId.isNone());

                int nowInSeconds = (int) FBUtilities.nowInSeconds();
                // apply it to the journal and open a pending write
                TrackedReadResponse response;
                SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, nowInSeconds, dk);
                TrackedKeyspaceWriteHandler trackedWriteHandler = new TrackedKeyspaceWriteHandler();
                MutationSummary initialSummary;
                try (WriteContext ctx = trackedWriteHandler.beginWrite(mutation, true))
                {
                    initialSummary = command.createMutationSummary(false);
                    MutationTrackingService.instance.startWriting(mutation);
                    try (ReadExecutionController controller = command.executionController(false);
                         UnfilteredPartitionIterator iterator = command.executeLocally(controller))
                    {
                        response = (TrackedReadResponse) command.createResponse(iterator, controller.getRepairedDataInfo(), initialSummary);
                    }
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
                Offsets summaryIds = summaryIdSpace(response.summary.get(logId));
                Assert.assertEquals(2, summaryIds.offsetCount());
                Assert.assertTrue(summaryIds.contains(secondId.offset()));

                // check that the returned data contains the unapplied mutation
                try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
                {
                    Assert.assertTrue(partitions.hasNext());
                    try (UnfilteredRowIterator rowIterator = partitions.next())
                    {
                        ImmutableBTreePartition partition = ImmutableBTreePartition.create(rowIterator);

                        Assert.assertEquals(2, partition.rowCount());

                        assertKcvRow(partition, cfs, 0, 0);
                        assertKcvRow(partition, cfs, 1, 1);
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
                                                            .set("mutation_tracking_enabled", "true")
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
//                try (ListeningPendingRead pendingRead = (ListeningPendingRead) MutationTrackingService.instance.startReading(command))
//                {
//                    Assert.assertTrue(pendingRead.mutationIds().isEmpty());
//
//                    // create and apply a mutation
//                    MutationId id = MutationTrackingService.instance.nextMutationId(keyspaceName, dk.getToken());
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
}
