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
import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.SinglePartitionReadCommand;
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
import org.apache.cassandra.replication.MutationTracker;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.simple.SimpleMutationSummary;
import org.apache.cassandra.replication.simple.SimpleMutationTracker.SimplePendingRead;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.logged.LoggedReadResponse;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertIdsForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getIdsForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryForKey;
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
                                        "AND replication_type='logged';", keyspaceName));

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));


            // insert a row at all, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);
            Set<MutationId> firstIds = getIdsForKey(cluster.get(1), keyspaceName, "tbl", 1);

            cluster.forEach(node -> {
                assertIdsForKey(node, keyspaceName, tableName, 1, firstIds);
            });

            cluster.get(1).runOnInstance(() -> {

                TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, tableName);
                DecoratedKey dk = metadata.partitioner.decorateKey(bytes(1));

                MutationId firstId = Iterables.getOnlyElement(summaryForKey(keyspaceName, tableName, dk).allIds);

                // create a mutation
                SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(keyspaceName, dk);
                PartitionUpdate.SimpleBuilder tableBuilder = builder.update(metadata);
                tableBuilder.row(bytes(1)).add("v", 1);
                Mutation mutation = builder.build();
                MutationId secondId = mutation.id();
                Assert.assertFalse(secondId.isNone());

                int nowInSeconds = (int) FBUtilities.nowInSeconds();
                // apply it to the journal and open a pending write
                LoggedReadResponse response;
                SimpleMutationSummary summary;
                SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, nowInSeconds, dk);
                try (MutationTracker.PendingWrite pendingWrite = MutationTrackingService.instance().startWrite(mutation))
                {
                    MutationTrackingService.instance().add(mutation);

                    try (ReadExecutionController controller = command.executionController(false);
                         UnfilteredPartitionIterator iterator = command.executeLocally(controller))
                    {
                        summary = (SimpleMutationSummary) command.createMutationSummary();
                        response = (LoggedReadResponse) command.createResponse(iterator, controller.getRepairedDataInfo(), summary, controller.pendingRead());
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

                // check that the summary does contain the unapplied mutation
                Assert.assertEquals(Set.of(firstId, secondId), summary.allIds);

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
                                        "AND replication_type='logged';", keyspaceName));

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
                try (SimplePendingRead pendingRead = (SimplePendingRead) MutationTrackingService.instance().startRead(command))
                {
                    Assert.assertTrue(pendingRead.mutationIds().isEmpty());

                    // create and apply a mutation
                    SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(keyspaceName, dk);
                    PartitionUpdate.SimpleBuilder tableBuilder = builder.update(metadata);
                    tableBuilder.row(bytes(1)).add("v", 1);
                    Mutation mutation = builder.build();
                    mutation.apply();

                    // the in flight read should be aware of the racing write
                    Assert.assertEquals(Set.of(mutation.id()), pendingRead.mutationIds());

                }
            });
        }
    }
}
