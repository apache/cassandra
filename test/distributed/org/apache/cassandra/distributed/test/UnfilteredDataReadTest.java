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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.function.Function;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnfilteredDataReadTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(2)
                              .start());

        cluster.schemaChange(withKeyspace("CREATE TABLE %s.test_digest_repair (k int, c int, v int, PRIMARY KEY (k, c)) " +
                                          "WITH read_repair='BLOCKING'"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.test_no_digest_repair (k int, c int, v int, PRIMARY KEY (k, c)) " +
                                          "WITH read_repair='BLOCKING'"));
    }

    @Before
    public void setup() throws InterruptedException
    {
        cluster.get(1).executeInternal("TRUNCATE " + KEYSPACE + ".test_digest_repair");
        cluster.get(1).executeInternal("TRUNCATE " + KEYSPACE + ".test_no_digest_repair");

        String[] queries = new String[] {
            // k=1,c=1 with normal row deletion
            "DELETE FROM " + KEYSPACE + ".%s USING TIMESTAMP ? WHERE k=1 AND c=1",
            // k=2,c=2,v=2 with TTL & expired row
            "INSERT INTO " + KEYSPACE + ".%s (k, c, v) VALUES (2, 2, 2) USING TTL 1 AND TIMESTAMP ?",
            // k=3,c=3,v=3 with TTL & expired cell
            "UPDATE " + KEYSPACE + ".%s USING TTL 1 AND TIMESTAMP ? SET v=3 WHERE k=3 AND c=3",
            // k=4,c=4,v=4 with cell tombstone
            "UPDATE " + KEYSPACE + ".%s USING TIMESTAMP ? SET v=null WHERE k=4 AND c=4",
            // k=5,c=5,v=5 regular row
            "INSERT INTO " + KEYSPACE + ".%s (k, c, v) VALUES (5, 5, 5) USING TIMESTAMP ?",
        };

        cluster.get(1).runOnInstance(() ->{
            for (String query : queries)
            {
                QueryProcessor.executeOnceInternalWithNowAndTimestamp(FBUtilities.nowInSeconds(), 1000L, String.format(query, "test_digest_repair"), 1000L);
                QueryProcessor.executeOnceInternalWithNowAndTimestamp(FBUtilities.nowInSeconds(), 2000L, String.format(query, "test_no_digest_repair"), 2000L);
            }
        });
        cluster.get(2).runOnInstance(() ->{
            for (String query : queries)
            {
                QueryProcessor.executeOnceInternalWithNowAndTimestamp(FBUtilities.nowInSeconds(), 2000L, String.format(query, "test_digest_repair"), 2000L);
                QueryProcessor.executeOnceInternalWithNowAndTimestamp(FBUtilities.nowInSeconds(), 2000L, String.format(query, "test_no_digest_repair"), 2000L);
            }
        });
        Thread.sleep(1000); // wait for TTL to expire
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testDigestMatching()
    {
        testDigestMismatchTriggersReadRepairImpl(true);
    }

    @Test
    public void testDigestMismatchTriggersReadRepair()
    {
        testDigestMismatchTriggersReadRepairImpl(false);
    }

    private void testDigestMismatchTriggersReadRepairImpl(boolean digestMatching)
    {
        String tableToRead = digestMatching ? "test_no_digest_repair" : "test_digest_repair";
        cluster.get(1).runOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, tableToRead);
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(metadata.id);
            Function<Integer, SinglePartitionReadCommand> getUnfilteredCommand = (Integer keyValue) -> {
                DecoratedKey key = cfs.decorateKey(ByteBufferUtil.bytes(keyValue));
                ClusteringIndexFilter filter = new ClusteringIndexSliceFilter(Slices.with(cfs.metadata().comparator,
                                                                                          Slice.ALL),
                                                                              false);
                return SinglePartitionReadCommand.createUnfiltered(false,
                                                                   0,
                                                                   false,
                                                                   cfs.metadata(),
                                                                   FBUtilities.nowInSeconds(),
                                                                   ColumnFilter.all(cfs.metadata()),
                                                                   RowFilter.NONE,
                                                                   DataLimits.NONE,
                                                                   key,
                                                                   filter,
                                                                   null,
                                                                   false);
            };

            Function<Integer, SinglePartitionReadCommand> getFilteredCommand = (Integer keyValue) -> {
                DecoratedKey key = cfs.decorateKey(ByteBufferUtil.bytes(keyValue));
                ClusteringIndexFilter filter = new ClusteringIndexSliceFilter(Slices.with(cfs.metadata().comparator,
                                                                                          Slice.ALL),
                                                                              false);
                return SinglePartitionReadCommand.create(cfs.metadata(),
                                                         FBUtilities.nowInSeconds(),
                                                         key,
                                                         ColumnFilter.all(cfs.metadata()),
                                                         filter);
            };

            try (PartitionIterator partitionIter = getUnfilteredCommand.apply(1).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertTrue("Should have at least one partition", partitionIter.hasNext());
                RowIterator iter = partitionIter.next();
                assertEquals("Partition key should match", cfs.decorateKey(ByteBufferUtil.bytes(1)), iter.partitionKey());
                assertTrue("Row iterator should not be null", iter.hasNext());
                Row row = iter.next();
                // check the row is the expected one
                assertEquals(Util.clustering(cfs.metadata().comparator, 1), row.clustering());
                // deletion should come from node 2 after digest repair (if any), even we run on node 1
                assertEquals(2000, row.deletion().time().markedForDeleteAt());
                assertTrue(row.deletion().time().localDeletionTime() < Integer.MAX_VALUE);
                // no data (row tombstone)
                assertEquals(0, row.columnCount());
            }
            try (PartitionIterator partitionIter = getFilteredCommand.apply(1).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertFalse("Should return nothing", partitionIter.hasNext());
            }

            try (PartitionIterator partitionIter = getUnfilteredCommand.apply(2).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertTrue("Should have at least one partition", partitionIter.hasNext());
                RowIterator iter = partitionIter.next();
                assertEquals("Partition key should match", cfs.decorateKey(ByteBufferUtil.bytes(2)), iter.partitionKey());
                assertTrue("Row iterator should not be null", iter.hasNext());
                Row row = iter.next();
                // check the row is the expected one
                assertEquals(Util.clustering(cfs.metadata().comparator, 2), row.clustering());
                // row should come from node 2 after digest repair (if any), even we run on node 1
                assertEquals(2000, row.primaryKeyLivenessInfo().timestamp());
                // ttl should be 1
                assertEquals(1, row.primaryKeyLivenessInfo().ttl());
                // data is purged after row ttl expiry (cell removed by local read)
                Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("v", true)));
                assertTrue(cell.isTombstone());
                assertEquals(0, cell.valueSize());
            }
            try (PartitionIterator partitionIter = getFilteredCommand.apply(2).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertFalse("Should return nothing", partitionIter.hasNext());
            }

            try (PartitionIterator partitionIter = getUnfilteredCommand.apply(3).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertTrue("Should have at least one partition", partitionIter.hasNext());
                RowIterator iter = partitionIter.next();
                assertEquals("Partition key should match", cfs.decorateKey(ByteBufferUtil.bytes(3)), iter.partitionKey());
                assertTrue("Row iterator should not be null", iter.hasNext());
                Row row = iter.next();
                // check the row is the expected one
                assertEquals(Util.clustering(cfs.metadata().comparator, 3), row.clustering());
                // empty row liveness info
                assertEquals(LivenessInfo.EMPTY, row.primaryKeyLivenessInfo());
                // data is purged after row ttl expiry (cell removed by local read)
                Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("v", true)));
                // cell should come from node 2 after digest repair (if any), even we run on node 1
                assertEquals(2000, cell.timestamp());
                // data (including ttl info) is purged after row ttl expiry (removed by local read)
                assertTrue(cell.isTombstone());
                assertEquals(0, cell.valueSize());
            }
            try (PartitionIterator partitionIter = getFilteredCommand.apply(3).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertFalse("Should return nothing", partitionIter.hasNext());
            }

            try (PartitionIterator partitionIter = getUnfilteredCommand.apply(4).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertTrue("Should have at least one partition", partitionIter.hasNext());
                RowIterator iter = partitionIter.next();
                assertEquals("Partition key should match", cfs.decorateKey(ByteBufferUtil.bytes(4)), iter.partitionKey());
                assertTrue("Row iterator should not be null", iter.hasNext());
                Row row = iter.next();
                // check the row is the expected one
                assertEquals(Util.clustering(cfs.metadata().comparator, 4), row.clustering());
                // empty row liveness info
                assertEquals(LivenessInfo.EMPTY, row.primaryKeyLivenessInfo());
                Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("v", true)));
                // cell should come from node 2 after digest repair (if any), even we run on node 1
                assertEquals(2000, cell.timestamp());
                // cell tombstone
                assertTrue(cell.isTombstone());
                assertEquals(0, cell.valueSize());
            }
            try (PartitionIterator partitionIter = getFilteredCommand.apply(4).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertFalse("Should return nothing", partitionIter.hasNext());
            }

            try (PartitionIterator partitionIter = getUnfilteredCommand.apply(5).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertTrue("Should have at least one partition", partitionIter.hasNext());
                RowIterator iter = partitionIter.next();
                assertEquals("Partition key should match", cfs.decorateKey(ByteBufferUtil.bytes(5)), iter.partitionKey());
                assertTrue("Row iterator should not be null", iter.hasNext());
                Row row = iter.next();
                // check the row is the expected one
                assertEquals(Util.clustering(cfs.metadata().comparator, 5), row.clustering());
                // row should come from node 2 after digest repair (if any), even we run on node 1
                assertEquals(2000, row.primaryKeyLivenessInfo().timestamp());
                Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("v", true)));
                assertEquals(2000, cell.timestamp());
                assertEquals(ByteBufferUtil.bytes(5), cell.buffer());
            }
            try (PartitionIterator partitionIter = getFilteredCommand.apply(5).execute(ConsistencyLevel.ALL, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertTrue("Should have at least one partition", partitionIter.hasNext());
                RowIterator iter = partitionIter.next();
                assertEquals("Partition key should match", cfs.decorateKey(ByteBufferUtil.bytes(5)), iter.partitionKey());
                assertTrue("Row iterator should not be null", iter.hasNext());
                Row row = iter.next();
                // check the row is the expected one
                assertEquals(Util.clustering(cfs.metadata().comparator, 5), row.clustering());
                // row should come from node 2 after digest repair (if any), even we run on node 1
                assertEquals(2000, row.primaryKeyLivenessInfo().timestamp());
                Cell<?> cell = row.getCell(cfs.metadata().getColumn(new ColumnIdentifier("v", true)));
                assertEquals(2000, cell.timestamp());
                assertEquals(ByteBufferUtil.bytes(5), cell.buffer());
            }
        });

    }
}
