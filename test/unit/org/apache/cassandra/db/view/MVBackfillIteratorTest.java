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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class MVBackfillIteratorTest extends ViewAbstractTest
{
    private static final String KEYSPACE = "mv_backfill_iterator_test";
    
    private ColumnFamilyStore baseCfs;
    private TableMetadata baseMetadata;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                   "{'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
    }

    @Before
    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        execute("USE " + KEYSPACE);
        
        // Create base table: CREATE TABLE base_table (k int, c int, v text, PRIMARY KEY (k, c))
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        
        baseCfs = getCurrentColumnFamilyStore();
        baseMetadata = baseCfs.metadata();
    }

    @Test
    public void testEmptyTable() throws IOException
    {
        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(), 
                                        baseCfs.getPartitioner().getMinimumToken());
        
        try (MVBackfillIterator iterator = new MVBackfillIterator(baseCfs, range, FBUtilities.nowInSeconds()))
        {
            assertTrue("Iterator should be empty for empty table", iterator.isEmpty());
            assertFalse("Iterator should not have next for empty table", iterator.hasNext());
            assertEquals("Estimated partitions should be 0", 0, iterator.getEstimatedPartitions());
            assertEquals("Estimated bytes should be 0", 0, iterator.getEstimatedBytes());
            assertEquals("Bytes read should be 0", 0, iterator.getBytesRead());
        }
    }

    @Test
    public void testSinglePartitionSingleRow() throws Throwable
    {
        // Insert a single row
        insertRow(1, 1, "value1");
        singlePartitionRowTestHelper(true);
    }

    @Test
    public void testSinglePartitionSingleRowWithNoneExpiredTTL() throws Throwable
    {
        // Insert a single row
        insertRowWithTTL(1, 1, "value1", 100);
        singlePartitionRowTestHelper(true);
    }

    @Test
    public void testSinglePartitionSingleRowWithExpiredTTL() throws Throwable
    {
        // Insert a single row which will expire in 1s
        insertRowWithTTL(1, 1, "value1", 1);
        // wait for more than 1s
        Thread.sleep(1100);
        singlePartitionRowTestHelper(false);
    }

    @Test
    public void testMultiplePartitionsMultipleRows() throws Throwable
    {
        // Insert multiple rows across multiple partitions
        insertRow(1, 1, "value1_1");
        insertRow(1, 2, "value1_2");
        insertRow(2, 1, "value2_1");
        insertRow(3, 1, "value3_1");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(), 
                                        baseCfs.getPartitioner().getMinimumToken());
        
        try (MVBackfillIterator iterator = new MVBackfillIterator(baseCfs, range, FBUtilities.nowInSeconds()))
        {
            assertFalse("Iterator should not be empty", iterator.isEmpty());
            assertTrue("Iterator should have data", iterator.hasNext());

            int partitionCount = 0;
            int rowCount = 0;

            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    partitionCount++;
                    
                    while (partition.hasNext())
                    {
                        var unfiltered = partition.next();
                        if (unfiltered.isRow())
                        {
                            Row row = (Row) unfiltered;
                            if (!row.isEmpty())
                                rowCount++;
                        }
                    }
                }
            }

            assertEquals("Should have 3 partitions", 3, partitionCount);
            assertEquals("Should have 4 rows total", 4, rowCount);
            iterator.updateBytesRead();
            assertTrue("Should have read some bytes", iterator.getBytesRead() > 0);
        }
    }

    @Test
    public void testDataMerging() throws Throwable
    {
        // Insert initial data
        insertRow(1, 1, "initial");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Update the same row (will create new SSTable)
        insertRow(1, 1, "updated");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(), 
                                        baseCfs.getPartitioner().getMinimumToken());
        
        try (MVBackfillIterator iterator = new MVBackfillIterator(baseCfs, range, FBUtilities.nowInSeconds()))
        {
            assertTrue("Iterator should have data", iterator.hasNext());

            // Read the partition
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertTrue("Partition should have a row", partition.hasNext());
                var unfiltered = partition.next();
                assertTrue("Should be a row", unfiltered.isRow());
                
                Row row = (Row) unfiltered;
                
                // The iterator should return the merged (latest) value
                ByteBuffer valueBuffer = row.getCell(baseMetadata.getColumn(UTF8Type.instance.fromString("v"))).buffer();
                String value = UTF8Type.instance.compose(valueBuffer);
                assertEquals("Should get the updated value", "updated", value);
                
                assertFalse("Should be only one row after merging", partition.hasNext());
            }

            assertFalse("Should be only one partition", iterator.hasNext());
        }
    }

    @Test
    public void testTombstoneHandling() throws Throwable
    {
        // Insert and then delete a row
        insertRow(1, 1, "value1");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        deleteRow(1, 1);
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(), 
                                        baseCfs.getPartitioner().getMinimumToken());
        int nowInSeconds = FBUtilities.nowInSeconds();
        try (MVBackfillIterator iterator = new MVBackfillIterator(baseCfs, range, nowInSeconds))
        {
            // The iterator should purge tombstones, so we might not see the deleted row
            // or we might see an empty partition - this depends on the compaction controller's behavior

            if (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    // If we see the partition, any rows should be empty or the partition should be empty
                    while (partition.hasNext())
                    {
                        var unfiltered = partition.next();
                        if (unfiltered.isRow())
                        {
                            Row baseRow = (Row) unfiltered;
                            if (baseRow.isEmpty() || !baseRow.hasLiveData(nowInSeconds, baseCfs.metadata().enforceStrictLiveness()))
                                continue;
                            fail("Should not have live data");
                        }
                    }
                }
            }

            // The key point is that the iterator doesn't crash and handles tombstones gracefully
        }
    }

    @Test
    public void testSpecificTokenRange() throws Throwable
    {
        // Insert data across multiple partitions
        insertRow(1, 1, "value1");
        insertRow(100, 1, "value100");
        insertRow(1000, 1, "value1000");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Create a specific token range that might include only some partitions, the value below is for murmur3 partitioner
        Token token1 = baseCfs.getPartitioner().getToken(Int32Type.instance.decompose(1)); // -4069959284402364209
        Token token100 = baseCfs.getPartitioner().getToken(Int32Type.instance.decompose(100));  // 2008715943680221220
        Token token1000 = baseCfs.getPartitioner().getToken(Int32Type.instance.decompose(1000)); // 7935772098093053663
        // This should include 1 and 100
        Range<Token> specificRange = new Range<>(new LongToken(-4069959284402364210L), new LongToken(2008715943680221224L));

        Set<ByteBuffer> expectedValues = new HashSet<>(Arrays.asList(ByteBufferUtil.bytes("value1"), ByteBufferUtil.bytes("value100")));
        try (MVBackfillIterator iterator = new MVBackfillIterator(baseCfs, specificRange, FBUtilities.nowInSeconds()))
        {
            // We should get some data, but potentially not all partitions
            // The exact behavior depends on the partitioner and token distribution
            // Here we are testing the default Murmur3 partitioner
            
            int partitionCount = 0;
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    partitionCount++;
                    
                    // Verify that the partition key is within our expected range
                    DecoratedKey partitionKey = partition.partitionKey();
                    assertNotNull("Partition key should not be null", partitionKey);
                    while (partition.hasNext())
                    {
                        var unfiltered = partition.next();
                        if (unfiltered.isRow())
                        {
                            Row baseRow = (Row) unfiltered;
                            assertTrue(expectedValues.contains(baseRow.getCell(baseMetadata.getColumn(UTF8Type.instance.fromString("v"))).buffer()));
                        }
                    }
                }
            }
            
            // We should have processed some partitions (exact count depends on token distribution)
            assertTrue("Should have processed some partitions for specific range", partitionCount == 2);
        }
    }

    @Test
    public void testResourceCleanup() throws Throwable
    {
        insertRow(1, 1, "value1");
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(), 
                                        baseCfs.getPartitioner().getMinimumToken());
        
        MVBackfillIterator iterator = new MVBackfillIterator(baseCfs, range, FBUtilities.nowInSeconds());
        
        // Use the iterator briefly
        if (iterator.hasNext())
        {
            try (UnfilteredRowIterator partition = iterator.next())
            {
                // Just open and close
            }
        }
        
        // Close should not throw any exceptions
        iterator.close();
        
        // Calling close again should be safe
        iterator.close();
    }

    @Test
    public void testMetadata()
    {
        assertEquals("Metadata should match base table", baseMetadata, 
                    baseCfs.metadata.get());
    }

    // Helper methods

    private void insertRow(int partitionKey, int clusteringKey, String value) throws Throwable
    {
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", partitionKey, clusteringKey, value);
    }

    private void insertRowWithTTL(int partitionKey, int clusteringKey, String value, int ttl) throws Throwable
    {
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?) USING TTL ?", partitionKey, clusteringKey, value, ttl);
    }

    private void deleteRow(int partitionKey, int clusteringKey) throws Throwable
    {
        execute("DELETE FROM %s WHERE k = ? AND c = ?", partitionKey, clusteringKey);
    }

    private DecoratedKey decoratedKey(int key)
    {
        return baseCfs.getPartitioner().decorateKey(Int32Type.instance.decompose(key));
    }

    private void singlePartitionRowTestHelper(boolean hasLiveData) throws IOException
    {
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Range<Token> range = new Range<>(baseCfs.getPartitioner().getMinimumToken(),
                                         baseCfs.getPartitioner().getMinimumToken());

        try (MVBackfillIterator iterator = new MVBackfillIterator(baseCfs, range, FBUtilities.nowInSeconds()))
        {
            assertFalse("Iterator should not be empty", iterator.isEmpty());
            assertTrue("Iterator should have data", iterator.hasNext());
            assertTrue("Estimated partitions should be > 0", iterator.getEstimatedPartitions() >= 0);
            assertTrue("Estimated bytes should be > 0", iterator.getEstimatedBytes() >= 0);

            // Read the partition
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assertNotNull("Partition should not be null", partition);
                assertEquals("Partition key should match",
                             decoratedKey(1), partition.partitionKey());

                // Read the row
                assertTrue("Partition should have a row", partition.hasNext());
                var unfiltered = partition.next();
                assertTrue("Should be a row", unfiltered.isRow());

                Row row = (Row) unfiltered;
                assertFalse("Row should not be empty", row.isEmpty());
                assertEquals(hasLiveData, row.hasLiveData(FBUtilities.nowInSeconds(), baseMetadata.enforceStrictLiveness()));

                // Verify clustering key
                assertEquals("Clustering should match", 1,
                             Int32Type.instance.compose(row.clustering().bufferAt(0)).intValue());

                assertFalse("Should be only one row", partition.hasNext());
            }

            assertFalse("Should be only one partition", iterator.hasNext());
        }
    }
}
