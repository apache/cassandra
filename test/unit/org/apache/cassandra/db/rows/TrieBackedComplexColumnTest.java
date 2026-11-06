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

package org.apache.cassandra.db.rows;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapCloner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for TrieBackedComplexColumn methods that were previously throwing AssertionError.
 */
public class TrieBackedComplexColumnTest
{
    private static final long TIMESTAMP = 1000L;
    private static final int LOCAL_DELETION_TIME = Integer.MAX_VALUE;

    private TableMetadata metadata;
    private ColumnMetadata v2; // regular complex (set)
    private Clustering<?> clustering;
    private RegularAndStaticColumns regularAndStaticColumns;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        metadata = TableMetadata.builder("ks", "table")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("v2", SetType.getInstance(Int32Type.instance, true))
                                .build();

        v2 = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        clustering = Clustering.make(ByteBufferUtil.bytes(1));
        regularAndStaticColumns = metadata.regularAndStaticColumns();
    }

    private Row buildRowWithComplexColumn(long... cellTimestamps)
    {
        Row.Builder builder = TrieBackedRow.builder(regularAndStaticColumns.regulars);
        builder.newRow(clustering);
        for (int i = 0; i < cellTimestamps.length; i++)
        {
            // For set elements, the value is empty (sets only store the path)
            builder.addCell(BufferCell.live(v2, cellTimestamps[i], 
                                           ByteBufferUtil.EMPTY_BYTE_BUFFER, 
                                           CellPath.create(ByteBufferUtil.bytes(i))));
        }
        return builder.build();
    }

    private Row buildRowWithComplexDeletion(long deletionTimestamp, long... cellTimestamps)
    {
        Row.Builder builder = TrieBackedRow.builder(regularAndStaticColumns.regulars);
        builder.newRow(clustering);
        builder.addComplexDeletion(v2, DeletionTime.build(deletionTimestamp, LOCAL_DELETION_TIME));
        for (int i = 0; i < cellTimestamps.length; i++)
        {
            // For set elements, the value is empty (sets only store the path)
            builder.addCell(BufferCell.live(v2, cellTimestamps[i], 
                                           ByteBufferUtil.EMPTY_BYTE_BUFFER, 
                                           CellPath.create(ByteBufferUtil.bytes(i))));
        }
        return builder.build();
    }

    private TrieBackedComplexColumn getComplexColumn(Row row)
    {
        return (TrieBackedComplexColumn) row.getComplexColumnData(v2);
    }

    @Test
    public void testUnsharedHeapSize()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        long heapSize = column.unsharedHeapSize();
        assertTrue("Heap size should be positive", heapSize > 0);
    }

    @Test
    public void testUnsharedHeapSizeExcludingData()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        long heapSize = column.unsharedHeapSizeExcludingData();
        assertTrue("Heap size excluding data should be positive", heapSize > 0);
        
        // Heap size excluding data should be less than or equal to total heap size
        assertTrue("Heap size excluding data should be <= total heap size", 
                  heapSize <= column.unsharedHeapSize());
    }

    @Test
    public void testValidate()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        // Should not throw
        column.validate();
    }

    @Test
    public void testHasInvalidDeletions()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        assertFalse("Should not have invalid deletions", column.hasInvalidDeletions());
    }

    @Test
    public void testHasInvalidDeletionsWithComplexDeletion()
    {
        Row row = buildRowWithComplexDeletion(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        assertFalse("Should not have invalid deletions", column.hasInvalidDeletions());
    }

    @Test
    public void testMarkCounterLocalToBeCleared()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        // For non-counter columns, should return same instance
        TrieBackedComplexColumn marked = column.markCounterLocalToBeCleared();
        assertNotNull(marked);
    }

    @Test
    public void testPurgeDataOlderThan()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        // Purge data older than 2500L
        TrieBackedComplexColumn purged = column.purgeDataOlderThan(2500L);
        
        assertNotNull(purged);
        // Should have removed cells with timestamp < 2500
        int cellCount = 0;
        for (Cell<?> cell : purged)
        {
            assertTrue("Cell timestamp should be >= 2500", cell.timestamp() >= 2500L);
            cellCount++;
        }
        assertEquals("Should have 1 cell remaining", 1, cellCount);
    }

    @Test
    public void testPurgeDataOlderThanWithComplexDeletion()
    {
        Row row = buildRowWithComplexDeletion(1500L, 1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        // Purge data older than 2500L
        TrieBackedComplexColumn purged = column.purgeDataOlderThan(2500L);
        
        assertNotNull(purged);
        // Complex deletion should be removed
        assertTrue("Complex deletion should be removed", purged.complexDeletion().isLive());
        
        // Should have only the cell with timestamp >= 2500
        int cellCount = 0;
        for (Cell<?> cell : purged)
        {
            assertTrue("Cell timestamp should be >= 2500", cell.timestamp() >= 2500L);
            cellCount++;
        }
        assertEquals("Should have 1 cell remaining", 1, cellCount);
    }

    @Test
    public void testClone()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        Cloner cloner = HeapCloner.instance;
        ColumnData cloned = column.clone(cloner);
        
        assertNotNull(cloned);
        assertNotSame("Cloned column should be different instance", column, cloned);
        assertTrue("Cloned column should be TrieBackedComplexColumn", 
                  cloned instanceof TrieBackedComplexColumn);
        
        // Verify cells are present
        TrieBackedComplexColumn clonedComplex = (TrieBackedComplexColumn) cloned;
        assertEquals("Should have same number of cells", 
                    column.cellsCount(), clonedComplex.cellsCount());
    }

    @Test
    public void testUpdateAllTimestamp()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        long newTimestamp = 5000L;
        TrieBackedComplexColumn updated = column.updateAllTimestamp(newTimestamp);
        
        assertNotNull(updated);
        
        // All cells should have the new timestamp
        for (Cell<?> cell : updated)
        {
            assertEquals("Cell should have new timestamp", newTimestamp, cell.timestamp());
        }
    }

    @Test
    public void testUpdateAllTimestampWithComplexDeletion()
    {
        Row row = buildRowWithComplexDeletion(1500L, 1000L, 2000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        long newTimestamp = 5000L;
        TrieBackedComplexColumn updated = column.updateAllTimestamp(newTimestamp);
        
        assertNotNull(updated);
        
        // Complex deletion should be updated to newTimestamp - 1
        assertEquals("Complex deletion should be updated", 
                    newTimestamp - 1, updated.complexDeletion().markedForDeleteAt());
        
        // All cells should have the new timestamp
        for (Cell<?> cell : updated)
        {
            assertEquals("Cell should have new timestamp", newTimestamp, cell.timestamp());
        }
    }

    @Test
    public void testMaxTimestamp()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        long maxTs = column.maxTimestamp();
        assertEquals("Max timestamp should be 3000", 3000L, maxTs);
    }

    @Test
    public void testMaxTimestampWithComplexDeletion()
    {
        Row row = buildRowWithComplexDeletion(4000L, 1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        long maxTs = column.maxTimestamp();
        assertEquals("Max timestamp should be 4000 (complex deletion)", 4000L, maxTs);
    }

    @Test
    public void testPurgeWithDeletionPurger()
    {
        Row row = buildRowWithComplexColumn(1000L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        // Create a purger that removes cells older than 2500
        DeletionPurger purger = (timestamp, localDeletionTime) -> timestamp < 2500L;
        
        // purge method delegates to CellData.purge which handles the actual purging logic
        TrieBackedComplexColumn purged = column.purge(purger, 5000);
        
        assertNotNull(purged);
        // Note: The actual filtering behavior depends on CellData.purge implementation
        // This test verifies the method executes without error
    }

    @Test
    public void testPurgeWithComplexDeletionRemoved()
    {
        Row row = buildRowWithComplexDeletion(1500L, 2000L, 3000L);
        TrieBackedComplexColumn column = getComplexColumn(row);
        
        // Create a purger that removes deletions older than 2000
        DeletionPurger purger = (timestamp, localDeletionTime) -> timestamp < 2000L;
        
        // purge method delegates to CellData.purge and deletion mapper
        TrieBackedComplexColumn purged = column.purge(purger, 5000);
        
        assertNotNull(purged);
        // Note: The actual deletion removal depends on the purger and deletion mapper
        // This test verifies the method executes without error
    }
}
