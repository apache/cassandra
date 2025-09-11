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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ViewUtils for base table row to MV row translation methods.
 */
public class ViewUtilsRowTranslateTest extends ViewAbstractTest
{
    @Test
    public void testComputeLivenessInfoForEntryWithAllColumnsIncluded() throws Throwable
    {
        // Create base table and view that includes all columns
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        createView("all_columns_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                      "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("all_columns_view");
        
        DecoratedKey partitionKey = baseCfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1));
        int nowInSec = FBUtilities.nowInSeconds();
        
        // Create a base row
        Row baseRow = createBaseRow(baseCfs.metadata(), 100, "test", nowInSec);
        
        // Test liveness computation
        LivenessInfo result = ViewUtils.computeLivenessInfoForEntry(view, baseRow, nowInSec);
        
        // When all columns are included, should return the base row's liveness info directly
        assertEquals("Liveness should match base row liveness", 
                    baseRow.primaryKeyLivenessInfo().timestamp(), 
                    result.timestamp());
        assertEquals("TTL should match base row TTL", 
                    baseRow.primaryKeyLivenessInfo().ttl(), 
                    result.ttl());
    }

    @Test
    public void testComputeLivenessInfoForEntryWithPartialColumns() throws Throwable
    {
        // Create base table and view that excludes some columns
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text, v3 int)");
        createView("partial_view", "CREATE MATERIALIZED VIEW %s AS SELECT k, v1 FROM %s " +
                                  "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("partial_view");
        
        int nowInSec = FBUtilities.nowInSeconds();
        
        // Create a base row with some unselected columns
        Row baseRow = createBaseRowWithUnselectedColumns(baseCfs.metadata(), 100, "test", 200, nowInSec);
        
        // Test liveness computation
        LivenessInfo result = ViewUtils.computeLivenessInfoForEntry(view, baseRow, nowInSec);
        
        // Should compute timestamp based on all live cells (including unselected ones)
        assertTrue("Result should be live", result.isLive(nowInSec));
        assertTrue("Timestamp should be >= base liveness timestamp", 
                  result.timestamp() >= baseRow.primaryKeyLivenessInfo().timestamp());
    }

    @Test
    public void testComputeLivenessInfoForEntryWithBaseNonPKColumnInViewPK() throws Throwable
    {
        // Create base table and view where a non-PK column becomes part of view PK
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        createView("non_pk_in_view_pk", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                       "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("non_pk_in_view_pk");
        
        int nowInSec = FBUtilities.nowInSeconds();
        long cellTimestamp = System.currentTimeMillis() * 1000;
        
        // Create a base row where v1 (non-PK in base, PK in view) has specific timestamp and TTL
        Row baseRow = createBaseRowWithSpecificCellTimestamp(baseCfs.metadata(), 100, "test", cellTimestamp, 3600, nowInSec);
        
        // Test liveness computation
        LivenessInfo result = ViewUtils.computeLivenessInfoForEntry(view, baseRow, nowInSec);
        
        // Should use the v1 cell's timestamp and TTL since v1 is part of view PK
        assertEquals("Timestamp should match v1 cell timestamp", cellTimestamp, result.timestamp());
        assertEquals("TTL should match v1 cell TTL", 3600, result.ttl());
    }

    @Test
    public void testIsLiveWithLiveCell() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        
        int nowInSec = FBUtilities.nowInSeconds();
        long timestamp = System.currentTimeMillis() * 1000;
        
        // Create a live cell
        ColumnMetadata v1Column = baseCfs.metadata().getColumn(ByteBufferUtil.bytes("v1"));
        Cell<?> liveCell = BufferCell.live(v1Column, timestamp, ByteBufferUtil.bytes(100));
        
        assertTrue("Live cell should be detected as live", ViewUtils.isLive(liveCell, nowInSec));
    }

    @Test
    public void testIsLiveWithNullCell() throws Throwable
    {
        int nowInSec = FBUtilities.nowInSeconds();
        
        assertFalse("Null cell should not be live", ViewUtils.isLive(null, nowInSec));
    }

    @Test
    public void testIsLiveWithExpiredCell() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        
        int nowInSec = FBUtilities.nowInSeconds();
        long timestamp = (nowInSec - 7200) * 1000000L; // 2 hours ago
        int ttl = 3600; // 1 hour TTL, so expired
        int localDeletionTime = nowInSec - 3600; // Expired 1 hour ago
        
        // Create an expired cell
        ColumnMetadata v1Column = baseCfs.metadata().getColumn(ByteBufferUtil.bytes("v1"));
        Cell<?> expiredCell = BufferCell.expiring(v1Column, timestamp, ttl, localDeletionTime, ByteBufferUtil.bytes(100));
        
        assertFalse("Expired cell should not be live", ViewUtils.isLive(expiredCell, nowInSec));
    }

    @Test
    public void testGetValueForPKWithPartitionKeyColumn() throws Throwable
    {
        // Create base table with composite partition key
        createTable("CREATE TABLE %s (k1 int, k2 text, v1 int, PRIMARY KEY ((k1, k2)))");
        
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = baseCfs.metadata();
        
        // Create partition key components
        ByteBuffer[] partitionKeyComponents = new ByteBuffer[] {
            ByteBufferUtil.bytes(100),
            ByteBufferUtil.bytes("test")
        };
        
        // Create a row (doesn't matter much for partition key testing)
        Row row = createSimpleRow(metadata, 50);
        
        // Test getting k1 (first partition key component)
        ColumnMetadata k1Column = metadata.getColumn(ByteBufferUtil.bytes("k1"));
        ByteBuffer k1Value = ViewUtils.getValueForPK(k1Column, row, partitionKeyComponents);
        assertEquals("Should return first partition key component", 
                    ByteBufferUtil.bytes(100), k1Value);
        
        // Test getting k2 (second partition key component)  
        ColumnMetadata k2Column = metadata.getColumn(ByteBufferUtil.bytes("k2"));
        ByteBuffer k2Value = ViewUtils.getValueForPK(k2Column, row, partitionKeyComponents);
        assertEquals("Should return second partition key component", 
                    ByteBufferUtil.bytes("test"), k2Value);
    }

    @Test
    public void testGetValueForPKWithClusteringColumn() throws Throwable
    {
        // Create base table with clustering columns
        createTable("CREATE TABLE %s (k int, c1 int, c2 text, v1 int, PRIMARY KEY (k, c1, c2))");
        
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = baseCfs.metadata();
        
        // Create partition key components (just k)
        ByteBuffer[] partitionKeyComponents = new ByteBuffer[] {
            ByteBufferUtil.bytes(1)
        };
        
        // Create a row with specific clustering values
        Row row = createRowWithClustering(metadata, 10, "cluster", 50);
        
        // Test getting c1 (first clustering component)
        ColumnMetadata c1Column = metadata.getColumn(ByteBufferUtil.bytes("c1"));
        ByteBuffer c1Value = ViewUtils.getValueForPK(c1Column, row, partitionKeyComponents);
        assertEquals("Should return first clustering component", 
                    ByteBufferUtil.bytes(10), c1Value);
        
        // Test getting c2 (second clustering component)
        ColumnMetadata c2Column = metadata.getColumn(ByteBufferUtil.bytes("c2"));
        ByteBuffer c2Value = ViewUtils.getValueForPK(c2Column, row, partitionKeyComponents);
        assertEquals("Should return second clustering component", 
                    ByteBufferUtil.bytes("cluster"), c2Value);
    }

    @Test
    public void testGetValueForPKWithRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = baseCfs.metadata();
        
        // Create partition key components
        ByteBuffer[] partitionKeyComponents = new ByteBuffer[] {
            ByteBufferUtil.bytes(1)
        };
        
        // Create a row with regular column values
        Row row = createSimpleRow(metadata, 100);
        
        // Test getting v1 (regular column)
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        ByteBuffer v1Value = ViewUtils.getValueForPK(v1Column, row, partitionKeyComponents);
        assertEquals("Should return regular column value", 
                    ByteBufferUtil.bytes(100), v1Value);
    }

    @Test
    public void testGetValueForPKWithNullRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = baseCfs.metadata();
        
        // Create partition key components
        ByteBuffer[] partitionKeyComponents = new ByteBuffer[] {
            ByteBufferUtil.bytes(1)
        };
        
        // Create a row without v1 column (simulating null)
        Row row = createRowWithNullColumn(metadata);
        
        // Test getting v1 (null regular column)
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        ByteBuffer v1Value = ViewUtils.getValueForPK(v1Column, row, partitionKeyComponents);
        assertNull("Should return null for missing regular column", v1Value);
    }

    @Test
    public void testExtractKeyComponentsWithSingleKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int)");
        
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = baseCfs.metadata();
        
        // Create a decorated key with single component
        DecoratedKey decoratedKey = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(100));
        
        // Test extracting components from single key
        ByteBuffer[] components = ViewUtils.extractKeyComponents(decoratedKey, metadata.partitionKeyType);
        
        assertEquals("Should have one component", 1, components.length);
        assertEquals("Component should match original key", 
                    ByteBufferUtil.bytes(100), components[0]);
    }

    @Test
    public void testExtractKeyComponentsWithCompositeKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 text, v1 int, PRIMARY KEY ((k1, k2)))");
        
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = baseCfs.metadata();
        
        // Create a decorated key with composite components
        CompositeType keyType = (CompositeType) metadata.partitionKeyType;
        ByteBuffer compositeKey = keyType.decompose(100, "test");
        DecoratedKey decoratedKey = metadata.partitioner.decorateKey(compositeKey);
        
        // Test extracting components from composite key
        ByteBuffer[] components = ViewUtils.extractKeyComponents(decoratedKey, metadata.partitionKeyType);
        
        assertEquals("Should have two components", 2, components.length);
        assertEquals("First component should match k1", 
                    ByteBufferUtil.bytes(100), components[0]);
        assertEquals("Second component should match k2", 
                    ByteBufferUtil.bytes("test"), components[1]);
    }

    @Test
    public void testAddColumnDataToBuilderWithSimpleColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = baseCfs.metadata();
        
        // Create a row builder
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        
        long timestamp = System.currentTimeMillis() * 1000;
        int nowInSec = FBUtilities.nowInSeconds();
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        
        // Create a cell to add
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        Cell<?> v1Cell = BufferCell.live(v1Column, timestamp, ByteBufferUtil.bytes(100));
        
        // Test adding the cell using ViewUtils
        ViewUtils.addColumnDataToBuilder(builder, v1Column, v1Cell);
        
        Row row = builder.build();
        Cell<?> resultCell = row.getCell(v1Column);
        
        assertNotNull("Cell should be added to row", resultCell);
        assertEquals("Cell value should match", 
                    ByteBufferUtil.bytes(100), resultCell.buffer());
    }

    // Helper methods
    
    private Row createBaseRow(TableMetadata metadata, int v1Value, String v2Value, int nowInSec)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        
        long timestamp = System.currentTimeMillis() * 1000;
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        
        // Add v1 column
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1Column != null)
        {
            Cell<?> v1Cell = BufferCell.live(v1Column, timestamp, ByteBufferUtil.bytes(v1Value));
            builder.addCell(v1Cell);
        }
        
        // Add v2 column
        ColumnMetadata v2Column = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Column != null && v2Value != null)
        {
            Cell<?> v2Cell = BufferCell.live(v2Column, timestamp, ByteBufferUtil.bytes(v2Value));
            builder.addCell(v2Cell);
        }
        
        return builder.build();
    }

    private Row createBaseRowWithUnselectedColumns(TableMetadata metadata, int v1Value, String v2Value, int v3Value, int nowInSec)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        
        long timestamp = System.currentTimeMillis() * 1000;
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        
        // Add v1 column (selected in view)
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1Column != null)
        {
            Cell<?> v1Cell = BufferCell.live(v1Column, timestamp, ByteBufferUtil.bytes(v1Value));
            builder.addCell(v1Cell);
        }
        
        // Add v2 column (selected in view)
        ColumnMetadata v2Column = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Column != null && v2Value != null)
        {
            Cell<?> v2Cell = BufferCell.live(v2Column, timestamp, ByteBufferUtil.bytes(v2Value));
            builder.addCell(v2Cell);
        }
        
        // Add v3 column (NOT selected in view, so it's an "unselected" column)
        ColumnMetadata v3Column = metadata.getColumn(ByteBufferUtil.bytes("v3"));
        if (v3Column != null)
        {
            // Use a slightly later timestamp for the unselected column
            Cell<?> v3Cell = BufferCell.live(v3Column, timestamp + 1000, ByteBufferUtil.bytes(v3Value));
            builder.addCell(v3Cell);
        }
        
        return builder.build();
    }

    private Row createBaseRowWithSpecificCellTimestamp(TableMetadata metadata, int v1Value, String v2Value, 
                                                      long cellTimestamp, int cellTtl, int nowInSec)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        
        long baseTimestamp = System.currentTimeMillis() * 1000;
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(baseTimestamp, nowInSec));
        
        // Add v1 column with specific timestamp and TTL
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1Column != null)
        {
            int localDeletionTime = nowInSec + cellTtl;
            Cell<?> v1Cell = BufferCell.expiring(v1Column, cellTimestamp, cellTtl, localDeletionTime, ByteBufferUtil.bytes(v1Value));
            builder.addCell(v1Cell);
        }
        
        // Add v2 column
        ColumnMetadata v2Column = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Column != null && v2Value != null)
        {
            Cell<?> v2Cell = BufferCell.live(v2Column, baseTimestamp, ByteBufferUtil.bytes(v2Value));
            builder.addCell(v2Cell);
        }
        
        return builder.build();
    }

    private Row createSimpleRow(TableMetadata metadata, int v1Value)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        
        long timestamp = System.currentTimeMillis() * 1000;
        int nowInSec = FBUtilities.nowInSeconds();
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        
        // Add v1 column
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1Column != null)
        {
            Cell<?> v1Cell = BufferCell.live(v1Column, timestamp, ByteBufferUtil.bytes(v1Value));
            builder.addCell(v1Cell);
        }
        
        return builder.build();
    }

    private Row createRowWithClustering(TableMetadata metadata, int c1Value, String c2Value, int v1Value)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        
        // Create clustering with c1 and c2 values
        Clustering clustering = Clustering.make(ByteBufferUtil.bytes(c1Value), ByteBufferUtil.bytes(c2Value));
        builder.newRow(clustering);
        
        long timestamp = System.currentTimeMillis() * 1000;
        int nowInSec = FBUtilities.nowInSeconds();
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        
        // Add v1 column
        ColumnMetadata v1Column = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1Column != null)
        {
            Cell<?> v1Cell = BufferCell.live(v1Column, timestamp, ByteBufferUtil.bytes(v1Value));
            builder.addCell(v1Cell);
        }
        
        return builder.build();
    }

    private Row createRowWithNullColumn(TableMetadata metadata)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        
        long timestamp = System.currentTimeMillis() * 1000;
        int nowInSec = FBUtilities.nowInSeconds();
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        
        // Don't add v1 column (simulating null)
        
        return builder.build();
    }
}
