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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ViewRowTranslator.
 */
public class ViewRowTranslatorTest extends ViewAbstractTest
{
    private static final String KEYSPACE = "view_row_translator_test";
    
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
    }

    @Test
    public void testBasicRowTranslation() throws Throwable
    {
        // Create base table: CREATE TABLE base (k int PRIMARY KEY, v1 int, v2 text)
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        
        // Create view: CREATE MATERIALIZED VIEW view AS SELECT * FROM base 
        // WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)
        createView("test_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                               "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("test_view");
        assertNotNull("View should exist", view);

        // Create a base table row
        DecoratedKey basePartitionKey = baseCfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1));
        Row baseRow = createBaseRow(baseCfs.metadata(), 100, "test_value");
        
        int nowInSec = FBUtilities.nowInSeconds();

        // Test translateForBackfill
        ViewRowTranslator.BackfillRowResult result = 
            ViewRowTranslator.translateForBackfill(view, baseRow, basePartitionKey, nowInSec);
        
        assertNotNull("Translation should succeed", result);
        assertNotNull("View row should not be null", result.viewRow);
        assertNotNull("View partition key should not be null", result.viewPartitionKey);

        // Verify the view row has the correct clustering (k=1)
        assertEquals("View clustering should match k value",
                    ByteBufferUtil.bytes(1),
                    result.viewRow.clustering().bufferAt(0));

        // Verify the view partition key is based on v1 (100)
        assertEquals("View partition key should be based on v1", 
                    ByteBufferUtil.bytes(100), 
                    result.viewPartitionKey.getKey());
    }

    @Test
    public void testViewFilterMatching() throws Throwable
    {
        // Create base table with composite partition key to allow filtering on partition key
        createTable("CREATE TABLE %s (k1 int, k2 int, v1 int, v2 text, PRIMARY KEY ((k1, k2)))");
        
        // Create view with WHERE clause on partition key component: k2 = 10
        createView("filtered_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE k1 IS NOT NULL AND k2 = 10 AND v1 IS NOT NULL PRIMARY KEY (v1, k1, k2)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("filtered_view");
        
        // Create base partition key (k1=1, k2=10) - matches filter
        CompositeType keyType = (CompositeType) baseCfs.metadata().partitionKeyType;
        ByteBuffer matchingKey = keyType.decompose(1, 10);
        DecoratedKey matchingPartitionKey = baseCfs.metadata().partitioner.decorateKey(matchingKey);
        
        // Create base partition key (k1=1, k2=20) - doesn't match filter  
        ByteBuffer nonMatchingKey = keyType.decompose(1, 20);
        DecoratedKey nonMatchingPartitionKey = baseCfs.metadata().partitioner.decorateKey(nonMatchingKey);
        
        int nowInSec = FBUtilities.nowInSeconds();

        // Test row that matches filter (k2 = 10)
        Row matchingRow = createBaseRowForCompositeKey(baseCfs.metadata(), 100, "test");
        ViewRowTranslator.BackfillRowResult result1 = 
            ViewRowTranslator.translateForBackfill(view, matchingRow, matchingPartitionKey, nowInSec);
        assertNotNull("Row matching filter should be translated", result1);

        // Test row that doesn't match filter (k2 = 20)
        Row nonMatchingRow = createBaseRowForCompositeKey(baseCfs.metadata(), 100, "test");
        ViewRowTranslator.BackfillRowResult result2 = 
            ViewRowTranslator.translateForBackfill(view, nonMatchingRow, nonMatchingPartitionKey, nowInSec);
        assertNull("Row not matching filter should be filtered out", result2);
    }

    @Test
    public void testNullColumnHandling() throws Throwable
    {
        // Create base table
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        
        // Create view
        createView("null_test_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                    "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("null_test_view");
        
        DecoratedKey basePartitionKey = baseCfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1));
        int nowInSec = FBUtilities.nowInSeconds();

        // Test row with null v1 (should be filtered out because v1 IS NOT NULL in view)
        Row rowWithNullV1 = createBaseRowWithNullV1(baseCfs.metadata(), "test");
        ViewRowTranslator.BackfillRowResult result = 
            ViewRowTranslator.translateForBackfill(view, rowWithNullV1, basePartitionKey, nowInSec);
        assertNull("Row with null required column should be filtered out", result);
    }

    @Test
    public void testDifferentPrimaryKeyStructure() throws Throwable
    {
        // Create base table with composite partition key
        createTable("CREATE TABLE %s (k1 int, k2 text, v1 int, v2 text, PRIMARY KEY ((k1, k2)))");
        
        // Create view with different PK structure: (v1, k1, k2)
        createView("pk_test_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                  "WHERE k1 IS NOT NULL AND k2 IS NOT NULL AND v1 IS NOT NULL " +
                                  "PRIMARY KEY (v1, k1, k2)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("pk_test_view");
        
        // Create composite partition key (k1=1, k2="test")
        TableMetadata baseMetadata = baseCfs.metadata();
        CompositeType keyType = (CompositeType) baseMetadata.partitionKeyType;
        ByteBuffer compositeKey = keyType.decompose(1, "test");
        DecoratedKey basePartitionKey = baseCfs.metadata().partitioner.decorateKey(compositeKey);
        
        // Create base row (no clustering columns since we have composite partition key)
        Row baseRow = createBaseRowForCompositeKey(baseCfs.metadata(), 100, "value");
        
        int nowInSec = FBUtilities.nowInSeconds();

        ViewRowTranslator.BackfillRowResult result = 
            ViewRowTranslator.translateForBackfill(view, baseRow, basePartitionKey, nowInSec);
        
        assertNotNull("Translation should succeed for composite keys", result);
        
        // View partition key should be v1 (100)
        assertEquals("View partition key should be v1", 
                    ByteBufferUtil.bytes(100), 
                    result.viewPartitionKey.getKey());
        
        // View clustering should be (k1=1, k2="test")
        assertEquals("First clustering component should be k1", 
                    ByteBufferUtil.bytes(1), 
                    result.viewRow.clustering().bufferAt(0));
        assertEquals("Second clustering component should be k2", 
                    ByteBufferUtil.bytes("test"), 
                    result.viewRow.clustering().bufferAt(1));
    }

    @Test
    public void testDirectMethodCalls() throws Throwable
    {
        // Create base table and view
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        createView("direct_test_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                      "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName("direct_test_view");
        
        DecoratedKey basePartitionKey = baseCfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1));
        Row baseRow = createBaseRow(baseCfs.metadata(), 100, "test");
        int nowInSec = FBUtilities.nowInSeconds();

        // Test individual method calls
        assertTrue("Row should match view filter",
                   view.matchesViewFilter(basePartitionKey, baseRow, nowInSec));

        Row viewRow = ViewRowTranslator.translateBaseRowToViewRow(view, baseRow, basePartitionKey, nowInSec);
        assertNotNull("View row should be created", viewRow);

        DecoratedKey viewPartitionKey = ViewRowTranslator.calculateViewPartitionKey(view, baseRow, basePartitionKey);
        assertNotNull("View partition key should be calculated", viewPartitionKey);
        
        assertEquals("View partition key should match expected value", 
                    ByteBufferUtil.bytes(100), 
                    viewPartitionKey.getKey());
    }

    // Helper methods

    private Row createBaseRow(TableMetadata metadata, int v1Value, String v2Value)
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
        
        // Add v2 column
        ColumnMetadata v2Column = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Column != null && v2Value != null)
        {
            Cell<?> v2Cell = BufferCell.live(v2Column, timestamp, ByteBufferUtil.bytes(v2Value));
            builder.addCell(v2Cell);
        }
        
        return builder.build();
    }

    private Row createBaseRowWithNullV1(TableMetadata metadata, String v2Value)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Clustering.EMPTY);
        
        long timestamp = System.currentTimeMillis() * 1000;
        int nowInSec = FBUtilities.nowInSeconds();
        
        // Add primary key liveness
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        
        // Don't add v1 column (simulating null)
        
        // Add v2 column
        ColumnMetadata v2Column = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Column != null && v2Value != null)
        {
            Cell<?> v2Cell = BufferCell.live(v2Column, timestamp, ByteBufferUtil.bytes(v2Value));
            builder.addCell(v2Cell);
        }
        
        return builder.build();
    }


    private Row createBaseRowForCompositeKey(TableMetadata metadata, int v1Value, String v2Value)
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
        
        // Add v2 column
        ColumnMetadata v2Column = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Column != null && v2Value != null)
        {
            Cell<?> v2Cell = BufferCell.live(v2Column, timestamp, ByteBufferUtil.bytes(v2Value));
            builder.addCell(v2Cell);
        }
        
        return builder.build();
    }
}
