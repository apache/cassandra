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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ViewUpdateGenerator, specifically focusing on the addBaseTableRowForReadRebuild functionality.
 */
public class ViewUpdateGeneratorTest extends ViewAbstractTest
{
    private static final AtomicInteger testCounter = new AtomicInteger(0);
    private long timestamp;
    private int nowInSec;
    private long readTime;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        timestamp = currentTimeMillis();
        nowInSec = FBUtilities.nowInSeconds();
        readTime = timestamp + 100; // read time slightly after write time
    }

    /**
     * Test addBaseTableRowForReadRebuild with REWRITE action for views with same PK as base table.
     * This should create a new view entry when the base row is alive.
     */
    @Test
    public void testAddBaseTableRowForReadRebuildRewriteSamePK() throws Throwable
    {
        // base: ((k,c),v,random), mv: ((c,k),v)
        createTable("CREATE TABLE %s (k int, c int, v text, random text, PRIMARY KEY (k, c))");
        String viewName = getUniqueViewName();
        createView(viewName, "CREATE MATERIALIZED VIEW %s AS SELECT k, c, v FROM %s " +
                               "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName(viewName);
        assertNotNull("View should exist", view);

        // Create test data (1, 2, test_value)
        DecoratedKey basePartitionKey = baseCfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1));
        Clustering<?> clustering = Clustering.make(ByteBufferUtil.bytes(2));
        Row baseRow = createBaseRow(baseCfs.metadata(), clustering, timestamp, nowInSec,
                                    createCell(baseCfs.metadata(), timestamp, "v", ByteBufferUtil.bytes("test_value")),
                                    createCell(baseCfs.metadata(), timestamp, "random", ByteBufferUtil.bytes("random_value")));

        // Test 1: REWRITE action - repair (2,1) -> base row is alive and matching the repair PK
        ViewUpdateGenerator generator = new ViewUpdateGenerator(view, basePartitionKey, nowInSec);
        generator.addBaseTableRowForReadRebuild(baseRow, clustering, readTime, null);
        Collection<PartitionUpdate> updates = generator.generateViewUpdates();

        assertEquals("Should generate exactly one partition update", 1, updates.size());
        PartitionUpdate update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 2",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(2)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // random shouldn't be included
            assertEquals(1, updatedRow.columnCount());
            ColumnMetadata vColumn = view.getDefinition().metadata.getColumn(ByteBufferUtil.bytes("v"));
            Cell<?> vCell = updatedRow.getCell(vColumn);
            assertEquals(ByteBufferUtil.bytes("test_value"), vCell.buffer());
            assertEquals(timestamp, vCell.timestamp());;
            assertEquals(Cell.NO_DELETION_TIME, vCell.localDeletionTime());
            assertEquals(Cell.NO_TTL, vCell.ttl());
        }

        // Test 2: DELETE action - repair (2,1) -> base row is null (deleted)
        generator.clear();
        generator.addBaseTableRowForReadRebuild(null, clustering, readTime, null);
        updates = generator.generateViewUpdates();

        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 2",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(2)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should delete with readTime
            assertEquals(readTime, updatedRow.deletion().time().markedForDeleteAt());
            // should be row tombstone only
            assertEquals(0, updatedRow.columnCount());
        }

        // Test 3: DELETE action - repair (2,1) -> base row is dead (with row deletion)
        // Create a dead base row (with row deletion)
        generator.clear();
        Row deadRow = createDeadRow(clustering, timestamp, nowInSec - 1);
        generator.addBaseTableRowForReadRebuild(deadRow, clustering, readTime, null);
        updates = generator.generateViewUpdates();

        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 2",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(2)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should be deleted with the tombstone
            assertEquals(timestamp, updatedRow.deletion().time().markedForDeleteAt());
            assertEquals(nowInSec - 1, updatedRow.deletion().time().localDeletionTime());
            // should be row tombstone only
            assertEquals(0, updatedRow.columnCount());
        }

        // Test 4: REWRITE action - repair (2,1) -> base row is alive with dead cell v
        generator.clear();
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        Row rowWithTS = createBaseRow(baseCfs.metadata(), clustering, timestamp, nowInSec,
                                      createCellTombstone(baseCfs.metadata(), timestamp - 1, nowInSec - 1, "v"),
                                      createCellTombstone(baseCfs.metadata(), timestamp - 2, nowInSec - 2, "random"));
        generator.addBaseTableRowForReadRebuild(rowWithTS, clustering, readTime, null);
        updates = generator.generateViewUpdates();

        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 2",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(2)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // row should be live (no row tombstone)
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // cell should be tombstone
            // random shouldn't be included
            assertEquals(1, updatedRow.columnCount());
            ColumnMetadata vColumn = view.getDefinition().metadata.getColumn(ByteBufferUtil.bytes("v"));
            Cell<?> vCell = updatedRow.getCell(vColumn);
            assertTrue(vCell.isTombstone());
            assertEquals(timestamp - 1, vCell.timestamp());;
            assertEquals(nowInSec - 1, vCell.localDeletionTime());
            assertEquals(Cell.NO_TTL, vCell.ttl());
        }

        // Test 5: DELETE action - repair (2,1) -> base only has expired unselected random column
        generator.clear();
        builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        builder.addCell(createExpiringCell(baseCfs.metadata(), timestamp - 1000, nowInSec - 1, 1, "random", ByteBufferUtil.bytes("random_value")));
        Row ttledRow = builder.build();
        generator.addBaseTableRowForReadRebuild(ttledRow, clustering, readTime, null);
        updates = generator.generateViewUpdates();

        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 2",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(2)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should see expired liveness
            assertEquals(LivenessInfo.EXPIRED_LIVENESS_TTL, updatedRow.primaryKeyLivenessInfo().ttl());
            // should see the deletion time as timestamp - 1000
            assertEquals(timestamp - 1000, updatedRow.primaryKeyLivenessInfo().timestamp());
            // should not see row tombstone
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // should have no column
            assertEquals(0, updatedRow.columnCount());
        }

        // Test 6: DELETE action - repair (2,1) -> base only has expired selected v column
        generator.clear();
        builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        builder.addCell(createExpiringCell(baseCfs.metadata(), timestamp - 1000, nowInSec - 1, 1, "v", ByteBufferUtil.bytes("test_value")));
        ttledRow = builder.build();
        generator.addBaseTableRowForReadRebuild(ttledRow, clustering, readTime, null);
        updates = generator.generateViewUpdates();

        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 2",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(2)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should see expired liveness
            assertEquals(LivenessInfo.EXPIRED_LIVENESS_TTL, updatedRow.primaryKeyLivenessInfo().ttl());
            // should see the deletion time as timestamp - 1000
            assertEquals(timestamp - 1000, updatedRow.primaryKeyLivenessInfo().timestamp());
            // should not see row tombstone
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // should have no column
            assertEquals(0, updatedRow.columnCount());
        }
    }

    /**
     * Test addBaseTableRowForReadRebuild for views with non-PK column in view PK.
     * This tests the scenario where the view PK includes a non-PK column from the base table.
     */
    @Test
    public void testAddBaseTableRowForReadRebuildDifferentPK() throws Throwable
    {
        // base: ((k),v1,v2,random) mv: ((v1,k),v2) - v1 is non-PK column in base table but PK in view
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text, random text)");
        String viewName = getUniqueViewName();
        createView(viewName, "CREATE MATERIALIZED VIEW %s AS SELECT v1, k, v2 FROM %s " +
                               "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");

        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName(viewName);

        DecoratedKey basePartitionKey = baseCfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1));
        Clustering<?> clustering = Clustering.EMPTY; // Primary key only has k
        ByteBuffer nonPKValue = ByteBufferUtil.bytes(100); // matches v1 value
        Row baseRow = createBaseRow(baseCfs.metadata(), clustering, timestamp, nowInSec,
                                    createCell(baseCfs.metadata(), timestamp, "v1", nonPKValue),
                                    createCell(baseCfs.metadata(), timestamp, "v2", ByteBufferUtil.bytes("test_value")),
                                    createCell(baseCfs.metadata(), timestamp, "random", ByteBufferUtil.bytes("random_value")));

        // Test 1: REWRITE action - repair (100,1) -> read (1) from base, nonPK matching
        ViewUpdateGenerator generator = new ViewUpdateGenerator(view, basePartitionKey, nowInSec);
        generator.addBaseTableRowForReadRebuild(baseRow, clustering, readTime, nonPKValue);
        Collection<PartitionUpdate> updates = generator.generateViewUpdates();
        // Should generate an update (REWRITE action)
        assertEquals("Should generate exactly one partition update", 1, updates.size());
        PartitionUpdate update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 100",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(100)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // should include v2 only
            assertEquals(1, updatedRow.columnCount());
            assertEquals(timestamp, updatedRow.primaryKeyLivenessInfo().timestamp());
            ColumnMetadata vColumn = view.getDefinition().metadata.getColumn(ByteBufferUtil.bytes("v2"));
            Cell<?> vCell = updatedRow.getCell(vColumn);
            assertEquals(ByteBufferUtil.bytes("test_value"), vCell.buffer());
            assertEquals(timestamp, vCell.timestamp());;
            assertEquals(Cell.NO_DELETION_TIME, vCell.localDeletionTime());
            assertEquals(Cell.NO_TTL, vCell.ttl());
        }

        // Test 2: DELETE action - repair (200,1) from view, read (1) from base, nonPK not matching (100)
        generator.clear();
        ByteBuffer mismatchedNonPKValue = ByteBufferUtil.bytes(200);
        generator.addBaseTableRowForReadRebuild(baseRow, clustering, readTime, mismatchedNonPKValue);
        updates = generator.generateViewUpdates();
        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 200",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(200)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should see expired liveness
            assertEquals(LivenessInfo.EXPIRED_LIVENESS_TTL, updatedRow.primaryKeyLivenessInfo().ttl());
            assertEquals(timestamp, updatedRow.primaryKeyLivenessInfo().timestamp());
            // no extra row tombstone
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // no columns
            assertEquals(0, updatedRow.columnCount());
        }

        // Test 3: DELETE action - repair (100,1) from view, read null from base
        generator.clear();
        generator.addBaseTableRowForReadRebuild(null, clustering, readTime, nonPKValue);
        updates = generator.generateViewUpdates();
        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 100",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(100)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should delete with readTime
            assertEquals(readTime, updatedRow.deletion().time().markedForDeleteAt());
            // should be row tombstone only
            assertEquals(0, updatedRow.columnCount());
        }

        // Test 4: DELETE action - repair (100,1) from view, read dead row from base
        generator.clear();
        Row deadRow = createDeadRow(clustering, timestamp, nowInSec - 1);
        generator.addBaseTableRowForReadRebuild(deadRow, clustering, readTime, nonPKValue);
        updates = generator.generateViewUpdates();
        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 100",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(100)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should be deleted with the tombstone
            assertEquals(timestamp, updatedRow.deletion().time().markedForDeleteAt());
            assertEquals(nowInSec - 1, updatedRow.deletion().time().localDeletionTime());
            // should be row tombstone only
            assertEquals(0, updatedRow.columnCount());
        }

        // Test 5: DELETE action - repair (100,1) from view, read (1) from base, but nonPK column is dead
        generator.clear();
        // Create a base row with dead v1 cell (tombstone)
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        builder.addCell(BufferCell.tombstone(baseCfs.metadata().getColumn(ByteBufferUtil.bytes("v1")), timestamp, nowInSec));
        builder.addCell(BufferCell.live(baseCfs.metadata().getColumn(ByteBufferUtil.bytes("v2")), timestamp, ByteBufferUtil.bytes("test_value")));
        builder.addCell(BufferCell.live(baseCfs.metadata().getColumn(ByteBufferUtil.bytes("random")), timestamp, ByteBufferUtil.bytes("random_value")));
        Row baseRowWithDeadNonPK = builder.build();
        generator.addBaseTableRowForReadRebuild(baseRowWithDeadNonPK, clustering, readTime, nonPKValue);
        updates = generator.generateViewUpdates();
        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 100",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(100)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should see expired liveness
            assertEquals(LivenessInfo.EXPIRED_LIVENESS_TTL, updatedRow.primaryKeyLivenessInfo().ttl());
            assertEquals(timestamp, updatedRow.primaryKeyLivenessInfo().timestamp());
            assertEquals(nowInSec, updatedRow.primaryKeyLivenessInfo().localExpirationTime());
            // should not see row tombstone
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // should be row tombstone only
            assertEquals(0, updatedRow.columnCount());
        }

        // Test 6: DELETE action - repair (100,1) from view, read (1) from base, but nonPK column is null
        generator.clear();
        // Create a base row with null v1 cell
        builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
        builder.addCell(BufferCell.live(baseCfs.metadata().getColumn(ByteBufferUtil.bytes("v2")), timestamp, ByteBufferUtil.bytes("test_value")));
        builder.addCell(BufferCell.live(baseCfs.metadata().getColumn(ByteBufferUtil.bytes("random")), timestamp, ByteBufferUtil.bytes("random_value")));
        Row baseRowWithNullNonPK = builder.build();
        generator.addBaseTableRowForReadRebuild(baseRowWithNullNonPK, clustering, readTime, nonPKValue);
        updates = generator.generateViewUpdates();
        assertEquals("Should generate exactly one partition update", 1, updates.size());
        update = updates.iterator().next();
        assertEquals("Update to view should have partition key = 100",
                     view.getDefinition().metadata.partitioner.decorateKey(ByteBufferUtil.bytes(100)), update.partitionKey());
        assertEquals("Should have only 1 row", 1, update.rowCount());
        for (Row updatedRow : update)
        {
            assertEquals("Update to view should have clustering = 1", Clustering.make(ByteBufferUtil.bytes(1)), updatedRow.clustering());
            // should see expired liveness
            assertEquals(LivenessInfo.EXPIRED_LIVENESS_TTL, updatedRow.primaryKeyLivenessInfo().ttl());
            // write time should be readTime (latest)
            assertEquals(readTime, updatedRow.primaryKeyLivenessInfo().timestamp());
            assertEquals(nowInSec, updatedRow.primaryKeyLivenessInfo().localExpirationTime());
            // should not see row tombstone
            assertEquals(Row.Deletion.LIVE, updatedRow.deletion());
            // should have no column
            assertEquals(0, updatedRow.columnCount());
        }
    }

    /**
     * Test maybeAddDeletionFromReadTime with complex column data.
     * This covers the complex column handling branch and the continue statement in maybeAddDeletionFromReadTime.
     */
    @Test
    public void testMaybeAddDeletionFromReadTimeComplexColumn() throws Throwable
    {
        // Create table with clustering column and complex column (map)
        // base: ((k,c),v1,complex_col) mv: ((v1,k,c),complex_col) - v1 is non-PK column in base table but PK in view
        createTable("CREATE TABLE %s (k int, c int, v1 int, complex_col map<text, int>, PRIMARY KEY (k, c))");
        String viewName = getUniqueViewName();
        createView(viewName, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                               "WHERE k IS NOT NULL AND c IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k, c)");
        ColumnFamilyStore baseCfs = getCurrentColumnFamilyStore();
        View view = baseCfs.keyspace.viewManager.getByName(viewName);
        DecoratedKey basePartitionKey = baseCfs.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(1));
        Clustering<?> clustering = Clustering.make(ByteBufferUtil.bytes(2)); // c = 2

        // Create base row with complex column data but null v1
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);
        LivenessInfo livenessInfo = LivenessInfo.create(nowInSec, nowInSec);
        builder.addPrimaryKeyLivenessInfo(livenessInfo);
        // Add complex column data (map entries)
        ColumnMetadata complexColumn = baseCfs.metadata().getColumn(ByteBufferUtil.bytes("complex_col"));
        DeletionTime complexDeletion = new DeletionTime(nowInSec - 2, nowInSec - 2);
        builder.addComplexDeletion(complexColumn, complexDeletion);
        // Add some map entries
        CellPath cellPath1 = CellPath.create(ByteBufferUtil.bytes("key1"));
        Cell<?> mapCell1 = BufferCell.live(complexColumn, nowInSec - 1, ByteBufferUtil.bytes(10), cellPath1);
        builder.addCell(mapCell1);
        CellPath cellPath2 = CellPath.create(ByteBufferUtil.bytes("key2"));
        Cell<?> mapCell2 = BufferCell.live(complexColumn, nowInSec - 1, ByteBufferUtil.bytes(20), cellPath2);
        builder.addCell(mapCell2);
        Row baseRowWithComplexData = builder.build();

        ViewUpdateGenerator generator = new ViewUpdateGenerator(view, basePartitionKey, nowInSec);
        // Test maybeAddDeletionFromReadTime with base row that has NULL v1
        Row result = generator.maybeAddDeletionFromReadTime(baseRowWithComplexData, clustering, readTime);
        // Should return a modified row with:
        // 1. A tombstone cell for v1 column with timestamp = readTime (because v1 was null)
        // 2. All the original complex column data preserved
        // The result should have a tombstone cell for v1 with timestamp = readTime
        ColumnMetadata v1Column = baseCfs.metadata().getColumn(ByteBufferUtil.bytes("v1"));
        Cell<?> resultV1Cell = result.getCell(v1Column);
        assertNotNull("Should have a cell for v1 column", resultV1Cell);
        assertTrue("v1 cell should be a tombstone", resultV1Cell.isTombstone());
        assertEquals("v1 tombstone timestamp should match read time", readTime, resultV1Cell.timestamp());

        // Verify that complex column data is preserved
        ComplexColumnData resultComplexData = result.getComplexColumnData(complexColumn);
        assertNotNull("Complex column data should be preserved", resultComplexData);

        // Check that complex deletion is preserved
        assertEquals("Complex deletion should be preserved",
                    complexDeletion.markedForDeleteAt(),
                    resultComplexData.complexDeletion().markedForDeleteAt());

        // Check that individual cells are preserved
        int cellCount = 0;
        for (Cell<?> cell : resultComplexData)
        {
            cellCount++;
            assertFalse("Map cells should not be tombstones", cell.isTombstone());
        }
        assertEquals("Should have preserved both map cells", 2, cellCount);
    }

    // Helper method to generate unique view names
    private String getUniqueViewName()
    {
        return "test_view_" + testCounter.incrementAndGet();
    }

    private Row createBaseRow(TableMetadata metadata, Clustering<?> clustering, long timestamp, int nowInSec, Cell<?>... cells)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);

        LivenessInfo livenessInfo = LivenessInfo.create(timestamp, nowInSec);
        builder.addPrimaryKeyLivenessInfo(livenessInfo);

        for (ColumnMetadata column : metadata.regularColumns())
        {
            for (Cell<?> cell : cells)
            {
                if (cell.column().equals(column))
                {
                    builder.addCell(cell);
                    break;
                }
            }
        }

        return builder.build();
    }

    private Cell<?> createCell(TableMetadata metadata, long timestamp, String col, ByteBuffer value)
    {
        ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(col));
        return BufferCell.live(column, timestamp, value);
    }

    private Cell<?> createExpiringCell(TableMetadata metadata, long timestamp, int nowInSec, int ttl, String col, ByteBuffer value)
    {
        ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(col));
        return BufferCell.expiring(column, timestamp, ttl, nowInSec, value);
    }

    private Cell<?> createCellTombstone(TableMetadata metadata, long markForDeleteAt, int ldt, String col)
    {
        ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(col));
        return BufferCell.tombstone(column, markForDeleteAt, ldt);
    }

    private Row createDeadRow(Clustering<?> clustering, long markForDeleteAt, int ldt)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering);

        DeletionTime deletion = new DeletionTime(markForDeleteAt, ldt);
        builder.addRowDeletion(Row.Deletion.regular(deletion));

        return builder.build();
    }
}
