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

package org.apache.cassandra.db.filter;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ColumnFilter's filtering behavior on actual Row instances.
 * Complements ColumnFilterTest which tests filter properties without applying them to rows.
 * 
 * This test is parameterized to run with all row implementations.
 */
@RunWith(Parameterized.class)
public class ColumnFilterRowTest
{
    private static final long TIMESTAMP = 1000L;
    private static final int TTL = 0;
    private static final long LOCAL_DELETION_TIME = Integer.MAX_VALUE;
    public static final DeletionTime COMPLEX_DELETION = DeletionTime.build(TIMESTAMP - 1, LOCAL_DELETION_TIME);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][] {
            { "BTreeRow", (Function<Columns, Row.Builder>) cols -> BTreeRow.unsortedBuilder() }
        });
    }

    @Parameterized.Parameter(0)
    public String rowType;

    @Parameterized.Parameter(1)
    public Function<Columns, Row.Builder> builderFactory;

    private TableMetadata metadata;
    private ColumnMetadata s1; // static simple
    private ColumnMetadata s2; // static complex (set)
    private ColumnMetadata v1; // regular simple
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
                                .addStaticColumn("s1", Int32Type.instance)
                                .addStaticColumn("s2", SetType.getInstance(Int32Type.instance, true))
                                .addRegularColumn("v1", Int32Type.instance)
                                .addRegularColumn("v2", SetType.getInstance(Int32Type.instance, true))
                                .build();

        s1 = metadata.getColumn(ByteBufferUtil.bytes("s1"));
        s2 = metadata.getColumn(ByteBufferUtil.bytes("s2"));
        v1 = metadata.getColumn(ByteBufferUtil.bytes("v1"));
        v2 = metadata.getColumn(ByteBufferUtil.bytes("v2"));
        clustering = Clustering.make(ByteBufferUtil.bytes(1));
        regularAndStaticColumns = metadata.regularAndStaticColumns();
    }

    private Row.Builder newBuilder(Clustering<?> clustering)
    {
        return builderFactory.apply(regularAndStaticColumns.columns(clustering == Clustering.STATIC_CLUSTERING));
    }

    @Test
    public void testFilterAllColumns()
    {
        // Create a row with all column types
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(simpleCell(v1, 100));
        builder.addCell(complexCell(v2, 0, 200));
        builder.addCell(complexCell(v2, 1, 201));
        Row row = builder.build();

        // Filter with "SELECT *" - should keep everything
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertEquals(100, ByteBufferUtil.toInt(filtered.getCell(v1).buffer()));
        assertNotNull(filtered.getCell(v2, cellPath(0)));
        assertNotNull(filtered.getCell(v2, cellPath(1)));
    }

    @Test
    public void testFilterSimpleColumn()
    {
        // Create a row with multiple simple columns
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(simpleCell(v1, 100));
        Row row = builder.build();

        // Filter to select only v1
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(v1).build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertEquals(100, ByteBufferUtil.toInt(filtered.getCell(v1).buffer()));
    }

    @Test
    public void testFilterExcludesUnselectedSimpleColumn()
    {
        // Create a row with v1
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(simpleCell(v1, 100));
        Row row = builder.build();

        // Filter that doesn't include v1
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(v2).build();
        Row filtered = row.filter(filter, metadata);

        // Row becomes empty when all columns are filtered out
        assertNull(filtered);
    }

    @Test
    public void testFilterComplexColumn()
    {
        // Create a row with complex column cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addComplexDeletion(v2, COMPLEX_DELETION);
        builder.addCell(complexCell(v2, 0, 200));
        builder.addCell(complexCell(v2, 1, 201));
        builder.addCell(complexCell(v2, 2, 202));
        Row row = builder.build();

        // Filter to select entire complex column
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(v2).build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v2, cellPath(0)));
        assertNotNull(filtered.getCell(v2, cellPath(1)));
        assertNotNull(filtered.getCell(v2, cellPath(2)));
        assertEquals(COMPLEX_DELETION, filtered.getComplexColumnData(v2).complexDeletion());
    }

    @Test
    public void testFilterComplexColumnWithCellSelection()
    {
        // Create a row with multiple cells in complex column
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addComplexDeletion(v2, COMPLEX_DELETION);
        builder.addCell(complexCell(v2, 0, 200));
        builder.addCell(complexCell(v2, 1, 201));
        builder.addCell(complexCell(v2, 2, 202));
        builder.addCell(complexCell(v2, 3, 203));
        Row row = builder.build();

        // Filter to select only specific cells
        ColumnFilter filter = ColumnFilter.selectionBuilder()
                                          .select(v2, cellPath(1))
                                          .select(v2, cellPath(3))
                                          .build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNull(filtered.getCell(v2, cellPath(0)));
        assertNotNull(filtered.getCell(v2, cellPath(1)));
        assertNull(filtered.getCell(v2, cellPath(2)));
        assertNotNull(filtered.getCell(v2, cellPath(3)));
        assertEquals(201, ByteBufferUtil.toInt(filtered.getCell(v2, cellPath(1)).buffer()));
        assertEquals(203, ByteBufferUtil.toInt(filtered.getCell(v2, cellPath(3)).buffer()));
        assertEquals(COMPLEX_DELETION, filtered.getComplexColumnData(v2).complexDeletion());
    }

    @Test
    public void testFilterComplexColumnWithSlice()
    {
        // Create a row with multiple cells in complex column
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addComplexDeletion(v2, COMPLEX_DELETION);
        builder.addCell(complexCell(v2, 0, 200));
        builder.addCell(complexCell(v2, 1, 201));
        builder.addCell(complexCell(v2, 2, 202));
        builder.addCell(complexCell(v2, 3, 203));
        builder.addCell(complexCell(v2, 4, 204));
        Row row = builder.build();

        // Filter with slice [1:3]
        ColumnFilter filter = ColumnFilter.selectionBuilder()
                                          .slice(v2, cellPath(1), cellPath(3))
                                          .build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNull(filtered.getCell(v2, cellPath(0)));
        assertNotNull(filtered.getCell(v2, cellPath(1)));
        assertNotNull(filtered.getCell(v2, cellPath(2)));
        assertNotNull(filtered.getCell(v2, cellPath(3)));
        assertNull(filtered.getCell(v2, cellPath(4)));
        assertEquals(COMPLEX_DELETION, filtered.getComplexColumnData(v2).complexDeletion());
    }

    @Test
    public void testFilterStaticSimpleColumn()
    {
        // Create a static row
        Row.Builder builder = newBuilder(Clustering.STATIC_CLUSTERING);
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addCell(simpleCell(s1, 100));
        Row row = builder.build();

        // Filter to select static column
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(s1).build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNotNull(filtered.getCell(s1));
        assertEquals(100, ByteBufferUtil.toInt(filtered.getCell(s1).buffer()));
    }

    @Test
    public void testFilterStaticComplexColumn()
    {
        // Create a static row with complex column
        Row.Builder builder = newBuilder(Clustering.STATIC_CLUSTERING);
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addComplexDeletion(s2, COMPLEX_DELETION);
        builder.addCell(complexCell(s2, 0, 200));
        builder.addCell(complexCell(s2, 1, 201));
        Row row = builder.build();

        // Filter to select specific cells from static complex column
        ColumnFilter filter = ColumnFilter.selectionBuilder()
                                          .select(s2, cellPath(1))
                                          .build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNull(filtered.getCell(s2, cellPath(0)));
        assertNotNull(filtered.getCell(s2, cellPath(1)));
        assertEquals(201, ByteBufferUtil.toInt(filtered.getCell(s2, cellPath(1)).buffer()));
    }

    @Test
    public void testFilterWithMetadataFetchesAllRegulars()
    {
        // Create a row with multiple columns
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(simpleCell(v1, 100));
        builder.addComplexDeletion(v2, COMPLEX_DELETION);
        builder.addCell(complexCell(v2, 1, 200));
        Row row = builder.build();

        // Use allRegularColumnsBuilder which fetches all regulars but only queries v1
        ColumnFilter filter = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                          .add(v1)
                                          .build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        Cell<?> queriedCell = filtered.getCell(v1);
        assertNotNull(queriedCell);
        assertTrue(queriedCell.buffer().remaining() > 0);
        assertEquals(100, ByteBufferUtil.toInt(queriedCell.buffer()));
        Cell<?> fetchedCell = filtered.getCell(v2, cellPath(1));
        assertNotNull(fetchedCell);
        assertTrue(fetchedCell.buffer().remaining() == 0);
        assertEquals(COMPLEX_DELETION, filtered.getComplexColumnData(v2).complexDeletion());
        // The cell should be present since it's fetched
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(v2));
        assertTrue(filter.fetchedColumnIsQueried(v1));


        // Use allRegularColumnsBuilder which fetches all regulars but only queries v2
        filter = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                          .add(v2)
                                          .build();
        filtered = row.filter(filter, metadata);

        queriedCell = filtered.getCell(v2, cellPath(1));
        assertNotNull(queriedCell);
        assertTrue(queriedCell.buffer().remaining() > 0);
        assertEquals(200, ByteBufferUtil.toInt(queriedCell.buffer()));
        assertEquals(COMPLEX_DELETION, filtered.getComplexColumnData(v2).complexDeletion());
        fetchedCell = filtered.getCell(v1);
        assertNotNull(fetchedCell);
        assertTrue(fetchedCell.buffer().remaining() == 0);
        // The cell should be present since it's fetched
        assertTrue(filter.fetches(v1));
        assertTrue(filter.fetches(v2));
        assertTrue(filter.fetchedColumnIsQueried(v2));
    }

    @Test
    public void testFilterWithRowDeletion()
    {
        // Create a row with cells and a row deletion
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(TIMESTAMP + 2, TTL, LOCAL_DELETION_TIME));
        builder.addCell(simpleCell(v1, 100));
        Row row = builder.build();

        // Apply filter with an active deletion that shadows the cell
        DeletionTime activeDeletion = DeletionTime.build(TIMESTAMP + 1, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, false, metadata);

        // The cell should be removed because it's shadowed by the deletion
        assertNull(filtered.getCell(v1));
        // But the row itself should still exist if it has liveness info
        assertNotNull(filtered);
    }

    @Test
    public void testFilterRemovesShadowedCells()
    {
        long cellTimestamp = 1000L;
        long deletionTimestamp = 2000L;

        // Create a row with a cell
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, cellTimestamp));
        Row row = builder.build();

        // Apply filter with deletion that shadows the cell
        DeletionTime activeDeletion = DeletionTime.build(deletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, false, metadata);

        // Row becomes empty when all cells are shadowed
        assertNull(filtered);

    }

    @Test
    public void testFilterKeepsNonShadowedCells()
    {
        long cellTimestamp = 2000L;
        long deletionTimestamp = 1000L;

        // Create a row with a cell
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, cellTimestamp));
        Row row = builder.build();

        // Apply filter with deletion that doesn't shadow the cell
        DeletionTime activeDeletion = DeletionTime.build(deletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, false, metadata);

        // Cell should still be present
        assertNotNull(filtered.getCell(v1));
        assertEquals(100, ByteBufferUtil.toInt(filtered.getCell(v1).buffer()));
    }

    @Test
    public void testFilterComplexColumnWithComplexDeletion()
    {
        long cellTimestamp = 1000L;
        long deletionTimestamp = 2000L;

        // Create a row with complex column cells and a complex deletion
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v2, cellPath(0), 200, cellTimestamp));
        builder.addCell(cell(v2, cellPath(1), 201, cellTimestamp));
        builder.addComplexDeletion(v2, DeletionTime.build(deletionTimestamp, LOCAL_DELETION_TIME));
        Row row = builder.build();

        // Filter should remove cells shadowed by complex deletion
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, metadata);

        // Cells are removed by the complex deletion
        assertNotNull(filtered);
        assertNull(filtered.getCell(v2, cellPath(0)));
        assertNull(filtered.getCell(v2, cellPath(1)));
    }

    @Test
    public void testFilterWithDroppedColumn()
    {
        long cellTimestamp = 1000L;
        long droppedTime = 2000L;

        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, cellTimestamp));
        builder.addCell(cell(v2, cellPath(0), 200, cellTimestamp));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v1, droppedTime)
                                                    .build();

        // Filter should remove cells for dropped columns
        ColumnFilter filter = ColumnFilter.all(metadataWithDropped);
        Row filtered = row.filter(filter, metadataWithDropped);

        // v1 cell should be removed because it's dropped and timestamp <= droppedTime
        assertNotNull(filtered);
        assertNull(filtered.getCell(v1));
        assertNotNull(filtered.getCell(v2, cellPath(0)));
    }

    @Test
    public void testFilterWithRecreatedColumn()
    {
        long cellTimestamp = 3000L; // after drop
        long droppedTime = 2000L;

        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, cellTimestamp));
        builder.addCell(cell(v2, cellPath(0), 200, cellTimestamp));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v1, droppedTime)
                                                    .build();

        // Filter should remove cells for dropped columns
        ColumnFilter filter = ColumnFilter.all(metadataWithDropped);
        Row filtered = row.filter(filter, metadataWithDropped);

        // v1 cell shouldn't be removed because it's dropped and timestamp > droppedTime
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertNotNull(filtered.getCell(v2, cellPath(0)));
    }

    @Test
    public void testFilterWithDroppedQueriedColumn()
    {
        long cellTimestamp = 1000L;
        long droppedTime = 2000L;

        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, cellTimestamp));
        builder.addCell(cell(v2, cellPath(0), 200, cellTimestamp));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v1, droppedTime)
                                                    .build();

        // Filter should remove cells for dropped columns
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(v1).build();
        Row filtered = row.filter(filter, metadataWithDropped);

        // v1 cell should be removed because it's dropped and timestamp <= droppedTime
        assertNull(filtered);
    }

    @Test
    public void testFilterWithRecreatedQueriedColumn()
    {
        long cellTimestamp = 3000L; // after drop
        long droppedTime = 2000L;

        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, cellTimestamp));
        builder.addCell(cell(v2, cellPath(0), 200, cellTimestamp));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v1, droppedTime)
                                                    .build();

        // Filter should remove cells for dropped columns
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(v1).build();
        Row filtered = row.filter(filter, metadataWithDropped);

        // v1 cell shouldn't be removed because it's dropped and timestamp > droppedTime
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertNull(filtered.getCell(v2, cellPath(0)));
    }

    @Test
    public void testFilterWithDroppedComplexColumn()
    {
        long droppedTime = 2000L;

        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, 1000));
        builder.addCell(cell(v2, cellPath(0), 200, 1000));
        builder.addCell(cell(v2, cellPath(1), 300, 2000));
        builder.addCell(cell(v2, cellPath(2), 400, 3000));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v2, droppedTime)
                                                    .build();

        // Filter should remove cells for dropped columns
        ColumnFilter filter = ColumnFilter.all(metadataWithDropped);
        Row filtered = row.filter(filter, metadataWithDropped);

        // some of the v2 cells should be dropped
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertNotNull(filtered.getCell(v2, cellPath(2)));
        assertNull(filtered.getCell(v2, cellPath(1)));
        assertNull(filtered.getCell(v2, cellPath(0)));
    }

    @Test
    public void testFilterWithDroppedQueriedComplexColumn()
    {
        long droppedTime = 2000L;

        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, 1000));
        builder.addCell(cell(v2, cellPath(0), 200, 1000));
        builder.addCell(cell(v2, cellPath(1), 300, 2000));
        builder.addCell(cell(v2, cellPath(2), 400, 3000));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v2, droppedTime)
                                                    .build();

        // Filter should remove cells for dropped columns
        ColumnFilter filter = ColumnFilter.selectionBuilder().slice(v2, cellPath(0), cellPath(1)).build();
        Row filtered = row.filter(filter, metadataWithDropped);

        // v1 cell should be removed because it's dropped and timestamp <= droppedTime
        assertNull(filtered);
    }

    @Test
    public void testFilterWithRecreatedQueriedComplexColumn()
    {
        long droppedTime = 2000L;

        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, 1000));
        builder.addCell(cell(v2, cellPath(0), 200, 1000));
        builder.addCell(cell(v2, cellPath(1), 300, 2000));
        builder.addCell(cell(v2, cellPath(2), 400, 3000));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v2, droppedTime)
                                                    .build();

        // Filter should remove cells for dropped columns
        ColumnFilter filter = ColumnFilter.selectionBuilder().slice(v2, cellPath(1), cellPath(2)).build();
        Row filtered = row.filter(filter, metadataWithDropped);

        // v1 cell shouldn't be removed because it's dropped and timestamp > droppedTime
        assertNotNull(filtered);
        assertNull(filtered.getCell(v1));
        assertNotNull(filtered.getCell(v2, cellPath(2)));
        assertNull(filtered.getCell(v2, cellPath(1)));
        assertNull(filtered.getCell(v2, cellPath(0)));
    }

    @Test
    public void testFilterWithDroppedColumnNewerTimestamp()
    {
        long droppedTime = 1000L;
        long cellTimestamp = 2000L;

        // Create a row with a cell that has timestamp > droppedTime
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, cellTimestamp));
        Row row = builder.build();

        // Create metadata with v1 as a dropped column
        TableMetadata metadataWithDropped = metadata.unbuild()
                                                    .recordColumnDrop(v1, droppedTime)
                                                    .build();

        // Filter should keep the cell because timestamp > droppedTime
        ColumnFilter filter = ColumnFilter.all(metadataWithDropped);
        Row filtered = row.filter(filter, metadataWithDropped);

        // v1 cell should be kept because timestamp > droppedTime
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertEquals(100, ByteBufferUtil.toInt(filtered.getCell(v1).buffer()));
    }

    @Test
    public void testFilterWithRowDeletion2()
    {
        long cellTimestamp = 1000L;
        long rowDeletionTimestamp = 2000L;

        // Create a row with a row deletion and cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(rowDeletionTimestamp, LOCAL_DELETION_TIME)));
        builder.addCell(cell(v1, 100, cellTimestamp));
        builder.addCell(cell(v2, cellPath(0), 200, cellTimestamp));
        Row row = builder.build();

        // Filter should remove cells shadowed by row deletion
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, metadata);

        // Cells should be removed by row deletion
        assertNotNull(filtered);
        assertNull(filtered.getCell(v1));
        assertNull(filtered.getCell(v2, cellPath(0)));
        // Row deletion should be preserved
        assertEquals(rowDeletionTimestamp, filtered.deletion().time().markedForDeleteAt());
    }

    @Test
    public void testFilterWithRowDeletionAndNewerCells()
    {
        long rowDeletionTimestamp = 1000L;
        long cellTimestamp = 2000L;

        // Create a row with a row deletion and newer cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(rowDeletionTimestamp, LOCAL_DELETION_TIME)));
        builder.addCell(cell(v1, 100, cellTimestamp));
        Row row = builder.build();

        // Filter should keep cells that are newer than row deletion
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, metadata);

        // Cell should be kept because it's newer than row deletion
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertEquals(100, ByteBufferUtil.toInt(filtered.getCell(v1).buffer()));
    }

    @Test
    public void testFilterWithActiveDeletionSupersedingAllData()
    {
        long rowDeletionTimestamp = 1000L;
        long activeDeletionTimestamp = 2000L;
        long cellTimestamp = 1500L;

        // Create a row with a row deletion and cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(cellTimestamp, TTL, LOCAL_DELETION_TIME));
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(rowDeletionTimestamp, LOCAL_DELETION_TIME)));
        builder.addCell(cell(v1, 100, cellTimestamp));
        Row row = builder.build();

        // Apply filter with active deletion that supersedes row deletion
        DeletionTime activeDeletion = DeletionTime.build(activeDeletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, false, metadata);

        // Cell, deletion and liveness should be removed by active deletion
        assertNull(filtered);
    }

    @Test
    public void testFilterWithActiveDeletionSupersedingRowDeletion()
    {
        long rowDeletionTimestamp = 1000L;
        long activeDeletionTimestamp = 1200L;
        long cellTimestamp = 1500L;

        // Create a row with a row deletion and cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(cellTimestamp, TTL, LOCAL_DELETION_TIME));
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(rowDeletionTimestamp, LOCAL_DELETION_TIME)));
        builder.addCell(cell(v1, 100, cellTimestamp));
        Row row = builder.build();

        // Apply filter with active deletion that supersedes row deletion
        DeletionTime activeDeletion = DeletionTime.build(activeDeletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, false, metadata);

        // Cell should be removed by active deletion
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        // Row deletion should be removed because it's superseded by active deletion
        assertTrue(filtered.deletion().isLive());
    }

    @Test
    public void testFilterWithActiveDeletionSupersedingNothing()
    {
        long rowDeletionTimestamp = 1000L;
        long activeDeletionTimestamp = 900L;
        long cellTimestamp = 1500L;

        // Create a row with a row deletion and cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(cellTimestamp, TTL, LOCAL_DELETION_TIME));
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(rowDeletionTimestamp, LOCAL_DELETION_TIME)));
        builder.addCell(cell(v1, 100, cellTimestamp));
        Row row = builder.build();

        // Apply filter with active deletion that supersedes row deletion
        DeletionTime activeDeletion = DeletionTime.build(activeDeletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, false, metadata);

        // Cell should not be removed by active deletion
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        // Row deletion should not be removed because it's not superseded by active deletion
        assertEquals(rowDeletionTimestamp, filtered.deletion().time().markedForDeleteAt());
    }

    @Test
    public void testFilterWithActiveDeletionSetToRow()
    {
        long rowDeletionTimestamp = 1000L;
        long activeDeletionTimestamp = 2000L;

        // Create a row with a row deletion
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(rowDeletionTimestamp, LOCAL_DELETION_TIME)));
        builder.addCell(cell(v1, 100, 1500L));
        Row row = builder.build();

        // Apply filter with active deletion and setActiveDeletionToRow=true
        DeletionTime activeDeletion = DeletionTime.build(activeDeletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, true, metadata);

        // Row deletion should be set to active deletion
        assertNotNull(filtered);
        assertEquals(activeDeletionTimestamp, filtered.deletion().time().markedForDeleteAt());
    }

    @Test
    public void testFilterWithActiveDeletionSetToRowAndNoExistingRowDeletion()
    {
        long activeDeletionTimestamp = 2000L;

        // Create a row with a row deletion
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, 1500L));
        Row row = builder.build();

        // Apply filter with active deletion and setActiveDeletionToRow=true
        DeletionTime activeDeletion = DeletionTime.build(activeDeletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.all(metadata);
        Row filtered = row.filter(filter, activeDeletion, true, metadata);

        // Row deletion should be set to active deletion
        assertNotNull(filtered);
        assertEquals(activeDeletionTimestamp, filtered.deletion().time().markedForDeleteAt());
    }

    @Test
    public void testFilterWithActiveDeletionSetToRowOnSelection()
    {
        long activeDeletionTimestamp = 2000L;

        // Create a row with a row deletion
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(1000, LOCAL_DELETION_TIME)));
        builder.addCell(cell(v1, 100, 2500L));
        Row row = builder.build();

        // Apply filter with active deletion and setActiveDeletionToRow=true
        DeletionTime activeDeletion = DeletionTime.build(activeDeletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(v1).build();
        Row filtered = row.filter(filter, activeDeletion, true, metadata);

        // Row deletion should be set to active deletion
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertEquals(activeDeletionTimestamp, filtered.deletion().time().markedForDeleteAt());
    }

    @Test
    public void testFilterWithActiveDeletionSetToRowOnEmpty()
    {
        long activeDeletionTimestamp = 2000L;

        // Create a row with a row deletion
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(cell(v1, 100, 2500L));
        Row row = builder.build();

        // Apply filter with active deletion and setActiveDeletionToRow=true
        DeletionTime activeDeletion = DeletionTime.build(activeDeletionTimestamp, LOCAL_DELETION_TIME);
        ColumnFilter filter = ColumnFilter.selectionBuilder().add(v2).build();
        Row filtered = row.filter(filter, activeDeletion, true, metadata);

        // Row deletion should be set to active deletion
        assertNotNull(filtered);
        assertNull(filtered.getCell(v1));
        assertEquals(activeDeletionTimestamp, filtered.deletion().time().markedForDeleteAt());
    }

    @Test
    public void testFilterMixedColumnsAndCells()
    {
        // Create a row with various column types
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(simpleCell(v1, 100));
        builder.addComplexDeletion(v2, COMPLEX_DELETION);
        builder.addCell(complexCell(v2, 0, 200));
        builder.addCell(complexCell(v2, 1, 201));
        builder.addCell(complexCell(v2, 2, 202));
        Row row = builder.build();

        // Filter with mixed selections
        ColumnFilter filter = ColumnFilter.selectionBuilder()
                                          .add(v1)
                                          .select(v2, cellPath(1))
                                          .build();
        Row filtered = row.filter(filter, metadata);

        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertNull(filtered.getCell(v2, cellPath(0)));
        assertNotNull(filtered.getCell(v2, cellPath(1)));
        assertNull(filtered.getCell(v2, cellPath(2)));
        assertEquals(COMPLEX_DELETION, filtered.getComplexColumnData(v2).complexDeletion());
    }

    @Test
    public void testWithOnlyQueriedData()
    {
        // Create a row with cells
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(simpleCell(v1, 100));
        builder.addComplexDeletion(v2, COMPLEX_DELETION);
        builder.addCell(complexCell(v2, 0, 200));
        builder.addCell(complexCell(v2, 1, 201));
        builder.addCell(complexCell(v2, 2, 202));
        Row row = builder.build();

        // Use a filter that fetches all but only queries v1
        ColumnFilter filter = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                          .add(v1)
                                          .build();
        
        // First apply the filter
        Row filtered = row.filter(filter, metadata);
        assertNotNull(filtered);
        assertNotNull(filtered.getCell(v1));
        assertNotNull(filtered.getComplexColumnData(v2));
        assertNotNull(filtered.getCell(v2, cellPath(0)));
        assertEquals(0, filtered.getCell(v2, cellPath(0)).buffer().remaining());

        // Then apply withOnlyQueriedData to remove fetched-but-not-queried columns
        Row queriedOnly = filtered.withOnlyQueriedData(filter);

        assertNotNull(queriedOnly);
        assertNotNull(queriedOnly.getCell(v1));
        assertNull(queriedOnly.getComplexColumnData(v2));
        assertNull(queriedOnly.getCell(v2, cellPath(0)));
    }

    @Test
    public void testWithOnlyQueriedDataSubselection()
    {
        Row.Builder builder = newBuilder(clustering);
        builder.newRow(clustering);
        builder.addCell(simpleCell(v1, 100));
        builder.addCell(complexCell(v2, 0, 200));
        builder.addCell(complexCell(v2, 1, 201));
        builder.addCell(complexCell(v2, 2, 202));
        Row row = builder.build();

        // Use a filter that fetches all but only queries one element of v2
        ColumnFilter filter = ColumnFilter.allRegularColumnsBuilder(metadata, false)
                                          .select(v2, cellPath(1))
                                          .build();

        Row filtered = row.filter(filter, metadata);
        assertNotNull(filtered);

        // withOnlyQueriedData should only leave the queried element
        Row queriedOnly = filtered.withOnlyQueriedData(filter);

        assertNotNull(queriedOnly);
        assertNull(queriedOnly.getCell(v1));
        assertNull(queriedOnly.getCell(v2, cellPath(0)));
        assertNotNull(queriedOnly.getCell(v2, cellPath(1)));
        assertEquals(201, ByteBufferUtil.toInt(queriedOnly.getCell(v2, cellPath(1)).buffer()));
        assertNull(queriedOnly.getCell(v2, cellPath(2)));
    }

    // Helper methods

    private Cell<?> simpleCell(ColumnMetadata column, int value)
    {
        return cell(column, value, TIMESTAMP);
    }

    private Cell<?> cell(ColumnMetadata column, int value, long timestamp)
    {
        return BufferCell.live(column, timestamp, ByteBufferUtil.bytes(value));
    }

    private Cell<?> complexCell(ColumnMetadata column, int pathValue, int cellValue)
    {
        return cell(column, cellPath(pathValue), cellValue, TIMESTAMP);
    }

    private Cell<?> cell(ColumnMetadata column, CellPath path, int value, long timestamp)
    {
        return BufferCell.live(column, timestamp, ByteBufferUtil.bytes(value), path);
    }

    private CellPath cellPath(int value)
    {
        return CellPath.create(ByteBufferUtil.bytes(value));
    }
}
