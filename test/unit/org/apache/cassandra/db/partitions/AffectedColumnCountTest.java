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

package org.apache.cassandra.db.partitions;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/// Tests for [PartitionUpdate#affectedColumnCount] and [PartitionUpdate#affectedRowCount].
@RunWith(Parameterized.class)
public class AffectedColumnCountTest
{
    @Parameterized.Parameter(0)
    public PartitionUpdate.Factory partitionUpdateFactory;

    @Parameterized.Parameters(name = "type {0}")
    public static List<PartitionUpdate.Factory> factories()
    {
        return Arrays.asList(TriePartitionUpdate.FACTORY,
                             BTreePartitionUpdate.FACTORY);
    }

    private static TableMetadata metadata;
    private static DecoratedKey partitionKey;
    private static ColumnMetadata r1;
    private static ColumnMetadata r2;
    private static ColumnMetadata r3;
    private static ColumnMetadata s1;
    private static ColumnMetadata s2;
    private static ColumnMetadata c1;
    private static ColumnMetadata c2;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("test_ks", "test_table")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addRegularColumn("r1", Int32Type.instance)
                                .addRegularColumn("r2", Int32Type.instance)
                                .addRegularColumn("r3", Int32Type.instance)
                                .addStaticColumn("s1", Int32Type.instance)
                                .addStaticColumn("s2", Int32Type.instance)
                                .addRegularColumn("c1", SetType.getInstance(Int32Type.instance, true))
                                .addRegularColumn("c2", SetType.getInstance(Int32Type.instance, true))
                                .build();
        
        partitionKey = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
        r1 = metadata.getColumn(new ColumnIdentifier("r1", false));
        r2 = metadata.getColumn(new ColumnIdentifier("r2", false));
        r3 = metadata.getColumn(new ColumnIdentifier("r3", false));
        s1 = metadata.getColumn(new ColumnIdentifier("s1", false));
        s2 = metadata.getColumn(new ColumnIdentifier("s2", false));
        c1 = metadata.getColumn(new ColumnIdentifier("c1", false));
        c2 = metadata.getColumn(new ColumnIdentifier("c2", false));
    }

    @Test
    public void testAffectedColumnCountWithPartitionDeletion()
    {
        PartitionUpdate.Builder builder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        builder.addPartitionDeletion(DeletionTime.build(1000, FBUtilities.nowInSeconds()));
        PartitionUpdate update = builder.build();

        // With partition deletion, should count all regular and static columns
        int expected = metadata.regularAndStaticColumns().size();
        assertEquals(expected, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithSingleSimpleCell()
    {
        PartitionUpdate.Builder builder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);

        Row.Builder rowBuilder = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        rowBuilder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
        rowBuilder.addCell(BufferCell.live(r1, 1000, ByteBufferUtil.bytes(100)));

        builder.add(rowBuilder.build());
        PartitionUpdate update = builder.build();

        // Should count 1 cell
        assertEquals(1, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithMultipleSimpleCells()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        Row.Builder builder = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
        builder.addCell(BufferCell.live(r1, 1000, ByteBufferUtil.bytes(100)));
        builder.addCell(BufferCell.live(r2, 1000, ByteBufferUtil.bytes(200)));
        builder.addCell(BufferCell.live(r3, 1000, ByteBufferUtil.bytes(300)));
        
        updateBuilder.add(builder.build());
        PartitionUpdate update = updateBuilder.build();

        // Should count 3 cells
        assertEquals(3, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithStaticCells()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        Row.Builder builder = partitionUpdateFactory.rowBuilder(metadata.staticColumns(), false);
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addCell(BufferCell.live(s1, 1000, ByteBufferUtil.bytes(100)));
        builder.addCell(BufferCell.live(s2, 1000, ByteBufferUtil.bytes(200)));
        
        updateBuilder.add(builder.build());
        PartitionUpdate update = updateBuilder.build();

        // Should count 2 static cells
        assertEquals(2, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithComplexColumn()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        Row.Builder builder = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
        
        // Add cells to a complex column (set)
        builder.addCell(BufferCell.live(c1, 1000, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(10))));
        builder.addCell(BufferCell.live(c1, 1000, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(20))));
        builder.addCell(BufferCell.live(c1, 1000, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(30))));
        
        updateBuilder.add(builder.build());
        PartitionUpdate update = updateBuilder.build();

        // Should count 1 column (complex columns count as 1, not individual cells)
        assertEquals(1, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithMixedColumns()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        // Add static row
        Row.Builder staticBuilder = partitionUpdateFactory.rowBuilder(metadata.staticColumns(), false);
        staticBuilder.newRow(Clustering.STATIC_CLUSTERING);
        staticBuilder.addCell(BufferCell.live(s1, 1010, ByteBufferUtil.bytes(100)));
        updateBuilder.add(staticBuilder.build());
        
        // Add regular row with simple and complex columns
        Row.Builder regularBuilder = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        regularBuilder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
        regularBuilder.addCell(BufferCell.live(r1, 1000, ByteBufferUtil.bytes(100)));
        regularBuilder.addCell(BufferCell.live(r2, 1001, ByteBufferUtil.bytes(200)));
        regularBuilder.addCell(BufferCell.live(c1, 1002, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(10))));
        regularBuilder.addCell(BufferCell.live(c1, 1002, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(20))));
        updateBuilder.add(regularBuilder.build());
        
        PartitionUpdate update = updateBuilder.build();

        // Should count: 1 static + 2 simple regular + 1 complex column = 4 columns (not cells)
        assertEquals(4, update.affectedColumnCount());
        assertEquals(2, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithRangeTombstone()
    {
        PartitionUpdate.Builder builder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        // Add a range tombstone
        builder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(1)),
                                                                           Clustering.make(ByteBufferUtil.bytes(10))),
                                       DeletionTime.build(1000, FBUtilities.nowInSeconds())));
        PartitionUpdate update = builder.build();

        // Each range tombstone creates 2 markers (open/close), each counting all regular columns
        int expected = 1 * metadata.regularColumns().size();
        assertEquals(expected, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithMultipleRangeTombstones()
    {
        PartitionUpdate.Builder builder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        // Add multiple range tombstones
        builder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(1)),
                                                                           Clustering.make(ByteBufferUtil.bytes(5))),
                                       DeletionTime.build(1000, FBUtilities.nowInSeconds())));
        builder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(10)),
                                                                           Clustering.make(ByteBufferUtil.bytes(15))),
                                       DeletionTime.build(1001, FBUtilities.nowInSeconds())));
        PartitionUpdate update = builder.build();

        // Each range tombstone creates 2 markers (open/close), each counting all regular columns
        int expected = 2 * metadata.regularColumns().size();
        assertEquals(expected, update.affectedColumnCount());
        assertEquals(2, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithOverridingRangeTombstone()
    {
        PartitionUpdate.Builder builder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);

        // Add multiple range tombstones
        builder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(3)),
                                                                          Clustering.make(ByteBufferUtil.bytes(5))),
                                       DeletionTime.build(1000, FBUtilities.nowInSeconds())));
        builder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(1)),
                                                                          Clustering.make(ByteBufferUtil.bytes(7))),
                                       DeletionTime.build(1001, FBUtilities.nowInSeconds())));
        PartitionUpdate update = builder.build();

        // Each range tombstone creates 2 markers (open/close), each counting all regular columns
        int expected = 1 * metadata.regularColumns().size();
        assertEquals(expected, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithOverlappingRangeTombstone()
    {
        PartitionUpdate.Builder builder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);

        // Add multiple range tombstones
        builder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(1)),
                                                                          Clustering.make(ByteBufferUtil.bytes(5))),
                                       DeletionTime.build(1000, FBUtilities.nowInSeconds())));
        builder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(3)),
                                                                          Clustering.make(ByteBufferUtil.bytes(7))),
                                       DeletionTime.build(1001, FBUtilities.nowInSeconds())));
        PartitionUpdate update = builder.build();

        // Each range tombstone creates 2 markers (open/close), each counting all regular columns
        int expected = 2 * metadata.regularColumns().size();
        assertEquals(expected, update.affectedColumnCount());
        assertEquals(2, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithRangeTombstoneAndCells()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        // Add a range tombstone
        updateBuilder.add(new RangeTombstone(org.apache.cassandra.db.Slice.make(Clustering.make(ByteBufferUtil.bytes(1)),
                                                                                 Clustering.make(ByteBufferUtil.bytes(10))),
                                             DeletionTime.build(1000, FBUtilities.nowInSeconds())));
        
        // Add a row with cells
        Row.Builder builder = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(20)));
        builder.addCell(BufferCell.live(r1, 1000, ByteBufferUtil.bytes(100)));
        builder.addCell(BufferCell.live(r2, 1000, ByteBufferUtil.bytes(200)));
        updateBuilder.add(builder.build());
        
        PartitionUpdate update = updateBuilder.build();

        // Should count: range tombstone creates 2 markers (open/close), each with 5 regular columns = 10, plus 2 cells = 12
        int expected = 1 * metadata.regularColumns().size() + 2;
        assertEquals(expected, update.affectedColumnCount());
        assertEquals(2, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithMultipleRows()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);
        
        // Add first row
        Row.Builder builder1 = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        builder1.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
        builder1.addCell(BufferCell.live(r1, 1000, ByteBufferUtil.bytes(100)));
        updateBuilder.add(builder1.build());
        
        // Add second row
        Row.Builder builder2 = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        builder2.newRow(Clustering.make(ByteBufferUtil.bytes(2)));
        builder2.addCell(BufferCell.live(r2, 1000, ByteBufferUtil.bytes(200)));
        builder2.addCell(BufferCell.live(r3, 1000, ByteBufferUtil.bytes(300)));
        updateBuilder.add(builder2.build());
        
        PartitionUpdate update = updateBuilder.build();

        // Should count: 1 + 2 = 3 cells
        assertEquals(3, update.affectedColumnCount());
        assertEquals(2, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountEmpty()
    {
        TriePartitionUpdate update = TriePartitionUpdate.emptyUpdate(metadata, partitionKey);

        // Empty update should have 0 affected columns
        assertEquals(0, update.affectedColumnCount());
        assertEquals(0, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithComplexColumnDeletion()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);

        Row.Builder builder = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));

        // Add a complex column deletion (marker)
        DeletionTime deletion = DeletionTime.build(1000, FBUtilities.nowInSeconds());
        builder.addComplexDeletion(c1, deletion);

        updateBuilder.add(builder.build());
        PartitionUpdate update = updateBuilder.build();

        // Complex column deletion counts as 1 column
        assertEquals(1, update.affectedColumnCount());
        /// Trie-backed partitions only count rows with live data. See [TrieBackedPartition#rowCount].
        if (partitionUpdateFactory != TriePartitionUpdate.FACTORY)
            assertEquals(1, update.affectedRowCount());
    }

    @Test
    public void testAffectedColumnCountWithComplexColumnDeletionAndLiveData()
    {
        PartitionUpdate.Builder updateBuilder = partitionUpdateFactory.builder(metadata, partitionKey, metadata.regularAndStaticColumns(), 16);

        Row.Builder builder = partitionUpdateFactory.rowBuilder(metadata.regularColumns(), false);
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(1)));

        // Add a complex column deletion (marker) at timestamp 1000
        DeletionTime deletion = DeletionTime.build(1000, FBUtilities.nowInSeconds());
        builder.addComplexDeletion(c1, deletion);

        // Add newer live cells to the same complex column at timestamp 2000
        builder.addCell(BufferCell.live(c1, 2000, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(10))));
        builder.addCell(BufferCell.live(c1, 2000, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(20))));

        updateBuilder.add(builder.build());
        PartitionUpdate update = updateBuilder.build();

        // Should count the complex column as 1 (deletion marker + cells count as the same column)
        assertEquals(1, update.affectedColumnCount());
        assertEquals(1, update.affectedRowCount());
    }
}
