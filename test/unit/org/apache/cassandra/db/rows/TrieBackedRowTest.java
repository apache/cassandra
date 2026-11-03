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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.HeapCloner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TrieBackedRowTest
{
    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testToBTreeRow()
    {
        // The complex column is declared first, so that its id is lower than the ids of the simple columns, which the
        // rows present before it.
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addClusteringColumn("ck", Int32Type.instance)
                                           .addStaticColumn("s", Int32Type.instance)
                                           .addRegularColumn("m", MapType.getInstance(Int32Type.instance, UTF8Type.instance, true))
                                           .addRegularColumn("a", Int32Type.instance)
                                           .addRegularColumn("b", UTF8Type.instance)
                                           .build();
        ColumnMetadata s = table.getColumn(ByteBufferUtil.bytes("s"));
        ColumnMetadata a = table.getColumn(ByteBufferUtil.bytes("a"));
        ColumnMetadata b = table.getColumn(ByteBufferUtil.bytes("b"));
        ColumnMetadata m = table.getColumn(ByteBufferUtil.bytes("m"));
        assertTrue(m.uniqueId < a.uniqueId);
        long now = FBUtilities.nowInSeconds();

        List<Row> rows = new ArrayList<>();

        // Live row with simple and complex cells.
        Row.Builder builder = TrieBackedRow.builder(table.regularColumns());
        builder.newRow(Util.clustering(table.comparator, 1));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(100));
        builder.addCell(BufferCell.live(a, 100, ByteBufferUtil.bytes(1)));
        builder.addCell(BufferCell.live(b, 101, ByteBufferUtil.bytes("one")));
        builder.addCell(BufferCell.live(m, 102, ByteBufferUtil.bytes("x"), CellPath.create(ByteBufferUtil.bytes(1))));
        builder.addCell(BufferCell.live(m, 102, ByteBufferUtil.bytes("y"), CellPath.create(ByteBufferUtil.bytes(2))));
        rows.add(builder.build());

        // Row without primary key liveness, with an expiring cell, a cell tombstone and a complex column deletion.
        builder = TrieBackedRow.builder(table.regularColumns());
        builder.newRow(Util.clustering(table.comparator, 2));
        builder.addCell(BufferCell.expiring(a, 200, 1000, now, ByteBufferUtil.bytes(2)));
        builder.addCell(BufferCell.tombstone(b, 201, now));
        builder.addComplexDeletion(m, DeletionTime.build(199, now));
        builder.addCell(BufferCell.live(m, 202, ByteBufferUtil.bytes("z"), CellPath.create(ByteBufferUtil.bytes(3))));
        builder.addCell(BufferCell.tombstone(m, 203, now, CellPath.create(ByteBufferUtil.bytes(4))));
        rows.add(builder.build());

        // Row deletion with newer data, and a deleted row with no data.
        builder = TrieBackedRow.builder(table.regularColumns());
        builder.newRow(Util.clustering(table.comparator, 3));
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(300, now)));
        builder.addCell(BufferCell.live(a, 301, ByteBufferUtil.bytes(3)));
        rows.add(builder.build());
        rows.add(TrieBackedRow.emptyDeletedRow(Util.clustering(table.comparator, 4), DeletionTime.build(400, now)));

        // Complex column deleted with no remaining cells.
        builder = TrieBackedRow.builder(table.regularColumns());
        builder.newRow(Util.clustering(table.comparator, 5));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(500));
        builder.addComplexDeletion(m, DeletionTime.build(501, now));
        rows.add(builder.build());

        // Row deletion with a newer complex column deletion and newer cells.
        builder = TrieBackedRow.builder(table.regularColumns());
        builder.newRow(Util.clustering(table.comparator, 6));
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(600, now)));
        builder.addCell(BufferCell.live(a, 601, ByteBufferUtil.bytes(6)));
        builder.addComplexDeletion(m, DeletionTime.build(602, now));
        builder.addCell(BufferCell.live(m, 603, ByteBufferUtil.bytes("w"), CellPath.create(ByteBufferUtil.bytes(6))));
        rows.add(builder.build());

        // Static row.
        builder = TrieBackedRow.builder(table.staticColumns());
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addCell(BufferCell.live(s, 600, ByteBufferUtil.bytes(6)));
        rows.add(builder.build());

        // Expired primary key liveness, with a cell.
        builder = TrieBackedRow.builder(table.regularColumns());
        builder.newRow(Util.clustering(table.comparator, 7));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(700, 10, now - 1000));
        builder.addCell(BufferCell.live(a, 701, ByteBufferUtil.bytes(7)));
        rows.add(builder.build());

        assertToBTreeRow(table, rows);

        // Counters.
        TableMetadata counterTable = TableMetadata.builder("ks", "counters")
                                                  .isCounter(true)
                                                  .addPartitionKeyColumn("pk", Int32Type.instance)
                                                  .addClusteringColumn("ck", Int32Type.instance)
                                                  .addRegularColumn("c", CounterColumnType.instance)
                                                  .build();
        ColumnMetadata c = counterTable.getColumn(ByteBufferUtil.bytes("c"));
        builder = TrieBackedRow.builder(counterTable.regularColumns());
        builder.newRow(Util.clustering(counterTable.comparator, 1));
        builder.addCell(BufferCell.live(c, 800, CounterContext.instance().createGlobal(CounterId.fromInt(1), 1, 5)));
        assertToBTreeRow(counterTable, List.of(builder.build()));
    }

    private static void assertToBTreeRow(TableMetadata table, List<Row> rows)
    {
        for (Row row : rows)
        {
            TrieBackedRow trieRow = (TrieBackedRow) row;
            BTreeRow expected = BTreeRow.asBTreeRow(trieRow);
            BTreeRow actual = TrieBackedRow.toBTreeRow(table, trieRow.clustering(), trieRow.trie(), HeapCloner.instance);
            // Note: toString may differ as the trie row's deletion time also prints the kind of the trie marker.
            assertEquals(expected, actual);
            assertFalse(actual.deletion().time() instanceof TrieTombstoneMarker);
            assertEquals(expected.deletion(), actual.deletion());
            assertEquals(expected.primaryKeyLivenessInfo(), actual.primaryKeyLivenessInfo());
            assertEquals(expected.hasComplexDeletion(), actual.hasComplexDeletion());
            assertEquals(expected.dataSize(), actual.dataSize());
            for (Cell<?> cell : actual.cells())
            {
                Cell<?> original = trieRow.getColumnData(cell.column()) instanceof Cell
                                   ? trieRow.getCell(cell.column())
                                   : trieRow.getCell(cell.column(), cell.path());
                assertEquals(original, cell);
                // The test rows hold BufferCells, which must be cloned like the trie cells of a memtable.
                assertTrue(cell.value() != original.value() || cell.valueSize() == 0);
            }
            assertNotSame(expected, actual);
        }
    }

    private final TableMetadata complexMetadata = TableMetadata.builder("", "")
                                                               .addPartitionKeyColumn("pk", Int32Type.instance)
                                                               .addClusteringColumn("ck", Int32Type.instance)
                                                               .addRegularColumn("v", Int32Type.instance)
                                                               .addRegularColumn("s", SetType.getInstance(Int32Type.instance, true))
                                                               .addRegularColumn("m", MapType.getInstance(Int32Type.instance, UTF8Type.instance, true))
                                                               .build();
    private final ColumnMetadata vMetadata = complexMetadata.getColumn(ByteBufferUtil.bytes("v"));
    private final ColumnMetadata sMetadata = complexMetadata.getColumn(ByteBufferUtil.bytes("s"));
    private final ColumnMetadata mMetadata = complexMetadata.getColumn(ByteBufferUtil.bytes("m"));

    /// Updating the timestamps makes deletions with the same local deletion time equal, which removes the boundaries
    /// between them (e.g. between the row deletion and a complex column deletion). This must not leave invalid complex
    /// columns in the updated row.
    @Test
    public void testUpdateAllTimestampWithComplexColumns()
    {
        long nowInSec = FBUtilities.nowInSeconds();
        DeletionTime rowDeletion = DeletionTime.build(100, nowInSec);
        DeletionTime columnDeletion = DeletionTime.build(200, nowInSec);
        DeletionTime laterColumnDeletion = DeletionTime.build(200, nowInSec + 1);

        testUpdateAllTimestamp(false, null, null, null);
        testUpdateAllTimestamp(true, null, columnDeletion, columnDeletion);
        testUpdateAllTimestamp(false, rowDeletion, null, null);
        testUpdateAllTimestamp(false, rowDeletion, columnDeletion, columnDeletion);
        testUpdateAllTimestamp(true, rowDeletion, columnDeletion, laterColumnDeletion);
        testUpdateAllTimestamp(false, rowDeletion, laterColumnDeletion, columnDeletion);
    }

    /// Builds a row whose set column only has a complex deletion, and whose map column has a complex deletion and
    /// cells (null deletions are not added). Checks that updating its timestamps gives the same data as doing that
    /// on the same [BTreeRow].
    private void testUpdateAllTimestamp(boolean withLiveness, DeletionTime rowDeletion, DeletionTime setDeletion, DeletionTime mapDeletion)
    {
        long newTimestamp = 1000;
        Row row = buildRow(TrieBackedRow.builder(complexMetadata.regularColumns()), withLiveness, rowDeletion, setDeletion, mapDeletion);
        Row updated = row.updateAllTimestamp(newTimestamp);
        Row expected = buildRow(BTreeRow.unsortedBuilder(), withLiveness, rowDeletion, setDeletion, mapDeletion).updateAllTimestamp(newTimestamp);
        // Converting to string iterates the complex columns of the row, which used to fail with NullPointerException.
        String message = "Updating " + row + " gave " + updated;

        assertEquals(message, expected.primaryKeyLivenessInfo(), updated.primaryKeyLivenessInfo());
        assertEquals(message, expected.deletion(), updated.deletion());
        assertEquals(message, columnContent(expected), columnContent(updated));

        for (ColumnMetadata column : List.of(sMetadata, mMetadata))
        {
            assertEquals(message, effectiveDeletion(expected, column), effectiveDeletion(updated, column));
            assertEquals(message, cells(expected.getComplexColumnData(column)), cells(updated.getComplexColumnData(column)));

            // test the complex-column version of updateAllTimestamp
            ComplexColumnData ccd = row.getComplexColumnData(column);
            if (ccd == null)
                continue;
            ComplexColumnData ccdUpdated = ccd.updateAllTimestamp(newTimestamp);
            String columnMessage = "Updating column " + ccd + " gave " + ccdUpdated;
            assertEquals(columnMessage, effectiveDeletion(expected, column), effectiveDeletion(updated, ccdUpdated));
            assertEquals(columnMessage, cells(expected.getComplexColumnData(column)), cells(ccdUpdated));
        }

        int columnCount = 0;
        int cellsCount = 0;
        boolean hasComplexDeletion = false;
        for (ColumnData cd : updated)
        {
            ++columnCount;
            if (cd instanceof ComplexColumnData)
            {
                ComplexColumnData ccd = (ComplexColumnData) cd;
                for (Cell<?> cell : ccd)
                    assertEquals(message, newTimestamp, cell.timestamp());
                cellsCount += ccd.cellsCount();
                hasComplexDeletion |= !ccd.complexDeletion().isLive();
            }
            else
            {
                assertEquals(message, newTimestamp, ((Cell<?>) cd).timestamp());
                ++cellsCount;
            }
        }
        assertEquals(message, columnCount, updated.columnCount());
        assertEquals(message, cellsCount, Iterables.size(updated.cells()));
        assertEquals(message, hasComplexDeletion, updated.hasComplexDeletion());
    }

    private Row buildRow(Row.Builder builder, boolean withLiveness, DeletionTime rowDeletion, DeletionTime setDeletion, DeletionTime mapDeletion)
    {
        builder.newRow(Util.clustering(complexMetadata.comparator, 1));
        if (withLiveness)
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(300));
        if (rowDeletion != null)
            builder.addRowDeletion(Row.Deletion.regular(rowDeletion));
        builder.addCell(BufferCell.live(vMetadata, 300, ByteBufferUtil.bytes(1)));
        if (setDeletion != null)
            builder.addComplexDeletion(sMetadata, setDeletion);
        if (mapDeletion != null)
            builder.addComplexDeletion(mMetadata, mapDeletion);
        builder.addCell(BufferCell.live(mMetadata, 300, ByteBufferUtil.bytes("a"), CellPath.create(ByteBufferUtil.bytes(1))));
        builder.addCell(BufferCell.live(mMetadata, 300, ByteBufferUtil.bytes("b"), CellPath.create(ByteBufferUtil.bytes(2))));
        return builder.build();
    }

    /// Lists the content of the columns of the row as presented by its iterator. Ignores complex deletions that do not
    /// supersede the row deletion, which trie-backed rows drop when they become equal to the row deletion.
    private static List<Object> columnContent(Row row)
    {
        List<Object> content = new ArrayList<>();
        DeletionTime rowDeletion = row.deletion().time();
        for (ColumnData cd : row)
        {
            if (cd instanceof ComplexColumnData)
            {
                ComplexColumnData ccd = (ComplexColumnData) cd;
                DeletionTime complexDeletion = ccd.complexDeletion().supersedes(rowDeletion) ? ccd.complexDeletion() : DeletionTime.LIVE;
                if (ccd.cellsCount() > 0 || !complexDeletion.isLive())
                    content.add(List.of(cd.column(), complexDeletion, cells(ccd)));
            }
            else
                content.add(cd);
        }
        return content;
    }

    /// Returns the deletion that applies to the given column of the row.
    private static DeletionTime effectiveDeletion(Row row, ColumnMetadata column)
    {
        ComplexColumnData ccd = row.getComplexColumnData(column);
        return effectiveDeletion(row, ccd);
    }

    private static DeletionTime effectiveDeletion(Row row, ComplexColumnData ccd)
    {
        DeletionTime rowDeletion = row.deletion().time();
        return ccd != null && ccd.complexDeletion().supersedes(rowDeletion) ? ccd.complexDeletion() : rowDeletion;
    }

    private static List<Cell<?>> cells(ComplexColumnData ccd)
    {
        return ccd != null ? Lists.newArrayList(ccd) : List.of();
    }
}
