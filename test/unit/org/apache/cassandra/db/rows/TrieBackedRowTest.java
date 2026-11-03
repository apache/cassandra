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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
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
}
