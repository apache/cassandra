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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Test;

import accord.utils.RandomSource;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static accord.utils.Property.qt;
import static org.junit.Assert.assertEquals;

/**
 * {@link RowIterators#merge} is what lets a tracked read answer one command from several sub-reads whose results can
 * carry the same partition. Its inputs have already been filtered and purged, so the whole contract is over rows: the
 * merged iterator yields each clustering once, in the partition's iteration order, carrying the newest cell every
 * input had for each column, with the static row and the column set unioned the same way, and closing it closes the
 * inputs.
 * <p>
 * The theory predicts that answer without repeating any reconciliation rule. Every cell in an example gets a
 * timestamp unique across it, so the winner of a column two inputs both carry is just the one with the highest
 * timestamp, and every cell's value records which input carried it, so keeping the right timestamp with the wrong
 * cell still fails.
 */
public class RowIteratorsMergeTest
{
    /** Distinct clusterings an example draws from, plus {@link #STATIC_ROW} for the static row. */
    private static final int ROWS = 4;
    private static final int STATIC_ROW = -1;

    private static final TableMetadata METADATA;
    private static final DecoratedKey KEY;
    private static final ColumnMetadata[] REGULARS;
    private static final ColumnMetadata[] STATICS;

    static
    {
        DatabaseDescriptor.daemonInitialization();
        METADATA = TableMetadata.builder("row_iterators_merge_test", "tbl")
                                .addPartitionKeyColumn("k", Int32Type.instance)
                                .addClusteringColumn("c", Int32Type.instance)
                                .addStaticColumn("s0", Int32Type.instance)
                                .addStaticColumn("s1", Int32Type.instance)
                                .addRegularColumn("v0", Int32Type.instance)
                                .addRegularColumn("v1", Int32Type.instance)
                                .build();
        KEY = METADATA.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        REGULARS = new ColumnMetadata[]{ column("v0"), column("v1") };
        STATICS = new ColumnMetadata[]{ column("s0"), column("s1") };
    }

    private static ColumnMetadata column(String name)
    {
        return METADATA.getColumn(ByteBufferUtil.bytes(name));
    }

    @Test
    public void mergeYieldsTheNewestCellOfEveryRowAndColumn()
    {
        qt().check(rs -> {
            int inputs = rs.nextInt(1, 5);
            boolean reversed = rs.nextBoolean();
            List<CellSpec> cells = generate(rs, inputs);

            List<Input> sources = new ArrayList<>();
            for (int input = 0; input < inputs; input++)
                sources.add(input(input, cells, reversed));

            // the newest cell of each (row, column), which is the row the merge has to yield for that clustering
            TreeMap<Integer, CellSpec[]> newest = new TreeMap<>();
            for (CellSpec cell : cells)
            {
                CellSpec[] row = newest.computeIfAbsent(cell.row, r -> new CellSpec[columnsOf(r).length]);
                if (row[cell.column] == null || row[cell.column].timestamp < cell.timestamp)
                    row[cell.column] = cell;
            }

            List<String> expected = new ArrayList<>();
            for (int row : reversed ? newest.descendingKeySet() : newest.navigableKeySet())
                if (row != STATIC_ROW)
                    expected.add(row + ":" + describe(newest.get(row)));

            try (RowIterator merged = RowIterators.merge(new ArrayList<>(sources)))
            {
                assertEquals(METADATA, merged.metadata());
                assertEquals(KEY, merged.partitionKey());
                assertEquals(reversed, merged.isReverseOrder());
                assertEquals("static row", describe(newest.get(STATIC_ROW)), describe(merged.staticRow()));
                assertEquals("columns", columns(cells, -1).toString(), merged.columns().toString());

                List<String> actual = new ArrayList<>();
                while (merged.hasNext())
                {
                    Row row = merged.next();
                    actual.add(Int32Type.instance.compose(row.clustering().bufferAt(0)) + ":" + describe(row));
                }
                assertEquals("merged rows", expected, actual);
            }

            for (int input = 0; input < inputs; input++)
                assertEquals("closes of input " + input, 1, sources.get(input).closes);
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void mergingNothingIsRejected()
    {
        RowIterators.merge(Collections.emptyList());
    }

    /**
     * A set of cells over {@code inputs} inputs, at most one per (input, row, column) since a row carries a column
     * once, and no two sharing a timestamp.
     */
    private static List<CellSpec> generate(RandomSource rs, int inputs)
    {
        List<CellSpec> cells = new ArrayList<>();
        Set<Integer> positions = new HashSet<>();
        Set<Long> timestamps = new HashSet<>();
        int count = rs.nextInt(0, 13);
        for (int i = 0; i < count; i++)
        {
            int input = rs.nextInt(0, inputs);
            int row = rs.nextInt(STATIC_ROW, ROWS);
            int column = rs.nextInt(0, columnsOf(row).length);
            long timestamp = rs.nextLong(1, 1 << 20);
            if (positions.add((input * (ROWS + 1) + row - STATIC_ROW) * 2 + column) && timestamps.add(timestamp))
                cells.add(new CellSpec(input, row, column, timestamp));
        }
        return cells;
    }

    /** The rows one input returns, carrying only the cells generated for it and reporting only their columns. */
    private static Input input(int input, List<CellSpec> cells, boolean reversed)
    {
        TreeMap<Integer, Row.Builder> builders = new TreeMap<>();
        for (CellSpec cell : cells)
        {
            if (cell.input != input)
                continue;
            builders.computeIfAbsent(cell.row, row -> {
                Row.Builder builder = BTreeRow.unsortedBuilder();
                builder.newRow(row == STATIC_ROW ? Clustering.STATIC_CLUSTERING
                                                 : METADATA.comparator.make(row));
                return builder;
            }).addCell(BufferCell.live(columnsOf(cell.row)[cell.column], cell.timestamp, ByteBufferUtil.bytes(input)));
        }

        Row.Builder staticRow = builders.remove(STATIC_ROW);
        List<Row> rows = new ArrayList<>();
        for (int row : reversed ? builders.descendingKeySet() : builders.navigableKeySet())
            rows.add(builders.get(row).build());

        return new Input(columns(cells, input), reversed,
                         staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow.build(), rows);
    }

    /** The columns the cells of one input use, or of every input when {@code input} is negative. */
    private static RegularAndStaticColumns columns(List<CellSpec> cells, int input)
    {
        RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
        for (CellSpec cell : cells)
            if (input < 0 || cell.input == input)
                builder.add(columnsOf(cell.row)[cell.column]);
        return builder.build();
    }

    private static ColumnMetadata[] columnsOf(int row)
    {
        return row == STATIC_ROW ? STATICS : REGULARS;
    }

    /** Which input carried each of a row's cells, and when, in the order a row iterates its cells. */
    private static String describe(Row row)
    {
        StringBuilder description = new StringBuilder();
        for (Cell<?> cell : row.cells())
            description.append(cell.column().name)
                       .append("=input").append(Int32Type.instance.compose(cell.buffer()))
                       .append('@').append(cell.timestamp()).append(' ');
        return description.toString();
    }

    /** The same description, built from the cells the merge is expected to keep rather than from a merged row. */
    private static String describe(CellSpec[] row)
    {
        StringBuilder description = new StringBuilder();
        if (row != null)
            for (int column = 0; column < row.length; column++)
                if (row[column] != null)
                    description.append(columnsOf(row[column].row)[column].name)
                               .append("=input").append(row[column].input)
                               .append('@').append(row[column].timestamp).append(' ');
        return description.toString();
    }

    /** A generated cell: the input that carries it, the row it belongs to, its column, and its timestamp. */
    private static class CellSpec
    {
        final int input, row, column;
        final long timestamp;

        CellSpec(int input, int row, int column, long timestamp)
        {
            this.input = input;
            this.row = row;
            this.column = column;
            this.timestamp = timestamp;
        }
    }

    /** One input to the merge, which counts how many times it was closed. */
    private static class Input extends AbstractRowIterator
    {
        private final Iterator<Row> rows;
        int closes = 0;

        Input(RegularAndStaticColumns columns, boolean reversed, Row staticRow, List<Row> rows)
        {
            super(METADATA, KEY, columns, reversed, staticRow);
            this.rows = rows.iterator();
        }

        protected Row computeNext()
        {
            return rows.hasNext() ? rows.next() : endOfData();
        }

        public void close()
        {
            closes++;
        }
    }
}
