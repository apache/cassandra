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

package org.apache.cassandra.db.memtable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.collect.Iterables;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/// Tests the read path of [TrieMemtable] partitions, whose rows are copied to the heap as [BTreeRow]s.
///
/// Every read is compared with the same read on an [ImmutableBTreePartition] built from the whole memtable partition,
/// and with the same read on the sstable that the memtable is flushed to (which checks the copied and filtered rows
/// against an independent implementation).
public class TrieMemtablePartitionReadTest extends CQLTester
{
    private static final String CLUSTERED_TABLE = "CREATE TABLE %s (pk int, ck1 int, ck2 text, s int static, v1 int, v2 text, m map<int, text>, " +
                                                  "PRIMARY KEY (pk, ck1, ck2)) WITH memtable = 'trie'";

    private static final String REVERSED_CLUSTERED_TABLE = CLUSTERED_TABLE + " AND CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)";

    private static final int ROWS = 8;

    /// Clustering (100, 'x'), used by some tests for rows without primary key liveness, and never written by others.
    private static final int EXTRA_ROW = 100;
    /// Clustering (101, 'y'): v2 is older than the row's primary key liveness, so it is skipped when v2 is fetched
    /// but not queried.
    private static final int OLDER_CELL_ROW = 101;

    @Test
    public void testRowsWithoutDeletions() throws Throwable
    {
        testRowsWithoutDeletions(CLUSTERED_TABLE);
    }

    @Test
    public void testRowsWithoutDeletionsReversedClusteringOrder() throws Throwable
    {
        testRowsWithoutDeletions(REVERSED_CLUSTERED_TABLE);
    }

    private void testRowsWithoutDeletions(String table) throws Throwable
    {
        createTable(table);
        insertRows(0);
        // This row has no primary key liveness and only a v2 cell: selecting only v1 filters it out completely.
        execute("UPDATE %s SET v2 = 'only v2' WHERE pk = 0 AND ck1 = 100 AND ck2 = 'x'");
        execute("UPDATE %s USING TTL 1000 SET v2 = 'ttl' WHERE pk = 0 AND ck1 = 1 AND ck2 = 'b'");
        execute("UPDATE %s SET v1 = null WHERE pk = 0 AND ck1 = 2 AND ck2 = 'c'");
        execute("UPDATE %s SET m = m + {7: 'seven'} WHERE pk = 0 AND ck1 = 3 AND ck2 = 'd'");

        assertMatchesReferences(0);
    }

    @Test
    public void testRowsWithDeletions() throws Throwable
    {
        testRowsWithDeletions(CLUSTERED_TABLE);
    }

    @Test
    public void testRowsWithDeletionsReversedClusteringOrder() throws Throwable
    {
        testRowsWithDeletions(REVERSED_CLUSTERED_TABLE);
    }

    private void testRowsWithDeletions(String table) throws Throwable
    {
        createTable(table);
        execute("DELETE FROM %s USING TIMESTAMP 1 WHERE pk = 0");
        insertRows(0);
        execute("DELETE FROM %s WHERE pk = 0 AND ck1 = 1 AND ck2 = 'b'");
        execute("DELETE FROM %s WHERE pk = 0 AND ck1 >= 4 AND ck1 <= 5");
        execute("DELETE v2 FROM %s WHERE pk = 0 AND ck1 = 2 AND ck2 = 'c'");
        execute("UPDATE %s SET m = {9: 'nine'} WHERE pk = 0 AND ck1 = 3 AND ck2 = 'd'");
        execute("INSERT INTO %s (pk, ck1, ck2, v1) VALUES (0, 1, 'b', 11)");

        assertMatchesReferences(0);
    }

    @Test
    public void testOnlyComplexColumnDeletion() throws Throwable
    {
        createTable(CLUSTERED_TABLE);
        insertRows(0);
        // Overwriting a collection deletes the previous content with a complex column deletion.
        execute("UPDATE %s SET m = {9: 'nine'} WHERE pk = 0 AND ck1 = 3 AND ck2 = 'd'");

        assertMatchesReferences(0);
    }

    @Test
    public void testOnlyStaticRow() throws Throwable
    {
        createTable(CLUSTERED_TABLE);
        execute("INSERT INTO %s (pk, s) VALUES (0, 1000)");

        assertMatchesReferences(0);
    }

    @Test
    public void testDroppedColumns() throws Throwable
    {
        createTable(CLUSTERED_TABLE);
        // Dropping a column flushes the memtable. Re-add the columns: with dropped columns in the table metadata,
        // every read of the memtable rows filters them by the dropped columns.
        execute("ALTER TABLE %s DROP v2");
        execute("ALTER TABLE %s ADD v2 text");
        execute("ALTER TABLE %s DROP m");
        execute("ALTER TABLE %s ADD m map<int, text>");
        insertRows(0);
        execute("UPDATE %s USING TIMESTAMP 1 SET v2 = 'old' WHERE pk = 0 AND ck1 = 100 AND ck2 = 'x'");
        execute("UPDATE %s SET v1 = 100 WHERE pk = 0 AND ck1 = 100 AND ck2 = 'x'");
        assertFalse(getCurrentColumnFamilyStore().metadata().droppedColumns.isEmpty());

        TrieBackedPartition partition = memtablePartition(0);
        for (Iterator<Row> it = partition.rowIterator(); it.hasNext(); )
            assertTrue(it.next() instanceof BTreeRow);
        // The write older than the drop is discarded by the write path, so the read returns no v2.
        assertRows(execute("SELECT v1, v2 FROM %s WHERE pk = 0 AND ck1 = 100 AND ck2 = 'x'"), row(100, null));
        assertRows(execute("SELECT v1, v2 FROM %s WHERE pk = 0 AND ck1 = 3 AND ck2 = 'd'"), row(3, "v3"));
        assertMatchesReferences(0);
    }

    @Test
    public void testTableWithoutClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, m map<int, text>) WITH memtable = 'trie'");
        execute("INSERT INTO %s (pk, v1) VALUES (0, 1)");
        execute("UPDATE %s SET m = m + {1: 'one'} WHERE pk = 0");
        execute("UPDATE %s SET v1 = 2 WHERE pk = 1");
        // Inserting a collection deletes its previous content: partition 2 has a complex column deletion.
        execute("INSERT INTO %s (pk, v1, m) VALUES (2, 3, {3: 'three'})");

        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        List<ColumnFilter> columnFilters = List.of(ColumnFilter.all(metadata),
                                                   ColumnFilter.selection(RegularAndStaticColumns.of(column(metadata, "v1"))));
        List<List<String>> memtableContent = new ArrayList<>();
        for (int pk = 0; pk < 3; ++pk)
        {
            TrieBackedPartition partition = memtablePartition(pk);
            ImmutableBTreePartition reference = ImmutableBTreePartition.create(partition.unfilteredIterator());
            assertEquals(1, reference.rowCount());

            for (ColumnFilter columnFilter : columnFilters)
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    try (UnfilteredRowIterator actual = partition.unfilteredIterator(columnFilter, Slices.ALL, reversed);
                         UnfilteredRowIterator expected = reference.unfilteredIterator(columnFilter, Slices.ALL, reversed))
                    {
                        memtableContent.add(assertSameContent(metadata, expected, actual));
                    }

                    NavigableSet<Clustering<?>> names = new TreeSet<>(metadata.comparator);
                    names.add(Clustering.EMPTY);
                    try (UnfilteredRowIterator actual = partition.unfilteredIterator(columnFilter, names, reversed);
                         UnfilteredRowIterator expected = reference.unfilteredIterator(columnFilter, names, reversed))
                    {
                        assertSameContent(metadata, expected, actual);
                    }
                }
            }
        }

        flush();
        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        Iterator<List<String>> memtableContentIterator = memtableContent.iterator();
        for (int pk = 0; pk < 3; ++pk)
        {
            for (ColumnFilter columnFilter : columnFilters)
            {
                for (boolean reversed : new boolean[]{ false, true })
                {
                    try (UnfilteredRowIterator fromSSTable = sstable.rowIterator(key(pk), Slices.ALL, columnFilter, reversed, SSTableReadsListener.NOOP_LISTENER))
                    {
                        assertEquals(memtableContentIterator.next(), contentOf(metadata, fromSSTable, false));
                    }
                }
            }
        }
    }

    @Test
    public void testDecimalClustering() throws Throwable
    {
        // The byte-comparable encoding of decimals is normalized: 1.0 and 1.00 are the same clustering.
        createTable("CREATE TABLE %s (pk int, ck decimal, v int, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 1.0, 1)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (0, 2.50, 2)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT v FROM %s WHERE pk = 0 AND ck = 1.00"), row(1));
            assertRows(execute("SELECT v FROM %s WHERE pk = 0 AND ck = 2.5"), row(2));
            assertRows(execute("SELECT v FROM %s WHERE pk = 0 AND ck IN (1, 2.500, 3)"), row(1), row(2));
            BigDecimal ck = DecimalType.instance.compose(execute("SELECT ck FROM %s WHERE pk = 0 AND ck = 1.00").one().getBytes("ck"));
            assertEquals(0, BigDecimal.ONE.compareTo(ck));
        });
    }

    @Test
    public void testCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, c counter, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
        for (int i = 0; i < 3; ++i)
        {
            execute("UPDATE %s SET c = c + 1 WHERE pk = 0 AND ck = ?", i);
            execute("UPDATE %s SET c = c + ? WHERE pk = 0 AND ck = ?", (long) i, i);
        }

        TrieBackedPartition partition = memtablePartition(0);
        TableMetadata metadata = partition.metadata();
        NavigableSet<Clustering<?>> names = new TreeSet<>(metadata.comparator);
        names.add(Clustering.make(Int32Type.instance.decompose(1)));
        try (UnfilteredRowIterator iter = partition.unfilteredIterator(ColumnFilter.all(metadata), names, false))
        {
            assertTrue(iter.next() instanceof BTreeRow);
            assertFalse(iter.hasNext());
        }

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT c FROM %s WHERE pk = 0 AND ck = 1"), row(2L));
            assertRows(execute("SELECT ck, c FROM %s WHERE pk = 0 AND ck IN (0, 2, 3)"), row(0, 1L), row(2, 3L));
            execute("UPDATE %s SET c = c + 10 WHERE pk = 0 AND ck = 0");
            assertRows(execute("SELECT c FROM %s WHERE pk = 0 AND ck = 0"), row(11L));
            execute("UPDATE %s SET c = c - 10 WHERE pk = 0 AND ck = 0");
        });
    }

    @Test
    public void testRowsAreCopiedToHeapAsBTreeRows() throws Throwable
    {
        createTable(CLUSTERED_TABLE);
        insertRows(0);

        TrieBackedPartition partition = memtablePartition(0);
        TableMetadata metadata = partition.metadata();
        Row staticRow = partition.staticRow();
        assertTrue(staticRow instanceof BTreeRow);
        assertEquals(1000, Int32Type.instance.compose(staticRow.getCell(column(metadata, "s")).buffer()).intValue());

        Row row = partition.getRow(clustering(3));
        assertTrue(row instanceof BTreeRow);
        assertEquals(3, Int32Type.instance.compose(row.getCell(column(metadata, "v1")).buffer()).intValue());
        assertEquals(2, row.getComplexColumnData(column(metadata, "m")).cellsCount());

        // Each read must make its own copy of the values: two reads of views of the memtable buffers would share
        // their backing storage.
        Row again = partition.getRow(clustering(3));
        assertNotSame(row, again);
        Iterator<Cell<?>> cells = row.cells().iterator();
        for (Cell<?> cellAgain : again.cells())
        {
            Cell<?> cell = cells.next();
            assertEquals(cell, cellAgain);
            if (cell.valueSize() > 0)
                assertNotSame(cell.buffer().array(), cellAgain.buffer().array());
        }
        assertFalse(cells.hasNext());

        int count = 0;
        for (Iterator<Row> it = partition.rowIterator(); it.hasNext(); ++count)
            assertTrue(it.next() instanceof BTreeRow);
        assertEquals(ROWS + 1, count); // with OLDER_CELL_ROW

        try (UnfilteredRowIterator iter = partition.unfilteredIterator(ColumnFilter.all(metadata), Slices.ALL, false))
        {
            assertTrue(iter.staticRow() instanceof BTreeRow);
            while (iter.hasNext())
                assertTrue(iter.next() instanceof BTreeRow);
        }
    }

    @Test
    public void testQueriesBeforeAndAfterFlush() throws Throwable
    {
        createTable(CLUSTERED_TABLE);
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        for (int pk = 0; pk < 3; ++pk)
            insertRows(pk);
        // Partition 1 has deletions.
        execute("DELETE FROM %s WHERE pk = 1 AND ck1 = 1 AND ck2 = 'b'");
        execute("DELETE FROM %s WHERE pk = 1 AND ck1 >= 4 AND ck1 <= 5");
        execute("DELETE v2 FROM %s WHERE pk = 1 AND ck1 = 2 AND ck2 = 'c'");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk, ck1, ck2, s, v1, v2 FROM %s WHERE pk = 0 AND ck1 = 2 AND ck2 = 'c'"),
                       row(0, 2, "c", 1000, 2, "v2"));
            assertEmpty(execute("SELECT * FROM %s WHERE pk = 0 AND ck1 = 2 AND ck2 = 'x'"));
            assertRows(execute("SELECT v1 FROM %s WHERE pk = 2 AND ck1 IN (1, 3, 99) AND ck2 IN ('b', 'd')"),
                       row(1), row(3));
            assertRows(execute("SELECT v1 FROM %s WHERE pk = 2 AND ck1 IN (1, 3) AND ck2 IN ('b', 'd') ORDER BY ck1 DESC, ck2 DESC"),
                       row(3), row(1));
            assertRows(execute("SELECT ck1, v1, v2 FROM %s WHERE pk = 1 AND ck1 IN (1, 2, 4, 6) AND ck2 IN ('b', 'c', 'e', 'g')"),
                       row(2, 2, null), row(6, 6, "v6"));
            assertRows(execute("SELECT m FROM %s WHERE pk = 0 AND ck1 = 3 AND ck2 = 'd'"),
                       row(map(3, "three", 30, "thirty")));
            assertRows(execute("SELECT m[30] FROM %s WHERE pk = 0 AND ck1 = 3 AND ck2 = 'd'"),
                       row("thirty"));
            assertRows(execute("SELECT v1, v2 FROM %s WHERE pk = 0 AND ck1 = 101 AND ck2 = 'y'"),
                       row(OLDER_CELL_ROW, "older"));
            assertRowsIgnoringOrder(execute("SELECT pk, ck1, ck2 FROM %s WHERE v1 = 3"),
                                    row(0, 3, "d"), row(1, 3, "d"), row(2, 3, "d"));
            assertRowsIgnoringOrder(execute("SELECT pk, ck1, ck2, v2 FROM %s WHERE v1 = 2"),
                                    row(0, 2, "c", "v2"), row(1, 2, "c", null), row(2, 2, "c", "v2"));
            assertEmpty(execute("SELECT pk FROM %s WHERE v1 = 4 AND pk = 1"));
        });
    }

    private void insertRows(int pk) throws Throwable
    {
        execute("INSERT INTO %s (pk, s) VALUES (?, 1000)", pk);
        for (int i = 0; i < ROWS; ++i)
        {
            execute("INSERT INTO %s (pk, ck1, ck2, v1, v2) VALUES (?, ?, ?, ?, ?)",
                    pk, i, ck2(i), i, i == 2 ? "v2" : "v" + i);
            // Appending to a collection, unlike inserting it, does not delete its previous content.
            execute("UPDATE %s SET m = m + ? WHERE pk = ? AND ck1 = ? AND ck2 = ?",
                    map(i, "three", i * 10, "thirty"), pk, i, ck2(i));
        }
        // A v2 cell older than the row's primary key liveness.
        execute("UPDATE %s USING TIMESTAMP 10 SET v2 = 'older' WHERE pk = ? AND ck1 = ? AND ck2 = ?",
                pk, OLDER_CELL_ROW, ck2(OLDER_CELL_ROW));
        execute("INSERT INTO %s (pk, ck1, ck2, v1) VALUES (?, ?, ?, ?) USING TIMESTAMP 20",
                pk, OLDER_CELL_ROW, ck2(OLDER_CELL_ROW), OLDER_CELL_ROW);
    }

    private static String ck2(int i)
    {
        switch (i)
        {
            case EXTRA_ROW: return "x";
            case OLDER_CELL_ROW: return "y";
            default: return String.valueOf((char) ('a' + i));
        }
    }

    private static Clustering<?> clustering(int i)
    {
        return Clustering.make(Int32Type.instance.decompose(i), ByteBufferUtil.bytes(ck2(i)));
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(name));
        assertNotNull(name, column);
        return column;
    }

    private DecoratedKey key(int pk)
    {
        return getCurrentColumnFamilyStore().decorateKey(Int32Type.instance.decompose(pk));
    }

    private TrieBackedPartition memtablePartition(int pk)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Memtable memtable = cfs.getCurrentMemtable();
        assertTrue(memtable instanceof TrieMemtable);
        Partition partition = ((TrieMemtable) memtable).getPartition(key(pk));
        assertNotNull(partition);
        return (TrieBackedPartition) partition;
    }

    /// A read of a partition: column filter, clustering selection and direction. Reads of single rows also give the
    /// selection as names, in query order; others have null names.
    private static class Read
    {
        final ColumnFilter columnFilter;
        final Slices slices;
        final NavigableSet<Clustering<?>> namesInQueryOrder;
        final boolean reversed;

        Read(ColumnFilter columnFilter, Slices slices, NavigableSet<Clustering<?>> namesInQueryOrder, boolean reversed)
        {
            this.columnFilter = columnFilter;
            this.slices = slices;
            this.namesInQueryOrder = namesInQueryOrder;
            this.reversed = reversed;
        }
    }

    private static List<Read> reads(TableMetadata metadata)
    {
        ColumnMetadata v1 = column(metadata, "v1");
        ColumnMetadata m = column(metadata, "m");
        List<ColumnFilter> columnFilters = List.of(ColumnFilter.all(metadata),
                                                   ColumnFilter.selection(RegularAndStaticColumns.of(v1)),
                                                   ColumnFilter.allRegularColumnsBuilder(metadata, false).add(v1).build(),
                                                   ColumnFilter.selectionBuilder().add(v1).select(m, CellPath.create(Int32Type.instance.decompose(30))).build());

        List<int[]> rowSets = List.of(new int[]{ 3 },
                                      new int[]{ 0, 1, 2, 3, 4, 5, 6, 7, EXTRA_ROW, OLDER_CELL_ROW },
                                      new int[]{ 1, 4, 99 },
                                      new int[]{ 99 },
                                      new int[]{ EXTRA_ROW },
                                      new int[]{ OLDER_CELL_ROW });

        List<Read> reads = new ArrayList<>();
        for (ColumnFilter columnFilter : columnFilters)
        {
            for (boolean reversed : new boolean[]{ false, true })
            {
                for (int[] rows : rowSets)
                {
                    Slices.Builder slicesBuilder = new Slices.Builder(metadata.comparator);
                    NavigableSet<Clustering<?>> names = new TreeSet<>(metadata.comparator);
                    for (int i : rows)
                    {
                        slicesBuilder.add(Slice.make(clustering(i)));
                        names.add(clustering(i));
                    }
                    reads.add(new Read(columnFilter, slicesBuilder.build(), reversed ? names.descendingSet() : names, reversed));
                }

                Slices range = Slices.with(metadata.comparator, metadata.comparator.compare(clustering(1), clustering(5)) < 0
                                                                ? Slice.make(clustering(1), clustering(5))
                                                                : Slice.make(clustering(5), clustering(1)));
                reads.add(new Read(columnFilter, range, null, reversed));
                reads.add(new Read(columnFilter, Slices.ALL, null, reversed));
            }
        }
        return reads;
    }

    /// Compares all the read shapes on the given memtable partition with the same reads on a [ImmutableBTreePartition]
    /// built from it, then flushes the memtable and compares them with the same reads on the sstable.
    private void assertMatchesReferences(int pk) throws Throwable
    {
        TrieBackedPartition partition = memtablePartition(pk);
        TableMetadata metadata = getCurrentColumnFamilyStore().metadata();
        ImmutableBTreePartition reference = ImmutableBTreePartition.create(partition.unfilteredIterator());

        List<Read> reads = reads(metadata);
        List<List<String>> memtableContent = new ArrayList<>();
        for (Read read : reads)
        {
            boolean singleRows = read.namesInQueryOrder != null;
            try (UnfilteredRowIterator actual = partition.unfilteredIterator(read.columnFilter, read.slices, read.reversed);
                 UnfilteredRowIterator expected = reference.unfilteredIterator(read.columnFilter, read.slices, read.reversed))
            {
                memtableContent.add(assertSameContent(metadata, expected, actual));
            }

            if (singleRows)
            {
                try (UnfilteredRowIterator actual = partition.unfilteredIterator(read.columnFilter, read.namesInQueryOrder, read.reversed);
                     UnfilteredRowIterator expected = reference.unfilteredIterator(read.columnFilter, read.slices, read.reversed))
                {
                    assertSameContent(metadata, expected, actual);
                }
            }
        }

        flush();
        SSTableReader sstable = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables());
        for (int i = 0; i < reads.size(); ++i)
        {
            Read read = reads.get(i);
            try (UnfilteredRowIterator fromSSTable = sstable.rowIterator(key(pk), read.slices, read.columnFilter, read.reversed, SSTableReadsListener.NOOP_LISTENER))
            {
                assertEquals("Read " + i, memtableContent.get(i), contentOf(metadata, fromSSTable, false));
            }
        }
    }

    /// Asserts that the two iterators have the same content and returns it, partition-level information first.
    private static List<String> assertSameContent(TableMetadata metadata, UnfilteredRowIterator expected, UnfilteredRowIterator actual)
    {
        assertEquals(expected.partitionKey(), actual.partitionKey());
        assertEquals(expected.isReverseOrder(), actual.isReverseOrder());
        assertEquals(expected.columns(), actual.columns());
        List<String> content = contentOf(metadata, actual, true);
        assertEquals(contentOf(metadata, expected, true), content);
        return content;
    }

    private static List<String> contentOf(TableMetadata metadata, UnfilteredRowIterator iterator, boolean mustBeBTreeRows)
    {
        List<String> content = new ArrayList<>();
        // As a plain DeletionTime: the trie's deletion also prints the kind of its marker.
        DeletionTime partitionLevelDeletion = iterator.partitionLevelDeletion();
        content.add(DeletionTime.build(partitionLevelDeletion.markedForDeleteAt(), partitionLevelDeletion.localDeletionTime()).toString());
        content.add(toString(metadata, iterator.staticRow()));
        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            assertNotNull(unfiltered);
            if (mustBeBTreeRows && unfiltered.isRow())
                assertTrue(unfiltered instanceof BTreeRow);
            String string = unfiltered.toString(metadata, true);
            // Empty clusterings decoded from sstables are equal to, but print differently from, Clustering.EMPTY.
            if (metadata.comparator.size() == 0)
                string = string.replace("]: EMPTY | ", "]:  | ");
            content.add(string);
        }
        return content;
    }

    private static String toString(TableMetadata metadata, Row row)
    {
        return row == Rows.EMPTY_STATIC_ROW || row.isEmpty() ? "empty" : row.toString(metadata, true);
    }
}
