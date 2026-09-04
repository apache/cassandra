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

package org.apache.cassandra.db.compaction.differential;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests how SSTableCursorReader reads multi-cell (complex) columns.
 *
 * This test drives the reader directly, in the same way the merge drives it. It reads one sstable
 * with the cursor and with the standard iterator, and compares the two results cell by cell.
 *
 * Each side makes the same records, in file order:
 * <pre>
 *   "SR" / "R" / "TM"                                        a static row, a row, a tombstone
 *   "CPLX &lt;col&gt; del=&lt;ts&gt;,&lt;ldt&gt; n=&lt;count&gt;"                the header of a complex column
 *   "CELL &lt;col&gt; path=&lt;hex|-&gt; live=&lt;ts&gt;,&lt;ttl&gt;,&lt;ldt&gt; v=&lt;hex&gt;"   one cell
 * </pre>
 * A CPLX record is made for a complex column that has no cells as well.
 *
 * The two sides compare cell values as the bytes that go to disk. The cursor writes a vint
 * length prefix before a value of variable length: see copyCellContents. The iterator side
 * writes the same format through AbstractType.writeValue.
 */
public class ComplexColumnCursorReadTest extends CQLTester
{
    /** Oracle: canonical records from the standard iterator read. */
    private static List<String> iteratorRecords(SSTableReader sstable) throws IOException
    {
        List<String> out = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    if (!partition.staticRow().isEmpty())
                    {
                        out.add("SR");
                        rowRecords(partition.staticRow(), out);
                    }
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (unfiltered.isRow())
                        {
                            out.add("R");
                            rowRecords((Row) unfiltered, out);
                        }
                        else
                        {
                            out.add("TM");
                        }
                    }
                }
            }
        }
        return out;
    }

    private static void rowRecords(Row row, List<String> out) throws IOException
    {
        for (ColumnData cd : row)
        {
            if (cd.column().isComplex())
            {
                ComplexColumnData complex = (ComplexColumnData) cd;
                out.add(String.format("CPLX %s del=%d,%d n=%d",
                                      cd.column().name, complex.complexDeletion().markedForDeleteAt(),
                                      complex.complexDeletion().localDeletionTime(), complex.cellsCount()));
                for (Cell<?> cell : complex)
                    out.add(cellRecord(cell));
            }
            else
            {
                out.add(cellRecord((Cell<?>) cd));
            }
        }
    }

    private static String cellRecord(Cell<?> cell) throws IOException
    {
        String path = cell.path() == null ? "-" : Hex.bytesToHex(ByteBufferUtil.getArray(cell.path().get(0)));
        String value = "";
        if (cell.valueSize() > 0)
        {
            try (DataOutputBuffer dob = new DataOutputBuffer())
            {
                cell.column().type.writeValue(cell.buffer(), org.apache.cassandra.db.marshal.ByteBufferAccessor.instance, dob);
                value = Hex.bytesToHex(dob.toByteArray());
            }
        }
        return String.format("CELL %s path=%s live=%d,%d,%d v=%s",
                             cell.column().name, path, cell.timestamp(), cell.ttl(), cell.localDeletionTime(), value);
    }

    /** Cursor side: same canonical records by driving SSTableCursorReader directly. */
    private static List<String> cursorRecords(SSTableReader sstable) throws Exception
    {
        return cursorRecords(sstable, true);
    }

    /**
     * @param explicitlyEnablePause if false, drive the reader with whatever
     *                              {@code pauseAtEmptyComplexColumns} ships as by default,
     *                              without touching the setter. This is how {@code StatefulCursor}
     *                              drives the reader in production: see
     *                              {@link #deletionOnlyComplexColumnsSurfaceWithoutExplicitPause}.
     */
    private static List<String> cursorRecords(SSTableReader sstable, boolean explicitlyEnablePause) throws Exception
    {
        List<String> out = new ArrayList<>();
        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable))
        {
            // Without this pause, the cursor side loses the CPLX record of a complex column
            // that has no cells. The field already defaults to true, and StatefulCursor (the
            // only production caller) relies on that default rather than calling this setter.
            // This explicit call only pins the setter's own contract for a caller that does flip
            // it by hand; see deletionOnlyComplexColumnsSurfaceWithoutExplicitPause for the test
            // that actually matches StatefulCursor's behavior.
            if (explicitlyEnablePause)
                cursor.pauseAtEmptyComplexColumns(true);

            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));
            byte[] transfer = new byte[4096];

            int state = cursor.readPartitionHeader(pHeader);
            while (state != DONE)
            {
                while (state != PARTITION_END)
                {
                    switch (state)
                    {
                        case STATIC_ROW_START:
                            out.add("SR");
                            state = cursor.readStaticRowHeader(rHeader);
                            state = readCells(cursor, state, transfer, out);
                            break;
                        case ROW_START:
                            out.add("R");
                            state = cursor.readRowHeader(rHeader);
                            state = readCells(cursor, state, transfer, out);
                            break;
                        case TOMBSTONE_START:
                            out.add("TM");
                            state = cursor.readTombstoneMarker(rHeader);
                            break;
                        default:
                            throw new IllegalStateException("state " + state);
                    }
                    if (state == UNFILTERED_END)
                        state = cursor.continueReading();
                }
                state = cursor.continueReading();
                if (state != DONE)
                    state = cursor.readPartitionHeader(pHeader);
            }
        }
        return out;
    }

    private static int readCells(SSTableCursorReader cursor, int state, byte[] transfer, List<String> out) throws IOException
    {
        SSTableCursorReader.CellCursor cc = cursor.cellCursor();
        // The cursor gives cells, not column headers. The CPLX record therefore comes from the
        // first cell of a column, or from the position of a column that has no cells.
        ColumnMetadata lastComplex = null;
        while (true)
        {
            if (state == UNFILTERED_END)
                return state;
            if (state == CELL_END)
            {
                state = cursor.continueReading();
                continue;
            }
            if (state != CELL_HEADER_START)
                throw new IllegalStateException("unexpected state " + state);

            state = cursor.readCellHeader();
            // CELL_END without a cell is the position of a column that has no cells. Any other
            // result gives no position, and cellColumn still holds an old value.
            boolean atPosition = cc.producedCell || state == CELL_END;
            if (atPosition && cc.cellColumn.isComplex() && cc.cellColumn != lastComplex)
            {
                lastComplex = cc.cellColumn;
                // readCellHeader already subtracted the cell it just gave from remainingCellsInColumn.
                int cellsInColumn = cc.producedCell ? cc.remainingCellsInColumn + 1 : 0;
                out.add(String.format("CPLX %s del=%d,%d n=%d",
                                      cc.cellColumn.name,
                                      cc.complexDeletion.markedForDeleteAt(), cc.complexDeletion.localDeletionTime(),
                                      cellsInColumn));
            }
            if (!cc.producedCell)
                continue; // a complex column that holds a deletion and no cells

            String path = cc.cellPathLength < 0 ? "-" : Hex.bytesToHex(java.util.Arrays.copyOf(cc.cellPathBuffer, cc.cellPathLength));
            String value = "";
            if (state == CELL_VALUE_START)
            {
                try (DataOutputBuffer dob = new DataOutputBuffer())
                {
                    state = cursor.copyCellValue(dob, transfer);
                    value = Hex.bytesToHex(dob.toByteArray());
                }
            }
            out.add(String.format("CELL %s path=%s live=%d,%d,%d v=%s",
                                  cc.cellColumn.name, path,
                                  cc.cellLiveness.timestamp(), cc.cellLiveness.ttl(), cc.cellLiveness.localDeletionTime(),
                                  value));
        }
    }

    private void assertCursorReadsMatch() throws Exception
    {
        assertCursorReadsMatch(true);
    }

    private void assertCursorReadsMatch(boolean explicitlyEnablePause) throws Exception
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        flush();
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        List<String> expected = iteratorRecords(sstable);
        List<String> actual = cursorRecords(sstable, explicitlyEnablePause);

        // The scenario must give complex columns and cells that hold a path.
        assertTrue("scenario produced no complex column records",
                   expected.stream().anyMatch(r -> r.startsWith("CPLX")));
        assertTrue("scenario produced no path-carrying cells",
                   expected.stream().anyMatch(r -> r.startsWith("CELL") && !r.contains("path=-")));

        int max = Math.max(expected.size(), actual.size());
        for (int i = 0; i < max; i++)
        {
            String e = i < expected.size() ? expected.get(i) : "<missing>";
            String a = i < actual.size() ? actual.get(i) : "<missing>";
            if (!e.equals(a))
                fail(String.format("record %d differs:%n  iterator: %s%n  cursor:   %s%n  context: %s",
                                   i, e, a, context(expected, actual, i)));
        }
    }

    private static String context(List<String> expected, List<String> actual, int i)
    {
        StringBuilder sb = new StringBuilder();
        for (int j = Math.max(0, i - 3); j < Math.min(expected.size(), i + 3); j++)
            sb.append(String.format("%n    it[%d]=%s", j, expected.get(j)));
        for (int j = Math.max(0, i - 3); j < Math.min(actual.size(), i + 3); j++)
            sb.append(String.format("%n    cu[%d]=%s", j, actual.get(j)));
        return sb.toString();
    }

    /**
     * Tests that the bytes a cursor read allocates do not grow with the number of rows.
     *
     * The reader copies every value through one buffer that it reuses, and the cell path buffer
     * grows only a few times.
     *
     * The limit allows for the memory that the test environment itself uses at these data sizes,
     * such as the chunk cache and Ref debug tracking. CursorCompactionAllocationGateTest
     * describes the same effect.
     */
    @Test
    public void complexReadAllocationDoesNotScale() throws Exception
    {
        java.lang.management.ThreadMXBean raw = java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.Assume.assumeTrue(raw instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) raw;
        if (!bean.isThreadAllocatedMemoryEnabled())
            bean.setThreadAllocatedMemoryEnabled(true);

        long small = measureWalk(bean, 6);
        long big = measureWalk(bean, 60);
        long delta = big - small;
        logger.info("complex cursor read allocation: small={}B big={}B delta={}B", small, big, delta);
        assertTrue(String.format("complex read allocation scales with rows: %,dB -> %,dB (delta %,dB)",
                                 small, big, delta),
                   delta <= 64 * 1024); // measured 4,736B at these sizes; trips at ~+2.7B/cell
    }

    private long measureWalk(com.sun.management.ThreadMXBean bean, int partitions) throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < partitions; pk++)
            for (long ck = 0; ck < 100; ck++)
                execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, ?, ?)",
                        pk, ck, map("k1" + ck, ck, "k2" + ck, pk), "v" + ck);
        flush();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        long best = Long.MAX_VALUE;
        long tid = Thread.currentThread().getId();
        for (int i = 0; i < 5; i++)
        {
            long before = bean.getThreadAllocatedBytes(tid);
            leanWalk(sstable);
            long allocated = bean.getThreadAllocatedBytes(tid) - before;
            if (i >= 2)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /** Pure consumption walk: every cell header + path + value, one reused value buffer. */
    private static void leanWalk(SSTableReader sstable) throws Exception
    {
        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable);
             DataOutputBuffer valueSink = new DataOutputBuffer())
        {
            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));
            byte[] transfer = new byte[4096];

            int state = cursor.readPartitionHeader(pHeader);
            while (state != DONE)
            {
                while (state != PARTITION_END)
                {
                    switch (state)
                    {
                        case STATIC_ROW_START: state = cursor.readStaticRowHeader(rHeader); break;
                        case ROW_START: state = cursor.readRowHeader(rHeader); break;
                        case TOMBSTONE_START: state = cursor.readTombstoneMarker(rHeader); break;
                        default: throw new IllegalStateException("state " + state);
                    }
                    // The row-header read above selects which cell cursor is active.
                    SSTableCursorReader.CellCursor cc = cursor.cellCursor();
                    while (state != UNFILTERED_END && state != PARTITION_END)
                    {
                        if (state == CELL_END) { state = cursor.continueReading(); continue; }
                        if (state != CELL_HEADER_START) break;
                        state = cursor.readCellHeader();
                        if (cc.producedCell && state == CELL_VALUE_START)
                        {
                            valueSink.clear();
                            state = cursor.copyCellValue(valueSink, transfer);
                        }
                    }
                    if (state == UNFILTERED_END)
                        state = cursor.continueReading();
                }
                state = cursor.continueReading();
                if (state != DONE)
                    state = cursor.readPartitionHeader(pHeader);
            }
        }
    }

    @Test
    public void multiCellCollections() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, l list<text>, v text, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 6; ck++)
            {
                execute("INSERT INTO %s (pk, ck, m, s, l, v) VALUES (?, ?, ?, ?, ?, ?)",
                        pk, ck, map("k" + ck, ck, "x", pk), set((int) ck, 42), list("a" + ck, "b"), "v" + ck);
                // An update of one element adds a cell and makes no complex deletion.
                execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "extra" + ck, ck * 10, pk, ck);
            }
        // An overwrite of the whole collection makes a complex deletion and new cells. The
        // memtable merges them.
        execute("UPDATE %s SET m = ? WHERE pk = ? AND ck = ?", map("only", 1L), 1L, 1L);

        assertCursorReadsMatch();
    }

    /**
     * A caller that explicitly enables {@code pauseAtEmptyComplexColumns} still sees the CPLX record
     * of a deletion-only complex column. No production caller does this today — {@code StatefulCursor}
     * relies on the field's default instead, which {@link #deletionOnlyComplexColumnsSurfaceWithoutExplicitPause}
     * pins — but the setter is public API on the raw reader and must keep working for a caller that uses it directly.
     */
    @Test
    public void deletionOnlyComplexColumns() throws Exception
    {
        // The column zz sorts after the columns a and b. If zz holds a deletion and no cells, it
        // is the last column of the row, which tests the path that returns -1.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a text, b bigint, zz map<text, bigint>, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, a, b, zz) VALUES (?, ?, ?, ?, ?)", 1L, ck, "a" + ck, ck, map("m" + ck, ck));
        // Delete the collection on some rows, but keep the rows.
        for (long ck = 0; ck < 8; ck += 2)
            execute("DELETE zz FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        // Make a row that holds the key and the deleted complex column, and nothing else.
        execute("DELETE zz FROM %s WHERE pk = ? AND ck = ?", 1L, 100L);

        assertCursorReadsMatch();
    }

    /**
     * Pins the behavior {@code StatefulCursor} actually relies on: the raw reader's
     * {@code pauseAtEmptyComplexColumns} field defaults to true, so a caller that never touches
     * the setter still sees the CPLX record of a deletion-only complex column. Same scenario as
     * {@link #deletionOnlyComplexColumns}, but without that test's explicit
     * {@code pauseAtEmptyComplexColumns(true)} call. If the default ever regresses to false, this
     * is the test that catches it; {@link #deletionOnlyComplexColumns} would still pass.
     */
    @Test
    public void deletionOnlyComplexColumnsSurfaceWithoutExplicitPause() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a text, b bigint, zz map<text, bigint>, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, a, b, zz) VALUES (?, ?, ?, ?, ?)", 1L, ck, "a" + ck, ck, map("m" + ck, ck));
        for (long ck = 0; ck < 8; ck += 2)
            execute("DELETE zz FROM %s WHERE pk = ? AND ck = ?", 1L, ck);
        execute("DELETE zz FROM %s WHERE pk = ? AND ck = ?", 1L, 100L);

        assertCursorReadsMatch(false);
    }

    @Test
    public void multiCellUdtAndStatics() throws Exception
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, sm map<text, bigint> static, ck bigint, u " + udt + ", v text, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
        {
            execute("UPDATE %s SET sm[?] = ? WHERE pk = ?", "static" + pk, pk, pk);
            for (long ck = 0; ck < 5; ck++)
            {
                execute("INSERT INTO %s (pk, ck, u, v) VALUES (?, ?, {a: ?, b: ?}, ?)", pk, ck, (int) ck, "f" + ck, "v" + ck);
                execute("UPDATE %s SET u.b = ? WHERE pk = ? AND ck = ?", "updated" + ck, pk, ck); // field-level cell
            }
        }

        assertCursorReadsMatch();
    }

    @Test
    public void sparseRowsWithComplex() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, a text, m map<text, bigint>, z text, " +
                    "PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
        {
            switch ((int) (ck % 4))
            {
                case 0: execute("INSERT INTO %s (pk, ck, a, m, z) VALUES (?, ?, ?, ?, ?)", 1L, ck, "a", map("k", ck), "z"); break;
                case 1: execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "only", ck, 1L, ck); break; // complex only
                case 2: execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1L, ck, "a" + ck); break;      // simple only
                case 3: execute("UPDATE %s SET z = ?, m[?] = ? WHERE pk = ? AND ck = ?", "z" + ck, "k2", ck, 1L, ck); break;
            }
        }

        assertCursorReadsMatch();
    }
}
