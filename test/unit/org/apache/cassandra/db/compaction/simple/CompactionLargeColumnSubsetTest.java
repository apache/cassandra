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

package org.apache.cassandra.db.compaction.simple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.TestHelper.verifyAndPrint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Compaction of rows whose column subset is written in the large-subset wire format, i.e. from a
 * 64-column superset up ({@link org.apache.cassandra.db.Columns.Serializer#serializeSubset}). That
 * format has two modes — a list of present indices or a list of missing ones — and both the mode
 * rule and the present-index encoding are boundary-sensitive. Each test below fixes a superset size
 * and a subset shape that sits on one of those boundaries, compacts, and asserts the output row
 * decodes back to exactly the columns and values it was written with.
 * <p>
 * The scenarios are shaped so that a mis-encoded subset cannot pass: the deserializer takes its
 * column count and its mode from the superset size and the delta, so an encoding that disagrees
 * with it on either runs off the end of the index list and consumes row-body bytes as indices.
 */
public class CompactionLargeColumnSubsetTest extends SimpleCompactionTest
{
    /**
     * 65 columns with 32 present: {@code presentCount == supersetCount / 2}, which the reference
     * rule — {@code presentCount < supersetCount / 2}, tested identically by
     * {@code Columns.Serializer.serializeLargeSubset} and {@code deserializeLargeSubset} — resolves
     * to missing-index mode.
     * <p>
     * An odd superset size is what makes the boundary observable. The same shape stated in terms of
     * the missing count is {@code missingCount == supersetCount / 2 + 1}, so a mode rule keyed off
     * the missing count answers present-index mode here while the deserializer reads missing-index
     * mode. For an even superset size the two formulations agree and the boundary is invisible.
     */
    @Test
    public void testLargeSubsetModeSelectionAtOddSupersetBoundary() throws Throwable
    {
        int columnCount = 65;
        // every even index below 64: 32 present, 33 missing (the 32 odd indices, plus index 64)
        int[] present = new int[32];
        for (int i = 0; i < present.length; i++)
            present[i] = 2 * i;

        int presentCount = present.length;
        int missingCount = columnCount - presentCount;
        assertTrue("the scenario must keep the two formulations of the mode rule in disagreement, " +
                   "or it stops covering the boundary: superset=" + columnCount +
                   " present=" + presentCount + " missing=" + missingCount,
                   (presentCount < columnCount / 2) != (missingCount > columnCount / 2));

        runLargeSubsetScenario(columnCount, present, false);
    }

    /**
     * 70 columns with 10 present: {@code presentCount < supersetCount / 2}, so the subset is encoded
     * as a list of present indices. The two highest columns of the superset are present and the
     * highest missing one is index 67, so the run of present indices after the last missing index is
     * non-empty — the part of the encoding that only exists when the subset holds the superset's
     * largest indices, and that a walk bounded by the last missing index never reaches.
     */
    @Test
    public void testLargeSubsetPresentIndexModeWithPresentTail() throws Throwable
    {
        int columnCount = 70;
        int[] present = { 0, 1, 2, 3, 4, 5, 6, 7, 68, 69 };

        assertEquals("the scenario must keep the superset's last column present, or there is no " +
                     "tail after the last missing index",
                     columnCount - 1, present[present.length - 1]);

        runLargeSubsetScenario(columnCount, present, true);
    }

    /**
     * Writes two rows into one partition — one holding every column of {@code columnCount}, one
     * holding only {@code presentIndices} — compacts them, and asserts the subset row still decodes
     * to exactly {@code presentIndices} with the values it was written with.
     *
     * @param presentIndexMode the mode the reference rule selects for this shape; asserted, so a
     *                         change to the superset size or the subset cannot silently move the
     *                         scenario onto the other mode
     */
    private void runLargeSubsetScenario(int columnCount, int[] presentIndices, boolean presentIndexMode) throws Throwable
    {
        int presentCount = presentIndices.length;
        assertTrue("the large-subset format starts at a 64-column superset; got " + columnCount,
                   columnCount >= 64);
        assertTrue("an all-present or all-missing row takes a fast path instead of the subset " +
                   "encoding; got " + presentCount + " of " + columnCount,
                   presentCount > 0 && presentCount < columnCount);
        assertEquals("the scenario no longer selects the large-subset mode it was written for",
                     presentIndexMode, presentCount < columnCount / 2);

        String[] names = columnNames(columnCount);
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : " +
                                         "'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        StringBuilder create = new StringBuilder("CREATE TABLE %s ( pk bigint, ck bigint");
        for (String name : names)
            create.append(", ").append(name).append(" bigint");
        create.append(", PRIMARY KEY(pk, ck))");
        String table = createTable(keyspace, create.toString());
        execute("use " + keyspace + ";");
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::disableAutoCompaction));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // ck 0 carries every column, and that is what puts all of them in the superset the writer
        // encodes against: a flushed sstable's serialization header lists the columns the memtable
        // actually saw (Flushing.createFlushWriter), and compaction's header is the union of the
        // input headers (SerializationHeader.make). Superset size selects both the wire format and
        // the mode, so a scenario that only ever wrote the subset's columns would encode against a
        // superset of presentCount and exercise nothing.
        int[] all = new int[columnCount];
        for (int i = 0; i < columnCount; i++)
            all[i] = i;
        insertRow(table, names, 0, all);
        insertRow(table, names, 1, presentIndices);

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        assertEquals("the scenario needs its two rows in one input sstable, so that the input " +
                     "header spans the whole superset", 1, cfs.getLiveSSTables().size());
        SSTableReader flushed = cfs.getLiveSSTables().iterator().next();
        assertSuperset(flushed, names);
        assertCursorPathWillRun(cfs);

        majorCompact(cfs);

        assertEquals("expected a single compaction output", 1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        // A major compaction that produced no task leaves the flushed sstable live, and every
        // assertion below would then pass against bytes the flush path wrote — the cursor writer
        // would never have run.
        assertNotEquals("compaction did not rewrite the input, so nothing below exercises the " +
                        "compaction writer", flushed.descriptor, sstable.descriptor);
        assertSuperset(sstable, names);

        try (ISSTableScanner scanner = sstable.getScanner())
        {
            assertTrue(scanner.hasNext());
            UnfilteredRowIterator partition = scanner.next();
            assertTrue(partition.partitionLevelDeletion().isLive());
            assertTrue(partition.staticRow().isEmpty());
            assertRowCells(partition, names, all);
            assertRowCells(partition, names, presentIndices);
            assertFalse(partition.hasNext());
            assertFalse(scanner.hasNext());
        }
        verifyAndPrint(cfs, sstable);
    }

    /**
     * Names for a {@code columnCount}-column superset, zero padded to a fixed width so that the name
     * order the superset is sorted in matches the numbering — which is what lets the scenarios state
     * their subsets as superset positions. {@link #assertSuperset} asserts that correspondence
     * against the sstable rather than leaving it assumed.
     */
    private static String[] columnNames(int columnCount)
    {
        String format = "c%0" + Integer.toString(columnCount - 1).length() + 'd';
        String[] names = new String[columnCount];
        for (int i = 0; i < columnCount; i++)
            names[i] = String.format(format, i);
        return names;
    }

    /**
     * Asserts the sstable's regular columns are exactly {@code names} in that order. This is the
     * superset the writer encodes subsets against and indexes positionally
     * ({@code SSTableCursorWriter} takes its column array from {@code header.columns(false)}), so it
     * pins both the superset size the mode rule is applied to and the position each name holds.
     */
    private static void assertSuperset(SSTableReader sstable, String[] names)
    {
        List<String> actual = new ArrayList<>(names.length);
        for (ColumnMetadata column : sstable.header.columns(false))
            actual.add(column.name.toString());
        assertEquals("the sstable's column superset is not the one the scenario states its subsets " +
                     "in terms of", Arrays.asList(names), actual);
    }

    private void insertRow(String table, String[] names, long ck, int[] presentIndices) throws Throwable
    {
        StringBuilder insert = new StringBuilder("INSERT INTO ").append(table).append(" (pk, ck");
        for (int i : presentIndices)
            insert.append(", ").append(names[i]);
        insert.append(") VALUES (?, ?");
        for (int i = 0; i < presentIndices.length; i++)
            insert.append(", ?");
        insert.append(") USING TIMESTAMP 1");

        Object[] values = new Object[2 + presentIndices.length];
        values[0] = 0L;
        values[1] = ck;
        // each cell carries its own superset position as its value, so a subset that decodes to the
        // wrong columns shows up as a name/value mismatch as well as a wrong column set
        for (int i = 0; i < presentIndices.length; i++)
            values[2 + i] = (long) presentIndices[i];
        execute(insert.toString(), values);
    }

    private static void assertRowCells(UnfilteredRowIterator partition, String[] names, int[] expectedIndices)
    {
        assertTrue("expected a further row", partition.hasNext());
        Unfiltered unfiltered = partition.next();
        assertTrue(unfiltered.isRow());
        Row row = (Row) unfiltered;
        assertTrue(row.deletion().time().isLive());

        List<String> expected = new ArrayList<>(expectedIndices.length);
        for (int i : expectedIndices)
            expected.add(names[i] + '=' + i);
        List<String> actual = new ArrayList<>(expectedIndices.length);
        for (Cell<?> cell : row.cells())
            actual.add(cell.column().name.toString() + '=' + value(cell));

        assertEquals("the row decoded to a different column subset than it was written with",
                     expected, actual);
    }

    /**
     * The cell's bigint value, or its width when it does not hold one — a subset that decodes to the
     * wrong columns also carves the row body up at the wrong offsets, so rendering the width keeps
     * the whole decoded subset in the comparison below instead of failing on the first bad cell.
     */
    private static String value(Cell<?> cell)
    {
        ByteBuffer buffer = cell.buffer();
        return buffer.remaining() == Long.BYTES ? Long.toString(ByteBufferUtil.toLong(buffer))
                                                : "<" + buffer.remaining() + " bytes>";
    }
}
