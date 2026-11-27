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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Cursor compaction that reads one sstable format and writes the other.
 *
 * <p>{@link org.apache.cassandra.db.compaction.CursorCompactor#isSupported} gates only on the selected
 * (output) format; it never inspects the input sstables' formats. Every other differential test sets the
 * selected format in {@code @Before} before flushing inputs, so inputs and output always share a format.
 * An operator can instead flush inputs, then change {@code sstable.selected_format}, then compact — so
 * BTI-in/BIG-out and BIG-in/BTI-out are real, otherwise-unexercised paths.
 *
 * <p>Each scenario flushes overlapping inputs under one format, switches the selected format, then compacts.
 * The differential tests assert the cursor and iterator paths agree byte-for-byte and that the output is the
 * newly selected format. They then commit the cursor compaction and re-read every merged partition through a
 * point read against hand-written expected values, which catches a conversion bug both paths share. The
 * key-cache test drives the BIG-output / BTI-input direction with key-cache migration on, so
 * {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}'s shouldCacheKey sees non-KeyCacheSupport
 * (BTI) originals.
 */
public class CrossFormatCursorCompactionTest extends DifferentialCompactionTester
{
    private SSTableFormat<?, ?> originalFormat;

    @Before
    public void rememberFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** Flushes two overlapping inputs under {@code inputFormat}, verifying they landed in that format. */
    private ColumnFamilyStore twoOverlappingInputs(String inputFormat) throws Exception
    {
        DatabaseDescriptor.setSelectedSSTableFormat(inputFormat);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // first sstable: pk 0..9
        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "v" + ck);
        flush();

        // second, newer sstable: pk 5..14 overwrites the overlap and adds new partitions
        for (long pk = 5; pk < 15; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck + 1000, "updated" + ck);
        flush();

        assertEquals("scenario needs two overlapping sstables to merge", 2, cfs.getLiveSSTables().size());
        for (SSTableReader in : cfs.getLiveSSTables())
            assertEquals("input sstable was not flushed in the intended format " + in.descriptor,
                         inputFormat, in.descriptor.getFormat().name());
        return cfs;
    }

    /**
     * BTI inputs, BIG output. Runs both compaction paths and asserts they agree byte-for-byte, and that the
     * output is BIG (the newly selected format), not a silent fall-through to the input's BTI format. Then
     * commits the cursor compaction and reads every merged row back.
     */
    @Test
    public void btiInputsBigOutputDifferential() throws Exception
    {
        ColumnFamilyStore cfs = twoOverlappingInputs("bti");
        DatabaseDescriptor.setSelectedSSTableFormat("big");

        CapturedOutput out = assertCursorMatchesIterator(cfs);
        assertEquals("expected a single merged output", 1, out.sstables.size());
        assertCursorOutputReadsBack(cfs, "big");
    }

    /**
     * BIG inputs, BTI output. Runs both compaction paths and asserts they agree byte-for-byte, and that the
     * output is BTI (the newly selected format), not a silent fall-through to the input's BIG format. Then
     * commits the cursor compaction and reads every merged row back.
     */
    @Test
    public void bigInputsBtiOutputDifferential() throws Exception
    {
        ColumnFamilyStore cfs = twoOverlappingInputs("big");
        DatabaseDescriptor.setSelectedSSTableFormat("bti");

        CapturedOutput out = assertCursorMatchesIterator(cfs);
        assertEquals("expected a single merged output", 1, out.sstables.size());
        assertCursorOutputReadsBack(cfs, "bti");
    }

    /**
     * Commits a cursor-only compaction of the inputs from {@link #twoOverlappingInputs}, asserts the output is
     * {@code outputFormat}, and reads every merged partition/row back through an absolute point read comparing
     * the exact merged values the two overlapping inputs imply.
     */
    private void assertCursorOutputReadsBack(ColumnFamilyStore cfs, String outputFormat) throws Exception
    {
        // expected merged state: pk 0..4 from first input, pk 5..14 from the newer input
        Map<Long, Long> expectedV1 = new HashMap<>();
        Map<Long, String> expectedV2 = new HashMap<>();
        for (long pk = 0; pk < 15; pk++)
            for (long ck = 0; ck < 20; ck++)
            {
                long key = pk * 100 + ck;
                if (pk >= 5)
                {
                    expectedV1.put(key, ck + 1000);
                    expectedV2.put(key, "updated" + ck);
                }
                else
                {
                    expectedV1.put(key, ck);
                    expectedV2.put(key, "v" + ck);
                }
            }

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);

        assertEquals("cursor compaction should have produced a single merged output",
                     1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();
        assertEquals("cursor output was written in the wrong format: expected the newly selected " +
                     outputFormat + " but got " + output.descriptor.getFormat().name() +
                     " (silent fall-through to the input format?)",
                     outputFormat, output.descriptor.getFormat().name());
        assertOutputFormatIsSelected(output);

        TableMetadata metadata = output.metadata();
        ColumnMetadata v1 = metadata.getColumn(ByteBuffer.wrap("v1".getBytes(StandardCharsets.UTF_8)));
        ColumnMetadata v2 = metadata.getColumn(ByteBuffer.wrap("v2".getBytes(StandardCharsets.UTF_8)));
        assertNotNull("schema is missing column v1", v1);
        assertNotNull("schema is missing column v2", v2);
        ColumnFilter fetchAll = ColumnFilter.all(metadata);

        int rowsChecked = 0;
        for (long pk = 0; pk < 16; pk++)
        {
            DecoratedKey key = cfs.decorateKey(LongType.instance.decompose(pk));
            List<Row> rows = readPartitionRows(output, key, fetchAll);

            if (pk >= 15)
            {
                assertTrue("point read of pk " + pk + " returned rows, but the merge holds no such " +
                           "partition in " + output.descriptor, rows.isEmpty());
                continue;
            }
            assertEquals("wrong row count for pk " + pk + " in merged " + outputFormat + " output", 20, rows.size());
            for (Row row : rows)
            {
                long ck = LongType.instance.compose(row.clustering().bufferAt(0));
                long ekey = pk * 100 + ck;
                Cell<?> cellV1 = row.getCell(v1);
                Cell<?> cellV2 = row.getCell(v2);
                assertNotNull("pk " + pk + " ck " + ck + ": v1 cell missing in merged " + outputFormat + " output", cellV1);
                assertNotNull("pk " + pk + " ck " + ck + ": v2 cell missing in merged " + outputFormat + " output", cellV2);
                assertEquals("pk " + pk + " ck " + ck + ": merged v1 wrong in " + outputFormat + " point read",
                             expectedV1.get(ekey), (Long) LongType.instance.compose(cellV1.buffer()));
                assertEquals("pk " + pk + " ck " + ck + ": merged v2 wrong in " + outputFormat + " point read",
                             expectedV2.get(ekey), UTF8Type.instance.compose(cellV2.buffer()));
                rowsChecked++;
            }
        }
        assertEquals("scenario must have read back every merged row", 15 * 20, rowsChecked);
    }

    /**
     * BIG output built from BTI (non-KeyCacheSupport) inputs, with key-cache migration ON. Exercises
     * {@link org.apache.cassandra.io.sstable.format.big.BigTableWriter}'s shouldCacheKey with originals that
     * are not KeyCacheSupport: it must not crash, must migrate nothing (BTI originals hold no cached
     * positions), and the BIG output must round-trip.
     */
    @Test
    public void bigOutputFromBtiInputsWithKeyCacheMigration() throws Exception
    {
        boolean originalMigrate = DatabaseDescriptor.shouldMigrateKeycacheOnCompaction();
        long originalKeyCacheCapacity = CacheService.instance.keyCache.getCapacity();
        try
        {
            DatabaseDescriptor.setSelectedSSTableFormat("bti");
            createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            int partitions = 100;
            String padding = "v".repeat(200);
            for (int round = 0; round < 2; round++)
            {
                for (int pk = 0; pk < partitions; pk++)
                    for (int ck = 0; ck < 8; ck++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck + round * 8, padding);
                flush();
            }
            assertTrue("fixture needs at least two BTI inputs", cfs.getLiveSSTables().size() >= 2);
            for (SSTableReader in : cfs.getLiveSSTables())
            {
                assertEquals("input must be BTI for this scenario", "bti", in.descriptor.getFormat().name());
                assertFalse("BTI inputs must not be KeyCacheSupport, which is the whole point of this scenario",
                            in instanceof KeyCacheSupport<?>);
            }

            // migration ON and an enabled key cache
            DatabaseDescriptor.setMigrateKeycacheOnCompaction(true);
            if (originalKeyCacheCapacity == 0)
                CacheService.instance.keyCache.setCapacity(1L << 20);
            CacheService.instance.invalidateKeyCache();

            // warm reads: for BTI inputs these cache nothing, but they are the reads shouldCacheKey would consult
            for (int pk = 0; pk < partitions; pk++)
                execute("SELECT * FROM %s WHERE pk = ?", pk);

            // now switch the OUTPUT to BIG and commit a cursor compaction
            DatabaseDescriptor.setSelectedSSTableFormat("big");
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);

            assertEquals("expected a single merged output", 1, cfs.getLiveSSTables().size());
            SSTableReader output = cfs.getLiveSSTables().iterator().next();
            assertEquals("output must be BIG", "big", output.descriptor.getFormat().name());
            assertOutputFormatIsSelected(output);

            // BTI originals held nothing in the key cache, so nothing should have migrated, but the compaction
            // must have completed without a crash and every partition must read back.
            int migrated = 0;
            ColumnFilter fetchAll = ColumnFilter.all(output.metadata());
            for (int pk = 0; pk < partitions; pk++)
            {
                DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(pk));
                if (((KeyCacheSupport<?>) output).getCachedPosition(key, false) != null)
                    migrated++;
                List<Row> rows = readPartitionRows(output, key, fetchAll);
                assertEquals("pk " + pk + " lost rows through the cross-format BIG output", 16, rows.size());
            }
            assertEquals("no BTI original is KeyCacheSupport, so shouldCacheKey must have migrated nothing",
                         0, migrated);
        }
        finally
        {
            DatabaseDescriptor.setMigrateKeycacheOnCompaction(originalMigrate);
            CacheService.instance.keyCache.setCapacity(originalKeyCacheCapacity);
            CacheService.instance.invalidateKeyCache();
        }
    }

    /** Pads each wide-partition value so one partition spans several row index blocks. */
    private static final String PAD = "p".repeat(200);
    private static final int WIDE_ROWS = 200;

    /**
     * BIG older input, BTI newer input, BIG output. The two inputs differ in format, so the merge reads both
     * formats' indexes in one compaction.
     */
    @Test
    public void mixedBigOlderBtiNewerBigOutput() throws Throwable
    {
        mixedInputsDifferential("big", "bti", "big");
    }

    /** BIG older input, BTI newer input, BTI output. */
    @Test
    public void mixedBigOlderBtiNewerBtiOutput() throws Throwable
    {
        mixedInputsDifferential("big", "bti", "bti");
    }

    /** BTI older input, BIG newer input, BIG output. */
    @Test
    public void mixedBtiOlderBigNewerBigOutput() throws Throwable
    {
        mixedInputsDifferential("bti", "big", "big");
    }

    /** BTI older input, BIG newer input, BTI output. */
    @Test
    public void mixedBtiOlderBigNewerBtiOutput() throws Throwable
    {
        mixedInputsDifferential("bti", "big", "bti");
    }

    /**
     * Flushes an older input in {@code olderFormat} and a newer one in {@code newerFormat}, then compacts both
     * into {@code outputFormat}. Asserts byte parity of the two paths, then commits the cursor compaction and
     * reads every merged partition back against values derived from the writes below.
     */
    private void mixedInputsDifferential(String olderFormat, String newerFormat, String outputFormat) throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, t text, tags set<text>, s text static, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        DatabaseDescriptor.setSelectedSSTableFormat(olderFormat);
        writeOlderInput();
        flush();
        DatabaseDescriptor.setSelectedSSTableFormat(newerFormat);
        writeNewerInput();
        flush();

        assertEquals("scenario needs two sstables to merge", 2, cfs.getLiveSSTables().size());
        List<SSTableReader> inputs = new ArrayList<>(cfs.getLiveSSTables());
        inputs.sort((a, b) -> Long.compare(a.getMaxTimestamp(), b.getMaxTimestamp()));
        assertEquals("older input is in the wrong format " + inputs.get(0).descriptor,
                     olderFormat, inputs.get(0).descriptor.getFormat().name());
        assertEquals("newer input is in the wrong format " + inputs.get(1).descriptor,
                     newerFormat, inputs.get(1).descriptor.getFormat().name());
        DecoratedKey wideKey = cfs.decorateKey(LongType.instance.decompose(0L));
        for (SSTableReader in : inputs)
            assertWidePartitionHasSeveralBlocks(in, wideKey);

        DatabaseDescriptor.setSelectedSSTableFormat(outputFormat);
        CapturedOutput out = assertCursorMatchesIterator(cfs);
        assertEquals("expected a single merged output", 1, out.sstables.size());

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);
        assertEquals("cursor compaction should have produced a single merged output",
                     1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();
        assertEquals("cursor output was written in the wrong format", outputFormat, output.descriptor.getFormat().name());
        assertOutputFormatIsSelected(output);
        assertWidePartitionHasSeveralBlocks(output, wideKey);

        assertMixedMergeReadsBack();
    }

    private static void assertWidePartitionHasSeveralBlocks(SSTableReader sstable, DecoratedKey key)
    {
        AbstractRowIndexEntry entry = sstable.getRowIndexEntry(key, SSTableReader.Operator.EQ);
        assertNotNull("wide partition missing from " + sstable.descriptor, entry);
        assertTrue("wide partition must span several row index blocks in " + sstable.descriptor +
                   " but has " + entry.blockCount(), entry.blockCount() > 1);
    }

    /** A tie at timestamp 1500 resolves to the larger value; the older input wins it on even ck. */
    private static String olderTieValue(long ck)
    {
        return (ck % 2 == 0 ? "y" : "x") + ck;
    }

    private static String newerTieValue(long ck)
    {
        return (ck % 2 == 0 ? "x" : "y") + ck;
    }

    private void writeOlderInput()
    {
        for (long ck = 0; ck < WIDE_ROWS; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?) USING TIMESTAMP 1000", ck, "a" + ck + PAD);
            execute("UPDATE %s USING TIMESTAMP 1000 SET tags = tags + ? WHERE pk = 0 AND ck = ?", Set.of("a" + ck), ck);
            if (ck < 40)
                execute("UPDATE %s USING TIMESTAMP 1500 SET t = ? WHERE pk = 0 AND ck = ?", olderTieValue(ck), ck);
        }
        execute("UPDATE %s USING TIMESTAMP 1000 SET s = 'sa' WHERE pk = 0");
        execute("DELETE FROM %s USING TIMESTAMP 1100 WHERE pk = 0 AND ck >= 150 AND ck < 160");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 0, 'a') USING TIMESTAMP 1000");
        execute("UPDATE %s USING TIMESTAMP 1500 SET s = 's-y' WHERE pk = 1");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 0, 'a') USING TIMESTAMP 1000");
        execute("UPDATE %s USING TIMESTAMP 1500 SET s = 's-x' WHERE pk = 2");
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (3, ?, 'a') USING TIMESTAMP 1000", ck);
        execute("INSERT INTO %s (pk, ck, v) VALUES (4, 0, 'late') USING TIMESTAMP 1300");
        for (long ck = 1; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (4, ?, 'a') USING TIMESTAMP 1000", ck);
    }

    private static boolean deletedByNewer(long ck)
    {
        return ck == 50 || (ck >= 100 && ck < 120);
    }

    private void writeNewerInput()
    {
        for (long ck = 0; ck < WIDE_ROWS; ck++)
        {
            if (deletedByNewer(ck))
                continue;
            if (ck % 2 == 0)
                execute("UPDATE %s USING TIMESTAMP 2000 SET v = ? WHERE pk = 0 AND ck = ?", "b" + ck + PAD, ck);
            if (ck % 5 == 0)
                execute("UPDATE %s USING TIMESTAMP 2000 SET tags = ? WHERE pk = 0 AND ck = ?", Set.of("c" + ck), ck);
            else
                execute("UPDATE %s USING TIMESTAMP 2000 SET tags = tags + ? WHERE pk = 0 AND ck = ?", Set.of("b" + ck), ck);
            if (ck < 40)
                execute("UPDATE %s USING TIMESTAMP 1500 SET t = ? WHERE pk = 0 AND ck = ?", newerTieValue(ck), ck);
        }
        execute("UPDATE %s USING TIMESTAMP 2000 SET s = 'sb' WHERE pk = 0");
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck = 50");
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck >= 100 AND ck < 120");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'b') USING TIMESTAMP 2000");
        execute("UPDATE %s USING TIMESTAMP 1500 SET s = 's-x' WHERE pk = 1");
        execute("UPDATE %s USING TIMESTAMP 1500 SET s = 's-y' WHERE pk = 2");
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 3");
        execute("DELETE FROM %s USING TIMESTAMP 1200 WHERE pk = 4");
        execute("INSERT INTO %s (pk, ck, v) VALUES (5, 0, 'b') USING TIMESTAMP 2000");
    }

    /** Reads the merged table, whose only live sstable is the cursor output, and checks every value. */
    private void assertMixedMergeReadsBack() throws Throwable
    {
        List<Object[]> wide = new ArrayList<>();
        for (long ck = 0; ck < WIDE_ROWS; ck++)
        {
            if (deletedByNewer(ck))
                continue;
            boolean underOlderRangeTombstone = ck >= 150 && ck < 160;
            String v;
            if (ck % 2 == 0)
                v = "b" + ck + PAD;
            else
                v = underOlderRangeTombstone ? null : "a" + ck + PAD;
            Set<String> tags = new HashSet<>();
            if (ck % 5 == 0)
            {
                tags.add("c" + ck);
            }
            else
            {
                if (!underOlderRangeTombstone)
                    tags.add("a" + ck);
                tags.add("b" + ck);
            }
            String t = ck < 40 ? "y" + ck : null;
            wide.add(row(0L, ck, "sb", v, t, tags));
        }
        assertRows(execute("SELECT pk, ck, s, v, t, tags FROM %s WHERE pk = 0"), wide.toArray(new Object[0][]));

        assertRows(execute("SELECT pk, ck, s, v FROM %s WHERE pk = 1"),
                   row(1L, 0L, "s-y", "a"),
                   row(1L, 1L, "s-y", "b"));
        assertRows(execute("SELECT pk, ck, s, v FROM %s WHERE pk = 2"),
                   row(2L, 0L, "s-y", "a"));
        assertEmpty(execute("SELECT * FROM %s WHERE pk = 3"));
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 4"),
                   row(4L, 0L, "late"));
        assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = 5"),
                   row(5L, 0L, "b"));
        assertRowCount(execute("SELECT DISTINCT pk FROM %s"), 5);
    }

    /** Reads one partition of {@code sstable} through a full-partition point read; returns its rows in order. */
    private static List<Row> readPartitionRows(SSTableReader sstable, DecoratedKey key, ColumnFilter fetchAll)
    {
        List<Row> rows = new ArrayList<>();
        try (UnfilteredRowIterator partition = sstable.rowIterator(key, Slices.ALL, fetchAll, false,
                                                                   SSTableReadsListener.NOOP_LISTENER))
        {
            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                if (unfiltered.isRow())
                    rows.add((Row) unfiltered);
            }
        }
        return rows;
    }
}
