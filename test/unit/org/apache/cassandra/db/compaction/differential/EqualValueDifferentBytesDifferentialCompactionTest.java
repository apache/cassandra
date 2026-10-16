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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionTask;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Sources that hold the same row, cell path or range tombstone bound with values that compare
 * equal but are written with different bytes: decimal 1.0 and 1.00, varint with and without
 * leading zero bytes, boolean true as 0x01 and 0x02.  The merged output keeps the bytes of one
 * source, and which one depends on the order the merge gives equal items in.  The iterator path
 * keeps the clustering of the last row and of the last marker it is given, and the first of two
 * cells that tie on everything.  The cursor path must keep the same source's bytes, so both
 * outputs must be byte-identical.
 *
 * <p>Timestamps are fixed so that most cells and rows tie.  Each seed writes a different mix of
 * partitions, rows and range deletions to each sstable, so the merge order of equal items changes
 * from one partition and row to the next.
 */
@RunWith(Parameterized.class)
public class EqualValueDifferentBytesDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final long BASE_TIMESTAMP = 1_000_000L;
    private static final int PARTITIONS = 24;
    private static final int SEEDS = 6;

    @Parameterized.Parameter(0)
    public String format;

    @Parameterized.Parameter(1)
    public int sstableCount;

    @Parameterized.Parameters(name = "{0}-{1}sstables")
    public static List<Object[]> parameters()
    {
        List<Object[]> params = new ArrayList<>();
        for (String format : new String[]{ "big", "bti" })
            for (int sstables : new int[]{ 2, 5, 9 })
                params.add(new Object[]{ format, sstables });
        return params;
    }

    @Before
    public void selectFormat()
    {
        selectSSTableFormat(format);
    }

    @After
    public void restoreFormat()
    {
        restoreSelectedFormat();
    }

    /** One row, written to every sstable with the same timestamp, each time with different clustering bytes. */
    @Test
    public void sameRowInEverySSTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, c1 decimal, c2 varint, c3 boolean, v int, PRIMARY KEY (pk, c1, c2, c3))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 0; i < sstableCount; i++)
        {
            execute("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                    1, decimal(1, i), varint(1, i), bool(true, i + 1), 7, BASE_TIMESTAMP);
            flush();
        }
        assertEquals(sstableCount, cfs.getLiveSSTables().size());
        assertCursorMatchesIterator(cfs);
    }

    /** A set element written to every sstable with the same timestamp, each time with different path bytes. */
    @Test
    public void sameCellPathInEverySSTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, s set<decimal>, m map<decimal, int>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 0; i < sstableCount; i++)
        {
            execute("UPDATE %s USING TIMESTAMP ? SET s = s + ?, m = m + ? WHERE pk = ? AND ck = ?",
                    BASE_TIMESTAMP, Collections.singleton(decimalValue(2, i)),
                    Collections.singletonMap(decimalValue(3, i), 9), 1, 1);
            flush();
        }
        assertCursorMatchesIterator(cfs);
    }

    /**
     * Sources with no static cells: in each partition one source holds an empty static row, and
     * the first sstable predates the static column, so its header has no static columns.  The
     * others hold the same static set element with other bytes.  The iterator path still gives
     * every source to its static column merge, as an empty row, and removing one moves the last
     * source up.
     */
    @Test
    public void staticRowMissingFromSomeSources() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < sstableCount * 4; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, 1, 1, BASE_TIMESTAMP);
        flush();
        execute("ALTER TABLE %s ADD st set<decimal> static");
        for (int i = 0; i < sstableCount; i++)
        {
            for (int pk = 0; pk < sstableCount * 4; pk++)
            {
                if (pk % sstableCount == i)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, 0, 1, BASE_TIMESTAMP);
                else
                    execute("UPDATE %s USING TIMESTAMP ? SET st = st + ? WHERE pk = ?",
                            BASE_TIMESTAMP, Collections.singleton(decimalValue(1, (i + pk) % 4)), pk);
            }
            flush();
        }
        assertCursorMatchesIterator(cfs);
    }

    /**
     * In each row one source has the row with no cells, and the others hold the same set element
     * with other bytes.  The iterator path gives the empty row to its column merge, and removing it
     * moves the last source up.
     */
    @Test
    public void rowWithoutCellsInOneSource() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, s set<decimal>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 0; i < sstableCount; i++)
        {
            for (int pk = 0; pk < sstableCount * 4; pk++)
            {
                if (pk % sstableCount == i)
                    execute("INSERT INTO %s (pk, ck) VALUES (?, ?) USING TIMESTAMP ?", pk, 0, BASE_TIMESTAMP);
                else
                    execute("UPDATE %s USING TIMESTAMP ? SET s = s + ? WHERE pk = ? AND ck = ?",
                            BASE_TIMESTAMP, Collections.singleton(decimalValue(1, (i + pk) % 4)), pk, 0);
            }
            flush();
        }
        assertCursorMatchesIterator(cfs);
    }

    /**
     * The first sstable holds a deletion of a set column and no cells, written before the column
     * was dropped and added again.  The read drops that deletion, so the iterator path gives that
     * sstable's row no such column.  The other sstables hold the same set element with other bytes,
     * written after the column came back.
     */
    @Test
    public void droppedComplexDeletionOfReaddedColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, s set<decimal>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int pk = 0; pk < sstableCount * 4; pk++)
            execute("UPDATE %s USING TIMESTAMP ? SET v = ?, s = {} WHERE pk = ? AND ck = ?", BASE_TIMESTAMP, 1, pk, 0);
        flush();
        execute("ALTER TABLE %s DROP s");
        execute("ALTER TABLE %s ADD s set<decimal>");
        // after the drop time, so the re-added column keeps these cells
        long afterDrop = FBUtilities.timestampMicros() + 60_000_000L;
        for (int i = 0; i < sstableCount; i++)
        {
            for (int pk = 0; pk < sstableCount * 4; pk++)
                if (pk % sstableCount != i)
                    execute("UPDATE %s USING TIMESTAMP ? SET s = s + ? WHERE pk = ? AND ck = ?",
                            afterDrop, Collections.singleton(decimalValue(1, (i + pk) % 4)), pk, 0);
            flush();
        }
        assertCursorMatchesIterator(cfs);
    }

    /** A range deletion written to every sstable with the same timestamp, each time with different bound bytes. */
    @Test
    public void sameRangeDeletionInEverySSTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, c1 decimal, c2 int, v int, PRIMARY KEY (pk, c1, c2)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int i = 0; i < sstableCount; i++)
        {
            execute("INSERT INTO %s (pk, c1, c2, v) VALUES (?, ?, ?, ?) USING TIMESTAMP ?",
                    1, decimal(0, i), 0, 1, BASE_TIMESTAMP + 10);
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND c1 >= ? AND c1 < ?",
                    BASE_TIMESTAMP, 1, decimal(1, i), decimal(3, i));
            flush();
        }
        assertCursorMatchesIterator(cfs);
    }

    /** Seeded mix of rows, static and regular collections and range deletions over many partitions. */
    @Test
    public void seededMix() throws Exception
    {
        for (int seed = 0; seed < SEEDS; seed++)
        {
            long fullSeed = 0x5EED_0000L + seed * 31L + sstableCount * 1009L + format.hashCode();
            logger.info("seededMix format={} sstables={} seed={}", format, sstableCount, fullSeed);
            createTable("CREATE TABLE %s (pk int, c1 decimal, c2 varint, c3 boolean, v int, " +
                        "s set<decimal>, m map<decimal, int>, st set<decimal> static, " +
                        "PRIMARY KEY (pk, c1, c2, c3)) WITH gc_grace_seconds = 864000");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            writeSeededMix(new Random(fullSeed));
            assertEquals(sstableCount, cfs.getLiveSSTables().size());
            assertCursorMatchesIterator(cfs);
        }
    }

    /**
     * Level 1 sstables reach the merge through one leveled scanner, which the iterator path merges
     * as a single source.  Level 0 sstables on top hold the same rows with other bytes.
     */
    @Test
    public void leveledScannerAsOneSource() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, c1 decimal, v text, s set<decimal>, PRIMARY KEY (pk, c1)) " +
                    "WITH compaction = {'class': 'LeveledCompactionStrategy'} AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String padding = "x".repeat(100);
        Random random = new Random(0x1E7E1L + sstableCount);
        for (int pk = 0; pk < 40; pk++)
            for (int c = 0; c < 10; c++)
                execute("UPDATE %s USING TIMESTAMP ? SET v = ?, s = s + ? WHERE pk = ? AND c1 = ?",
                        BASE_TIMESTAMP, padding, Collections.singleton(decimalValue(c % 3, random.nextInt(4))), pk, decimal(c, random.nextInt(4)));
        flush();
        commitThroughFactory(cfs, false, (c, txn, gcBefore) -> new LeveledCompactionTask(c, txn, 1, gcBefore, 8 * 1024, false));
        assertLeveledScannerHoldsSeveralSSTables(cfs);

        for (int sstable = 0; sstable < sstableCount; sstable++)
        {
            for (int pk = 0; pk < 40; pk++)
                for (int c = 0; c < 10; c++)
                    if (random.nextInt(10) < 3)
                        execute("UPDATE %s USING TIMESTAMP ? SET v = ?, s = s + ? WHERE pk = ? AND c1 = ?",
                                BASE_TIMESTAMP, padding, Collections.singleton(decimalValue(c % 3, random.nextInt(4))), pk, decimal(c, random.nextInt(4)));
            flush();
        }
        assertCursorMatchesIterator(cfs);
        assertLeveledScannerHoldsSeveralSSTables(cfs);
    }

    private static void assertLeveledScannerHoldsSeveralSSTables(ColumnFamilyStore cfs)
    {
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(cfs.getLiveSSTables()))
        {
            int most = 0;
            for (ISSTableScanner scanner : scanners.scanners)
                most = Math.max(most, scanner.getBackingSSTables().size());
            assertTrue("no scanner reads more than one sstable", most > 1);
        }
    }

    private void writeSeededMix(Random random)
    {
        for (int sstable = 0; sstable < sstableCount; sstable++)
        {
            for (int pk = 0; pk < PARTITIONS; pk++)
            {
                if (random.nextInt(10) < 2)
                    continue;

                if (random.nextBoolean())
                    execute("UPDATE %s USING TIMESTAMP ? SET st = st + ? WHERE pk = ?",
                            timestamp(random), Collections.singleton(decimalValue(random.nextInt(3), random.nextInt(4))), pk);

                for (int a = 0; a < 4; a++)
                    for (int b = 0; b < 2; b++)
                        for (int c = 0; c < 2; c++)
                            if (random.nextInt(10) < 6)
                                writeRow(random, pk, a, b, c == 1);

                if (random.nextInt(10) < 3)
                {
                    int from = random.nextInt(3);
                    execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND c1 >= ? AND c1 < ?",
                            random.nextBoolean() ? BASE_TIMESTAMP - 1 : BASE_TIMESTAMP + 1, pk,
                            decimal(from, random.nextInt(4)), decimal(from + 1 + random.nextInt(2), random.nextInt(4)));
                }
            }
            flush();
        }
    }

    private void writeRow(Random random, int pk, int a, int b, boolean c)
    {
        Object c1 = decimal(a, random.nextInt(4));
        Object c2 = varint(b, random.nextInt(3));
        Object c3 = bool(c, 1 + random.nextInt(3));
        long timestamp = timestamp(random);
        execute("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?",
                pk, c1, c2, c3, random.nextInt(10) < 8 ? 5 : random.nextInt(3), timestamp);
        if (random.nextBoolean())
        {
            Map<BigDecimal, Integer> m = new HashMap<>();
            m.put(decimalValue(random.nextInt(3), random.nextInt(4)), random.nextInt(10) < 8 ? 1 : random.nextInt(3));
            execute("UPDATE %s USING TIMESTAMP ? SET s = s + ?, m = m + ? WHERE pk = ? AND c1 = ? AND c2 = ? AND c3 = ?",
                    timestamp(random), Collections.singleton(decimalValue(random.nextInt(3), random.nextInt(4))), m,
                    pk, c1, c2, c3);
        }
    }

    /** Mostly the same timestamp, so most rows and cells tie. */
    private static long timestamp(Random random)
    {
        return random.nextInt(10) < 7 ? BASE_TIMESTAMP : BASE_TIMESTAMP + random.nextInt(3);
    }

    /** {@code value} with {@code scale} digits after the point: 1, 1.0, 1.00 and so on. */
    private static BigDecimal decimalValue(int value, int scale)
    {
        return new BigDecimal(BigInteger.valueOf(value).multiply(BigInteger.TEN.pow(scale)), scale);
    }

    private static ByteBuffer decimal(int value, int scale)
    {
        return DecimalType.instance.decompose(decimalValue(value, scale));
    }

    /** A non-negative varint with {@code padding} extra leading zero bytes. */
    private static ByteBuffer varint(int value, int padding)
    {
        byte[] minimal = BigInteger.valueOf(value).toByteArray();
        byte[] padded = new byte[minimal.length + padding];
        System.arraycopy(minimal, 0, padded, padding, minimal.length);
        return ByteBuffer.wrap(padded);
    }

    /** False is 0x00; true is {@code trueByte}, any non-zero byte. */
    private static ByteBuffer bool(boolean value, int trueByte)
    {
        return ByteBuffer.wrap(new byte[]{ value ? (byte) trueByte : 0 });
    }
}
