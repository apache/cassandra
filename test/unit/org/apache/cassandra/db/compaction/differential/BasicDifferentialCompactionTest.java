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
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

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
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Smoke scenarios for the differential cursor-vs-iterator compaction harness, over the
 * simplest supported table shapes. The edge-case corpus lives in
 * EdgeCaseDifferentialCompactionTest.
 */
public class BasicDifferentialCompactionTest extends DifferentialCompactionTester
{
    @Test
    public void overlappingOverwrites() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 10; pk++)
                for (long ck = 0; ck < 20; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)",
                            pk, ck, round * 1000 + ck, "round-" + round + "-" + ck);
            flush();
        }

        assertCursorMatchesIterator(cfs);
    }

    @Test
    public void tombstonesRetained() throws Exception
    {
        // large gc_grace: nothing written below is purgeable at compaction time
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "v" + ck);
        flush();

        // one sstable holding every tombstone kind
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 5");
        execute("DELETE v1 FROM %s WHERE pk = 2 AND ck = 7");
        execute("DELETE FROM %s WHERE pk = 3 AND ck >= 5 AND ck < 15");
        execute("DELETE FROM %s WHERE pk = 4");
        flush();

        // third sstable: writes that land inside the pk 3 range deletion
        for (long ck = 0; ck < 20; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 3L, ck, ck + 100, "resurrect" + ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    @Test
    public void tombstonesPurged() throws Exception
    {
        // gc_grace 0: every tombstone written below is purgeable at compaction time
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 0");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 6; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
        flush();

        execute("DELETE FROM %s WHERE pk = 4");
        execute("DELETE FROM %s WHERE pk = 5 AND ck >= 0 AND ck < 5");
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 0");
        flush();

        // purge boundary: gcBefore goes strictly past the tombstones' local deletion time.
        // The test reads that time from the sstable stats instead of sleeping past wall-clock now.
        long maxLdt = maxTombstoneLocalDeletionTime(cfs.getLiveSSTables());

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK, maxLdt + 1);

        // ABSOLUTE, and this scenario's non-vacuity guard: deriving gcBefore from the sstable stats makes
        // everything above purgeable, so the merge must actually DROP it. Byte-equality cannot see a purge
        // rule both paths get wrong, and maxTombstoneLocalDeletionTime only establishes that a tombstone
        // existed to purge, not that it went. Surviving rows: pk 0 loses ck 0 (9), pk 1..3 keep all 10,
        // pk 4 is dropped whole (0), pk 5 loses ck 0-4 (5).
        assertEquals("expected a single compaction output", 1, out.sstables.size());
        assertTrue("purgeable tombstones and the data they shadow were not dropped; expected totalRows=44, " +
                   "got: " + out.sstables.get(0).statsSummary,
                   out.sstables.get(0).statsSummary.contains("totalRows=44 "));
    }

    /** Same as tombstonesRetained but uncompressed, so Data.db bytes are directly comparable. */
    @Test
    public void tombstonesRetainedUncompressed() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000 AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "v" + ck);
        flush();

        execute("DELETE FROM %s WHERE pk = 1 AND ck = 5");
        execute("DELETE v1 FROM %s WHERE pk = 2 AND ck = 7");
        execute("DELETE FROM %s WHERE pk = 3 AND ck >= 5 AND ck < 15");
        execute("DELETE FROM %s WHERE pk = 4");
        flush();

        for (long ck = 0; ck < 20; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 3L, ck, ck + 100, "resurrect" + ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    @Test
    public void staticRows() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s1 bigint static, s2 text static, ck bigint, v1 bigint, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 8; pk++)
        {
            execute("INSERT INTO %s (pk, s1, s2, ck, v1) VALUES (?, ?, ?, ?, ?)", pk, pk * 10, "static" + pk, 0L, 0L);
            for (long ck = 1; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
        }
        flush();

        // second sstable: static overwrites, plus a partition that holds only a static row
        for (long pk = 0; pk < 4; pk++)
            execute("INSERT INTO %s (pk, s1, s2) VALUES (?, ?, ?)", pk, pk * 100, "updated" + pk);
        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 100L, 1L);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Compacts a table with no clustering columns. CursorSupportMatrixTest.noClusteringSupported
     * declares that shape supported, but never compacts one. Every partition holds a single row
     * at the empty clustering, so the merge decides on the partition key alone.
     */
    @Test
    public void noClusteringColumns() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v1 bigint, v2 text)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 20; pk++)
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk, "v" + pk);
        flush();

        // the overlap at pk 10..19 makes the output a genuine merge
        for (long pk = 10; pk < 30; pk++)
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk + 100, "updated" + pk);
        flush();

        assertEquals("the scenario needs two overlapping sstables to merge", 2, cfs.getLiveSSTables().size());
        assertCursorMatchesIterator(cfs);
    }

    /**
     * The differential harness's slice read-back returns early for a zero-clustering table, because its index
     * has no seek to route. This test compacts a clustering-free table through the cursor path, then reads every
     * partition of the merged output back through an absolute point read and asserts each merged value.
     */
    @Test
    public void clusteringFreeReadBack() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v1 bigint, v2 text)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk -> {v1, v2}; a missing key means the partition must not exist
        Map<Long, Object[]> expected = new HashMap<>();

        // first sstable: pk 0..19
        for (long pk = 0; pk < 20; pk++)
        {
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk, "v" + pk);
            expected.put(pk, new Object[]{ pk, "v" + pk });
        }
        flush();

        // second, newer sstable: pk 10..29 overwrites the overlap and adds pk 20..29
        for (long pk = 10; pk < 30; pk++)
        {
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk + 100, "updated" + pk);
            expected.put(pk, new Object[]{ pk + 100, "updated" + pk });
        }
        flush();

        assertEquals("the scenario needs two overlapping sstables to merge", 2, cfs.getLiveSSTables().size());

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore);

        assertEquals("cursor compaction should have produced a single output sstable",
                     1, cfs.getLiveSSTables().size());
        SSTableReader out = cfs.getLiveSSTables().iterator().next();
        assertOutputFormatIsSelected(out);

        TableMetadata metadata = out.metadata();
        ColumnMetadata v1 = metadata.getColumn(ByteBuffer.wrap("v1".getBytes(StandardCharsets.UTF_8)));
        ColumnMetadata v2 = metadata.getColumn(ByteBuffer.wrap("v2".getBytes(StandardCharsets.UTF_8)));
        assertNotNull("schema is missing column v1", v1);
        assertNotNull("schema is missing column v2", v2);

        ColumnFilter fetchAll = ColumnFilter.all(metadata);

        // point read every key that could exist, including the two just past the end that must not
        for (long pk = 0; pk < 32; pk++)
        {
            DecoratedKey key = cfs.decorateKey(LongType.instance.decompose(pk));
            Row row = readOnlyRow(out, key, fetchAll);
            Object[] want = expected.get(pk);

            if (want == null)
            {
                assertNull("point read of pk " + pk + " returned a row, but the merge should hold no such partition " +
                           "in " + out.descriptor, row);
                continue;
            }

            assertNotNull("point read of pk " + pk + " returned no row from the merged output " + out.descriptor, row);

            Cell<?> cellV1 = row.getCell(v1);
            Cell<?> cellV2 = row.getCell(v2);
            assertNotNull("pk " + pk + ": v1 cell missing in merged output", cellV1);
            assertNotNull("pk " + pk + ": v2 cell missing in merged output", cellV2);

            assertEquals("pk " + pk + ": merged v1 wrong in point read", want[0], LongType.instance.compose(cellV1.buffer()));
            assertEquals("pk " + pk + ": merged v2 wrong in point read", want[1], UTF8Type.instance.compose(cellV2.buffer()));
        }
    }

    /** Reads exactly one partition of {@code sstable} through a full-partition point read; returns its lone row or null. */
    private static Row readOnlyRow(SSTableReader sstable, DecoratedKey key, ColumnFilter fetchAll)
    {
        try (UnfilteredRowIterator partition = sstable.rowIterator(key, Slices.ALL, fetchAll, false,
                                                                   SSTableReadsListener.NOOP_LISTENER))
        {
            Row found = null;
            int rows = 0;
            while (partition.hasNext())
            {
                Unfiltered unfiltered = partition.next();
                if (!unfiltered.isRow())
                    continue;
                rows++;
                found = (Row) unfiltered;
            }
            assertEquals("a clustering-free partition must hold exactly one row on a hit; pk read " + key +
                         " in " + sstable.descriptor, found == null ? 0 : 1, rows);
            return found;
        }
    }
}
