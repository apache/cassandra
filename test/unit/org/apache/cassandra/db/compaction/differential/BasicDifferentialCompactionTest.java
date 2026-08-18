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


import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Smoke scenarios for the differential cursor-vs-iterator compaction harness.
 * The full edge-case corpus lives in EdgeCaseDifferentialCompactionTest; this class
 * exists to prove the harness mechanics on the simplest supported shapes.
 */
public class BasicDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** Start empty: byte-identical output is the hypothesis increment 1 tests. */

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
        // large gc_grace: nothing purgeable, all tombstone kinds must survive the merge identically
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "v" + ck);
        flush();

        // row deletes, cell deletes, range deletes, partition delete — one sstable of tombstones
        execute("DELETE FROM %s WHERE pk = 1 AND ck = 5");
        execute("DELETE v1 FROM %s WHERE pk = 2 AND ck = 7");
        execute("DELETE FROM %s WHERE pk = 3 AND ck >= 5 AND ck < 15");
        execute("DELETE FROM %s WHERE pk = 4");
        flush();

        // and a third sstable with data written over some of the deletions
        for (long ck = 0; ck < 20; ck += 2)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 3L, ck, ck + 100, "resurrect" + ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    @Test
    public void tombstonesPurged() throws Exception
    {
        // gc_grace 0: tombstones written below are purgeable by the time we compact;
        // partition 4 becomes fully empty and must be dropped by both paths
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

        // purge boundary: gcBefore strictly past the tombstones' recorded local deletion time,
        // read from the sstable's own stats instead of sleeping past wall-clock now
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

        // static-only partitions + static overwrite in second sstable
        for (long pk = 0; pk < 4; pk++)
            execute("INSERT INTO %s (pk, s1, s2) VALUES (?, ?, ?)", pk, pk * 100, "updated" + pk);
        execute("INSERT INTO %s (pk, s1) VALUES (?, ?)", 100L, 1L);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * A table with no clustering columns, which CursorSupportMatrixTest.noClusteringSupported
     * declares supported without ever compacting one. Every row is the empty clustering, so each
     * partition holds exactly one row and the merge decides on the partition key alone.
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

        // pk 10..19 are overwritten and pk 20..29 are new, so the output is a genuine merge
        for (long pk = 10; pk < 30; pk++)
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, pk + 100, "updated" + pk);
        flush();

        assertEquals("the scenario needs two overlapping sstables to merge", 2, cfs.getLiveSSTables().size());
        assertCursorMatchesIterator(cfs);
    }
}
