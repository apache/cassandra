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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Edge-case corpus for the differential cursor-vs-iterator compaction harness, covering the
 * CURRENTLY SUPPORTED cursor compaction surface (see CursorCompactor.isSupported). Every
 * scenario here must run the cursor path for real — the harness fails loudly on silent
 * fallback. Scenarios for unsupported shapes belong in CursorSupportMatrixTest instead.
 *
 * Every scenario runs the differential at TWO generations (see
 * assertCursorMatchesIteratorAcrossGenerations): gen 2 re-reads genuinely cursor-produced
 * outputs, so every scenario is also decoded from input shapes only a compaction produces. It is
 * not a write-side backstop; gen 1's byte comparison is.
 */
public class EdgeCaseDifferentialCompactionTest extends DifferentialCompactionTester
{

    /**
     * Static-column table where some partitions have NO static values: an empty static row is
     * written for those partitions but must not be counted in stats (totalRows/totalColumnsSet).
     * Found by the randomized soak; the original staticRows scenario gave every
     * partition static data and so never produced an empty static row.
     */
    @Test
    public void emptyStaticRows() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s1 text static, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 8; pk++)
            {
                // only even partitions ever get a static value
                if (pk % 2 == 0)
                    execute("INSERT INTO %s (pk, s1, ck, v) VALUES (?, ?, ?, ?)", pk, "static" + pk, (long) round, "v" + round);
                else
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, (long) round, "v" + round);
            }
            flush();
        }

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE: the empty static rows written for the four odd partitions must not be counted.
        // 8 partitions x 2 clusterings = 16 regular rows, plus one NON-empty static row for each of
        // the 4 even partitions = 20. statsSummary carries totalRows, so an over-count shared by both
        // paths would pass byte-equality and fail here.
        assertEquals("expected a single compaction output", 1, out.sstables.size());
        assertTrue("an absent static row was counted: expected totalRows=20 (16 regular + 4 " +
                   "non-empty static), got: " + out.sstables.get(0).statsSummary,
                   out.sstables.get(0).statsSummary.contains("totalRows=20 "));
        // totalColumnsSet is unaffected by that defect — an empty static row contributes zero columns —
        // so pinning it keeps that half of the claim honest: 16 regular v cells + 4 static s1 cells.
        assertTrue("expected totalColumnsSet=20, got: " + out.sstables.get(0).statsSummary,
                   out.sstables.get(0).statsSummary.contains("totalColumnsSet=20 "));
    }

    /** Reversed clustering order changes on-disk ordering and bound comparisons. */
    @Test
    public void descendingClustering() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 5; pk++)
                for (long ck = 0; ck < 30; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round + "v" + ck);
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", (long) round, 5L, 15L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Multi-component clusterings: mixed types, shared prefixes, per-component bounds. */
    @Test
    public void compositeClustering() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck1 text, ck2 int, ck3 bigint, v text, " +
                    "PRIMARY KEY (pk, ck1, ck2, ck3))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String[] names = { "alpha", "beta", "gamma", "" /* empty string component */ };
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 4; pk++)
                for (String ck1 : names)
                    for (int ck2 = 0; ck2 < 5; ck2++)
                        execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (?, ?, ?, ?, ?)",
                                pk, ck1, ck2, (long) round, "v" + round);
            // prefix range delete: full ck1, partial (ck1, ck2) prefix
            execute("DELETE FROM %s WHERE pk = ? AND ck1 = ?", (long) round, "beta");
            execute("DELETE FROM %s WHERE pk = ? AND ck1 = ? AND ck2 >= ? AND ck2 < ?",
                    (long) round, "gamma", 1, 4);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Wide partition crossing column-index block boundaries (indexed RowIndexEntry path). */
    @Test
    public void widePartitionCrossingIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        // two sstables, each with the same single wide partition (~4000 rows * ~200B >> 64KiB index block)
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 4000; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, padding + "-" + round + "-" + ck);
            // plus range tombstones inside the wide partition
            execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, round * 500L, round * 500L + 250L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Partition that crosses the column-index block threshold exactly once: the index has one
     * cut block plus a tail. Iterator promotes the index (2 entries); exercises the cursor's
     * promotion decision boundary (rowIndexEntriesOffsets.size() <= 1 check happens before the
     * tail block is added).
     */
    @Test
    public void partitionCrossingOneIndexBlock() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        // ~6KB partition: crosses the test config's column_index_size (4KiB) exactly once,
        // producing one cut block plus a tail — the index promotion boundary
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 30; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, padding + "-" + round);
            // pk 2 stays well UNDER the threshold: the sub-threshold control, without which this
            // scenario would pass on "large partitions get an index" rather than on the boundary
            for (long ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, ck, "small-" + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE: the cross-generation rung leaves the CURSOR-produced output live, so the promoted
        // index can be read back directly. The iterator promotes when the total block count INCLUDING
        // the tail exceeds one (RowIndexEntry.create); a merge that decides before counting the tail
        // leaves a partition crossing the threshold exactly once with no promoted index at all, and no
        // intra-partition seeks. Byte-equality pins this via Index.db only while the reference stays
        // correct, so the promotion is stated here directly.
        assertEquals("the cross-generation rung should leave one cursor-produced output",
                     1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();
        assertEquals("a partition crossing column_index_size exactly once must be promoted with the " +
                     "cut block AND its tail", 2, blockCount(output, 1L));
        assertEquals("a partition well under column_index_size must not be promoted", 0,
                     blockCount(output, 2L));
    }

    /** Promoted index block count for {@code pk} in {@code sstable}; 0 when the partition is not indexed. */
    private static int blockCount(SSTableReader sstable, long pk)
    {
        RowIndexEntry entry = ((BigTableReader) sstable).getRowIndexEntry(sstable.decorateKey(ByteBufferUtil.bytes(pk)),
                                                                         SSTableReader.Operator.EQ);
        assertNotNull("expected pk " + pk + " to be present in " + sstable.descriptor, entry);
        return entry.blockCount();
    }

    /** Overlapping range tombstones across sstables: boundary markers must merge identically. */
    @Test
    public void overlappingRangeTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
            for (long ck = 0; ck < 100; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        flush();

        // sstable 2: ranges [10, 50), [60, 70]
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 10 AND ck < 50");
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 60 AND ck <= 70");
        flush();

        // sstable 3: ranges overlapping/adjacent to sstable 2's: [30, 65), (70, 80]
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 30 AND ck < 65");
        execute("DELETE FROM %s WHERE pk = 0 AND ck > 70 AND ck <= 80");
        // and exact adjacency in another partition: [10,20) then [20,30)
        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 10 AND ck < 20");
        flush();

        execute("DELETE FROM %s WHERE pk = 1 AND ck >= 20 AND ck < 30");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Frozen collections and tuples are single cells and inside the supported surface. */
    @Test
    public void frozenCollections() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, " +
                    "m frozen<map<text, bigint>>, l frozen<list<text>>, s frozen<set<int>>, " +
                    "t frozen<tuple<int, text>>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 5; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, m, l, s, t) VALUES (?, ?, ?, ?, ?, (?, ?))",
                            pk, ck,
                            map("k" + round, ck, "x", (long) round),
                            list("a" + round, "b" + ck),
                            set((int) ck, round, 42),
                            round, "tup" + ck);
            execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 0L, (long) round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Partition keys larger than 128 bytes: every other scenario in this suite uses an 8-byte
     * bigint pk, which never exercises anything beyond the smallest possible key. Partition keys
     * are length-prefixed with an unsigned SHORT (not a vint), so there is no 128-byte encoding
     * boundary here the way there is for clustering/cell values — this scenario instead pins
     * that large (100s-1000s of bytes) partition keys round-trip correctly through the cursor's
     * partition-key copy/compare/index paths, which nothing else in this suite reaches.
     */
    @Test
    public void largePartitionKey() throws Exception
    {
        createTable("CREATE TABLE %s (pk text, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String[] pks = { "k".repeat(200), "m".repeat(500), "z".repeat(1000) };
        for (int round = 0; round < 3; round++)
        {
            for (String pk : pks)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "round" + round + "-" + ck);
            flush();
        }
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", pks[1], 2L, 5L);
        flush();

        assertEquals("expected four overlapping inputs; a lost flush() would degrade this to a single-sstable rewrite",
                     4, cfs.getLiveSSTables().size());
        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);
        // and that the keys really are large: shrinking pks would leave the scenario passing while
        // its name and javadoc still claimed 100s-1000s of bytes
        assertTrue("the 1000-byte partition key is not in the output",
                   allJson(out).contains("z".repeat(1000)));
    }

    /** Composite partition key whose components individually stay small but sum past 128 bytes. */
    @Test
    public void largeCompositePartitionKey() throws Exception
    {
        createTable("CREATE TABLE %s (pk1 text, pk2 text, ck bigint, v text, PRIMARY KEY ((pk1, pk2), ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String a = "a".repeat(100);
        String b = "b".repeat(100);
        for (int round = 0; round < 3; round++)
        {
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk1, pk2, ck, v) VALUES (?, ?, ?, ?)", a, b, ck, "v" + round + "-" + ck);
            flush();
        }
        execute("DELETE FROM %s WHERE pk1 = ? AND pk2 = ? AND ck >= ? AND ck < ?", a, b, 2L, 6L);
        flush();

        assertEquals("expected four overlapping inputs; a lost flush() would degrade this to a single-sstable rewrite",
                     4, cfs.getLiveSSTables().size());
        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);
        assertTrue("the 100-byte composite key components are not in the output",
                   allJson(out).contains(a) && allJson(out).contains(b));
    }

    /**
     * Clustering column values straddling the 1-byte/2-byte vint length-prefix boundary (128
     * bytes) — timestampTiesDifferentLengthValues below pins this boundary for regular VALUES,
     * but the clustering block's own per-component length vints (readUnfilteredClustering) are
     * never exercised at the boundary anywhere else in this suite.
     */
    @Test
    public void largeClusteringColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck text, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // 127/128/129: astride the one-byte/two-byte vint length-prefix boundary; 300: well past it
        String[] cks = { "a".repeat(127), "b".repeat(128), "c".repeat(129), "d".repeat(300) };
        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 3; pk++)
                for (String ck : cks)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + round);
            flush();
        }
        execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", cks[1]);
        flush();

        assertEquals("expected four overlapping inputs; a lost flush() would degrade this to a single-sstable rewrite",
                     4, cfs.getLiveSSTables().size());
        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);
        // the boundary value specifically: 128 bytes is the 1-byte/2-byte vint length-prefix step
        assertTrue("the 128-byte clustering value is not in the output",
                   allJson(out).contains("b".repeat(128)));
    }

    /** Frozen UDT as the CLUSTERING key (not just a regular column, as in frozenCollections above). */
    @Test
    public void frozenUdtInClusteringKey() throws Exception
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, ck frozen<" + udt + ">, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 4; pk++)
                for (int i = 0; i < 5; i++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, userType("a", i, "b", "b" + i), "v" + round);
            flush();
        }
        execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", userType("a", 2, "b", "b2"));
        flush();

        assertEquals("expected four overlapping inputs; a lost flush() would degrade this to a single-sstable rewrite",
                     4, cfs.getLiveSSTables().size());
        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Frozen UDT as (part of) the PARTITION key. */
    @Test
    public void frozenUdtInPartitionKey() throws Exception
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk frozen<" + udt + ">, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (int i = 0; i < 4; i++)
                for (long ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", userType("a", i, "b", "p" + i), ck, "v" + round);
            flush();
        }
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", userType("a", 1, "b", "p1"), 1L, 3L);
        flush();

        assertEquals("expected four overlapping inputs; a lost flush() would degrade this to a single-sstable rewrite",
                     4, cfs.getLiveSSTables().size());
        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Frozen collection as the CLUSTERING key (not just a regular column, as in frozenCollections above). */
    @Test
    public void frozenCollectionInClusteringKey() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck frozen<list<int>>, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 4; pk++)
                for (int i = 0; i < 5; i++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, list(i, i + 1, i + 2), "v" + round);
            flush();
        }
        execute("DELETE FROM %s WHERE pk = 0 AND ck = ?", list(2, 3, 4));
        flush();

        assertEquals("expected four overlapping inputs; a lost flush() would degrade this to a single-sstable rewrite",
                     4, cfs.getLiveSSTables().size());
        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** TTLs: live expiring cells and already-expired cells (expiry far from run boundaries). */
    @Test
    public void expiringCells() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // long TTLs: alive during both runs
        for (long pk = 0; pk < 5; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 86400", pk, ck, "a" + ck, "b" + ck);
        flush();

        // short TTLs: expired well before either run
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?) USING TTL 1", 1L, ck, "expired" + ck);
        // mixed: row with one TTL'd and one permanent cell
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s USING TTL 86400 SET v1 = ? WHERE pk = ? AND ck = ?", "ttl" + ck, 2L, ck);
        flush();

        // fixed "now" two seconds past the LAST write, so the TTL=1 cells have expired relative to
        // it however long the write phase took, while the 86400s TTLs stay comfortably alive
        long fixedNow = FBUtilities.nowInSeconds() + 2;
        assertSomethingExpiredAt(cfs, fixedNow);

        assertCursorMatchesIteratorAcrossGenerations(cfs, () -> fixedNow);
    }

    /** Same-timestamp conflicting writes: reconciliation must tie-break identically. */
    @Test
    public void timestampTies() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "aaa" + ck);
        flush();

        for (long ck = 0; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "zzz" + ck);
        flush();

        // tombstone vs write at the same timestamp: delete wins
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 5");
        for (long ck = 4; ck < 7; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 2000", 1L, ck, "tie" + ck);
        flush();

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE: at equal timestamps the GREATER raw value wins — the value compare
        // Cells.resolveRegular falls through to when the shared decision table returns COMPARE — so
        // every "zzz" beats its "aaa" partner. A merge with that comparison inverted keeps the "aaa"
        // partner, which byte equality cannot see if both paths share the inversion.
        // ck 4..6 are excluded: they are overwritten at ts 2000 and ck 5 is row-deleted there, so
        // their survivor is decided by timestamp, not by the value rule under test.
        String json = allJson(out);
        for (long ck = 0; ck < 20; ck++)
        {
            if (ck >= 4 && ck <= 6)
                continue;
            assertTrue("the greater value must win the same-timestamp tie at ck " + ck,
                       json.contains(cellValue("zzz" + ck)));
            assertFalse("the lexicographically smaller value won the same-timestamp tie at ck " + ck,
                        json.contains(cellValue("aaa" + ck)));
        }
        assertEquals("expected one surviving zzz value per tie the loop above covers, or it is " +
                     "covering fewer ties than it claims",
                     17, countOccurrences(json, "\"value\":\"zzz"));
    }

    /**
     * Same-timestamp ties between values of DIFFERENT LENGTHS: the reference tie-break
     * (the value compare Cells.resolveRegular falls through to when the shared decision table
     * returns COMPARE, i.e. ValueAccessor.compare) is plain unsigned lexicographic on the RAW value
     * bytes, where a comparison of the WIRE form would see
     * the leading length vint first and order by LENGTH (the vint's first byte encodes it).
     * The timestampTies pin above uses equal-length values and cannot see the difference.
     * Covers both directions and the 1-byte/2-byte vint boundary (length 128).
     */
    @Test
    public void timestampTiesDifferentLengthValues() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // raw order: "z" > "aa"; wire order: len 1 < len 2 — the reference keeps "z"
        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "z");
        // vint-boundary variant: raw keeps the 100-char "b..."; wire would pick the
        // 200-char "a..." (len 100 = one-byte vint 0x64, len 200 = two-byte vint 0x81 0x48)
        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 2L, ck, "b".repeat(100));
        flush();

        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "aa");
        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 2L, ck, "a".repeat(200));
        flush();

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE: the reference compares RAW value bytes, so "z" (0x7a) beats "aa" (0x61 0x61) and
        // the 100-char "b" run beats the 200-char "a" run. A comparison over the WIRE form would see
        // the leading length vint first and order by length, picking the other winner in both pairs.
        // It only shows up where the two orderings disagree, which is what this scenario arranges.
        String json = allJson(out);
        assertEquals("the shorter-but-greater value must win all four ties", 4,
                     countOccurrences(json, cellValue("z")));
        assertFalse("length ordering won over raw-byte ordering: the longer \"aa\" survived",
                    json.contains(cellValue("aa")));
        assertEquals("the shorter-but-greater value must win all four vint-boundary ties", 4,
                     countOccurrences(json, cellValue("b".repeat(100))));
        assertFalse("length ordering won over raw-byte ordering at the vint boundary: the 200-char " +
                    "value survived", json.contains(cellValue("a".repeat(200))));
    }

    /** Newer partition deletion shadowing older data across several sstables. */
    @Test
    public void shadowedPartitions() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = 0; pk < 6; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round);
            flush();
        }
        execute("DELETE FROM %s WHERE pk = 2");
        execute("DELETE FROM %s WHERE pk = 3");
        // resurrection after the partition delete
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3L, 0L, "alive-again");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Single-input compaction: pure rewrite, no merge. */
    @Test
    public void singleInputSSTable() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 10; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 2 AND ck < 6");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Many inputs: 8-way merge exercises the merge heap harder than the usual 2-4. */
    @Test
    public void eightWayMerge() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 8; round++)
        {
            // partial, interleaved coverage: each sstable covers a sliding window
            for (long pk = round; pk < round + 6; pk++)
                for (long ck = 0; ck < 10; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "r" + round + "c" + ck);
            if (round % 2 == 0)
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?", (long) round, 3L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Disjoint inputs: no overlapping partitions, pure concatenation. */
    @Test
    public void disjointInputs() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 3; round++)
        {
            for (long pk = round * 100; pk < round * 100 + 10; pk++)
                for (long ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Empty (zero-length) values are valid and distinct from null; both must survive merge. */
    @Test
    public void emptyAndNullValues() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 blob, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, "value" + ck, ByteBufferUtil.bytes("cafe"));
        flush();

        // empty-string / empty-blob overwrites
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", 1L, ck, "", ByteBufferUtil.EMPTY_BYTE_BUFFER);
        // null overwrites (cell tombstones)
        for (long ck = 5; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, null, null)", 1L, ck);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Empty clustering values on a DESC (reversed) clustering column, found by the randomized
     * soak (seed 99303954147053). Every base type sorts empty before
     * values, so a reversed column sorts empty AFTER values (ReversedType swaps operands
     * around the base comparison). The cursor path's raw clustering comparison decided
     * empty-vs-valued purely from the serialized flag bits, ignoring reversal:
     *  - same-partition variant: rows with empty and valued clusterings for the SAME pk in
     *    different sstables merge in the wrong order (Data.db divergence — corruption class);
     *  - cross-partition variant: the global covered-clustering max picks the wrong row
     *    (Statistics.db divergence — what the soak caught).
     */
    @Test
    public void emptyClusteringValuesDescending() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable 1: valued clusterings for pk 1 (same-partition variant) and pk 2 (the
        // lexically-largest valued rows, cross-partition variant)
        for (long ck = 1; ck <= 5; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "p1v" + ck);
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, ck * 1000, "p2v" + ck);
        }
        flush();

        // sstable 2: EMPTY clustering values — same partition as pk 1's valued rows (the
        // merge must order empty AFTER values under DESC), plus an empty-only partition
        // (the global max clustering must be the empty value, not pk 2's large bigints)
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p1empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p3empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 3L, "p1v3-overwrite");
        flush();

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE: under DESC the reversed type sorts an EMPTY component AFTER valued ones, so pk 1's
        // empty-clustering row must be emitted last in its partition. The dump is in sstable order, so
        // comparing positions states the merge order directly. Byte-equality cannot see this if both
        // paths share the ordering, and a comparison derived from the serialized flag bits alone
        // ignores reversal.
        assertEmptyClusteringOrder(allJson(out), true);
    }

    /**
     * Asserts where pk 1's empty-clustering row sits relative to its smallest valued clustering.
     * Under DESC (reversed) empty sorts after values; under ASC it sorts before them. Both scenarios
     * write the same shape, so the pair is what stops a fix from over-applying the reversal flip or
     * from dropping the null guard that keeps nulls type-independent.
     */
    private static void assertEmptyClusteringOrder(String json, boolean descending)
    {
        int emptyAt = json.indexOf(cellValue("p1empty"));
        // ck 1 is the smallest valued clustering, so it is emitted LAST under DESC and FIRST under ASC
        int smallestValuedAt = json.indexOf(cellValue("p1v1"));
        assertTrue("the scenario stopped writing its empty-clustering row", emptyAt >= 0);
        assertTrue("the scenario stopped writing its smallest valued clustering", smallestValuedAt >= 0);
        if (descending)
            assertTrue("under DESC an empty clustering component sorts AFTER valued ones, but the " +
                       "merge emitted it first", emptyAt > smallestValuedAt);
        else
            assertTrue("under ASC an empty clustering component sorts BEFORE valued ones, but the " +
                       "merge emitted it last", emptyAt < smallestValuedAt);
    }

    /** ASC counterpart of emptyClusteringValuesDescending: empty sorts BEFORE values on a
     *  non-reversed column; pins the unflipped flag ordering. */
    @Test
    public void emptyClusteringValuesAscending() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 1; ck <= 5; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "p1v" + ck);
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, -ck * 1000, "p2v" + ck);
        }
        flush();

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p1empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3L, ByteBufferUtil.EMPTY_BYTE_BUFFER, "p3empty");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 3L, "p1v3-overwrite");
        flush();

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE, and the control half of the pair: without reversal the empty component sorts
        // FIRST, so a fix that flipped unconditionally would fail here while the DESC scenario passed.
        assertEmptyClusteringOrder(allJson(out), false);
    }

    /**
     * Row liveness shapes: UPDATE-built rows carry NO primary-key liveness (different row
     * flags than INSERT-built rows), primary-key-only INSERTs carry liveness and ZERO cells,
     * and merges must reconcile liveness presence/absence across sstables exactly.
     */
    @Test
    public void rowLivenessShapes() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // UPDATE-built rows (no liveness) and liveness-only rows in sstable 1
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s SET v1 = ?, v2 = ? WHERE pk = ? AND ck = ?", "u" + ck, "w" + ck, 1L, ck);
        for (long ck = 10; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1L, ck);
        flush();

        // sstable 2: INSERT onto UPDATE-rows (liveness arrives later), cell tombstones onto
        // liveness-only rows (row must survive on liveness alone), and a cell delete that strips
        // every cell from an UPDATE-row — which leaves a row with no liveness and two cell
        // tombstones, NOT an absent row: the table takes the default 10-day gc_grace, so the
        // tombstones' localDeletionTime sits far above the gcBefore this runs at and both paths
        // emit them. The vanishing shape needs a gcBefore above that localDeletionTime, which the
        // explicit-gcBefore overload of assertCursorMatchesIterator can supply.
        for (long ck = 0; ck < 4; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, "i" + ck);
        for (long ck = 10; ck < 13; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, null, null)", 1L, ck);
        execute("DELETE v1, v2 FROM %s WHERE pk = ? AND ck = ?", 1L, 5L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Row-level TTL (INSERT USING TTL sets liveness TTL + cell TTLs) merged against
     * cell-level TTL (UPDATE USING TTL sets only cell TTLs) and against plain writes;
     * includes same-timestamp expiring-vs-expiring writes whose TTLs differ, which
     * CellLivenessInfo.resolveSameTimestampTie settles on its greater-localDeletionTime branch:
     * both sides carry one,
     * so the GREATER wins — here the TTL 100000 cell, written in the EARLIER sstable. The branch
     * below it (equal
     * expiration times, differing TTLs, LOWER TTL wins) is NOT covered: expiration is
     * nowInSec + ttl, so reaching it from CQL against a live clock would need the two
     * same-timestamp writes separated by EXACTLY the TTL difference in wall-clock seconds. Any
     * other spacing leaves the expirations differing, and the greater-expiration branch decides
     * first.
     */
    @Test
    public void rowAndCellTtlMix() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 text, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // ck 9..11 are deliberately NOT written here: the tie under test is at TIMESTAMP 5000, and a
        // wall-clock write to the same cell dominates it on timestamp before any tie-break is
        // consulted, which would leave the tie constructed but discarded.
        for (long ck = 0; ck < 9; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) USING TTL 86400", 1L, ck, "a" + ck, "b" + ck);
        flush();

        // cell-level TTL different from the row TTL; plain overwrites clearing TTLs;
        // expiring-vs-expiring same-timestamp ties with different TTLs
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TTL 172800 SET v1 = ? WHERE pk = ? AND ck = ?", "c" + ck, 1L, ck);
        for (long ck = 6; ck < 9; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", 1L, ck, "plain" + ck);
        for (long ck = 9; ck < 12; ck++)
            execute("UPDATE %s USING TTL 100000 AND TIMESTAMP 5000 SET v2 = ? WHERE pk = ? AND ck = ?", "t1" + ck, 1L, ck);
        flush();

        for (long ck = 9; ck < 12; ck++)
            execute("UPDATE %s USING TTL 50000 AND TIMESTAMP 5000 SET v2 = ? WHERE pk = ? AND ck = ?", "t2" + ck, 1L, ck);
        flush();

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE: byte equality cannot see a tie-break both paths get wrong, so the surviving value
        // is stated outright. A merge resolving a same-timestamp expiring pair by write order, or by the
        // lower localExpirationTime, keeps "t2..".
        String json = allJson(out);
        for (long ck = 9; ck < 12; ck++)
        {
            assertTrue("the greater localExpirationTime must win the same-timestamp expiring tie at ck " + ck,
                       json.contains(cellValue("t1" + ck)));
            assertFalse("the lower localExpirationTime won the same-timestamp expiring tie at ck " + ck,
                        json.contains(cellValue("t2" + ck)));
        }
        // Pins the loop from the other side: three ties, three survivors. The negative assertions above
        // are all satisfied by an output that dropped the rows entirely.
        assertEquals("expected one greater-localExpirationTime winner per tie", 3, countOccurrences(json, "\"value\":\"t1"));
    }

    /**
     * Expiring-vs-live cells at the SAME timestamp, both directions across sstables: the
     * CASSANDRA-14592 rule — an expiring (or deleted) cell beats a live one on timestamp
     * tie regardless of value. Implemented in
     * CellLivenessInfo.resolveSameTimestampTie's tombstone-or-expiring-beats-live branch; this pins
     * it at the differential level (timestampTies only covered live-vs-live and delete-vs-live).
     */
    @Test
    public void expiringVsLiveTies() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // direction 1: live first, expiring second
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 1L, ck, "zzz-live" + ck);
        // direction 2 partition: expiring first
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000 AND TTL 86400", 2L, ck, "aaa-ttl" + ck);
        flush();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000 AND TTL 86400", 1L, ck, "aaa-ttl" + ck);
        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", 2L, ck, "zzz-live" + ck);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Cell TOMBSTONE vs EXPIRING cell at the SAME timestamp: the shared decision table
     * (CellLivenessInfo.resolveSameTimestampTie, which Cells.resolveRegular defers to) gives the
     * tombstone the tie, BEFORE any localDeletionTime comparison. This is the one same-timestamp pairing where both
     * sides carry a localExpirationTime (tombstone: deletion second; expiring: expiry
     * second), so a resolver that classifies "tombstone" by the presence of a
     * localExpirationTime rather than by the absence of a TTL never reaches that branch and
     * falls through to the ldt compare,
     * which the expiring cell's future expiry second wins: deleted data resurrected
     * until the TTL lapses. Both flush orders and both tombstone shapes
     * (UPDATE SET v = null, DELETE v) across distinct partitions.
     */
    @Test
    public void tombstoneVsExpiringTies() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk 1: tombstone flushed first, expiring second; tombstone via UPDATE SET null
        for (long ck = 0; ck < 5; ck++)
            execute("UPDATE %s USING TIMESTAMP 5000 SET v = null WHERE pk = ? AND ck = ?", 1L, ck);
        // pk 2: expiring flushed first, tombstone second; tombstone via DELETE column
        for (long ck = 0; ck < 5; ck++)
            execute("UPDATE %s USING TTL 86400 AND TIMESTAMP 5000 SET v = ? WHERE pk = ? AND ck = ?", "live" + ck, 2L, ck);
        flush();

        for (long ck = 0; ck < 5; ck++)
            execute("UPDATE %s USING TTL 86400 AND TIMESTAMP 5000 SET v = ? WHERE pk = ? AND ck = ?", "live" + ck, 1L, ck);
        for (long ck = 0; ck < 5; ck++)
            execute("DELETE v FROM %s USING TIMESTAMP 5000 WHERE pk = ? AND ck = ?", 2L, ck);
        flush();

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // ABSOLUTE: the tombstone must win every tie, in both flush orders. Byte-equality cannot see
        // this, and a merge that never reaches the rule picks the expiring cell, resurrecting deleted
        // data together with its value.
        String json = allJson(out);
        for (long ck = 0; ck < 5; ck++)
            assertFalse("an expiring cell won a same-timestamp tie against a tombstone at ck " + ck +
                        ", which resurrects deleted data", json.contains(cellValue("live" + ck)));
        assertEquals("expected one surviving cell tombstone per tie, over both flush orders",
                     10, countOccurrences(json, CELL_TOMBSTONE));
        // The survivor is a tombstone, not a TTL'd cell: an expiring cell would render a ttl field,
        // and nothing else in this scenario carries one.
        assertFalse("a survivor still carries a TTL, so an expiring cell won a tie",
                    json.contains("\"ttl\":"));
    }

    /** Vector and duration columns: fixed-dimension float vectors and the
     *  variable-length duration encoding as ordinary single cells, overwritten and
     *  null-overwritten (cell tombstone) across sstables. */
    @Test
    public void vectorAndDuration() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, vec vector<float, 3>, dur duration, v text, " +
                    "PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, vec, dur, v) VALUES (?, ?, [1.5, 2.5, " + ck + ".0], 2h30m, ?)",
                    1L, ck, "v" + ck);
        flush();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, vec, dur, v) VALUES (?, ?, [9.0, 8.0, 7.0], 45s500ms, ?)",
                    1L, ck, "w" + ck);
        // null overwrites: cell tombstones for vector and duration cells
        execute("INSERT INTO %s (pk, ck, vec, dur) VALUES (?, ?, null, null)", 1L, 7L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * Fixed-length values LARGER than the cursor's 4KiB copy buffer. A {@code vector<float, 1536>} — a
     * mainstream embedding width — is 6144 bytes, and unlike a variable-length value it carries no
     * length vint, so the copy cannot be driven off the wire length. Covers a value spanning several
     * chunks, one landing exactly on the buffer boundary ({@code vector<float, 1024>} is 4096), a
     * same-timestamp tie ACROSS sstables so the value also travels through the compactor's temp
     * buffers rather than straight to the writer, and a null overwrite of an oversized column.
     */
    @Test
    public void fixedLengthValuesLargerThanCopyBuffer() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, big vector<float, 1536>, " +
                    "exact vector<float, 1024>, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 4; ck++)
        {
            // ck 2 is written only by the same-timestamp inserts below. An auto-timestamp write
            // here would reconcile with them in the memtable and win, and the merge would then
            // resolve on timestamp and never reach the value comparison.
            if (ck == 2)
                continue;
            execute("INSERT INTO %s (pk, ck, big, exact, v) VALUES (?, ?, ?, ?, ?)",
                    1L, ck, floats(1536, ck), floats(1024, ck), "v" + ck);
        }
        // ck 2: one half of a same-timestamp tie. It must be in a DIFFERENT sstable from its
        // partner, or the memtable reconciles the two before either reaches disk and the merge
        // resolves on timestamp instead of reaching the value comparison.
        execute("INSERT INTO %s (pk, ck, big, exact) VALUES (?, ?, ?, ?) USING TIMESTAMP 5000",
                1L, 2L, floats(1536, 7), floats(1024, 7));
        flush();

        // ck 0..1: overwritten at a later timestamp, so the winner is copied straight through
        for (long ck = 0; ck < 2; ck++)
            execute("INSERT INTO %s (pk, ck, big, exact, v) VALUES (?, ?, ?, ?, ?)",
                    1L, ck, floats(1536, ck + 100), floats(1024, ck + 100), "w" + ck);
        // ck 2: the other half of the tie, so both oversized values are buffered and compared
        execute("INSERT INTO %s (pk, ck, big, exact) VALUES (?, ?, ?, ?) USING TIMESTAMP 5000",
                1L, 2L, floats(1536, 8), floats(1024, 8));
        // ck 3: null overwrite -> cell tombstone on an oversized fixed-length column
        execute("INSERT INTO %s (pk, ck, big) VALUES (?, ?, null)", 1L, 3L);
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** Deterministic float vector of the given dimension; dimension * 4 bytes on the wire. */
    private Vector<Float> floats(int dimension, long salt)
    {
        float[] v = new float[dimension];
        for (int i = 0; i < dimension; i++)
            v[i] = i + salt;
        return vector(v);
    }

    /**
     * More than 64 regular columns: rows lacking columns switch from the 64-bit-mask
     * column-subset encoding to the structurally different large-subset wire format —
     * byte-compared here for the first time; previously only the old simple-suite
     * exercised it without comparing the paths against each other.
     */
    @Test
    public void over64Columns() throws Exception
    {
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int i = 0; i < 70; i++)
            ddl.append(", c").append(i).append(" int");
        ddl.append(", PRIMARY KEY (pk, ck))");
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sparse rows: each ck sets a sliding 10-column window (subset encoding for >64 columns)
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 14; ck++)
            {
                StringBuilder stmt = new StringBuilder("INSERT INTO %s (pk, ck");
                int base = (int) ck * 5 + round * 3;
                for (int i = 0; i < 10; i++)
                    stmt.append(", c").append((base + i) % 70);
                stmt.append(") VALUES (?, ?");
                for (int i = 0; i < 10; i++)
                    stmt.append(", ").append(base + i);
                stmt.append(')');
                execute(stmt.toString(), 1L, ck);
            }
            // one full row per round: the HAS_ALL_COLUMNS path next to large subsets
            StringBuilder full = new StringBuilder("INSERT INTO %s (pk, ck");
            for (int i = 0; i < 70; i++)
                full.append(", c").append(i);
            full.append(") VALUES (?, ?");
            for (int i = 0; i < 70; i++)
                full.append(", ").append(i);
            full.append(')');
            execute(full.toString(), 1L, 99L);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * OPEN-ENDED (single-sided) range tombstones: DELETE with only a lower or upper
     * clustering bound produces markers whose other side is the unbounded partition edge —
     * zero-component TOP/BOTTOM bounds, the same empty-prefix region bound-kind comparisons
     * and covered-clustering stats exercise. Open RTs nest with each other, overlap bounded
     * RTs and rows across sstables, and one partition is open-RT-only.
     * Previously covered only probabilistically by the widened soak.
     */
    @Test
    public void openEndedRangeTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 1; pk <= 2; pk++)
            for (long ck = 0; ck < 30; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + ck);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?", 1L, 10L, 20L); // bounded, for interleave
        flush();

        // open-ended deletes: up to TOP, down from BOTTOM, nested opens, and an RT-only partition
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, 25L);
        execute("DELETE FROM %s WHERE pk = ? AND ck > ?", 1L, 27L);  // nests inside the >= 25 open range
        execute("DELETE FROM %s WHERE pk = ? AND ck <= ?", 2L, 4L);
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 3L, 0L);  // partition with ONLY an open RT
        flush();

        // resurrection inside open-deleted ranges with newer timestamps
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 26L, "resurrected");
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, 2L, "resurrected");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /** DESC counterpart: single-sided bounds invert in on-disk clustering order, so the
     *  open edge swaps between TOP and BOTTOM relative to the CQL bound direction. */
    @Test
    public void openEndedRangeTombstonesDescending() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH CLUSTERING ORDER BY (ck DESC) AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 30; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, "v" + ck);
        flush();

        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", 1L, 25L);
        execute("DELETE FROM %s WHERE pk = ? AND ck <= ?", 1L, 4L);
        flush();

        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 27L, "resurrected");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }

    /**
     * ODD superset size at the subset-encoding mode boundary: with 71 columns, the encoder
     * and decoder must agree on present-index vs missing-index
     * mode at exactly presentCount == 35 (the integer-division boundary of supersetCount/2).
     * Rows at 34/35/36 present columns straddle the boundary from both sides.
     */
    @Test
    public void over64ColumnsOddSupersetBoundary() throws Exception
    {
        StringBuilder ddl = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int i = 0; i < 71; i++)
            ddl.append(", c").append(i).append(" int");
        ddl.append(", PRIMARY KEY (pk, ck))");
        createTable(ddl.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            long ck = 0;
            for (int present : new int[]{ 34, 35, 36, 70 })
            {
                StringBuilder stmt = new StringBuilder("INSERT INTO %s (pk, ck");
                for (int i = 0; i < present; i++)
                    stmt.append(", c").append((i + round) % 71); // shift per round so the merge unions
                stmt.append(") VALUES (?, ?");
                for (int i = 0; i < present; i++)
                    stmt.append(", ").append(i);
                stmt.append(')');
                execute(stmt.toString(), 1L, ck++);
            }
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
    }
}
