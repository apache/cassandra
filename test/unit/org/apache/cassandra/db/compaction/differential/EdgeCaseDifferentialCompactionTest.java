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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Holds the edge-case scenarios that compare cursor compaction against iterator compaction.
 *
 * Every scenario here uses a table shape that cursor compaction supports: see
 * CursorCompactor.isSupported. Every scenario must run the cursor path, and the test harness
 * fails if the code falls back to the iterator path. Put a scenario for an unsupported shape in
 * CursorSupportMatrixTest instead.
 *
 * Every scenario compacts twice: see assertCursorMatchesIteratorAcrossGenerations. The second
 * compaction reads the output of the first, which only a compaction can produce, so the test
 * also reads input shapes that no flush makes. The byte comparison in the first compaction is
 * what tests the write side.
 */
public class EdgeCaseDifferentialCompactionTest extends DifferentialCompactionTester
{

    /**
     * Static-column table where some partitions have NO static values: an empty static row is
     * written for those partitions but must not be counted in stats (totalRows/totalColumnsSet).
     * The staticRows scenario gives every partition static data, so it never writes an empty
     * static row.
     * <p>
     * pk 0 additionally carries a static row LARGER than column_index_size, which pins the other
     * half of the same rule: {@code SSTableCursorWriter} routes a static row to
     * {@code CursorIndexWriter.staticRowWritten}, which moves the next block's start past it,
     * because a static row belongs to the partition header and not to a row index block. The
     * block count below is what states that; the size of a static row cannot otherwise be seen,
     * since {@code staticRowWritten} is final and branch-free.
     */
    @Test
    public void emptyStaticRows() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s1 text static, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // pk 0's static alone exceeds the 4 KiB column_index_size; its two regular rows are tiny
        String bigStatic = "s".repeat(5000);
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 8; pk++)
            {
                if (pk % 2 == 0)
                    execute("INSERT INTO %s (pk, s1, ck, v) VALUES (?, ?, ?, ?)",
                            pk, pk == 0 ? bigStatic + round : "static" + pk, (long) round, "v" + round);
                else
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, (long) round, "v" + round);
            }
            flush();
        }

        CapturedOutput out = assertCursorMatchesIteratorAcrossGenerations(cfs);

        // The empty static rows of the four odd partitions must not be counted. The correct count
        // is 20: 8 partitions with 2 clusterings give 16 regular rows, and each of the 4 even
        // partitions adds one static row that is not empty. If both paths counted the empty rows,
        // the byte comparison would still pass, and only this test would fail.
        assertEquals("expected a single compaction output", 1, out.sstables.size());
        assertTrue("an absent static row was counted: expected totalRows=20 (16 regular + 4 " +
                   "non-empty static), got: " + out.sstables.get(0).statsSummary,
                   out.sstables.get(0).statsSummary.contains("totalRows=20 "));
        // An empty static row adds no column, so that fault cannot change totalColumnsSet. Test it
        // as well, to show the count is right for the other reason: 16 regular v cells and 4
        // static s1 cells.
        assertTrue("expected totalColumnsSet=20, got: " + out.sstables.get(0).statsSummary,
                   out.sstables.get(0).statsSummary.contains("totalColumnsSet=20 "));

        // ABSOLUTE. If pk 0's oversized static counted toward the first index block, its row at
        // ck 0 would cut that block and the row at ck 1 would leave a tail, giving 2 blocks.
        assertEquals("the cross-generation rung should leave one cursor-produced output",
                     1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();
        assertEquals("a static row larger than column_index_size opened an index block: two tiny " +
                     "regular rows cannot reach the threshold on their own, so this partition must " +
                     "not be promoted however large its static row is",
                     0, blockCount(output, 0L));
    }

    /**
     * Merges multi-cell collections across sstables. It covers:
     *  - updates of single elements;
     *  - overwrites of a whole collection, which give a complex deletion and new cells;
     *  - columns that hold a deletion and no cell;
     *  - merges of UDT fields.
     */
    @Test
    public void multiCellColumnsAcrossSSTables() throws Exception
    {
        String udt = createType("CREATE TYPE %s (a int, b text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, u " + udt + ", v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 8; ck++)
                execute("INSERT INTO %s (pk, ck, m, s, u, v) VALUES (?, ?, ?, ?, {a: ?, b: ?}, ?)",
                        pk, ck, map("k" + ck, ck, "shared", pk), set((int) ck, 7), (int) ck, "b" + ck, "v" + ck);
        flush();

        // sstable 2 updates single elements, which adds new paths to columns that already exist.
        for (long pk = 0; pk < 4; pk++)
            for (long ck = 0; ck < 8; ck += 2)
            {
                execute("UPDATE %s SET m[?] = ?, s = s + ? WHERE pk = ? AND ck = ?", "added" + ck, ck * 10, set(99), pk, ck);
                execute("UPDATE %s SET u.b = ? WHERE pk = ? AND ck = ?", "upd" + ck, pk, ck);
            }
        flush();

        // sstable 3 overwrites whole collections, which gives a complex deletion and new cells. It
        // also deletes a collection without writing a cell, and overwrites elements at paths that
        // already exist, with newer timestamps.
        execute("UPDATE %s SET m = ? WHERE pk = ? AND ck = ?", map("fresh", 1L), 0L, 0L);
        execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", 1L, 2L);
        execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "shared", 555L, 2L, 4L);
        execute("DELETE s FROM %s WHERE pk = ? AND ck = ?", 3L, 6L);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Merges multi-cell columns across sstables whose headers were built against different
     * TableMetadata versions.
     *
     * An ALTER TYPE ADD rebuilds the column through ColumnMetadata.withNewType; see
     * CASSANDRA-13776. An sstable flushed before that ALTER therefore holds a different
     * ColumnMetadata instance for one column than an sstable flushed after it, because
     * SSTableReader.header is built once, when the sstable is opened. The merge must therefore
     * compare columns by value, and not by reference.
     *
     * This test does not use the differential harness, because that harness opens every input
     * again against the current schema. That would give both inputs the same instance and remove
     * the condition under test. Only the sstables that were opened first hold different
     * instances, and those are the ones production compacts.
     *
     * The test therefore does three things: it shows that the two instances differ, it runs a
     * cursor compaction on the sstables as they were opened, and it reads the result with CQL.
     * Field f2 is a good test value, because sstable 2 never writes it. Only the complex deletion
     * of the overwrite can remove f2. If the merge loses that deletion, f2 comes back, and no
     * timestamp can hide that.
     */
    @Test
    public void complexColumnsAcrossTypeAlter() throws Exception
    {
        String udt = createType("CREATE TYPE %s (f1 text, f2 text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, u " + udt + ", v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // sstable 1 gets its header from the schema as it is before the ALTER.
        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET u.f1 = ?, u.f2 = ?, v = ? WHERE pk = ? AND ck = ?",
                    "old" + ck, "keepable" + ck, "x" + ck, 1L, ck);
        flush();

        // This ALTER rebuilds the ColumnMetadata of column u. The route is
        // TableMetadata.withUpdatedUserType and then withNewType.
        execute("ALTER TYPE " + KEYSPACE + "." + udt + " ADD f3 text");

        // sstable 2 gets its header from the schema as it is after the ALTER. It overwrites whole
        // columns, which gives a complex deletion and new cells. One row gets a deletion and no
        // cell, which makes the merge look for the deletion on the old instance as well.
        for (long ck = 0; ck < 6; ck += 2)
            execute("UPDATE %s USING TIMESTAMP 2000 SET u = {f1: ?, f3: ?} WHERE pk = ? AND ck = ?",
                    "new" + ck, "three" + ck, 1L, ck);
        execute("DELETE u FROM %s USING TIMESTAMP 2000 WHERE pk = 1 AND ck = 5");
        flush();

        // The two open sstables must hold different ColumnMetadata instances for column u.
        List<ColumnMetadata> uInstances = new ArrayList<>();
        for (SSTableReader r : cfs.getLiveSSTables())
            for (ColumnMetadata c : r.header.columns(false))
                if (c.name.toString().equals("u"))
                    uInstances.add(c);
        assertEquals("expected one u column per input sstable", 2, uInstances.size());
        assertNotSame("ALTER TYPE no longer skews header instances — scenario is vacuous",
                     uInstances.get(0), uInstances.get(1));

        // Compact the sstables as production does, with the two different instances in place.
        // commitCompaction fails if the cursor path does not run.
        commitCompaction(cfs, cfs.getLiveSSTables(), true,
                         cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));

        // The correct result: an overwritten row loses f2 to the deletion of the overwrite; a row
        // that was not overwritten keeps f1 and f2; the row that was deleted loses all of u.
        assertRows(execute("SELECT ck, u.f1, u.f2, u.f3, v FROM %s WHERE pk = 1"),
                   row(0L, "new0", null, "three0", "x0"),
                   row(1L, "old1", "keepable1", null, "x1"),
                   row(2L, "new2", null, "three2", "x2"),
                   row(3L, "old3", "keepable3", null, "x3"),
                   row(4L, "new4", null, "three4", "x4"),
                   row(5L, null, null, null, "x5"));
    }

    /**
     * Tests a row deletion and a collection deletion that are exactly equal. They share one USING
     * TIMESTAMP value, and they are made in the same second, so their local deletion times are
     * equal as well.
     *
     * The iterator keeps a complex deletion only if it supersedes the active deletion: see
     * Row.Merger.ColumnDataReducer. If the two are equal, the iterator drops the complex deletion.
     * A merge that instead keeps the deletion when the two are equal writes a spurious
     * HAS_COMPLEX_DELETION flag and spurious deletion bytes.
     *
     * The two deletions go into different sstables, so compaction reconciles them, and not the
     * memtable.
     *
     * The local deletion time comes from the server clock. The loop below reads the times back and
     * repeats the setup until both statements fall in the same second, so the test is repeatable.
     */
    @Test
    public void rowAndComplexDeletionEqualityTies() throws Exception
    {
        for (int attempt = 0; attempt < 8; attempt++)
        {
            createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, " +
                        "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            // Data around the deletions, and data they shadow. It gets its own sstable.
            for (long ck = 0; ck < 4; ck++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 100", 0L, ck, "keep" + ck);
                execute("UPDATE %s USING TIMESTAMP 100 SET m[?] = ?, v = ? WHERE pk = ? AND ck = ?",
                        "k" + ck, ck, "old" + ck, 1L, ck);
            }
            flush();

            // The collection deletion, alone in its sstable.
            for (long ck = 0; ck < 4; ck++)
                execute("DELETE m FROM %s USING TIMESTAMP 10000 WHERE pk = ? AND ck = ?", 1L, ck);
            flush();

            // The row deletion, with the same USING TIMESTAMP, in a third sstable.
            for (long ck = 0; ck < 4; ck++)
                execute("DELETE FROM %s USING TIMESTAMP 10000 WHERE pk = ? AND ck = ?", 1L, ck);
            flush();

            // The two deletions are equal only if both were made in the same second.
            Set<Long> ldts = new HashSet<>();
            for (SSTableReader r : cfs.getLiveSSTables())
            {
                long ldt = r.getSSTableMetadata().maxLocalDeletionTime;
                if (ldt != Long.MAX_VALUE)
                    ldts.add(ldt);
            }
            if (ldts.size() == 1)
            {
                assertCursorMatchesIteratorAcrossGenerations(cfs);
                return;
            }
            // The clock passed a second boundary between the two deletes. Build the data again.
        }
        fail("could not land both deletions in the same second after 8 attempts");
    }

    /** Mixes complex deletions with range tombstones. A range delete shadows whole rows, complex
     *  columns included. A complex deletion shadows the cells of one column. Both are merged
     *  across sstables. */
    @Test
    public void complexDeletionsWithRangeTombstones() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 3; pk++)
            for (long ck = 0; ck < 20; ck++)
                execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, ?, ?)", pk, ck, map("a" + ck, ck, "b", pk), "v" + ck);
        flush();

        // Range tombstones above rows that hold complex data, and complex deletions inside rows
        // that survive.
        execute("DELETE FROM %s WHERE pk = 0 AND ck >= 5 AND ck < 12");
        execute("UPDATE %s SET m = ? WHERE pk = 0 AND ck = ?", map("replaced", 1L), 2L);
        execute("DELETE m FROM %s WHERE pk = 1 AND ck = ?", 15L);
        flush();

        // Newer writes into the deleted ranges, and into paths that the earlier complex deletion
        // shadows.
        execute("INSERT INTO %s (pk, ck, m, v) VALUES (?, ?, ?, ?)", 0L, 7L, map("resurrect", 7L), "back");
        execute("UPDATE %s SET m[?] = ? WHERE pk = 1 AND ck = ?", "post", 999L, 15L);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Merges list cells across sstables. List paths are timeuuids and sort by timestamp, not by
     * bytes. CQL prepend vs append is the ordinary merge. ck=99 holds two crafted paths that invert
     * byte order vs timeuuid order; CQL does not emit that pair.
     */
    @Test
    public void listCellsAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();

        for (long ck = 0; ck < 4; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 0L, ck, "v" + ck);
            execute("UPDATE %s SET l = l + ? WHERE pk = ? AND ck = ?", list("a", "b"), 0L, ck);
        }
        // v only: an INSERT of the list would add a complex deletion that shadows the crafted cells.
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 0L, 99L, "crafted");
        applyListCell(metadata, 0L, 99L, listTimeUuid(0xFFFFFFFF00001001L), "byte-high", 1000L);
        flush();

        for (long ck = 0; ck < 4; ck++)
            execute("UPDATE %s SET l = ? + l WHERE pk = ? AND ck = ?", list("x"), 0L, ck);
        applyListCell(metadata, 0L, 99L, listTimeUuid(0x0000000000001002L), "time-high", 1000L);
        flush();

        CapturedOutput out = assertCursorMatchesIterator(cfs);
        String json = allJson(out);
        assertEquals("crafted list cell missing: " + json, 1, countOccurrences(json, cellValue("byte-high")));
        assertEquals("crafted list cell missing: " + json, 1, countOccurrences(json, cellValue("time-high")));
    }

    /**
     * Same-timestamp live map cells. mergeCells COMPARE copies collection values through
     * tempCellBuffer, skips the length vint, then Arrays.compareUnsigned. timestampTies covers
     * that rule only for simple cells. text values so {@code valueLengthIfFixed()} is negative.
     */
    @Test
    public void mapValueTiesAtSameTimestamp() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 5; ck++)
        {
            execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ?, v = ? WHERE pk = ? AND ck = ?",
                    "cmp-aaa" + ck, "keep1-" + ck, 1L, ck);
            execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ?, v = ? WHERE pk = ? AND ck = ?",
                    "stay-zzz" + ck, "keep2-" + ck, 2L, ck);
        }
        execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ?, v = ? WHERE pk = 3 AND ck = 5",
                "ts-old", "keep-ts");
        execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ?, v = ? WHERE pk = 4 AND ck = 6",
                "tie-same", "keep-tie");
        flush();

        for (long ck = 0; ck < 5; ck++)
        {
            execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ? WHERE pk = ? AND ck = ?",
                    "cmp-zzz" + ck, 1L, ck);
            execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ? WHERE pk = ? AND ck = ?",
                    "late-aaa" + ck, 2L, ck);
        }
        execute("UPDATE %s USING TIMESTAMP 2000 SET m['k'] = ? WHERE pk = 3 AND ck = 5", "ts-new");
        execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ? WHERE pk = 4 AND ck = 6", "tie-same");
        flush();

        CapturedOutput out = assertCursorMatchesIterator(cfs);
        String json = allJson(out);
        for (long ck = 0; ck < 5; ck++)
        {
            assertTrue("greater map value must win the same-timestamp COMPARE at ck " + ck,
                       json.contains(cellValue("cmp-zzz" + ck)));
            assertFalse("lesser map value won the same-timestamp COMPARE at ck " + ck,
                        json.contains(cellValue("cmp-aaa" + ck)));
            assertTrue("greater map value already in the first sstable must be kept at ck " + ck,
                       json.contains(cellValue("stay-zzz" + ck)));
            assertFalse("later lesser map value replaced the first sstable at ck " + ck,
                        json.contains(cellValue("late-aaa" + ck)));
        }
        assertTrue("later timestamp must win the control row", json.contains(cellValue("ts-new")));
        assertFalse("earlier timestamp won the control row", json.contains(cellValue("ts-old")));
        assertEquals("the equal-value tie dropped the map cell",
                     1, countOccurrences(json, cellValue("tie-same")));
    }

    /**
     * A TTL on one map entry expires against a second sstable that holds the same path. The expired
     * winner becomes a tombstone and shadows the older live cell. Pathological tests TTL whole
     * INSERT windows, not one element.
     */
    @Test
    public void mapElementTtlAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET v = ? WHERE pk = 1 AND ck = ?", "rowkeep" + ck, ck);
        for (long ck = 0; ck < 3; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ? WHERE pk = 1 AND ck = ?",
                    "live-drop" + ck, ck);
        for (long ck = 3; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 AND TTL 1 SET m['k'] = ? WHERE pk = 1 AND ck = ?",
                    "ttl-old" + ck, ck);
        flush();

        for (long ck = 0; ck < 3; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 AND TTL 1 SET m['k'] = ? WHERE pk = 1 AND ck = ?",
                    "expiring-drop" + ck, ck);
        for (long ck = 3; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET m['k'] = ? WHERE pk = 1 AND ck = ?",
                    "live-keep" + ck, ck);
        flush();

        long pinnedNow = FBUtilities.nowInSeconds() + 60;
        assertSomethingExpiredAt(cfs, pinnedNow);
        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                         taskWithFixedNow(pinnedNow),
                                                         cfs.getDefaultGcBefore(pinnedNow));
        String json = allJson(out);
        for (long ck = 0; ck < 3; ck++)
        {
            assertFalse("expired map cell did not shadow the older live value at ck " + ck,
                        json.contains(cellValue("live-drop" + ck)));
            assertFalse("expired map cell kept its value at ck " + ck,
                        json.contains(cellValue("expiring-drop" + ck)));
        }
        for (long ck = 3; ck < 6; ck++)
        {
            assertTrue("later live map cell must survive an older TTL at ck " + ck,
                       json.contains(cellValue("live-keep" + ck)));
            assertFalse("older TTL map cell survived against a later live cell at ck " + ck,
                        json.contains(cellValue("ttl-old" + ck)));
        }
        for (long ck = 0; ck < 6; ck++)
            assertEquals("keep-column missing at ck " + ck, 1, countOccurrences(json, cellValue("rowkeep" + ck)));
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

    /**
     * Wide partition crossing column-index block boundaries (indexed RowIndexEntry path).
     * <p>
     * Round 0's delete opens at {@code ck >= 0}, and the table has no static column, so that
     * {@code INCL_START_BOUND} sorts ahead of every row and IS the partition's first unfiltered.
     * At the config's 4 KiB granularity the first block cut falls around row 17, well inside the
     * range covering rows 0-249, so {@code BtiCursorIndexWriter.blockStartOpenMarker} starts LIVE,
     * is replaced with the range's deletion at the first cut, and is back to LIVE well before the
     * last: the whole open-marker-across-a-cut cycle. A writer that carried an open marker across
     * a partition boundary, or that recorded the marker open at the START of a block rather than
     * at the end of the previous one, has to produce different index bytes here.
     * <p>
     * Cannot see: the recorded marker itself. {@code IndexInfo.openDeletion} is not exposed by any
     * reader; the byte comparison of the index component is what pins it.
     */
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
        assertIndexedCursorOutput(cfs);
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
        // intra-partition seeks. Byte-equality pins this through the index component only while the
        // reference stays correct, so the promotion is stated here directly.
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
        AbstractRowIndexEntry entry = sstable.getRowIndexEntry(sstable.decorateKey(ByteBufferUtil.bytes(pk)),
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
     * Partition keys of 100s to 1000s of bytes. Every other scenario in this suite uses an 8-byte
     * bigint pk, which is the smallest possible key.
     *
     * A partition key is length-prefixed with an unsigned SHORT, and not with a vint, so it has no
     * 128-byte encoding boundary. Clustering and cell values do have one. This scenario therefore
     * pins that a large key round-trips through the cursor's partition-key copy, compare and index
     * paths, which nothing else in this suite reaches.
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
        // The keys must really be large. A smaller pks array would leave the scenario passing
        // while its name and javadoc still claim 100s to 1000s of bytes.
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
     * Empty clustering values on a DESC (reversed) clustering column. The randomized soak reaches
     * this shape with seed 99303954147053.
     *
     * Every base type sorts empty before values, so a reversed column sorts empty AFTER values.
     * ReversedType swaps the operands around the base comparison. A raw clustering comparison that
     * reads empty-vs-valued from the serialized flag bits alone ignores that reversal, and then:
     *  - same-partition variant: rows with empty and valued clusterings for the SAME pk in
     *    different sstables merge in the wrong order (Data.db divergence — corruption class);
     *  - cross-partition variant: the global covered-clustering max picks the wrong row
     *    (Statistics.db divergence).
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
     * Row-level TTL merged against cell-level TTL and against plain writes. An INSERT USING TTL
     * sets a liveness TTL and cell TTLs; an UPDATE USING TTL sets only cell TTLs.
     *
     * The scenario also writes same-timestamp expiring-vs-expiring pairs whose TTLs differ.
     * CellLivenessInfo.resolveSameTimestampTie settles those on its greater-localDeletionTime
     * branch, because both sides carry one. The GREATER wins: here the TTL 100000 cell, written in
     * the EARLIER sstable.
     *
     * The branch below it takes equal expiration times and differing TTLs, and gives the tie to the
     * LOWER TTL. This scenario does NOT cover it. Expiration is nowInSec + ttl, so reaching that
     * branch from CQL against a live clock needs the two same-timestamp writes separated by EXACTLY
     * the TTL difference in wall-clock seconds. Any other spacing leaves the expirations differing,
     * and the greater-expiration branch decides first.
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
     * Expiring-vs-live cells at the SAME timestamp, in both directions across sstables. The
     * CASSANDRA-14592 rule gives the tie to an expiring or deleted cell, whatever the values are.
     * CellLivenessInfo.resolveSameTimestampTie holds that rule in its
     * tombstone-or-expiring-beats-live branch. The timestampTies scenario covers only live-vs-live
     * and delete-vs-live, so this scenario is what pins the rule at the differential level.
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
     * Cell TOMBSTONE vs EXPIRING cell at the SAME timestamp. The shared decision table
     * (CellLivenessInfo.resolveSameTimestampTie, which Cells.resolveRegular defers to) gives the
     * tombstone the tie, BEFORE any localDeletionTime comparison.
     *
     * This is the one same-timestamp pairing where both sides carry a localExpirationTime: the
     * tombstone carries its deletion second, and the expiring cell carries its expiry second. A
     * resolver that classifies "tombstone" by the presence of a localExpirationTime, rather than by
     * the absence of a TTL, never reaches that branch. It falls through to the ldt compare, which
     * the expiring cell's future expiry second wins, and the deleted data comes back until the TTL
     * lapses.
     *
     * The scenario covers both flush orders and both tombstone shapes (UPDATE SET v = null,
     * DELETE v) across distinct partitions.
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
     * Fixed-length values LARGER than the cursor's 4KiB copy buffer. A {@code vector<float, 1536>}
     * is 6144 bytes, which is a mainstream embedding width. A fixed-length value carries no length
     * vint, so the copy cannot be driven off the wire length.
     *
     * The scenario covers:
     *  - a value spanning several chunks;
     *  - a value landing exactly on the buffer boundary ({@code vector<float, 1024>} is 4096);
     *  - a same-timestamp tie ACROSS sstables, so the value also travels through the compactor's
     *    temp buffers, and not straight to the writer;
     *  - a null overwrite of an oversized column.
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
     * More than 64 regular columns: a row that lacks columns switches from the 64-bit-mask
     * column-subset encoding to the structurally different large-subset wire format.
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

    private void applyListCell(TableMetadata metadata, long pk, long ck, ByteBuffer path, String value, long timestamp)
    {
        ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes("l"));
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(LongType.instance.decompose(ck)));
        builder.addCell(BufferCell.live(column, timestamp, UTF8Type.instance.decompose(value),
                                        CellPath.create(path)));
        new Mutation(PartitionUpdate.singleRowUpdate(metadata, LongType.instance.decompose(pk), builder.build())).apply();
    }

    /** Same construction as CursorCellPathOrderingTest.timeUuid. */
    private static ByteBuffer listTimeUuid(long msb)
    {
        ByteBuffer uuid = ByteBuffer.allocate(16);
        uuid.putLong(msb);
        uuid.putLong(0x8080808080808080L);
        uuid.flip();
        return uuid;
    }

    /**
     * The empty-collection assignment, {@code SET l = []} and {@code SET m = {}}: a complex
     * deletion with no cells behind it. The set form was covered; the list and map forms were not.
     */
    @Test
    public void emptyCollectionAssignments() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, l list<text>, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET l = l + ['doomed-l'], m = m + {'k': 'doomed-m'}, " +
                    "v = ? WHERE pk = 0 AND ck = ?", "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET l = [], m = {} WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the empty-list assignment must shadow the earlier element",
                     0, countOccurrences(json, cellValue("doomed-l")));
        assertEquals("the empty-map assignment must shadow the earlier entry",
                     0, countOccurrences(json, cellValue("doomed-m")));
        for (long ck = 0; ck < 6; ck++)
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
    }

    /**
     * A map key past the 128-byte boundary where the cell path's length vint grows from one byte to
     * two, and a key just below it. The longest key elsewhere in the suite is about 114 bytes, so
     * the two-byte form was never written.
     */
    @Test
    public void mapKeysAcrossTheVintLengthBoundary() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String shortKey = repeat('s', 127);
        String longKey = repeat('l', 128);
        String longerKey = repeat('x', 5000);

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m[?] = ?, m[?] = ?, m[?] = ?, v = ? " +
                    "WHERE pk = 0 AND ck = ?",
                    shortKey, "under" + ck, longKey, "at" + ck, longerKey, "over" + ck, "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET m[?] = ? WHERE pk = 0 AND ck = ?",
                    longKey, "rewritten" + ck, ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
        {
            assertTrue("a 127-byte key must survive, ck " + ck, json.contains(cellValue("under" + ck)));
            assertTrue("a 5000-byte key must survive, ck " + ck, json.contains(cellValue("over" + ck)));
            assertTrue("the 128-byte key must take the newer value, ck " + ck,
                       json.contains(cellValue("rewritten" + ck)));
            assertFalse("the 128-byte key kept its older value, ck " + ck,
                        json.contains(cellValue("at" + ck)));
        }
    }

    /** Multi-byte map keys, so the cell path's byte comparison is not an ASCII comparison. */
    @Test
    public void nonAsciiMapKeysMergeAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Chosen so UTF-8 byte order and Java char order disagree, and so one key is a byte-prefix
        // of another.
        String[] keys = { "é", "é", "中文", "中", "😀", "z" };

        for (long ck = 0; ck < 6; ck++)
        {
            for (int i = 0; i < keys.length; i++)
                execute("UPDATE %s USING TIMESTAMP 1000 SET m[?] = ? WHERE pk = 0 AND ck = ?",
                        keys[i], "first" + i + "-" + ck, ck);
            execute("UPDATE %s USING TIMESTAMP 1000 SET v = ? WHERE pk = 0 AND ck = ?", "row" + ck, ck);
        }
        flush();

        for (long ck = 0; ck < 6; ck++)
            for (int i = 0; i < keys.length; i += 2)
                execute("UPDATE %s USING TIMESTAMP 2000 SET m[?] = ? WHERE pk = 0 AND ck = ?",
                        keys[i], "second" + i + "-" + ck, ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
            for (int i = 0; i < keys.length; i++)
                if (i % 2 == 0)
                {
                    assertTrue("rewritten multi-byte key lost its newer value, key " + i + " ck " + ck,
                               json.contains(cellValue("second" + i + "-" + ck)));
                    assertFalse("rewritten multi-byte key kept its older value, key " + i + " ck " + ck,
                                json.contains(cellValue("first" + i + "-" + ck)));
                }
                else
                {
                    assertTrue("untouched multi-byte key lost its value, key " + i + " ck " + ck,
                               json.contains(cellValue("first" + i + "-" + ck)));
                }
    }

    /**
     * Negative {@code int32} map keys merged in a real compaction. Int32Type does not sort in
     * unsigned byte order, so a cursor comparing paths as raw bytes orders these wrongly.
     * CursorCellPathOrderingTest pins comparePaths directly, but no compaction ever merged two
     * sstables holding the same negatively-keyed entry.
     */
    @Test
    public void negativeIntegerMapKeysMergeAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<int, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int[] keys = { Integer.MIN_VALUE, -1000, -1, 0, 1, Integer.MAX_VALUE };

        for (long ck = 0; ck < 6; ck++)
        {
            for (int i = 0; i < keys.length; i++)
                execute("UPDATE %s USING TIMESTAMP 1000 SET m[?] = ? WHERE pk = 0 AND ck = ?",
                        keys[i], "first" + i + "-" + ck, ck);
            execute("UPDATE %s USING TIMESTAMP 1000 SET v = ? WHERE pk = 0 AND ck = ?", "row" + ck, ck);
        }
        flush();

        for (long ck = 0; ck < 6; ck++)
            for (int i = 0; i < keys.length; i += 2)
                execute("UPDATE %s USING TIMESTAMP 2000 SET m[?] = ? WHERE pk = 0 AND ck = ?",
                        keys[i], "second" + i + "-" + ck, ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
            for (int i = 0; i < keys.length; i++)
                if (i % 2 == 0)
                {
                    assertTrue("rewritten negative key lost its newer value, key " + keys[i] + " ck " + ck,
                               json.contains(cellValue("second" + i + "-" + ck)));
                    assertFalse("rewritten negative key kept its older value, key " + keys[i] + " ck " + ck,
                                json.contains(cellValue("first" + i + "-" + ck)));
                }
                else
                {
                    assertTrue("untouched negative key lost its value, key " + keys[i] + " ck " + ck,
                               json.contains(cellValue("first" + i + "-" + ck)));
                }
    }

    /**
     * Nested collections, which appear nowhere in the deterministic corpus: only the fuzz generator
     * can produce one, by chance. The inner collection is frozen, so it is one opaque value inside
     * the outer collection's cell, and the outer collection is still multi-cell.
     */
    @Test
    public void nestedCollectionsAcrossSSTables() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, frozen<list<int>>>, " +
                    "l list<frozen<set<int>>>, s set<frozen<map<text, int>>>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m = m + {'a': [1, 2, 3]}, " +
                    "l = l + [{4, 5}], s = s + {{'n': 6}}, v = ? WHERE pk = 0 AND ck = ?",
                    "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 SET m = m + {'a': [7, 8]}, " +
                    "l = l + [{9}] WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
        assertEquals("the rewritten nested map entry must merge to one cell per row",
                     6, countOccurrences(json, "\"a\""));
    }

    /**
     * A complex deletion sitting INSIDE a range tombstone that opens before it and closes after it.
     * Every other scenario places its complex deletions outside the range tombstone's span, and the
     * scenarios with overlapping or open-ended range tombstones use tables with no collection.
     */
    @Test
    public void complexDeletionBracketedByARangeTombstone() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, text>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 12; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET m['k'] = ?, v = ? WHERE pk = 0 AND ck = ?",
                    "cell" + ck, "row" + ck, ck);
        flush();

        // The complex deletion at ck = 5 sits strictly inside the range below.
        execute("DELETE m FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck = 5");
        flush();

        execute("DELETE FROM %s USING TIMESTAMP 3000 WHERE pk = 0 AND ck >= 2 AND ck < 8");
        flush();

        // A resurrecting write above the range tombstone, inside its span, so the merge must order
        // the range tombstone, the complex deletion and this cell against each other.
        execute("UPDATE %s USING TIMESTAMP 4000 SET m['k'] = ?, v = ? WHERE pk = 0 AND ck = 5",
                "resurrected", "row-resurrected");
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 2; ck < 8; ck++)
            assertFalse("a cell under the range tombstone survived at ck " + ck,
                        json.contains(cellValue("cell" + ck)));
        for (long ck = 0; ck < 2; ck++)
            assertTrue("a cell outside the range tombstone was lost at ck " + ck,
                       json.contains(cellValue("cell" + ck)));
        for (long ck = 8; ck < 12; ck++)
            assertTrue("a cell outside the range tombstone was lost at ck " + ck,
                       json.contains(cellValue("cell" + ck)));
        assertTrue("the write above the range tombstone must survive",
                   json.contains(cellValue("resurrected")));
    }

    /**
     * A whole-UDT column delete compared through the differential harness. The one existing
     * scenario that deletes a UDT column deliberately bypasses the harness and checks through CQL,
     * and the wide-table deletion loop strides past every UDT column onto maps.
     */
    @Test
    public void wholeUdtColumnDeleteAcrossSSTables() throws Exception
    {
        String udt = createType("CREATE TYPE %s (f1 text, f2 text)");
        createTable("CREATE TABLE %s (pk bigint, ck bigint, u " + udt + ", v text, " +
                    "PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET u.f1 = ?, u.f2 = ?, v = ? WHERE pk = 0 AND ck = ?",
                    "doomed1-" + ck, "doomed2-" + ck, "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 6; ck += 2)
            execute("DELETE u FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck = ?", ck);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        for (long ck = 0; ck < 6; ck++)
        {
            boolean deleted = ck % 2 == 0;
            assertEquals("UDT field f1 at ck " + ck, deleted ? 0 : 1,
                         countOccurrences(json, cellValue("doomed1-" + ck)));
            assertEquals("UDT field f2 at ck " + ck, deleted ? 0 : 1,
                         countOccurrences(json, cellValue("doomed2-" + ck)));
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
        }
    }

    /**
     * A frozen collection deleted and a frozen collection expiring by TTL. frozenCollections covers
     * a delete of the frozen MAP column only, and no frozen collection anywhere carries a TTL.
     */
    @Test
    public void frozenCollectionDeleteAndTtl() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, fs frozen<set<text>>, " +
                    "fl frozen<list<text>>, v text, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 1000 SET fs = {'doomed-fs'}, fl = ['doomed-fl'], v = ? " +
                    "WHERE pk = 0 AND ck = ?", "row" + ck, ck);
        flush();

        for (long ck = 0; ck < 3; ck++)
            execute("DELETE fs FROM %s USING TIMESTAMP 2000 WHERE pk = 0 AND ck = ?", ck);
        for (long ck = 3; ck < 6; ck++)
            execute("UPDATE %s USING TIMESTAMP 2000 AND TTL 1 SET fl = ['expiring-fl'] WHERE pk = 0 AND ck = ?", ck);
        flush();

        long pinnedNow = FBUtilities.nowInSeconds() + 60;
        assertSomethingExpiredAt(cfs, pinnedNow);
        String json = allJson(assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(),
                                                          taskWithFixedNow(pinnedNow),
                                                          cfs.getDefaultGcBefore(pinnedNow)));
        assertEquals("the deleted frozen set must not survive in the rows that deleted it",
                     3, countOccurrences(json, "doomed-fs"));
        assertEquals("the expired frozen list must not survive",
                     0, countOccurrences(json, "expiring-fl"));
        for (long ck = 0; ck < 6; ck++)
            assertEquals("row column missing at ck " + ck, 1, countOccurrences(json, cellValue("row" + ck)));
    }

    // ------------------------------------------------------------------------------------------
    // Scenarios below exist for the ROW INDEX, i.e. for partitions above column_index_size.
    //
    // Every one of them is here because ClusteringDescriptorPrefixView.parse — the only
    // cursor-specific input to a BTI row trie — ran in exactly one shape before them: a single
    // `ck bigint`, fixed width, never null, never empty, never a second component. parse only runs
    // from BtiCursorIndexWriter.addIndexBlock, which only runs at a block cut, which only happens
    // above column_index_size (4 KiB in test/conf/cassandra.yaml), so a scenario that stays under
    // that threshold exercises none of it however exotic its clustering is.
    //
    // The oracles are: the harness byte comparison over Rows.db / Partitions.db (or Index.db under
    // BIG) for the written bytes, assertEveryRowReadableThroughASlice for retrievability, and the
    // absolute blockCount assertions here for the block arithmetic. A block count is stated
    // outright wherever the shape makes it computable, because byte equality is blind to a rule
    // both pipelines get wrong.
    // ------------------------------------------------------------------------------------------

    /**
     * Granularity the two one-byte-resolution sweeps below run at. 1 KiB is the smallest
     * column_index_size the config accepts, and a sweep pays one partition per byte, so the
     * smallest granularity is the cheapest place to bracket a cut.
     */
    private static final int SWEEP_GRANULARITY_KIB = 1;
    private static final int SWEEP_GRANULARITY = SWEEP_GRANULARITY_KIB * 1024;

    /**
     * How many one-byte padding steps a block-boundary sweep walks, ending one byte short of
     * {@link #SWEEP_GRANULARITY}. It has to exceed the per-row serialization overhead — row flags,
     * clustering, the body and previous-body length vints, the liveness delta and the cell header,
     * comfortably under 40 bytes today — so the sweep straddles the cut rather than sitting wholly
     * on one side of it; the marker sweep additionally needs it to exceed that plus one range
     * tombstone boundary marker. Both sweeps fail with an explicit "widen this" message if the
     * value ever stops being enough, so a serialization change that outgrows it is a loud failure
     * and not a silently vacuous test.
     */
    private static final int SWEEP_BYTES =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_BLOCK_BOUNDARY_SWEEP.getInt();

    /** Named in every sweep failure message, so a failure says which knob to turn. */
    private static final String SWEEP_PROPERTY =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_BLOCK_BOUNDARY_SWEEP.getKey();

    /**
     * The single cursor-written sstable the cross-generation rung leaves live, with the absolute
     * assertion that at least one of its partitions really carried a promoted row index.
     * <p>
     * Every scenario in this section is defined by crossing column_index_size. A change to row
     * sizing, to the config, or to a merge rule that quietly dropped a scenario back under the
     * threshold would leave it passing while testing an unindexed partition, which is the shape
     * the rest of this class already covers to death. The non-zero return is what says the index
     * was exercised at all.
     */
    private SSTableReader assertIndexedCursorOutput(ColumnFamilyStore cfs)
    {
        assertEquals("the cross-generation rung should leave one cursor-produced output",
                     1, cfs.getLiveSSTables().size());
        SSTableReader output = cfs.getLiveSSTables().iterator().next();
        assertTrue("no partition of the cursor-written output carries a promoted row index: this " +
                   "scenario has stopped crossing column_index_size and now says nothing about the " +
                   "row index at all",
                   assertEveryRowReadableThroughASlice(output) > 0);
        return output;
    }

    /**
     * A single {@code blob} clustering above the block threshold, whose largest value is a run of
     * {@code 0xFF} bytes and one of whose block-cutting values ends in {@code 0x00}.
     * <p>
     * What it covers, stated as what it reaches rather than as what it is named for:
     * <ul>
     * <li>The vint length branch of {@code ClusteringDescriptorPrefixView.parse} for a single
     *     variable-width component, which the tree's {@code ck bigint} partitions never touch.
     *     {@code UTF8Type} and {@code BytesType} are both variable width and the type identity does
     *     not change the walk, so a {@code text} clustering of this shape is not written as a
     *     second scenario; {@code partitionEndingOnABlockCutHasNoTailBlock} below runs one anyway,
     *     over a 200-byte shared prefix.</li>
     * <li>The {@code 0x00} escape {@code ByteSource.escaped} performs on the way into a trie
     *     separator. A clustering only reaches a separator as a block's FIRST or LAST, the two
     *     positions {@code snapshotOf} is applied to, so the row carrying the trailing
     *     {@code 0x00} clustering takes a value larger than the granularity and cuts a block by
     *     itself, making it both. {@code blob} reaches this and {@code 0xFF} in one table;
     *     {@code text} cannot hold {@code 0xFF} at all.</li>
     * <li>Aimed at, but NOT asserted: the {@code 0xFF} arm of {@code RowIndexWriter.nudge}, which
     *     returns the byte unchanged rather than incrementing it. The partition's maximum is
     *     {@code 0xFF} followed by eight more {@code 0xFF} bytes, so the branch fires if the
     *     preceding separator diverges exactly on the order byte, and that position is decided by
     *     where {@code prevSep} left {@code prevMax}.</li>
     * </ul>
     * <p>
     * Cannot see: that the nudge branch fired. Nothing counts it, and a nudge that silently
     * produced a separator no greater than the maximum still yields an ordered trie, so
     * {@code IncrementalTrieWriterBase.add}'s order assertion stays quiet. The byte comparison pins
     * that both pipelines nudged alike; the slice read-back pins that the result routes.
     */
    @Test
    public void blobClusteringCrossingIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck blob, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            // orders 0xD8..0xFF, so the partition's maximum clustering — the one nudge() is applied
            // to — is 0xFF followed by eight more 0xFF bytes
            for (int order = 0xD8; order <= 0xFF; order++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                        1L, blobClustering(order, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF),
                        padding + "-" + round);
            // trailing 0x00 bytes, kept at low order bytes so they cannot become the maximum
            for (int order = 0xD8; order < 0xDD; order++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                        1L, blobClustering(order, 0x00, 0x00), padding + "-" + round);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                        1L, blobClustering(order, 0x01, 0x00), padding + "-" + round);
            }
            // one clustering ending in 0x00 whose value exceeds the 4 KiB granularity, so it cuts
            // a block on its own and is that block's FIRST and LAST: the escape reaches a trie
            // separator by design here, not by wherever the cuts happened to fall. Order 0xDD is
            // below 0xFF, so it cannot become the partition's maximum.
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                    1L, blobClustering(0xDD, 0x00, 0x00), "e".repeat(5000) + "-" + round);
            for (int order = 0; order < 3; order++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                        2L, blobClustering(order, 0x01), "small-" + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
        SSTableReader output = assertIndexedCursorOutput(cfs);
        assertEquals("a partition well under column_index_size must not be promoted", 0, blockCount(output, 2L));
    }

    /** A blob clustering: a 16-byte shared prefix, one ordering byte, then {@code suffix}. */
    private static ByteBuffer blobClustering(int order, int... suffix)
    {
        byte[] bytes = new byte[16 + 1 + suffix.length];
        for (int i = 0; i < 16; i++)
            bytes[i] = 0x11;
        bytes[16] = (byte) order;
        for (int i = 0; i < suffix.length; i++)
            bytes[17 + i] = (byte) suffix[i];
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Three clustering columns above the block threshold, variable width then two fixed widths.
     * <p>
     * {@code compositeClustering} above builds the same column shape but ~800 bytes per partition,
     * so it never cuts a block. Here {@code ClusteringDescriptorPrefixView.parse} walks PAST
     * component 0: the {@code pos += len} advance after a variable-width component, and the
     * two-bit-per-component header shift, both run only for {@code i > 0}. A single wrong offset
     * there still yields an ordered separator, so the trie is written and misroutes.
     * <p>
     * {@code descendingClusteringCrossingIndexBlocks} below takes the SAME parse branches in the
     * opposite component order; it is kept for {@code ReversedType}, which is outside parse, not
     * for this walk. Both are named, deterministic shapes that fail loudly rather than drifting
     * out of a generator's range, which is what they hold over
     * {@code RandomDifferentialCompactionTest}'s generated clusterings.
     * <p>
     * Cannot see: more than 32 components, where parse reads a second header vint. No table
     * written by CQL in this class has that many clustering columns;
     * {@code RandomDifferentialCompactionTest} draws 33-36 on one example in four, and
     * {@code ClusteringDescriptorPrefixViewTest.parseMatchesTheSerializer} asserts its corpus
     * reached that branch.
     */
    @Test
    public void compositeClusteringCrossingIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck1 text, ck2 int, ck3 bigint, v text, " +
                    "PRIMARY KEY (pk, ck1, ck2, ck3))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String prefix = "p".repeat(48);
        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            for (String ck1 : new String[]{ prefix + "a", prefix + "b" })
                for (int ck2 = 0; ck2 < 5; ck2++)
                    for (long ck3 = 0; ck3 < 3; ck3++)
                        execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (?, ?, ?, ?, ?)",
                                1L, ck1, ck2, ck3, padding + "-" + round);
            for (long ck3 = 0; ck3 < 3; ck3++)
                execute("INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (?, ?, ?, ?, ?)",
                        2L, prefix + "a", 0, ck3, "small-" + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
        SSTableReader output = assertIndexedCursorOutput(cfs);
        assertEquals("a partition well under column_index_size must not be promoted", 0, blockCount(output, 2L));
    }

    /**
     * A {@code DESC} clustering column above the block threshold, paired with an {@code ASC} one.
     * <p>
     * Its parse branch set is the one {@code compositeClusteringCrossingIndexBlocks} above already
     * walks, in the opposite component order — fixed width then variable rather than the reverse —
     * and {@code parse} has no branch that separates the two orders. What it adds is outside
     * {@code parse} entirely:
     * {@code ClusteringComparator.asByteComparable} emits inverted bytes and
     * {@code NEXT_COMPONENT_EMPTY_REVERSED} for a {@link org.apache.cassandra.db.marshal.ReversedType}
     * component, and that is the encoding the row trie's separators are built from. The write side
     * builds its comparator from {@code SerializationHeader.clusteringTypes()} while the read side
     * uses {@code metadata.comparator}. No other DETERMINISTIC scenario pairs DESC with an indexed
     * partition: {@code descendingClustering} and {@code openEndedRangeTombstonesDescending} above
     * both stay under the threshold and so never reach a trie.
     * {@code BtiRandomDifferentialCompactionTest} does reach the shape, because its generator wraps
     * a clustering type in {@code ReversedType} on a coin flip, but only when the draw also lands a
     * hub partition across the drawn granularity; this scenario is the one that always does.
     * Mixing DESC with ASC means a comparator that inverted unconditionally fails here too.
     * <p>
     * Cannot see: a disagreement between the two comparators that both pipelines share. Both build
     * the write-side comparator the same way, so the byte comparison is blind to it; the slice
     * read-back, which goes through {@code metadata.comparator}, is what covers it.
     */
    @Test
    public void descendingClusteringCrossingIndexBlocks() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck1 bigint, ck2 text, v text, " +
                    "PRIMARY KEY (pk, ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 DESC, ck2 ASC)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String prefix = "p".repeat(48);
        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            for (long ck1 = 0; ck1 < 10; ck1++)
                for (int ck2 = 0; ck2 < 3; ck2++)
                    execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)",
                            1L, ck1, prefix + ck2, padding + "-" + round);
            for (long ck1 = 0; ck1 < 3; ck1++)
                execute("INSERT INTO %s (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)",
                        2L, ck1, prefix + "0", "small-" + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
        SSTableReader output = assertIndexedCursorOutput(cfs);
        assertEquals("a partition well under column_index_size must not be promoted", 0, blockCount(output, 2L));
    }

    /**
     * An EMPTY clustering component inside a multi-block partition, ascending.
     * <p>
     * {@code ClusteringDescriptorPrefixView.parse} has a null branch and an empty branch that no
     * test reaches, because the two scenarios in this class that write an empty clustering
     * ({@code emptyClusteringValuesAscending} and its DESC twin) build partitions far under
     * column_index_size. Landing an empty component in a trie separator takes more than writing
     * one: {@code snapshotOf} is only applied to a block's FIRST and LAST clustering. The
     * empty-clustering row therefore carries a value larger than the granularity, so it cuts a
     * block on its own and is necessarily both — under ASC it sorts first, so it is block 1
     * entire.
     * <p>
     * Cannot see: a null (as opposed to empty) clustering component. CQL cannot write one on a
     * single-column clustering; that branch stays unreached.
     */
    @Test
    public void emptyClusteringComponentCrossingIndexBlocksAscending() throws Exception
    {
        emptyClusteringComponentCrossingIndexBlocks(false);
    }

    /**
     * DESC twin of the above: the empty component sorts LAST, so it is the last block's last
     * clustering rather than the first block's first. That is the other of the two positions
     * {@code snapshotOf} is applied to, and under {@code ReversedType} it is also the value
     * {@code RowIndexWriter.complete} nudges.
     */
    @Test
    public void emptyClusteringComponentCrossingIndexBlocksDescending() throws Exception
    {
        emptyClusteringComponentCrossingIndexBlocks(true);
    }

    private void emptyClusteringComponentCrossingIndexBlocks(boolean descending) throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck text, v text, PRIMARY KEY (pk, ck))" +
                    (descending ? " WITH CLUSTERING ORDER BY (ck DESC)" : ""));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        // larger than the 4 KiB column_index_size, so the empty-clustering row cuts a block by itself
        String bigPadding = "e".repeat(5000);
        for (int round = 0; round < 2; round++)
        {
            for (int i = 0; i < 30; i++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                        1L, "c" + String.format("%04d", i), padding + "-" + round);
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)",
                    1L, ByteBufferUtil.EMPTY_BYTE_BUFFER, bigPadding + "-" + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
        assertIndexedCursorOutput(cfs);
    }

    /**
     * The tail-block decision, stated as four absolute block counts, over clusterings that share a
     * 200-byte prefix.
     * <p>
     * {@code BtiCursorIndexWriter.endPartition} cuts a trailing block only when a block is still
     * open, and BIG reaches the same decision from a tail size of more than the one end-of-partition
     * marker byte. {@code fixedLengthValuesLargerThanCopyBuffer} and
     * {@code mapKeysAcrossTheVintLengthBoundary} above already REACH the closed-block arm, because
     * every row of theirs exceeds the granularity, but neither asserts a block count, so a writer
     * that cut a tail unconditionally passes both. The pk 1 / pk 2 pair below is what separates the
     * branches.
     * <p>
     * The {@code text} clustering carries a 200-byte shared prefix, so this is also where the
     * separator chain runs deep: every {@code ByteComparable.separatorGt} result runs the whole
     * prefix before it diverges, and {@code RowIndexWriter.complete} walks that prefix to find its
     * nudge point. Neither has a length-dependent branch, so the prefix buys reach into the loops
     * rather than a new branch.
     * <p>
     * Each row here exceeds the 4 KiB granularity on its own, which makes the counts exact without
     * any arithmetic on the serialized row size:
     * <ul>
     * <li>pk 1, two big rows: both cut, nothing is left open, 2 blocks.</li>
     * <li>pk 2, two big rows and a small one: the small row leaves a block open, so a tail is cut,
     *     3 blocks. This is the pair that pins the branch — a writer that never cut a tail gives
     *     pk 2 two blocks, one that always cut gives pk 1 three.</li>
     * <li>pk 3, ONE big row: one cut, no tail, and a one-block index is not promoted at all
     *     (BTI's {@code finish} returns a -1 trie root, BIG's {@code totalBlocks <= 1} writes a
     *     plain entry), so 0.</li>
     * <li>pk 4, two small rows: never reaches the threshold, 0.</li>
     * </ul>
     * <p>
     * Cannot see: a partition that ends EXACTLY on a granularity multiple. Hitting that needs the
     * serialized size of a row, which no reader exposes; {@code blockCutBracketsTheGranularityCut}
     * below brackets it to one byte instead of naming it.
     */
    @Test
    public void partitionEndingOnABlockCutHasNoTailBlock() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck text, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // shared by every clustering, so each separator runs 200 bytes deep before it diverges.
        // The suffix is zero-padded, so lexicographic order is the numeric order the counts assume.
        String prefix = "p".repeat(200);
        String big = "x".repeat(4500);   // one row > the 4 KiB column_index_size
        String small = "s".repeat(50);
        for (int round = 0; round < 2; round++)
        {
            for (int ck = 0; ck < 2; ck++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, clustering(prefix, ck), big + "-" + round);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, clustering(prefix, ck), big + "-" + round);
            }
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, clustering(prefix, 2), small + "-" + round);
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 3L, clustering(prefix, 0), big + big + "-" + round);
            for (int ck = 0; ck < 2; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 4L, clustering(prefix, ck), small + "-" + round);
            flush();
        }

        assertCursorMatchesIteratorAcrossGenerations(cfs);
        SSTableReader output = assertIndexedCursorOutput(cfs);
        assertEquals("both rows exceed column_index_size, so the last one ends a block and no tail " +
                     "remains to cut", 2, blockCount(output, 1L));
        assertEquals("the trailing small row leaves a block open, so endPartition must cut a tail",
                     3, blockCount(output, 2L));
        assertEquals("a single row cuts one block and leaves no tail, and a one-block index is never " +
                     "promoted", 0, blockCount(output, 3L));
        assertEquals("a partition under column_index_size must not be promoted", 0, blockCount(output, 4L));
    }

    /** A {@code text} clustering: a long shared prefix, then a zero-padded suffix that orders. */
    private static String clustering(String prefix, int suffix)
    {
        return prefix + String.format("%04d", suffix);
    }

    /**
     * Brackets the granularity cut to a single byte, by sweeping the row size across it.
     * <p>
     * The exact shapes this stands in for — a partition of exactly N granularities, of a
     * granularity plus one byte, a single row of exactly the block size — need the serialized size
     * of a row, which is a function of the row flags, the clustering encoding, two length vints, the
     * liveness delta against the sstable's encoding stats and the cell header. No reader exposes it,
     * and guessing it would give a scenario that CLAIMS to sit on the cut and does not. This sweeps
     * instead: one partition per padding length, one byte apart, so the exact boundary is somewhere
     * inside and the shape of the crossing is asserted rather than its position.
     * <p>
     * Every partition holds two rows of the same padding. Row 1 cuts iff its serialized size reaches
     * the granularity; if it does, row 2 is at least as large and cuts too, giving 2 blocks with no
     * tail. If row 1 does not cut, the two together do, giving one block — never promoted, reported
     * as 0. So the count is 0 below the cut and 2 at or above it, and the serialized size is
     * monotone in the padding, so the sweep must show one step and no other value. A fixed
     * {@code USING TIMESTAMP} keeps the liveness delta constant, so the row size is a function of
     * the padding alone.
     * <p>
     * Runs at {@link #SWEEP_GRANULARITY_KIB} KiB rather than the config's 4 KiB purely for cost:
     * the sweep pays one partition per byte either way.
     * <p>
     * Cannot see: WHICH padding sits exactly on the cut, only that exactly one does. And it says
     * nothing about a writer whose cut is off by a constant — the step would simply move, and this
     * asserts the step's shape, not its position.
     */
    @Test
    public void blockCutBracketsTheGranularityCut() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int previousGranularity = DatabaseDescriptor.getColumnIndexSizeInKiB();
        // set BEFORE the compaction, not before the schema: BtiCursorIndexWriter reads
        // column_index_size once, in its constructor
        DatabaseDescriptor.setColumnIndexSizeInKiB(SWEEP_GRANULARITY_KIB);
        try
        {
            for (int step = 0; step < SWEEP_BYTES; step++)
            {
                String padding = "x".repeat(SWEEP_GRANULARITY - SWEEP_BYTES + step);
                for (long ck = 0; ck < 2; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000",
                            (long) step, ck, padding);
            }
            flush();
            // a second input, so this is a merge and not a single-sstable rewrite
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", -1L, 0L, "control");
            flush();

            assertCursorMatchesIteratorAcrossGenerations(cfs);
            SSTableReader output = assertIndexedCursorOutput(cfs);

            assertEquals("the sweep starts ABOVE the cut, so it does not bracket it: the shortest " +
                         "padding already cuts a block. Widen " + SWEEP_PROPERTY,
                         0, blockCount(output, 0));
            assertEquals("the sweep ends BELOW the cut, so it does not bracket it: even the longest " +
                         "padding never reaches " + SWEEP_GRANULARITY + " serialized bytes. Widen " +
                         SWEEP_PROPERTY,
                         2, blockCount(output, SWEEP_BYTES - 1));

            int steps = 0;
            int previousCount = 0;
            for (int step = 0; step < SWEEP_BYTES; step++)
            {
                int count = blockCount(output, step);
                assertTrue("padding step " + step + " gave " + count + " blocks: two rows can cut at " +
                           "most one block each, and a one-block index is never promoted, so 0 and 2 " +
                           "are the only counts reachable here",
                           count == 0 || count == 2);
                if (count != previousCount)
                {
                    assertEquals("the promoted block count FELL as the rows grew, at padding step " +
                                 step + ": the serialized row size is monotone in the padding, so the " +
                                 "cut cannot un-fire", 2, count);
                    steps++;
                }
                previousCount = count;
            }
            assertEquals("the block count crossed the cut more than once, so the serialized row size " +
                         "is not monotone in the padding and the byte the cut sits on is not bracketed",
                         1, steps);
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(previousGranularity);
        }
    }

    /**
     * Puts a range tombstone BOUNDARY marker at the end of an index block, by sweeping the row
     * before it across the cut.
     * <p>
     * {@code SSTableCursorWriter.writeRangeTombstone} sets the open marker to a boundary's
     * {@code deletionTime2} — the deletion of the range that OPENS there — and when a block is cut
     * on that marker, that is the value {@code addIndexBlock} carries into the NEXT block's
     * {@code IndexInfo}. Nothing in the tree lands a boundary marker at a cut by design; the wide
     * partitions that hold both do it by whatever the layout happened to be.
     * <p>
     * Each partition is: the open bound of [0,1), a row of swept padding, the boundary at 1, a row
     * larger than the granularity, and the close bound of [1,3). The rows are written in one
     * sstable and the two ranges in another, so the boundary is formed by the MERGE. The block
     * count is 2 while neither the open bound nor the first row reaches the cut, and 3 once
     * something does. The first padding at which it becomes 3 is necessarily a partition where the
     * BOUNDARY MARKER, not the row, ended block 1: the step happens the moment
     * {@code openBound + row + marker} reaches the granularity, and one padding byte earlier
     * {@code openBound + row} was already below it by at least a marker's width. So the sweep
     * contains at least one such partition by construction, not by luck.
     * <p>
     * Cannot see: which partition that is, or that the {@code IndexInfo} carried the right
     * deletion. The byte comparison of Rows.db / Index.db is the oracle for the value; this
     * scenario's job is only to make the shape occur.
     */
    @Test
    public void blockCutLandsOnARangeTombstoneBoundaryMarker() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        int previousGranularity = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(SWEEP_GRANULARITY_KIB);
        try
        {
            // comfortably over the granularity on its own, so it always cuts, and large enough
            // that a three-block partition clears the harness's own
            // "length >= (blocks - 1) * granularity" bound with room
            String trailing = "y".repeat(SWEEP_GRANULARITY + 512);
            for (int step = 0; step < SWEEP_BYTES; step++)
            {
                String padding = "x".repeat(SWEEP_GRANULARITY - SWEEP_BYTES + step);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 3000",
                        (long) step, 0L, padding);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 3000",
                        (long) step, 1L, trailing);
            }
            flush();

            // two abutting ranges with DIFFERENT deletion times: equal ones would merge into a
            // single range and produce no boundary marker at all. Both are older than the rows, so
            // the rows survive and the markers stay.
            for (int step = 0; step < SWEEP_BYTES; step++)
            {
                execute("DELETE FROM %s USING TIMESTAMP 1000 WHERE pk = ? AND ck >= ? AND ck < ?",
                        (long) step, 0L, 1L);
                execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ? AND ck >= ? AND ck < ?",
                        (long) step, 1L, 3L);
            }
            flush();

            assertCursorMatchesIteratorAcrossGenerations(cfs);
            SSTableReader output = assertIndexedCursorOutput(cfs);

            assertEquals("the sweep starts ABOVE the cut: the shortest padding already ends block 1 " +
                         "before the trailing row, so the step this scenario relies on is outside the " +
                         "sweep. Widen " + SWEEP_PROPERTY,
                         2, blockCount(output, 0));
            assertEquals("the sweep ends BELOW the cut: even the longest padding leaves block 1 open " +
                         "until the trailing row, so no partition ended a block on the boundary " +
                         "marker. Widen " + SWEEP_PROPERTY,
                         3, blockCount(output, SWEEP_BYTES - 1));

            int steps = 0;
            int previousCount = 2;
            for (int step = 0; step < SWEEP_BYTES; step++)
            {
                int count = blockCount(output, step);
                assertTrue("padding step " + step + " gave " + count + " blocks; the trailing row " +
                           "always cuts and the close bound always leaves a tail, so the only counts " +
                           "reachable here are 2 (nothing cut before the trailing row) and 3",
                           count == 2 || count == 3);
                if (count != previousCount)
                {
                    assertEquals("the promoted block count FELL as the first row grew, at padding step " +
                                 step, 3, count);
                    steps++;
                }
                previousCount = count;
            }
            assertEquals("the block count crossed the cut more than once, so the step from 2 to 3 does " +
                         "not identify the partitions whose block 1 ended on the boundary marker",
                         1, steps);
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(previousGranularity);
        }
    }

    /**
     * An INDEXED partition carrying a non-LIVE partition-level deletion.
     * <p>
     * {@code TrieIndexEntry.serialize} writes a partition deletion time into the index entry, and
     * only for an indexed entry; BIG's promoted entry has the same field. Partition-level deletes
     * exist elsewhere in the suite but always on small partitions, so neither field has ever been
     * written non-LIVE, in either format. That also leaves the eager serialization in
     * {@code BtiTableWriter.IndexWriter.append} unpinned — the caller hands it a REUSED
     * {@code DeletionTime} instance, and the entry is correct only because it is serialized before
     * the call returns.
     * <p>
     * pk 1 is deleted between two rounds of inserts, so the deletion survives compaction (it is not
     * purgeable inside gc_grace) while the later rows survive it, leaving a partition that is both
     * indexed and deleted. pk 2 is the same shape without the delete, so the entry's deletion field
     * is asserted against both values and cannot pass as a constant.
     * <p>
     * The index entry's copy is read back directly here; the harness's
     * {@code assertPartitionDeletionReadableFromIndexEntry}, which compares it against the data
     * file's, is dormant until a scenario like this one exists.
     * <p>
     * Cannot see: a reused-instance defect that happens to reuse the SAME value. Both partitions'
     * deletions would have to differ within one sstable for that, which one partition delete
     * cannot arrange.
     */
    @Test
    public void indexedPartitionCarriesAPartitionDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        for (long pk = 1; pk <= 2; pk++)
            for (long ck = 0; ck < 30; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", pk, ck, padding + "-0");
        flush();

        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE pk = ?", 1L);
        flush();

        // re-inserted above the deletion, so pk 1 stays wide enough to be indexed
        for (long pk = 1; pk <= 2; pk++)
            for (long ck = 0; ck < 30; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 3000", pk, ck, padding + "-1");
        flush();

        assertCursorMatchesIteratorAcrossGenerations(cfs);
        SSTableReader output = assertIndexedCursorOutput(cfs);

        assertEquals("pk 1 must still cross column_index_size exactly once after the delete",
                     2, blockCount(output, 1L));
        assertEquals("pk 2 is the undeleted control and must be indexed the same way",
                     2, blockCount(output, 2L));

        AbstractRowIndexEntry deleted = output.getRowIndexEntry(output.decorateKey(ByteBufferUtil.bytes(1L)),
                                                                SSTableReader.Operator.EQ);
        assertNotNull("pk 1 lost its index entry", deleted);
        assertNotNull("an indexed entry must carry a partition deletion time", deleted.deletionTime());
        assertFalse("the index entry for a deleted partition reports a LIVE deletion: the entry's " +
                    "deletion field is the only copy a read takes when the column filter fetches no " +
                    "statics, so a partition delete lost here is a partition delete lost on read",
                    deleted.deletionTime().isLive());
        assertEquals("the index entry carries the wrong deletion timestamp",
                     2000L, deleted.deletionTime().markedForDeleteAt());

        AbstractRowIndexEntry undeleted = output.getRowIndexEntry(output.decorateKey(ByteBufferUtil.bytes(2L)),
                                                                  SSTableReader.Operator.EQ);
        assertNotNull("pk 2 lost its index entry", undeleted);
        assertTrue("the undeleted control partition's index entry reports a deletion, so the field is " +
                   "not being read from the partition at all",
                   undeleted.deletionTime().isLive());
    }

    /**
     * A DESIGNED partition at BTI's own default granularity, 16 KiB
     * ({@code BtiFormatPartitionWriter.DEFAULT_GRANULARITY}).
     * <p>
     * Every config in the tree sets column_index_size to 4 KiB — test/conf/cassandra.yaml,
     * test/conf/latest_diff.yaml and InstanceConfig alike. 16 KiB is not unreached, though:
     * {@code RandomDifferentialCompactionTest} draws its granularity per example from
     * {@code COLUMN_INDEX_SIZES_KIB}, one of whose eight entries is 16, and applies it immediately
     * before the compaction. What that soak does not do is state a designed count, so a partition
     * whose promotion decision moved between 4 KiB and 16 KiB would still leave it green. This
     * scenario names one. {@code BtiCursorIndexWriter} reads the granularity ONCE, in its
     * constructor, so it is set after the writes and before the compaction; setting it before
     * {@code createTable} would change nothing about the compaction under test.
     * <p>
     * pk 2 is what proves the setting took effect. It holds 30 padded rows, the shape
     * {@code partitionCrossingOneIndexBlock} above pins at exactly 2 blocks under the config's
     * 4 KiB. At 16 KiB it must not be promoted at all. Without that assertion this scenario would
     * pass identically if the setter were a no-op.
     * <p>
     * Cannot see: a granularity read at the wrong TIME. A writer that re-read column_index_size per
     * partition instead of per writer would behave identically here, because the value does not
     * change during the compaction.
     */
    @Test
    public void realBtiGranularity() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        String padding = "x".repeat(200);
        for (int round = 0; round < 2; round++)
        {
            for (long ck = 0; ck < 400; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, ck, padding + "-" + round);
            for (long ck = 0; ck < 30; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 2L, ck, padding + "-" + round);
            flush();
        }

        int previousGranularity = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(16);
        try
        {
            assertCursorMatchesIteratorAcrossGenerations(cfs);
            SSTableReader output = assertIndexedCursorOutput(cfs);
            // 400 rows carrying a 202-byte value each serialize to at least 82 KiB and at most
            // ~104 KiB, so the count is between 4 and 8 whatever the exact per-row overhead is. The
            // bound is deliberately loose: the assertion that matters is pk 2's zero below.
            int wide = blockCount(output, 1L);
            assertTrue("pk 1 is over 80 KiB and must cut at least four 16 KiB blocks, got " + wide,
                       wide >= 4);
            assertTrue("pk 1 is under 110 KiB and cannot cut more than eight 16 KiB blocks, got " + wide,
                       wide <= 8);
            assertEquals("a 30-row partition crosses 4 KiB but not 16 KiB: a 0 here is what says the " +
                         "granularity change reached the writer at all",
                         0, blockCount(output, 2L));
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(previousGranularity);
        }
    }

    /** {@code "c".repeat(n)}, spelled out because this suite targets a source level without it. */
    private static String repeat(char c, int n)
    {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++)
            sb.append(c);
        return sb.toString();
    }
}
