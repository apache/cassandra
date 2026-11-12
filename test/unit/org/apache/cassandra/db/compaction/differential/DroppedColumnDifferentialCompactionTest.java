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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.btree.BTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Dropped-column scenarios. At deserialization the iterator path discards cells and complex
 * deletions of a dropped column whose timestamp is at or before the drop.
 * UnfilteredSerializer.readSimpleColumn and readComplexColumn apply that rule through
 * DeserializationHelper.isDropped and isDroppedComplexDeletion. The cursor path must filter
 * identically, or dropped data survives cursor compaction and ALTER TABLE ... ADD of the same
 * column resurrects it.
 *
 * Timestamp determinism: ClientState timestamps increase strictly per node. A write executed
 * before ALTER TABLE DROP therefore carries a timestamp strictly below droppedTime. A write
 * that must survive the drop uses an explicit far-future USING TIMESTAMP.
 *
 * droppedComplexColumnSurvivingCells is @Ignore'd on CASSANDRA-21607: the iterator path throws for
 * that shape, so the harness cannot produce a reference run.
 */
public class DroppedColumnDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** Far in the future relative to any wall-clock droppedTime (year ~2100, microseconds). */
    private static final long FUTURE_TS = 4102444800_000000L;

    @Test
    public void droppedRegularColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 5; pk++)
            for (long ck = 0; ck < 10; ck++)
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "dropped-" + ck);
        flush();

        alterTable("ALTER TABLE %s DROP v2");

        // second generation written after the drop, overlapping the first
        for (long pk = 0; pk < 5; pk++)
            for (long ck = 5; ck < 15; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck + 100);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * Cells written with a timestamp ABOVE droppedTime survive compaction on both paths
     * (isDropped is timestamp-gated, not column-gated) — pins the comparison direction.
     */
    @Test
    public void cellsNewerThanDropRetained() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?) USING TIMESTAMP " + FUTURE_TS,
                    ck, ck, "survives-" + ck);
        // and some cells that do not survive
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v2) VALUES (1, ?, ?)", ck, "dropped-" + ck);
        flush();

        alterTable("ALTER TABLE %s DROP v2");

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (2, ?, ?)", ck, ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** DROP then ADD: pre-drop cells are filtered, post-re-add cells survive — the resurrection shape. */
    @Test
    public void droppedColumnReAdded() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck, "old-" + ck);
        flush();

        alterTable("ALTER TABLE %s DROP v2");
        alterTable("ALTER TABLE %s ADD v2 text");

        for (long ck = 5; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v2) VALUES (0, ?, ?)", ck, "new-" + ck);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * A pre-drop sstable that holds a complex deletion of {@code m} and no cell of it, from a
     * plain {@code DELETE m FROM ...}. {@link #droppedColumnReAdded} covers the pre-drop write
     * that holds cells; this one covers the deletion on its own.
     *
     * The re-add returns {@code m} to the schema, so
     * {@code CursorCompactor.unsupportedHeaderColumns} accepts the header and the cursor path
     * runs.
     */
    @Test
    public void droppedThenReaddedComplexColumnDeletionNotResurrected() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
            execute("DELETE m FROM %s WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        alterTable("ALTER TABLE %s DROP m");
        alterTable("ALTER TABLE %s ADD m map<text, bigint>");

        // second generation written after the re-add, overlapping the first
        for (long ck = 5; ck < 15; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck + 100);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * A dropped multi-cell column whose cells carry a timestamp ABOVE droppedTime, meeting a second row
     * version. This is CASSANDRA-21607, verbatim: the drop filter is timestamp-gated, so the cells survive
     * the read, and the merger then builds its column set from the schema, which no longer holds the
     * column. Row.Merger.ColumnDataReducer.getReduced then dereferences a null complexBuilder.
     *
     * The ITERATOR path throws. The differential harness therefore fails on its own reference run, at
     * DifferentialCompactionTester.compactPath, before the cursor path is reached. No change to the cursor
     * path can make this scenario pass.
     *
     * CASSANDRA-21607 fixes this by discarding a column that is dropped and not re-added, whatever
     * the cell timestamps are, so the merge never sees it and both paths agree. Remove the @Ignore
     * once that lands.
     */
    @Ignore("Blocked on CASSANDRA-21607")
    @Test
    public void droppedComplexColumnSurvivingCells() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text, bigint>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s USING TIMESTAMP " + FUTURE_TS + " SET v1 = ?, m = m + {'a': 1} WHERE pk = 0 AND ck = ?", ck, ck);
        flush();
        for (long ck = 0; ck < 10; ck++)
            execute("UPDATE %s USING TIMESTAMP " + (FUTURE_TS + 1) + " SET m = m + {'a': 2} WHERE pk = 0 AND ck = ?", ck);
        flush();

        alterTable("ALTER TABLE %s DROP m");

        assertCursorMatchesIterator(cfs);
    }

    @Test
    public void droppedStaticColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s text static, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < 5; pk++)
        {
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, "static-" + pk);
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
        }
        flush();

        alterTable("ALTER TABLE %s DROP s");

        for (long pk = 0; pk < 5; pk++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, 99L, 99L);
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /**
     * The dropped column was the table's ONLY static column, and its cells are above the drop time.
     * The iterator discards the whole static row when the schema has no static column
     * (UnfilteredRowIterators.mergeStaticRows), whatever the cell timestamps are. The cursor must do
     * the same.
     */
    @Test
    public void droppedOnlyStaticColumnSurvivingCells() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s text static, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        writeStaticAndRows(FUTURE_TS, "first-");
        flush();
        writeStaticAndRows(FUTURE_TS + 1, "second-");
        flush();

        alterTable("ALTER TABLE %s DROP s");

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the static row must be discarded once the table has no static column",
                     0, countOccurrences(json, "\"static_block\""));
        assertEquals("the regular rows must survive", 3 * 5, countOccurrences(json, "\"name\":\"v1\""));
    }

    /** As above, but the static column is re-added, so the schema has a static column again. */
    @Test
    public void droppedOnlyStaticColumnReAdded() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s text static, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // below the drop time, so the read discards these static cells
        writeStaticAndRows(null, "old-");
        flush();
        // above the drop time, so these survive
        writeStaticAndRows(FUTURE_TS, "future-");
        flush();

        alterTable("ALTER TABLE %s DROP s");
        alterTable("ALTER TABLE %s ADD s text static");

        for (long pk = 0; pk < 3; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, "new-" + pk);
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("a static row with surviving cells must be kept once the column is back",
                     3, countOccurrences(json, "\"static_block\""));
        assertEquals("static cells below the drop time must not survive", 0, countOccurrences(json, cellValue("old-0")));
    }

    /** One of two static columns is dropped, with cells above the drop time. The static row is kept. */
    @Test
    public void droppedStaticColumnWithAnotherStaticColumnKept() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, s text static, s2 text static, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 3; pk++)
            {
                execute("INSERT INTO %s (pk, s, s2) VALUES (?, ?, ?) USING TIMESTAMP " + (FUTURE_TS + round),
                        pk, "s-" + round + '-' + pk, "s2-" + round + '-' + pk);
                for (long ck = 0; ck < 5; ck++)
                    execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
            }
            flush();
        }

        alterTable("ALTER TABLE %s DROP s");

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the static row must be kept while the table has a static column",
                     3, countOccurrences(json, "\"static_block\""));
    }

    /**
     * Writes a static value and five rows into each of three partitions. A null timestamp writes
     * at the current time.
     */
    private void writeStaticAndRows(Long timestamp, String staticPrefix)
    {
        String using = timestamp == null ? "" : " USING TIMESTAMP " + timestamp;
        for (long pk = 0; pk < 3; pk++)
        {
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)" + using, pk, staticPrefix + pk);
            for (long ck = 0; ck < 5; ck++)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)" + using, pk, ck, ck);
        }
    }

    /**
     * A cell timestamped exactly {@link LivenessInfo#NO_TIMESTAMP}, in an sstable whose header carries a
     * dropped column. Long.MIN_VALUE is that sentinel, {@code DeletionTime.LIVE}'s
     * {@code markedForDeleteAt}, and the cursor's "no drop horizon" sentinel at the same time. One
     * compaction therefore walks the value into three independent collisions. The active deletion must not
     * shadow the cell at either of the merge's two deletion checks. The drop filter must not discard it
     * either. The iterator path keeps it on all counts.
     *
     * These are CALL-SITE pins, which is why this is a differential scenario and not only a unit test. A
     * rule-level test can gate the drop rule or the deletion rule in isolation. It still stays green if a
     * call site inlines the bare, unguarded comparison again. Different conditions reach those call sites,
     * so the scenario writes both shapes:
     *
     *  - the single-source check, after the merge chooses the winner, needs only one copy of the cell;
     *  - the cross-source check sits inside the merge loop's COMPARE arm. It needs two or more sources that
     *    carry the same column of the same row at an equal timestamp. Partitions 1 and 2 supply that, with
     *    the two values in opposite order. Without the guard the merge keeps whichever source it visits
     *    first, so one of the two partitions diverges whichever order that is.
     *
     * CQL cannot reach this timestamp. {@code cql3.RowUpdateBuilder}'s constructor rejects it for every
     * modification statement, and {@code QueryOptions} rejects it at the native protocol. Both reject it
     * because the engine uses that value for "absence of timestamp". The encoding does NOT block it. A cell
     * timestamp goes to disk as an unsigned vint delta from the header's {@code minTimestamp}, and the read
     * adds the delta back. The two operations are inverses mod 2^64 for any base, so the value round-trips
     * through an sstable exactly. {@code PartitionUpdate.simpleBuilder} carries no such guard, which is what
     * makes this cell constructible.
     */
    @Test
    public void noTimestampCellSurvivesBothSentinelCollisions() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // ordinary rows, so the sstable's timestamp stats do not consist solely of the sentinel, and so v2
        // exists on disk to be dropped below
        for (long ck = 0; ck < 10; ck++)
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (0, ?, ?, ?)", ck, ck, "v2-" + ck);

        // v1 is never dropped, and no deletion covers these rows. The first long argument is a
        // PARTITION KEY, not a timestamp: each sentinel cell lands in its own partition, disjoint from the
        // pk=0 rows above.
        writeSentinelTimestampCell(cfs, 1L, 7L);
        writeSentinelTimestampCell(cfs, 2L, 8L);
        flush();

        // makes THIS sstable's header carry a dropped column, so sstableHasDroppedColumns is true and the
        // horizon array is consulted for every cell read from it — v1's included
        alterTable("ALTER TABLE %s DROP v2");

        // the same two cells again with the values swapped, in a second sstable. Each of partitions 1 and 2
        // then has the sentinel cell in two sources, so the merge must compare their values
        writeSentinelTimestampCell(cfs, 1L, 8L);
        writeSentinelTimestampCell(cfs, 2L, 7L);

        for (long ck = 10; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
        flush();

        // Preconditions, both on ONE sstable. Asserting them separately would pass with the sentinel cells in
        // an sstable whose header carries no dropped column. That leaves the drop-filter half silently dead:
        // an sstable only gets a horizon array when its header carries a dropped column.
        assertTrue("no input sstable carries both a NO_TIMESTAMP cell and a dropped column in its header, "
                   + "so the drop filter is never applied to the cell this scenario is about",
                   cfs.getLiveSSTables().stream()
                      .anyMatch(s -> s.getMinTimestamp() == LivenessInfo.NO_TIMESTAMP
                                     && s.header.columns().size() > cfs.metadata().regularAndStaticColumns().size()));
        assertEquals("the sentinel cells must sit in two sources for the cross-source check to be reached",
                     2, cfs.getLiveSSTables().stream()
                           .filter(s -> s.getMinTimestamp() == LivenessInfo.NO_TIMESTAMP).count());

        assertCursorMatchesIterator(cfs);
    }

    private void writeSentinelTimestampCell(ColumnFamilyStore cfs, long partitionKey, long value)
    {
        PartitionUpdate.SimpleBuilder update = PartitionUpdate.simpleBuilder(cfs.metadata(), partitionKey);
        update.row(0L).timestamp(LivenessInfo.NO_TIMESTAMP).add("v1", value);
        new Mutation(update.build()).apply();
    }

    /**
     * Drop and re-add of a SET column. Every existing scenario here uses {@code text},
     * {@code bigint} or {@code map<text, bigint>}, so the complex arm of
     * {@code CursorCompactor.isDroppedMultiCellOrCounterColumn} was only ever reached with a map.
     */
    @Test
    public void droppedThenReaddedSetColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, s set<text>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
            execute("DELETE s FROM %s WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        alterTable("ALTER TABLE %s DROP s");
        alterTable("ALTER TABLE %s ADD s set<text>");

        for (long ck = 5; ck < 15; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck + 100);
            execute("UPDATE %s SET s = s + {'after-readd'} WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("elements written after the re-add must survive",
                     10, countOccurrences(json, "\"after-readd\""));
    }

    /**
     * Drop and re-add of a LIST column. A list's cell paths are timeuuids rather than values, so
     * its dropped-column handling reaches the same code with a different path type.
     */
    @Test
    public void droppedThenReaddedListColumn() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, l list<text>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long ck = 0; ck < 10; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
            execute("DELETE l FROM %s WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        alterTable("ALTER TABLE %s DROP l");
        alterTable("ALTER TABLE %s ADD l list<text>");

        for (long ck = 5; ck < 15; ck++)
        {
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck + 100);
            execute("UPDATE %s SET l = l + ['after-readd'] WHERE pk = 0 AND ck = ?", ck);
        }
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("elements written after the re-add must survive",
                     10, countOccurrences(json, cellValue("after-readd")));
    }

    /** The sstable counts for the tests above, whose rows the drop filter leaves empty. */
    private static final int[] SSTABLE_COUNTS = { 2, 3, 5 };

    /** Before any wall-clock drop time, so the drop filter covers every cell written at it. */
    private static final long BEFORE_DROP_TS = 1_000_000L;

    /**
     * In each partition one sstable holds a row with a row marker, and each of the others holds the
     * same row, with other clustering bytes (decimal 1.0, 1.00, ...), with only a cell of the
     * column that is dropped later. UnfilteredSerializer.deserialize skips a row that the drop
     * filter leaves empty (CASSANDRA-13337), so the iterator path writes the clustering of the row
     * with the marker. The cursor path must skip those rows too, or the row merge order picks the
     * clustering bytes of a skipped row. The partition chooses which sstable holds the marker, so
     * that row comes first in some partitions and last in others.
     */
    @Test
    public void rowLeftEmptyByDropDoesNotChooseClusteringBytes() throws Exception
    {
        for (int sstableCount : SSTABLE_COUNTS)
        {
            createTable("CREATE TABLE %s (pk int, ck decimal, v int, d int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < sstableCount * 4; pk++)
                {
                    if (pk % sstableCount == i)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, decimal(1, i), 1, BEFORE_DROP_TS);
                    else
                        execute("UPDATE %s USING TIMESTAMP ? SET d = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, pk, decimal(1, i));
                }
                flush();
            }
            alterTable("ALTER TABLE %s DROP d");

            String json = allJson(assertCursorMatchesIterator(cfs));
            assertEquals("each partition keeps its one row", sstableCount * 4, countOccurrences(json, "\"name\":\"v\""));
        }
    }

    /**
     * As above, for the column merge order. In each row one sstable holds only a cell of the
     * column that is dropped later, and each of the others holds the same set element with other
     * bytes. The iterator path gives the skipped row to no merge, so the cursor path must not give
     * it to the column merge.
     */
    @Test
    public void rowLeftEmptyByDropDoesNotChooseCellPathBytes() throws Exception
    {
        for (int sstableCount : SSTABLE_COUNTS)
        {
            createTable("CREATE TABLE %s (pk int, ck int, s set<decimal>, d int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < sstableCount * 4; pk++)
                {
                    if (pk % sstableCount == i)
                        execute("UPDATE %s USING TIMESTAMP ? SET d = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, pk, 0);
                    else
                        execute("UPDATE %s USING TIMESTAMP ? SET s = s + ? WHERE pk = ? AND ck = ?",
                                BEFORE_DROP_TS, Collections.singleton(decimal(1, (i + pk) % 4)), pk, 0);
                }
                flush();
            }
            alterTable("ALTER TABLE %s DROP d");

            assertCursorMatchesIterator(cfs);
        }
    }

    /**
     * The control for the two tests above: the rows also hold a cell of a column that is not
     * dropped, so the drop filter leaves them not empty, and both paths merge every row.
     */
    @Test
    public void rowWithLiveCellAfterDropIsKept() throws Exception
    {
        for (int sstableCount : SSTABLE_COUNTS)
        {
            createTable("CREATE TABLE %s (pk int, ck decimal, v int, w int, d int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < sstableCount * 4; pk++)
                {
                    if (pk % sstableCount == i)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, decimal(1, i), 1, BEFORE_DROP_TS);
                    else
                        execute("UPDATE %s USING TIMESTAMP ? SET d = ?, w = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, i, pk, decimal(1, i));
                }
                flush();
            }
            alterTable("ALTER TABLE %s DROP d");

            String json = allJson(assertCursorMatchesIterator(cfs));
            assertEquals("the cells of the column that is not dropped must survive",
                         sstableCount * 4, countOccurrences(json, "\"name\":\"w\""));
        }
    }

    /**
     * As {@link #rowLeftEmptyByDropDoesNotChooseClusteringBytes}, but the other rows hold only a
     * deletion of a complex column, written before that column is dropped and added again. The read
     * drops that deletion, so the row is empty, and the iterator path skips it.
     */
    @Test
    public void rowWithOnlyDroppedComplexDeletionDoesNotChooseClusteringBytes() throws Exception
    {
        for (int sstableCount : SSTABLE_COUNTS)
        {
            createTable("CREATE TABLE %s (pk int, ck decimal, v int, s set<int>, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < sstableCount * 4; pk++)
                {
                    if (pk % sstableCount == i)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, decimal(1, i), 1, BEFORE_DROP_TS);
                    else
                        execute("DELETE s FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, pk, decimal(1, i));
                }
                flush();
            }
            alterTable("ALTER TABLE %s DROP s");
            alterTable("ALTER TABLE %s ADD s set<int>");

            String json = allJson(assertCursorMatchesIterator(cfs));
            assertEquals("each partition keeps its one row", sstableCount * 4, countOccurrences(json, "\"name\":\"v\""));
        }
    }

    /**
     * The rows that the drop filter leaves empty sit next to the other unfiltereds of a partition:
     * two of them in a row, one before a range tombstone, and one as the last row of the
     * partition. The static row holds only a dropped static cell. Each partition also holds a
     * dropped cell newer than the drop, which survives, and cells written after the column is
     * added again, which also survive.
     */
    @Test
    public void rowsLeftEmptyByDropNextToOtherUnfiltereds() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, ds int static, v int, d int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        int partitions = 10;
        for (int pk = 0; pk < partitions; pk++)
        {
            execute("UPDATE %s USING TIMESTAMP ? SET ds = ? WHERE pk = ?", BEFORE_DROP_TS, 1, pk);
            for (int ck = 0; ck < 10; ck++)
                execute("UPDATE %s USING TIMESTAMP ? SET d = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, pk, ck);
            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck >= 3 AND ck < 5", BEFORE_DROP_TS + 1, pk);
            execute("UPDATE %s USING TIMESTAMP ? SET d = ? WHERE pk = ? AND ck = ?", FUTURE_TS, 2, pk, 6);
        }
        flush();
        alterTable("ALTER TABLE %s DROP d");
        alterTable("ALTER TABLE %s DROP ds");
        alterTable("ALTER TABLE %s ADD d int");
        for (int pk = 0; pk < partitions; pk++)
        {
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 2, 1);
            execute("UPDATE %s SET d = ? WHERE pk = ? AND ck = ?", 3, pk, 8);
        }
        flush();

        String json = allJson(assertCursorMatchesIterator(cfs));
        assertEquals("the dropped cells newer than the drop must survive",
                     partitions, countOccurrences(json, "\"value\":2"));
        assertEquals("the cells written after the re-add must survive",
                     partitions, countOccurrences(json, "\"value\":3"));
        assertEquals("each range tombstone must survive as two markers",
                     2 * partitions, countOccurrences(json, "\"type\":\"range_tombstone_bound\""));
    }

    /**
     * A row whose only cells are of the dropped column, but which also holds a row deletion, is not
     * empty after the drop filter: {@link org.apache.cassandra.db.rows.BTreeRow#isEmpty} is false
     * because the deletion is not live. The iterator path keeps it, so the cursor path must too.
     * Each of the other sstables holds the same row, with other clustering bytes, in one of three
     * forms, chosen by the partition:
     * <ul>
     *   <li>a regular row deletion and a cell of the dropped column;</li>
     *   <li>a row marker, a newer row deletion that deletes it, and a cell of the dropped column;</li>
     *   <li>a shadowable row deletion and a cell of the dropped column.</li>
     * </ul>
     * These rows take the early return in the reader, so this test passes with and without the fix.
     * It pins the rows that the fix must keep.
     */
    @Test
    public void rowWithRowDeletionAndOnlyDroppedCellsIsKept() throws Exception
    {
        for (int sstableCount : SSTABLE_COUNTS)
        {
            createTable("CREATE TABLE %s (pk int, ck decimal, v int, d int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = sstableCount * 6;
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < partitions; pk++)
                {
                    BigDecimal ck = decimal(1, i);
                    if (pk % sstableCount == i)
                    {
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, ck, 1, BEFORE_DROP_TS);
                        continue;
                    }
                    switch ((pk / sstableCount) % 3)
                    {
                        case 0:
                            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS - 10, pk, ck);
                            break;
                        case 1:
                            execute("INSERT INTO %s (pk, ck) VALUES (?, ?) USING TIMESTAMP ?", pk, ck, BEFORE_DROP_TS - 20);
                            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS - 10, pk, ck);
                            break;
                        default:
                            writeShadowableRowDeletion(cfs, pk, ck, BEFORE_DROP_TS - 10);
                    }
                    execute("UPDATE %s USING TIMESTAMP ? SET d = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, pk, ck);
                }
                flush();
            }
            alterTable("ALTER TABLE %s DROP d");

            String json = allJson(assertCursorMatchesIterator(cfs));
            assertEquals("each partition keeps its one row", partitions, countOccurrences(json, "\"name\":\"v\""));
            // The merge keeps a regular row deletion, and the newer row marker shadows a shadowable one.
            assertEquals("each merged row with a regular row deletion keeps it", sstableCount * 4,
                         countOccurrences(json, "\"deletion_info\""));
        }
    }

    private static void writeShadowableRowDeletion(ColumnFamilyStore cfs, int pk, BigDecimal ck, long timestamp)
    {
        Clustering<?> clustering = Clustering.make(DecimalType.instance.decompose(ck));
        Row row = BTreeRow.create(clustering, LivenessInfo.EMPTY,
                                  Row.Deletion.shadowable(DeletionTime.build(timestamp, FBUtilities.nowInSeconds())), BTree.empty());
        new Mutation(PartitionUpdate.singleRowUpdate(cfs.metadata(), Int32Type.instance.decompose(pk), row)).apply();
    }

    /**
     * A row with no liveness and no row deletion, whose only content is a cell of the dropped
     * column and a complex deletion of {@code s}, which is not dropped. The deletion survives the
     * drop filter, so the row is not empty and the iterator path keeps it. If the cursor path
     * skipped it, the element of {@code s} from the other sstable would come back.
     */
    @Test
    public void rowWithDroppedCellAndComplexDeletionIsKept() throws Exception
    {
        for (int sstableCount : SSTABLE_COUNTS)
        {
            createTable("CREATE TABLE %s (pk int, ck decimal, s set<int>, d int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = sstableCount * 4;
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < partitions; pk++)
                {
                    BigDecimal ck = decimal(1, i);
                    if (pk % sstableCount == i)
                        execute("UPDATE %s USING TIMESTAMP ? SET s = s + ? WHERE pk = ? AND ck = ?",
                                BEFORE_DROP_TS - 100, Collections.singleton(1), pk, ck);
                    else
                        execute("UPDATE %s USING TIMESTAMP ? SET d = ?, s = ? WHERE pk = ? AND ck = ?",
                                BEFORE_DROP_TS, 1, Collections.emptySet(), pk, ck);
                }
                flush();
            }
            alterTable("ALTER TABLE %s DROP d");

            String json = allJson(assertCursorMatchesIterator(cfs));
            assertEquals("the complex deletion of s must remove every element of s",
                         0, countOccurrences(json, "\"name\":\"s\",\"path\""));
        }
    }

    /**
     * Partitions that hold only cells of the dropped column have nothing left after the drop
     * filter, so the iterator path writes no partition for them. They sit at the lowest token, at
     * the highest token, and between partitions that are kept. With one sstable, and with several
     * sstables where each one holds only dropped cells for those keys.
     */
    @Test
    public void partitionsWithOnlyDroppedCellsAreNotWritten() throws Exception
    {
        for (int sstableCount : new int[]{ 1, 3 })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, d int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = 12;
            List<Integer> byToken = new ArrayList<>();
            for (int pk = 0; pk < partitions; pk++)
                byToken.add(pk);
            byToken.sort(Comparator.comparing(pk -> cfs.getPartitioner().getToken(Int32Type.instance.decompose(pk))));
            Set<Integer> onlyDropped = new HashSet<>(Arrays.asList(byToken.get(0), byToken.get(5), byToken.get(6),
                                                                   byToken.get(partitions - 1)));
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < partitions; pk++)
                {
                    for (int ck = 0; ck < 3; ck++)
                    {
                        if (i == 0 && !onlyDropped.contains(pk))
                            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, ck, 1, BEFORE_DROP_TS);
                        else
                            execute("UPDATE %s USING TIMESTAMP ? SET d = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, pk, ck);
                    }
                }
                flush();
            }
            alterTable("ALTER TABLE %s DROP d");

            String json = allJson(assertCursorMatchesIterator(cfs));
            assertEquals("only the partitions with a kept cell are written",
                         partitions - onlyDropped.size(), countOccurrences(json, "\"partition\":{\"key\""));
            for (int pk : onlyDropped)
                assertEquals("partition " + pk + " holds only dropped cells", 0,
                             countOccurrences(json, "\"key\":[\"" + pk + "\"]"));
        }
    }

    /**
     * The two paths count source rows in different ways. The iterator path counts each merged row,
     * plus one static row for each partition, also when the partition has no static row. The cursor
     * path counts each source row that is not skipped. So the test checks each count against the
     * number expected for the shape. A row left empty by the drop must not be counted on either path.
     */
    @Test
    public void totalSourceCQLRowsLeavesOutSkippedRows() throws Exception
    {
        int partitions = 10;
        int rows = 6;
        createTable("CREATE TABLE %s (pk int, ck int, v int, d int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        // Even rows hold only the dropped column, so they are skipped; odd rows also hold v.
        for (int pk = 0; pk < partitions; pk++)
            for (int ck = 0; ck < rows; ck++)
                if (ck % 2 == 0)
                    execute("UPDATE %s USING TIMESTAMP ? SET d = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, pk, ck);
                else
                    execute("UPDATE %s USING TIMESTAMP ? SET d = ?, v = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, 1, pk, ck);
        flush();
        alterTable("ALTER TABLE %s DROP d");

        long kept = partitions * rows / 2;
        assertEquals("iterator path, one sstable: kept rows and one static row per partition",
                     kept + partitions, iteratorTotalSourceCQLRows(cfs));
        assertEquals("cursor path, one sstable: kept rows", kept, cursorTotalSourceCQLRows(cfs));

        // A second sstable holds every row with a row marker, so the merge has each row once.
        for (int pk = 0; pk < partitions; pk++)
            for (int ck = 0; ck < rows; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, ck, 2, BEFORE_DROP_TS + 1);
        flush();

        assertEquals("iterator path, two sstables: each merged row and one static row per partition",
                     (long) partitions * rows + partitions, iteratorTotalSourceCQLRows(cfs));
        assertEquals("cursor path, two sstables: each source row that is not skipped",
                     partitions * rows + kept, cursorTotalSourceCQLRows(cfs));
    }

    private static long iteratorTotalSourceCQLRows(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        long nowInSec = FBUtilities.nowInSeconds();
        try (CompactionController controller = new CompactionController(cfs, inputs, cfs.getDefaultGcBefore(nowInSec));
             AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(inputs);
             CompactionIterator iterator = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller,
                                                                  nowInSec, TimeUUID.Generator.nextTimeUUID()))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    while (partition.hasNext())
                        partition.next();
                }
            }
            return iterator.getTotalSourceCQLRows();
        }
    }

    /** Runs a cursor compaction to its end, then aborts it, so the input sstables stay live. */
    private static long cursorTotalSourceCQLRows(ColumnFamilyStore cfs) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        long nowInSec = FBUtilities.nowInSeconds();
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark the inputs compacting", txn);
        try (CompactionController controller = new CompactionController(cfs, inputs, cfs.getDefaultGcBefore(nowInSec));
             AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(inputs);
             DefaultCompactionWriter writer = new DefaultCompactionWriter(cfs, cfs.getDirectories(), txn, inputs))
        {
            assertTrue("the cursor path must run for this table", CursorCompactor.isSupported(scanners, controller));
            CursorCompactor compactor = new CursorCompactor(OperationType.COMPACTION, scanners.scanners, controller,
                                                            nowInSec, TimeUUID.Generator.nextTimeUUID());
            try
            {
                while (compactor.writeNextPartition(writer)) { }
                return compactor.getTotalSourceCQLRows();
            }
            finally
            {
                compactor.close();
            }
        }
        finally
        {
            try
            {
                txn.abort();
            }
            catch (Throwable ignored)
            {
                // the writer's abort may already have aborted the transaction
            }
        }
    }

    /** Enough regular columns that the cell walk uses its present-mask words, which it does from 64 columns. */
    private static final int WIDE_COLUMNS = 70;

    /**
     * As {@link #rowLeftEmptyByDropDoesNotChooseClusteringBytes}, on a table of more than 64
     * columns, with the dropped column last, past index 64. In each partition one sstable holds the
     * row with a row marker. Each of the others holds the same row, with other clustering bytes,
     * and with only a cell of the dropped column, or with that cell and a cell of a column that is
     * kept. The second kind is not empty, so the reader goes back to its first cell.
     */
    @Test
    public void rowLeftEmptyByDropOnWideTable() throws Exception
    {
        StringBuilder columns = new StringBuilder();
        StringBuilder setAll = new StringBuilder();
        for (int c = 0; c < WIDE_COLUMNS; c++)
        {
            columns.append(String.format("c%02d int, ", c));
            setAll.append(String.format("c%02d = 1, ", c));
        }
        for (int sstableCount : SSTABLE_COUNTS)
        {
            createTable("CREATE TABLE %s (pk int, ck decimal, " + columns + "zd int, PRIMARY KEY (pk, ck))");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = sstableCount * 4;
            for (int i = 0; i < sstableCount; i++)
            {
                for (int pk = 0; pk < partitions; pk++)
                {
                    if (pk % sstableCount == i)
                        execute("INSERT INTO %s (pk, ck, c65) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, decimal(1, i), 1, BEFORE_DROP_TS);
                    else if (pk % 2 == 0)
                        execute("UPDATE %s USING TIMESTAMP ? SET zd = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, 1, pk, decimal(1, i));
                    else
                        execute("UPDATE %s USING TIMESTAMP ? SET c66 = ?, zd = ? WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, i, 1, pk, decimal(1, i));
                }
                // A flush writes only the columns that hold data into the header, so one row sets them all.
                execute("UPDATE %s USING TIMESTAMP ? SET " + setAll + "zd = 1 WHERE pk = ? AND ck = ?", BEFORE_DROP_TS, partitions, decimal(1, i));
                flush();
            }
            for (SSTableReader sstable : cfs.getLiveSSTables())
                assertTrue("the sstable must have 64 columns or more, or the present-mask words are not used",
                           sstable.header.columns().regulars.size() >= 64);
            alterTable("ALTER TABLE %s DROP zd");

            String json = allJson(assertCursorMatchesIterator(cfs));
            assertEquals("each partition keeps its one row", partitions + 1, countOccurrences(json, "\"name\":\"c65\""));
        }
    }

    /** {@code value} with {@code scale} extra zero digits: equal values, other bytes. */
    private static BigDecimal decimal(int value, int scale)
    {
        return new BigDecimal(BigInteger.valueOf(value).multiply(BigInteger.TEN.pow(scale)), scale);
    }

    /*
     * There is no drop-and-re-add scenario for a non-frozen UDT column: ALTER TABLE ... DROP
     * rejects one outright, "Cannot drop non-frozen column %s of user type %s"
     * (AlterTableStatement.DropColumns.dropColumn). The UDT arm of
     * CursorCompactor.isDroppedMultiCellOrCounterColumn is therefore only reachable through a
     * dropped non-frozen COLLECTION, which the two scenarios above cover.
     */
}
