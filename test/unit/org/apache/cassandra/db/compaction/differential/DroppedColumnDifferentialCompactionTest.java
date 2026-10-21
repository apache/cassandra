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

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;

import static org.junit.Assert.assertEquals;
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

    /*
     * There is no drop-and-re-add scenario for a non-frozen UDT column: ALTER TABLE ... DROP
     * rejects one outright, "Cannot drop non-frozen column %s of user type %s"
     * (AlterTableStatement.DropColumns.dropColumn). The UDT arm of
     * CursorCompactor.isDroppedMultiCellOrCounterColumn is therefore only reachable through a
     * dropped non-frozen COLLECTION, which the two scenarios above cover.
     */
}
