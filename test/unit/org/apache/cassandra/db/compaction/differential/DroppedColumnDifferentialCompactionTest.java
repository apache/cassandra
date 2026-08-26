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
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Dropped-column scenarios: the iterator path discards cells (and complex deletions) of a
 * dropped column at deserialization when their timestamp is at or before the drop
 * (UnfilteredSerializer.readSimpleColumn/readComplexColumn via DeserializationHelper.isDropped /
 * isDroppedComplexDeletion). The cursor path must filter identically, or dropped data survives
 * cursor compaction and is resurrected by ALTER TABLE ... ADD of the same column.
 *
 * Timestamp determinism: ClientState timestamps are strictly increasing per node, so a write
 * executed before ALTER TABLE DROP always carries a timestamp strictly below droppedTime;
 * writes that must survive the drop use an explicit far-future USING TIMESTAMP.
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

    @Test
    public void complexCellsNewerThanDropRetained() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint PRIMARY KEY, v bigint, m map<text, bigint>)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("UPDATE %s USING TIMESTAMP " + FUTURE_TS + " SET v = 7, m['a'] = 1 WHERE pk = 0");
        flush();
        execute("UPDATE %s USING TIMESTAMP " + FUTURE_TS + " SET m['b'] = 2 WHERE pk = 0");
        flush();

        alterTable("ALTER TABLE %s DROP m");

        assertRows(execute("SELECT * FROM %s"), row(0L, 7L));
        commitCompaction(cfs, cfs.getLiveSSTables(), false, cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
        assertRows(execute("SELECT * FROM %s"), row(0L, 7L));

        alterTable("ALTER TABLE %s ADD m map<text, bigint>");
        assertRows(execute("SELECT m FROM %s WHERE pk = 0"), row(map("a", 1L, "b", 2L)));
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
     * dropped column. Long.MIN_VALUE is simultaneously that sentinel, {@code DeletionTime.LIVE}'s
     * {@code markedForDeleteAt}, and the cursor's "no drop horizon" sentinel, so the value walks into three
     * independent collisions on its way through a compaction: the active deletion must not shadow the cell at
     * either of the merge's two deletion checks, and the drop filter must not discard it. The iterator path
     * keeps it on all counts.
     *
     * These are CALL-SITE pins, which is why this is a differential scenario and not only a unit test: a
     * rule-level test can gate the drop rule or the deletion rule in isolation but stay green if a call site
     * inlines the bare, unguarded comparison again. Those call sites are reached under different conditions,
     * so the scenario writes both shapes:
     *
     *  - the single-source check, after the winner is chosen, needs only one copy of the cell;
     *  - the cross-source check, inside the merge loop's COMPARE arm, is reached only when two or more
     *    sources carry the same column of the same row at an equal timestamp. Partitions 1 and 2 supply that,
     *    with the two values in opposite order, because under unfixed code the surviving value would be
     *    whichever source the merge visits first — so one of the two partitions must diverge whichever order
     *    that is.
     *
     * The timestamp is unreachable through CQL — {@code cql3.RowUpdateBuilder}'s constructor rejects it for
     * every modification statement and {@code QueryOptions} rejects it at the native protocol, both because
     * the engine uses that value for "absence of timestamp". It is NOT blocked by the encoding: a cell
     * timestamp is written as an unsigned vint delta from the header's {@code minTimestamp} and read back by
     * adding, inverses mod 2^64 for any base, so it round-trips through an sstable exactly.
     * {@code PartitionUpdate.simpleBuilder} carries no such guard, which is what makes this constructible.
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

        // v1 is never dropped, and no deletion covers these rows. The long argument after the metadata is a
        // PARTITION KEY, not a timestamp: each sentinel cell lands in its own partition, disjoint from the
        // pk=0 rows above.
        writeSentinelTimestampCell(cfs, 1L, 7L);
        writeSentinelTimestampCell(cfs, 2L, 8L);
        flush();

        // makes THIS sstable's header carry a dropped column, so sstableHasDroppedColumns is true and the
        // horizon array is consulted for every cell read from it — v1's included
        alterTable("ALTER TABLE %s DROP v2");

        // the same two cells again with the values swapped, in a second sstable, so each of partitions 1
        // and 2 has the sentinel cell in two sources and the merge must compare their values
        writeSentinelTimestampCell(cfs, 1L, 8L);
        writeSentinelTimestampCell(cfs, 2L, 7L);

        for (long ck = 10; ck < 20; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
        flush();

        // Preconditions, both on ONE sstable. Asserting them separately would pass with the sentinel cells in
        // an sstable whose header carries no dropped column, which leaves the drop-filter half silently dead:
        // the horizon array is only built for an sstable that has one.
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
}
