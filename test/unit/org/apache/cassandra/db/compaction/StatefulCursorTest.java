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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for cursor-read defects that the differential harness cannot isolate. */
public class StatefulCursorTest extends CQLTester
{
    /** Hooks for {@link #drive}, so the cursor-driving tests share one state machine. */
    private interface CursorVisitor
    {
        default void onRow(StatefulCursor cursor) {}
        default void onTombstone(StatefulCursor cursor) {}
        /** @param state what readCellHeader returned; UNFILTERED_END means no cell was surfaced */
        default void onCellHeader(StatefulCursor cursor, int state) {}
    }

    /** Drives the cursor over the whole sstable, calling back at rows, markers and cell headers. */
    private static void drive(StatefulCursor cursor, CursorVisitor visitor)
    {
        int state = cursor.readPartitionHeader();
        while (!isState(state, DONE))
        {
            if (isState(state, PARTITION_START))
            {
                state = cursor.readPartitionHeader();
            }
            else if (isState(state, STATIC_ROW_START))
            {
                cursor.readStaticRowHeader();
                state = cursor.state();
            }
            else if (isState(state, ROW_START))
            {
                cursor.readRowHeader();
                visitor.onRow(cursor);
                state = cursor.state();
            }
            else if (isState(state, TOMBSTONE_START))
            {
                cursor.readTombstoneMarker();
                visitor.onTombstone(cursor);
                state = cursor.state();
            }
            else if (isState(state, CELL_HEADER_START))
            {
                state = cursor.readCellHeader();
                visitor.onCellHeader(cursor, state);
            }
            else if (isState(state, CELL_VALUE_START))
            {
                state = cursor.skipCellValue();
            }
            else
            {
                state = cursor.continueReading();
            }
        }
    }

    /**
     * The cursor's cell-liveness check must agree with the iterator path's oracle,
     * {@code AbstractCell.hasInvalidDeletions()}, on every combination — including a cell that
     * declares a TTL with no expiration time, which the previous predicate could not report because
     * it tested "has an expiration time" where the oracle tests "has a TTL".
     */
    @Test
    public void cellDeletionPredicateAgreesWithCellOracle()
    {
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "tbl", "v", Int32Type.instance, 0);
        ByteBuffer value = Int32Type.instance.decompose(1);

        // A Cell cannot hold a negative localExpirationTime: the BufferCell constructor runs it
        // through Cell.deletionTimeLongToUnsignedInteger, which rejects negatives. So the oracle is
        // only defined over the values below, and the negative cases are asserted separately.
        for (int ttl : new int[]{ Cell.NO_TTL, 1, 100, -1, Integer.MIN_VALUE })
        {
            for (long ldt : new long[]{ Cell.NO_DELETION_TIME, Cell.INVALID_DELETION_TIME, 0, 1,
                                        1_700_000_000L, Cell.MAX_DELETION_TIME })
            {
                boolean oracle = new BufferCell(column, 1000L, ttl, ldt, value, null).hasInvalidDeletions();
                assertEquals("cell validation must match AbstractCell.hasInvalidDeletions() at ttl=" +
                             ttl + " localExpirationTime=" + ldt,
                             oracle, StatefulCursor.hasInvalidCellDeletion(ttl, ldt));
            }
        }

        // the combination the old predicate could never report: its last clause asked whether the
        // cell had an EXPIRATION TIME, contradicting the ldt == NO_DELETION_TIME it was ANDed with
        assertTrue("a cell with a TTL but no expiration time is invalid",
                   StatefulCursor.hasInvalidCellDeletion(100, Cell.NO_DELETION_TIME));
        assertFalse("a cell with no TTL and no expiration time is the normal live cell",
                    StatefulCursor.hasInvalidCellDeletion(Cell.NO_TTL, Cell.NO_DELETION_TIME));
        // a negative expiration time cannot occur in a Cell, and off disk it only decodes
        // alongside a negative TTL, so neither reference can express this pair
        assertTrue("a negative expiration time is invalid",
                   StatefulCursor.hasInvalidCellDeletion(Cell.NO_TTL, -1));
    }

    /**
     * The row-liveness check has the same oracle mismatch: {@code AbstractRow.hasInvalidDeletions()}
     * guards with {@code LivenessInfo.isExpiring()}, which is "has a TTL".
     */
    @Test
    public void rowLivenessPredicateAgreesWithRowOracle()
    {
        // ExpiringLivenessInfo asserts ttl != NO_TTL && localExpirationTime != NO_EXPIRATION_TIME,
        // so — as with the cell oracle — the reference type cannot represent the corrupt states the
        // cursor decodes off disk. Compare against the oracle over what it CAN represent, then
        // assert the corrupt domain against the specification.
        for (int ttl : new int[]{ LivenessInfo.NO_TTL, 1, 100, -1, Integer.MIN_VALUE })
        {
            for (long ldt : new long[]{ LivenessInfo.NO_EXPIRATION_TIME, 0, 1, 1_700_000_000L, -1,
                                        Long.MIN_VALUE })
            {
                if (ttl != LivenessInfo.NO_TTL && ldt == LivenessInfo.NO_EXPIRATION_TIME)
                    continue;
                LivenessInfo liveness = LivenessInfo.withExpirationTime(1000L, ttl, ldt);
                boolean oracle = liveness.isExpiring()
                                 && (liveness.ttl() < 0 || liveness.localExpirationTime() < 0);
                assertEquals("row validation must match AbstractRow.hasInvalidDeletions() at ttl=" +
                             ttl + " localExpirationTime=" + ldt,
                             oracle, StatefulCursor.hasInvalidRowLiveness(ttl, ldt));
            }
        }

        // the combination the old predicate could never report, and which no LivenessInfo can hold:
        // a corrupt negative TTL decoded alongside the no-expiration sentinel
        assertTrue("row liveness with a negative TTL is invalid",
                   StatefulCursor.hasInvalidRowLiveness(-1, LivenessInfo.NO_EXPIRATION_TIME));
        assertFalse("row liveness with no TTL is not expiring, so there is nothing to validate",
                    StatefulCursor.hasInvalidRowLiveness(LivenessInfo.NO_TTL, -1));
    }

    /**
     * When the dropped-column filter discards every remaining column of a row, readCellHeader
     * surfaces no cell and returns UNFILTERED_END, leaving the cell liveness describing the
     * DISCARDED cell. Validation keys off exactly that state, so pin it: the drop-filtered row must
     * report UNFILTERED_END and a surviving row must report a cell state.
     */
    @Test
    public void readCellHeaderReturnsUnfilteredEndWhenEveryColumnIsDropFiltered() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, dropped int, kept int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Both rows are written below the wall-clock droppedTime recorded by the ALTER, so the drop
        // filter definitely covers the dropped cell.
        // ck 0: only the to-be-dropped column is set, so after the drop the row has no cells left
        execute("INSERT INTO %s (pk, ck, dropped) VALUES (0, 0, 1) USING TIMESTAMP 1000");
        // ck 1: a surviving cell, the control for the same code path
        execute("INSERT INTO %s (pk, ck, kept) VALUES (0, 1, 2) USING TIMESTAMP 1000");
        flush();
        alterTable("ALTER TABLE %s DROP dropped");

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        boolean[] seen = new boolean[2];   // [0] no cell surfaced, [1] cell surfaced
        try (StatefulCursor cursor = new StatefulCursor(sstable, DiskAccessMode.standard))
        {
            drive(cursor, new CursorVisitor()
            {
                public void onCellHeader(StatefulCursor c, int state)
                {
                    seen[isState(state, UNFILTERED_END) ? 0 : 1] = true;
                }
            });
        }

        assertTrue("expected the row whose only cell was drop-filtered to return UNFILTERED_END",
                   seen[0]);
        assertTrue("expected the surviving row's cell to return a cell state, as a control", seen[1]);
    }

    /**
     * The column-subset fields are row state. A range tombstone marker has no columns, so reading
     * one must clear them; otherwise missingColumnsMask() keeps reporting the previous row's subset
     * while rowColumns() is null.
     */
    @Test
    public void rangeTombstoneClearsTheColumnSubsetState() throws Throwable
    {
        // two regular columns, so a row setting only one is subset-encoded and has a non-zero mask
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // both columns must appear SOMEWHERE in the sstable, or the header superset is just {a} and
        // the row below is an all-columns row rather than a subset-encoded one
        execute("INSERT INTO %s (pk, ck, a) VALUES (0, 0, 1)");
        execute("INSERT INTO %s (pk, ck, b) VALUES (0, 1, 2)");
        execute("DELETE FROM %s WHERE pk = 0 AND ck > 5");
        flush();

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        boolean[] sawNonZeroMaskOnRow = { false };
        int[] markers = { 0 };
        long[] maskBitsAfterAnyMarker = { 0 };   // OR across markers, so no violation can be hidden
        try (StatefulCursor cursor = new StatefulCursor(sstable, DiskAccessMode.standard))
        {
            drive(cursor, new CursorVisitor()
            {
                public void onRow(StatefulCursor c)
                {
                    sawNonZeroMaskOnRow[0] |= c.unfiltered().missingColumnsMask() != 0;
                }

                public void onTombstone(StatefulCursor c)
                {
                    markers[0]++;
                    maskBitsAfterAnyMarker[0] |= c.unfiltered().missingColumnsMask();
                    assertNull("a marker has no columns, so rowColumns must be null",
                               c.unfiltered().rowColumns());
                    assertNull("a marker has no columns, so presentColumnsWords must be null",
                               c.unfiltered().presentColumnsWords());
                }
            });
        }

        assertTrue("expected the sparse row to carry a non-zero column-subset mask",
                   sawNonZeroMaskOnRow[0]);
        assertTrue("expected at least one range tombstone marker", markers[0] > 0);
        assertEquals("a marker has no columns, so the subset mask must be cleared",
                     0L, maskBitsAfterAnyMarker[0]);
    }

    /**
     * The cell walk must consume exactly the body its row header declared. Nothing else checks that,
     * so a cell-level desync would run on into the NEXT unfiltered and surface far from its cause —
     * either as a corruption report naming an unrelated offset or as garbage written to the output.
     * <p>
     * Driven through the plain reader rather than {@link StatefulCursor} because the descriptor has
     * to be supplied: over-declaring {@link UnfilteredDescriptor#size()} by one byte is a desync the
     * check must catch, and the unmodified descriptor is the control that it does not fire normally.
     * Off-by-one matters here — the check runs before the next unfiltered's flag byte is consumed —
     * so a version comparing against the wrong position would fail the control.
     * <p>
     * The desync is reported as a {@link CorruptSSTableException} wrapping the diagnostic, not as a bare
     * assertion, so it reaches callers through the same path as any other corrupted sstable and fires
     * whether or not {@code -ea} is on. The cause carries the offsets and the wrapper carries the file
     * name, so the cause is what this asserts against.
     */
    @Test
    public void cellWalkMustConsumeExactlyTheDeclaredRowBody() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // several rows, including a subset-encoded one, so the walk covers more than a single shape
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (0, 0, 1, 2)");
        execute("INSERT INTO %s (pk, ck, a) VALUES (0, 1, 3)");
        execute("INSERT INTO %s (pk, ck, b) VALUES (1, 0, 4)");
        flush();

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // control: the declared size is the real one, so the whole sstable reads without tripping. It
        // has to run BEFORE the desync, which marks the reader suspect.
        driveReader(sstable, false);
        assertFalse("the control read must not have marked the sstable suspect", sstable.isMarkedSuspect());

        // No assertion runs inside the try; every check that observes the desync runs after the block.
        // The catch is narrow to CorruptSSTableException, so any other throwable propagates as itself
        // instead of being recorded as the expected one.
        CorruptSSTableException tripped = null;
        try
        {
            driveReader(sstable, true);
        }
        catch (CorruptSSTableException e)
        {
            tripped = e;
        }
        assertNotNull("a row body declared one byte longer than it is must trip the "
                      + "consumed-vs-declared check", tripped);
        assertNotNull("the desync diagnostic must be carried as the cause", tripped.getCause());
        assertTrue("expected the cell-desync diagnostic, got: " + tripped.getCause().getMessage(),
                   tripped.getCause().getMessage() != null
                   && tripped.getCause().getMessage().startsWith("cell desync:"));
        assertTrue("a desync must mark the sstable suspect, as any other corruption report does",
                   sstable.isMarkedSuspect());
    }

    /**
     * Reads every unfiltered of the sstable through the plain cursor reader.
     *
     * @param overDeclareRowBody report each row body as one byte longer than it is. This moves the
     *                           EXPECTED end where a real desync would move the ACTUAL position; the
     *                           two meet at the same inequality, which is what makes the {@code ==}
     *                           comparison observable without needing a corrupt sstable.
     */
    private static void driveReader(SSTableReader sstable, boolean overDeclareRowBody) throws Exception
    {
        AbstractType<?>[] clusteringTypes = sstable.header.clusteringTypes().toArray(AbstractType[]::new);
        UnfilteredDescriptor unfiltered = overDeclareRowBody
                                         ? new UnfilteredDescriptor(clusteringTypes)
                                           {
                                               @Override
                                               public long size()
                                               {
                                                   return super.size() + 1;
                                               }
                                           }
                                         : new UnfilteredDescriptor(clusteringTypes);
        PartitionDescriptor partition =
            new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));

        try (SSTableCursorReader reader = new SSTableCursorReader(sstable, DiskAccessMode.standard))
        {
            int state = reader.readPartitionHeader(partition);
            while (!isState(state, DONE))
            {
                if (isState(state, PARTITION_START))
                    state = reader.readPartitionHeader(partition);
                else if (isState(state, STATIC_ROW_START))
                    state = reader.readStaticRowHeader(unfiltered);
                else if (isState(state, ROW_START))
                    state = reader.readRowHeader(unfiltered);
                else if (isState(state, TOMBSTONE_START))
                    state = reader.readTombstoneMarker(unfiltered);
                else if (isState(state, CELL_HEADER_START))
                    state = reader.readCellHeader();
                else if (isState(state, CELL_VALUE_START))
                    state = reader.skipCellValue();
                else
                    state = reader.continueReading();
            }
        }
    }
}
