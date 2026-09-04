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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
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
import static org.junit.Assert.fail;

/** Unit tests for cursor-read defects that the differential harness cannot isolate. */
public class StatefulCursorTest extends CQLTester
{
    /** Hooks for {@link #drive}, so the cursor-driving tests share one state machine. */
    private interface CursorVisitor
    {
        default void onPartition(StatefulCursor cursor) {}
        default void onRow(StatefulCursor cursor) {}
        default void onTombstone(StatefulCursor cursor) {}
        /** @param state what readCellHeader returned; UNFILTERED_END means no cell was surfaced */
        default void onCellHeader(StatefulCursor cursor, int state) {}
    }

    /** Drives the cursor over the whole sstable, calling back at partitions, rows, markers and cell headers. */
    private static void drive(StatefulCursor cursor, CursorVisitor visitor)
    {
        int state = cursor.state();
        while (!isState(state, DONE))
        {
            if (isState(state, PARTITION_START))
            {
                state = cursor.readPartitionHeader();
                visitor.onPartition(cursor);
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
     * {@code AbstractCell.hasInvalidDeletions()}, on every combination of TTL and expiration time.
     */
    @Test
    public void cellDeletionPredicateAgreesWithCellOracle()
    {
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "tbl", "v", Int32Type.instance, 0);
        ByteBuffer value = Int32Type.instance.decompose(1);

        // A Cell cannot hold a negative localExpirationTime: the BufferCell constructor runs it
        // through Cell.deletionTimeLongToUnsignedInteger, which rejects negatives.
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

        assertTrue("a cell with a TTL but no expiration time is invalid",
                   StatefulCursor.hasInvalidCellDeletion(100, Cell.NO_DELETION_TIME));
        assertFalse("a cell with no TTL and no expiration time is the normal live cell",
                    StatefulCursor.hasInvalidCellDeletion(Cell.NO_TTL, Cell.NO_DELETION_TIME));
        // Off disk a negative expiration time decodes only alongside a negative TTL, so no Cell
        // and no real decode produces this pair.
        assertTrue("a negative expiration time is invalid",
                   StatefulCursor.hasInvalidCellDeletion(Cell.NO_TTL, -1));
    }

    /**
     * The cursor's row-liveness check must agree with the iterator path's oracle,
     * {@code AbstractRow.hasInvalidDeletions()}, which guards with
     * {@code LivenessInfo.isExpiring()}, that is "has a TTL".
     */
    @Test
    public void rowLivenessPredicateAgreesWithRowOracle()
    {
        // ExpiringLivenessInfo asserts ttl != NO_TTL && localExpirationTime != NO_EXPIRATION_TIME,
        // so the oracle cannot hold a TTL beside the no-expiration sentinel.
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

        // the pair the loop skips: a negative TTL beside the no-expiration sentinel
        assertTrue("row liveness with a negative TTL is invalid",
                   StatefulCursor.hasInvalidRowLiveness(-1, LivenessInfo.NO_EXPIRATION_TIME));
        assertFalse("row liveness with no TTL is not expiring, so there is nothing to validate",
                    StatefulCursor.hasInvalidRowLiveness(LivenessInfo.NO_TTL, -1));
    }

    /**
     * The dropped-column filter can discard every remaining column of a row. readCellHeader then
     * surfaces no cell and returns UNFILTERED_END, leaving the cell liveness of the DISCARDED cell
     * in place. Validation keys off exactly that state, so this test pins it.
     */
    @Test
    public void readCellHeaderReturnsUnfilteredEndWhenEveryColumnIsDropFiltered() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, dropped int, kept int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Both rows carry a timestamp below the wall-clock droppedTime the ALTER records, so the
        // drop filter covers the dropped cell.
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
     * one must clear them. Otherwise missingColumnsMask() keeps reporting the previous row's subset
     * while rowColumns() is null.
     */
    @Test
    public void rangeTombstoneClearsTheColumnSubsetState() throws Throwable
    {
        // two regular columns, so a row setting only one is subset-encoded and has a non-zero mask
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Both columns must appear SOMEWHERE in the sstable. Otherwise the header superset holds
        // only {a}, and the row below encodes all columns rather than a subset.
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
     * The cell walk must consume exactly the body its row header declared. Nothing else checks that.
     * A cell-level desync would run on into the NEXT unfiltered and surface far from its cause: it
     * would report corruption at an unrelated offset, or write garbage to the output.
     * <p>
     * The test drives the plain reader rather than {@link StatefulCursor} because it must supply the
     * descriptor. Over-declaring {@link UnfilteredDescriptor#size()} by one byte is the desync the
     * check must catch. The check runs before the next unfiltered's flag byte is consumed, so a
     * comparison against the wrong position would fail the control read.
     * <p>
     * The reader reports the desync as a {@link CorruptSSTableException} wrapping the diagnostic,
     * not as a bare assertion, so it fires whether or not {@code -ea} is on. The cause carries the
     * offsets and the wrapper carries the file name.
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

        // Every check that observes the desync runs after the try block. The catch is narrow to
        // CorruptSSTableException, so any other throwable propagates as itself instead of standing
        // in for the expected one.
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
     *                           EXPECTED end where a real desync would move the ACTUAL position.
     *                           Both reach the same inequality, so the equality check fires without
     *                           a corrupt sstable.
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

    // BOUNDED CURSORS

    /** Twenty single-row partitions in one sstable, so a token range can pick an interior run of them. */
    private SSTableReader twentyPartitionSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck int, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 20; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, 0, 'v')", pk);
        flush();
        return cfs.getLiveSSTables().iterator().next();
    }

    /** The sstable's partition keys in file order, read by the iterator path. */
    private static List<DecoratedKey> keysInFileOrder(SSTableReader sstable)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    keys.add(partition.partitionKey());
                }
            }
        }
        return keys;
    }

    /** The pk values a cursor over {@code bounds} gives, then its DONE-time byte count and position. */
    private static List<Long> pksRead(SSTableReader sstable, Collection<PartitionPositionBounds> bounds, long[] bytesReadAndPosition)
    {
        List<Long> pks = new ArrayList<>();
        try (StatefulCursor cursor = new StatefulCursor(sstable, bounds, DiskAccessMode.standard))
        {
            drive(cursor, new CursorVisitor()
            {
                public void onPartition(StatefulCursor c)
                {
                    pks.add(pkOf(c.currPartition().key()));
                }
            });
            assertEquals(DONE, cursor.state());
            bytesReadAndPosition[0] = cursor.bytesRead();
            bytesReadAndPosition[1] = cursor.position();
        }
        return pks;
    }

    private static long pkOf(DecoratedKey key)
    {
        return LongType.instance.compose(key.getKey());
    }

    private static List<Long> pksOf(List<DecoratedKey> keys, int fromInclusive, int toExclusive)
    {
        List<Long> pks = new ArrayList<>();
        for (DecoratedKey key : keys.subList(fromInclusive, toExclusive))
            pks.add(pkOf(key));
        return pks;
    }

    /** The token range (keys[fromExclusive], keys[toInclusive]], which holds keys fromExclusive+1 .. toInclusive. */
    private static Range<Token> rangeAfter(List<DecoratedKey> keys, int fromExclusive, int toInclusive)
    {
        return new Range<>(keys.get(fromExclusive).getToken(), keys.get(toInclusive).getToken());
    }

    /** The bounds an SSTableSimpleScanner over {@code ranges} would read, which the cursor must honour. */
    @SafeVarargs
    private static List<PartitionPositionBounds> boundsFor(SSTableReader sstable, Range<Token>... ranges)
    {
        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Arrays.asList(ranges));
        assertEquals("expected one data-file segment per range", ranges.length, bounds.size());
        for (PartitionPositionBounds b : bounds)
        {
            assertTrue("expected a segment that starts after the file start: " + b.lowerPosition, b.lowerPosition > 0);
            assertTrue("expected a segment that ends before the file end: " + b.upperPosition,
                       b.upperPosition < sstable.uncompressedLength());
        }
        return bounds;
    }

    /**
     * A cursor over one interior segment reads exactly the partitions inside it. It reads nothing
     * before the start, nothing past the end, and counts only the segment's bytes as read.
     */
    @Test
    public void boundedCursorReadsExactlyItsSegment() throws Throwable
    {
        SSTableReader sstable = twentyPartitionSSTable();
        List<DecoratedKey> keys = keysInFileOrder(sstable);
        assertEquals(20, keys.size());

        List<PartitionPositionBounds> bounds = boundsFor(sstable, rangeAfter(keys, 4, 12));
        long[] bytesReadAndPosition = new long[2];
        assertEquals(pksOf(keys, 5, 13), pksRead(sstable, bounds, bytesReadAndPosition));

        PartitionPositionBounds segment = bounds.get(0);
        assertEquals("bytesRead must count the segment only", segment.upperPosition - segment.lowerPosition, bytesReadAndPosition[0]);
        assertEquals("the cursor must stop at the segment end", segment.upperPosition, bytesReadAndPosition[1]);
    }

    /**
     * Several segments of one sstable are walked in order. Nothing between them is read, nothing
     * inside them is skipped, and the byte count sums the segments only.
     */
    @Test
    public void boundedCursorWalksSegmentsInOrder() throws Throwable
    {
        SSTableReader sstable = twentyPartitionSSTable();
        List<DecoratedKey> keys = keysInFileOrder(sstable);

        List<PartitionPositionBounds> bounds = boundsFor(sstable,
                                                         rangeAfter(keys, 1, 4),
                                                         rangeAfter(keys, 9, 14),
                                                         rangeAfter(keys, 16, 18));
        List<Long> expected = new ArrayList<>();
        expected.addAll(pksOf(keys, 2, 5));
        expected.addAll(pksOf(keys, 10, 15));
        expected.addAll(pksOf(keys, 17, 19));

        long[] bytesReadAndPosition = new long[2];
        assertEquals(expected, pksRead(sstable, bounds, bytesReadAndPosition));

        long segmentBytes = 0;
        for (PartitionPositionBounds b : bounds)
            segmentBytes += b.upperPosition - b.lowerPosition;
        assertEquals("bytesRead must not count the gaps between segments", segmentBytes, bytesReadAndPosition[0]);
        assertEquals("the cursor must stop at the last segment's end", bounds.get(2).upperPosition, bytesReadAndPosition[1]);
    }

    /**
     * The whole file as one explicit segment reads the same as the unbounded cursor, which is what
     * a full-range scanner now hands the compactor. The existing suite covers the unbounded form.
     */
    @Test
    public void wholeFileSegmentMatchesTheUnboundedCursor() throws Throwable
    {
        SSTableReader sstable = twentyPartitionSSTable();
        List<Long> expected = pksOf(keysInFileOrder(sstable), 0, 20);

        long[] unbounded = new long[2];
        assertEquals(expected, pksRead(sstable, null, unbounded));

        long[] bounded = new long[2];
        List<PartitionPositionBounds> wholeFile = Collections.singletonList(new PartitionPositionBounds(0, sstable.uncompressedLength()));
        assertEquals(expected, pksRead(sstable, wholeFile, bounded));

        assertEquals(sstable.uncompressedLength(), unbounded[0]);
        assertEquals(unbounded[0], bounded[0]);
        assertEquals(unbounded[1], bounded[1]);
    }

    /** A scanner over a range the sstable does not cover has no bounds. The cursor is DONE at birth. */
    @Test
    public void noSegmentsGiveACursorThatIsDone() throws Throwable
    {
        SSTableReader sstable = twentyPartitionSSTable();
        long[] bytesReadAndPosition = new long[2];
        assertEquals(Collections.emptyList(), pksRead(sstable, Collections.emptyList(), bytesReadAndPosition));
        assertEquals(0, bytesReadAndPosition[0]);
    }

    /**
     * A segment start inside a partition is not a partition boundary. The byte before it is the
     * high byte of the key length here, not an end-of-partition marker, so the seek must refuse.
     */
    @Test
    public void aSegmentStartInsideAPartitionIsCorrupt() throws Throwable
    {
        SSTableReader sstable = twentyPartitionSSTable();
        List<DecoratedKey> keys = keysInFileOrder(sstable);
        PartitionPositionBounds segment = boundsFor(sstable, rangeAfter(keys, 4, 12)).get(0);
        List<PartitionPositionBounds> misaligned =
            Collections.singletonList(new PartitionPositionBounds(segment.lowerPosition + 1, segment.upperPosition));
        try (StatefulCursor cursor = new StatefulCursor(sstable, misaligned, DiskAccessMode.standard))
        {
            fail("expected a seek into the middle of a partition to be refused; got state " + cursor.state());
        }
        catch (CorruptSSTableException expected)
        {
            assertTrue(expected.getMessage() + " / " + expected.getCause(),
                       String.valueOf(expected.getCause()).contains("end-of-partition marker"));
        }
    }

    /** Segments out of order or overlapping break the scanner contract, and the cursor refuses them as the scanner does. */
    @Test
    public void overlappingSegmentsAreRefused() throws Throwable
    {
        SSTableReader sstable = twentyPartitionSSTable();
        List<DecoratedKey> keys = keysInFileOrder(sstable);
        List<PartitionPositionBounds> overlapping = Arrays.asList(boundsFor(sstable, rangeAfter(keys, 4, 12)).get(0),
                                                                  boundsFor(sstable, rangeAfter(keys, 8, 15)).get(0));
        try
        {
            pksRead(sstable, overlapping, new long[2]);
            fail("expected overlapping segments to be refused");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("non-overlapping"));
        }
    }
}
