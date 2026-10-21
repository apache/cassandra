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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A cursor reads exactly the data-file segments it was given.
 * <p>
 * This is what lets a UCS shard task whose sstables straddle a shard boundary take the cursor path
 * instead of falling back to the iterator. The segments come from
 * {@code SSTableSimpleScanner.positionBounds()}, whose contract is that each starts and ends on a
 * partition boundary, and that they are ascending and non-overlapping.
 */
public class CursorSegmentBoundsTest extends CQLTester
{
    /** One partition of the sstable: the key the cursor surfaced, and where it started. */
    private static final class Seen
    {
        final long key;
        final long position;

        Seen(long key, long position)
        {
            this.key = key;
            this.position = position;
        }
    }

    /** Walks a cursor to DONE, recording each partition it surfaces. */
    private static List<Seen> readAll(StatefulCursor cursor)
    {
        List<Seen> seen = new ArrayList<>();
        int state = cursor.state();
        while (!isState(state, DONE))
        {
            if (isState(state, PARTITION_START))
            {
                state = cursor.readPartitionHeader();
                if (!isState(state, DONE))
                    seen.add(new Seen(cursor.currentKey().getKey().getLong(cursor.currentKey().getKey().position()),
                                      cursor.currPartition().position()));
            }
            else if (isState(state, STATIC_ROW_START))
            {
                cursor.readStaticRowHeader();
                state = cursor.state();
            }
            else if (isState(state, ROW_START))
            {
                cursor.readRowHeader();
                state = cursor.state();
            }
            else if (isState(state, TOMBSTONE_START))
            {
                cursor.readTombstoneMarker();
                state = cursor.state();
            }
            else if (isState(state, CELL_HEADER_START))
            {
                state = cursor.readCellHeader();
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
        return seen;
    }

    private static List<Long> keysOf(List<Seen> seen)
    {
        List<Long> keys = new ArrayList<>(seen.size());
        for (Seen s : seen)
            keys.add(s.key);
        return keys;
    }

    private SSTableReader oneSSTableOfManyPartitions()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < 40; pk++)
            for (long ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "value-" + pk + '-' + ck);
        flush();
        assertEquals("expected exactly one sstable", 1, cfs.getLiveSSTables().size());
        return cfs.getLiveSSTables().iterator().next();
    }

    /** The whole-file baseline every case below is compared against. */
    private List<Seen> wholeFile(SSTableReader sstable)
    {
        try (StatefulCursor cursor = new StatefulCursor(sstable, DiskAccessMode.standard))
        {
            List<Seen> all = readAll(cursor);
            assertTrue("expected the fixture to produce several partitions, got " + all.size(), all.size() > 8);
            return all;
        }
    }

    /**
     * The whole-file cursor is one segment of [0, length), so it must cover every byte. The old
     * constructor delegates to the null-bounds one, so comparing the two reads nothing; the count
     * is what the segment code could get wrong.
     */
    @Test
    public void aWholeFileCursorCoversEveryByte()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);

        try (StatefulCursor cursor = new StatefulCursor(sstable, null, DiskAccessMode.standard))
        {
            assertEquals(keysOf(baseline), keysOf(readAll(cursor)));
            assertEquals("a whole-file cursor must count the whole file",
                         sstable.uncompressedLength(), cursor.bytesRead());
        }
    }

    /**
     * One segment covering the whole file is also the whole file. This separates "bounds are
     * honoured" from "bounds are ignored": a cursor that ignored its bounds would pass this and
     * fail the two below.
     */
    @Test
    public void oneFullSegmentReadsTheWholeFile()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, length)),
                                                        DiskAccessMode.standard))
        {
            assertEquals(keysOf(baseline), keysOf(readAll(cursor)));
        }
    }

    /**
     * The file split at a real partition boundary. The first cursor must stop at the split and the
     * second must start there, so together they read every partition exactly once and neither reads
     * one of the other's.
     */
    @Test
    public void twoSegmentsSplitAtAPartitionBoundaryPartitionTheFile()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();

        int splitIndex = baseline.size() / 2;
        long split = baseline.get(splitIndex).position;
        assertTrue("the split must not be the file start", split > 0);

        List<Long> head;
        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, split)),
                                                        DiskAccessMode.standard))
        {
            head = keysOf(readAll(cursor));
        }

        List<Long> tail;
        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(split, length)),
                                                        DiskAccessMode.standard))
        {
            tail = keysOf(readAll(cursor));
        }

        assertEquals("the head segment must stop at the split", keysOf(baseline).subList(0, splitIndex), head);
        assertEquals("the tail segment must start at the split",
                     keysOf(baseline).subList(splitIndex, baseline.size()), tail);

        List<Long> rejoined = new ArrayList<>(head);
        rejoined.addAll(tail);
        assertEquals("the two segments together must be the whole file", keysOf(baseline), rejoined);
    }

    /**
     * Both halves handed to ONE cursor, which must walk them in order across the seek between them.
     * A cursor that stopped at the first segment's end would return only the head.
     */
    @Test
    public void twoSegmentsOnOneCursorAreWalkedInOrder()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();
        long split = baseline.get(baseline.size() / 2).position;

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, split),
                                                                      new PartitionPositionBounds(split, length)),
                                                        DiskAccessMode.standard))
        {
            assertEquals("two adjacent segments on one cursor must read the whole file, in order",
                         keysOf(baseline), keysOf(readAll(cursor)));
        }
    }

    /**
     * A gap between the segments. This is the shard case: the cursor must skip what lies between
     * them and read nothing from the hole.
     */
    @Test
    public void aGapBetweenSegmentsIsSkipped()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();

        int firstEnd = baseline.size() / 3;
        int secondStart = (2 * baseline.size()) / 3;
        long gapStart = baseline.get(firstEnd).position;
        long gapEnd = baseline.get(secondStart).position;

        List<Long> expected = new ArrayList<>(keysOf(baseline).subList(0, firstEnd));
        expected.addAll(keysOf(baseline).subList(secondStart, baseline.size()));

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, gapStart),
                                                                      new PartitionPositionBounds(gapEnd, length)),
                                                        DiskAccessMode.standard))
        {
            assertEquals("the partitions inside the gap must not be read", expected, keysOf(readAll(cursor)));
        }
    }

    /**
     * An sstable that does not intersect the task's range gives an empty bounds list. The cursor is
     * born DONE and reads nothing, rather than reading the whole file.
     */
    @Test
    public void anEmptyBoundsListReadsNothing()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();

        try (StatefulCursor cursor = new StatefulCursor(sstable, new ArrayList<>(), DiskAccessMode.standard))
        {
            assertTrue("a cursor with no segments must be DONE at construction", isState(cursor.state(), DONE));
            assertTrue("and must surface no partition", readAll(cursor).isEmpty());
        }
    }

    /** A zero-length segment carries no partition and must be skipped, not read as the whole file. */
    @Test
    public void anEmptySegmentIsSkipped()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();
        long split = baseline.get(baseline.size() / 2).position;

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(split, split),
                                                                      new PartitionPositionBounds(split, length)),
                                                        DiskAccessMode.standard))
        {
            assertEquals("an empty leading segment must not disturb the one after it",
                         keysOf(baseline).subList(baseline.size() / 2, baseline.size()),
                         keysOf(readAll(cursor)));
        }
    }

    /** Descending or overlapping segments break the scanner's contract and must be refused. */
    @Test
    public void overlappingSegmentsAreRefused()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();
        long split = baseline.get(baseline.size() / 2).position;

        boolean refused = false;
        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, length),
                                                                      new PartitionPositionBounds(split, length)),
                                                        DiskAccessMode.standard))
        {
            readAll(cursor);
        }
        catch (IllegalArgumentException e)
        {
            refused = true;
        }
        assertTrue("overlapping segments must be refused rather than read twice", refused);
    }

    /**
     * The contract that makes the UCS change correct: for the same token range, the cursor's
     * segment walk must select exactly the partitions the scanner selects.
     * <p>
     * The scanner is the reference. It is what the iterator path reads, so if the two agree on the
     * partition set, a shard task produces the same input either way. The bounds handed to the
     * cursor are the scanner's own, which is what {@code convertScannersToCursors} does.
     */
    @Test
    public void aBoundedCursorSelectsWhatTheScannerSelects()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);

        // a range ending part way through the sstable, so the scanner is genuinely partial
        Token split = sstable.getPartitioner()
                             .decorateKey(org.apache.cassandra.utils.ByteBufferUtil.bytes(baseline.get(baseline.size() / 2).key))
                             .getToken();
        List<Range<Token>> ranges =
            Collections.singletonList(new Range<>(sstable.getPartitioner().getMinimumToken(), split));

        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(ranges);

        List<Long> fromScanner = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner(ranges))
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    fromScanner.add(partition.partitionKey().getKey().getLong(partition.partitionKey().getKey().position()));
                }
            }
        }

        List<Long> fromCursor;
        try (StatefulCursor cursor = new StatefulCursor(sstable, bounds, DiskAccessMode.standard))
        {
            fromCursor = keysOf(readAll(cursor));
        }

        assertEquals("the cursor must read exactly the partitions the scanner reads for the same range",
                     fromScanner, fromCursor);
        assertFalse("the range must be partial, or this proves nothing about bounded reads",
                    fromScanner.size() == baseline.size());
        assertTrue("the range must select something", fromScanner.size() > 0);
    }

    /**
     * The byte count must equal the bytes the segments cover, and must never go backwards.
     * <p>
     * It feeds {@code StatefulCursor.bytesReadSinceSnapshot}, which feeds
     * {@code CursorCompactor.totalBytesRead}, which drives compaction progress and the rate
     * limiter. A negative delta there makes progress run backwards.
     */
    @Test
    public void theByteCountEqualsTheSegmentsCovered()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();
        long split = baseline.get(baseline.size() / 2).position;

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, split)),
                                                        DiskAccessMode.standard))
        {
            readAll(cursor);
            assertEquals("a single segment must count exactly its own bytes", split, cursor.bytesRead());
        }

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, split),
                                                                      new PartitionPositionBounds(split, length)),
                                                        DiskAccessMode.standard))
        {
            readAll(cursor);
            assertEquals("two adjacent segments must count the whole file", length, cursor.bytesRead());
        }
    }

    /**
     * The count is bytes COVERED, not the file position.
     * <p>
     * Every other byte-count case here starts at 0 or is adjacent, so the file pointer happens to
     * equal the total and {@code bytesRead()} could be {@code dataReader.getFilePointer()} and still
     * pass. This one starts part way in and leaves a gap, so the two numbers differ.
     */
    @Test
    public void theByteCountIsBytesCoveredNotFilePosition()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        int quarter = baseline.size() / 4;
        long a = baseline.get(quarter).position;
        long b = baseline.get(2 * quarter).position;
        long c = baseline.get(3 * quarter).position;
        assertTrue("the first segment must not start at the file start", a > 0);

        // cover [a,b) and [c,end): the file pointer ends at the file end, the covered bytes do not
        long length = sstable.uncompressedLength();
        long covered = (b - a) + (length - c);

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(a, b),
                                                                      new PartitionPositionBounds(c, length)),
                                                        DiskAccessMode.standard))
        {
            readAll(cursor);
            long counted = cursor.bytesRead();
            assertEquals("the count must be the bytes the segments cover", covered, counted);
            assertTrue("the count must differ from the file position, or this proves nothing",
                       counted != length);
        }
    }

    /**
     * A trailing empty segment must not disturb the count. It carries no partition, so the total is
     * the same as without it, and in particular is not negative.
     */
    @Test
    public void aTrailingEmptySegmentDoesNotCorruptTheByteCount()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long split = baseline.get(baseline.size() / 2).position;

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, split),
                                                                      new PartitionPositionBounds(split, split)),
                                                        DiskAccessMode.standard))
        {
            readAll(cursor);
            long counted = cursor.bytesRead();
            assertTrue("the byte count must never be negative, was " + counted, counted >= 0);
            assertEquals("a trailing empty segment adds nothing", split, counted);
        }
    }

    /** The count must not go backwards at any point in the walk. */
    @Test
    public void theByteCountNeverGoesBackwards()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();
        int third = baseline.size() / 3;
        long a = baseline.get(third).position;
        long b = baseline.get(2 * third).position;

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, a),
                                                                      new PartitionPositionBounds(a, a),
                                                                      new PartitionPositionBounds(b, length)),
                                                        DiskAccessMode.standard))
        {
            long previous = 0;
            int state = cursor.state();
            while (!isState(state, DONE))
            {
                long now = cursor.bytesRead();
                assertTrue("the byte count went backwards: " + previous + " then " + now, now >= previous);
                previous = now;

                if (isState(state, PARTITION_START)) state = cursor.readPartitionHeader();
                else if (isState(state, STATIC_ROW_START)) { cursor.readStaticRowHeader(); state = cursor.state(); }
                else if (isState(state, ROW_START)) { cursor.readRowHeader(); state = cursor.state(); }
                else if (isState(state, TOMBSTONE_START)) { cursor.readTombstoneMarker(); state = cursor.state(); }
                else if (isState(state, CELL_HEADER_START)) state = cursor.readCellHeader();
                else if (isState(state, CELL_VALUE_START)) state = cursor.skipCellValue();
                else state = cursor.continueReading();
            }
            assertTrue("the final count must never be negative", cursor.bytesRead() >= 0);
        }
    }

    /**
     * The deltas that reach the rate limiter. {@code bytesReadSinceSnapshot} is what
     * {@code CursorCompactor.updateTotalBytesRead} adds to {@code totalBytesRead}, which drives
     * {@code getTotalBytesScanned}, compaction progress and {@code compactionRateLimiterAcquire}.
     * Nothing else tests it, and a negative delta there makes progress run backwards.
     */
    @Test
    public void theSnapshotDeltasAreNonNegativeAndSumToTheSegments()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long length = sstable.uncompressedLength();
        int quarter = baseline.size() / 4;
        long a = baseline.get(quarter).position;
        long b = baseline.get(2 * quarter).position;
        long c = baseline.get(3 * quarter).position;
        long covered = (b - a) + (length - c);

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(a, b),
                                                                      new PartitionPositionBounds(c, length),
                                                                      new PartitionPositionBounds(length, length)),
                                                        DiskAccessMode.standard))
        {
            long total = 0;
            int state = cursor.state();
            while (!isState(state, DONE))
            {
                long delta = cursor.bytesReadSinceSnapshot();
                assertTrue("a snapshot delta must never be negative, was " + delta, delta >= 0);
                total += delta;

                if (isState(state, PARTITION_START)) state = cursor.readPartitionHeader();
                else if (isState(state, STATIC_ROW_START)) { cursor.readStaticRowHeader(); state = cursor.state(); }
                else if (isState(state, ROW_START)) { cursor.readRowHeader(); state = cursor.state(); }
                else if (isState(state, TOMBSTONE_START)) { cursor.readTombstoneMarker(); state = cursor.state(); }
                else if (isState(state, CELL_HEADER_START)) state = cursor.readCellHeader();
                else if (isState(state, CELL_VALUE_START)) state = cursor.skipCellValue();
                else state = cursor.continueReading();
            }
            long last = cursor.bytesReadSinceSnapshot();
            assertTrue("the final delta must not be negative, was " + last, last >= 0);
            total += last;

            assertEquals("the deltas must sum to the bytes the segments cover", covered, total);
        }
    }

    /** Bounds that end before they start are refused rather than silently skipped. */
    @Test
    public void invertedSegmentsAreRefused()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();

        boolean refused = false;
        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(500, 300)),
                                                        DiskAccessMode.standard))
        {
            readAll(cursor);
        }
        catch (IllegalArgumentException e)
        {
            refused = true;
        }
        assertTrue("an inverted range must be refused, not skipped: skipping poisons the byte count",
                   refused);
    }

    /** The whole-file path must not be marked suspect by any of this. */
    @Test
    public void aBoundedReadDoesNotMarkTheSSTableSuspect()
    {
        SSTableReader sstable = oneSSTableOfManyPartitions();
        List<Seen> baseline = wholeFile(sstable);
        long split = baseline.get(baseline.size() / 2).position;

        try (StatefulCursor cursor = new StatefulCursor(sstable,
                                                        Arrays.asList(new PartitionPositionBounds(0, split)),
                                                        DiskAccessMode.standard))
        {
            readAll(cursor);
        }
        assertFalse("a bounded read of a healthy sstable must not mark it suspect", sstable.isMarkedSuspect());
    }
}
