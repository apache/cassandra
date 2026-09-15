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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Low-level tests directly against {@link StatefulCursor}'s partial-range bound support
 * (the bounds constructor), driving the cursor's own state machine by hand -
 * {@link CursorCompactor} is not involved at all. Bounds are computed via
 * {@link SSTableReader#getPositionsForRanges}, the same production entry point repair
 * validation will use.
 */
public class StatefulCursorPartialRangeTest extends CQLTester
{
    private static final int PARTITION_COUNT = 6;

    private SSTableReader flushSinglePartitionPerRowTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long pk = 0; pk < PARTITION_COUNT; pk++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, 0L, pk);
        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        return cfs.getLiveSSTables().iterator().next();
    }

    /**
     * Drives a single cursor from wherever {@code state} indicates it currently sits through
     * the rest of the current partition (if any) to the next {@code PARTITION_START} or
     * {@code DONE} - no merging, just enough of the state machine to move past row/tombstone
     * content without inspecting it (mirrors {@code CursorCompactor.skipRowsOnStrictLiveness}'s
     * use of the same {@code skipUnfiltered} primitive, but with {@code autoContinue=true} so it
     * collapses straight through to the next meaningful state).
     */
    private static int finishPartition(StatefulCursor cursor, int state)
    {
        while (state == ROW_START || state == TOMBSTONE_START)
            state = cursor.skipUnfiltered(true);
        if (state == PARTITION_END)
            state = cursor.continueReading();
        return state;
    }

    private static List<DecoratedKey> readAllPartitionKeys(StatefulCursor cursor, IPartitioner partitioner)
    {
        List<DecoratedKey> keys = new ArrayList<>();
        int state = cursor.state();
        while (state != DONE)
        {
            assertEquals(PARTITION_START, state);
            // Never DONE: readPartitionHeader(PartitionDescriptor) returns a header state or throws,
            // and bound exhaustion is reported by finishPartition's continueReading() below.
            state = cursor.readPartitionHeader();
            // currentKey() is backed by a reusable, mutated-in-place key AND token (see
            // IPartitioner.createReusableKey / Murmur3Partitioner.ReusableLongToken) - snapshot
            // via decorateKey() immediately, or every entry in this list ends up aliasing the
            // same mutable objects and reflecting whatever was read last.
            keys.add(partitioner.decorateKey(ByteBufferUtil.clone(cursor.currentKey().getKey())));
            state = finishPartition(cursor, state);
        }
        return keys;
    }

    /** {@code (exclusiveStart, inclusiveEnd]}, matching Cassandra's {@code Range<Token>} convention. */
    private static Range<Token> rangeBetween(Token exclusiveStart, DecoratedKey inclusiveEnd)
    {
        return new Range<>(exclusiveStart, inclusiveEnd.getToken());
    }

    @Test
    public void singleSegmentCoveringWholeFileMatchesFullRangeRead() throws Throwable
    {
        SSTableReader sstable = flushSinglePartitionPerRowTable();
        List<DecoratedKey> allKeysInTokenOrder = readAllPartitionKeys(new StatefulCursor(sstable, DiskAccessMode.standard), sstable.getPartitioner());
        assertEquals(PARTITION_COUNT, allKeysInTokenOrder.size());

        List<PartitionPositionBounds> bounds = Collections.singletonList(sstable.getPositionsForFullRange());

        StatefulCursor bounded = new StatefulCursor(sstable, bounds, DiskAccessMode.standard);
        assertEquals(allKeysInTokenOrder, readAllPartitionKeys(bounded, sstable.getPartitioner()));
    }

    @Test
    public void multipleDisjointSegmentsReadOnlyTheSelectedPartitions() throws Throwable
    {
        SSTableReader sstable = flushSinglePartitionPerRowTable();
        List<DecoratedKey> allKeysInTokenOrder = readAllPartitionKeys(new StatefulCursor(sstable, DiskAccessMode.standard), sstable.getPartitioner());
        assertEquals(PARTITION_COUNT, allKeysInTokenOrder.size());

        // Isolate index 1 and index 4 (out of 6) as two separate, non-adjacent byte segments in
        // the same sstable - exactly the shape a repair session covering disjoint token ranges
        // produces against one file.
        Range<Token> firstSegment = rangeBetween(allKeysInTokenOrder.get(0).getToken(), allKeysInTokenOrder.get(1));
        Range<Token> secondSegment = rangeBetween(allKeysInTokenOrder.get(3).getToken(), allKeysInTokenOrder.get(4));
        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Arrays.asList(firstSegment, secondSegment));
        assertEquals("expected two disjoint byte segments for two disjoint token ranges", 2, bounds.size());

        // The first segment starts mid-file (after key[0]'s partition) - a prerequisite for the
        // byte-accounting assertion below to actually be able to catch a stale snapshot bug.
        long firstSegmentLowerPosition = bounds.get(0).lowerPosition;
        assertTrue("test setup: first segment must not start at file offset 0, or the assertion below can't expose a stale snapshot",
                   firstSegmentLowerPosition > 0);

        StatefulCursor bounded = new StatefulCursor(sstable, bounds, DiskAccessMode.standard);
        assertEquals("bytesReadSinceSnapshot() immediately after construction must not count the skipped prefix before the first segment",
                     0L, bounded.bytesReadSinceSnapshot());
        assertEquals(Arrays.asList(allKeysInTokenOrder.get(1), allKeysInTokenOrder.get(4)), readAllPartitionKeys(bounded, sstable.getPartitioner()));
    }

    /**
     * Reads every assigned partition, accumulating {@link StatefulCursor#bytesReadSinceSnapshot()}
     * the same incremental way {@code CursorCompactor.updateBytesRead} does (once per partition),
     * and returns the total bytes reported as read.
     */
    private static long readAllAccumulatingBytesRead(StatefulCursor cursor)
    {
        long totalBytesRead = 0;
        int state = cursor.state();
        while (state != DONE)
        {
            state = cursor.readPartitionHeader();
            if (state == DONE)
                break;
            state = finishPartition(cursor, state);
            totalBytesRead += cursor.bytesReadSinceSnapshot();
        }
        totalBytesRead += cursor.bytesReadSinceSnapshot();
        return totalBytesRead;
    }

    @Test
    public void byteAccountingStaysSaneAcrossSegmentHop() throws Throwable
    {
        SSTableReader sstable = flushSinglePartitionPerRowTable();
        List<DecoratedKey> allKeysInTokenOrder = readAllPartitionKeys(new StatefulCursor(sstable, DiskAccessMode.standard), sstable.getPartitioner());
        assertEquals(PARTITION_COUNT, allKeysInTokenOrder.size());

        // Two disjoint segments (index 1 and index 4 of 6) with skipped partitions BETWEEN them -
        // the seek from the first segment's end to the second segment's start jumps over partitions
        // 2 and 3, whose bytes must never be counted as read.
        Range<Token> firstSegment = rangeBetween(allKeysInTokenOrder.get(0).getToken(), allKeysInTokenOrder.get(1));
        Range<Token> secondSegment = rangeBetween(allKeysInTokenOrder.get(3).getToken(), allKeysInTokenOrder.get(4));
        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Arrays.asList(firstSegment, secondSegment));
        assertEquals("expected two disjoint byte segments for two disjoint token ranges", 2, bounds.size());
        assertTrue("test setup: segments must have a byte gap between them to expose a stale snapshot",
                   bounds.get(1).lowerPosition > bounds.get(0).upperPosition);

        long estimatedBytes = (bounds.get(0).upperPosition - bounds.get(0).lowerPosition)
                              + (bounds.get(1).upperPosition - bounds.get(1).lowerPosition);

        StatefulCursor bounded = new StatefulCursor(sstable, bounds, DiskAccessMode.standard);
        long totalBytesRead = readAllAccumulatingBytesRead(bounded);

        // Must equal exactly the sum of the two segments' sizes (getEstimatedBytes). Before the
        // segment-hop snapshot fix, the inter-segment gap was counted too, pushing this past the
        // estimate (>100% progress).
        assertEquals("bytes read across a multi-segment read must equal the summed segment sizes, not include the skipped gap",
                     estimatedBytes, totalBytesRead);
    }

    @Test
    public void boundExhaustionBeforeEndOfFileReportsDoneShortOfTheFileEnd() throws Throwable
    {
        SSTableReader sstable = flushSinglePartitionPerRowTable();
        List<DecoratedKey> allKeysInTokenOrder = readAllPartitionKeys(new StatefulCursor(sstable, DiskAccessMode.standard), sstable.getPartitioner());

        // A segment covering only the first 3 (of 6) partitions - stops well before true EOF.
        Range<Token> earlySegment = rangeBetween(sstable.getPartitioner().getMinimumToken(), allKeysInTokenOrder.get(2));
        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Collections.singletonList(earlySegment));

        StatefulCursor bounded = new StatefulCursor(sstable, bounds, DiskAccessMode.standard);
        List<DecoratedKey> keysRead = readAllPartitionKeys(bounded, sstable.getPartitioner());

        assertEquals(allKeysInTokenOrder.subList(0, 3), keysRead);
        assertEquals("bound-exhausted cursor must report DONE", DONE, bounded.state());
        assertTrue("bound-exhausted cursor must report DONE (isEOF)", bounded.isEOF());
        assertTrue("position must be short of the sstable's full uncompressed length - the file has more data past the assigned bounds",
                   bounded.position() < bounded.uncompressedLength());

        // Byte accounting must reflect the actual position reached, not the whole file's length.
        assertEquals(bounded.position(), bounded.bytesReadSinceSnapshot());
    }

    @Test
    public void lastSegmentReachingTheEndOfFileStopsAtTheFileEnd() throws Throwable
    {
        SSTableReader sstable = flushSinglePartitionPerRowTable();

        List<PartitionPositionBounds> bounds = Collections.singletonList(sstable.getPositionsForFullRange());

        StatefulCursor bounded = new StatefulCursor(sstable, bounds, DiskAccessMode.standard);
        readAllPartitionKeys(bounded, sstable.getPartitioner());

        assertEquals(DONE, bounded.state());
        assertTrue(bounded.isEOF());
        assertEquals("a bound that extends to the end of the file must stop at the file's end",
                     bounded.uncompressedLength(), bounded.position());
    }

    /** Reads to DONE via bound exhaustion, stopping short of true end of file. */
    private static StatefulCursor exhaustBounds(SSTableReader sstable, List<DecoratedKey> allKeysInTokenOrder)
    {
        Range<Token> firstTwo = rangeBetween(sstable.getPartitioner().getMinimumToken(), allKeysInTokenOrder.get(1));
        List<PartitionPositionBounds> bounds = sstable.getPositionsForRanges(Collections.singletonList(firstTwo));
        StatefulCursor bounded = new StatefulCursor(sstable, bounds, DiskAccessMode.standard);
        readAllPartitionKeys(bounded, sstable.getPartitioner());
        assertEquals(DONE, bounded.state());
        assertTrue("test setup: cursor must stop on bound exhaustion, short of the file's end",
                   bounded.position() < bounded.uncompressedLength());
        return bounded;
    }

    /**
     * A cursor in DONE must reject a repeat {@code readPartitionHeader()}, and must reject it before
     * the curr/prev swap. {@code readPartitionHeader(PartitionDescriptor)} throws on its
     * {@code state != PARTITION_START} check, but that is past the swap, so without the guard every
     * rejected call still rotated the descriptors and left prev on content nothing had written.
     * <p>
     * At DONE the slots hold the last partition read in curr and the one before it in prev, on both
     * DONE routes: {@code readPartitionHeader(PartitionDescriptor)} never returns DONE, so the
     * transition always happens in {@code advanceSegment}, with no swap.
     */
    @Test
    public void readPartitionHeaderRejectsReentryAfterBoundExhaustedDone() throws Throwable
    {
        SSTableReader sstable = flushSinglePartitionPerRowTable();
        IPartitioner partitioner = sstable.getPartitioner();
        List<DecoratedKey> allKeysInTokenOrder = readAllPartitionKeys(new StatefulCursor(sstable, DiskAccessMode.standard), partitioner);
        StatefulCursor bounded = exhaustBounds(sstable, allKeysInTokenOrder);

        assertEquals("test setup: currentKey must be the last partition inside the bounds",
                     allKeysInTokenOrder.get(1),
                     partitioner.decorateKey(ByteBufferUtil.clone(bounded.currentKey().getKey())));
        assertEquals("test setup: prevKey must be the partition before it",
                     allKeysInTokenOrder.get(0),
                     partitioner.decorateKey(ByteBufferUtil.clone(bounded.prevKey().getKey())));
        long swapsAtDone = bounded.partitionSwaps();

        for (int attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                bounded.readPartitionHeader();
                fail("readPartitionHeader() must reject re-entry once the cursor is DONE");
            }
            catch (IllegalStateException expected)
            {
                // a rejected call must not have disturbed the cursor's descriptors
                assertEquals("a rejected re-entry must leave currentKey() untouched",
                             allKeysInTokenOrder.get(1),
                             partitioner.decorateKey(ByteBufferUtil.clone(bounded.currentKey().getKey())));
                assertEquals("a rejected re-entry must leave prevKey() untouched",
                             allKeysInTokenOrder.get(0),
                             partitioner.decorateKey(ByteBufferUtil.clone(bounded.prevKey().getKey())));
                assertEquals("a rejected re-entry must not advance the slots",
                             swapsAtDone, bounded.partitionSwaps());
            }
        }
    }

    /**
     * Pins the POST-reset invariant {@code CursorCompactor} depends on: whichever route a cursor took
     * to DONE, {@code resetAfterDone()} leaves {@code prevKey()} on the last partition actually read
     * (consulted as the last key written to an output sstable) and {@code currPartition} cleared.
     * <p>
     * What this does NOT prove, spelled out so it is not over-trusted: on its own it does not
     * discriminate {@code readPartitionHeader()}'s swap-ORDERING fix. Revert that fix AND
     * {@code resetAfterDone()}'s conditional swap together, and the bound-exhaustion route still nets
     * exactly one swap by the time this test looks - the two bugs cancel - so it passes against the
     * fully pre-fix code. {@link #readPartitionHeaderRejectsReentryAfterBoundExhaustedDone} is what
     * catches that, because it inspects {@code prevKey()} BEFORE any reset, where the cancellation
     * has not happened yet. Verified by reverting both fixes and observing exactly that split. This
     * test does discriminate a revert of the conditional swap alone.
     */
    @Test
    public void resetAfterDonePreservesLastReadKeyOnBothDoneRoutes() throws Throwable
    {
        SSTableReader sstable = flushSinglePartitionPerRowTable();
        IPartitioner partitioner = sstable.getPartitioner();
        List<DecoratedKey> allKeysInTokenOrder = readAllPartitionKeys(new StatefulCursor(sstable, DiskAccessMode.standard), partitioner);

        // route 1: bound exhaustion
        StatefulCursor bounded = exhaustBounds(sstable, allKeysInTokenOrder);
        assertTrue(bounded.resetAfterDone());
        assertEquals("bound-exhausted cursor must keep the last partition it read in prevKey()",
                     allKeysInTokenOrder.get(1),
                     partitioner.decorateKey(ByteBufferUtil.clone(bounded.prevKey().getKey())));
        assertEquals("the stale current partition must be cleared", 0, bounded.currPartition().keyLength());
        assertFalse("resetAfterDone() is once-only", bounded.resetAfterDone());

        // route 2: true end of file (DONE returned by the read itself)
        StatefulCursor unbounded = new StatefulCursor(sstable, DiskAccessMode.standard);
        readAllPartitionKeys(unbounded, partitioner);
        assertEquals(DONE, unbounded.state());
        assertEquals(unbounded.uncompressedLength(), unbounded.position());
        assertTrue(unbounded.resetAfterDone());
        assertEquals("EOF cursor must keep the last partition it read in prevKey()",
                     allKeysInTokenOrder.get(PARTITION_COUNT - 1),
                     partitioner.decorateKey(ByteBufferUtil.clone(unbounded.prevKey().getKey())));
        assertEquals("the stale current partition must be cleared", 0, unbounded.currPartition().keyLength());
    }
}
