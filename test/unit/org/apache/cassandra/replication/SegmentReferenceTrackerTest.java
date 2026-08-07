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
package org.apache.cassandra.replication;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.notifications.InitialSSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentReferenceTrackerTest
{
    @Test
    public void testInitialAddRefsEverySegmentInTheInterval()
    {
        int startSegment = 5;
        int endSegment = 7;
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader sstable = unrepaired(intervals(startSegment, 0, endSegment, 100));

        tracker.handleNotification(new InitialSSTableAddedNotification(List.of(sstable)), null);

        // Coarse range covers segments 5..7 inclusive.
        for (long segment = startSegment; segment <= endSegment; segment++)
            assertEquals("segment " + segment, 1L, tracker.referenceCountForTesting(segment));
        assertFalse(tracker.isReferenced(4));
        assertFalse(tracker.isReferenced(8));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testAddedRepairedSSTableHoldsNoRefs()
    {
        int startSegment = 5;
        int endSegment = 7;
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader sstable = repaired(intervals(startSegment, 0, endSegment, 100));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        for (long segment = startSegment; segment <= endSegment; segment++)
            assertFalse("segment " + segment, tracker.isReferenced(segment));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testUnrepairedSSTableWithoutCoordinatorLogOffsetsHoldsNoRefs()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        // Unrepaired, but carries no tracked mutations (empty coordinatorLogOffsets) -> not tracked. This is the
        // untracked / pre-migration case: such an sstable's commitLogIntervals reference the commit log, not the
        // mutation journal, so it must never hold a segment reference (CASSANDRA-21406).
        SSTableReader sstable = sstable(intervals(5, 0, 7, 100), () -> false, coordinatorLogOffsets(false));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        for (long segment = 5; segment <= 7; segment++)
            assertFalse("segment " + segment, tracker.isReferenced(segment));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testAddIsIdempotent()
    {
        int startSegment = 5;
        int endSegment = 7;
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader sstable = unrepaired(intervals(startSegment, 0, endSegment, 100));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testMultipleDisjointIntervalsRefEachContainedSegment()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        // Two disjoint intervals: [3:0..3:100] and [9:0..10:50].
        IntervalSet.Builder<CommitLogPosition> builder = new IntervalSet.Builder<>();
        builder.add(new CommitLogPosition(3, 0), new CommitLogPosition(3, 100));
        builder.add(new CommitLogPosition(9, 0), new CommitLogPosition(10, 50));
        SSTableReader sstable = unrepaired(builder.build());

        tracker.handleNotification(new InitialSSTableAddedNotification(List.of(sstable)), null);

        assertEquals(1L, tracker.referenceCountForTesting(3));
        assertEquals(1L, tracker.referenceCountForTesting(9));
        assertEquals(1L, tracker.referenceCountForTesting(10));
        // Gap between disjoint intervals is not referenced.
        for (long gap = 4; gap <= 8; gap++)
            assertFalse("segment " + gap, tracker.isReferenced(gap));
    }

    @Test
    public void testEmptyIntervalsHoldNoRefs()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader sstable = unrepaired(IntervalSet.empty());

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        // Tracked sstable but no segments to ref.
        assertEquals(1, tracker.trackedSstableCountForTesting());
        assertFalse(tracker.isReferenced(0));
    }

    @Test
    public void testCompactionPreservesRefsWhenInputAndOutputOverlapSameSegment()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader input = unrepaired(intervals(5, 0, 5, 100));
        SSTableReader output = unrepaired(intervals(5, 0, 5, 200));

        tracker.handleNotification(new SSTableAddedNotification(List.of(input), null), null);
        assertEquals(1L, tracker.referenceCountForTesting(5));

        // Compaction emits the SSTableListChangedNotification with added + removed atomically.
        tracker.handleNotification(
        new SSTableListChangedNotification(List.of(output),
                                           List.of(input),
                                           OperationType.COMPACTION),
        null);

        // Net: still one unrepaired sstable referencing segment 5.
        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testCompactionToRepairedOutputReleasesRefs()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader input = unrepaired(intervals(5, 0, 5, 100));
        SSTableReader output = repaired(intervals(5, 0, 5, 200));

        tracker.handleNotification(new SSTableAddedNotification(List.of(input), null), null);
        assertEquals(1L, tracker.referenceCountForTesting(5));

        tracker.handleNotification(
        new SSTableListChangedNotification(List.of(output),
                                           List.of(input),
                                           OperationType.COMPACTION),
        null);

        assertFalse(tracker.isReferenced(5));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testRepairPromotionReleasesRefs()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        AtomicReference<Boolean> repaired = new AtomicReference<>(false);
        SSTableReader sstable = sstableWithRepairSupplier(intervals(5, 0, 7, 0), repaired::get);

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        for (long segment = 5; segment <= 7; segment++)
            assertEquals(1L, tracker.referenceCountForTesting(segment));

        // Promote to repaired; repair-status-changed delivers the transition.
        repaired.set(true);
        tracker.handleNotification(new SSTableRepairStatusChanged(List.of(sstable)), null);

        for (long segment = 5; segment <= 7; segment++)
            assertFalse("segment " + segment, tracker.isReferenced(segment));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testRepairStatusFlippingBackToUnrepairedReAcquires()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        AtomicReference<Boolean> repaired = new AtomicReference<>(true);
        SSTableReader sstable = sstableWithRepairSupplier(intervals(5, 0, 5, 100), repaired::get);

        // Starts repaired -> add is a no-op.
        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        assertEquals(0, tracker.trackedSstableCountForTesting());

        // Flip back to unrepaired (e.g. failed repair session) and deliver repair-status-changed.
        repaired.set(false);
        tracker.handleNotification(new SSTableRepairStatusChanged(List.of(sstable)), null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testMultipleSstablesAccumulateRefsOnSharedSegments()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader a = unrepaired(intervals(5, 0, 6, 0));
        SSTableReader b = unrepaired(intervals(6, 0, 7, 0));

        tracker.handleNotification(new SSTableAddedNotification(List.of(a, b), null), null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(2L, tracker.referenceCountForTesting(6));
        assertEquals(1L, tracker.referenceCountForTesting(7));

        // Drop one; the shared segment still has a holder.
        tracker.handleNotification(
        new SSTableListChangedNotification(List.of(),
                                           List.of(a),
                                           OperationType.COMPACTION),
        null);

        assertFalse(tracker.isReferenced(5));
        assertEquals(1L, tracker.referenceCountForTesting(6));
        assertEquals(1L, tracker.referenceCountForTesting(7));
    }

    @Test
    public void testReleaseOfUntrackedSstableIsNoOp()
    {
        SegmentReferenceTracker tracker = new SegmentReferenceTracker();
        SSTableReader sstable = unrepaired(intervals(5, 0, 5, 100));

        assertFalse(tracker.isReferenced(5));

        // Never added -- removal must not underflow.
        tracker.handleNotification(
        new SSTableListChangedNotification(List.of(),
                                           List.of(sstable),
                                           OperationType.COMPACTION),
        null);

        assertFalse(tracker.isReferenced(5));
    }

    @Test
    public void testCallbackFiredWhenLastReferenceReleased()
    {
        int[] calls = { 0 };
        SegmentReferenceTracker tracker = new SegmentReferenceTracker(() -> calls[0]++);
        SSTableReader sstable = unrepaired(intervals(5, 0, 7, 0));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        assertEquals("adding never fires the unreferenced callback", 0, calls[0]);

        // Releasing the only referrer drives segments 5..7 to zero -> callback fires (once per notification).
        tracker.handleNotification(
        new SSTableListChangedNotification(List.of(), List.of(sstable), OperationType.COMPACTION),
        null);

        assertFalse(tracker.isReferenced(5));
        assertEquals("releasing the last reference fires the unreferenced callback", 1, calls[0]);
    }

    @Test
    public void testCallbackNotFiredWhileSegmentStillReferenced()
    {
        int[] calls = { 0 };
        SegmentReferenceTracker tracker = new SegmentReferenceTracker(() -> calls[0]++);
        SSTableReader a = unrepaired(intervals(5, 0, 5, 100));
        SSTableReader b = unrepaired(intervals(5, 0, 5, 200));

        tracker.handleNotification(new SSTableAddedNotification(List.of(a, b), null), null);
        assertEquals(2L, tracker.referenceCountForTesting(5));

        // Releasing one of two referrers leaves segment 5 still referenced -> no callback.
        tracker.handleNotification(
        new SSTableListChangedNotification(List.of(), List.of(a), OperationType.COMPACTION),
        null);

        assertTrue(tracker.isReferenced(5));
        assertEquals("callback must not fire while the segment is still referenced", 0, calls[0]);
    }

    // -- helpers ---------------------------------------------------------

    private static IntervalSet<CommitLogPosition> intervals(long startSegment, int startPosition, long endSegment, int endPosition)
    {
        assertTrue("startSegment " + startSegment + " is less than or equal to endSegment " + endSegment, startSegment <= endSegment);
        assertTrue("startPosition " + startPosition + " is less than or equal to endPosition " + endPosition, startPosition <= endPosition);
        return new IntervalSet<>(new CommitLogPosition(startSegment, startPosition),
                                 new CommitLogPosition(endSegment, endPosition));
    }

    private static SSTableReader unrepaired(IntervalSet<CommitLogPosition> intervals)
    {
        return stub(intervals, false);
    }

    private static SSTableReader repaired(IntervalSet<CommitLogPosition> intervals)
    {
        return stub(intervals, true);
    }

    private static SSTableReader stub(IntervalSet<CommitLogPosition> intervals, boolean isRepaired)
    {
        return sstableWithRepairSupplier(intervals, () -> isRepaired);
    }

    private static SSTableReader sstableWithRepairSupplier(IntervalSet<CommitLogPosition> intervals,
                                                           BooleanSupplier isRepairedSupplier)
    {
        return sstable(intervals, isRepairedSupplier, coordinatorLogOffsets(true));
    }

    private static SSTableReader sstable(IntervalSet<CommitLogPosition> intervals,
                                         BooleanSupplier isRepairedSupplier,
                                         ImmutableCoordinatorLogOffsets coordinatorLogOffsets)
    {
        SSTableReader reader = Mockito.mock(SSTableReader.class);
        Mockito.when(reader.isRepaired()).thenAnswer(ref -> isRepairedSupplier.getAsBoolean());
        Mockito.when(reader.getSSTableMetadata()).thenReturn(stats(intervals));
        Mockito.when(reader.getCoordinatorLogOffsets()).thenReturn(coordinatorLogOffsets);
        return reader;
    }

    private static ImmutableCoordinatorLogOffsets coordinatorLogOffsets(boolean nonEmpty)
    {
        // A bare mock reports isEmpty()==false (Mockito's default boolean), which is all the tracker inspects.
        return nonEmpty ? Mockito.mock(ImmutableCoordinatorLogOffsets.class) : ImmutableCoordinatorLogOffsets.NONE;
    }

    private static StatsMetadata stats(IntervalSet<CommitLogPosition> intervals)
    {
        return new StatsMetadata(new EstimatedHistogram(155),                     // estimatedPartitionSize
                                 new EstimatedHistogram(118),                     // estimatedCellPerPartitionCount
                                 intervals,                                       // commitLogIntervals
                                 0L,                                              // minTimestamp
                                 0L,                                              // maxTimestamp
                                 Cell.NO_DELETION_TIME,                           // minLocalDeletionTime
                                 Cell.NO_DELETION_TIME,                           // maxLocalDeletionTime
                                 Cell.NO_TTL,                                     // minTTL
                                 Cell.NO_TTL,                                     // maxTTL
                                 -1.0,                                            // compressionRatio
                                 TombstoneHistogram.createDefault(),
                                 0,                                               // sstableLevel
                                 List.of(),                                       // clusteringTypes
                                 Slice.ALL,                                       // coveredClustering
                                 false,                                           // hasLegacyCounterShards
                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                 0L,                                              // totalColumnsSet
                                 0L,                                              // totalRows
                                 Double.NaN,                                      // tokenSpaceCoverage
                                 null,                                            // originatingHostId
                                 ActiveRepairService.NO_PENDING_REPAIR,
                                 false,                                           // hasPartitionLevelDeletions
                                 ImmutableCoordinatorLogOffsets.NONE,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }
}
