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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.journal.Segment;
import org.apache.cassandra.notifications.InitialSSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentReferenceTrackerTest
{
    private static final UUID LOCAL_HOST = UUID.randomUUID();
    private static final UUID REMOTE_HOST = UUID.randomUUID();
    private static final long LOG_ID = 100L;

    @Test
    public void testInitialAddRefsEverySegmentInTheInterval()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(4L, mockSegment(4L, LOG_ID, 0, 5));
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));
        segments.put(6L, mockSegment(6L, LOG_ID, 25, 40));
        segments.put(7L, mockSegment(7L, LOG_ID, 45, 60));
        segments.put(8L, mockSegment(8L, LOG_ID, 65, 80));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 10, 60));

        tracker.handleNotification(new InitialSSTableAddedNotification(List.of(sstable)), null);

        for (long segment = 5; segment <= 7; segment++)
            assertEquals("segment " + segment, 1L, tracker.referenceCountForTesting(segment));
        assertFalse(tracker.isReferenced(4));
        assertFalse(tracker.isReferenced(8));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testAddedRepairedSSTableHoldsNoRefs()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));
        segments.put(6L, mockSegment(6L, LOG_ID, 25, 40));
        segments.put(7L, mockSegment(7L, LOG_ID, 45, 60));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = repaired(offsets(LOG_ID, 10, 60));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        for (long segment = 5; segment <= 7; segment++)
            assertFalse("segment " + segment, tracker.isReferenced(segment));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testUnrepairedSSTableWithoutCoordinatorLogOffsetsHoldsNoRefs()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = sstable(ImmutableCoordinatorLogOffsets.NONE, () -> false, LOCAL_HOST, ReplicationType.tracked);

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertFalse(tracker.isReferenced(5));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testStreamedSSTableHoldsNoRefs()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = sstable(offsets(LOG_ID, 10, 20), () -> false, REMOTE_HOST, ReplicationType.tracked);

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertFalse(tracker.isReferenced(5));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testUntrackedTableSSTableHoldsNoRefs()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = sstable(offsets(LOG_ID, 10, 20), () -> false, LOCAL_HOST, ReplicationType.untracked);

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertFalse(tracker.isReferenced(5));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testEvictReleasesReferencesAndFiresCallback()
    {
        int[] calls = { 0 };
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        for (long s = 5; s <= 7; s++)
            segments.put(s, mockSegment(s, LOG_ID, (int) s * 10, (int) s * 10 + 9));

        SegmentReferenceTracker tracker = newTracker(segments, () -> calls[0]++);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 50, 79));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        for (long segment = 5; segment <= 7; segment++)
            assertEquals(1L, tracker.referenceCountForTesting(segment));

        tracker.evict(List.of(sstable));

        for (long segment = 5; segment <= 7; segment++)
            assertFalse("segment " + segment, tracker.isReferenced(segment));
        assertEquals(0, tracker.trackedSstableCountForTesting());
        assertEquals("evicting the last referrer fires the unreferenced callback", 1, calls[0]);
    }

    @Test
    public void testEvictOnlyReleasesGivenSstables()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader a = unrepaired(offsets(LOG_ID, 10, 15), "a");
        SSTableReader b = unrepaired(offsets(LOG_ID, 16, 20), "b");

        tracker.handleNotification(new SSTableAddedNotification(List.of(a, b), null), null);
        assertEquals(2L, tracker.referenceCountForTesting(5));

        tracker.evict(List.of(a));
        assertTrue(tracker.isReferenced(5));
        assertEquals(1L, tracker.referenceCountForTesting(5));

        tracker.evict(List.of(b));
        assertFalse(tracker.isReferenced(5));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testAddIsIdempotent()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 10, 20));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testMultipleDisjointIntervalsRefEachContainedSegment()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(3L, mockSegment(3L, LOG_ID, 0, 100));
        segments.put(5L, mockSegment(5L, LOG_ID, 200, 300));
        segments.put(9L, mockSegment(9L, LOG_ID, 500, 550));
        segments.put(10L, mockSegment(10L, LOG_ID, 551, 600));

        SegmentReferenceTracker tracker = newTracker(segments);
        ImmutableCoordinatorLogOffsets offsets = new ImmutableCoordinatorLogOffsets.Builder()
                                                 .add(LOG_ID, 0, 100)
                                                 .add(LOG_ID, 500, 600)
                                                 .build();
        SSTableReader sstable = unrepaired(offsets);

        tracker.handleNotification(new InitialSSTableAddedNotification(List.of(sstable)), null);

        assertEquals(1L, tracker.referenceCountForTesting(3));
        assertEquals(1L, tracker.referenceCountForTesting(9));
        assertEquals(1L, tracker.referenceCountForTesting(10));
        assertFalse(tracker.isReferenced(5));
    }

    @Test
    public void testMultipleIntervalsInSameSegmentCountedOnce()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 0, 300));

        SegmentReferenceTracker tracker = newTracker(segments);
        ImmutableCoordinatorLogOffsets offsets = new ImmutableCoordinatorLogOffsets.Builder()
                                                 .add(LOG_ID, 0, 100)
                                                 .add(LOG_ID, 200, 300)
                                                 .build();
        SSTableReader sstable = unrepaired(offsets);

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(List.of(sstable.getFilename()), tracker.referrerDescriptors(5));
    }

    @Test
    public void testCompactionPreservesRefsWhenInputAndOutputOverlapSameSegment()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 50));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader input = unrepaired(offsets(LOG_ID, 10, 30), "input");
        SSTableReader output = unrepaired(offsets(LOG_ID, 10, 50), "output");

        tracker.handleNotification(new SSTableAddedNotification(List.of(input), null), null);
        assertEquals(1L, tracker.referenceCountForTesting(5));

        tracker.handleNotification(new SSTableListChangedNotification(List.of(output),
                                                                      List.of(input),
                                                                      OperationType.COMPACTION),
                                   null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testCompactionToRepairedOutputReleasesRefs()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 50));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader input = unrepaired(offsets(LOG_ID, 10, 30));
        SSTableReader output = repaired(offsets(LOG_ID, 10, 50));

        tracker.handleNotification(new SSTableAddedNotification(List.of(input), null), null);
        assertEquals(1L, tracker.referenceCountForTesting(5));

        tracker.handleNotification(new SSTableListChangedNotification(List.of(output),
                                                                      List.of(input),
                                                                      OperationType.COMPACTION),
                                   null);

        assertFalse(tracker.isReferenced(5));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testRepairPromotionReleasesRefs()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        for (long s = 5; s <= 7; s++)
            segments.put(s, mockSegment(s, LOG_ID, (int) s * 10, (int) s * 10 + 9));

        SegmentReferenceTracker tracker = newTracker(segments);
        AtomicReference<Boolean> repaired = new AtomicReference<>(false);
        SSTableReader sstable = sstableWithRepairSupplier(offsets(LOG_ID, 50, 79), repaired::get);

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        for (long segment = 5; segment <= 7; segment++)
            assertEquals(1L, tracker.referenceCountForTesting(segment));

        repaired.set(true);
        tracker.handleNotification(new SSTableRepairStatusChanged(List.of(sstable)), null);

        for (long segment = 5; segment <= 7; segment++)
            assertFalse("segment " + segment, tracker.isReferenced(segment));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testRepairPromotionWithClearedOffsetsReleasesRefs()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        for (long s = 5; s <= 7; s++)
            segments.put(s, mockSegment(s, LOG_ID, (int) s * 10, (int) s * 10 + 9));

        SegmentReferenceTracker tracker = newTracker(segments);
        AtomicReference<Boolean> repaired = new AtomicReference<>(false);
        AtomicReference<ImmutableCoordinatorLogOffsets> offsetsRef = new AtomicReference<>(offsets(LOG_ID, 50, 79));

        SSTableReader sstable = Mockito.mock(SSTableReader.class);
        Mockito.when(sstable.isRepaired()).thenAnswer(inv -> repaired.get());
        Mockito.when(sstable.getSSTableMetadata()).thenReturn(stats(LOCAL_HOST));
        Mockito.when(sstable.getCoordinatorLogOffsets()).thenAnswer(inv -> offsetsRef.get());
        TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
        Mockito.when(tableMetadata.replicationType()).thenReturn(ReplicationType.tracked);
        Mockito.when(sstable.metadata()).thenReturn(tableMetadata);
        Mockito.when(sstable.getFilename()).thenReturn("sstable");

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        for (long segment = 5; segment <= 7; segment++)
            assertEquals(1L, tracker.referenceCountForTesting(segment));

        // When promoted, SSTableReader.mutatePromotedToRepairedAndReload marks repaired and clears coordinatorLogOffsets.
        repaired.set(true);
        offsetsRef.set(ImmutableCoordinatorLogOffsets.NONE);
        tracker.handleNotification(new SSTableRepairStatusChanged(List.of(sstable)), null);

        for (long segment = 5; segment <= 7; segment++)
            assertFalse("segment " + segment, tracker.isReferenced(segment));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testRepairStatusFlippingBackToUnrepairedReAcquires()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        AtomicReference<Boolean> repaired = new AtomicReference<>(true);
        SSTableReader sstable = sstableWithRepairSupplier(offsets(LOG_ID, 10, 20), repaired::get);

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        assertEquals(0, tracker.trackedSstableCountForTesting());

        repaired.set(false);
        tracker.handleNotification(new SSTableRepairStatusChanged(List.of(sstable)), null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(1, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testMultipleSstablesAccumulateRefsOnSharedSegments()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 50, 59));
        segments.put(6L, mockSegment(6L, LOG_ID, 60, 69));
        segments.put(7L, mockSegment(7L, LOG_ID, 70, 79));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader a = unrepaired(offsets(LOG_ID, 50, 65), "a");
        SSTableReader b = unrepaired(offsets(LOG_ID, 65, 79), "b");

        tracker.handleNotification(new SSTableAddedNotification(List.of(a, b), null), null);

        assertEquals(1L, tracker.referenceCountForTesting(5));
        assertEquals(2L, tracker.referenceCountForTesting(6));
        assertEquals(1L, tracker.referenceCountForTesting(7));

        tracker.handleNotification(new SSTableListChangedNotification(List.of(),
                                                                      List.of(a),
                                                                      OperationType.COMPACTION),
                                   null);

        assertFalse(tracker.isReferenced(5));
        assertEquals("reference from b remains", 1L, tracker.referenceCountForTesting(6));
        assertEquals(1L, tracker.referenceCountForTesting(7));
    }

    @Test
    public void testReleaseOfUntrackedSstableIsNoOp()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 10, 20));

        assertFalse(tracker.isReferenced(5));

        tracker.handleNotification(new SSTableListChangedNotification(List.of(),
                                                                      List.of(sstable),
                                                                      OperationType.COMPACTION),
                                   null);

        assertFalse(tracker.isReferenced(5));
    }

    @Test
    public void testCallbackFiredWhenLastReferenceReleased()
    {
        int[] calls = { 0 };
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        for (long s = 5; s <= 7; s++)
            segments.put(s, mockSegment(s, LOG_ID, (int) s * 10, (int) s * 10 + 9));

        SegmentReferenceTracker tracker = newTracker(segments, () -> calls[0]++);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 50, 79));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);
        assertEquals("adding does not fire the unreferenced callback", 0, calls[0]);

        tracker.handleNotification(new SSTableListChangedNotification(List.of(),
                                                                      List.of(sstable),
                                                                      OperationType.COMPACTION),
                                   null);

        assertFalse(tracker.isReferenced(5));
        assertEquals("releasing the last reference fires the unreferenced callback", 1, calls[0]);
    }

    @Test
    public void testCallbackNotFiredWhileSegmentStillReferenced()
    {
        int[] calls = { 0 };
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments, () -> calls[0]++);
        SSTableReader a = unrepaired(offsets(LOG_ID, 10, 15), "a");
        SSTableReader b = unrepaired(offsets(LOG_ID, 16, 20), "b");

        tracker.handleNotification(new SSTableAddedNotification(List.of(a, b), null), null);
        assertEquals(2L, tracker.referenceCountForTesting(5));

        tracker.handleNotification(new SSTableListChangedNotification(List.of(),
                                                                      List.of(a),
                                                                      OperationType.COMPACTION),
                                   null);

        assertTrue(tracker.isReferenced(5));
        assertEquals(0, calls[0]);
    }

    @Test
    public void testSSTableWithNoOverlappingSegmentsIsNotTracked()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 100, 200));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertEquals(0, tracker.trackedSstableCountForTesting());
        assertTrue(tracker.trackedSSTables().isEmpty());
        assertFalse(tracker.isReferenced(5));

        tracker.handleNotification(new SSTableListChangedNotification(List.of(), List.of(sstable), OperationType.COMPACTION),
                                   null);
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    @Test
    public void testSegmentOverloadsAndNullHandling()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        Segment<ShortMutationId, Mutation> segment5 = mockSegment(5L, LOG_ID, 10, 20);
        segments.put(5L, segment5);

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 10, 20), "sstable5");

        assertFalse(tracker.isReferenced(segment5));
        assertFalse(tracker.isReferenced((Segment<ShortMutationId, Mutation>) null));
        assertEquals(0, tracker.referenceCount(segment5));
        assertEquals(0, tracker.referenceCount((Segment<ShortMutationId, Mutation>) null));
        assertEquals(List.of(), tracker.referrerDescriptors(segment5));
        assertEquals(List.of(), tracker.referrerDescriptors((Segment<ShortMutationId, Mutation>) null));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertTrue(tracker.isReferenced(segment5));
        assertEquals(1, tracker.referenceCount(segment5));
        assertEquals(1L, tracker.referenceCountForTesting(segment5));
        assertEquals(List.of("sstable5"), tracker.referrerDescriptors(segment5));
    }

    @Test
    public void testDefensivePruneOfEmptyReferrers()
    {
        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, mockSegment(5L, LOG_ID, 10, 20));

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 10, 20), "sstable5");

        tracker.addReferenceForTesting(5L, sstable);
        assertTrue(tracker.isReferenced(5));
        assertEquals(1, tracker.referenceCount(5));

        tracker.evict(List.of(sstable));
        assertFalse(tracker.isReferenced(5));
        assertEquals(0, tracker.referenceCount(5));
        assertTrue(tracker.referrerDescriptors(5).isEmpty());
    }

    @Test
    public void testTrackingActiveSegment()
    {
        MutationJournal.ActiveOffsetRanges activeRanges = new MutationJournal.ActiveOffsetRanges();
        activeRanges.update(new ShortMutationId(LOG_ID, 10));
        activeRanges.update(new ShortMutationId(LOG_ID, 50));

        @SuppressWarnings("unchecked")
        Segment<ShortMutationId, Mutation> activeSegment = Mockito.mock(Segment.class);
        Mockito.when(activeSegment.id()).thenReturn(5L);
        Mockito.when(activeSegment.keyStats()).thenReturn(activeRanges);

        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(5L, activeSegment);

        SegmentReferenceTracker tracker = newTracker(segments);
        SSTableReader sstable = unrepaired(offsets(LOG_ID, 20, 30));

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertTrue(tracker.isReferenced(activeSegment));
        assertEquals(1, tracker.referenceCount(activeSegment));
        assertEquals(List.of(sstable.getFilename()), tracker.referrerDescriptors(activeSegment));

        tracker.handleNotification(new SSTableListChangedNotification(List.of(), List.of(sstable), OperationType.COMPACTION),
                                   null);

        assertFalse(tracker.isReferenced(activeSegment));
        assertEquals(0, tracker.referenceCount(activeSegment));
    }

    @Test
    public void testSSTableWithMultipleLogsRefsAllOverlappingSegments()
    {
        long log1 = 100L;
        long log2 = 200L;

        Map<Long, Segment<ShortMutationId, Mutation>> segments = new HashMap<>();
        segments.put(1L, mockSegment(1L, log1, 10, 20));
        segments.put(2L, mockSegment(2L, log2, 30, 40));

        SegmentReferenceTracker tracker = newTracker(segments);
        ImmutableCoordinatorLogOffsets offsets = new ImmutableCoordinatorLogOffsets.Builder()
                                                 .add(log1, 15, 25)
                                                 .add(log2, 35, 45)
                                                 .build();
        SSTableReader sstable = unrepaired(offsets, "sstable-multi-log");

        tracker.handleNotification(new SSTableAddedNotification(List.of(sstable), null), null);

        assertTrue(tracker.isReferenced(1L));
        assertTrue(tracker.isReferenced(2L));
        assertEquals(1, tracker.referenceCount(1L));
        assertEquals(1, tracker.referenceCount(2L));
        assertEquals(1, tracker.trackedSstableCountForTesting());

        tracker.handleNotification(new SSTableListChangedNotification(List.of(), List.of(sstable), OperationType.COMPACTION),
                                   null);

        assertFalse(tracker.isReferenced(1L));
        assertFalse(tracker.isReferenced(2L));
        assertEquals(0, tracker.referenceCount(1L));
        assertEquals(0, tracker.referenceCount(2L));
        assertEquals(0, tracker.trackedSstableCountForTesting());
    }

    // -- helpers ---------------------------------------------------------

    private static SegmentReferenceTracker newTracker(Map<Long, Segment<ShortMutationId, Mutation>> segments)
    {
        return newTracker(segments, () -> {});
    }

    private static SegmentReferenceTracker newTracker(Map<Long, Segment<ShortMutationId, Mutation>> segments, Runnable onSegmentsUnreferenced)
    {
        return new SegmentReferenceTracker(onSegmentsUnreferenced, () -> LOCAL_HOST, segments::values);
    }

    @SuppressWarnings("unchecked")
    private static Segment<ShortMutationId, Mutation> mockSegment(long segmentId, long logId, int minOffset, int maxOffset)
    {
        Segment<ShortMutationId, Mutation> segment = Mockito.mock(Segment.class);
        Mockito.when(segment.id()).thenReturn(segmentId);
        Mockito.when(segment.keyStats()).thenReturn(MutationJournal.StaticOffsetRanges.of(logId, minOffset, maxOffset));
        return segment;
    }

    private static ImmutableCoordinatorLogOffsets offsets(long logId, int start, int end)
    {
        return new ImmutableCoordinatorLogOffsets.Builder().add(logId, start, end).build();
    }

    private static SSTableReader unrepaired(ImmutableCoordinatorLogOffsets offsets)
    {
        return stub(offsets, false, "sstable");
    }

    private static SSTableReader unrepaired(ImmutableCoordinatorLogOffsets offsets, String name)
    {
        return stub(offsets, false, name);
    }

    private static SSTableReader repaired(ImmutableCoordinatorLogOffsets offsets)
    {
        return stub(offsets, true, "sstable");
    }

    private static SSTableReader stub(ImmutableCoordinatorLogOffsets offsets, boolean isRepaired, String name)
    {
        return sstableWithRepairSupplier(offsets, () -> isRepaired, name);
    }

    private static SSTableReader sstableWithRepairSupplier(ImmutableCoordinatorLogOffsets offsets,
                                                           BooleanSupplier isRepairedSupplier)
    {
        return sstableWithRepairSupplier(offsets, isRepairedSupplier, "sstable");
    }

    private static SSTableReader sstableWithRepairSupplier(ImmutableCoordinatorLogOffsets offsets,
                                                           BooleanSupplier isRepairedSupplier,
                                                           String name)
    {
        return sstable(offsets, isRepairedSupplier, LOCAL_HOST, ReplicationType.tracked, name);
    }

    private static SSTableReader sstable(ImmutableCoordinatorLogOffsets offsets,
                                         BooleanSupplier isRepairedSupplier,
                                         UUID originatingHostId,
                                         ReplicationType replicationType)
    {
        return sstable(offsets, isRepairedSupplier, originatingHostId, replicationType, "sstable");
    }

    private static SSTableReader sstable(ImmutableCoordinatorLogOffsets offsets,
                                         BooleanSupplier isRepairedSupplier,
                                         UUID originatingHostId,
                                         ReplicationType replicationType,
                                         String name)
    {
        SSTableReader reader = Mockito.mock(SSTableReader.class);
        Mockito.when(reader.isRepaired()).thenAnswer(inv -> isRepairedSupplier.getAsBoolean());
        Mockito.when(reader.getSSTableMetadata()).thenReturn(stats(originatingHostId));
        Mockito.when(reader.getCoordinatorLogOffsets()).thenReturn(offsets);
        TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
        Mockito.when(tableMetadata.replicationType()).thenReturn(replicationType);
        Mockito.when(reader.metadata()).thenReturn(tableMetadata);
        Mockito.when(reader.getFilename()).thenReturn(name);

        return reader;
    }

    private static StatsMetadata stats(UUID originatingHostId)
    {
        return new StatsMetadata(new EstimatedHistogram(155),
                                 new EstimatedHistogram(118),
                                 IntervalSet.empty(),
                                 0L,
                                 0L,
                                 Cell.NO_DELETION_TIME,
                                 Cell.NO_DELETION_TIME,
                                 Cell.NO_TTL,
                                 Cell.NO_TTL,
                                 -1.0,
                                 TombstoneHistogram.createDefault(),
                                 0,
                                 List.of(),
                                 Slice.ALL,
                                 false,
                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                 0L,
                                 0L,
                                 Double.NaN,
                                 originatingHostId,
                                 ActiveRepairService.NO_PENDING_REPAIR,
                                 false,
                                 ImmutableCoordinatorLogOffsets.NONE,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }
}
