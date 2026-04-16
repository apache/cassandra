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
package org.apache.cassandra.dht;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeSplitterTest
{
    private static Murmur3Partitioner partitioner;

    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        DatabaseDescriptor.daemonInitialization();
        partitioner = (Murmur3Partitioner) DatabaseDescriptor.getPartitioner();
    }

    // ========== No-split cases ==========

    @Test
    public void testNoBoundaries()
    {
        AbstractBounds<PartitionPosition> range = range(0, 500);
        NormalizedRanges<Token> boundaries = NormalizedRanges.empty();

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(1, splits.size());
        assertEquals(range, splits.get(0));
    }

    @Test
    public void testRangeEntirelyBeforeBoundary()
    {
        AbstractBounds<PartitionPosition> range = range(0, 100);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 400));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(1, splits.size());
        assertContiguous(range, splits);
    }

    @Test
    public void testRangeEntirelyAfterBoundary()
    {
        AbstractBounds<PartitionPosition> range = range(500, 800);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(100, 300));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(1, splits.size());
        assertContiguous(range, splits);
    }

    @Test
    public void testRangeEntirelyInsideBoundary()
    {
        AbstractBounds<PartitionPosition> range = range(200, 300);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(100, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(1, splits.size());
        assertContiguous(range, splits);
    }

    // ========== Single boundary cases ==========

    @Test
    public void testRangeCrossesBoundaryLeft()
    {
        // Range starts before boundary and ends inside it
        AbstractBounds<PartitionPosition> range = range(0, 300);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(2, splits.size());
        assertContiguous(range, splits);

        // First split is the gap (outside boundary), second is inside
        assertFalse(boundaries.intersects(splits.get(0).right.getToken()));
        assertTrue(boundaries.intersects(splits.get(1).right.getToken()));
    }

    @Test
    public void testRangeCrossesBoundaryRight()
    {
        // Range starts inside boundary and extends past it
        AbstractBounds<PartitionPosition> range = range(300, 700);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(2, splits.size());
        assertContiguous(range, splits);

        // First split is inside boundary, second is the gap (outside)
        assertTrue(boundaries.intersects(splits.get(0).right.getToken()));
        assertFalse(boundaries.intersects(splits.get(1).right.getToken()));
    }

    @Test
    public void testRangeSpansBoundaryEntirely()
    {
        // Range starts before boundary and ends after it
        AbstractBounds<PartitionPosition> range = range(0, 700);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(3, splits.size());
        assertContiguous(range, splits);

        // gap, inside, gap
        assertFalse(boundaries.intersects(splits.get(0).right.getToken()));
        assertTrue(boundaries.intersects(splits.get(1).right.getToken()));
        assertFalse(boundaries.intersects(splits.get(2).right.getToken()));
    }

    // ========== Multiple boundary cases ==========

    @Test
    public void testTwoDisjointBoundaries()
    {
        AbstractBounds<PartitionPosition> range = range(0, 900);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(100, 300), tokenRange(500, 700));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        // gap, inside1, gap, inside2, gap
        assertEquals(5, splits.size());
        assertContiguous(range, splits);

        assertFalse(boundaries.intersects(splits.get(0).right.getToken()));
        assertTrue(boundaries.intersects(splits.get(1).right.getToken()));
        assertFalse(boundaries.intersects(splits.get(2).right.getToken()));
        assertTrue(boundaries.intersects(splits.get(3).right.getToken()));
        assertFalse(boundaries.intersects(splits.get(4).right.getToken()));
    }

    @Test
    public void testRangeStartsInsideFirstBoundary()
    {
        // No leading gap
        AbstractBounds<PartitionPosition> range = range(150, 900);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(100, 300), tokenRange(500, 700));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        // inside1, gap, inside2, gap
        assertEquals(4, splits.size());
        assertContiguous(range, splits);

        assertTrue(boundaries.intersects(splits.get(0).right.getToken()));
        assertFalse(boundaries.intersects(splits.get(1).right.getToken()));
        assertTrue(boundaries.intersects(splits.get(2).right.getToken()));
        assertFalse(boundaries.intersects(splits.get(3).right.getToken()));
    }

    @Test
    public void testRangeEndsInsideSecondBoundary()
    {
        // No trailing gap
        AbstractBounds<PartitionPosition> range = range(0, 600);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(100, 300), tokenRange(500, 700));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        // gap, inside1, gap, inside2
        assertEquals(4, splits.size());
        assertContiguous(range, splits);

        assertFalse(boundaries.intersects(splits.get(0).right.getToken()));
        assertTrue(boundaries.intersects(splits.get(1).right.getToken()));
        assertFalse(boundaries.intersects(splits.get(2).right.getToken()));
        assertTrue(boundaries.intersects(splits.get(3).right.getToken()));
    }

    @Test
    public void testRangeSpansOnlyGapBetweenBoundaries()
    {
        AbstractBounds<PartitionPosition> range = range(350, 450);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(100, 300), tokenRange(500, 700));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertEquals(1, splits.size());
        assertContiguous(range, splits);
        assertFalse(boundaries.intersects(splits.get(0).right.getToken()));
    }

    // ========== Boundary type variations ==========

    @Test
    public void testBoundsRange()
    {
        // Bounds are [inclusive, inclusive]
        AbstractBounds<PartitionPosition> range = new Bounds<>(token(0).minKeyBound(),
                                                               token(700).maxKeyBound());
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertTrue("Bounds crossing a boundary should split", splits.size() >= 2);
        assertCoverage(range, splits);
    }

    @Test
    public void testExcludingBoundsRange()
    {
        // ExcludingBounds are (exclusive, exclusive)
        AbstractBounds<PartitionPosition> range = new ExcludingBounds<>(token(0).maxKeyBound(),
                                                                       token(700).minKeyBound());
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertTrue("ExcludingBounds crossing a boundary should split", splits.size() >= 2);
        assertCoverage(range, splits);
    }

    @Test
    public void testIncludingExcludingBoundsRange()
    {
        // IncludingExcludingBounds are [inclusive, exclusive)
        AbstractBounds<PartitionPosition> range = new IncludingExcludingBounds<>(token(0).minKeyBound(),
                                                                                token(700).minKeyBound());
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        assertTrue("IncludingExcludingBounds crossing a boundary should split", splits.size() >= 2);
        assertCoverage(range, splits);
    }

    // ========== Edge cases ==========

    @Test
    public void testRangeStartsJustBeforeBoundary()
    {
        // Range starts just before the boundary
        AbstractBounds<PartitionPosition> range = range(190, 700);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        // Small gap (190, 200], then inside (200, 500], then trailing gap (500, 700]
        assertEquals(3, splits.size());
        assertContiguous(range, splits);
    }

    @Test
    public void testRangeEndsExactlyAtBoundaryEnd()
    {
        AbstractBounds<PartitionPosition> range = range(0, 500);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 500));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        // gap + inside, ending exactly at boundary
        assertEquals(2, splits.size());
        assertContiguous(range, splits);
    }

    @Test
    public void testFullRingBoundary()
    {
        // A boundary that covers the full token ring
        Token min = partitioner.getMinimumToken();
        NormalizedRanges<Token> fullRing = NormalizedRanges.normalizedRanges(
            Collections.singleton(new Range<>(min, min)));

        AbstractBounds<PartitionPosition> range = range(0, 500);
        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, fullRing);

        // Entire range is inside the boundary -- no split
        assertEquals(1, splits.size());
        assertContiguous(range, splits);
    }

    @Test
    public void testSinglePointRange()
    {
        // A range that covers a single token
        AbstractBounds<PartitionPosition> range = new Bounds<>(token(200).minKeyBound(),
                                                               token(200).maxKeyBound());
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(100, 300));

        List<AbstractBounds<PartitionPosition>> splits = RangeSplitter.splitAtBoundaries(range, boundaries);

        // Single point entirely inside boundary -- no split
        assertEquals(1, splits.size());
    }

    // ========== Min-token / end-of-ring ranges ==========

    @Test
    public void testRangeEndingAtMinTokenSplitsAtInteriorBoundary()
    {
        // A range ending at the minimum token extends to the end of the ring (MIN is the wrap sentinel),
        // so an interior boundary must still split it. Regression: addGapBefore's !isMinimum() guard --
        // without it, MIN <= boundaryStart reads as "remainder ends before boundary" and the whole
        // remainder is emitted as a single outside split, dropping the boundary.
        AbstractBounds<PartitionPosition> range = rangeToMinToken(100);
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(200, 300));

        List<RangeSplitter.Split> splits = RangeSplitter.splitAtBoundariesTagged(range, boundaries);

        // (100, 200] outside, (200, 300] inside, (300, MIN] outside
        assertEquals(3, splits.size());
        assertContiguousTagged(range, splits);
        assertFalse(splits.get(0).isWithinBoundary);
        assertTrue(splits.get(1).isWithinBoundary);
        assertFalse(splits.get(2).isWithinBoundary);
    }

    @Test
    public void testFullRingRangeSplitsAtInteriorBoundary()
    {
        // (MIN, MIN] is a full-ring scan; an interior boundary splits it into three.
        AbstractBounds<PartitionPosition> range = fullRingRange();
        NormalizedRanges<Token> boundaries = normalizedRanges(tokenRange(0, 100));

        List<RangeSplitter.Split> splits = RangeSplitter.splitAtBoundariesTagged(range, boundaries);

        // (MIN, 0] outside, (0, 100] inside, (100, MIN] outside
        assertEquals(3, splits.size());
        assertContiguousTagged(range, splits);
        assertFalse(splits.get(0).isWithinBoundary);
        assertTrue(splits.get(1).isWithinBoundary);
        assertFalse(splits.get(2).isWithinBoundary);
    }

    // ========== Helpers ==========

    private static Token token(long value)
    {
        return new Murmur3Partitioner.LongToken(value);
    }

    private static Range<Token> tokenRange(long left, long right)
    {
        return new Range<>(token(left), token(right));
    }

    /**
     * Creates a Range of PartitionPosition: (left.minKeyBound, right.maxKeyBound]
     */
    private static AbstractBounds<PartitionPosition> range(long left, long right)
    {
        return new Range<>(token(left).minKeyBound(), token(right).maxKeyBound());
    }

    /**
     * Creates a Range of PartitionPosition ending at the minimum token, i.e. extending to the end of the
     * ring (MIN is the wrap sentinel): (left.minKeyBound, MIN.maxKeyBound]
     */
    private static AbstractBounds<PartitionPosition> rangeToMinToken(long left)
    {
        return new Range<>(token(left).minKeyBound(), partitioner.getMinimumToken().maxKeyBound());
    }

    /**
     * Creates a full-ring Range of PartitionPosition: (MIN.minKeyBound, MIN.maxKeyBound]
     */
    private static AbstractBounds<PartitionPosition> fullRingRange()
    {
        Token min = partitioner.getMinimumToken();
        return new Range<>(min.minKeyBound(), min.maxKeyBound());
    }

    @SafeVarargs
    private static NormalizedRanges<Token> normalizedRanges(Range<Token>... ranges)
    {
        return NormalizedRanges.normalizedRanges(Arrays.asList(ranges));
    }

    /**
     * Verify splits are contiguous and cover the original range.
     */
    private static void assertContiguous(AbstractBounds<PartitionPosition> original,
                                         List<AbstractBounds<PartitionPosition>> splits)
    {
        assertFalse("Splits should not be empty", splits.isEmpty());
        assertEquals("First split should start at original start", original.left, splits.get(0).left);
        assertEquals("Last split should end at original end", original.right, splits.get(splits.size() - 1).right);

        for (int i = 0; i < splits.size() - 1; i++)
            assertEquals("Splits should be contiguous at index " + i,
                         splits.get(i).right, splits.get(i + 1).left);
    }

    /**
     * Verify splits cover the original range (start and end match).
     */
    private static void assertCoverage(AbstractBounds<PartitionPosition> original,
                                       List<AbstractBounds<PartitionPosition>> splits)
    {
        assertFalse("Splits should not be empty", splits.isEmpty());
        assertEquals("First split should start at original start", original.left, splits.get(0).left);
        assertEquals("Last split should end at original end", original.right, splits.get(splits.size() - 1).right);
    }

    /**
     * Verify tagged splits are contiguous and cover the original range.
     */
    private static void assertContiguousTagged(AbstractBounds<PartitionPosition> original,
                                               List<RangeSplitter.Split> splits)
    {
        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>(splits.size());
        for (RangeSplitter.Split split : splits)
            ranges.add(split.range);
        assertContiguous(original, ranges);
    }
}
