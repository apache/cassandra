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

package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pure arithmetic tests for {@link ZeroCopySSTableSlice}. No sstables, no disk, no schema, no config -- the
 * three published helpers ({@link ZeroCopySSTableSlice#runCount}, {@link ZeroCopySSTableSlice#dataLength},
 * {@link ZeroCopySSTableSlice#deadBytes}) and the run grouping underneath them are static and take only
 * fabricated {@link PartitionPositionBounds}, so this is where their off-by-ones have to be caught.
 *
 * <p>The properties under test, restated independently of the implementation ({@code L} is the cell length,
 * i.e. the compression chunk length for a compressed parent and the CRC chunk length for an uncompressed one):
 * <ul>
 *   <li><b>grouping</b>: sections {@code s[i-1]} and {@code s[i]} share a run exactly when
 *       {@code s[i].lower / L <= (s[i-1].upper - 1) / L + 1}, i.e. the next section's first cell is the
 *       previous section's last cell or the one immediately after it. A gap of a whole cell or more leaves
 *       that cell out of the middle, which cannot be sent as one byte range, so it is a new run;</li>
 *   <li><b>dataLength</b>: every run but the last contributes whole cells; the last stops at its last live
 *       byte, because the slice's uncompressed length is what a reader is allowed to address;</li>
 *   <li><b>deadBytes</b>: {@code dataLength - sum(upper - lower)} -- what the slice is long enough to hold,
 *       less what was asked for. Never negative;</li>
 *   <li>a single run carries exactly {@code lastUpper - (firstLower / L) * L}, and no set of sections can
 *       ever carry more than that.</li>
 * </ul>
 *
 * <p>{@code ZeroCopySSTableSliceTest} exercises the same helpers against real sstables; this class is the one
 * that runs without a server.
 */
public class ZeroCopySSTableSliceArithmeticTest
{
    /**
     * Realistic cell lengths: 1024 (what the hand-computed case below is written against), the 16 KiB
     * {@code CompressionParams.DEFAULT_CHUNK_LENGTH}, and the two other chunk lengths in common use.
     */
    private static final int[] CELL_LENGTHS = { 1024, 4 * 1024, 16 * 1024, 64 * 1024 };

    /** Fixed so a sweep failure reproduces; the value is echoed in every sweep failure message. */
    private static final long SEED = 20260726L;

    // ------------------------------------------------------------------------------------------------
    // The case lifted verbatim from ZeroCopySSTableSliceTest -- hand-computed expectations
    // ------------------------------------------------------------------------------------------------

    /** The published arithmetic helpers, on fabricated sections: no sstable, no I/O. */
    @Test
    public void runAndDeadSpaceArithmetic()
    {
        int cellLength = 1024;

        // One section: dead space is the head of its first cell, and nothing else -- dataLength stops at hi.
        List<PartitionPositionBounds> one = Collections.singletonList(new PartitionPositionBounds(1500, 4000));
        assertEquals(1, ZeroCopySSTableSlice.runCount(one, cellLength));
        assertEquals(4000 - 1024, ZeroCopySSTableSlice.dataLength(one, cellLength));
        assertEquals(1500 % 1024, ZeroCopySSTableSlice.deadBytes(one, cellLength));

        // Adjacent sections: still one run, and the gap between them is dead.
        List<PartitionPositionBounds> near = Arrays.asList(new PartitionPositionBounds(1024, 1500),
                                                          new PartitionPositionBounds(1800, 2400));
        assertEquals(1, ZeroCopySSTableSlice.runCount(near, cellLength));
        assertEquals(300, ZeroCopySSTableSlice.deadBytes(near, cellLength));

        // Cells 1 and 2 are adjacent, so a gap that stays inside them is still one run...
        assertEquals(1, ZeroCopySSTableSlice.runCount(Arrays.asList(new PartitionPositionBounds(1100, 1200),
                                                                   new PartitionPositionBounds(2100, 2200)),
                                                     cellLength));
        // ... but skipping cell 2 entirely is two runs, and then only the LAST one may stop mid-cell: run 0
        // contributes its whole cell 1 (1024 bytes) and run 1 stops at 3200, i.e. 128 bytes into cell 3.
        List<PartitionPositionBounds> far = Arrays.asList(new PartitionPositionBounds(1100, 1200),
                                                          new PartitionPositionBounds(3100, 3200));
        assertEquals(2, ZeroCopySSTableSlice.runCount(far, cellLength));
        assertEquals(1024 + 128, ZeroCopySSTableSlice.dataLength(far, cellLength));
        assertEquals(1024 + 128 - 200, ZeroCopySSTableSlice.deadBytes(far, cellLength));

        // A section ending exactly on a cell boundary ends on the cell BEFORE it: [1024, 2048) is cell 1 alone, so
        // cell 2 is the next one and a section starting there is contiguous.
        assertEquals(1, ZeroCopySSTableSlice.runCount(Arrays.asList(new PartitionPositionBounds(1024, 2048),
                                                                   new PartitionPositionBounds(2048, 2500)),
                                                     cellLength));

        // A run per section, each in its own distant cell.
        List<PartitionPositionBounds> many = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            many.add(new PartitionPositionBounds(i * 10L * cellLength, i * 10L * cellLength + 100));
        assertEquals(10, ZeroCopySSTableSlice.runCount(many, cellLength));
        // Nine whole cells plus the last run's 100 bytes; every section is cell-aligned so there is no prefix.
        assertEquals(9L * cellLength + 100, ZeroCopySSTableSlice.dataLength(many, cellLength));
        assertEquals(9L * cellLength + 100 - 1000, ZeroCopySSTableSlice.deadBytes(many, cellLength));
    }

    // ------------------------------------------------------------------------------------------------
    // The grouping rule: one cell of slack joins, two split
    // ------------------------------------------------------------------------------------------------

    /**
     * The rule stated as a boundary: a gap that keeps the next section in the following cell is still one byte
     * range, because that cell is copied whole anyway. A gap that skips a cell entirely is not, because the
     * skipped cell would have to be carried for nothing.
     */
    @Test
    public void oneCellOfSlackJoinsAndTwoSplits()
    {
        for (int L : CELL_LENGTHS)
        {
            for (long k : new long[]{ 0, 1, 2, 17, 1000, 1L << 20 })
            {
                String ctx = "L=" + L + " k=" + k;
                // same cell
                assertEquals(ctx, 1, runCount(L, k * L + 10, k * L + 20, k * L + 30, k * L + 40));
                // the immediately following cell -- still one run
                assertEquals(ctx, 1, runCount(L, k * L + 10, k * L + 20, (k + 1) * L, (k + 1) * L + 20));
                assertEquals(ctx, 1, runCount(L, k * L + 10, k * L + 20, (k + 1) * L + 999, (k + 1) * L + 1000));
                // one whole cell skipped -- two runs
                assertEquals(ctx, 2, runCount(L, k * L + 10, k * L + 20, (k + 2) * L, (k + 2) * L + 20));
                assertEquals(ctx, 2, runCount(L, k * L + 10, k * L + 20, (k + 9) * L + 5, (k + 9) * L + 6));

                // and the exclusive upper bound: a section ending exactly on a boundary ends on the cell
                // BEFORE it, so the section starting there is contiguous and the one after that is not
                assertEquals(ctx, 1, runCount(L, k * L, (k + 1L) * L, (k + 1L) * L, (k + 1L) * L + 1));
                assertEquals(ctx, 2, runCount(L, k * L, (k + 1L) * L, (k + 2L) * L, (k + 2L) * L + 1));
                // contrast: one byte more reaches into cell k+1, so cell k+2 is then contiguous
                assertEquals(ctx, 1, runCount(L, k * L, (k + 1L) * L + 1, (k + 2L) * L, (k + 2L) * L + 1));
            }
        }
    }

    /** Every run's index bounds, so the grouping is pinned as a partition of the section list, not just a count. */
    @Test
    public void runBoundsPartitionTheSectionList()
    {
        int L = 1024;
        List<PartitionPositionBounds> sections = sections(100, 200,          // cell 0
                                                          1100, 1200,        // cell 1     -- joins
                                                          3100, 3200,        // cell 3     -- cell 2 skipped
                                                          4000, 4100,        // cells 3..4 -- joins
                                                          9000, 9100);       // cell 8     -- cells 5..7 skipped
        List<int[]> bounds = ZeroCopySSTableSlice.runBounds(sections, L);
        assertEquals(3, bounds.size());
        assertArrayIs("run 0", 0, 1, bounds.get(0));
        assertArrayIs("run 1", 2, 3, bounds.get(1));
        assertArrayIs("run 2", 4, 4, bounds.get(2));
        assertEquals(bounds.size(), ZeroCopySSTableSlice.runCount(sections, L));

        // run 0 contributes cells 0..1 whole, run 1 cells 3..4 whole, and only the last run may stop
        // mid-cell: run 2 ends 9100 - 8*1024 bytes into cell 8
        assertEquals(2L * L + 2L * L + (9100 - 8L * L), ZeroCopySSTableSlice.dataLength(sections, L));
        assertEquals(ZeroCopySSTableSlice.dataLength(sections, L) - usefulBytes(sections),
                     ZeroCopySSTableSlice.deadBytes(sections, L));
    }

    // ------------------------------------------------------------------------------------------------
    // Degenerate shapes
    // ------------------------------------------------------------------------------------------------

    /** A single section is one run whose dead space is exactly the head of its first cell. */
    @Test
    public void aSingleSectionIsOneRunWithOnlyAHeadPrefixDead()
    {
        Random rnd = new Random(SEED);
        for (int L : CELL_LENGTHS)
        {
            for (int t = 0; t < 500; t++)
            {
                long lo = nextLong(rnd, 1L << 36);
                long hi = lo + 1 + nextLong(rnd, 8L * L);
                List<PartitionPositionBounds> one = sections(lo, hi);
                String ctx = "L=" + L + " lo=" + lo + " hi=" + hi;

                assertEquals(ctx, 1, ZeroCopySSTableSlice.runCount(one, L));
                assertEquals(ctx, hi - (lo / L) * (long) L, ZeroCopySSTableSlice.dataLength(one, L));
                assertEquals(ctx, lo % L, ZeroCopySSTableSlice.deadBytes(one, L));
            }
        }
    }

    /** A section starting exactly on a cell boundary carries nothing dead, whatever the cell length. */
    @Test
    public void aCellAlignedSingleSectionHasNoDeadSpace()
    {
        for (int L : CELL_LENGTHS)
        {
            for (long k : new long[]{ 0, 1, 3, 4096, 1L << 20 })
            {
                List<PartitionPositionBounds> one = sections(k * L, k * L + 7L * L);
                assertEquals("L=" + L + " k=" + k, 0, ZeroCopySSTableSlice.deadBytes(one, L));
                assertEquals("L=" + L + " k=" + k, 7L * L, ZeroCopySSTableSlice.dataLength(one, L));
            }
        }
    }

    /** Nothing may overflow into a negative length at offsets a real terabyte-scale sstable reaches. */
    @Test
    public void largeOffsetsAreComputedInLongArithmetic()
    {
        final long fourTiB = 4L * 1024 * 1024 * 1024 * 1024;
        for (int L : CELL_LENGTHS)
        {
            List<PartitionPositionBounds> sections = sections(fourTiB + 7, fourTiB + 7 + 100,
                                                              fourTiB + 50L * L, fourTiB + 50L * L + 100);
            String ctx = "L=" + L;
            assertEquals(ctx, 2, ZeroCopySSTableSlice.runCount(sections, L));
            long dataLength = ZeroCopySSTableSlice.dataLength(sections, L);
            assertTrue(ctx + " dataLength must stay positive, got " + dataLength, dataLength > 0);
            // run 0 is the single cell holding fourTiB + 7; run 1 stops 100 bytes into its own first cell
            assertEquals(ctx, (long) L + 100, dataLength);
            assertEquals(ctx, dataLength - 200, ZeroCopySSTableSlice.deadBytes(sections, L));
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Randomised sweep -- every helper recomputed independently of the implementation
    // ------------------------------------------------------------------------------------------------

    @Test
    public void sweepOverRandomSectionLists()
    {
        long seed = SEED + 1;
        Random rnd = new Random(seed);
        int checked = 0;
        try
        {
            for (int L : CELL_LENGTHS)
            {
                for (int t = 0; t < 3000; t++)
                {
                    int n = 1 + (int) nextLong(rnd, 12);
                    List<PartitionPositionBounds> sections = new ArrayList<>(n);
                    long p = nextLong(rnd, 1L << 30);
                    for (int i = 0; i < n; i++)
                    {
                        // gaps up to four cells wide, so both sides of the grouping rule are hit often
                        long lo = p + nextLong(rnd, 4L * L);
                        long hi = lo + 1 + nextLong(rnd, 2L * L);
                        sections.add(new PartitionPositionBounds(lo, hi));
                        p = hi;
                    }
                    assertSectionInvariants("L=" + L + " sections=" + describe(sections), sections, L);
                    checked++;
                }
            }
        }
        catch (AssertionError | RuntimeException e)
        {
            throw new AssertionError("sweep failed with seed=" + seed + " after " + checked +
                                     " cases: " + e, e);
        }
        assertTrue("sweep should have checked a lot of cases, got " + checked, checked > 10000);
    }

    /**
     * Recompute the grouping, the length and the dead space independently of the implementation, and check the
     * structural invariants a caller relies on.
     */
    private static void assertSectionInvariants(String ctx, List<PartitionPositionBounds> sections, int L)
    {
        int n = sections.size();

        // 1. the grouping rule, restated as plain division
        Set<Integer> expectedRunStarts = new HashSet<>();
        expectedRunStarts.add(0);
        for (int i = 1; i < n; i++)
        {
            long firstCell = sections.get(i).lowerPosition / L;
            long previousLastCell = (sections.get(i - 1).upperPosition - 1) / L;
            if (firstCell > previousLastCell + 1)
                expectedRunStarts.add(i);
        }

        List<int[]> bounds = ZeroCopySSTableSlice.runBounds(sections, L);
        assertEquals(ctx + " runCount", bounds.size(), ZeroCopySSTableSlice.runCount(sections, L));
        assertEquals(ctx + " run count must be the number of run starts", expectedRunStarts.size(), bounds.size());
        assertTrue(ctx + " at least one run", bounds.size() >= 1);
        assertTrue(ctx + " at most one run per section", bounds.size() <= n);

        // 2. the bounds partition [0, n) contiguously and in order
        assertEquals(ctx + " first run starts at section 0", 0, bounds.get(0)[0]);
        assertEquals(ctx + " last run ends at the last section", n - 1, bounds.get(bounds.size() - 1)[1]);
        for (int r = 0; r < bounds.size(); r++)
        {
            assertTrue(ctx + " run " + r + " is non-empty", bounds.get(r)[0] <= bounds.get(r)[1]);
            assertTrue(ctx + " run " + r + " must be a predicted run start",
                       expectedRunStarts.contains(bounds.get(r)[0]));
            if (r > 0)
                assertEquals(ctx + " runs must be contiguous at " + r, bounds.get(r - 1)[1] + 1, bounds.get(r)[0]);
        }

        // 3. dataLength: whole cells for every run but the last, exact for the last
        long expectedLength = 0;
        for (int r = 0; r < bounds.size(); r++)
        {
            long firstCell = sections.get(bounds.get(r)[0]).lowerPosition / L;
            long hi = sections.get(bounds.get(r)[1]).upperPosition;
            expectedLength += (r == bounds.size() - 1)
                              ? hi - firstCell * (long) L
                              : ((hi - 1) / L - firstCell + 1) * (long) L;
        }
        long dataLength = ZeroCopySSTableSlice.dataLength(sections, L);
        assertEquals(ctx + " dataLength", expectedLength, dataLength);
        assertTrue(ctx + " dataLength must be positive", dataLength > 0);

        // 4. dead space is what is carried less what was asked for, and is never negative
        long useful = usefulBytes(sections);
        long dead = ZeroCopySSTableSlice.deadBytes(sections, L);
        assertEquals(ctx + " deadBytes", dataLength - useful, dead);
        assertTrue(ctx + " deadBytes must not be negative, got " + dead, dead >= 0);

        // 5. the slice can never be longer than the span from its first cell to its last live byte, and is
        //    exactly that when there is a single run
        long span = sections.get(n - 1).upperPosition - (sections.get(0).lowerPosition / L) * (long) L;
        assertTrue(ctx + " dataLength " + dataLength + " must not exceed the span " + span, dataLength <= span);
        if (bounds.size() == 1)
            assertEquals(ctx + " a single run carries the whole span", span, dataLength);
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    private static int runCount(int cellLength, long... bounds)
    {
        return ZeroCopySSTableSlice.runCount(sections(bounds), cellLength);
    }

    /** {@code bounds} is a flat sequence of {@code lower, upper} pairs. */
    private static List<PartitionPositionBounds> sections(long... bounds)
    {
        assertEquals("bounds must come in pairs", 0, bounds.length % 2);
        List<PartitionPositionBounds> out = new ArrayList<>(bounds.length / 2);
        for (int i = 0; i < bounds.length; i += 2)
            out.add(new PartitionPositionBounds(bounds[i], bounds[i + 1]));
        return out;
    }

    private static long usefulBytes(List<PartitionPositionBounds> sections)
    {
        long useful = 0;
        for (PartitionPositionBounds section : sections)
            useful += section.upperPosition - section.lowerPosition;
        return useful;
    }

    private static void assertArrayIs(String ctx, int from, int to, int[] bound)
    {
        assertEquals(ctx + " from", from, bound[0]);
        assertEquals(ctx + " to", to, bound[1]);
    }

    private static String describe(List<PartitionPositionBounds> sections)
    {
        StringBuilder sb = new StringBuilder("[");
        for (PartitionPositionBounds section : sections)
            sb.append('[').append(section.lowerPosition).append(',').append(section.upperPosition).append(')');
        return sb.append(']').toString();
    }

    /** Uniform in [0, bound). {@code Math.floorMod} keeps it non-negative even for {@code Long.MIN_VALUE}. */
    private static long nextLong(Random rnd, long bound)
    {
        return Math.floorMod(rnd.nextLong(), bound);
    }
}
