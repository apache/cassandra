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
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.ChunkRange;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.CopyPlan;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pure arithmetic tests for {@link ZeroCopySSTableSplitter}. No sstables, no disk, no schema, no config --
 * every method exercised here is static and operates only on primitives, so this test is where the off-by-one
 * bugs in the chunk-run computation have to be caught.
 *
 * <p>The properties under test, restated independently of the implementation:
 * <ul>
 *   <li>{@code i = lo / L} -- the chunk containing the child's first live byte;</li>
 *   <li>{@code j = (hi - 1) / L} -- the chunk containing the child's LAST live byte. Not {@code hi / L}:
 *       {@code hi} is exclusive, so a {@code hi} that lands exactly on a chunk boundary must NOT pull in the
 *       chunk that starts there;</li>
 *   <li>{@code C = j - i + 1}, {@code Dp = hi - i*L}, {@code shift = i*L}, {@code dead = lo mod L};</li>
 *   <li>{@code (C-1)*L < Dp <= C*L} -- the last chunk holds at least one live byte and at most a full chunk
 *       of them;</li>
 *   <li>{@code Dp - dead == hi - lo} -- the child's live span is exactly the parent's;</li>
 *   <li>everything is computed in long arithmetic; an {@code int} product {@code k*L} overflows past 2 GiB.</li>
 * </ul>
 */
public class ZeroCopySSTableSplitterArithmeticTest
{
    private static final int K = 1024;
    private static final int L4 = 4 * K;
    private static final int L16 = 16 * K;
    private static final int L64 = 64 * K;
    private static final int[] REAL_CHUNK_LENGTHS = { L4, L16, L64 };

    /** Fixed so a sweep failure reproduces; the value is echoed in every sweep failure message. */
    private static final long SEED = 20260726L;

    /**
     * The alignment {@code copyPlan} works to, restated here rather than read from the class under test: these
     * tests are the definition of it. It has to match {@link org.apache.cassandra.io.util.Reflink#RANGE_ALIGNMENT}.
     */
    private static final long A = 64 * 1024;

    // ------------------------------------------------------------------------------------------------
    // chunkIndexFor / firstChunk: boundary, one before, one after
    // ------------------------------------------------------------------------------------------------

    @Test
    public void chunkIndexForOnAroundAndBetweenBoundaries()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            assertEquals("first byte of the file", 0, ZeroCopySSTableSplitter.chunkIndexFor(0, L));
            assertEquals("last byte of chunk 0", 0, ZeroCopySSTableSplitter.chunkIndexFor(L - 1, L));
            assertEquals("first byte of chunk 1", 1, ZeroCopySSTableSplitter.chunkIndexFor(L, L));
            assertEquals("second byte of chunk 1", 1, ZeroCopySSTableSplitter.chunkIndexFor(L + 1, L));

            for (long k = 0; k < 5; k++)
            {
                long base = k * L;
                assertEquals("boundary k=" + k + " L=" + L, k, ZeroCopySSTableSplitter.chunkIndexFor(base, L));
                assertEquals("boundary+1 k=" + k + " L=" + L, k, ZeroCopySSTableSplitter.chunkIndexFor(base + 1, L));
                assertEquals("boundary-1 k=" + k + " L=" + L,
                             Math.max(0, k - 1), ZeroCopySSTableSplitter.chunkIndexFor(Math.max(0, base - 1), L));
                assertEquals("mid-chunk k=" + k + " L=" + L, k, ZeroCopySSTableSplitter.chunkIndexFor(base + L / 2, L));
                assertEquals("last byte k=" + k + " L=" + L, k, ZeroCopySSTableSplitter.chunkIndexFor(base + L - 1, L));
            }
        }
    }

    @Test
    public void firstChunkIsChunkIndexFor()
    {
        Random rnd = new Random(SEED);
        for (int L : REAL_CHUNK_LENGTHS)
        {
            for (int t = 0; t < 500; t++)
            {
                long lo = nextLong(rnd, 1L << 36);
                assertEquals("lo=" + lo + " L=" + L,
                             ZeroCopySSTableSplitter.chunkIndexFor(lo, L),
                             ZeroCopySSTableSplitter.firstChunk(lo, L));
                assertEquals("lo=" + lo + " L=" + L, lo / L, ZeroCopySSTableSplitter.firstChunk(lo, L));
            }
        }
    }

    /** Division, not a power-of-two bit mask: a masking shortcut would give the wrong answer here. */
    @Test
    public void chunkArithmeticIsPlainDivisionNotAMask()
    {
        assertEquals(3, ZeroCopySSTableSplitter.chunkIndexFor(10, 3));
        assertEquals(1, ZeroCopySSTableSplitter.deadPrefixBytes(10, 3));
        assertEquals(3, ZeroCopySSTableSplitter.lastChunk(10, 3));   // (10-1)/3
        assertEquals(2, ZeroCopySSTableSplitter.lastChunk(9, 3));    // (9-1)/3 -- boundary, not 3
        assertEquals(0, ZeroCopySSTableSplitter.chunkIndexFor(999, 1000));
        assertEquals(1, ZeroCopySSTableSplitter.chunkIndexFor(1000, 1000));
    }

    // ------------------------------------------------------------------------------------------------
    // lastChunk: the (hi - 1)/L vs hi/L distinction
    // ------------------------------------------------------------------------------------------------

    @Test
    public void lastChunkUsesHiMinusOneNotHi()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            // hi exactly on a boundary: the naive hi/L would be one too far.
            for (long k = 1; k <= 5; k++)
            {
                long hi = k * L;
                assertEquals("hi==" + k + "*L must stop at chunk " + (k - 1) + " (L=" + L + ')',
                             k - 1, ZeroCopySSTableSplitter.lastChunk(hi, L));
                assertNotEquals("lastChunk must not be hi/L for a boundary hi",
                                hi / L, ZeroCopySSTableSplitter.lastChunk(hi, L));

                // one byte before the boundary is still in the previous chunk
                assertEquals(k - 1, ZeroCopySSTableSplitter.lastChunk(hi - 1, L));
                // one byte after crosses into chunk k
                assertEquals(k, ZeroCopySSTableSplitter.lastChunk(hi + 1, L));
            }

            assertEquals("a one-byte file lives entirely in chunk 0", 0, ZeroCopySSTableSplitter.lastChunk(1, L));
        }
    }

    /**
     * The whole point of {@code (hi-1)/L}: the child that ends exactly on a chunk boundary must copy one
     * chunk fewer than the child that ends one byte later, and the {@code (C-1)*L < Dp} invariant is what
     * would break if it did not.
     */
    @Test
    public void hiOnChunkBoundaryDoesNotPullInAnExtraChunk()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            ChunkRange exact = ZeroCopySSTableSplitter.chunkRange(0, 2L * L, L);
            assertEquals(0, exact.firstChunk);
            assertEquals("hi == 2L must end at chunk 1", 1, exact.lastChunk);
            assertEquals(2, exact.chunkCount);
            assertEquals(2L * L, exact.dataLength);
            assertRangeInvariants("exact L=" + L, 0, 2L * L, L, exact);

            ChunkRange onePast = ZeroCopySSTableSplitter.chunkRange(0, 2L * L + 1, L);
            assertEquals("one byte past the boundary needs a third chunk", 2, onePast.lastChunk);
            assertEquals(3, onePast.chunkCount);
            assertEquals(2L * L + 1, onePast.dataLength);
            assertRangeInvariants("onePast L=" + L, 0, 2L * L + 1, L, onePast);

            ChunkRange oneBefore = ZeroCopySSTableSplitter.chunkRange(0, 2L * L - 1, L);
            assertEquals(1, oneBefore.lastChunk);
            assertEquals(2, oneBefore.chunkCount);
            assertRangeInvariants("oneBefore L=" + L, 0, 2L * L - 1, L, oneBefore);

            assertEquals("boundary and one-byte-before must copy the same chunk run",
                         exact.chunkCount, oneBefore.chunkCount);
            assertEquals(exact.chunkCount + 1, onePast.chunkCount);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Degenerate children
    // ------------------------------------------------------------------------------------------------

    @Test
    public void singleChunkChildren()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            // whole chunk 0
            ChunkRange whole = ZeroCopySSTableSplitter.chunkRange(0, L, L);
            assertEquals(0, whole.firstChunk);
            assertEquals(0, whole.lastChunk);
            assertEquals(1, whole.chunkCount);
            assertEquals(L, whole.dataLength);
            assertEquals(0, whole.shift);
            assertEquals(0, whole.deadPrefixBytes);
            assertRangeInvariants("whole chunk 0 L=" + L, 0, L, L, whole);

            // a child living entirely inside chunk 7
            long lo = 7L * L + 10;
            long hi = 7L * L + 4000;
            ChunkRange inside = ZeroCopySSTableSplitter.chunkRange(lo, hi, L);
            assertEquals(7, inside.firstChunk);
            assertEquals(7, inside.lastChunk);
            assertEquals(1, inside.chunkCount);
            assertEquals(7L * L, inside.shift);
            assertEquals(10, inside.deadPrefixBytes);
            assertEquals(4000, inside.dataLength);
            assertRangeInvariants("inside chunk 7 L=" + L, lo, hi, L, inside);

            // the very last live byte of chunk 7
            ChunkRange toEnd = ZeroCopySSTableSplitter.chunkRange(7L * L, 8L * L, L);
            assertEquals(7, toEnd.firstChunk);
            assertEquals(7, toEnd.lastChunk);
            assertEquals(1, toEnd.chunkCount);
            assertEquals(L, toEnd.dataLength);
            assertRangeInvariants("chunk 7 exactly L=" + L, 7L * L, 8L * L, L, toEnd);
        }
    }

    @Test
    public void singleByteChildren()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            long[] los = { 0, 1, L - 1, L, L + 1, 5L * L, 5L * L + L / 2, 5L * L + L - 1 };
            for (long lo : los)
            {
                ChunkRange r = ZeroCopySSTableSplitter.chunkRange(lo, lo + 1, L);
                assertEquals("a one-byte child spans exactly one chunk (lo=" + lo + " L=" + L + ')',
                             1, r.chunkCount);
                assertEquals(r.firstChunk, r.lastChunk);
                assertEquals("Dp == dead + 1", r.deadPrefixBytes + 1, r.dataLength);
                assertRangeInvariants("single byte lo=" + lo + " L=" + L, lo, lo + 1, L, r);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Rejections -- an invalid range must throw, never produce a bogus ChunkRange
    // ------------------------------------------------------------------------------------------------

    @Test
    public void emptyRangeIsRejected()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkRange(0, 0, L))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkRange(1000, 1000, L))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkRange(3L * L, 3L * L, L))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void invertedRangeIsRejected()
    {
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkRange(100, 99, L4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkRange(1L << 30, 1, L64))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void negativePositionsAreRejected()
    {
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkIndexFor(-1, L4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.firstChunk(-1, L4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.deadPrefixBytes(-1, L4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkRange(-1, 10, L4))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void nonPositiveHiIsRejectedByLastChunk()
    {
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.lastChunk(0, L4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.lastChunk(-1, L4))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void nonPositiveChunkLengthIsRejected()
    {
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkIndexFor(0, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkIndexFor(0, -4096))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.lastChunk(10, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.deadPrefixBytes(10, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.childDataLength(10, 0, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.chunkRange(0, 10, 0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    /** {@code hi} at or below the start of the first chunk means the child has no live bytes at all. */
    @Test
    public void nonPositiveChildDataLengthIsRejected()
    {
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.childDataLength(L4, 1, L4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.childDataLength(L4 - 1, 1, L4))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.childDataLength(0, 0, L4))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------------------------------
    // deadPrefixBytes
    // ------------------------------------------------------------------------------------------------

    @Test
    public void deadPrefixIsLoModChunkLengthAndZeroExactlyWhenAligned()
    {
        Random rnd = new Random(SEED);
        for (int L : REAL_CHUNK_LENGTHS)
        {
            for (long k = 0; k < 4; k++)
            {
                assertEquals("aligned lo must have no dead prefix",
                             0, ZeroCopySSTableSplitter.deadPrefixBytes(k * L, L));
                if (k > 0)
                    assertEquals(L - 1, ZeroCopySSTableSplitter.deadPrefixBytes(k * L - 1, L));
                assertEquals(1, ZeroCopySSTableSplitter.deadPrefixBytes(k * L + 1, L));
            }

            for (int t = 0; t < 2000; t++)
            {
                long lo = nextLong(rnd, 1L << 36);
                long dead = ZeroCopySSTableSplitter.deadPrefixBytes(lo, L);
                assertEquals("lo=" + lo + " L=" + L, lo % L, dead);
                assertTrue("dead prefix must be < L: lo=" + lo + " L=" + L, dead >= 0 && dead < L);
                assertEquals("dead == 0 iff lo is chunk aligned: lo=" + lo + " L=" + L,
                             lo % L == 0, dead == 0);
                // and it is exactly the distance from the start of the first chunk
                assertEquals(lo - ZeroCopySSTableSplitter.firstChunk(lo, L) * (long) L, dead);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // childDataLength invariant, swept
    // ------------------------------------------------------------------------------------------------

    @Test
    public void childDataLengthInvariantHoldsOverASweep()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            for (long i = 0; i < 8; i++)
            {
                long chunkStart = i * L;
                // every interesting hi in the chunks [i, i+3]
                for (long span = 1; span <= 3L * L; span += Math.max(1, L / 8))
                {
                    checkChildDataLength(chunkStart + span, i, L);
                }
                // and the exact boundaries
                for (long c = 1; c <= 4; c++)
                {
                    checkChildDataLength(chunkStart + c * L, i, L);
                    checkChildDataLength(chunkStart + c * L - 1, i, L);
                    checkChildDataLength(chunkStart + c * L + 1, i, L);
                }
            }
        }
    }

    private static void checkChildDataLength(long hi, long firstChunk, int L)
    {
        long dp = ZeroCopySSTableSplitter.childDataLength(hi, firstChunk, L);
        assertEquals("Dp = hi - i*L (hi=" + hi + " i=" + firstChunk + " L=" + L + ')',
                     hi - firstChunk * (long) L, dp);
        long lastChunk = ZeroCopySSTableSplitter.lastChunk(hi, L);
        long c = lastChunk - firstChunk + 1;
        assertTrue("(C-1)*L < Dp violated: C=" + c + " L=" + L + " Dp=" + dp,
                   (c - 1) * (long) L < dp);
        assertTrue("Dp <= C*L violated: C=" + c + " L=" + L + " Dp=" + dp,
                   dp <= c * (long) L);
    }

    // ------------------------------------------------------------------------------------------------
    // Exhaustive tiny sweep -- cheap and total, catches any off-by-one immediately
    // ------------------------------------------------------------------------------------------------

    @Test
    public void exhaustiveTinyChunkLengthSweep()
    {
        for (int L : new int[]{ 1, 2, 3, 8, 16 })
        {
            for (long lo = 0; lo <= 4L * L; lo++)
            {
                for (long hi = lo + 1; hi <= 4L * L + 3; hi++)
                {
                    ChunkRange r = ZeroCopySSTableSplitter.chunkRange(lo, hi, L);
                    assertRangeInvariants("tiny L=" + L + " lo=" + lo + " hi=" + hi, lo, hi, L, r);
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Brute-force sweep over realistic chunk lengths
    // ------------------------------------------------------------------------------------------------

    @Test
    public void bruteForceSweepOverRealisticChunkLengths()
    {
        long seed = SEED;
        Random rnd = new Random(seed);
        int checked = 0;
        try
        {
            for (int L : REAL_CHUNK_LENGTHS)
            {
                long[] offsets = interestingOffsets(L);
                for (int a = 0; a < offsets.length; a++)
                {
                    for (int b = 0; b < offsets.length; b++)
                    {
                        long lo = offsets[a];
                        long hi = offsets[b];
                        if (hi <= lo)
                            continue;
                        ChunkRange r = ZeroCopySSTableSplitter.chunkRange(lo, hi, L);
                        assertRangeInvariants("systematic L=" + L + " lo=" + lo + " hi=" + hi, lo, hi, L, r);
                        checked++;
                    }
                }

                for (int t = 0; t < 20000; t++)
                {
                    long lo = nextLong(rnd, 1L << 38);
                    long hi = lo + 1 + nextLong(rnd, 6L * L);
                    ChunkRange r = ZeroCopySSTableSplitter.chunkRange(lo, hi, L);
                    assertRangeInvariants("random L=" + L + " lo=" + lo + " hi=" + hi, lo, hi, L, r);
                    checked++;
                }
            }
        }
        catch (AssertionError | RuntimeException e)
        {
            throw new AssertionError("sweep failed with seed=" + seed + " after " + checked +
                                     " cases: " + e, e);
        }
        assertTrue("sweep should have checked a lot of cases, got " + checked, checked > 50000);
    }

    /** Chunk boundaries, and one byte either side of them, at small, medium and very large chunk indices. */
    private static long[] interestingOffsets(int L)
    {
        long[] chunkIndices = { 0, 1, 2, 3, 17, 1000, 65535, 65536, 65537, 1048576 };
        long[] deltas = { -2, -1, 0, 1, 2, L / 2, L - 1 };
        List<Long> out = new ArrayList<>();
        for (long k : chunkIndices)
        {
            for (long d : deltas)
            {
                long v = k * L + d;
                if (v >= 0)
                    out.add(v);
            }
        }
        long[] arr = new long[out.size()];
        for (int i = 0; i < arr.length; i++)
            arr[i] = out.get(i);
        return arr;
    }

    // ------------------------------------------------------------------------------------------------
    // Overflow: positions beyond Integer.MAX_VALUE
    // ------------------------------------------------------------------------------------------------

    @Test
    public void largeOffsetsAreComputedInLongArithmetic()
    {
        final long fortyGiB = 40L * 1024 * 1024 * 1024;   // 42949672960, way past Integer.MAX_VALUE

        for (int L : REAL_CHUNK_LENGTHS)
        {
            long expectedChunk = fortyGiB / L;
            assertEquals("40GiB / " + L, expectedChunk, ZeroCopySSTableSplitter.chunkIndexFor(fortyGiB, L));
            assertTrue("chunk index for 40GiB must be positive", expectedChunk > 0);

            ChunkRange r = ZeroCopySSTableSplitter.chunkRange(fortyGiB + 123, fortyGiB + 123 + 5L * L, L);
            assertRangeInvariants("40GiB L=" + L, fortyGiB + 123, fortyGiB + 123 + 5L * L, L, r);
            assertTrue("shift must not overflow: " + r.shift, r.shift > Integer.MAX_VALUE);
            assertEquals(r.firstChunk * (long) L, r.shift);
        }
    }

    /**
     * The concrete trap: {@code (int) (k * L)} is exactly 0 when {@code k * L == 2^32}, and negative when
     * {@code k * L} lands in {@code [2^31, 2^32)}. Both would silently produce a bogus shift.
     */
    @Test
    public void chunkTimesChunkLengthDoesNotOverflowInt()
    {
        // k * L == 2^32 exactly -> an int product would be 0
        checkNoIntOverflow(1L << 32, L64, 65536);
        checkNoIntOverflow(1L << 32, L16, 262144);
        checkNoIntOverflow(1L << 32, L4, 1048576);

        // k * L == 2^31 exactly -> an int product would be Integer.MIN_VALUE
        checkNoIntOverflow(1L << 31, L64, 32768);
        checkNoIntOverflow(1L << 31, L16, 131072);
        checkNoIntOverflow(1L << 31, L4, 524288);

        // k * L somewhere in the negative half of the int range
        checkNoIntOverflow(3L << 30, L64, (3L << 30) / L64);
    }

    private static void checkNoIntOverflow(long alignedLo, int L, long expectedChunk)
    {
        assertEquals("test setup: lo must be chunk aligned", 0, alignedLo % L);
        assertEquals("chunk index at lo=" + alignedLo + " L=" + L,
                     expectedChunk, ZeroCopySSTableSplitter.firstChunk(alignedLo, L));
        // fixture sanity: an int-truncated product really would give the wrong answer here
        assertNotEquals("test fixture is pointless unless the int product overflows",
                        alignedLo, (long) (int) (expectedChunk * L));

        ChunkRange r = ZeroCopySSTableSplitter.chunkRange(alignedLo, alignedLo + 3L * L, L);
        assertEquals("shift must be the exact long product", alignedLo, r.shift);
        assertTrue("shift must be positive, got " + r.shift, r.shift > 0);
        assertEquals("aligned lo means no dead prefix", 0, r.deadPrefixBytes);
        assertEquals(3, r.chunkCount);
        assertEquals(3L * L, r.dataLength);
        assertRangeInvariants("intOverflow lo=" + alignedLo + " L=" + L,
                              alignedLo, alignedLo + 3L * L, L, r);

        // and one byte in from the alignment, so dead prefix and shift are both large
        ChunkRange off = ZeroCopySSTableSplitter.chunkRange(alignedLo + 1, alignedLo + 1 + L, L);
        assertEquals(alignedLo, off.shift);
        assertEquals(1, off.deadPrefixBytes);
        assertEquals(2, off.chunkCount);
        assertEquals(L + 1, off.dataLength);
        assertRangeInvariants("intOverflow+1 lo=" + (alignedLo + 1) + " L=" + L,
                              alignedLo + 1, alignedLo + 1 + L, L, off);
    }

    /** Nothing anywhere in the arithmetic may go negative for very large but legal inputs. */
    @Test
    public void veryLargePositionsStayPositive()
    {
        long huge = 1L << 45;   // 32 TiB
        for (int L : REAL_CHUNK_LENGTHS)
        {
            ChunkRange r = ZeroCopySSTableSplitter.chunkRange(huge + 7, huge + 7 + 2L * L, L);
            assertTrue(r.firstChunk > 0);
            assertTrue(r.lastChunk >= r.firstChunk);
            assertTrue(r.chunkCount > 0);
            assertTrue(r.dataLength > 0);
            assertTrue(r.shift > 0);
            assertTrue(r.deadPrefixBytes >= 0);
            assertRangeInvariants("huge L=" + L, huge + 7, huge + 7 + 2L * L, L, r);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Adjacent children and the shared boundary chunk
    // ------------------------------------------------------------------------------------------------

    @Test
    public void adjacentChildrenShareTheBoundaryChunkOnlyWhenUnaligned()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            // unaligned boundary -> the chunk containing it is copied into BOTH children
            assertBoundary(0, 3L * L + 17, 7L * L, L, true);
            assertBoundary(L / 3, 3L * L + 1, 4L * L, L, true);
            assertBoundary(0, L - 1, 2L * L, L, true);

            // aligned boundary -> no shared chunk
            assertBoundary(0, 3L * L, 7L * L, L, false);
            assertBoundary(L / 3, 4L * L, 9L * L + 5, L, false);
            assertBoundary(0, L, 2L * L, L, false);
        }
    }

    @Test
    public void sharedBoundaryChunkSweep()
    {
        long seed = SEED + 1;
        Random rnd = new Random(seed);
        try
        {
            for (int L : REAL_CHUNK_LENGTHS)
            {
                for (int t = 0; t < 5000; t++)
                {
                    long lo = nextLong(rnd, 1L << 34);
                    long mid = lo + 1 + nextLong(rnd, 4L * L);
                    long hi = mid + 1 + nextLong(rnd, 4L * L);
                    assertBoundary(lo, mid, hi, L, mid % L != 0);
                }
            }
        }
        catch (AssertionError | RuntimeException e)
        {
            throw new AssertionError("boundary sweep failed with seed=" + seed + ": " + e, e);
        }
    }

    /**
     * A run of adjacent children partitions the parent, so every child's {@code hi} is the next child's
     * {@code lo}. Verify the whole chain: no gaps, no negative overlap, and duplication bounded by exactly
     * one chunk per unaligned interior boundary.
     */
    @Test
    public void chainOfAdjacentChildrenCoversTheParentExactly()
    {
        for (int L : REAL_CHUNK_LENGTHS)
        {
            long[] cuts = { 0, L / 2, L, 3L * L, 3L * L + 1, 6L * L, 6L * L + L - 1, 10L * L };
            List<ChunkRange> ranges = new ArrayList<>();
            for (int i = 0; i + 1 < cuts.length; i++)
                ranges.add(ZeroCopySSTableSplitter.chunkRange(cuts[i], cuts[i + 1], L));

            long liveBytes = 0;
            long shared = 0;
            for (int i = 0; i < ranges.size(); i++)
            {
                ChunkRange r = ranges.get(i);
                assertRangeInvariants("chain[" + i + "] L=" + L, cuts[i], cuts[i + 1], L, r);
                liveBytes += r.dataLength - r.deadPrefixBytes;

                if (i > 0)
                {
                    ChunkRange prev = ranges.get(i - 1);
                    assertEquals("children must be contiguous", prev.hi, r.lo);
                    assertTrue("chunk runs must be non-decreasing", r.firstChunk >= prev.lastChunk);
                    if (r.lo % L == 0)
                    {
                        assertEquals("aligned boundary must not share a chunk",
                                     prev.lastChunk + 1, r.firstChunk);
                        assertEquals(0, r.deadPrefixBytes);
                    }
                    else
                    {
                        assertEquals("unaligned boundary must share exactly one chunk",
                                     prev.lastChunk, r.firstChunk);
                        assertEquals(r.lo % L, r.deadPrefixBytes);
                        shared++;
                    }
                }
            }
            assertEquals("the chain's live bytes must equal the parent's span",
                         cuts[cuts.length - 1] - cuts[0], liveBytes);
            assertTrue("shared chunks are bounded by the number of interior boundaries",
                       shared <= ranges.size() - 1);
            assertTrue("this fixture is supposed to contain unaligned boundaries", shared > 0);
        }
    }

    private static void assertBoundary(long lo, long mid, long hi, int L, boolean expectShared)
    {
        ChunkRange left = ZeroCopySSTableSplitter.chunkRange(lo, mid, L);
        ChunkRange right = ZeroCopySSTableSplitter.chunkRange(mid, hi, L);
        assertRangeInvariants("left L=" + L + " [" + lo + ',' + mid + ')', lo, mid, L, left);
        assertRangeInvariants("right L=" + L + " [" + mid + ',' + hi + ')', mid, hi, L, right);

        String ctx = "L=" + L + " lo=" + lo + " mid=" + mid + " hi=" + hi + " mid%L=" + (mid % L);
        assertEquals("expectShared must match alignment: " + ctx, mid % L != 0, expectShared);

        if (expectShared)
        {
            assertEquals("unaligned boundary shares the chunk: " + ctx, left.lastChunk, right.firstChunk);
            assertTrue("the shared chunk gives the right child a dead prefix: " + ctx,
                       right.deadPrefixBytes > 0);
        }
        else
        {
            assertEquals("aligned boundary shares no chunk: " + ctx, left.lastChunk + 1, right.firstChunk);
            assertEquals("aligned boundary means no dead prefix: " + ctx, 0, right.deadPrefixBytes);
            assertNotEquals(left.lastChunk, right.firstChunk);
        }

        // in both cases the right child's shift never goes backwards and the left child's run ends
        // at or after the byte before the boundary
        assertTrue(right.shift >= left.shift);
        assertEquals(right.lo - right.shift, right.deadPrefixBytes);
        assertTrue("left child must contain the byte before the boundary",
                   left.lastChunk == (mid - 1) / L);
    }

    // ------------------------------------------------------------------------------------------------
    // ChunkRange value semantics
    // ------------------------------------------------------------------------------------------------

    @Test
    public void chunkRangeIsAValue()
    {
        ChunkRange a = ZeroCopySSTableSplitter.chunkRange(1234, 98765, L4);
        ChunkRange b = ZeroCopySSTableSplitter.chunkRange(1234, 98765, L4);
        ChunkRange c = ZeroCopySSTableSplitter.chunkRange(1234, 98766, L4);
        ChunkRange d = ZeroCopySSTableSplitter.chunkRange(1234, 98765, L16);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a ChunkRange"));
        assertTrue(a.toString().contains("lo=1234"));
    }

    // ------------------------------------------------------------------------------------------------
    // chooseByByteShare -- also pure arithmetic (package-visible test hook)
    // ------------------------------------------------------------------------------------------------

    @Test
    public void chooseByByteShareOnAUniformLayout()
    {
        long[] positions = new long[10];
        for (int i = 0; i < positions.length; i++)
            positions[i] = i * 100L;
        long uncompressedLength = 1000;

        assertArrayEqualsInt(new int[]{ 0 },
                             ZeroCopySSTableSplitter.chooseByByteShare(positions, uncompressedLength, 1));
        assertArrayEqualsInt(new int[]{ 0, 5 },
                             ZeroCopySSTableSplitter.chooseByByteShare(positions, uncompressedLength, 2));
        assertArrayEqualsInt(new int[]{ 0, 3, 7 },
                             ZeroCopySSTableSplitter.chooseByByteShare(positions, uncompressedLength, 3));
        assertArrayEqualsInt(new int[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                             ZeroCopySSTableSplitter.chooseByByteShare(positions, uncompressedLength, 10));
    }

    @Test
    public void chooseByByteShareNeverEmitsAnEmptyRun()
    {
        long seed = SEED + 2;
        Random rnd = new Random(seed);
        try
        {
            for (int t = 0; t < 2000; t++)
            {
                int n = 1 + (int) nextLong(rnd, 200);
                long[] positions = new long[n];
                long p = nextLong(rnd, 1L << 20);
                for (int i = 0; i < n; i++)
                {
                    positions[i] = p;
                    p += 1 + nextLong(rnd, 100000);
                }
                long uncompressedLength = p + 1;
                int numChildren = 1 + (int) nextLong(rnd, n);

                int[] runStarts = ZeroCopySSTableSplitter.chooseByByteShare(positions, uncompressedLength, numChildren);

                String ctx = "n=" + n + " numChildren=" + numChildren;
                assertEquals(ctx, numChildren, runStarts.length);
                assertEquals(ctx + " first run must start at 0", 0, runStarts[0]);
                for (int m = 1; m < numChildren; m++)
                {
                    assertTrue(ctx + " runs must be strictly increasing at " + m,
                               runStarts[m] > runStarts[m - 1]);
                    assertTrue(ctx + " run start out of range at " + m,
                               runStarts[m] >= 0 && runStarts[m] < n);
                }
                assertTrue(ctx + " the last run must be non-empty", runStarts[numChildren - 1] <= n - 1);
            }
        }
        catch (AssertionError | RuntimeException e)
        {
            throw new AssertionError("chooseByByteShare sweep failed with seed=" + seed + ": " + e, e);
        }
    }

    /**
     * The load-bearing test for the streaming selector.
     *
     * <p>{@code RunSelector} exists because materialising a {@code long} per partition is a hard ceiling on how
     * large an sstable can be split -- a terabyte of small partitions is tens of gigabytes of heap for an array
     * whose every access is sequential. It is also much harder to read than {@link
     * ZeroCopySSTableSplitter#chooseByByteShare}, which is kept precisely so that this test can assert the two
     * agree exactly, run start for run start, on randomised layouts. Anything that makes them disagree is a
     * regression in the streaming version, not a new policy.
     *
     * <p>The sweep is shaped to hit the two clamps that are the whole difficulty, because they are the only
     * places the array version reaches somewhere other than the cursor:
     * <ul>
     *   <li>{@code numChildren == n} and near it, which forces the tail-room clamp on nearly every run;</li>
     *   <li>partitions large enough that one of them spans several byte-share targets, which forces the
     *       non-empty clamp and, with it, the deferred offset resolution.</li>
     * </ul>
     */
    @Test
    public void runSelectorAgreesWithChooseByByteShare()
    {
        long seed = SEED + 7;
        Random rnd = new Random(seed);
        String ctx = "";
        try
        {
            for (int t = 0; t < 4000; t++)
            {
                int n = 1 + (int) nextLong(rnd, 120);
                // A mix of tiny and huge partitions: a partition wider than total/numChildren is what forces
                // several targets onto one record, hence the non-empty clamp.
                boolean lumpy = (t % 3) == 0;
                long[] positions = new long[n];
                long p = nextLong(rnd, 1L << 20);
                for (int i = 0; i < n; i++)
                {
                    positions[i] = p;
                    p += 1 + nextLong(rnd, lumpy && (i % 7) == 0 ? 5_000_000 : 1000);
                }
                long uncompressedLength = p + 1 + nextLong(rnd, 1000);

                // exercise the extremes as well as the middle
                int numChildren;
                if (t % 4 == 0)
                    numChildren = n;                              // every run on the tail-room clamp
                else if (t % 4 == 1)
                    numChildren = Math.max(1, n - 1);
                else if (t % 4 == 2)
                    numChildren = 1;
                else
                    numChildren = 1 + (int) nextLong(rnd, n);

                ctx = "n=" + n + " numChildren=" + numChildren + " lumpy=" + lumpy;
                int[] expected = ZeroCopySSTableSplitter.chooseByByteShare(positions, uncompressedLength, numChildren);

                ZeroCopySSTableSplitter.RunSelector selector =
                    new ZeroCopySSTableSplitter.RunSelector(uncompressedLength, numChildren, n);
                for (int i = 0; i < n; i++)
                    selector.offer(i, positions[i]);
                ZeroCopySSTableSplitter.Runs runs = selector.finish();

                assertArrayEquals(ctx, expected, runs.runStarts);
                assertEquals(ctx, n, runs.partitionCount);

                // and the offsets it carries have to be the ones those run starts point at, since build() takes
                // every child's lo straight from them
                for (int m = 0; m < numChildren; m++)
                    assertEquals(ctx + " offset of run " + m, positions[expected[m]], runs.runPositions[m]);
            }
        }
        catch (AssertionError | RuntimeException e)
        {
            throw new AssertionError("RunSelector sweep failed with seed=" + seed + " at " + ctx + ": " + e, e);
        }
    }

    /**
     * Byte shares chosen by {@code chooseByByteShare} must be consumable by {@code chunkRange}: every run is
     * non-empty, so every {@code [lo, hi)} it implies is a legal child.
     */
    @Test
    public void chooseByByteShareProducesLegalChunkRanges()
    {
        int n = 137;
        long[] positions = new long[n];
        long p = 0;
        Random rnd = new Random(SEED + 3);
        for (int i = 0; i < n; i++)
        {
            positions[i] = p;
            p += 1 + nextLong(rnd, 40000);
        }
        long uncompressedLength = p + 1;

        for (int numChildren = 1; numChildren <= 16; numChildren++)
        {
            int[] runStarts = ZeroCopySSTableSplitter.chooseByByteShare(positions, uncompressedLength, numChildren);
            for (int L : REAL_CHUNK_LENGTHS)
            {
                long previousHi = -1;
                for (int b = 0; b < runStarts.length; b++)
                {
                    int from = runStarts[b];
                    int to = (b + 1 < runStarts.length) ? runStarts[b + 1] : n;
                    assertTrue("empty run " + b, from < to);
                    long lo = positions[from];
                    long hi = (to < n) ? positions[to] : uncompressedLength;
                    if (previousHi >= 0)
                        assertEquals("runs must be contiguous", previousHi, lo);
                    previousHi = hi;
                    ChunkRange r = ZeroCopySSTableSplitter.chunkRange(lo, hi, L);
                    assertRangeInvariants("share K=" + numChildren + " b=" + b + " L=" + L, lo, hi, L, r);
                }
                assertEquals("the runs must cover the whole parent", uncompressedLength, previousHi);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // copyPlan: the physical half, i.e. the alignment extent sharing needs
    // ------------------------------------------------------------------------------------------------

    /** Without alignment the plan must be exactly what the splitter did before extent sharing existed. */
    @Test
    public void copyPlanWithoutAlignmentIsTheOldBehaviour()
    {
        for (long copyFrom : new long[]{ 0, 1, 4095, A, A + 1, 3 * A - 7, 1L << 40, (1L << 40) + 12345 })
        {
            for (long physical : new long[]{ 1, 4096, A - 1, A, A + 1, 1 << 20, 3L << 30 })
            {
                CopyPlan plan = ZeroCopySSTableSplitter.copyPlan(copyFrom, physical, false, false);
                String ctx = "from=" + copyFrom + " physical=" + physical;
                assertEquals(ctx + " srcStart", copyFrom, plan.srcStart);
                assertEquals(ctx + " pad", 0, plan.headPadBytes);
                assertEquals(ctx + " childLength", physical, plan.childLength);
                assertEquals(ctx + " cloneLength", 0, plan.cloneLength);
                assertEquals(ctx + " tailLength", physical, plan.tailLength());
            }
        }
    }

    /**
     * The three properties the ioctl actually demands, over every residue of the alignment: the source offset
     * is aligned, the destination offset is aligned (it is always 0), and the cloned length is aligned. Plus
     * the two the format demands: the child's byte 0 comes from at or before {@code O(i)}, and the pad is
     * exactly the distance between them.
     */
    @Test
    public void copyPlanAlignsEveryResidue()
    {
        Random rnd = new Random(SEED + 11);
        for (int trial = 0; trial < 20000; trial++)
        {
            // A base far enough out that a 32-bit intermediate would have overflowed long ago
            long copyFrom = trial < A ? trial : (1L << 42) + nextLong(rnd, 1L << 30);
            long physical = 1 + nextLong(rnd, 1L << 26);
            CopyPlan plan = ZeroCopySSTableSplitter.copyPlan(copyFrom, physical, true, true);
            String ctx = "from=" + copyFrom + " physical=" + physical + ' ' + plan;

            assertEquals(ctx + " -- srcStart must be alignment aligned", 0, plan.srcStart % A);
            assertEquals(ctx + " -- cloneLength must be alignment aligned", 0, plan.cloneLength % A);
            assertEquals(ctx + " -- pad is the distance from srcStart to O(i)",
                         copyFrom - plan.srcStart, plan.headPadBytes);
            assertTrue(ctx + " -- pad must be under one alignment unit", plan.headPadBytes < A);
            assertTrue(ctx + " -- srcStart must not overshoot O(i)", plan.srcStart <= copyFrom);
            assertEquals(ctx + " -- childLength", plan.headPadBytes + physical, plan.childLength);

            // the clone must never read past the child's last live byte, i.e. into the parent's trailing slack
            assertTrue(ctx + " -- clone overruns the run", plan.cloneLength <= plan.childLength);
            assertTrue(ctx + " -- tail must be under one alignment unit", plan.tailLength() < A);
            assertEquals(ctx + " -- clone + tail must cover the child exactly",
                         plan.childLength, plan.cloneLength + plan.tailLength());
            // and the range read from the parent is exactly [srcStart, O(i) + physical)
            assertEquals(ctx + " -- range end", copyFrom + physical, plan.srcStart + plan.childLength);
        }
    }

    /**
     * Aligning without sharing is what a test does on a filesystem that cannot share extents: identical layout,
     * nothing cloned. The layout has to be independent of the mechanism or that test proves nothing.
     */
    @Test
    public void copyPlanCanAlignWithoutCloning()
    {
        for (long copyFrom : new long[]{ 0, 1, 999, A - 1, A, A + 1, 5 * A + 4097 })
        {
            CopyPlan shared = ZeroCopySSTableSplitter.copyPlan(copyFrom, 1 << 20, true, true);
            CopyPlan copied = ZeroCopySSTableSplitter.copyPlan(copyFrom, 1 << 20, true, false);
            String ctx = "from=" + copyFrom;
            assertEquals(ctx + " srcStart", shared.srcStart, copied.srcStart);
            assertEquals(ctx + " pad", shared.headPadBytes, copied.headPadBytes);
            assertEquals(ctx + " childLength", shared.childLength, copied.childLength);
            assertEquals(ctx + " nothing cloned", 0, copied.cloneLength);
            assertEquals(ctx + " everything tail", copied.childLength, copied.tailLength());
        }
    }

    /** A run whose whole length is under one alignment unit has nothing to clone, but still gets its pad. */
    @Test
    public void copyPlanBelowOneAlignmentUnitClonesNothing()
    {
        CopyPlan plan = ZeroCopySSTableSplitter.copyPlan(A + 100, 200, true, true);
        assertEquals(A, plan.srcStart);
        assertEquals(100, plan.headPadBytes);
        assertEquals(300, plan.childLength);
        assertEquals(0, plan.cloneLength);
        assertEquals(300, plan.tailLength());
    }

    /** Exactly one alignment unit, and one byte either side of it. */
    @Test
    public void copyPlanAtTheAlignmentBoundary()
    {
        // copyFrom already aligned: no pad, and the whole run is cloneable when it is a whole number of units
        assertEquals(new CopyPlan(2 * A, 0, 3 * A, 3 * A),
                     ZeroCopySSTableSplitter.copyPlan(2 * A, 3 * A, true, true));
        // one byte short of a unit: the last (partial) unit is the tail
        assertEquals(new CopyPlan(2 * A, 0, 3 * A - 1, 2 * A),
                     ZeroCopySSTableSplitter.copyPlan(2 * A, 3 * A - 1, true, true));
        // one byte over: the extra byte is the tail
        assertEquals(new CopyPlan(2 * A, 0, 3 * A + 1, 3 * A),
                     ZeroCopySSTableSplitter.copyPlan(2 * A, 3 * A + 1, true, true));
        // pad and tail together, both maximal
        assertEquals(new CopyPlan(0, A - 1, 3 * A - 1, 2 * A),
                     ZeroCopySSTableSplitter.copyPlan(A - 1, 2 * A, true, true));
    }

    @Test
    public void copyPlanRejectsNonsense()
    {
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.copyPlan(-1, 1024, true, true))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("negative copyFrom");
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.copyPlan(0, 0, true, true))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-positive physicalBytes");
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.copyPlan(0, -4096, false, false))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("non-positive physicalBytes");
    }

    /**
     * {@code share} without {@code align} is refused rather than planned, and that asymmetry is not tidiness:
     * unpadded, {@code srcStart} is {@code O(i)}, which is aligned to nothing, and
     * {@link org.apache.cassandra.io.util.Reflink#tryCloneRange} answers an unaligned offset with
     * {@code IllegalArgumentException} BEFORE it consults its per-filesystem support cache -- deliberately, since a
     * caller bug must not be answered by silently copying. So such a plan does not fall through to the transfer
     * loop, it kills the split. Refusing to BUILD it keeps the combination away from the ioctl entirely.
     *
     * <p>The reverse asymmetry stays legal, and {@link #copyPlanCanAlignWithoutCloning} is the test for it: that is
     * how a test produces the padded layout on a filesystem that cannot share extents.
     */
    @Test
    public void copyPlanRefusesToShareWithoutAligning()
    {
        for (long copyFrom : new long[]{ 0, 1, 4095, A, A + 1, 3 * A - 7, 1L << 40 })
        {
            for (long physical : new long[]{ 1, 4096, A - 1, A, A + 1, 1 << 20 })
            {
                assertThatThrownBy(() -> ZeroCopySSTableSplitter.copyPlan(copyFrom, physical, false, true))
                .describedAs("from=%d physical=%d", copyFrom, physical)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("share requires align");
            }
        }

        // Including the one case that looks harmless -- an already-aligned copyFrom, where the pad would have been
        // zero anyway and the plan would have been perfectly cloneable. The rule is about the ARGUMENTS, not about
        // whether this particular pair happens to work out, so that no caller can come to depend on the exception
        // being conditional.
        assertThatThrownBy(() -> ZeroCopySSTableSplitter.copyPlan(4 * A, 8 * A, false, true))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("share requires align");
        assertEquals("...and the aligned form of that very plan is the one that IS built",
                     new CopyPlan(4 * A, 0, 8 * A, 8 * A),
                     ZeroCopySSTableSplitter.copyPlan(4 * A, 8 * A, true, true));
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    /**
     * Recompute every field of a {@link ChunkRange} independently of the implementation and cross-check the
     * standalone helpers against it.
     */
    private static void assertRangeInvariants(String ctx, long lo, long hi, int L, ChunkRange r)
    {
        long i = lo / L;
        long j = (hi - 1) / L;
        long c = j - i + 1;
        long dp = hi - i * (long) L;
        long shift = i * (long) L;
        long dead = lo % L;

        assertEquals(ctx + " lo", lo, r.lo);
        assertEquals(ctx + " hi", hi, r.hi);
        assertEquals(ctx + " chunkLength", L, r.chunkLength);
        assertEquals(ctx + " firstChunk", i, r.firstChunk);
        assertEquals(ctx + " lastChunk", j, r.lastChunk);
        assertEquals(ctx + " chunkCount", c, r.chunkCount);
        assertEquals(ctx + " dataLength", dp, r.dataLength);
        assertEquals(ctx + " shift", shift, r.shift);
        assertEquals(ctx + " deadPrefixBytes", dead, r.deadPrefixBytes);

        // structural invariants
        assertTrue(ctx + " firstChunk <= lastChunk", r.firstChunk <= r.lastChunk);
        assertTrue(ctx + " chunkCount >= 1", r.chunkCount >= 1);
        assertTrue(ctx + " dataLength > 0", r.dataLength > 0);
        assertTrue(ctx + " shift >= 0", r.shift >= 0);
        assertTrue(ctx + " dead prefix in [0, L)", r.deadPrefixBytes >= 0 && r.deadPrefixBytes < L);
        assertTrue(ctx + " shift <= lo", r.shift <= lo);
        assertEquals(ctx + " lo - shift == dead", r.deadPrefixBytes, lo - r.shift);

        // the load-bearing invariant: the last chunk holds at least one live byte and no more than a chunk
        assertTrue(ctx + " (C-1)*L < Dp  [C=" + r.chunkCount + " Dp=" + r.dataLength + ']',
                   (r.chunkCount - 1) * (long) L < r.dataLength);
        assertTrue(ctx + " Dp <= C*L  [C=" + r.chunkCount + " Dp=" + r.dataLength + ']',
                   r.dataLength <= r.chunkCount * (long) L);

        // the child's live span is exactly the parent's
        assertEquals(ctx + " Dp - dead == hi - lo", hi - lo, r.dataLength - r.deadPrefixBytes);

        // the standalone helpers must agree with the aggregate
        assertEquals(ctx + " firstChunk()", r.firstChunk, ZeroCopySSTableSplitter.firstChunk(lo, L));
        assertEquals(ctx + " lastChunk()", r.lastChunk, ZeroCopySSTableSplitter.lastChunk(hi, L));
        assertEquals(ctx + " childDataLength()", r.dataLength,
                     ZeroCopySSTableSplitter.childDataLength(hi, r.firstChunk, L));
        assertEquals(ctx + " deadPrefixBytes()", r.deadPrefixBytes,
                     ZeroCopySSTableSplitter.deadPrefixBytes(lo, L));
        assertEquals(ctx + " chunkIndexFor(lo)", r.firstChunk, ZeroCopySSTableSplitter.chunkIndexFor(lo, L));
        assertEquals(ctx + " chunkIndexFor(hi-1)", r.lastChunk,
                     ZeroCopySSTableSplitter.chunkIndexFor(hi - 1, L));

        // and the value is reproducible
        assertEquals(ctx + " reproducible", r, ZeroCopySSTableSplitter.chunkRange(lo, hi, L));
    }

    private static void assertArrayEqualsInt(int[] expected, int[] actual)
    {
        assertEquals("length " + java.util.Arrays.toString(actual), expected.length, actual.length);
        for (int i = 0; i < expected.length; i++)
            assertEquals("index " + i + " of " + java.util.Arrays.toString(actual), expected[i], actual[i]);
    }

    /** Uniform in [0, bound). {@code Math.floorMod} keeps it non-negative even for {@code Long.MIN_VALUE}. */
    private static long nextLong(Random rnd, long bound)
    {
        return Math.floorMod(rnd.nextLong(), bound);
    }
}
