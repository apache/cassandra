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

package org.apache.cassandra.utils;

import java.util.Arrays;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.utils.LongTimSort.LongComparator;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link LongTimSort}.
 * <p>
 * Verifies sorting correctness, permutation preservation, stability, range sorting,
 * and consistency with {@link Arrays#sort(long[])} across a wide variety of randomly
 * generated long arrays.
 */
public class LongTimSortPropertyTest
{
    /**
     * Meta-random size generator: each test example picks a different distribution
     * strategy for array sizes in [0, 501), giving coverage of empty, tiny, medium,
     * and large arrays with varying frequency across examples.
     */
    private static final Gen<Gen.IntGen> SIZE_DISTRO = Gens.mixedDistribution(0, 501);

    /**
     * Generates a random long array with meta-random size and a mix of value strategies:
     * sometimes all-equal, sometimes small-range, sometimes full-range longs.
     */
    private static long[] genLongArray(RandomSource rs, Gen.IntGen sizeGen)
    {
        int size = sizeGen.nextInt(rs);
        long[] a = new long[size];
        // Pick a value strategy for this array
        int strategy = rs.nextInt(0, 7);
        switch (strategy)
        {
            case 0: // all same value
                long v = rs.nextLong();
                Arrays.fill(a, v);
                break;
            case 1: // small range [-10, 10]
                for (int i = 0; i < size; i++)
                    a[i] = rs.nextInt(-10, 11);
                break;
            case 2: // medium range [-1000, 1000]
                for (int i = 0; i < size; i++)
                    a[i] = rs.nextInt(-1000, 1001);
                break;
            case 3: // pre-sorted ascending
                for (int i = 0; i < size; i++)
                    a[i] = i;
                break;
            case 4: // pre-sorted descending
                for (int i = 0; i < size; i++)
                    a[i] = size - i;
                break;
            case 5: // nearly sorted (sorted then perturb a few elements)
                for (int i = 0; i < size; i++)
                    a[i] = i;
                int perturbations = Math.max(1, size / 20);
                for (int p = 0; p < perturbations && size > 1; p++)
                {
                    int j = rs.nextInt(0, size);
                    int k = rs.nextInt(0, size);
                    long tmp = a[j];
                    a[j] = a[k];
                    a[k] = tmp;
                }
                break;
            case 6: // full range longs
                for (int i = 0; i < size; i++)
                    a[i] = rs.nextLong();
                break;
            default:
                throw new IllegalStateException("Unknown strategy: " + strategy);
        }
        return a;
    }

    // ---- Property 1: Sorted output ----

    @Test
    public void sortedOutput()
    {
        qt().check(rs -> {
            Gen.IntGen sizeGen = SIZE_DISTRO.next(rs);
            long[] a = genLongArray(rs, sizeGen);
            LongTimSort.sort(a, Long::compare);
            for (int i = 0; i < a.length - 1; i++)
                assertThat(a[i]).describedAs("a[%d] <= a[%d]", i, i + 1).isLessThanOrEqualTo(a[i + 1]);
        });
    }

    // ---- Property 2: Permutation (multiset preservation) ----

    @Test
    public void permutation()
    {
        qt().check(rs -> {
            Gen.IntGen sizeGen = SIZE_DISTRO.next(rs);
            long[] a = genLongArray(rs, sizeGen);
            long[] original = a.clone();
            LongTimSort.sort(a, Long::compare);
            // Both sorted arrays must be identical element-by-element
            Arrays.sort(original);
            assertThat(a).describedAs("sorted array must be a permutation of the original").isEqualTo(original);
        });
    }

    // ---- Property 3: Idempotency ----

    @Test
    public void idempotency()
    {
        qt().check(rs -> {
            Gen.IntGen sizeGen = SIZE_DISTRO.next(rs);
            long[] a = genLongArray(rs, sizeGen);
            LongTimSort.sort(a, Long::compare);
            long[] sortedOnce = a.clone();
            LongTimSort.sort(a, Long::compare);
            assertThat(a).describedAs("sorting an already-sorted array must produce the same result").isEqualTo(sortedOnce);
        });
    }

    // ---- Property 4: Stability ----

    @Test
    public void stability()
    {
        qt().check(rs -> {
            Gen.IntGen sizeGen = SIZE_DISTRO.next(rs);
            int size = sizeGen.nextInt(rs);
            // Encode (value, originalIndex) into a single long:
            // high 32 bits = value (from small range to force duplicates), low 32 bits = original index
            long[] a = new long[size];
            for (int i = 0; i < size; i++)
            {
                int value = rs.nextInt(-50, 51);
                a[i] = ((long) value << 32) | (i & 0xFFFFFFFFL);
            }

            // Compare by high 32 bits only (the value)
            LongComparator cmp = (o1, o2) -> Integer.compare((int) (o1 >> 32), (int) (o2 >> 32));
            LongTimSort.sort(a, cmp);

            // After sort, elements with equal high bits must have increasing low bits (original indices)
            for (int i = 0; i < a.length - 1; i++)
            {
                int valI = (int) (a[i] >> 32);
                int valNext = (int) (a[i + 1] >> 32);
                assertThat(valI).describedAs("sorted by value at position %d", i).isLessThanOrEqualTo(valNext);
                if (valI == valNext)
                {
                    long idxI = a[i] & 0xFFFFFFFFL;
                    long idxNext = a[i + 1] & 0xFFFFFFFFL;
                    assertThat(idxI)
                        .describedAs("stability: equal elements at positions %d and %d must preserve relative order", i, i + 1)
                        .isLessThan(idxNext);
                }
            }
        });
    }

    // ---- Property 5: Range sort ----

    @Test
    public void rangeSort()
    {
        qt().check(rs -> {
            Gen.IntGen sizeGen = SIZE_DISTRO.next(rs);
            long[] a = genLongArray(rs, sizeGen);
            if (a.length == 0) return;

            int lo = rs.nextInt(0, a.length);
            int hi = rs.nextInt(lo, a.length + 1);
            long[] original = a.clone();

            LongTimSort.sort(a, lo, hi, Long::compare);

            // Elements outside [lo, hi) must be unchanged
            for (int i = 0; i < lo; i++)
                assertThat(a[i]).describedAs("element before range at index %d must be unchanged", i).isEqualTo(original[i]);
            for (int i = hi; i < a.length; i++)
                assertThat(a[i]).describedAs("element after range at index %d must be unchanged", i).isEqualTo(original[i]);

            // Elements within [lo, hi) must be sorted
            for (int i = lo; i < hi - 1; i++)
                assertThat(a[i]).describedAs("a[%d] <= a[%d] in sorted range", i, i + 1).isLessThanOrEqualTo(a[i + 1]);

            // Elements within [lo, hi) must be a permutation of original[lo..hi)
            long[] sortedRange = Arrays.copyOfRange(a, lo, hi);
            long[] originalRange = Arrays.copyOfRange(original, lo, hi);
            Arrays.sort(originalRange);
            assertThat(sortedRange).describedAs("range must be a permutation of original range").isEqualTo(originalRange);
        });
    }

    // ---- Property 6: Consistency with Arrays.sort ----

    @Test
    public void consistencyWithArraysSort()
    {
        qt().check(rs -> {
            Gen.IntGen sizeGen = SIZE_DISTRO.next(rs);
            long[] a = genLongArray(rs, sizeGen);
            long[] expected = a.clone();
            Arrays.sort(expected);
            LongTimSort.sort(a, Long::compare);
            assertThat(a).describedAs("LongTimSort must produce the same result as Arrays.sort").isEqualTo(expected);
        });
    }

    // ---- Property 7: Reverse comparator ----

    @Test
    public void reverseComparator()
    {
        qt().check(rs -> {
            Gen.IntGen sizeGen = SIZE_DISTRO.next(rs);
            long[] a = genLongArray(rs, sizeGen);
            LongTimSort.sort(a, (x, y) -> Long.compare(y, x));
            for (int i = 0; i < a.length - 1; i++)
                assertThat(a[i]).describedAs("a[%d] >= a[%d] in descending order", i, i + 1).isGreaterThanOrEqualTo(a[i + 1]);
        });
    }
}
