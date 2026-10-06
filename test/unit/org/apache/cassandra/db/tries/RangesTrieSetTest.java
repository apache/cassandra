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

package org.apache.cassandra.db.tries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.FORWARD_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.EMPTY;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RangesTrieSetTest
{
    @Parameterized.Parameter(0)
    public Boolean endsInclusive;

    @Parameterized.Parameter(1)
    public Boolean negated;

    @Parameterized.Parameters(name = "endInclusive {0} negated {1}")
    public static List<Object[]> data()
    {
        return Arrays.asList(new Object[][] {
            { true, false },
            { false, false },
            { true, true },
            { false, true }
        });
    }

    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static RangeTrie<TrieSetCursor.RangeState> fullTrie(TrieSet s)
    {
        return new RangeTrie<>()
        {
            @Override
            public RangeCursor<TrieSetCursor.RangeState> makeCursor(Direction direction)
            {
                throw new AssertionError();
            }

            // Override cursor to disable verification which does not like the content this returns.
            // The source is already verified.
            @Override
            public RangeCursor<TrieSetCursor.RangeState> cursor(Direction direction)
            {
                return new RangeCursor<>()
                {
                    private final TrieSetCursor cursor = s.cursor(direction);

                    public TrieSetCursor.RangeState content()
                    {
                        return cursor.state();
                    }

                    @Override
                    public TrieSetCursor.RangeState state()
                    {
                        return cursor.state();
                    }

                    public long encodedPosition()
                    {
                        return cursor.encodedPosition();
                    }

                    @Override
                    public long advance()
                    {
                        return cursor.advance();
                    }

                    @Override
                    public long skipTo(long encodedSkipPosition)
                    {
                        return cursor.skipTo(encodedSkipPosition);
                    }

                    @Override
                    public RangeCursor<TrieSetCursor.RangeState> tailCursor(Direction dir)
                    {
                        throw new AssertionError();
                    }

                    @Override
                    public ByteComparable.Version byteComparableVersion()
                    {
                        return VERSION;
                    }
                };
            }
        };
    }

    static void dumpToOut(TrieSet s)
    {
        TrieUtil.dumpToOut(fullTrie(s));
    }

    void check(String... boundariesAsStrings)
    {
        if (!negated)
            check(endsInclusive, boundariesAsStrings);
        else
            checkNegated(endsInclusive, boundariesAsStrings);
    }

    TrieSet ranges(boolean endsInclusive, ByteComparable[] boundaries)
    {
        return dir -> RangesCursor.create(dir, VERSION, true, endsInclusive, boundaries);
    }

    void check(boolean endsInclusive, String... boundariesAsStrings)
    {
        Preencoded[] boundaries = new Preencoded[boundariesAsStrings.length];
        for (int i = 0; i < boundariesAsStrings.length; ++i)
            boundaries[i] = boundariesAsStrings[i] != null ? TrieUtil.directComparable(boundariesAsStrings[i]) : null;

        System.out.println("Boundaries: " + Arrays.stream(boundaries).map(x -> x != null ? x.byteComparableAsString(VERSION) : null).collect(Collectors.toList()));
        if (!boundariesValid(endsInclusive, false, boundaries))
        {
            System.out.println("Skipping endsInclusive " + endsInclusive + " because boundaries do not make sense for it");
            return;
        }

        TrieSet set = ranges(endsInclusive, boundaries);
        check(endsInclusive, false, boundaries, set);
        verifyTails(endsInclusive, false, boundaries, set);
    }

    void checkNegated(boolean endsInclusive, String... boundariesAsStrings)
    {
        Preencoded[] boundaries = new Preencoded[boundariesAsStrings.length];
        for (int i = 0; i < boundariesAsStrings.length; ++i)
            boundaries[i] = boundariesAsStrings[i] != null ? TrieUtil.directComparable(boundariesAsStrings[i]) : null;

        TrieSet set = ranges(endsInclusive, boundaries);

        TrieSet negatedSet = set.negation();
        Preencoded[] negatedBoundaries = getNegatedBoundaries(boundaries, Preencoded[]::new);
        System.out.println("Negated boundaries: " + Arrays.stream(negatedBoundaries).map(x -> x != null ? x.byteComparableAsString(VERSION) : null).collect(Collectors.toList()));
        if (!boundariesValid(false, endsInclusive, negatedBoundaries))
        {
            System.out.println("Skipping negated for endsInclusive " + endsInclusive + " because boundaries do not make sense for it");
            return;
        }

        System.out.println("Negated set");
        check(false, endsInclusive, negatedBoundaries, negatedSet);
        verifyTails(false, endsInclusive, negatedBoundaries, negatedSet);
    }

    private boolean boundariesValid(boolean endsInclusive, boolean startsExclusive, ByteComparable[] boundaries)
    {
        ByteComparable[] processedBoundaries = new ByteComparable[boundaries.length];
        for (int i = 0; i < processedBoundaries.length; ++i)
        {
            if (boundaries[i] == null)
            {
                assert i == 0 || i == processedBoundaries.length - 1;
                continue;
            }
            if (i % 2 == 0)
                processedBoundaries[i] = append(boundaries[i], (startsExclusive ? 255 : 0));
            else
                processedBoundaries[i] = append(boundaries[i], (endsInclusive ? 255 : 0));
        }
        ByteComparable prev = null;
        for (ByteComparable v : processedBoundaries)
        {
            if (prev != null && v != null && ByteComparable.compare(prev, v, VERSION) > 0)
                return false;
            prev = v;
        }
        return true;
    }

    private static ByteComparable append(ByteComparable bc, int lastByte)
    {
        return dir -> ByteSource.append(bc.asComparableBytes(VERSION), lastByte);
    }

    private static <T> T[] getNegatedBoundaries(T[] boundaries, Function<Integer, T[]> createArray)
    {
        // If the first entry is not null, drop it; otherwise add a null.
        int addLeft = boundaries.length == 0 || boundaries[0] != null ? +1
                                                                      : -1;
        // If the last entry is not null, drop it; otherwise add a null. If the length is odd, don't adjust anything
        // as the length will now become even.
        int addRight = boundaries.length == 0
                       ? +1
                       : boundaries.length % 2 != 0 ? 0
                                                    : boundaries[boundaries.length - 1] != null ? +1
                                                                                                : -1;

        // Add/remove nulls on both sides of the boundaries
        T[] negatedBoundaries = createArray.apply(boundaries.length + addLeft + addRight);
        for (int i = Math.max(-addLeft, 0); i < boundaries.length + Math.min(addRight, 0); ++i)
            negatedBoundaries[i + addLeft] = boundaries[i];
        return negatedBoundaries;
    }

    private static TrieSet tailTrie(TrieSet set, ByteComparable prefix, Direction direction)
    {
        TrieSetCursor c = set.cursor(direction);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return dir -> c.tailCursor(dir);
        else if (c.precedingIncluded())
            return TrieSet.full(c.byteComparableVersion());
        else
            return TrieSet.empty(c.byteComparableVersion());
    }

    private static boolean startsWith(ByteComparable b, ByteComparable prefix)
    {
        ByteSource sb = b.asComparableBytes(VERSION);
        ByteSource pb = prefix.asComparableBytes(VERSION);
        int next = pb.next();
        while (next != ByteSource.END_OF_STREAM)
        {
            if (sb.next() != next)
                return false;
            next = pb.next();
        }
        return true;
    }

    private static void verifyTails(boolean endsInclusive, boolean startsExclusive, Preencoded[] boundaries, TrieSet set)
    {
        Set<Preencoded> prefixes = new TreeSet<>(FORWARD_COMPARATOR);
        for (ByteComparable b : boundaries)
        {
            if (b == null)
                continue;
            for (int i = 0; i <= ByteComparable.length(b, VERSION); ++i)
                prefixes.add(ByteComparable.cut(b, i).preencode(VERSION));
        }

        for (ByteComparable prefix : prefixes)
        {
            List<Preencoded> tails = null;
            int prefixLength = ByteComparable.length(prefix, VERSION);
            for (int i = 0; i < boundaries.length; ++i)
            {
                ByteComparable b = boundaries[i];
                if (b == null || !startsWith(b, prefix))
                    continue;
                if (tails == null)
                {
                    tails = new ArrayList<>();
                    if ((i & 1) != 0)
                        tails.add(null);
                }

                final byte[] byteComparableArray = b.asByteComparableArray(VERSION);
                tails.add(ByteComparable.preencoded(VERSION, Arrays.copyOfRange(byteComparableArray, prefixLength, byteComparableArray.length)));
            }

            for (Direction dir : Direction.values())
            {
                System.out.println("Tail for " + prefix.byteComparableAsString(VERSION) + " " + dir);
                System.out.println("  tail bounds " + tails.stream().map(x -> x == null ? "null" : x.byteComparableAsString(VERSION)).collect(Collectors.toList()));
                TrieSet tail = tailTrie(set, prefix, dir);
                check(endsInclusive, startsExclusive, tails.toArray(ByteComparable[]::new), tail);
            }
        }
    }

    static void check(boolean endsInclusive, boolean startsExclusive, ByteComparable[] boundaries, TrieSet s)
    {
        dumpToOut(s);
        var expectations = getExpectations(endsInclusive, startsExclusive, boundaries);
        assertTrieEquals(expectations, s);
    }

    private static void assertTrieEquals(NavigableMap<Preencoded, PointState> expectations, TrieSet s)
    {
        BaseTrie<TrieSetCursor.RangeState, ?, ?> trie = fullTrie(s);
        TrieUtil.assertMapEquals(TrieUtil.toStringMap(trie, Direction.FORWARD),
                                 TrieUtil.toStringMap(expectations, PointState::forwardSide));
        TrieUtil.assertMapEquals(TrieUtil.toStringMap(trie, Direction.REVERSE),
                                 TrieUtil.toStringMap(TrieUtil.reorderBy(expectations, TrieUtil.REVERSE_COMPARATOR), PointState::reverseSide));
    }

    static class PointState
    {
        int firstIndex = Integer.MAX_VALUE;
        int lastIndex = Integer.MIN_VALUE;
        boolean firstExact = false;
        boolean lastExact = false;
        boolean firstIsAfter = true;
        boolean lastIsAfter = false;

        void addIndex(int index, boolean exact, boolean pointIsAfter)
        {
            if (index < firstIndex)
            {
                firstIndex = index;
                firstExact = exact;
                firstIsAfter = !exact || pointIsAfter;
            }
            else if (index == firstIndex)
            {
                firstExact |= exact;
                firstIsAfter &= !exact || pointIsAfter;
            }

            if (index > lastIndex)
            {
                lastIndex = index;
                lastExact = exact;
                lastIsAfter = exact && pointIsAfter;
            }
            else if (index == lastIndex)
            {
                lastExact |= exact;
                lastIsAfter |= exact && pointIsAfter;
            }
        }

        static PointState empty()
        {
            PointState state = new PointState();
            state.firstIndex = 0;
            state.lastIndex = 1;
            return state;
        }

        public static Object forwardSide(PointState pointState)
        {
            boolean applicableBefore = (pointState.firstIndex & 1) == 1;
            TrieSetCursor.RangeState b1 = null;
            TrieSetCursor.RangeState b2 = null;
            // choose to report b1 based on diff between first and last
            if (pointState.firstExact)
            {
                if (pointState.firstIsAfter)
                    b2 = TrieSetCursor.RangeState.fromProperties(applicableBefore, !applicableBefore);
                else
                    b1 = TrieSetCursor.RangeState.fromProperties(applicableBefore, !applicableBefore);
            }
            else if (pointState.lastIndex > pointState.firstIndex)
                b1 = TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);

            if (pointState.lastExact)
            {
                boolean applicableAfter = (pointState.lastIndex & 1) == 1;
                if (pointState.lastIsAfter)
                    b2 = combineFwd(b2, TrieSetCursor.RangeState.fromProperties(applicableAfter, !applicableAfter));
                else
                    b1 = combineFwd(b1, TrieSetCursor.RangeState.fromProperties(applicableAfter, !applicableAfter));
            }

            if (b1 == null && b2 == null)
                return TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);
            if (b1 != null && b2 != null)
                return Pair.create(b1, b2);
            if (b1 != null)
                return b1;
            return b2;
        }

        static TrieSetCursor.RangeState combineFwd(TrieSetCursor.RangeState b1, TrieSetCursor.RangeState b2)
        {
            if (b1 == null)
                return b2;
            return TrieSetCursor.RangeState.fromProperties(b1.applicableBefore, b2.applicableAfter);
        }

        public static Object reverseSide(PointState pointState)
        {
            boolean applicableBefore = (pointState.lastIndex & 1) != 1;
            TrieSetCursor.RangeState b1 = null;
            TrieSetCursor.RangeState b2 = null;
            if (pointState.lastExact)
            {
                if (pointState.lastIsAfter)
                    b1 = TrieSetCursor.RangeState.fromProperties(!applicableBefore, applicableBefore);
                else
                    b2 = TrieSetCursor.RangeState.fromProperties(!applicableBefore, applicableBefore);
            }
            else if (pointState.lastIndex > pointState.firstIndex)
                b1 = TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);

            if (pointState.firstExact)
            {
                boolean applicableAfter = (pointState.firstIndex & 1) != 1;
                if (pointState.firstIsAfter)
                    b1 = combineRev(b1, TrieSetCursor.RangeState.fromProperties(!applicableAfter, applicableAfter));
                else
                    b2 = combineRev(b2, TrieSetCursor.RangeState.fromProperties(!applicableAfter, applicableAfter));
            }

            if (b1 == null && b2 == null)
                return TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);
            if (b1 != null && b2 != null)
                return Pair.create(b1, b2);
            if (b1 != null)
                return b1;
            return b2;
        }
    }

    static TrieSetCursor.RangeState combineRev(TrieSetCursor.RangeState b1, TrieSetCursor.RangeState b2)
    {
        if (b1 == null)
            return b2;
        return TrieSetCursor.RangeState.fromProperties(b2.applicableBefore, b1.applicableAfter);
    }


    static NavigableMap<Preencoded, PointState> getExpectations(boolean endsInclusive, boolean startsExclusive, ByteComparable... boundaries)
    {
        // Leading [null, EMPTY ...] sequence is nonsensical if endsInclusive is not true and causes us trouble.
        if (!endsInclusive &&
            boundaries.length >= 2 &&
            boundaries[0] == null &&
            boundaries[1] != null &&
            ByteComparable.compare(EMPTY, boundaries[1], VERSION) == 0)
        {
            boundaries = Arrays.copyOfRange(boundaries, 2, boundaries.length);
        }
        int l = (boundaries.length + 1) & ~1;
        // Same for trailing [... EMPTY, null] when startsExclusive is true
        if (startsExclusive &&
            boundaries.length >= 2 &&
            (boundaries.length <= l - 1 || boundaries[l - 1] == null) &&
            boundaries[l - 2] != null &&
            ByteComparable.compare(EMPTY, boundaries[l - 2], VERSION) == 0)
        {
            l -= 2;
            boundaries = Arrays.copyOfRange(boundaries, 0, l);
        }

        var expectations = new TreeMap<Preencoded, PointState>(FORWARD_COMPARATOR);
        for (int bi = 0; bi < l; ++bi)
        {
            boolean pointIsAfter = bi % 2 == 0 ? startsExclusive : endsInclusive;
            ByteComparable b = bi < boundaries.length ? boundaries[bi] : null;
            if (b == null)
            {
                b = ByteComparable.EMPTY;
                pointIsAfter = bi % 2 == 1; // always inclusive left exclusive right
            }
            int len = ByteComparable.length(b, VERSION);
            for (int i = 0; i <= len; ++i)
            {
                Preencoded v = ByteComparable.cut(b, i).preencode(VERSION);
                PointState state = expectations.computeIfAbsent(v, k -> new PointState());
                state.addIndex(bi, i == len, pointIsAfter);
            }
        }
        if (expectations.isEmpty())
            expectations.put(ByteComparable.EMPTY.preencode(VERSION), PointState.empty());
        return expectations;
    }

    @Test
    public void testEmptyInterval()
    {
        check();
    }

    @Test
    public void testFullInterval()
    {
        check(null, null);
    }

    @Test
    public void testOneNull()
    {
        check((String) null);
    }

    @Test
    public void testLeftNull()
    {
        check(null, "afg");
    }

    @Test
    public void testRightNull()
    {
        check("abc", null);
    }

    @Test
    public void testSpan()
    {
        check("abc", "afg");
    }

    @Test
    public void testPoint()
    {
        check("abc", "abc");
    }

    @Test
    public void testDual()
    {
        check("abc", "afg", "aga", "ajb");
    }

    @Test
    public void testHole()
    {
        check(null, "abc", "afg", null);
    }

    // prefixes

    @Test
    public void testPrefixLeft()
    {
        check(" a", " abc");
    }

    @Test
    public void testPrefixRight()
    {
        check(" abc", " a");
    }

    @Test
    public void testPrefixHole()
    {
        check(" a", " aaa", " acc", " a");
    }

    @Test
    public void testPrefixLeftHole()
    {
        check(" a", " aaa", " acc", " d");
    }

    @Test
    public void testPrefixRightHole()
    {
        check(" a", " daa", " dcc", " d");
    }


    // Repeats aren't valid, because they doubly list a branch

    @Test
    public void testRepeatLeft()
    {
        check("abc", "abc", "abc", null);
    }

    @Test
    public void testRepeatRight()
    {
        check(null, "abc", "abc", "abc");
    }

    @Test
    public void testPointRepeat()
    {
        check("abc", "abc", "abc", "abc");
    }

    @Test
    public void testPointInSpan()
    {
        check("aa", "abc", "abc", "ad");
    }

    @Test
    public void testPrefixRepeatsInSpanOdd()
    {
        check("aaa", "abc", "abe", "aff");
    }

    @Test
    public void testPrefixRepeatsInSpanEven()
    {
        check("abc", "abe", "aff");
    }

    @Test
    public void testBothEmpty()
    {
        check("", "");
    }

    @Test
    public void testLeftEmpty()
    {
        check("", null);
    }

    @Test
    public void testOneEmpty()
    {
        check("");
    }

    @Test
    public void testRightEmpty()
    {
        check(null, "");
    }

    @Test
    public void testLong()
    {
        check("aaa", "aab", "aba", "aca", "acb", "ada", "adba", "adba", "baa", "bba", "bbb", "bbc", "bcc", "bcd");
    }

    @Test
    public void testRangeStateFromProperties()
    {
        for (boolean applicableBefore : List.of(false, true))
            for (boolean applicableAfter : List.of(false, true))
                {
                    TrieSetCursor.RangeState state = TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableAfter);
                    assertEquals(applicableBefore, state.applicableBefore);
                    assertEquals(applicableAfter, state.applicableAfter);
                }
    }
}
