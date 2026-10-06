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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.FORWARD_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertTrieEquals;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RangesTrieSetTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static Trie<TrieSetCursor.RangeState> fullTrie(TrieSet s)
    {
        return dir -> new Cursor<>()
        {
            private final TrieSetCursor cursor = s.cursor(dir);

            public TrieSetCursor.RangeState content()
            {
                return cursor.state();
            }

            public int depth()
            {
                return cursor.depth();
            }

            @Override
            public int incomingTransition()
            {
                return cursor.incomingTransition();
            }

            @Override
            public int advance()
            {
                return cursor.advance();
            }

            @Override
            public int skipTo(int skipDepth, int skipTransition)
            {
                return cursor.skipTo(skipDepth, skipTransition);
            }

            @Override
            public Cursor<TrieSetCursor.RangeState> tailCursor(Direction dir)
            {
                throw new AssertionError();
            }

            @Override
            public Direction direction()
            {
                return dir;
            }

            @Override
            public ByteComparable.Version byteComparableVersion()
            {
                return VERSION;
            }
        };
    }

    static String dump(TrieSet s, Direction direction)
    {
        return fullTrie(s).process(direction, new TrieDumper.Plain<>(Object::toString));
    }

    static void dumpToOut(TrieSet s)
    {
        System.out.println("Forward:");
        System.out.println(dump(s, Direction.FORWARD));
        System.out.println("Reverse:");
        System.out.println(dump(s, Direction.REVERSE));
    }

    void check(String... boundariesAsStrings)
    {
        Preencoded[] boundaries = new Preencoded[boundariesAsStrings.length];
        for (int i = 0; i < boundariesAsStrings.length; ++i)
            boundaries[i] = boundariesAsStrings[i] != null ? TrieUtil.comparable(boundariesAsStrings[i]) : null;
        check(boundaries);

        verifySkipTo(boundariesAsStrings, TrieSet.ranges(VERSION, boundaries));
        verifyTails(boundaries, TrieSet.ranges(VERSION, boundaries));
    }

    private static TrieSet tailTrie(TrieSet set, ByteComparable prefix, Direction direction)
    {
        TrieSetCursor c = set.cursor(direction);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return dir -> c.tailCursor(dir);
        else
            return null;
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

    private static void verifyTails(Preencoded[] boundaries, TrieSet set)
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
                TrieSet tail = tailTrie(set, prefix, dir);
                assertNotNull(tail);
                dumpToOut(tail);
                var expectations = getExpectations(tails.toArray(Preencoded[]::new));
                assertTrieEquals(fullTrie(tail), expectations);
            }
        }
    }

    private static void verifySkipTo(String[] boundariesAsStrings, TrieSet set)
    {
        String arr = Arrays.toString(boundariesAsStrings);
        // Verify that we get the right covering state for all positions around the boundaries.
        for (int bi = 0, ei = 0; bi < boundariesAsStrings.length; bi = ei)
        {
            ++ei;
            String s = boundariesAsStrings[bi];
            if (s == null)
                continue;
            while (ei < boundariesAsStrings.length && s.equals(boundariesAsStrings[ei]))
                ++ei;
            for (int terminator : Arrays.asList(ByteSource.LT_NEXT_COMPONENT, ByteSource.TERMINATOR, ByteSource.GT_NEXT_COMPONENT))
                for (Direction direction : Direction.values())
                {
                    String term = terminator == ByteSource.LT_NEXT_COMPONENT ? "<" : terminator == ByteSource.TERMINATOR ? "=" : ">";
                    String dir = direction == Direction.FORWARD ? "FWD" : "REV";
                    String msg = term + s + " " + dir + " in " + arr + " ";
                    ByteSource b = ByteSource.withTerminator(terminator, ByteSource.of(s, VERSION));
                    TrieSetCursor cursor = set.cursor(direction);
                    // skip to nearest position in cursor
                    int next = b.next();
                    int depth = 0;
                    while (next != ByteSource.END_OF_STREAM && cursor.skipTo(depth + 1, next) == depth + 1 && cursor.incomingTransition() == next)
                    {
                        next = b.next();
                        ++depth;
                    }
                    // Check the resulting state.
                    int effectiveIndexFwd = terminator <= ByteSource.TERMINATOR ? bi : ei;
                    int effectiveIndexRev = terminator >= ByteSource.TERMINATOR ? ei : bi;
                    boolean isExact = next == ByteSource.END_OF_STREAM;
                    TrieSetCursor.RangeState state = isExact ? cursor.state() : (cursor.state().precedingIncluded(direction) ? TrieSetCursor.RangeState.END_START_PREFIX : TrieSetCursor.RangeState.START_END_PREFIX);
                    assertEquals(msg + "covering FWD", (effectiveIndexFwd & 1) != 0, state.precedingIncluded(Direction.FORWARD));
                    assertEquals(msg + "covering REV", (effectiveIndexRev & 1) != 0, state.precedingIncluded(Direction.REVERSE));
                }
        }
    }

    void check(ByteComparable... boundaries)
    {
        TrieSet s = TrieSet.ranges(VERSION, boundaries);
        dumpToOut(s);
        var expectations = getExpectations(boundaries);
        assertTrieEquals(fullTrie(s), expectations);
    }

    static class PointState
    {
        int firstIndex = Integer.MAX_VALUE;
        int lastIndex = Integer.MIN_VALUE;
        boolean exact = false;

        void addIndex(int index, boolean exact)
        {
            firstIndex = Math.min(index, firstIndex);
            lastIndex = Math.max(index, lastIndex);
            this.exact |= exact;
        }

        TrieSetCursor.RangeState state()
        {
            boolean appliesBefore = (firstIndex & 1) != 0;
            boolean appliesAfter = (lastIndex & 1) == 0;
            return TrieSetCursor.RangeState.values()[(appliesBefore ? 1 : 0) | (appliesAfter ? 2 : 0) | (exact ? 4 : 0)];
        }

        static PointState fullRange()
        {
            PointState state = new PointState();
            state.firstIndex = 1;
            state.lastIndex = 2;
            state.exact = false;
            return state;
        }
    }

    static NavigableMap<Preencoded, TrieSetCursor.RangeState> getExpectations(ByteComparable... boundaries)
    {
        var expectations = new TreeMap<Preencoded, PointState>(FORWARD_COMPARATOR);
        for (int bi = 0; bi < boundaries.length; ++bi)
        {
            ByteComparable b = boundaries[bi];
            if (b == null)
                continue;
            int len = ByteComparable.length(b, VERSION);
            for (int i = 0; i <= len; ++i)
            {
                Preencoded v = ByteComparable.cut(b, i).preencode(VERSION);
                PointState state = expectations.computeIfAbsent(v, k -> new PointState());
                state.addIndex(bi, i == len);
            }
        }
        if (expectations.isEmpty())
            expectations.put(ByteComparable.preencoded(VERSION, new byte[0]), PointState.fullRange());
        return expectations.entrySet()
                           .stream()
                           .collect(() -> new TreeMap(FORWARD_COMPARATOR),
                                    (m, e) -> m.put(e.getKey(), e.getValue().state()),
                                    NavigableMap::putAll);
    }

    @Test
    public void testFullInterval()
    {
        check(new String[0]);
        check((String) null, null);
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
        check(ByteComparable.EMPTY, ByteComparable.EMPTY);
    }

    @Test
    public void testLeftEmpty()
    {
        check(ByteComparable.EMPTY, null);
    }

    @Test
    public void testRightEmpty()
    {
        check(null, ByteComparable.EMPTY);
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
                for (boolean applicableAt : List.of(false, true))
                {
                    TrieSetCursor.RangeState state = TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableAfter, applicableAt);
                    assertEquals(applicableBefore, state.applicableBefore);
                    assertEquals(applicableAfter, state.applicableAfter);
                    assertEquals(applicableAt, state.isBoundary);
                }
    }
}
