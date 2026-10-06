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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.TestRangeState.fromList;
import static org.apache.cassandra.db.tries.TestRangeState.toList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RangeTrieIntersectionTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static final int bitsNeeded = 4;


    @Parameterized.Parameters(name = "bits per transition {0}")
    public static List<Object> data()
    {
        return IntStream.rangeClosed(1, bitsNeeded)
                        .boxed()
                        .collect(Collectors.toList());
    }

    @Parameterized.Parameter(0)
    public int bits = bitsNeeded;

    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
    private ByteComparable of(int value)
    {
        assert value >= 0 && value <= Byte.MAX_VALUE;

        byte[] splitBytes = new byte[(bitsNeeded + bits - 1) / bits];
        int pos = 0;
        int mask = (1 << bits) - 1;
        for (int i = bitsNeeded - bits; i > 0; i -= bits)
            splitBytes[pos++] = (byte) ((value >> i) & mask);

        splitBytes[pos] = (byte) (value & mask);
        return ByteComparable.preencoded(TrieUtil.VERSION, splitBytes);
    }

    private TestRangeState from(int where, int value)
    {
        return new TestRangeState(of(where), -1, value);
    }

    private TestRangeState to(int where, int value)
    {
        return new TestRangeState(of(where), value, -1);
    }

    private TestRangeState change(int where, int from, int to)
    {
        return new TestRangeState(of(where), from, to);
    }

    private TrieSet range(ByteComparable left, ByteComparable right)
    {
        return TrieSet.rangeInclusiveEnd(TrieUtil.VERSION, left, right);
    }

    private TrieSet ranges(ByteComparable... bounds)
    {
        return TrieSet.ranges(TrieUtil.VERSION, true, true, bounds);
    }

    @Test
    public void testSubtrie()
    {
        {
            RangeTrie<TestRangeState> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)));

            TrieUtil.dumpToOut(trie);

            assertEquals("No intersection", asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)), toList(trie, Direction.FORWARD));

            testIntersection("all",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             range(null, null));
            testIntersection("fully covered range",
                             asList(from(1, 10), to(4, 10)),
                             trie,
                             range(of(0), of(5)));
            testIntersection("fully covered range",
                             asList(from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             range(of(5), of(13)));
            testIntersection("matching range",
                             asList(from(1, 10), to(4, 10)),
                             trie,
                             range(of(1), of(4)));
            testIntersection("touching",
                             asList(from(4, 10), to(4, 10), from(6, 11), to(6, 11)),
                             trie,
                             range(of(4), of(6)));

            testIntersection("partial left",
                             asList(from(2, 10), to(4, 10)),
                             trie,
                             range(of(2), of(5)));
            testIntersection("partial left on change",
                             asList(from(8, 12), to(10, 12)),
                             trie,
                             range(of(8), of(12)));
            testIntersection("partial left with null",
                             asList(from(9, 12), to(10, 12)),
                             trie,
                             range(of(9), null));


            testIntersection("partial right",
                             asList(from(6, 11), to(7, 11)),
                             trie,
                             range(of(5), of(7)));
            testIntersection("partial right on change",
                             asList(from(6, 11), change(8, 11, 12), to(8, 12)),
                             trie,
                             range(of(5), of(8)));
            testIntersection("partial right with null",
                             asList(from(1, 10), to(2, 10)),
                             trie,
                             range(null, of(2)));

            testIntersection("inside range",
                             asList(from(2, 10), to(3, 10)),
                             trie,
                             range(of(2), of(3)));
            testIntersection("inside with change",
                             asList(from(7, 11), change(8, 11, 12), to(9, 12)),
                             trie,
                             range(of(7), of(9)));

            testIntersection("point inside",
                             asList(from(7, 11), to(7, 11)),
                             trie,
                             range(of(7), of(7)));
        }
    }

    @Test
    public void testRanges()
    {
        {
            RangeTrie<TestRangeState> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)));

            testIntersection("fully covered ranges",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             ranges(of(0), of(13)));
            testIntersection("matching ranges",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             ranges(of(1), of(4), of(6), of(11)));
            testIntersection("touching",
                             asList(from(1, 10), to(1, 10), from(4, 10), to(4, 10), from(6, 11), to(6, 11)),
                             trie,
                             ranges(of(0), of(1), of(4), of(6), of(12), of(15)));
            testIntersection("partial left",
                             asList(from(2, 10), to(4, 10), from(9, 12), to(10, 12)),
                             trie,
                             ranges(of(2), of(5), of(9), null));

            testIntersection("partial right",
                             asList(from(1, 10), to(2, 10), from(6, 11), to(7, 11)),
                             trie,
                             ranges(null, of(2), of(5), of(7)));

            testIntersection("inside ranges",
                             asList(from(2, 10), to(3, 10), from(7, 11), change(8, 11, 12), to(9, 12)),
                             trie,
                             ranges(of(2), of(3), of(7), of(9)));

            testIntersection("jumping inside",
                             asList(from(1, 10), to(2, 10), from(3, 10), to(4, 10), from(6, 11), to(6, 11), from(7, 11), change(8, 11, 12), to(8, 12), from(9, 12), to(10, 12)),
                             trie,
                             ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        {
            RangeTrie<TestRangeState> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)));

            // non-overlapping
            testIntersection("", asList(), trie, range(of(0), of(3)), range(of(4), of(7)));
            // touching
            testIntersection("", asList(from(3, 10), to(3, 10)), trie, range(of(0), of(3)), range(of(3), of(7)));
            // overlapping 1
            testIntersection("", asList(from(2, 10), to(3, 10)), trie, range(of(0), of(3)), range(of(2), of(7)));
            // overlapping 2
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, range(of(0), of(3)), range(of(1), of(7)));
            // covered
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, range(of(0), of(3)), range(of(0), of(7)));
            // covered
            testIntersection("", asList(from(3, 10), to(4, 10), from(6, 11), to(7, 11)), trie, range(of(3), of(7)), range(of(0), of(7)));
            // covered 2
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, range(of(1), of(3)), range(of(0), of(7)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        testIntersections(fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13))));
    }

    private void testIntersections(RangeTrie<TestRangeState> trie)
    {
        System.out.println(trie.dump());
        testIntersection("", asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)), trie);

        TrieSet set1 = ranges(null, of(4), of(5), of(9), of(12), null);
        TrieSet set2 = ranges(of(2), of(7), of(8), of(10), of(12), of(14));
        TrieSet set3 = ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));

        testIntersections(trie, set1, set2, set3);

        testSetAlgebraIntersection(trie);
    }

    private void testSetAlgebraIntersection(RangeTrie<TestRangeState> trie)
    {
        TrieSet set1 = range(null, of(3))
                              .union(range(of(2), of(4)))
                              .union(range(of(5), of(7)))
                              .union(range(of(7), of(9)))
                              .union(range(of(14), of(15)))
                              .union(range(of(12), null));
        TrieSet set2 = range(of(2), of(7))
                              .union(ranges(null, of(8), of(10), null).negation())
                              .union(ranges(of(8), of(10), of(12), of(14)));
        TrieSet set3 = range(of(1), of(2))
                              .union(range(of(3), of(4)))
                              .union(range(of(5), of(6)))
                              .union(range(of(7), of(8)))
                              .union(range(of(9), of(10)));
//        System.out.println("Set 0:\n" + set1.dump());
//        System.out.println("Set 1:\n" + set2.dump());
//        System.out.println("Set 2:\n" + set3.dump());

        testIntersections(trie, set1, set2, set3);
    }

    private void testIntersections(RangeTrie<TestRangeState> trie, TrieSet set1, TrieSet set2, TrieSet set3)
    {
        // set1 = ranges(-4, 5-9, 12-);
        // set2 = ranges(2-7, 8-10, 12-14);
        // set3 = ranges(1-2, 3-4, 5-6, 7-8, 9-10);
        testIntersection("1", asList(from(1, 10), to(4, 10),
                                     from(6, 11), change(8, 11, 12), to(9, 12),
                                     from(13, 13), to(14,13)), trie, set1);

        testIntersection("2", asList(from(2, 10), to(4, 10),
                                     from(6, 11), to(7, 11),
                                     from(8, 12), to(10, 12),
                                     from(13, 13), to(14, 13)), trie, set2);

        testIntersection("3", asList(from(1, 10), to(2, 10),
                                     from(3, 10), to(4, 10),
                                     from(6, 11), to(6, 11),
                                     from(7, 11), change(8, 11, 12), to(8, 12),
                                     from(9, 12), to(10, 12)), trie, set3);

        testIntersection("12", asList(from(2, 10), to(4, 10),
                                      from(6, 11), to(7, 11),
                                      from(8, 12), to(9, 12),
                                      from(13, 13), to(14, 13)), trie, set1, set2);

        testIntersection("13", asList(from(1, 10), to(2, 10),
                                      from(3, 10), to(4, 10),
                                      from(6, 11), to(6, 11),
                                      from(7, 11), change(8, 11, 12), to(8, 12),
                                      from(9, 12), to(9, 12)), trie, set1, set3);

        testIntersection("23", asList(from(2, 10), to(2, 10),
                                      from(3, 10), to(4, 10),
                                      from(6, 11), to(6, 11), from(7, 11), to(7, 11), from(8, 12), to(8, 12),
                                      from(9, 12), to(10, 12)), trie, set2, set3);

        testIntersection("123", asList(from(2, 10), to(2, 10),
                                       from(3, 10), to(4, 10),
                                       from(6, 11), to(6, 11), from(7, 11), to(7, 11),
                                       from(8, 12), to(8, 12), from(9, 12), to(9, 12)), trie, set1, set2, set3);
    }

    public void testIntersection(String message, List<TestRangeState> expected, RangeTrie<TestRangeState> trie, TrieSet... sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            try
            {
                assertEquals(message + " forward b" + bits, expected, toList(trie, Direction.FORWARD));
                assertEquals(message + " reverse b" + bits, Lists.reverse(expected), toList(trie, Direction.REVERSE));
            }
            catch (AssertionError e)
            {
                System.out.println("\nFORWARD:\n" + trie.dump(TestRangeState::toStringNoPosition));
                System.out.println("\nREVERSE:\n" + trie.cursor(Direction.REVERSE).process(new TrieDumper.Plain<>(TestRangeState::toStringNoPosition)));
                throw e;
            }
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersection(message + " " + toRemove, expected,
                                 trie.intersect(set),
                                 Arrays.stream(sets)
                                       .filter(x -> x != set)
                                       .toArray(TrieSet[]::new)
                );
            }
        }
    }

    @Test
    public void testRangeMethod()
    {
        ByteComparable left = TrieUtil.directComparable("aa");
        ByteComparable right = TrieUtil.directComparable("bb");
        RangeTrie<TestRangeState> trie = RangeTrie.range(left, true, right, true, TrieUtil.VERSION, new TestRangeState(ByteComparable.EMPTY, 1, 1));
        RangeTrie<TestRangeState> expected = TrieUtil.directRangeTrie("aa", "bb");
        TrieUtil.verifyEqualRangeTries(trie, expected);
    }

    @Test
    public void testSkipToSimple()
    {
        String[] ranges1 = {"aaa", "ddd"};
        String[] ranges2 = {"bbb", "eee"};
        String[] ixranges = {"bbb", "ddd"};
        String[] points = {"___", "aaa", "bbb", "ccc", "ddd", "eee"};
        testIntersectionSkipTo(ranges1, ranges2, ixranges, points);
    }

    @Test
    public void testRangeUnderCoveredBranchPoint()
    {
        String[] ranges1 = {"ba", "bc"};
        String[] ranges2 = {"aa", "ab", "bbc", "bbd", "bbfff", "bbffg", "bde", "bdf", "ce", "cf"};
        String[] expected2 = {"bbc", "bbd", "bbfff", "bbffg"};
        String[] ranges3 = {"bbfe", "bbfg"};
        String[] expected3 = {"bbfff", "bbffg"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testRangeUnderCoveredBranchRight()
    {
        String[] ranges1 = {"_a", "_b", "abba", "abf", "d", "e"};
        String[] ranges2 = {"aaa", "aab", "abc", "abd", "abef", "abeg", "abehhh", "abehhi", "ce", "cf"};
        String[] expected2 = {"abc", "abd", "abef", "abeg", "abehhh", "abehhi"};
        String[] ranges3 = {"abehg", "abehi"};
        String[] expected3 = {"abehhh", "abehhi"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testRangeUnderCoveredBranchLeft()
    {
        String[] ranges1 = {"_a", "_b", "abb_", "abe", "d", "e"};
        String[] ranges2 = {"aaa", "aab", "abbac", "abbad", "abbafff", "abbaffg", "abc", "abd", "ce", "cf"};
        String[] expected2 = {"abbac", "abbad", "abbafff", "abbaffg", "abc", "abd"};
        String[] ranges3 = {"abbafe", "abbafg"};
        String[] expected3 = {"abbafff", "abbaffg"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    private void testDirectIntersections(String[] ranges1, String[] ranges2, String[] expected2, String[] ranges3, String[] expected3)
    {
        testDirectIntersectionsRangeSet(ranges1, ranges2, expected2, ranges3, expected3);
        testDirectIntersectionsRangeSet(ranges2, ranges1, expected2, ranges3, expected3);
    }

    private void testDirectIntersectionsRangeSet(String[] ranges1, String[] ranges2, String[] expected2, String[] ranges3, String[] expected3)
    {
        RangeTrie<TestRangeState> set1 = TrieUtil.directRangeTrie(ranges1);
        TrieSet set2 = TrieUtil.directRanges(ranges2);
        RangeTrie<TestRangeState> expected = TrieUtil.directRangeTrie(expected2);
        TrieUtil.verifyEqualRangeTries(set1.intersect(set2), expected);
        String[] allpoints = Stream.of(ranges1, ranges2, expected2, ranges3, expected3)
                                   .flatMap(Arrays::stream)
                                   .distinct()
                                   .toArray(String[]::new);
        verifyIntersectionContainsCorrectness(allpoints, set1, set2);
        // check skipTo in a covered branch
        TrieSet set3 = TrieUtil.directRanges(ranges3);
        expected = TrieUtil.directRangeTrie(expected3);
        TrieUtil.verifyEqualRangeTries(set1.intersect(set2).intersect(set3), expected);
        TrieUtil.verifyEqualRangeTries(set1.intersect(set3.intersection(set2)), expected);
        verifyIntersectionContainsCorrectness(allpoints, set1.intersect(set2), set3);
        verifyIntersectionContainsCorrectness(allpoints, set1, set3.intersection(set2));
    }

    private void testIntersectionSkipTo(String[] ranges1, String[] ranges2, String[] ixranges, String[] points)
    {
        testIntersectionSkipToRangeSet(ranges1, ranges2, ixranges, points);
        testIntersectionSkipToRangeSet(ranges2, ranges1, ixranges, points);
    }

    private void testIntersectionSkipToRangeSet(String[] ranges1, String[] ranges2, String[] ixranges, String[] points)
    {
        RangeTrie<TestRangeState> set1 = TrieUtil.directRangeTrie(ranges1);
        TrieSet set2 = TrieUtil.directRanges(ranges2);
        RangeTrie<TestRangeState> ix = TrieUtil.directRangeTrie(ixranges);
        TrieUtil.verifyEqualRangeTries(set1.intersect(set2), ix);

        verifyIntersectionContainsCorrectness(points, set1, set2);

        for (int i = 1; i < 1 << points.length; i++) // at least one set bit
        {
            String[] ranges = new String[Integer.bitCount(i) * 2];
            int p = 0;
            for (int j = 0; j < points.length; j++)
            {
                if ((i & (1 << j)) != 0)
                {
                    ranges[p++] = points[j];
                    ranges[p++] = points[j];
                }
            }
            System.out.println(Arrays.toString(ranges));
            TrieSet set3 = TrieUtil.directRanges(ranges);
            RangeTrie<TestRangeState> expected = TrieUtil.directRangeTrie(Arrays.stream(ranges)
                                                                                .filter(x -> ix.applicableRange(TrieUtil.directComparable(x)) != null)
                                                                                .toArray(String[]::new));
            TrieUtil.verifyEqualRangeTries(set1.intersect(set2).intersect(set3), expected);
            TrieUtil.verifyEqualRangeTries(set1.intersect(set3.intersection(set2)), expected);
        }
    }

    private static void verifyIntersectionContainsCorrectness(String[] points, RangeTrie<TestRangeState> trie, TrieSet set)
    {
        RangeTrie<TestRangeState> ix = trie.intersect(set);
        for (String s : points)
        {
            ByteComparable bc = TrieUtil.directComparable(s);
            assertEquals(s, set.strictlyContains(bc) && trie.applicableRange(bc) != null, ix.applicableRange(bc) != null);
        }
    }
}
