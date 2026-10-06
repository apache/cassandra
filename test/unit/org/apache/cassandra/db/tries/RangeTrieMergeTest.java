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
import java.util.Iterator;
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
import static org.apache.cassandra.db.tries.TestRangeState.verify;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class RangeTrieMergeTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static final int bitsNeeded = 6;

    @Parameterized.Parameters(name = "bits per transition {0} open-ended {1}")
    public static List<Object[]> data()
    {
        return IntStream.rangeClosed(1, bitsNeeded)
                        .boxed()
                        .flatMap(x -> Stream.of(false, true)
                                            .map(y -> new Object[] { x, y }))
                        .collect(Collectors.toList());
    }

    @Parameterized.Parameter(0)
    public int bits;

    @Parameterized.Parameter(1)
    public boolean useNulls;

    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
    private ByteComparable of(int value)
    {
        assert value >= 0 && value < 1<< bitsNeeded;

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
        return new TestRangeState(of(where), false, -1, value);
    }

    private TestRangeState fromAfter(int where, int value)
    {
        return new TestRangeState(of(where), true, -1, value);
    }

    private TestRangeState to(int where, int value)
    {
        return new TestRangeState(of(where), true, value, -1);
    }

    private TestRangeState toBefore(int where, int value)
    {
        return new TestRangeState(of(where), false, value, -1);
    }

    private TestRangeState changeBefore(int where, int from, int to)
    {
        return new TestRangeState(of(where), false, from, to);
    }

    private TestRangeState changeAfter(int where, int from, int to)
    {
        return new TestRangeState(of(where), true, from, to);
    }

    private List<TestRangeState> deletedRanges(ByteComparable... dataPoints)
    {
        List<ByteComparable> data = new ArrayList<>(asList(dataPoints));
        invertDataRangeList(data);
        filterOutEmptyRepetitions(data);

        List<TestRangeState> markers = new ArrayList<>();
        for (int i = 0; i < data.size(); ++i)
        {
            ByteComparable pos = data.get(i);
            if (pos == null)
                pos = i % 2 == 0 ? useNulls ? null : of(0) : useNulls ? null : of((1<<bitsNeeded) - 1);
            if (i % 2 == 0)
                markers.add(new TestRangeState(pos, -1, 100));
            else
                markers.add(new TestRangeState(pos, 100, -1));
        }
        return verify(markers);
    }

    private static void invertDataRangeList(List<ByteComparable> data)
    {
        // invert list
        if (data.get(0) != null)
            data.add(0, null);
        else
            data.remove(0);
        if (data.get(data.size() - 1) != null)
            data.add(null);
        else
            data.remove(data.size() - 1);
    }

    private static void filterOutEmptyRepetitions(List<ByteComparable> data)
    {
        for (int i = 0; i < data.size() - 1; ++i)
        {
            if (data.get(i) != null && data.get(i + 1) != null &&
                ByteComparable.compare(data.get(i), data.get(i + 1), TrieUtil.VERSION) == 0)
            {
                data.remove(i + 1);
                data.remove(i);
                --i;
            }
        }
    }

    @Test
    public void testSubtrie()
    {
        testMerge("no merge");

        testMerge("all",
                  deletedRanges(null, null));
        testMerge("fully covered range",
                  deletedRanges(of(20), of(25)));
        testMerge("fully covered range",
                  deletedRanges(of(25), of(33)));
        testMerge("matching range",
                  deletedRanges(of(21), of(24)));
        testMerge("touching empty",
                  deletedRanges(of(24), of(26)));

        testMerge("partial left",
                  deletedRanges(of(22), of(25)));
        testMerge("partial left on change",
                  deletedRanges(of(28), of(32)));
        testMerge("partial left with null",
                  deletedRanges(of(29), null));


        testMerge("partial right",
                  deletedRanges(of(25), of(27)));
        testMerge("partial right on change",
                  deletedRanges(of(25), of(28)));
        testMerge("partial right with null",
                  deletedRanges(null, of(22)));

        testMerge("inside range",
                  deletedRanges(of(22), of(23)));
        testMerge("inside with change",
                  deletedRanges(of(27), of(29)));

        testMerge("empty range inside",
                  deletedRanges(of(27), of(27)));

        testMerge("point covered",
                  deletedRanges(of(16), of(18)));
        testMerge("point at range start",
                  deletedRanges(of(17), of(18)));
        testMerge("point at range end",
                  deletedRanges(of(16), of(17)));


        testMerge("start point covered",
                  deletedRanges(of(32), of(35)));
        testMerge("start point at range start",
                  deletedRanges(of(33), of(35)));
        testMerge("start point at range end",
                  deletedRanges(of(32), of(33)));


        testMerge("end point covered",
                  deletedRanges(of(36), of(40)));
        testMerge("end point at range start",
                  deletedRanges(of(38), of(40)));
        testMerge("end point at range end",
                  deletedRanges(of(36), of(38)));
    }

    @Test
    public void testRanges()
    {
        {
            testMerge("fully covered ranges",
                      deletedRanges(of(20), of(25), of(25), of(33)));
            testMerge("matching ranges",
                      deletedRanges(of(21), of(24), of(26), of(31)));
            testMerge("touching empty",
                      deletedRanges(of(20), of(21), of(24), of(26), of(32), of(33), of(34), of(36)));
            testMerge("partial left",
                      deletedRanges(of(22), of(25), of(29), null));

            testMerge("partial right",
                      deletedRanges(null, of(22), of(25), of(27)));

            testMerge("inside ranges",
                      deletedRanges(of(22), of(23), of(27), of(29)));

            testMerge("jumping inside",
                      deletedRanges(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        {
            // non-overlapping
            testMerge("non-overlapping", deletedRanges(of(20), of(23)), deletedRanges(of(24), of(27)));
            // touching, i.e. still non-overlapping
            testMerge("touching", deletedRanges(of(20), of(23)), deletedRanges(of(23), of(27)));
            // overlapping 1
            testMerge("overlapping1", deletedRanges(of(20), of(23)), deletedRanges(of(22), of(27)));
            // overlapping 2
            testMerge("overlapping2", deletedRanges(of(20), of(23)), deletedRanges(of(21), of(27)));
            // covered
            testMerge("covered1", deletedRanges(of(20), of(23)), deletedRanges(of(20), of(27)));
            // covered
            testMerge("covered2", deletedRanges(of(23), of(27)), deletedRanges(of(20), of(27)));
            // covered 2
            testMerge("covered3", deletedRanges(of(21), of(23)), deletedRanges(of(20), of(27)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        testMerges();
    }

    private List<TestRangeState> getTestRanges()
    {
        return asList(fromAfter(3, 15), toBefore(5, 15),
                      from(17, 20), to(17, 20),
                      from(21, 10), changeBefore(22, 10, 21), changeAfter(22, 21, 10), to(24, 10),
                      from(26, 11), changeBefore(28, 11, 22), changeAfter(28, 22, 12), to(30, 12),
                      from(33, 23), changeAfter(33, 23, 13), to(34, 13),
                      from(36, 14), changeBefore(38, 14, 24), to(38, 24),
                      fromAfter(40, 15), toBefore(43, 15));
    }

    private void testMerges()
    {
        testMerge("", fromList(getTestRanges()), getTestRanges());

        List<TestRangeState> set1 = deletedRanges(null, of(24), of(25), of(29), of(32), null);
        List<TestRangeState> set2 = deletedRanges(of(14), of(17),
                                                  of(22), of(27),
                                                  of(28), of(30),
                                                  of(32), of(34),
                                                  of(36), of(40));
        List<TestRangeState> set3 = deletedRanges(of(17), of(18),
                                                  of(19), of(20),
                                                  of(21), of(22),
                                                  of(23), of(24),
                                                  of(25), of(26),
                                                  of(27), of(28),
                                                  of(29), of(30),
                                                  of(31), of(32),
                                                  of(33), of(34),
                                                  of(35), of(36),
                                                  of(37), of(38));

        testMerges(set1, set2, set3);
    }

    private void testMerges(List<TestRangeState> set1, List<TestRangeState> set2, List<TestRangeState> set3)
    {
        // set1 = TrieSet.ranges(null, of(24), of(25), of(29), of(32), null);
        // set2 = TrieSet.ranges(of(22), of(27), of(28), of(30), of(32), of(34));
        // set3 = TrieSet.ranges(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30));
        // from(21, 10), to(24, 10), from(26, 11), change(28, 11, 12), to(30, 12), from(33, 13), to(34, 13)
        testMerge("1", set1);

        testMerge("2", set2);

        testMerge("3", set3);

        testMerge("12", set1, set2);

        testMerge("13", set1, set3);

        testMerge("23", set2, set3);

        testMerge("123", set1, set2, set3);
    }

    @SafeVarargs
    public final void testMerge(String message, List<TestRangeState>... sets)
    {
        List<TestRangeState> testRanges = getTestRanges();
        testMerge(message, fromList(testRanges), testRanges, sets);
        testCollectionMerge(message + " collection", Lists.newArrayList(fromList(testRanges)), testRanges, sets);
        testMergeToInMemoryTrie(message + " inmem.apply", fromList(testRanges), testRanges, sets);
    }


    public void testMerge(String message, RangeTrie<TestRangeState> trie, List<TestRangeState> merged, List<TestRangeState>... sets)
    {
        System.out.println("Markers: " + merged);
        verify(merged);
        // Test that merging the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            try
            {
                assertEquals(message + " forward b" + bits, merged, toList(trie, Direction.FORWARD));
                assertEquals(message + " reverse b" + bits, Lists.reverse(merged), toList(trie, Direction.REVERSE));
                System.out.println(message + " b" + bits + " matched.");
            }
            catch (AssertionError e)
            {
                System.out.println("\n" + trie.dump());
                throw e;
            }
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                List<TestRangeState> ranges = sets[toRemove];
                System.out.println("Adding:  " + ranges);
                testMerge(message + " " + toRemove,
                          trie.mergeWith(fromList(ranges), TestRangeState::combine),
                          mergeLists(merged, ranges),
                          Arrays.stream(sets)
                                .filter(x -> x != ranges)
                                .toArray(List[]::new)
                );
            }
        }
    }

    InMemoryRangeTrie<TestRangeState> duplicateTrie(RangeTrie<TestRangeState> trie)
    {
        try
        {
            InMemoryRangeTrie<TestRangeState> dupe = InMemoryRangeTrie.shortLived(TrieUtil.VERSION);
            dupe.apply(trie, this::upsertMarkers, x -> false);
            return dupe;
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    public void testMergeToInMemoryTrie(String message, InMemoryRangeTrie<TestRangeState> trie, List<TestRangeState> merged, List<TestRangeState>... sets)
    {
        System.out.println("Markers: " + merged);
        verify(merged);
        System.out.println("Trie: \n" + trie.dump());
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            try
            {
                assertEquals(message + " forward b" + bits, merged, toList(trie, Direction.FORWARD));
                assertEquals(message + " reverse b" + bits, Lists.reverse(merged), toList(trie, Direction.REVERSE));
                System.out.println(message + " b" + bits + " matched.");
            }
            catch (AssertionError e)
            {
                System.out.println("\n" + trie.dump());
                throw e;
            }
        }
        else
        {
            try
            {
                for (int toRemove = 0; toRemove < sets.length; ++toRemove)
                {
                    List<TestRangeState> ranges = sets[toRemove];
                    System.out.println("Adding:  " + ranges);
                    InMemoryRangeTrie<TestRangeState> dupe = duplicateTrie(trie);
                    dupe.apply(fromList(ranges), this::upsertMarkers, x -> false);
                    testMergeToInMemoryTrie(message + " " + toRemove,
                                            dupe,
                                            mergeLists(merged, ranges),
                                            Arrays.stream(sets)
                                                  .filter(x -> x != ranges)
                                                  .toArray(List[]::new)
                    );
                }
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    TestRangeState upsertMarkers(TestRangeState left, TestRangeState right)
    {
        if (left == null)
            return right;
        if (right == null)
            return left;
        return TestRangeState.combine(left, right);
    }

    public void testCollectionMerge(String message, List<RangeTrie<TestRangeState>> triesToMerge, List<TestRangeState> merged, List<TestRangeState>... sets)
    {
        System.out.println("Markers: " + merged);
        verify(merged);
        // Test that merging the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            RangeTrie<TestRangeState> trie = RangeTrie.merge(triesToMerge, TestRangeState::combineCollection);
            try
            {
                assertEquals(message + " forward b" + bits, merged, toList(trie, Direction.FORWARD));
                assertEquals(message + " reverse b" + bits, Lists.reverse(merged), toList(trie, Direction.REVERSE));
                System.out.println(message + " b" + bits + " matched.");
            }
            catch (AssertionError e)
            {
                System.out.println("\n" + trie.dump());
                throw e;
            }
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                List<TestRangeState> ranges = sets[toRemove];
                System.out.println("Adding:  " + ranges);
                triesToMerge.add(fromList(ranges));
                testCollectionMerge(message + " " + toRemove,
                                    triesToMerge,
                                    mergeLists(merged, ranges),
                                    Arrays.stream(sets)
                                          .filter(x -> x != ranges)
                                          .toArray(List[]::new)
                );
                triesToMerge.remove(triesToMerge.size() - 1);
            }
        }
    }

    int delete(int deletionTime, int data)
    {
        if (data <= deletionTime)
            return -1;
        else
            return data;
    }

    TestRangeState delete(int deletionTime, TestRangeState marker)
    {
        if (deletionTime < 0)
            return marker;

        int newLeft = delete(deletionTime, marker.leftSide);
        int newRight = delete(deletionTime, marker.rightSide);
        if (newLeft < 0 && newRight < 0 || newLeft == newRight)
            return null;
        if (newLeft == marker.leftSide && newRight == marker.rightSide)
            return marker;
        return new TestRangeState(marker.position, newLeft, newRight);
    }

    List<TestRangeState> mergeLists(List<TestRangeState> left, List<TestRangeState> right)
    {
        int active = -1;
        Iterator<TestRangeState> rightIt = right.iterator();
        TestRangeState nextRight = rightIt.hasNext() ? rightIt.next() : null;
        List<TestRangeState> result = new ArrayList<>();
        for (TestRangeState nextLeft : left)
        {
            while (true)
            {
                int cmp;
                if (nextRight == null)
                    cmp = -1;
                else
                {
                    if (nextLeft.position == null || nextRight.position == null)
                    {
                        if (nextLeft.position == null && nextRight.position == null)
                            cmp = Boolean.compare(nextLeft.appliesAfter, nextRight.appliesAfter);
                        else if (nextLeft.position == null)
                            cmp = nextLeft.appliesAfter ? 1 : -1;
                        else // (nextRight.position == null)
                            cmp = nextRight.appliesAfter ? -1 : 1;
                    }
                    else
                        cmp = ByteComparable.compare(nextLeft.position, nextRight.position, TrieUtil.VERSION);
                    if (cmp == 0)
                        cmp = Boolean.compare(nextLeft.appliesAfter, nextRight.appliesAfter);
                }

                if (cmp < 0)
                {
                    maybeAdd(result, nextRight != null ? delete(nextRight.leftSide, nextLeft) : nextLeft);
                    break;
                }

                if (cmp == 0)
                {
                    TestRangeState processed = TestRangeState.combine(nextRight, nextLeft).toContent();
                    maybeAdd(result, processed);
                    nextRight = rightIt.hasNext() ? rightIt.next() : null;
                    break;
                }
                else
                {
                    // Must close active if it becomes covered, and must open active if it is no longer covered.
                    if (active >= 0)
                    {
                        TestRangeState activeMarker = new TestRangeState(nextRight.position, active, active);
                        nextRight = TestRangeState.combine(activeMarker, nextRight).toContent();
                    }
                    maybeAdd(result, nextRight);
                }

                nextRight = rightIt.hasNext() ? rightIt.next() : null;
            }
            active = nextLeft.rightSide;
        }
        assert active == -1;
        while (nextRight != null)
        {
            maybeAdd(result, delete(active, nextRight));// deletion is not needed (active == -1), do just in case
            nextRight = rightIt.hasNext() ? rightIt.next() : null;
        }
        return result;
    }

    static <T> void maybeAdd(List<T> list, T value)
    {
        if (value == null)
            return;
        list.add(value);
    }

    @Test
    public void testRangeUnderCoveredRange()
    {
        String[] ranges1 = {"ba", "bb"};
        String[] ranges2 = {"aa", "ab", "bbc", "bbd", "bbfff", "bbfff", "bce", "bcf", "ce", "cf"};
        // We don't currently handle boundaries that are prefixes of entries and we should identify this and throw an exception.
        RangeTrie<TestRangeState> merge = RangeTrie.merge(List.of(TrieUtil.directRangeTrie(1, ranges1),
                                                                  TrieUtil.directRangeTrie(2, ranges2)),
                                                          TestRangeState::combineCollection);
        var list = toList(merge,
                          Direction.FORWARD);
        System.out.println(list);
        System.out.println(merge.dump());
    }
}
