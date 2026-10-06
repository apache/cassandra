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

import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DataPoint.dumpDeletionAwareTrie;
import static org.apache.cassandra.db.tries.DataPoint.verify;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

@RunWith(Parameterized.class)
public class DeletionAwareMergeTest extends DeletionAwareTestBase
{
    @Parameterized.Parameters(name = "bits per transition {0} deletion point {1} at root {2}")
    public static List<Object[]> mergeData()
    {
        List<Object[]> list = new ArrayList<>();
        int bound = bitsNeeded;
        for (int i = 1; i <= bound; i++)
        {
            Integer x = i;
            for (int y : new int[]{ 4, 13, 22, 31, 40 })
            {
                list.add(new Object[]{ x, y, false });
                list.add(new Object[]{ x, y, true });
            }
        }
        return list;
    }


    @Parameterized.Parameter(1)
    public int deletionPoint = 100;

    @Parameterized.Parameter(2)
    public boolean deletionsAtRoot = false;

    private List<DataPoint> deletedRanges(ByteComparable... dataPoints)
    {
        List<ByteComparable> data = new ArrayList<>(asList(dataPoints));
        invertDataRangeList(data);
        filterOutEmptyRepetitions(data);

        List<DataPoint> markers = new ArrayList<>();
        for (int i = 0; i < data.size(); ++i)
        {
            ByteComparable pos = data.get(i);
            if (pos == null)
                pos = i % 2 == 0 ? before(0) : before((1<<bitsNeeded) - 1);
            if (i % 2 == 0)
                markers.add(new DeletionMarker(pos, -1, deletionPoint));
            else
                markers.add(new DeletionMarker(pos, deletionPoint, -1));
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
                ByteComparable.compare(data.get(i), data.get(i + 1), VERSION) == 0)
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
        {
            testMerge("no merge");

            testMerge("all",
                      deletedRanges(null, null));
            testMerge("fully covered range",
                      deletedRanges(before(20), before(25)));
            testMerge("fully covered range",
                      deletedRanges(before(25), before(33)));
            testMerge("matching range",
                      deletedRanges(before(21), before(24)));
            testMerge("touching empty",
                      deletedRanges(before(24), before(26)));

            testMerge("partial left",
                      deletedRanges(before(22), before(25)));
            testMerge("partial left on change",
                      deletedRanges(before(28), before(32)));
            testMerge("partial left with null",
                      deletedRanges(before(29), null));


            testMerge("partial right",
                      deletedRanges(before(25), before(27)));
            testMerge("partial right on change",
                      deletedRanges(before(25), before(28)));
            testMerge("partial right with null",
                      deletedRanges(null, before(22)));

            testMerge("inside range",
                      deletedRanges(before(22), before(23)));
            testMerge("inside with change",
                      deletedRanges(before(27), before(29)));

            testMerge("empty range inside",
                      deletedRanges(before(27), before(27)));

            testMerge("point covered",
                      deletedRanges(before(16), before(18)));
            testMerge("point at range start",
                      deletedRanges(before(17), before(18)));
            testMerge("point at range end",
                      deletedRanges(before(16), before(17)));


            testMerge("start point covered",
                      deletedRanges(before(32), before(35)));
            testMerge("start point at range start",
                      deletedRanges(before(33), before(35)));
            testMerge("start point at range end",
                      deletedRanges(before(32), before(33)));


            testMerge("end point covered",
                      deletedRanges(before(36), before(40)));
            testMerge("end point at range start",
                      deletedRanges(before(38), before(40)));
            testMerge("end point at range end",
                      deletedRanges(before(36), before(38)));
        }
    }

    @Test
    public void testRanges()
    {
        {
            testMerge("fully covered ranges",
                      deletedRanges(before(20), before(25), before(25), before(33)));
            testMerge("matching ranges",
                      deletedRanges(before(21), before(24), before(26), before(31)));
            testMerge("touching empty",
                      deletedRanges(before(20), before(21), before(24), before(26), before(32), before(33), before(34), before(36)));
            testMerge("partial left",
                      deletedRanges(before(22), before(25), before(29), null));

            testMerge("partial right",
                      deletedRanges(null, before(22), before(25), before(27)));

            testMerge("inside ranges",
                      deletedRanges(before(22), before(23), before(27), before(29)));

            testMerge("jumping inside",
                      deletedRanges(before(21), before(22), before(23), before(24), before(25), before(26), before(27), before(28), before(29), before(30)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        {
            // non-overlapping
            testMerge("non-overlapping", deletedRanges(before(20), before(23)), deletedRanges(before(24), before(27)));
            // touching, i.e. still non-overlapping
            testMerge("touching", deletedRanges(before(20), before(23)), deletedRanges(before(23), before(27)));
            // overlapping 1
            testMerge("overlapping1", deletedRanges(before(20), before(23)), deletedRanges(before(22), before(27)));
            // overlapping 2
            testMerge("overlapping2", deletedRanges(before(20), before(23)), deletedRanges(before(21), before(27)));
            // covered
            testMerge("covered1", deletedRanges(before(20), before(23)), deletedRanges(before(20), before(27)));
            // covered 2
            testMerge("covered2", deletedRanges(before(23), before(27)), deletedRanges(before(20), before(27)));
            // covered 3
            testMerge("covered3", deletedRanges(before(21), before(23)), deletedRanges(before(20), before(27)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
            testMerges();
    }

    private List<DataPoint> getTestRanges()
    {
        return flatten(asList(deletedPoint(17, 20),
                              livePoint(19, 30),
                              from(21, 10), deletedPointInside(22, 21, 10), livePoint(23, 31), to(24, 10),
                              from(26, 11), livePoint(27, 32), change(28, 11, 12).withPoint(22), livePoint(29, 33), to(30, 12),
                              livePoint(32, 34), from(33, 13).withPoint(23), to(34, 13),
                              from(36, 14), to(38, 14).withPoint(24), livePoint(39, 35)));
    }

    private void testMerges()
    {
        testMergeWith("", fromList(getTestRanges()), getTestRanges());

        List<DataPoint> set1 = deletedRanges(null, before(24), before(25), before(29), before(32), null);
        List<DataPoint> set2 = deletedRanges(before(14), before(17),
                                             before(22), before(27),
                                             before(28), before(30),
                                             before(32), before(34),
                                             before(36), before(40));
        List<DataPoint> set3 = deletedRanges(before(17), before(18),
                                             before(19), before(20),
                                             before(21), before(22),
                                             before(23), before(24),
                                             before(25), before(26),
                                             before(27), before(28),
                                             before(29), before(30),
                                             before(31), before(32),
                                             before(33), before(34),
                                             before(35), before(36),
                                             before(37), before(38));

        testMerges(set1, set2, set3);
    }

    private InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> fromList(List<DataPoint> testRanges)
    {
        return DataPoint.fromList(testRanges, false, deletionsAtRoot);
    }

    private void testMerges(List<DataPoint> set1, List<DataPoint> set2, List<DataPoint> set3)
    {
        testMerge("1", set1);

        testMerge("2", set2);

        testMerge("3", set3);

        testMerge("12", set1, set2);

        testMerge("13", set1, set3);

        testMerge("23", set2, set3);

        testMerge("123", set1, set2, set3);
    }

    @SafeVarargs
    public final void testMerge(String message, List<DataPoint>... sets)
    {
        List<DataPoint> testRanges = getTestRanges();
        testMergeWith(message, fromList(testRanges), testRanges, sets);
        testCollectionMerge(message + " collection", Lists.newArrayList(fromList(testRanges)), testRanges, sets);
        testMergeInMemoryTrie(message + " inmem.apply", fromList(testRanges), testRanges, sets);
        testMergeInMemoryTrieIntoSet(message + " inmem.apply into set", fromList(testRanges), testRanges, sets);
    }


    public void testMergeWith(String message, DeletionAwareTrie<LivePoint, DeletionMarker> trie, List<DataPoint> merged, List<DataPoint>... sets)
    {
        if (VERBOSE)
        {
            System.out.println("Markers: " + merged);
            dumpDeletionAwareTrie(trie);
        }
        verify(merged);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertDeletionAwareEqual(message + " forward b" + bits, merged, trie);
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                List<DataPoint> ranges = sets[toRemove];
                InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> adding = fromList(ranges);
                if (VERBOSE)
                {
                    System.out.println("Adding:  " + ranges);
                    dumpDeletionAwareTrie(adding);
                }
                testMergeWith(message + " " + toRemove,
                              trie.mergeWith(adding, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, deletionsAtRoot),
                              mergeLists(merged, ranges),
                              Arrays.stream(sets)
                                .filter(x -> x != ranges)
                                .toArray(List[]::new)
                );
            }
        }
    }

    public void testCollectionMerge(String message, List<DeletionAwareTrie<LivePoint, DeletionMarker>> triesToMerge, List<DataPoint> merged, List<DataPoint>... sets)
    {
        if (VERBOSE)
            System.out.println("Markers: " + merged);
        verify(merged);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            if (VERBOSE)
            {
                System.out.println("Sources:");
                triesToMerge.forEach(DataPoint::dumpDeletionAwareTrie);
            }

            DeletionAwareTrie<LivePoint, DeletionMarker> trie = DeletionAwareTrie.merge(triesToMerge,
                                                                                        LivePoint::combineCollection,
                                                                                        DeletionMarker::combineCollection,
                                                                                        DeletionMarker::applyTo,
                                                                                        deletionsAtRoot);
            if (VERBOSE)
            {
                System.out.println("Result:");
                dumpDeletionAwareTrie(trie);
            }

            assertDeletionAwareEqual(message + " forward b" + bits, merged, trie);
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                List<DataPoint> ranges = sets[toRemove];
                if (VERBOSE)
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

    public void testMergeInMemoryTrie(String message, DeletionAwareTrie<LivePoint, DeletionMarker> trie, List<DataPoint> merged, List<DataPoint>... sets)
    {
        if (VERBOSE)
        {
            System.out.println("Markers: " + merged);
            dumpDeletionAwareTrie(trie);
        }
        verify(merged);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertDeletionAwareEqual(message + " forward b" + bits, merged, trie);
        }
        else
        {
            try
            {
                for (int toRemove = 0; toRemove < sets.length; ++toRemove)
                {
                    List<DataPoint> ranges = sets[toRemove];
                    InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> adding = fromList(ranges);
                    if (VERBOSE)
                    {
                        System.out.println("Adding:  " + ranges);
                        dumpDeletionAwareTrie(adding);
                    }
                    var dupe = duplicateTrie(trie);
                    dupe.apply(adding,
                               DataPoint::combineLive,
                               DataPoint::combineDeletion,
                               DataPoint::deleteLive,
                               DataPoint::deleteLive,
                               deletionsAtRoot,
                               v -> false);
                    testMergeInMemoryTrie(message + " " + toRemove,
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

    public void testMergeInMemoryTrieIntoSet(String message, DeletionAwareTrie<LivePoint, DeletionMarker> trie, List<DataPoint> merged, List<DataPoint>... sets)
    {
        if (VERBOSE)
        {
            System.out.println("Markers: " + merged);
            dumpDeletionAwareTrie(trie);
        }
        verify(merged);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertDeletionAwareEqual(message + " forward b" + bits, merged, trie);
        }
        else
        {
            try
            {
                for (int toRemove = 0; toRemove < sets.length; ++toRemove)
                {
                    List<DataPoint> ranges = sets[toRemove];
                    var set = fromList(ranges);
                    if (VERBOSE)
                    {
                        System.out.println("Adding:  " + ranges);
                        dumpDeletionAwareTrie(set);
                    }
                    set.apply(trie,
                              DataPoint::combineLive,
                              DataPoint::combineDeletion,
                              DataPoint::deleteLive,
                              DataPoint::deleteLive,
                              false,
                              v -> false);
                    testMergeInMemoryTrieIntoSet(message + " " + toRemove,
                                                 set,
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

    InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> duplicateTrie(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        try
        {
            InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> copy = InMemoryDeletionAwareTrie.shortLived(VERSION);
            copy.apply(trie,
                    DataPoint::combineLive,
                    DataPoint::combineDeletion,
                    DataPoint::deleteLive,
                    DataPoint::deleteLive,
                    false,
                    v -> false);
            return copy;
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    static DeletionMarker delete(int deletionTime, DeletionMarker marker)
    {
        if (deletionTime < 0 || marker == null)
            return marker;

        int newLeft = Math.max(deletionTime, marker.leftSide);
        int newRight = Math.max(deletionTime, marker.rightSide);
        if (newLeft < 0 && newRight < 0 || newLeft == newRight)
            return null;
        if (newLeft == marker.leftSide && newRight == marker.rightSide)
            return marker;
        return new DeletionMarker(marker.position, newLeft, newRight);
    }

    static LivePoint delete(int deletionTime, LivePoint marker)
    {
        if (deletionTime < 0 || marker == null)
            return marker;
        return marker.delete(deletionTime);
    }

    static DataPoint delete(int deletionTime, DataPoint marker)
    {
        LivePoint live = delete(deletionTime, marker.live());
        DeletionMarker deletion = delete(deletionTime, marker.marker());
        return DataPoint.resolve(live, deletion);
    }

    static int leftSide(DataPoint point)
    {
        if (point.marker() == null)
            return -1;
        return point.marker().leftSide;
    }

    static int rightSide(DataPoint point)
    {
        if (point.marker() == null)
            return -1;
        return point.marker().rightSide;
    }

    static List<DataPoint> mergeLists(List<DataPoint> left, List<DataPoint> right)
    {
        int active = -1;
        Iterator<DataPoint> rightIt = right.iterator();
        DataPoint nextRight = rightIt.hasNext() ? rightIt.next() : null;
        List<DataPoint> result = new ArrayList<>();
        for (DataPoint nextLeft : left)
        {
            while (true)
            {
                int cmp;
                if (nextRight == null)
                    cmp = -1;
                else
                    cmp = ByteComparable.compare(nextLeft.position(), nextRight.position(), VERSION);

                if (cmp < 0)
                {
                    maybeAdd(result, nextRight != null ? delete(leftSide(nextRight), nextLeft) : nextLeft);
                    break;
                }

                if (cmp == 0)
                {
                    if (nextLeft.marker() == null)
                        nextRight = delete(active, nextRight);
                    if (nextRight != null)
                        maybeAdd(result, DataPoint.combine(nextRight, nextLeft).toContent());
                    else
                        maybeAdd(result, nextLeft);

                    nextRight = rightIt.hasNext() ? rightIt.next() : null;
                    break;
                }
                else
                {
                    // Must close active if it becomes covered, and must open active if it is no longer covered.
                    maybeAdd(result, delete(active, nextRight));
                }

                nextRight = rightIt.hasNext() ? rightIt.next() : null;
            }
            if (nextLeft.marker() != null)
                active = nextLeft.marker().rightSide;
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
}
