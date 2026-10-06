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

import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DataPoint.fromList;
import static org.apache.cassandra.db.tries.DataPoint.verify;

public class DeletionAwareIntersectionTest extends DeletionAwareTestBase
{

    @Test
    public void testSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            testIntersection("no intersection");

            testIntersection("all",
                             array(null, null));
            testIntersection("fully covered range",
                             array(before(20), before(25)));
            testIntersection("fully covered range",
                             array(before(25), before(33)));
            testIntersection("matching range",
                             array(before(21), before(24)));
            testIntersection("touching empty",
                             array(before(24), before(26)));

            testIntersection("partial left",
                             array(before(22), before(25)));
            testIntersection("partial left on change",
                             array(before(28), before(32)));
            testIntersection("partial left with null",
                             array(before(29), null));


            testIntersection("partial right",
                             array(before(25), before(27)));
            testIntersection("partial right on change",
                             array(before(25), before(28)));
            testIntersection("partial right with null",
                             array(null, before(22)));

            testIntersection("inside range",
                             array(before(22), before(23)));
            testIntersection("inside with change",
                             array(before(27), before(29)));

            testIntersection("point covered",
                             array(before(16), before(18)));
            testIntersection("point at range start",
                             array(before(17), before(18)));
            testIntersection("point at range end",
                             array(before(16), before(17)));


            testIntersection("start point covered",
                             array(before(32), before(35)));
            testIntersection("start point at range start",
                             array(before(33), before(35)));
            testIntersection("start point at range end",
                             array(before(32), before(33)));


            testIntersection("end point covered",
                             array(before(36), before(40)));
            testIntersection("end point at range start",
                             array(before(38), before(40)));
            testIntersection("end point at range end",
                             array(before(36), before(38)));
        }
    }

    @Test
    public void testRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            testIntersection("fully covered ranges",
                             array(before(20), before(25), before(25), before(33)));
            testIntersection("matching ranges",
                             array(before(21), before(24), before(26), before(31)));
            testIntersection("touching empty",
                             array(before(20), before(21), before(24), before(26), before(32), before(33), before(34), before(36)));
            testIntersection("partial left",
                             array(before(22), before(25), before(29), null));

            testIntersection("partial right",
                             array(null, before(22), before(25), before(27)));

            testIntersection("inside ranges",
                             array(before(22), before(23), before(27), before(29)));

            testIntersection("jumping inside",
                             array(before(21), before(22), before(23), before(24), before(25), before(26), before(27), before(28), before(29), before(30)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            // non-overlapping
            testIntersection("", array(before(20), before(23)), array(before(24), before(27)));
            // touching, i.e. still non-overlapping
            testIntersection("", array(before(20), before(23)), array(before(23), before(27)));
            // overlapping 1
            testIntersection("", array(before(20), before(23)), array(before(22), before(27)));
            // overlapping 2
            testIntersection("", array(before(20), before(23)), array(before(21), before(27)));
            // covered
            testIntersection("", array(before(20), before(23)), array(before(20), before(27)));
            // covered
            testIntersection("", array(before(23), before(27)), array(before(20), before(27)));
            // covered 2
            testIntersection("", array(before(21), before(23)), array(before(20), before(27)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections();
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

    private DeletionAwareTrie<LivePoint, DeletionMarker> mergeGeneratedRanges()
    {
        return fromList(asList(from(21, 10), to(24, 10),
                               from(26, 11), to(29, 11),
                               from(33, 13), to(34, 13),
                               from(36, 14), to(38, 14)))
               .mergeWith(fromList(asList(from(28, 12), to(30, 12))),
                          LivePoint::combine,
                          DeletionMarker::combine,
                          DeletionMarker::applyTo, false)
               .mergeWith(fromList(flatten(asList(deletedPoint(17, 20),
                                                  deletedPoint(22, 21),
                                                  deletedPoint(28, 22),
                                                  deletedPoint(33, 23),
                                                  deletedPoint(38, 24)))),
                          LivePoint::combine,
                          DeletionMarker::combine,
                          DeletionMarker::applyTo, false)
               .mergeWith(fromList(asList(livePoint(19, 30),
                                          livePoint(23, 31),
                                          livePoint(27, 32),
                                          livePoint(29, 33),
                                          livePoint(32, 34),
                                          livePoint(39, 35))),
                          LivePoint::combine,
                          DeletionMarker::combine,
                          DeletionMarker::applyTo, false)
                          ;
    }

    private DeletionAwareTrie<LivePoint, DeletionMarker> collectionMergeGeneratedRanges()
    {
        return DeletionAwareTrie.merge(asList(
                                       fromList(asList(from(21, 10), to(24, 10),
                                                       from(26, 11), to(29, 11),
                                                       from(33, 13), to(34, 13),
                                                       from(36, 14), to(38, 14))),
                                       fromList(asList(from(28, 12), to(30, 12))),
                                       fromList(flatten(asList(deletedPoint(17, 20),
                                                               deletedPoint(22, 21),
                                                               deletedPoint(28, 22),
                                                               deletedPoint(33, 23),
                                                               deletedPoint(38, 24)))),
                                       fromList(asList(livePoint(19, 30),
                                                       livePoint(23, 31),
                                                       livePoint(27, 32),
                                                       livePoint(29, 33),
                                                       livePoint(32, 34),
                                                       livePoint(39, 35)))
                                       ),
                                       LivePoint::combineCollection,
                                       DeletionMarker::combineCollection,
                                       DeletionMarker::applyTo,
                                       false);
    }

    private void testIntersections()
    {
        testIntersection("");

        ByteComparable[] set1 = array(null, before(24),
                                      before(25), before(29),
                                      before(32), null);
        ByteComparable[] set2 = array(before(14), before(17),
                                      before(22), before(27),
                                      before(28), before(30),
                                      before(32), before(34),
                                      before(36), before(40));
        ByteComparable[] set3 = array(before(17), before(18),
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

        testIntersections(set1, set2, set3);
    }

    private void testIntersections(ByteComparable[] set1, ByteComparable[] set2, ByteComparable[] set3)
    {
        testIntersection("1", set1);

        testIntersection("2", set2);

        testIntersection("3", set3);

        testIntersection("12", set1, set2);

        testIntersection("13", set1, set3);

        testIntersection("23", set2, set3);

        testIntersection("123", set1, set2, set3);
    }

    public void testIntersection(String message, ByteComparable[]... sets)
    {
        final List<DataPoint> testRanges = getTestRanges();
        testIntersection(message, fromList(testRanges), testRanges, sets);
        testIntersection(message + " on merge ", mergeGeneratedRanges(), testRanges, sets); // Mainly tests MergeCursor's skipTo
        testIntersection(message + " on collection merge ", collectionMergeGeneratedRanges(), testRanges, sets); // Mainly tests MergeCursor's skipTo
    }

    public void testIntersection(String message, DeletionAwareTrie<LivePoint, DeletionMarker> trie, List<DataPoint> intersected, ByteComparable[]... sets)
    {
        if (VERBOSE)
        {
            System.out.println("Markers: " + intersected);
            DataPoint.dumpDeletionAwareTrie(trie);
        }
        verify(intersected);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertDeletionAwareEqual(message + " forward b" + bits, intersected, trie);
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                ByteComparable[] ranges = sets[toRemove];
                System.out.println("Ranges:  " + toString(ranges));
                testIntersection(message + " " + toRemove,
                                 trie.intersect(TrieSet.ranges(TrieUtil.VERSION, ranges)),
                                 intersect(intersected, ranges),
                                 Arrays.stream(sets)
                                       .filter(x -> x != ranges)
                                       .toArray(ByteComparable[][]::new)
                );
            }
        }
    }
}
