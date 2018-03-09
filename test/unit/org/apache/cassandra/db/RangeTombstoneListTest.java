/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.google.common.base.Joiner;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RangeTombstoneListTest
{
    private static final ClusteringComparator cmp = new ClusteringComparator(Int32Type.instance);

    @Test
    public void sortedAdditionTest()
    {
        sortedAdditionTest(0);
        sortedAdditionTest(10);
    }

    private void sortedAdditionTest(int initialCapacity)
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, initialCapacity);
        RangeTombstone rt1 = rt(1, 5, 3);
        RangeTombstone rt2 = rt(7, 10, 2);
        RangeTombstone rt3 = rt(10, 13, 1);

        l.add(rt1);
        l.add(rt2);
        l.add(rt3);

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rt1, iter.next());
        assertRT(rt2, iter.next());
        assertRT(rtei(10, 13, 1), iter.next());

        assert !iter.hasNext();
    }

    @Test
    public void nonSortedAdditionTest()
    {
        nonSortedAdditionTest(0);
        nonSortedAdditionTest(10);
    }

    private void nonSortedAdditionTest(int initialCapacity)
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, initialCapacity);
        RangeTombstone rt1 = rt(1, 5, 3);
        RangeTombstone rt2 = rt(7, 10, 2);
        RangeTombstone rt3 = rt(10, 13, 1);

        l.add(rt2);
        l.add(rt1);
        l.add(rt3);

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rt1, iter.next());
        assertRT(rt2, iter.next());
        assertRT(rtei(10, 13, 1), iter.next());

        assert !iter.hasNext();
    }

    @Test
    public void overlappingAdditionTest()
    {
        overlappingAdditionTest(0);
        overlappingAdditionTest(10);
    }

    private void overlappingAdditionTest(int initialCapacity)
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, initialCapacity);

        l.add(rt(4, 10, 3));
        l.add(rt(1, 7, 2));
        l.add(rt(8, 13, 4));
        l.add(rt(0, 15, 1));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rtie(0, 1, 1), iter.next());
        assertRT(rtie(1, 4, 2), iter.next());
        assertRT(rtie(4, 8, 3), iter.next());
        assertRT(rt(8, 13, 4), iter.next());
        assertRT(rtei(13, 15, 1), iter.next());
        assert !iter.hasNext();

        RangeTombstoneList l2 = new RangeTombstoneList(cmp, initialCapacity);
        l2.add(rt(4, 10, 12L));
        l2.add(rt(0, 8, 25L));

        assertEquals(25L, l2.searchDeletionTime(clustering(8)).markedForDeleteAt());
    }

    @Test
    public void largeAdditionTest()
    {
        int N = 3000;
        // Test that the StackOverflow from #6181 is fixed
        RangeTombstoneList l = new RangeTombstoneList(cmp, N);
        for (int i = 0; i < N; i++)
            l.add(rt(2*i+1, 2*i+2, 1));
        assertEquals(l.size(), N);

        l.add(rt(0, 2*N+3, 2));
    }

    @Test
    public void simpleOverlapTest()
    {
        RangeTombstoneList l1 = new RangeTombstoneList(cmp, 0);
        l1.add(rt(0, 10, 3));
        l1.add(rt(3, 7, 5));

        Iterator<RangeTombstone> iter1 = l1.iterator();
        assertRT(rtie(0, 3, 3), iter1.next());
        assertRT(rt(3, 7, 5), iter1.next());
        assertRT(rtei(7, 10, 3), iter1.next());
        assert !iter1.hasNext();

        RangeTombstoneList l2 = new RangeTombstoneList(cmp, 0);
        l2.add(rt(0, 10, 3));
        l2.add(rt(3, 7, 2));

        Iterator<RangeTombstone> iter2 = l2.iterator();
        assertRT(rt(0, 10, 3), iter2.next());
        assert !iter2.hasNext();
    }

    @Test
    public void overlappingPreviousEndEqualsStartTest()
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);
        // add a RangeTombstone, so, last insert is not in insertion order
        l.add(rt(11, 12, 2));
        l.add(rt(1, 4, 2));
        l.add(rt(4, 10, 5));

        assertEquals(2, l.searchDeletionTime(clustering(3)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(4)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(8)).markedForDeleteAt());
        assertEquals(3, l.size());
    }

    @Test
    public void searchTest()
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);
        l.add(rt(0, 4, 5));
        l.add(rt(4, 6, 2));
        l.add(rt(9, 12, 1));
        l.add(rt(14, 15, 3));
        l.add(rt(15, 17, 6));

        assertEquals(null, l.searchDeletionTime(clustering(-1)));

        assertEquals(5, l.searchDeletionTime(clustering(0)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(3)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(4)).markedForDeleteAt());

        assertEquals(2, l.searchDeletionTime(clustering(5)).markedForDeleteAt());

        assertEquals(null, l.searchDeletionTime(clustering(7)));

        assertEquals(3, l.searchDeletionTime(clustering(14)).markedForDeleteAt());

        assertEquals(6, l.searchDeletionTime(clustering(15)).markedForDeleteAt());
        assertEquals(null, l.searchDeletionTime(clustering(18)));
    }

    @Test
    public void addAllTest()
    {
        RangeTombstoneList l1 = new RangeTombstoneList(cmp, 0);
        l1.add(rt(0, 4, 5));
        l1.add(rt(6, 10, 2));
        l1.add(rt(15, 17, 1));

        RangeTombstoneList l2 = new RangeTombstoneList(cmp, 0);
        l2.add(rt(3, 5, 7));
        l2.add(rt(7, 8, 3));
        l2.add(rt(8, 12, 1));
        l2.add(rt(14, 17, 4));

        l1.addAll(l2);

        Iterator<RangeTombstone> iter = l1.iterator();
        assertRT(rtie(0, 3, 5), iter.next());
        assertRT(rt(3, 5, 7), iter.next());
        assertRT(rtie(6, 7, 2), iter.next());
        assertRT(rt(7, 8, 3), iter.next());
        assertRT(rtei(8, 10, 2), iter.next());
        assertRT(rtei(10, 12, 1), iter.next());
        assertRT(rt(14, 17, 4), iter.next());

        assert !iter.hasNext();
    }

    @Test
    public void addAllSequentialTest()
    {
        RangeTombstoneList l1 = new RangeTombstoneList(cmp, 0);
        l1.add(rt(3, 5, 2));

        RangeTombstoneList l2 = new RangeTombstoneList(cmp, 0);
        l2.add(rt(5, 7, 7));

        l1.addAll(l2);

        Iterator<RangeTombstone> iter = l1.iterator();
        assertRT(rtie(3, 5, 2), iter.next());
        assertRT(rt(5, 7, 7), iter.next());

        assert !iter.hasNext();
    }

    @Test
    public void addAllIncludedTest()
    {
        RangeTombstoneList l1 = new RangeTombstoneList(cmp, 0);
        l1.add(rt(3, 10, 5));

        RangeTombstoneList l2 = new RangeTombstoneList(cmp, 0);
        l2.add(rt(4, 5, 2));
        l2.add(rt(5, 7, 3));
        l2.add(rt(7, 9, 4));

        l1.addAll(l2);

        Iterator<RangeTombstone> iter = l1.iterator();
        assertRT(rt(3, 10, 5), iter.next());

        assert !iter.hasNext();
    }

    @Test
    public void addAllBugFrom9799()
    {
        RangeTombstoneList l1 = fromString("{ (6, 7]@4 - (7, 8)@1 - [12, 12]@0 - [13, 13]@0 - (20, 21)@3 - [27, 27]@2 - (33, 34)@2 - (35, 36]@4 - (40, 41]@0 - (42, 43)@2 - (44, 45)@3 - [47, 47]@1 - (47, 48)@0 - [55, 55]@4 - [61, 61]@4 - [67, 67]@0 - [70, 70]@4 - [77, 77]@1 - (83, 84)@1 - [90, 90]@0 - (91, 92]@4 - [93, 93]@0 - (94, 95)@2 - (100, 101]@3 - (103, 104]@0 - (108, 109]@2 - (115, 116]@3 - (116, 117]@3 - (118, 119)@4 - (125, 126)@2 - [131, 131]@1 - [132, 132]@3 - [139, 139]@0 - [145, 145]@1 - (145, 146]@3 - (147, 148]@4 - (150, 151]@1 - (156, 157)@2 - (158, 159)@2 - [164, 164]@4 - (168, 169)@0 - (171, 172)@4 - (173, 174]@0 - [179, 179]@1 - (186, 187]@4 - [191, 191]@1 }");
        RangeTombstoneList l2 = fromString("{ (1, 12)@8 - [12, 13)@8 - [13, 18]@7 }");
        l1.addAll(l2);
        assertValid(l1);
    }

    private RangeTombstoneList makeRandom(Random rand, int size, int maxItSize, int maxItDistance, int maxMarkedAt)
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, size);

        int prevStart = -1;
        int prevEnd = 0;
        boolean prevStartInclusive = false;
        boolean prevEndInclusive = false;
        for (int i = 0; i < size; i++)
        {
            int nextStart = prevEnd + rand.nextInt(maxItDistance);
            int nextEnd = nextStart + rand.nextInt(maxItSize);

            boolean startInclusive = rand.nextBoolean();
            boolean endInclusive = rand.nextBoolean();

            // Now make sure we create meaningful ranges

            if (prevEnd == nextStart)
                startInclusive = !prevEndInclusive;

            if (nextStart == nextEnd)
            {
                if (startInclusive)
                    endInclusive = true;
                else
                    nextEnd += 1;
            }

            l.add(rt(nextStart, startInclusive, nextEnd, endInclusive, rand.nextInt(maxMarkedAt)));

            prevStart = nextStart;
            prevEnd = nextEnd;
            prevStartInclusive = startInclusive;
            prevEndInclusive = endInclusive;
        }
        return l;
    }

    @Test
    public void addAllRandomTest() throws Throwable
    {
        int TEST_COUNT = 1000;
        int MAX_LIST_SIZE = 50;

        int MAX_IT_SIZE = 20;
        int MAX_IT_DISTANCE = 10;
        int MAX_MARKEDAT = 10;

        long seed = System.nanoTime();
        Random rand = new Random(seed);

        for (int i = 0; i < TEST_COUNT; i++)
        {
            RangeTombstoneList l1 = makeRandom(rand, rand.nextInt(MAX_LIST_SIZE) + 1, rand.nextInt(MAX_IT_SIZE) + 1, rand.nextInt(MAX_IT_DISTANCE) + 1, rand.nextInt(MAX_MARKEDAT) + 1);
            RangeTombstoneList l2 = makeRandom(rand, rand.nextInt(MAX_LIST_SIZE) + 1, rand.nextInt(MAX_IT_SIZE) + 1, rand.nextInt(MAX_IT_DISTANCE) + 1, rand.nextInt(MAX_MARKEDAT) + 1);

            RangeTombstoneList l1Initial = l1.copy();

            try
            {
                // We generate the list randomly, so "all" we check is that the resulting range tombstone list looks valid.
                l1.addAll(l2);
                assertValid(l1);
            }
            catch (Throwable e)
            {
                System.out.println("Error merging:");
                System.out.println(" l1: " + toString(l1Initial));
                System.out.println(" l2: " + toString(l2));
                System.out.println("Seed was: " + seed);
                throw e;
            }
        }
    }

    @Test
    public void nonSortedAdditionTestWithOneTombstoneWithEmptyEnd()
    {
        nonSortedAdditionTestWithOneRangeWithEmptyEnd(0);
        nonSortedAdditionTestWithOneRangeWithEmptyEnd(10);
    }

  private static void nonSortedAdditionTestWithOneRangeWithEmptyEnd(int initialCapacity)
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, initialCapacity);
        RangeTombstone rt1 = rt(1, 5, 3);
        RangeTombstone rt2 = rt(7, 10, 2);
        RangeTombstone rt3 = atLeast(11, 1, 0);

        l.add(rt2);
        l.add(rt3);
        l.add(rt1);

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rt1, iter.next());
        assertRT(rt2, iter.next());
        assertRT(rt3, iter.next());

        assert !iter.hasNext();
    }

    @Test
    public void addRangeWithEmptyEndWitchIncludeExistingRange()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(rt(4, 10, 3));
        l.add(atLeast(3, 4, 0));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(atLeast(3, 4, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void addRangeWithEmptyStartAndEnd()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(rt(4, 10, 3));
        l.add(atMost(12, 4, 0));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(atMost(12, 4, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void addRangeWithEmptyEndToRangeWithEmptyStartAndEnd()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(new RangeTombstone(Slice.ALL, new DeletionTime(2, 0)));
        l.add(atLeast(12, 4, 0));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(lessThan(12, 2, 0), iter.next());
        assertRT(atLeast(12, 4, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void addRangeWithEmptyEndWitchIncludeExistingRangeWithEmptyEnd()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(atLeast(5, 3, 0));
        l.add(atLeast(3, 4, 0));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(atLeast(3, 4, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void addIncludedRangeToRangeWithEmptyEnd()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(atLeast(3, 3, 0));
        l.add(rt(4, 10, 4));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rtie(3, 4, 3), iter.next());
        assertRT(rt(4, 10, 4), iter.next());
        assertRT(greaterThan(10, 3, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void addIncludedRangeWithEmptyEndToRangeWithEmptyEnd()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(atLeast(3, 3, 0));
        l.add(atLeast(5, 4, 0));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rtie(3, 5, 3), iter.next());
        assertRT(atLeast(5, 4, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void addRangeWithEmptyEndWitchOverlapExistingRange()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(rt(4, 10, 3));
        l.add(atLeast(6, 4, 0));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rtie(4, 6, 3), iter.next());
        assertRT(atLeast(6, 4, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void addOverlappingRangeToRangeWithEmptyEnd()
    {

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);

        l.add(atLeast(3, 3, 0));
        l.add(rt(1, 10, 4));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rt(1, 10, 4), iter.next());
        assertRT(greaterThan(10, 3, 0), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void searchTestWithEmptyStart()
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);
        l.add(atMost(4, 5, 0));
        l.add(rt(4, 6, 2));
        l.add(rt(9, 12, 1));
        l.add(rt(14, 15, 3));
        l.add(rt(15, 17, 6));

        assertEquals(5, l.searchDeletionTime(clustering(-1)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(0)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(3)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(4)).markedForDeleteAt());

        assertEquals(2, l.searchDeletionTime(clustering(5)).markedForDeleteAt());

        assertEquals(null, l.searchDeletionTime(clustering(7)));

        assertEquals(3, l.searchDeletionTime(clustering(14)).markedForDeleteAt());

        assertEquals(6, l.searchDeletionTime(clustering(15)).markedForDeleteAt());
        assertEquals(null, l.searchDeletionTime(clustering(18)));
    }

    @Test
    public void searchTestWithRangeWithEmptyEnd()
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);
        l.add(rt(0, 4, 5));
        l.add(rt(4, 6, 2));
        l.add(rt(9, 12, 1));
        l.add(rt(14, 15, 3));
        l.add(atLeast(15, 6, 0));

        assertEquals(null, l.searchDeletionTime(clustering(-1)));

        assertEquals(5, l.searchDeletionTime(clustering(0)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(3)).markedForDeleteAt());
        assertEquals(5, l.searchDeletionTime(clustering(4)).markedForDeleteAt());

        assertEquals(2, l.searchDeletionTime(clustering(5)).markedForDeleteAt());

        assertEquals(null, l.searchDeletionTime(clustering(7)));

        assertEquals(3, l.searchDeletionTime(clustering(14)).markedForDeleteAt());

        assertEquals(6, l.searchDeletionTime(clustering(15)).markedForDeleteAt());
        assertEquals(6, l.searchDeletionTime(clustering(1000)).markedForDeleteAt());
    }

    private static void assertRT(RangeTombstone expected, RangeTombstone actual)
    {
        assertTrue(String.format("%s != %s", toString(expected), toString(actual)), cmp.compare(expected.deletedSlice().start(), actual.deletedSlice().start()) == 0);
        assertTrue(String.format("%s != %s", toString(expected), toString(actual)), cmp.compare(expected.deletedSlice().end(), actual.deletedSlice().end()) == 0);
        assertEquals(String.format("%s != %s", toString(expected), toString(actual)), expected.deletionTime(), actual.deletionTime());
    }

    private static void assertValid(RangeTombstoneList l)
    {
        if (l.isEmpty())
            return;

        // We check that ranges are in the right order and non overlapping
        Iterator<RangeTombstone> iter = l.iterator();
        Slice prev = iter.next().deletedSlice();
        assertFalse("Invalid empty slice " + prev.toString(cmp), prev.isEmpty(cmp));

        while (iter.hasNext())
        {
            Slice curr = iter.next().deletedSlice();

            assertFalse("Invalid empty slice " + curr.toString(cmp), curr.isEmpty(cmp));
            assertTrue("Slice not in order or overlapping : " + prev.toString(cmp) + curr.toString(cmp), cmp.compare(prev.end(), curr.start()) <= 0);
        }
    }

    private static String toString(RangeTombstone rt)
    {
        return String.format("%s@%d", rt.deletedSlice().toString(cmp), rt.deletionTime().markedForDeleteAt());
    }

    private static String toString(RangeTombstoneList l)
    {
        String[] ranges = new String[l.size()];
        int i = 0;
        for (RangeTombstone rt : l)
            ranges[i++] = toString(rt);

        return "{ " + Joiner.on(" - ").join(ranges) + " }";
    }

    private static RangeTombstone rangeFromString(String range)
    {
        Matcher matcher = Pattern.compile("([\\[(])(\\d+), (\\d+)([)\\]])@(\\d+)").matcher(range.trim());
        matcher.matches();
        boolean isOpenInclusive = matcher.group(1).equals("[");
        int start = Integer.valueOf(matcher.group(2));
        int end = Integer.valueOf(matcher.group(3));
        boolean isCloseInclusive = matcher.group(4).equals("]");
        long timestamp = Long.valueOf(matcher.group(5));
        return rt(start, isOpenInclusive, end, isCloseInclusive, timestamp);
    }

    private static RangeTombstoneList fromString(String str)
    {
        str = str.trim();
        String[] ranges = str.substring(1, str.length() - 1).split("-", 0);
        RangeTombstoneList l = new RangeTombstoneList(cmp, ranges.length);
        for (String range : ranges)
            l.add(rangeFromString(range));
        return l;
    }


    private static Clustering clustering(int i)
    {
        return Clustering.make(bb(i));
    }

    private static ByteBuffer bb(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    private static RangeTombstone rt(int start, int end, long tstamp)
    {
        return rt(start, end, tstamp, 0);
    }

    private static RangeTombstone rt(int start, boolean startInclusive, int end, boolean endInclusive, long tstamp)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.create(cmp, true, startInclusive, start), ClusteringBound.create(cmp, false, endInclusive, end)), new DeletionTime(tstamp, 0));
    }

    private static RangeTombstone rt(int start, int end, long tstamp, int delTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.inclusiveStartOf(bb(start)), ClusteringBound.inclusiveEndOf(bb(end))), new DeletionTime(tstamp, delTime));
    }

    private static RangeTombstone rtei(int start, int end, long tstamp)
    {
        return rtei(start, end, tstamp, 0);
    }

    private static RangeTombstone rtei(int start, int end, long tstamp, int delTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.exclusiveStartOf(bb(start)), ClusteringBound.inclusiveEndOf(bb(end))), new DeletionTime(tstamp, delTime));
    }

    private static RangeTombstone rtie(int start, int end, long tstamp)
    {
        return rtie(start, end, tstamp, 0);
    }

    private static RangeTombstone rtie(int start, int end, long tstamp, int delTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.inclusiveStartOf(bb(start)), ClusteringBound.exclusiveEndOf(bb(end))), new DeletionTime(tstamp, delTime));
    }

    private static RangeTombstone atLeast(int start, long tstamp, int delTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.inclusiveStartOf(bb(start)), ClusteringBound.TOP), new DeletionTime(tstamp, delTime));
    }

    private static RangeTombstone atMost(int end, long tstamp, int delTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.BOTTOM, ClusteringBound.inclusiveEndOf(bb(end))), new DeletionTime(tstamp, delTime));
    }

    private static RangeTombstone lessThan(int end, long tstamp, int delTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.BOTTOM, ClusteringBound.exclusiveEndOf(bb(end))), new DeletionTime(tstamp, delTime));
    }

    private static RangeTombstone greaterThan(int start, long tstamp, int delTime)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.exclusiveStartOf(bb(start)), ClusteringBound.TOP), new DeletionTime(tstamp, delTime));
    }
}
