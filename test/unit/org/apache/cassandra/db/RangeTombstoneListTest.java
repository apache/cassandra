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

import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RangeTombstoneListTest
{
    private static final Comparator<Composite> cmp = new SimpleDenseCellNameType(IntegerType.instance);

    @Test
    public void testDiff()
    {
        RangeTombstoneList superset;
        RangeTombstoneList subset;
        RangeTombstoneList diff;
        Iterator<RangeTombstone> iter;

        // no difference
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 50, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(20, 30, 10));
        subset.add(rt(40, 50, 10));
        assertNull( subset.diff(superset));

        // all items in subset are contained by the first range in the superset
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        subset.add(rt(1, 2, 3));
        subset.add(rt(3, 4, 4));
        subset.add(rt(5, 6, 5));
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(1, 10, 10), iter.next());
        assertRT(rt(20, 30, 10), iter.next());
        assertRT(rt(40, 50, 10), iter.next());
        assertFalse(iter.hasNext());

        // multiple subset RTs are contained by superset RTs
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        subset.add(rt(1, 2, 1));
        subset.add(rt(3, 4, 2));
        subset.add(rt(5, 6, 3));
        superset.add(rt(1, 5, 2));
        superset.add(rt(5, 6, 3));
        superset.add(rt(6, 10, 2));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(1, 5, 2), iter.next());
        assertRT(rt(6, 10, 2), iter.next());
        assertFalse(iter.hasNext());

        // the superset has one RT that covers the entire subset
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 50, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(20, 30, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(1, 50, 10), iter.next());
        assertFalse(iter.hasNext());

        // the superset has one RT that covers the remainder of the subset
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 50, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(20, 30, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(20, 50, 10), iter.next());
        assertFalse(iter.hasNext());

        // only the timestamp differs on one RT
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 20));
        superset.add(rt(40, 50, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(20, 30, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(20, 30, 20), iter.next());
        assertFalse(iter.hasNext());

        // superset has a large range on an RT at the start
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 50, 10));
        subset.add(rt(1, 2, 3));
        subset.add(rt(20, 30, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(1, 10, 10), iter.next());
        assertFalse(iter.hasNext());

        // superset has a larger range on an RT in the middle
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 50, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(20, 25, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(20, 30, 10), iter.next());
        assertFalse(iter.hasNext());

        // superset has a larger range on an RT at the end
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 55, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(20, 30, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(40, 55, 10), iter.next());
        assertFalse(iter.hasNext());

         // superset has one additional RT in the middle
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 50, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(20, 30, 10), iter.next());
        assertFalse(iter.hasNext());

        // superset has one additional RT at the start
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 50, 10));
        subset.add(rt(20, 30, 10));
        subset.add(rt(40, 50, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(1, 10, 10), iter.next());
        assertFalse(iter.hasNext());

        // superset has one additional RT at the end
        superset = new RangeTombstoneList(cmp, 10);
        subset = new RangeTombstoneList(cmp, 10);
        superset.add(rt(1, 10, 10));
        superset.add(rt(20, 30, 10));
        superset.add(rt(40, 50, 10));
        subset.add(rt(1, 10, 10));
        subset.add(rt(20, 30, 10));
        diff = subset.diff(superset);
        iter = diff.iterator();
        assertRT(rt(40, 50, 10), iter.next());
        assertFalse(iter.hasNext());
    }

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
        assertRT(rt3, iter.next());

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
        assertRT(rt3, iter.next());

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
        assertRT(rt(0, 1, 1), iter.next());
        assertRT(rt(1, 4, 2), iter.next());
        assertRT(rt(4, 8, 3), iter.next());
        assertRT(rt(8, 13, 4), iter.next());
        assertRT(rt(13, 15, 1), iter.next());
        assert !iter.hasNext();

        RangeTombstoneList l2 = new RangeTombstoneList(cmp, initialCapacity);
        l2.add(rt(4, 10, 12L));
        l2.add(rt(0, 8, 25L));

        assertEquals(25L, l2.searchDeletionTime(b(8)).markedForDeleteAt);
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
        assertRT(rt(0, 3, 3), iter1.next());
        assertRT(rt(3, 7, 5), iter1.next());
        assertRT(rt(7, 10, 3), iter1.next());
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

        assertEquals(2, l.searchDeletionTime(b(3)).markedForDeleteAt);
        assertEquals(5, l.searchDeletionTime(b(4)).markedForDeleteAt);
        assertEquals(5, l.searchDeletionTime(b(8)).markedForDeleteAt);
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

        assertEquals(null, l.searchDeletionTime(b(-1)));

        assertEquals(5, l.searchDeletionTime(b(0)).markedForDeleteAt);
        assertEquals(5, l.searchDeletionTime(b(3)).markedForDeleteAt);
        assertEquals(5, l.searchDeletionTime(b(4)).markedForDeleteAt);

        assertEquals(2, l.searchDeletionTime(b(5)).markedForDeleteAt);

        assertEquals(null, l.searchDeletionTime(b(7)));

        assertEquals(3, l.searchDeletionTime(b(14)).markedForDeleteAt);

        assertEquals(6, l.searchDeletionTime(b(15)).markedForDeleteAt);
        assertEquals(null, l.searchDeletionTime(b(18)));
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
        assertRT(rt(0, 3, 5), iter.next());
        assertRT(rt(3, 4, 7), iter.next());
        assertRT(rt(4, 5, 7), iter.next());
        assertRT(rt(6, 7, 2), iter.next());
        assertRT(rt(7, 8, 3), iter.next());
        assertRT(rt(8, 10, 2), iter.next());
        assertRT(rt(10, 12, 1), iter.next());
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
        assertRT(rt(3, 5, 2), iter.next());
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
    public void purgetTest()
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);
        l.add(rt(0, 4, 5, 110));
        l.add(rt(4, 6, 2, 98));
        l.add(rt(9, 12, 1, 200));
        l.add(rt(14, 15, 3, 3));
        l.add(rt(15, 17, 6, 45));

        l.purge(100);

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rt(0, 4, 5, 110), iter.next());
        assertRT(rt(9, 12, 1, 200), iter.next());

        assert !iter.hasNext();
    }

    @Test
    public void minMaxTest()
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);
        l.add(rt(0, 4, 5, 110));
        l.add(rt(4, 6, 2, 98));
        l.add(rt(9, 12, 1, 200));
        l.add(rt(14, 15, 3, 3));
        l.add(rt(15, 17, 6, 45));

        assertEquals(1, l.minMarkedAt());
        assertEquals(6, l.maxMarkedAt());
    }

    @Test
    public void insertSameTest()
    {
        // Simple test that adding the same element multiple time ends up
        // with that element only a single time (CASSANDRA-9485)

        RangeTombstoneList l = new RangeTombstoneList(cmp, 0);
        l.add(rt(4, 4, 5, 100));
        l.add(rt(4, 4, 6, 110));
        l.add(rt(4, 4, 4, 90));

        Iterator<RangeTombstone> iter = l.iterator();
        assertRT(rt(4, 4, 6, 110), iter.next());
        assert !iter.hasNext();
    }

    private RangeTombstoneList makeRandom(Random rand, int size, int maxItSize, int maxItDistance, int maxMarkedAt)
    {
        RangeTombstoneList l = new RangeTombstoneList(cmp, size);

        int prevStart = -1;
        int prevEnd = 0;
        for (int i = 0; i < size; i++)
        {
            int nextStart = prevEnd + rand.nextInt(maxItDistance);
            int nextEnd = nextStart + rand.nextInt(maxItSize);

            // We can have an interval [x, x], but not 2 consecutives ones for the same x
            if (nextEnd == nextStart && prevEnd == prevStart && prevEnd == nextStart)
                nextEnd += 1 + rand.nextInt(maxItDistance);

            l.add(rt(nextStart, nextEnd, rand.nextInt(maxMarkedAt)));

            prevStart = nextStart;
            prevEnd = nextEnd;
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

    private static void assertRT(RangeTombstone expected, RangeTombstone actual)
    {
        assertEquals(String.format("Expected %s but got %s", toString(expected), toString(actual)), expected, actual);
    }

    private static void assertValid(RangeTombstoneList l)
    {
        // We check that ranges are in the right order and that we never have something
        // like ...[x, x][x, x] ...
        int prevStart = -2;
        int prevEnd = -1;
        for (RangeTombstone rt : l)
        {
            int curStart = i(rt.min);
            int curEnd = i(rt.max);

            assertTrue("Invalid " + toString(l), prevEnd <= curStart);
            assertTrue("Invalid " + toString(l), curStart <= curEnd);

            if (curStart == curEnd && prevEnd == curStart)
                assertTrue("Invalid " + toString(l), prevStart != prevEnd);

            prevStart = curStart;
            prevEnd = curEnd;
        }
    }

    private static String toString(RangeTombstone rt)
    {
        return String.format("[%d, %d]@%d", i(rt.min), i(rt.max), rt.data.markedForDeleteAt);
    }

    private static String toString(RangeTombstoneList l)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (RangeTombstone rt : l)
            sb.append(" ").append(toString(rt));
        return sb.append(" }").toString();
    }

    private static Composite b(int i)
    {
        return Util.cellname(i);
    }

    private static int i(Composite c)
    {
        return ByteBufferUtil.toInt(c.toByteBuffer());
    }

    private static RangeTombstone rt(int start, int end, long tstamp)
    {
        return rt(start, end, tstamp, 0);
    }

    private static RangeTombstone rt(int start, int end, long tstamp, int delTime)
    {
        return new RangeTombstone(b(start), b(end), tstamp, delTime);
    }
}
