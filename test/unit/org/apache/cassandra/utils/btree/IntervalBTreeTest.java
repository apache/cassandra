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

package org.apache.cassandra.utils.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Test;

import accord.utils.Invariants;

import static org.apache.cassandra.utils.btree.BTree.getChildCount;
import static org.apache.cassandra.utils.btree.BTree.getChildStart;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;

public class IntervalBTreeTest
{
    static class TestInterval implements Comparable<TestInterval>
    {
        final int start, end;
        final int value;

        TestInterval(int start, int end, int value)
        {
            this.start = start;
            this.end = end;
            this.value = value;
        }

        @Override
        public String toString()
        {
            return start + "," + end + ':' + value;
        }

        @Override
        public int compareTo(TestInterval that)
        {
            int c = Integer.compare(this.start, that.start);
            if (c == 0) c = Integer.compare(this.end, that.end);
            if (c == 0) c = Integer.compare(this.value, that.value);
            return c;
        }
    }

    static class TestComparators implements IntervalBTree.IntervalComparators<TestInterval>
    {
        static final TestComparators INSTANCE = new TestComparators();

        @Override
        public Comparator<TestInterval> totalOrder()
        {
            return TestInterval::compareTo;
        }

        @Override
        public Comparator<TestInterval> startWithStartComparator()
        {
            return (a, b) -> Integer.compare(a.start, b.start);
        }

        @Override
        public Comparator<TestInterval> startWithEndComparator()
        {
            return (a, b) -> Integer.compare(a.start, b.end);
        }

        @Override
        public Comparator<TestInterval> endWithStartComparator()
        {
            return (a, b) -> Integer.compare(a.end, b.start);
        }

        @Override
        public Comparator<TestInterval> endWithEndComparator()
        {
            return (a, b) -> Integer.compare(a.end, b.end);
        }
    }
    
    @Test
    public void testN()
    {
//        testOne(4391017837511000309L);
        Random seeds = new Random();
        for (int i = 0 ; i < 1000 ; ++i)
            testOne(seeds.nextLong());
    }

    public static void testOne(long seed)
    {
        try
        {
            List<TestInterval> list = new ArrayList<>();
            Random random = new Random();
            random.setSeed(seed);
            System.out.println(seed);

            int count = 1 << random.nextInt(11);
            int maxRemoveSize = count == 1 ? 1 : 1 + random.nextInt(count - 1);
            count = count + random.nextInt(count);

            TreeSet<TestInterval> unique = new TreeSet<>();
            for (int i = 0 ; i < count ; ++i)
            {
                TestInterval interval = newInterval(random, 0, 10000);
                if (unique.add(interval))
                    list.add(interval);
            }

            Object[] tree = BTree.empty();
            for (TestInterval v : list)
            {
                tree = IntervalBTree.update(tree, BTree.singleton(v), TestComparators.INSTANCE);
            }

            for (int i1 = 0 ; i1 < list.size() ; ++i1)
            {
                TestInterval iv = list.get(i1);
                TreeSet<TestInterval> collect = collect(list, 0, iv);
                remove(tree, collect, iv);
                Invariants.require(collect.isEmpty());
                iv = newInterval(random, 0, 10000);
                collect = collect(list, 0, iv);
                remove(tree, collect, iv);
                Invariants.require(collect.isEmpty());
            }

            Collections.shuffle(list, random);
            for (int i = 0 ; i < list.size() ;)
            {
                int remaining = list.size() - i;
                int c = remaining == 1 ? 1 : 1 + random.nextInt(Math.min(remaining, maxRemoveSize));

                TestInterval iv = list.get(i++);
                Object[] remove;
                {
                    remove = IntervalBTree.singleton(iv);
                    while (--c > 0)
                        remove = IntervalBTree.update(remove, IntervalBTree.singleton(list.get(i++)), TestComparators.INSTANCE);

                    int notPresentCount;
                    switch (random.nextInt(4))
                    {
                        default: throw new IllegalStateException();
                        case 0: notPresentCount = 0; break;
                        case 1: notPresentCount = random.nextInt(5); break;
                        case 2: notPresentCount = random.nextInt(Math.max(2, count/2)); break;
                        case 3: notPresentCount = random.nextInt(count*2); break;
                    }

                    while (--notPresentCount > 0)
                    {
                        TestInterval add = newInterval(random, 0, 10000);
                        if (!unique.contains(add))
                            remove = IntervalBTree.update(remove, IntervalBTree.singleton(add), TestComparators.INSTANCE);
                    }
                }
                tree = IntervalBTree.subtract(tree, remove, TestComparators.INSTANCE);
                validate(tree, TestComparators.INSTANCE.endWithEndComparator());
                TreeSet<TestInterval> collect = collect(list, i, iv);
                remove(tree, collect, iv);
                Invariants.require(collect.isEmpty());
                iv = newInterval(random, 0, 10000);
                collect = collect(list, i, iv);
                remove(tree, collect, iv);
                Invariants.require(collect.isEmpty());
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with seed " + seed, t);
        }
    }

    private static TreeSet<TestInterval> collect(List<TestInterval> list, int from, TestInterval intersects)
    {
        TreeSet<TestInterval> collect = new TreeSet<>();
        for (int i = from; i < list.size() ; ++i)
        {
            TestInterval iv2 = list.get(i);
            if (intersects.start < iv2.end && iv2.start < intersects.end)
                collect.add(iv2);
        }
        return collect;
    }

    private static void remove(Object[] tree, TreeSet<TestInterval> removeFrom, TestInterval intersects)
    {
        IntervalBTree.accumulate(tree, TestComparators.INSTANCE, intersects, (c, v, i, s) -> {
            Invariants.require(c.remove(i));
            return null;
        }, removeFrom, null, null);
    }

    private static TestInterval newInterval(Random random, int from, int to)
    {
        int end = 1 + from + random.nextInt(to - (1 + from));
        int start = from + random.nextInt(end - from);
        return new TestInterval(start, end, random.nextInt(10000));
    }

    static Object validate(Object[] tree, Comparator endSorter)
    {
        if (isLeaf(tree))
            return max(tree, endSorter);

        IntervalBTree.IntervalMaxIndex index = (IntervalBTree.IntervalMaxIndex) tree[tree.length - 1];
        Object[] tmp = new Object[getChildCount(tree)];
        for (int i = 0 ; i < index.sortedByEndIndex.length ; ++i)
            tmp[index.sortedByEndIndex[i]] = index.sortedByEnd[i];

        Object max = max(tree, endSorter);
        for (int i = 0 ; i < getChildCount(tree) ; ++i)
        {
            Object childMax = validate((Object[])tree[getChildStart(tree) + i], endSorter);
            if (endSorter.compare(childMax, max) > 0)
                max = childMax;
            Invariants.require(endSorter.compare(childMax, tmp[i]) == 0);
        }
        return max;
    }

    private static Object max(Object[] tree, Comparator endSorter)
    {
        Object max = tree[0];
        for (int i = 1; i < getKeyEnd(tree) ; ++i)
        {
            if (endSorter.compare(tree[i], max) > 0)
                max = tree[i];
        }
        return max;
    }

}
