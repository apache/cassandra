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

import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.SymmetricComparator;
import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.utils.btree.BTree.getChildCount;
import static org.apache.cassandra.utils.btree.BTree.getChildStart;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.endWithStart;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.startWithEnd;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.startWithStart;

public class IntervalBTreeTest
{
    static
    {
        // build deeper trees to explore more behaviours
        CassandraRelevantProperties.BTREE_BRANCH_SHIFT.setInt(3);
    }

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

        @Override public Comparator<TestInterval> totalOrder() { return TestInterval::compareTo; }
        @Override public SymmetricComparator<TestInterval> endWithEndSorter() { return (a, b) -> Integer.compare(a.end, b.end); }
        @Override public SymmetricComparator<TestInterval> startWithStartSeeker() { return (a, b) -> startWithStart(Integer.compare(a.start, b.start)); }
        @Override public SymmetricComparator<TestInterval> startWithEndSeeker() { return (a, b) -> startWithEnd(Integer.compare(a.start, b.end)); }
        @Override public SymmetricComparator<TestInterval> endWithStartSeeker() { return (a, b) -> endWithStart(Integer.compare(a.end, b.start)); }
    }
    
    @Test
    public void testN()
    {
        testOne(7817231705170378212L);
        Random seeds = new Random();
        for (int i = 0 ; i < 200 ; ++i)
            testOne(seeds.nextLong());
    }

    // TODO (expected): test extreme patterns
    public static void testOne(long seed)
    {
        try
        {
            List<TestInterval> tmp = new ArrayList<>();
            List<TestInterval> list = new ArrayList<>();
            RandomSource random = new DefaultRandom();
            random.setSeed(seed);
            System.out.println(seed);

            int count = random.nextFloat() < 0.001 ? random.nextInt(1, 4) : 4 << random.nextInt(10);
            int keyDomain = count == 1 ? 2 : 1 + log2uniform(random, count);
            int valueDomain = count == 1 ? 1 : random.nextInt((int)Math.sqrt(count), count);
            int maxRemoveSize = log2uniform(random, count);
            int maxRunLength = log2uniform(random, count);
            float runChance = random.nextFloat();
            count = count + random.nextInt(count);

            TreeSet<TestInterval> unique = new TreeSet<>();
            for (int i = 0 ; i < count ; ++i)
            {
                TestInterval interval = newInterval(random, keyDomain, valueDomain);
                if (unique.add(interval))
                    list.add(interval);

                if (maxRunLength > 1 && random.decide(runChance) && count - i > 1)
                {
                    boolean startRun = random.nextBoolean();
                    int runLength = random.nextBoolean() ? log2uniform(random, maxRunLength - 1)
                                                         : random.nextInt(0, maxRunLength - 1);
                    runLength = Math.min(runLength, count - (i + 1));
                    runLength = Math.min(runLength, startRun ? keyDomain - (1 + interval.start) : interval.end);
                    while (--runLength >= 0)
                    {
                        int start = startRun ? interval.start : random.nextInt(interval.end);
                        int end = startRun ? random.nextInt(interval.start + 1, keyDomain) : interval.end;
                        int value = random.nextInt(valueDomain);
                        TestInterval interval2 = new TestInterval(start, end, value);
                        if (unique.add(interval2))
                        {
                            list.add(interval2);
                            ++i;
                        }
                    }
                }
            }

            Object[] tree = BTree.empty();
            if (random.nextBoolean())
            {
                for (TestInterval v : list)
                {
                    tree = IntervalBTree.update(tree, BTree.singleton(v), TestComparators.INSTANCE);
                }
            }
            else
            {
                tmp.addAll(list);
                tmp.sort(TestComparators.INSTANCE.totalOrder());
                try (BTree.FastBuilder<TestInterval> builder = IntervalBTree.fastBuilder(TestComparators.INSTANCE))
                {
                    for (TestInterval add : tmp)
                        builder.add(add);
                    tree = builder.build();
                }
            }
            validate(tree, TestComparators.INSTANCE.endWithEndSorter());

            for (int i = 0 ; i < list.size() ; ++i)
            {
                TestInterval iv = list.get(i);
                TreeSet<TestInterval> collect = collect(list, 0, iv);
                remove(tree, collect, iv);
                Invariants.require(collect.isEmpty());
                iv = newInterval(random, keyDomain, valueDomain);
                collect = collect(list, 0, iv);
                remove(tree, collect, iv);
                Invariants.require(collect.isEmpty());
            }

            Collections.shuffle(list, random.asJdkRandom());
            for (int i = 0 ; i < list.size() ;)
            {
                int remaining = list.size() - i;
                int removeCount = remaining == 1 ? 1 : 1 + random.nextInt(Math.min(remaining, maxRemoveSize));
                remaining -= removeCount;

                TestInterval iv = list.get(i++);
                Object[] remove;
                {
                    tmp.clear();
                    tmp.add(iv);
                    int c = removeCount;
                    while (--c > 0)
                        tmp.add(list.get(i++));

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
                        TestInterval add = newInterval(random, keyDomain, valueDomain);
                        if (!unique.contains(add))
                            tmp.add(add);
                    }

                    tmp.sort(TestComparators.INSTANCE.totalOrder());
                    try (BTree.FastBuilder<TestInterval> builder = IntervalBTree.fastBuilder(TestComparators.INSTANCE))
                    {
                        for (TestInterval add : tmp)
                            builder.add(add);
                        remove = builder.build();
                    }
                }
                tree = IntervalBTree.subtract(tree, remove, TestComparators.INSTANCE);
                validate(tree, TestComparators.INSTANCE.endWithEndSorter());
                TreeSet<TestInterval> collect = collect(list, i, iv);
                remove(tree, collect, iv);
                Invariants.require(collect.isEmpty());
                iv = newInterval(random, keyDomain, valueDomain);
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

    private static int log2uniform(RandomSource random, int max)
    {
        int logn = 31 - Integer.numberOfLeadingZeros(max);
        int loglogn = 31 - Integer.numberOfLeadingZeros(logn);
        int scale = 1 << random.nextInt(loglogn, logn);
        if (scale == max)
            return max;
        return random.nextInt(scale, Math.min(scale * 2, max));
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

    private static TestInterval newInterval(RandomSource random, int keyDomain, int valueDomain)
    {
        int end = 1 + random.nextInt(keyDomain - 1);
        int start = random.nextInt(end);
        return new TestInterval(start, end, random.nextInt(valueDomain));
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
            if (i < getChildStart(tree) && endSorter.compare(childMax, tree[i]) < 0)
                childMax = tree[i];
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
