/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.Assert;
import org.junit.Test;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.stats.Snapshot;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.btree.UpdateFunction;

// TODO : should probably lower fan-factor for tests to make them more intensive
public class LongBTreeTest
{

    private static final Timer BTREE_TIMER = Metrics.newTimer(BTree.class, "BTREE", TimeUnit.NANOSECONDS, TimeUnit.NANOSECONDS);
    private static final Timer TREE_TIMER = Metrics.newTimer(BTree.class, "TREE", TimeUnit.NANOSECONDS, TimeUnit.NANOSECONDS);
    private static final ExecutorService MODIFY = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("MODIFY"));
    private static final ExecutorService COMPARE = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("COMPARE"));
    private static final RandomAbort<Integer> SPORADIC_ABORT = new RandomAbort<>(new Random(), 0.0001f);

    static
    {
        System.setProperty("cassandra.btree.fanfactor", "4");
    }

    @Test
    public void testOversizedMiddleInsert()
    {
        TreeSet<Integer> canon = new TreeSet<>();
        for (int i = 0 ; i < 10000000 ; i++)
            canon.add(i);
        Object[] btree = BTree.build(Arrays.asList(Integer.MIN_VALUE, Integer.MAX_VALUE), ICMP, true, null);
        btree = BTree.update(btree, ICMP, canon, true);
        canon.add(Integer.MIN_VALUE);
        canon.add(Integer.MAX_VALUE);
        Assert.assertTrue(BTree.isWellFormed(btree, ICMP));
        testEqual("Oversize", BTree.<Integer>slice(btree, true), canon.iterator());
    }

    @Test
    public void testIndividualInsertsSmallOverlappingRange() throws ExecutionException, InterruptedException
    {
        testInsertions(10000000, 50, 1, 1, true);
    }

    @Test
    public void testBatchesSmallOverlappingRange() throws ExecutionException, InterruptedException
    {
        testInsertions(10000000, 50, 1, 5, true);
    }

    @Test
    public void testIndividualInsertsMediumSparseRange() throws ExecutionException, InterruptedException
    {
        testInsertions(10000000, 500, 10, 1, true);
    }

    @Test
    public void testBatchesMediumSparseRange() throws ExecutionException, InterruptedException
    {
        testInsertions(10000000, 500, 10, 10, true);
    }

    @Test
    public void testLargeBatchesLargeRange() throws ExecutionException, InterruptedException
    {
        testInsertions(100000000, 5000, 3, 100, true);
    }

    @Test
    public void testSlicingSmallRandomTrees() throws ExecutionException, InterruptedException
    {
        testInsertions(10000, 50, 10, 10, false);
    }

    private static void testInsertions(int totalCount, int perTestCount, int testKeyRatio, int modificationBatchSize, boolean quickEquality) throws ExecutionException, InterruptedException
    {
        int batchesPerTest = perTestCount / modificationBatchSize;
        int maximumRunLength = 100;
        int testKeyRange = perTestCount * testKeyRatio;
        int tests = totalCount / perTestCount;
        System.out.println(String.format("Performing %d tests of %d operations, with %.2f max size/key-range ratio in batches of ~%d ops",
                tests, perTestCount, 1 / (float) testKeyRatio, modificationBatchSize));

        // if we're not doing quick-equality, we can spam with garbage for all the checks we perform, so we'll split the work into smaller chunks
        int chunkSize = quickEquality ? tests : (int) (100000 / Math.pow(perTestCount, 2));
        for (int chunk = 0 ; chunk < tests ; chunk += chunkSize)
        {
            final List<ListenableFutureTask<List<ListenableFuture<?>>>> outer = new ArrayList<>();
            for (int i = 0 ; i < chunkSize ; i++)
            {
                outer.add(doOneTestInsertions(testKeyRange, maximumRunLength, modificationBatchSize, batchesPerTest, quickEquality));
            }

            final List<ListenableFuture<?>> inner = new ArrayList<>();
            int complete = 0;
            int reportInterval = totalCount / 100;
            int lastReportAt = 0;
            for (ListenableFutureTask<List<ListenableFuture<?>>> f : outer)
            {
                inner.addAll(f.get());
                complete += perTestCount;
                if (complete - lastReportAt >= reportInterval)
                {
                    System.out.println(String.format("Completed %d of %d operations", (chunk * perTestCount) + complete, totalCount));
                    lastReportAt = complete;
                }
            }
            Futures.allAsList(inner).get();
        }
        Snapshot snap = BTREE_TIMER.getSnapshot();
        System.out.println(String.format("btree   : %.2fns, %.2fns, %.2fns", snap.getMedian(), snap.get95thPercentile(), snap.get999thPercentile()));
        snap = TREE_TIMER.getSnapshot();
        System.out.println(String.format("snaptree: %.2fns, %.2fns, %.2fns", snap.getMedian(), snap.get95thPercentile(), snap.get999thPercentile()));
        System.out.println("Done");
    }

    private static ListenableFutureTask<List<ListenableFuture<?>>> doOneTestInsertions(final int upperBound, final int maxRunLength, final int averageModsPerIteration, final int iterations, final boolean quickEquality)
    {
        ListenableFutureTask<List<ListenableFuture<?>>> f = ListenableFutureTask.create(new Callable<List<ListenableFuture<?>>>()
        {
            @Override
            public List<ListenableFuture<?>> call()
            {
                final List<ListenableFuture<?>> r = new ArrayList<>();
                NavigableMap<Integer, Integer> canon = new TreeMap<>();
                Object[] btree = BTree.empty();
                final TreeMap<Integer, Integer> buffer = new TreeMap<>();
                final Random rnd = new Random();
                for (int i = 0 ; i < iterations ; i++)
                {
                    buffer.clear();
                    int mods = (averageModsPerIteration >> 1) + 1 + rnd.nextInt(averageModsPerIteration);
                    while (mods > 0)
                    {
                        int v = rnd.nextInt(upperBound);
                        int rc = Math.max(0, Math.min(mods, maxRunLength) - 1);
                        int c = 1 + (rc <= 0 ? 0 : rnd.nextInt(rc));
                        for (int j = 0 ; j < c ; j++)
                        {
                            buffer.put(v, v);
                            v++;
                        }
                        mods -= c;
                    }
                    TimerContext ctxt;
                    ctxt = TREE_TIMER.time();
                    canon.putAll(buffer);
                    ctxt.stop();
                    ctxt = BTREE_TIMER.time();
                    Object[] next = null;
                    while (next == null)
                        next = BTree.update(btree, ICMP, buffer.keySet(), true, SPORADIC_ABORT);
                    btree = next;
                    ctxt.stop();

                    if (!BTree.isWellFormed(btree, ICMP))
                    {
                        System.out.println("ERROR: Not well formed");
                        throw new AssertionError("Not well formed!");
                    }
                    if (quickEquality)
                        testEqual("", BTree.<Integer>slice(btree, true), canon.keySet().iterator());
                    else
                        r.addAll(testAllSlices("RND", btree, new TreeSet<>(canon.keySet())));
                }
                return r;
            }
        });
        MODIFY.execute(f);
        return f;
    }

    @Test
    public void testSlicingAllSmallTrees() throws ExecutionException, InterruptedException
    {
        Object[] cur = BTree.empty();
        TreeSet<Integer> canon = new TreeSet<>();
        // we set FAN_FACTOR to 4, so 128 items is four levels deep, three fully populated
        for (int i = 0 ; i < 128 ; i++)
        {
            String id = String.format("[0..%d)", canon.size());
            System.out.println("Testing " + id);
            Futures.allAsList(testAllSlices(id, cur, canon)).get();
            Object[] next = null;
            while (next == null)
                next = BTree.update(cur, ICMP, Arrays.asList(i), true, SPORADIC_ABORT);
            cur = next;
            canon.add(i);
        }
    }

    static final Comparator<Integer> ICMP = new Comparator<Integer>()
    {
        @Override
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    private static List<ListenableFuture<?>> testAllSlices(String id, Object[] btree, NavigableSet<Integer> canon)
    {
        List<ListenableFuture<?>> waitFor = new ArrayList<>();
        testAllSlices(id + " ASC", new BTreeSet<>(btree, ICMP), canon, true, waitFor);
        testAllSlices(id + " DSC", new BTreeSet<>(btree, ICMP).descendingSet(), canon.descendingSet(), false, waitFor);
        return waitFor;
    }

    private static void testAllSlices(String id, NavigableSet<Integer> btree, NavigableSet<Integer> canon, boolean ascending, List<ListenableFuture<?>> results)
    {
        testOneSlice(id, btree, canon, results);
        for (Integer lb : range(canon.size(), Integer.MIN_VALUE, ascending))
        {
            // test head/tail sets
            testOneSlice(String.format("%s->[%d..)", id, lb), btree.headSet(lb, true), canon.headSet(lb, true), results);
            testOneSlice(String.format("%s->(%d..)", id, lb), btree.headSet(lb, false), canon.headSet(lb, false), results);
            testOneSlice(String.format("%s->(..%d]", id, lb), btree.tailSet(lb, true), canon.tailSet(lb, true), results);
            testOneSlice(String.format("%s->(..%d]", id, lb), btree.tailSet(lb, false), canon.tailSet(lb, false), results);
            for (Integer ub : range(canon.size(), lb, ascending))
            {
                // test subsets
                testOneSlice(String.format("%s->[%d..%d]", id, lb, ub), btree.subSet(lb, true, ub, true), canon.subSet(lb, true, ub, true), results);
                testOneSlice(String.format("%s->(%d..%d]", id, lb, ub), btree.subSet(lb, false, ub, true), canon.subSet(lb, false, ub, true), results);
                testOneSlice(String.format("%s->[%d..%d)", id, lb, ub), btree.subSet(lb, true, ub, false), canon.subSet(lb, true, ub, false), results);
                testOneSlice(String.format("%s->(%d..%d)", id, lb, ub), btree.subSet(lb, false, ub, false), canon.subSet(lb, false, ub, false), results);
            }
        }
    }

    private static void testOneSlice(final String id, final NavigableSet<Integer> test, final NavigableSet<Integer> canon, List<ListenableFuture<?>> results)
    {
        ListenableFutureTask<?> f = ListenableFutureTask.create(new Runnable()
        {

            @Override
            public void run()
            {
                test(id + " Count", test.size(), canon.size());
                testEqual(id, test.iterator(), canon.iterator());
                testEqual(id + "->DSCI", test.descendingIterator(), canon.descendingIterator());
                testEqual(id + "->DSCS", test.descendingSet().iterator(), canon.descendingSet().iterator());
                testEqual(id + "->DSCS->DSCI", test.descendingSet().descendingIterator(), canon.descendingSet().descendingIterator());
            }
        }, null);
        results.add(f);
        COMPARE.execute(f);
    }

    private static void test(String id, int test, int expect)
    {
        if (test != expect)
        {
            System.out.println(String.format("%s: Expected %d, Got %d", id, expect, test));
        }
    }

    private static <V> void testEqual(String id, Iterator<V> btree, Iterator<V> canon)
    {
        boolean equal = true;
        while (btree.hasNext() && canon.hasNext())
        {
            Object i = btree.next();
            Object j = canon.next();
            if (!i.equals(j))
            {
                System.out.println(String.format("%s: Expected %d, Got %d", id, j, i));
                equal = false;
            }
        }
        while (btree.hasNext())
        {
            System.out.println(String.format("%s: Expected <Nil>, Got %d", id, btree.next()));
            equal = false;
        }
        while (canon.hasNext())
        {
            System.out.println(String.format("%s: Expected %d, Got Nil", id, canon.next()));
            equal = false;
        }
        if (!equal)
            throw new AssertionError("Not equal");
    }

    // should only be called on sets that range from 0->N or N->0
    private static final Iterable<Integer> range(final int size, final int from, final boolean ascending)
    {
        return new Iterable<Integer>()
        {
            int cur;
            int delta;
            int end;
            {
                if (ascending)
                {
                    end = size + 1;
                    cur = from == Integer.MIN_VALUE ? -1 : from;
                    delta = 1;
                }
                else
                {
                    end = -2;
                    cur = from == Integer.MIN_VALUE ? size : from;
                    delta = -1;
                }
            }
            @Override
            public Iterator<Integer> iterator()
            {
                return new Iterator<Integer>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return cur != end;
                    }

                    @Override
                    public Integer next()
                    {
                        Integer r = cur;
                        cur += delta;
                        return r;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    private static final class RandomAbort<V> implements UpdateFunction<V>
    {
        final Random rnd;
        final float chance;
        private RandomAbort(Random rnd, float chance)
        {
            this.rnd = rnd;
            this.chance = chance;
        }

        public V apply(V replacing, V update)
        {
            return update;
        }

        public boolean abortEarly()
        {
            return rnd.nextFloat() < chance;
        }

        public void allocated(long heapSize)
        {

        }

        public V apply(V v)
        {
            return v;
        }
    }

}
