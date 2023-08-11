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

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.btree.*;

import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;
import static org.apache.cassandra.config.CassandraRelevantProperties.BTREE_FAN_FACTOR;
import static org.apache.cassandra.utils.btree.BTree.iterable;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// TODO: randomise all parameters for all tests, with a target wall time for each iteration
//       should dedicate as much wall time to any depth of tree as any other, except depth 1 (which should be less frequent)
// TODO: verify update with no changes returns original
// TODO: verify updateF.allocated()
// TODO: verify reverseInSitu
// TODO: introduce patterns to verification, esp. to transform and update
public class LongBTreeTest
{
    private static final boolean DEBUG = false;
    private static int perThreadTrees = 10;
    private static int minTreeSize = 4;
    private static int maxTreeSize = 10000; // TODO randomise this for each test
    private static int threads = DEBUG ? 1 : Runtime.getRuntime().availableProcessors() * 8;
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Timer BTREE_TIMER = metrics.timer(MetricRegistry.name(BTree.class, "BTREE"));
    private static final Timer TREE_TIMER = metrics.timer(MetricRegistry.name(BTree.class, "TREE"));
    private static final ExecutorService MODIFY = Executors.newFixedThreadPool(threads, new NamedThreadFactory("MODIFY"));
    private static final ExecutorService COMPARE = DEBUG ? MODIFY : Executors.newFixedThreadPool(threads, new NamedThreadFactory("COMPARE"));

    /************************** TEST ACCESS ********************************************/

    @Test
    public void testSearchIterator() throws InterruptedException
    {
        final int perTreeSelections = 10; // TODO randomise this for each test
        testRandomSelection(randomSeed(), perThreadTrees, perTreeSelections,
        (test) -> {
            IndexedSearchIterator<Integer, Integer> iter1 = test.testAsSet.iterator();
            IndexedSearchIterator<Integer, Integer> iter2 = test.testAsList.iterator();
            return (key) ->
            {
                Integer found1 = iter1.hasNext() ? iter1.next(key) : null;
                Integer found2 = iter2.hasNext() ? iter2.next(key) : null;
                Assert.assertSame(found1, found2);
                if (found1 != null)
                    Assert.assertEquals(iter1.indexOfCurrent(), iter2.indexOfCurrent());

                int index = Collections.binarySearch(test.canonicalList, key, test.comparator);
                if (index < 0)
                {
                    Assert.assertNull(found1);
                }
                else
                {
                    Assert.assertEquals(key, found1);
                    Assert.assertEquals(index, iter1.indexOfCurrent());
                }

                // check that by advancing the same key again we get null, but only do it on one of the two iterators
                // to ensure they both advance differently
                if (test.random.nextBoolean())
                    Assert.assertNull(iter1.next(key));
                else
                    Assert.assertNull(iter2.next(key));
            };
        });
    }

    @Test
    public void testInequalityLookups() throws InterruptedException
    {
        final int perTreeSelections = 2;
        testRandomSelectionOfSet(randomSeed(), perThreadTrees, perTreeSelections,
                                 (test, canonical) -> {
                                     if (!canonical.isEmpty() || !test.isEmpty())
                                     {
                                         Assert.assertEquals(canonical.isEmpty(), test.isEmpty());
                                         Assert.assertEquals(canonical.first(), test.first());
                                         Assert.assertEquals(canonical.last(), test.last());
                                     }
                                     return (key) ->
                                     {
                                         Assert.assertEquals(test.ceiling(key), canonical.ceiling(key));
                                         Assert.assertEquals(test.higher(key), canonical.higher(key));
                                         Assert.assertEquals(test.floor(key), canonical.floor(key));
                                         Assert.assertEquals(test.lower(key), canonical.lower(key));
                                     };
                                 });
    }

    @Test
    public void testListIndexes() throws InterruptedException
    {
        testRandomSelectionOfList(randomSeed(), perThreadTrees, 4,
                                  (test, canonical, cmp) ->
                                  (key) ->
                                  {
                                      int javaIndex = Collections.binarySearch(canonical, key, cmp);
                                      int btreeIndex = test.indexOf(key);
                                      Assert.assertEquals(javaIndex, btreeIndex);
                                      if (javaIndex >= 0)
                                          Assert.assertEquals(canonical.get(javaIndex), test.get(btreeIndex));
                                  }
        );
    }

    @Test
    public void testToArray() throws InterruptedException
    {
        testRandomSelection(randomSeed(), perThreadTrees, 4,
                            (selection) ->
                            {
                                Integer[] array = new Integer[selection.canonicalList.size() + 1];
                                selection.testAsList.toArray(array, 1);
                                Assert.assertEquals(null, array[0]);
                                for (int j = 0; j < selection.canonicalList.size(); j++)
                                    Assert.assertEquals(selection.canonicalList.get(j), array[j + 1]);
                            });
    }

    private static final class CountingFunction implements Function<Integer, Integer>
    {
        final Function<Integer, Integer> wrapped;
        int count = 0;
        protected CountingFunction(Function<Integer, Integer> wrapped)
        {
            this.wrapped = wrapped;
        }
        public Integer apply(Integer integer)
        {
            count++;
            return wrapped.apply(integer);
        }
    }

    @Test
    public void testTransformAndFilterNone() throws InterruptedException
    {
        testRandomSelection(randomSeed(), perThreadTrees, 4, false, false, false,
                            (selection) ->
                            {
                                Map<Integer, Integer> update = new LinkedHashMap<>();
                                for (Integer i : selection.testKeys)
                                    update.put(i, Integer.valueOf(i));

                                CountingFunction function = new CountingFunction((x) -> x);
                                Object[] original = selection.testAsSet.tree();
                                Object[] transformed = BTree.transformAndFilter(original, function);

                                Assert.assertEquals(BTree.size(original), function.count);
                                assertTrue(BTree.<Integer>isWellFormed(transformed, naturalOrder()));
                                Assert.assertSame(original, transformed);
                            });
    }

    @Test
    public void testTransformAndFilterReplace() throws InterruptedException
    {
        testRandomSelection(randomSeed(), perThreadTrees, 4, false, false, false,
                            (selection) ->
                            {
                                Map<Integer, Integer> update = new LinkedHashMap<>();
                                for (Integer i : selection.testKeys)
                                    update.put(i, Integer.valueOf(i));

                                CountingFunction function = new CountingFunction((x) -> update.getOrDefault(x, x));
                                Object[] original = selection.testAsSet.tree();
                                Object[] transformed = BTree.transformAndFilter(original, function);

                                Assert.assertEquals(BTree.size(original), function.count);
                                assertTrue(BTree.<Integer>isWellFormed(transformed, naturalOrder()));
                                assertSame(transform(selection.canonicalList, function.wrapped::apply), iterable(transformed));
                            });
    }

    @Test
    public void testTransformAndFilterReplaceAndRemove() throws InterruptedException
    {
        testRandomSelection(randomSeed(), perThreadTrees, 4, false, false, false,
                            (selection) ->
                            {
                                Map<Integer, Integer> update = new LinkedHashMap<>();
                                for (Integer i : selection.testKeys)
                                    update.put(i, Integer.valueOf(i));

                                CountingFunction function = new CountingFunction(update::get);
                                Object[] original = selection.testAsSet.tree();
                                Object[] transformed = BTree.transformAndFilter(original, function);
                                Assert.assertEquals(BTree.size(original), function.count);
                                assertTrue(BTree.<Integer>isWellFormed(transformed, naturalOrder()));
                                assertSame(filter(transform(selection.canonicalList, function.wrapped::apply), notNull()), iterable(transformed));
                            });
    }

    @Test
    public void testTransformAndFilterRemove() throws InterruptedException
    {
        testRandomSelection(randomSeed(), perThreadTrees, 4, false, false, false,
                            (selection) ->
                            {
                                Map<Integer, Integer> update = new LinkedHashMap<>();
                                for (Integer i : selection.testKeys)
                                    update.put(i, Integer.valueOf(i));

                                CountingFunction function = new CountingFunction((x) -> update.containsKey(x) ? null : x);
                                Object[] original = selection.testAsSet.tree();
                                Object[] transformed = BTree.transformAndFilter(selection.testAsList.tree(), function);
                                Assert.assertEquals(BTree.size(original), function.count);
                                assertTrue(BTree.<Integer>isWellFormed(transformed, naturalOrder()));
//                                Assert.assertEquals(BTree.size(original) - update.size(), BTree.size(transformed));
                                assertSame(filter(transform(selection.canonicalList, function.wrapped::apply), notNull()), iterable(transformed));
                            });
    }

    private static void assertSame(Iterable<Integer> i1, Iterable<Integer> i2)
    {
        assertSame(i1.iterator(), i2.iterator());
    }

    private static void assertSame(Iterator<Integer> i1, Iterator<Integer> i2)
    {
        while (i1.hasNext() && i2.hasNext())
            Assert.assertSame(i1.next(), i2.next());
        Assert.assertEquals(i1.hasNext(), i2.hasNext());
    }

    private static Pair<Integer, Integer> firstDiff(Iterable<Integer> i1, Iterable<Integer> i2)
    {
        return firstDiff(i1.iterator(), i2.iterator());
    }

    private static Pair<Integer, Integer> firstDiff(Iterator<Integer> i1, Iterator<Integer> i2)
    {
        while (i1.hasNext() && i2.hasNext())
        {
            Integer v1 = i1.next();
            Integer v2 = i2.next();
            if (v1 != v2)
                return Pair.create(v1, v2);
        }
        return i1.hasNext() ? Pair.create(i1.next(), null) : i2.hasNext() ? Pair.create(null, i2.next()) : null;
    }

    private void testRandomSelectionOfList(long testSeed, int perThreadTrees, int perTreeSelections, BTreeListTestFactory testRun) throws InterruptedException
    {
        testRandomSelection(testSeed, perThreadTrees, perTreeSelections,
                            (BTreeTestFactory) (selection) -> testRun.get(selection.testAsList, selection.canonicalList, selection.comparator));
    }

    private void testRandomSelectionOfSet(long testSeed, int perThreadTrees, int perTreeSelections, BTreeSetTestFactory testRun) throws InterruptedException
    {
        testRandomSelection(testSeed, perThreadTrees, perTreeSelections,
                            (BTreeTestFactory) (selection) -> testRun.get(selection.testAsSet, selection.canonicalSet));
    }

    static interface BTreeSetTestFactory
    {
        TestEachKey get(BTreeSet<Integer> test, NavigableSet<Integer> canonical);
    }

    static interface BTreeListTestFactory
    {
        TestEachKey get(BTreeSet<Integer> test, List<Integer> canonical, Comparator<Integer> comparator);
    }

    static interface BTreeTestFactory
    {
        TestEachKey get(RandomSelection test);
    }

    static interface TestEachKey
    {
        void testOne(Integer value);
    }

    private void testRandomSelection(long seed, int perThreadTrees, int perTreeSelections, BTreeTestFactory testRun) throws InterruptedException
    {
        testRandomSelection(seed, perThreadTrees, perTreeSelections, (selection) -> {
            TestEachKey testEachKey = testRun.get(selection);
            for (Integer key : selection.testKeys)
                testEachKey.testOne(key);
        });
    }

    private void testRandomSelection(long seed, int perThreadTrees, int perTreeSelections, Consumer<RandomSelection> testRun) throws InterruptedException
    {
        testRandomSelection(seed, perThreadTrees, perTreeSelections, true, true, true, testRun);
    }

    private void testRandomSelection(long seed, int perThreadTrees, int perTreeSelections, boolean narrow, boolean mixInNotPresentItems, boolean permitReversal, Consumer<RandomSelection> testRun) throws InterruptedException
    {
        final Random outerSeedGenerator = new Random(seed);
        final CountDownLatch latch = new CountDownLatch(threads);
        final AtomicLong errors = new AtomicLong();
        final AtomicLong count = new AtomicLong();
        final long totalCount = threads * perThreadTrees * perTreeSelections;
        for (int t = 0 ; t < threads ; t++)
        {
            Runnable runnable = () -> {
                final Random seedGenerator = new Random(outerSeedGenerator.nextLong());
                try
                {
                    for (int i = 0 ; i < perThreadTrees ; i++)
                    {
                        long dataSeed = seedGenerator.nextLong();
                        RandomTree tree = randomTree(dataSeed, minTreeSize, maxTreeSize);
                        for (int j = 0 ; j < perTreeSelections ; j++)
                        {
                            long selectionSeed = seedGenerator.nextLong();
                            testRun.accept(tree.select(selectionSeed, narrow, mixInNotPresentItems, permitReversal));
                            count.incrementAndGet();
                        }
                    }
                }
                catch (Throwable t1)
                {
                    errors.incrementAndGet();
                    t1.printStackTrace();
                }
                latch.countDown();
            };
            MODIFY.execute(runnable);
        }
        while (latch.getCount() > 0)
        {
            for (int i = 0 ; i < 10L ; i++)
            {
                latch.await(1L, TimeUnit.SECONDS);
                Assert.assertEquals(0, errors.get());
            }
            log("%.1f%% complete %s", 100 * count.get() / (double) totalCount, errors.get() > 0 ? ("Errors: " + errors.get()) : "");
        }
    }

    private static class RandomSelection
    {
        final long dataSeed;
        final long selectionSeed;
        final Random random;
        final List<Integer> testKeys;
        final NavigableSet<Integer> canonicalSet;
        final List<Integer> canonicalList;
        final BTreeSet<Integer> testAsSet;
        final BTreeSet<Integer> testAsList;
        final Comparator<Integer> comparator;

        private RandomSelection(long dataSeed, long selectionSeed, Random random,
                                List<Integer> testKeys, NavigableSet<Integer> canonicalSet, BTreeSet<Integer> testAsSet,
                                List<Integer> canonicalList, BTreeSet<Integer> testAsList, Comparator<Integer> comparator)
        {
            this.dataSeed = dataSeed;
            this.selectionSeed = selectionSeed;
            this.random = random;
            this.testKeys = testKeys;
            this.canonicalList = canonicalList;
            this.canonicalSet = canonicalSet;
            this.testAsSet = testAsSet;
            this.testAsList = testAsList;
            this.comparator = comparator;
        }
    }

    private static class RandomTree
    {
        final long dataSeed;
        final NavigableSet<Integer> canonical;
        final BTreeSet<Integer> test;

        private RandomTree(long dataSeed, NavigableSet<Integer> canonical, BTreeSet<Integer> test)
        {
            this.dataSeed = dataSeed;
            this.canonical = canonical;
            this.test = test;
        }

        // TODO: revisit logic, document and ensure producing enough distinct patterns
        RandomSelection select(long selectionSeed, boolean narrow, boolean mixInNotPresentItems, boolean permitReversal)
        {
            Random random = new Random(selectionSeed);

            NavigableSet<Integer> canonicalSet = this.canonical;
            BTreeSet<Integer> testAsSet = this.test;
            List<Integer> canonicalList = new ArrayList<>(canonicalSet);
            BTreeSet<Integer> testAsList = this.test;

            Assert.assertEquals(canonicalSet.size(), testAsSet.size());
            Assert.assertEquals(canonicalList.size(), testAsList.size());

            // TODO: select random patterns of data as well as pure random data (i.e. random sequences, random fixed offsets, random mixes of the above)
            // sometimes select keys first, so we cover full range
            List<Integer> allKeys = randomKeys(random, canonical, mixInNotPresentItems);
            List<Integer> keys = allKeys;

            int narrowCount = random.nextInt(3);
            while (narrow && canonicalList.size() > 10 && keys.size() > 10 && narrowCount-- > 0)
            {
                boolean useLb = random.nextBoolean();
                boolean useUb = random.nextBoolean();
                if (!(useLb | useUb))
                    continue;

                // select a range smaller than the total span when we have more narrowing iterations left
                int indexRange = keys.size() / (narrowCount + 1);

                boolean lbInclusive = true;
                Integer lbKey = canonicalList.get(0);
                int lbKeyIndex = 0, lbIndex = 0;
                boolean ubInclusive = true;
                Integer ubKey = canonicalList.get(canonicalList.size() - 1);
                int ubKeyIndex = keys.size(), ubIndex = canonicalList.size();

                if (useLb)
                {
                    lbKeyIndex = nextInt(random, 0, indexRange - 1);
                    Integer candidate = keys.get(lbKeyIndex);
                    if (useLb = (candidate > lbKey && candidate <= ubKey))
                    {
                        lbInclusive = random.nextBoolean();
                        lbKey = keys.get(lbKeyIndex);
                        lbIndex = Collections.binarySearch(canonicalList, lbKey);
                        if (lbIndex >= 0 && !lbInclusive) lbIndex++;
                        else if (lbIndex < 0) lbIndex = -1 -lbIndex;
                    }
                }
                if (useUb)
                {
                    ubKeyIndex = nextInt(random, Math.max(lbKeyIndex, keys.size() - indexRange), keys.size() - 1);
                    Integer candidate = keys.get(ubKeyIndex);
                    if (useUb = (candidate < ubKey && candidate >= lbKey))
                    {
                        ubInclusive = random.nextBoolean();
                        ubKey = keys.get(ubKeyIndex);
                        ubIndex = Collections.binarySearch(canonicalList, ubKey);
                        if (ubIndex >= 0 && ubInclusive) { ubIndex++; }
                        else if (ubIndex < 0) ubIndex = -1 -ubIndex;
                    }
                }
                if (ubIndex < lbIndex) { ubIndex = lbIndex; ubKey = lbKey; ubInclusive = false; }

                canonicalSet = !useLb ? canonicalSet.headSet(ubKey, ubInclusive)
                                      : !useUb ? canonicalSet.tailSet(lbKey, lbInclusive)
                                               : canonicalSet.subSet(lbKey, lbInclusive, ubKey, ubInclusive);
                testAsSet = !useLb ? testAsSet.headSet(ubKey, ubInclusive)
                                   : !useUb ? testAsSet.tailSet(lbKey, lbInclusive)
                                            : testAsSet.subSet(lbKey, lbInclusive, ubKey, ubInclusive);

                keys = keys.subList(lbKeyIndex, ubKeyIndex);
                canonicalList = canonicalList.subList(lbIndex, ubIndex);
                testAsList = testAsList.subList(lbIndex, ubIndex);

                Assert.assertEquals(canonicalSet.size(), testAsSet.size());
                Assert.assertEquals(canonicalList.size(), testAsList.size());
            }

            // possibly restore full set of keys, to test case where we are provided existing keys that are out of bounds
            if (keys != allKeys && random.nextBoolean())
                keys = allKeys;

            Comparator<Integer> comparator = naturalOrder();
            if (permitReversal && random.nextBoolean())
            {
                if (allKeys != keys)
                    keys = new ArrayList<>(keys);
                if (canonicalSet != canonical)
                    canonicalList = new ArrayList<>(canonicalList);
                Collections.reverse(keys);
                Collections.reverse(canonicalList);
                testAsList = testAsList.descendingSet();

                canonicalSet = canonicalSet.descendingSet();
                testAsSet = testAsSet.descendingSet();
                comparator = reverseOrder();
            }

            Assert.assertEquals(canonicalSet.size(), testAsSet.size());
            Assert.assertEquals(canonicalList.size(), testAsList.size());
            if (!canonicalSet.isEmpty())
            {
                Assert.assertEquals(canonicalSet.first(), canonicalList.get(0));
                Assert.assertEquals(canonicalSet.last(), canonicalList.get(canonicalList.size() - 1));
                Assert.assertEquals(canonicalSet.first(), testAsSet.first());
                Assert.assertEquals(canonicalSet.last(), testAsSet.last());
                Assert.assertEquals(canonicalSet.first(), testAsList.get(0));
                Assert.assertEquals(canonicalSet.last(), testAsList.get(testAsList.size() - 1));
            }

            assertSame(canonicalList, testAsList);
            return new RandomSelection(dataSeed, selectionSeed, random, keys, canonicalSet, testAsSet, canonicalList, testAsList, comparator);
        }
    }

    private static int nextInt(Random random, int lb, int ub)
    {
        return lb >= ub ? lb : lb + random.nextInt(ub - lb);
    }

    private static RandomTree randomTree(long seed, int minSize, int maxSize)
    {
        Random random = new Random(seed);
        // perform most of our tree constructions via update, as this is more efficient; since every run uses this
        // we test builder disproportionately more often than if it had its own test anyway
        return random.nextFloat() < 0.95 ? randomTreeByUpdate(seed, random, minSize, maxSize)
                                         : randomTreeByBuilder(seed, random, minSize, maxSize);
    }

    private static RandomTree randomTreeByUpdate(long seed, Random random, int minSize, int maxSize)
    {
        assert minSize > 3;
        TreeSet<Integer> canonical = new TreeSet<>();

        int targetSize = nextInt(random, minSize, maxSize);
        int maxModificationSize = nextInt(random, 2, targetSize);
        Object[] accmumulate = BTree.empty();
        int curSize = 0;
        while (curSize < targetSize)
        {
            int nextSize = maxModificationSize == 1 ? 1 : nextInt(random, 1, maxModificationSize);
            TreeSet<Integer> build = new TreeSet<>();
            boolean keepOriginal = random.nextBoolean();
            // we don't use no-op, to ensure we know which value will actually result (as no-op doesn't guarantee which makes it through)
            UpdateFunction<Integer, Integer> updateF = keepOriginal ? UpdateFunction.Simple.of((a, b) -> a) : InverseNoOp.instance;
            for (int i = 0 ; i < nextSize ; i++)
            {
                Integer next = random.nextInt();
                if (build.add(next))
                {
                    if (!canonical.add(next) && !keepOriginal)
                    {
                        canonical.remove(next);
                        canonical.add(next);
                    }
                }
            }
            Object[] tmp = BTree.update(accmumulate, BTree.build(build), naturalOrder(), updateF);
            assertSame(canonical, BTreeSet.<Integer>wrap(tmp, naturalOrder()));
            accmumulate = tmp;
            curSize += nextSize;
            maxModificationSize = Math.min(maxModificationSize, targetSize - curSize);
        }
        assertSame(canonical, BTreeSet.<Integer>wrap(accmumulate, naturalOrder()));
        return new RandomTree(seed, canonical, BTreeSet.<Integer>wrap(accmumulate, naturalOrder()));
    }

    private static RandomTree randomTreeByBuilder(long seed, Random random, int minSize, int maxSize)
    {
        assert minSize > 3;
        BTree.Builder<Integer> builder = BTree.builder(naturalOrder());

        int targetSize = nextInt(random, minSize, maxSize);
        int maxModificationSize = (int) Math.sqrt(targetSize);

        TreeSet<Integer> canonical = new TreeSet<>();

        int curSize = 0;
        TreeSet<Integer> ordered = new TreeSet<>();
        List<Integer> shuffled = new ArrayList<>();
        while (curSize < targetSize)
        {
            int nextSize = nextInt(random, 1, maxModificationSize);

            // leave a random selection of previous values
            (random.nextBoolean() ? ordered.headSet(random.nextInt()) : ordered.tailSet(random.nextInt())).clear();
            shuffled = new ArrayList<>(shuffled.subList(0, shuffled.size() < 2 ? 0 : random.nextInt(shuffled.size() / 2)));

            for (int i = 0 ; i < nextSize ; i++)
            {
                Integer next = random.nextInt();
                ordered.add(next);
                shuffled.add(next);
                canonical.add(next);
            }

            switch (random.nextInt(5))
            {
                case 0:
                    builder.addAll(ordered);
                    break;
                case 1:
                    builder.addAll(BTreeSet.of(ordered));
                    break;
                case 2:
                    for (Integer i : ordered)
                        builder.add(i);
                case 3:
                    builder.addAll(shuffled);
                    break;
                case 4:
                    for (Integer i : shuffled)
                        builder.add(i);
            }

            curSize += nextSize;
            maxModificationSize = Math.min(maxModificationSize, targetSize - curSize);
        }

        BTreeSet<Integer> btree = BTreeSet.<Integer>wrap(builder.build(), naturalOrder());
        Assert.assertEquals(canonical.size(), btree.size());
        assertSame(canonical, btree);
        return new RandomTree(seed, canonical, btree);
    }

    // select a random subset of the keys, with an optional random population of keys inbetween those that are present
    // return a value with the search position
    private static List<Integer> randomKeys(Random random, Iterable<Integer> canonical, boolean mixInNotPresentItems)
    {
        boolean useFake = mixInNotPresentItems && random.nextBoolean();
        final float fakeRatio = random.nextFloat();
        List<Integer> results = new ArrayList<>();
        Long fakeLb = (long) Integer.MIN_VALUE, fakeUb = null;
        Integer max = null;
        for (Integer v : canonical)
        {
            if (    !useFake
                ||  (fakeUb == null ? v - 1 : fakeUb) <= fakeLb + 1
                ||  random.nextFloat() < fakeRatio)
            {
                // if we cannot safely construct a fake value, or our randomizer says not to, we emit the next real value
                results.add(v);
                fakeLb = v.longValue();
                fakeUb = null;
            }
            else
            {
                // otherwise we emit a fake value in the range immediately proceeding the last real value, and not
                // exceeding the real value that would have proceeded (ignoring any other suppressed real values since)
                if (fakeUb == null)
                    fakeUb = v.longValue() - 1;
                long mid = (fakeLb + fakeUb) / 2;
                assert mid < fakeUb;
                results.add((int) mid);
                fakeLb = mid;
            }
            max = v;
        }
        if (useFake && max != null && max < Integer.MAX_VALUE)
            results.add(max + 1);
        final float useChance = random.nextFloat();
        return Lists.newArrayList(filter(results, (x) -> random.nextFloat() < useChance));
    }

    /************************** TEST BUILD ********************************************/

    @Test
    public void testBuild()
    {
        Integer[] vs = IntStream.rangeClosed(0, 100000).boxed().toArray(Integer[]::new);
        for (UpdateFunction<Integer, Integer> updateF : LongBTreeTest.updateFunctions())
        {
            try (BulkIterator<Integer> emptyIter = BulkIterator.of(vs))
            {
                Object[] empty = BTree.build(emptyIter, 0, updateF);
                assertTrue("" + 0, BTree.isEmpty(empty)); // empty is tested by object identity, so verify we test correctly
            }
            for (int i = 0 ; i < vs.length ; ++i)
            {
                try (BulkIterator<Integer> iter = BulkIterator.of(vs))
                {
                    Object[] btree = BTree.build(iter, i + 1, updateF);
                    assertTrue("" + i, BTree.<Integer>isWellFormed(btree, naturalOrder()));
                }
            }
        }
    }

    @Test
    public void testFastBuilder()
    {
        Integer[] vs = IntStream.rangeClosed(0, 100000).boxed().toArray(Integer[]::new);
        try (BTree.FastBuilder<Integer> builder = BTree.fastBuilder())
        {
            Object[] empty = builder.build();
            assertTrue("" + 0, BTree.isEmpty(empty)); // empty is tested by object identity, so verify we test correctly
        }
        for (int i = 0 ; i < vs.length ; ++i)
        {
            try (BTree.FastBuilder<Integer> builder = BTree.fastBuilder())
            {
                for (int j = 0 ; j <= i ; ++j)
                    builder.add(vs[j]);
                Object[] btree = builder.build();
                assertEquals(i + 1, BTree.size(btree));
                assertTrue(""+i, BTree.<Integer>isWellFormed(btree, naturalOrder()));
            }
        }
    }

    @Test
    public void testBuildByUpdate()
    {
        Integer[] vs = IntStream.rangeClosed(0, 100000).boxed().toArray(Integer[]::new);
        Object[] base = BTree.singleton(vs[0]);
        for (int i = 0 ; i < vs.length ; ++i)
        {
            try (BulkIterator<Integer> iter = BulkIterator.of(vs))
            {
                Object[] insert = BTree.build(iter, i + 1, UpdateFunction.noOp());
                Object[] btree = BTree.<Integer, Integer, Integer>update(base, insert, naturalOrder(), InverseNoOp.instance);
                assertTrue("" + i, BTree.<Integer>isWellFormed(btree, naturalOrder()));
            }
        }
    }


    /************************** TEST MUTATION ********************************************/

    @Test
    public void testOversizedMiddleInsert()
    {
        for (UpdateFunction<Integer, Integer> updateF : LongBTreeTest.updateFunctions())
        {
            TreeSet<Integer> canon = new TreeSet<>();
            for (int i = 0 ; i < 10000000 ; i++)
                canon.add(i);
            Object[] btree = BTree.build(Arrays.asList(Integer.MIN_VALUE, Integer.MAX_VALUE), updateF);
            btree = BTree.update(btree, BTree.build(canon), naturalOrder(), updateF);
            canon.add(Integer.MIN_VALUE);
            canon.add(Integer.MAX_VALUE);
            assertTrue(BTree.<Integer>isWellFormed(btree, naturalOrder()));
            testEqual("Oversize", BTree.iterator(btree), canon.iterator());
        }
    }

    @Test
    public void testIndividualInsertsSmallOverlappingRange() throws ExecutionException, InterruptedException
    {
        testInsertions(randomSeed(), 50, 1, 1, true);
    }

    @Test
    public void testBatchesSmallOverlappingRange() throws ExecutionException, InterruptedException
    {
        testInsertions(randomSeed(), 50, 1, 5, true);
    }

    @Test
    public void testIndividualInsertsMediumSparseRange() throws ExecutionException, InterruptedException
    {
        testInsertions(randomSeed(), 500, 10, 1, true);
    }

    @Test
    public void testBatchesMediumSparseRange() throws ExecutionException, InterruptedException
    {
        testInsertions(randomSeed(), 500, 10, 10, true);
    }

    @Test
    public void testLargeBatchesLargeRange() throws ExecutionException, InterruptedException
    {
        testInsertions(randomSeed(), Math.max(maxTreeSize, 5000), 3, 100, true);
    }

    @Test
    public void testRandomRangeAndBatches() throws ExecutionException, InterruptedException
    {
        Random seedGenerator = new Random(randomSeed());
        for (int i = 0 ; i < 10 ; i++)
        {
            int treeSize = nextInt(seedGenerator, maxTreeSize / 10, maxTreeSize * 10);
            testInsertions(seedGenerator.nextLong(), treeSize, nextInt(seedGenerator, 1, 100) / 10f, treeSize / 100, true);
        }
    }

    @Test
    public void testSlicingSmallRandomTrees() throws ExecutionException, InterruptedException
    {
        testInsertions(randomSeed(), 50, 10, 10, false);
    }

    private static void testInsertions(long seed, int perTestCount, float testKeyRatio, int modificationBatchSize, boolean quickEquality) throws ExecutionException, InterruptedException
    {
        int tests = perThreadTrees * threads;
        testInsertions(seed, tests, perTestCount, testKeyRatio, modificationBatchSize, quickEquality);
    }

    private static void testInsertions(long seed, int tests, int perTestCount, float testKeyRatio, int modificationBatchSize, boolean quickEquality) throws ExecutionException, InterruptedException
    {
        Random random = new Random(seed);
        int batchesPerTest = perTestCount / modificationBatchSize;
        int testKeyRange = (int) (perTestCount * testKeyRatio);
        long totalCount = (long) perTestCount * tests;
        log("Performing %d tests of %d operations, with %.2f max size/key-range ratio in batches of ~%d ops",
            tests, perTestCount, 1 / testKeyRatio, modificationBatchSize);

        // if we're not doing quick-equality, we can spam with garbage for all the checks we perform, so we'll split the work into smaller chunks
        int chunkSize = quickEquality ? tests : (int) (100000 / Math.pow(perTestCount, 2));
        for (int chunk = 0 ; chunk < tests ; chunk += chunkSize)
        {
            final List<ListenableFutureTask<List<ListenableFuture<?>>>> outer = new ArrayList<>();
            for (int i = 0 ; i < chunkSize ; i++)
            {
                int maxRunLength = modificationBatchSize == 1 ? 1 : nextInt(random, 1, modificationBatchSize);
                outer.add(doOneTestInsertions(random.nextLong(), testKeyRange, maxRunLength, modificationBatchSize, batchesPerTest, quickEquality));
            }

            final List<ListenableFuture<?>> inner = new ArrayList<>();
            long complete = 0;
            int reportInterval = Math.max(1000, (int) (totalCount / 10000));
            long lastReportAt = 0;
            for (ListenableFutureTask<List<ListenableFuture<?>>> f : outer)
            {
                inner.addAll(f.get());
                complete += perTestCount;
                if (complete - lastReportAt >= reportInterval)
                {
                    long done = (chunk * perTestCount) + complete;
                    float ratio = done / (float) totalCount;
                    log("Completed %.1f%% (%d of %d operations)", ratio * 100, done, totalCount);
                    lastReportAt = complete;
                }
            }
            Futures.allAsList(inner).get();
        }
        Snapshot snap = BTREE_TIMER.getSnapshot();
        log("btree: %.2fns, %.2fns, %.2fns", snap.getMedian(), snap.get95thPercentile(), snap.get999thPercentile());
        snap = TREE_TIMER.getSnapshot();
        log("java: %.2fns, %.2fns, %.2fns", snap.getMedian(), snap.get95thPercentile(), snap.get999thPercentile());
        log("Done");
    }

    @Test
    public void debug()
    {
        randomTree(384037044131282656L, 4, 10000);
    }

    private static ListenableFutureTask<List<ListenableFuture<?>>> doOneTestInsertions(long seed, final int upperBound, final int maxRunLength, final int averageModsPerIteration, final int iterations, final boolean quickEquality)
    {
        String id = String.format("<%dL,%d,%d,%d,%d,%b>", seed, upperBound, maxRunLength, averageModsPerIteration, iterations, quickEquality);
        Random random = new Random(seed);
        ListenableFutureTask<List<ListenableFuture<?>>> f = ListenableFutureTask.create(() -> {
            try
            {
                final List<ListenableFuture<?>> r = new ArrayList<>();
                NavigableMap<Integer, Integer> canon = new TreeMap<>();
                Object[] btree = BTree.empty();
                final TreeMap<Integer, Integer> buffer = new TreeMap<>();
                for (int i = 0 ; i < iterations ; i++)
                {
                    buffer.clear();
                    int mods = nextInt(random, 1, averageModsPerIteration * 2);
                    while (mods > 0)
                    {
                        int v = random.nextInt(upperBound);
                        int rc = Math.max(0, Math.min(mods, maxRunLength) - 1);
                        int c = 1 + (rc <= 0 ? 0 : random.nextInt(rc));
                        for (int j = 0 ; j < c ; j++)
                        {
                            buffer.put(v, v);
                            v++;
                        }
                        mods -= c;
                    }
                    Timer.Context ctxt;
                    ctxt = TREE_TIMER.time();
                    canon.putAll(buffer);
                    ctxt.stop();
                    ctxt = BTREE_TIMER.time();
                    Object[] add = BTree.build(buffer.keySet());
                    Object[] newTree = BTree.update(btree, add, naturalOrder(), updateFunction(random));
                    ctxt.stop();

                    if (!BTree.<Integer>isWellFormed(newTree, naturalOrder()))
                    {
                        log(id + " ERROR: Not well formed");
                        throw new AssertionError("Not well formed!");
                    }
                    btree = newTree;
                    if (quickEquality)
                        testEqual(id, BTree.iterator(btree), canon.keySet().iterator());
                    else
                        r.addAll(testAllSlices(id, btree, new TreeSet<>(canon.keySet())));
                }
                return r;
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                log("Failed %s: %s", id, t.getMessage());
                throw t;
            }
        });
        if (DEBUG)
            f.run();
        else
            MODIFY.execute(f);
        return f;
    }

    @Test
    public void testSlicingAllSmallTrees() throws ExecutionException, InterruptedException
    {
        for (UpdateFunction<Integer, Integer> updateF : LongBTreeTest.<Integer>updateFunctions())
        {
            Object[] cur = BTree.empty();
            TreeSet<Integer> canon = new TreeSet<>();
            // we set FAN_FACTOR to 4, so 128 items is four levels deep, three fully populated
            for (int i = 0 ; i < 128 ; i++)
            {
                String id = String.format("[0..%d)", canon.size());
                log("Testing " + id);
                Futures.allAsList(testAllSlices(id, cur, canon)).get();
                Object[] next = null;
                while (next == null)
                    next = BTree.update(cur, BTree.singleton(i), naturalOrder(), updateF);
                cur = next;
                canon.add(i);
            }
        }
    }

    private static List<ListenableFuture<?>> testAllSlices(String id, Object[] btree, NavigableSet<Integer> canon)
    {
        List<ListenableFuture<?>> waitFor = new ArrayList<>();
        testAllSlices(id + " ASC", new BTreeSet<>(btree, naturalOrder()), canon, true, waitFor);
        testAllSlices(id + " DSC", new BTreeSet<Integer>(btree, naturalOrder()).descendingSet(), canon.descendingSet(), false, waitFor);
        return waitFor;
    }

    private static void testAllSlices(String id, NavigableSet<Integer> btree, NavigableSet<Integer> canon, boolean ascending, List<ListenableFuture<?>> results)
    {
        testOneSlice(id, btree, canon, results);
        for (Integer lb : range(canon.size(), Integer.MIN_VALUE, ascending))
        {
            // test head/tail sets
            testOneSlice(String.format("%s->[..%d)", id, lb), btree.headSet(lb, true), canon.headSet(lb, true), results);
            testOneSlice(String.format("%s->(..%d)", id, lb), btree.headSet(lb, false), canon.headSet(lb, false), results);
            testOneSlice(String.format("%s->(%d..]", id, lb), btree.tailSet(lb, true), canon.tailSet(lb, true), results);
            testOneSlice(String.format("%s->(%d..]", id, lb), btree.tailSet(lb, false), canon.tailSet(lb, false), results);
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
        if (DEBUG)
            f.run();
        else
            COMPARE.execute(f);
    }

    private static void test(String id, int test, int expect)
    {
        if (test != expect)
        {
            log("%s: Expected %d, Got %d", id, expect, test);
        }
    }

    private static <V> void testEqual(String id, Iterator<V> btree, Iterator<V> canon)
    {
        boolean equal = true;
        while (btree.hasNext() && canon.hasNext())
        {
            Object i = btree.next();
            Object j = canon.next();
            if (!Objects.equals(i, j))
            {
                log("%s: Expected %d, Got %d", id, j, i);
                equal = false;
            }
        }
        while (btree.hasNext())
        {
            log("%s: Expected <Nil>, Got %d", id, btree.next());
            equal = false;
        }
        while (canon.hasNext())
        {
            log("%s: Expected %d, Got Nil", id, canon.next());
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

    private static List<UpdateFunction<Integer, Integer>> updateFunctions()
    {
        return ImmutableList.of(UpdateFunction.noOp(), InverseNoOp.instance);
    }

    private static UpdateFunction<Integer, Integer> updateFunction(Random random)
    {
        return random.nextBoolean() ? InverseNoOp.instance : UpdateFunction.noOp();
    }

    public static final class InverseNoOp<V> implements UpdateFunction<V, V>
    {
        public static final InverseNoOp instance = new InverseNoOp();
        public V merge(V replacing, V update)
        {
            return update;
        }
        public void onAllocatedOnHeap(long heapSize)
        {
        }
        public V insert(V v)
        {
            return v;
        }
        public V retain(V v)
        {
            return v;
        }
    }

    private static long randomSeed()
    {
        return new SecureRandom().nextLong();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, InvocationTargetException, IllegalAccessException
    {
        for (String arg : args)
        {
            if (arg.startsWith("fan="))
                BTREE_FAN_FACTOR.setString(arg.substring(4));
            else if (arg.startsWith("min="))
                minTreeSize = Integer.parseInt(arg.substring(4));
            else if (arg.startsWith("max="))
                maxTreeSize = Integer.parseInt(arg.substring(4));
            else if (arg.startsWith("count="))
                perThreadTrees = Integer.parseInt(arg.substring(6));
            else
                exit();
        }

        List<Method> methods = new ArrayList<>();
        for (Method m : LongBTreeTest.class.getDeclaredMethods())
        {
            if (m.getParameters().length > 0)
                continue;
            for (Annotation annotation : m.getAnnotations())
                if (annotation.annotationType() == Test.class)
                    methods.add(m);
        }

        LongBTreeTest test = new LongBTreeTest();
        Collections.sort(methods, (a, b) -> a.getName().compareTo(b.getName()));
        log(Lists.transform(methods, (m) -> m.getName()).toString());
        for (Method m : methods)
        {
            log(m.getName());
            m.invoke(test);
        }
        log("success");
    }

    private static void exit()
    {
        log("usage: fan=<int> min=<int> max=<int> count=<int>");
        log("fan:   btree fanout");
        log("min:   minimum btree size (must be >= 4)");
        log("max:   maximum btree size (must be >= 4)");
        log("count: number of trees to assign each core, for each test");
    }

    private static void log(String formatstr, Object ... args)
    {
        args = Arrays.copyOf(args, args.length + 1);
        System.arraycopy(args, 0, args, 1, args.length - 1);
        args[0] = currentTimeMillis();
        System.out.printf("%tT: " + formatstr + "\n", args);
    }
}