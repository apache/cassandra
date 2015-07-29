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
package org.apache.cassandra.utils;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Iterables;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

import static org.junit.Assert.*;

public class BTreeTest
{
    static Integer[] ints = new Integer[20];
    static
    {
        System.setProperty("cassandra.btree.fanfactor", "4");
        for (int i = 0 ; i < ints.length ; i++)
            ints[i] = new Integer(i);
    }

    static final UpdateFunction<Integer, Integer> updateF = new UpdateFunction<Integer, Integer>()
    {
        public Integer apply(Integer replacing, Integer update)
        {
            return ints[update];
        }

        public boolean abortEarly()
        {
            return false;
        }

        public void allocated(long heapSize)
        {

        }

        public Integer apply(Integer integer)
        {
            return ints[integer];
        }
    };

    private static final UpdateFunction<Integer, Integer> noOp = new UpdateFunction<Integer, Integer>()
    {
        public Integer apply(Integer replacing, Integer update)
        {
            return update;
        }

        public boolean abortEarly()
        {
            return false;
        }

        public void allocated(long heapSize)
        {
        }

        public Integer apply(Integer k)
        {
            return k;
        }
    };

    private static List<Integer> seq(int count)
    {
        List<Integer> r = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
            r.add(i);
        return r;
    }

    private static List<Integer> rand(int count)
    {
        Random rand = ThreadLocalRandom.current();
        List<Integer> r = seq(count);
        for (int i = 0 ; i < count - 1 ; i++)
        {
            int swap = i + rand.nextInt(count - i);
            Integer tmp = r.get(i);
            r.set(i, r.get(swap));
            r.set(swap, tmp);
        }
        return r;
    }

    private static final Comparator<Integer> CMP = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    @Test
    public void testBuilding_UpdateFunctionReplacement()
    {
        for (int i = 0; i < 20 ; i++)
            checkResult(i, BTree.build(seq(i), updateF));
    }

    @Test
    public void testUpdate_UpdateFunctionReplacement()
    {
        for (int i = 0; i < 20 ; i++)
            checkResult(i, BTree.update(BTree.build(seq(i), noOp), CMP, seq(i), updateF));
    }

    /**
     * Tests that the apply method of the <code>UpdateFunction</code> is only called once with each key update.
     * (see CASSANDRA-8018).
     */
    @Test
    public void testUpdate_UpdateFunctionCallBack()
    {
        Object[] btree = new Object[1];
        CallsMonitor monitor = new CallsMonitor();

        btree = BTree.update(btree, CMP, Arrays.asList(1), monitor);
        assertArrayEquals(new Object[] {1}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));

        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(2), monitor);
        assertArrayEquals(new Object[] {1, 2, null}, btree);
        assertEquals(1, monitor.getNumberOfCalls(2));

        // with existing value
        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(1), monitor);
        assertArrayEquals(new Object[] {1, 2, null}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));

        // with two non-existing values
        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(3, 4), monitor);
        assertArrayEquals(new Object[] {1, 2, 3, 4, null}, btree);
        assertEquals(1, monitor.getNumberOfCalls(3));
        assertEquals(1, monitor.getNumberOfCalls(4));

        // with one existing value and one non existing value
        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(2, 5), monitor);
        assertArrayEquals(new Object[] {3, new Object[]{1, 2, null}, new Object[]{4, 5, null},  new int[]{2, 5}}, btree);
        assertEquals(1, monitor.getNumberOfCalls(2));
        assertEquals(1, monitor.getNumberOfCalls(5));
    }

    /**
     * Tests that the apply method of the <code>UpdateFunction</code> is only called once per value with each build call.
     */
    @Test
    public void testBuilding_UpdateFunctionCallBack()
    {
        CallsMonitor monitor = new CallsMonitor();
        Object[] btree = BTree.build(Arrays.asList(1), monitor);
        assertArrayEquals(new Object[] {1}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));

        monitor.clear();
        btree = BTree.build(Arrays.asList(1, 2), monitor);
        assertArrayEquals(new Object[] {1, 2, null}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));
        assertEquals(1, monitor.getNumberOfCalls(2));

        monitor.clear();
        btree = BTree.build(Arrays.asList(1, 2, 3), monitor);
        assertArrayEquals(new Object[] {1, 2, 3}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));
        assertEquals(1, monitor.getNumberOfCalls(2));
        assertEquals(1, monitor.getNumberOfCalls(3));
    }

    /**
     * Tests that the apply method of the <code>QuickResolver</code> is called exactly once per duplicate value
     */
    @Test
    public void testBuilder_QuickResolver()
    {
        // for numbers x in 1..N, we repeat x x times, and resolve values to their sum,
        // so that the resulting tree is of square numbers
        BTree.Builder.QuickResolver<Accumulator> resolver = (a, b) -> new Accumulator(a.base, a.sum + b.sum);

        for (int count = 0 ; count < 10 ; count ++)
        {
            BTree.Builder<Accumulator> builder;
            // first check we produce the right output for sorted input
            List<Accumulator> sorted = resolverInput(count, false);
            builder = BTree.builder(Comparator.naturalOrder());
            builder.setQuickResolver(resolver);
            for (Accumulator i : sorted)
                builder.add(i);
            // for sorted input, check non-resolve path works before checking resolution path
            checkResolverOutput(count, builder.build(), BTree.Dir.ASC);
            builder.reuse();
            for (int i = 0 ; i < 10 ; i++)
            {
                // now do a few runs of randomized inputs
                for (Accumulator j : resolverInput(count, true))
                    builder.add(j);
                checkResolverOutput(count, builder.build(), BTree.Dir.ASC);
                builder.reuse();
            }
            for (List<Accumulator> add : splitResolverInput(count))
            {
                if (ThreadLocalRandom.current().nextBoolean())
                    builder.addAll(add);
                else
                    builder.addAll(new TreeSet<>(add));
            }
            checkResolverOutput(count, builder.build(), BTree.Dir.ASC);
            builder.reuse();
        }
    }

    private static class Accumulator extends Number implements Comparable<Accumulator>
    {
        final int base;
        final int sum;
        private Accumulator(int base, int sum)
        {
            this.base = base;
            this.sum = sum;
        }

        public int compareTo(Accumulator that) { return Integer.compare(base, that.base); }
        public int intValue() { return sum; }
        public long longValue() { return sum; }
        public float floatValue() { return sum; }
        public double doubleValue() { return sum; }
    }

    /**
     * Tests that the apply method of the <code>Resolver</code> is called exactly once per unique value
     */
    @Test
    public void testBuilder_ResolverAndReverse()
    {
        // for numbers x in 1..N, we repeat x x times, and resolve values to their sum,
        // so that the resulting tree is of square numbers
        BTree.Builder.Resolver resolver = (array, lb, ub) -> {
            int sum = 0;
            for (int i = lb ; i < ub ; i++)
                sum += ((Accumulator) array[i]).sum;
            return new Accumulator(((Accumulator) array[lb]).base, sum);
        };

        for (int count = 0 ; count < 10 ; count ++)
        {
            BTree.Builder<Accumulator> builder;
            // first check we produce the right output for sorted input
            List<Accumulator> sorted = resolverInput(count, false);
            builder = BTree.builder(Comparator.naturalOrder());
            builder.auto(false);
            for (Accumulator i : sorted)
                builder.add(i);
            // for sorted input, check non-resolve path works before checking resolution path
            Assert.assertTrue(Iterables.elementsEqual(sorted, BTree.iterable(builder.build())));
            checkResolverOutput(count, builder.resolve(resolver).build(), BTree.Dir.ASC);
            builder = BTree.builder(Comparator.naturalOrder());
            builder.auto(false);
            for (int i = 0 ; i < 10 ; i++)
            {
                // now do a few runs of randomized inputs
                for (Accumulator j : resolverInput(count, true))
                    builder.add(j);
                checkResolverOutput(count, builder.sort().resolve(resolver).build(), BTree.Dir.ASC);
                builder.reuse();
                for (Accumulator j : resolverInput(count, true))
                    builder.add(j);
                checkResolverOutput(count, builder.sort().reverse().resolve(resolver).build(), BTree.Dir.DESC);
                builder.reuse();
            }
        }
    }

    private static List<Accumulator> resolverInput(int count, boolean shuffled)
    {
        List<Accumulator> result = new ArrayList<>();
        for (int i = 1 ; i <= count ; i++)
            for (int j = 0 ; j < i ; j++)
                result.add(new Accumulator(i, i));
        if (shuffled)
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0 ; i < result.size() ; i++)
            {
                int swapWith = random.nextInt(i, result.size());
                Accumulator t = result.get(swapWith);
                result.set(swapWith, result.get(i));
                result.set(i, t);
            }
        }
        return result;
    }

    private static List<List<Accumulator>> splitResolverInput(int count)
    {
        List<Accumulator> all = resolverInput(count, false);
        List<List<Accumulator>> result = new ArrayList<>();
        while (!all.isEmpty())
        {
            List<Accumulator> is = new ArrayList<>();
            int prev = -1;
            for (Accumulator i : new ArrayList<>(all))
            {
                if (i.base == prev)
                    continue;
                is.add(i);
                all.remove(i);
                prev = i.base;
            }
            result.add(is);
        }
        return result;
    }

    private static void checkResolverOutput(int count, Object[] btree, BTree.Dir dir)
    {
        int i = 1;
        for (Accumulator current : BTree.<Accumulator>iterable(btree, dir))
        {
            Assert.assertEquals(i * i, current.sum);
            i++;
        }
        Assert.assertEquals(i, count + 1);
    }

    private static void checkResult(int count, Object[] btree)
    {
        Iterator<Integer> iter = BTree.slice(btree, CMP, BTree.Dir.ASC);
        int i = 0;
        while (iter.hasNext())
            assertEquals(iter.next(), ints[i++]);
        assertEquals(count, i);
    }

    @Test
    public void testClearOnAbort()
    {
        Object[] btree = BTree.build(seq(2), noOp);
        Object[] copy = Arrays.copyOf(btree, btree.length);
        BTree.update(btree, CMP, seq(94), new AbortAfterX(90));

        assertArrayEquals(copy, btree);

        btree = BTree.update(btree, CMP, seq(94), noOp);
        assertTrue(BTree.isWellFormed(btree, CMP));
    }

    private static final class AbortAfterX implements UpdateFunction<Integer, Integer>
    {
        int counter;
        final int abortAfter;
        private AbortAfterX(int abortAfter)
        {
            this.abortAfter = abortAfter;
        }
        public Integer apply(Integer replacing, Integer update)
        {
            return update;
        }
        public boolean abortEarly()
        {
            return counter++ > abortAfter;
        }
        public void allocated(long heapSize)
        {
        }
        public Integer apply(Integer v)
        {
            return v;
        }
    }

    /**
     * <code>UpdateFunction</code> that count the number of call made to apply for each value.
     */
    public static final class CallsMonitor implements UpdateFunction<Integer, Integer>
    {
        private int[] numberOfCalls = new int[20];

        public Integer apply(Integer replacing, Integer update)
        {
            numberOfCalls[update] = numberOfCalls[update] + 1;
            return update;
        }

        public boolean abortEarly()
        {
            return false;
        }

        public void allocated(long heapSize)
        {

        }

        public Integer apply(Integer integer)
        {
            numberOfCalls[integer] = numberOfCalls[integer] + 1;
            return integer;
        }

        public int getNumberOfCalls(Integer key)
        {
            return numberOfCalls[key];
        }

        public void clear()
        {
            Arrays.fill(numberOfCalls, 0);
        }
    };
}
