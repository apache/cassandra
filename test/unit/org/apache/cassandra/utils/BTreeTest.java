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

import org.junit.Test;

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
        Object[] btree = new Object[0];
        CallsMonitor monitor = new CallsMonitor();

        btree = BTree.update(btree, CMP, Arrays.asList(1), monitor);
        assertArrayEquals(new Object[] {1, null}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));

        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(2), monitor);
        assertArrayEquals(new Object[] {1, 2}, btree);
        assertEquals(1, monitor.getNumberOfCalls(2));

        // with existing value
        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(1), monitor);
        assertArrayEquals(new Object[] {1, 2}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));

        // with two non-existing values
        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(3, 4), monitor);
        assertArrayEquals(new Object[] {1, 2, 3, 4}, btree);
        assertEquals(1, monitor.getNumberOfCalls(3));
        assertEquals(1, monitor.getNumberOfCalls(4));

        // with one existing value and one non existing value
        monitor.clear();
        btree = BTree.update(btree, CMP, Arrays.asList(2, 5), monitor);
        assertArrayEquals(new Object[] {3, new Object[]{1, 2}, new Object[]{4, 5}}, btree);
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
        assertArrayEquals(new Object[] {1, null}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));

        monitor.clear();
        btree = BTree.build(Arrays.asList(1, 2), monitor);
        assertArrayEquals(new Object[] {1, 2}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));
        assertEquals(1, monitor.getNumberOfCalls(2));

        monitor.clear();
        btree = BTree.build(Arrays.asList(1, 2, 3), monitor);
        assertArrayEquals(new Object[] {1, 2, 3, null}, btree);
        assertEquals(1, monitor.getNumberOfCalls(1));
        assertEquals(1, monitor.getNumberOfCalls(2));
        assertEquals(1, monitor.getNumberOfCalls(3));
    }

    private static void checkResult(int count, Object[] btree)
    {
        Iterator<Integer> iter = BTree.slice(btree, true);
        int i = 0;
        while (iter.hasNext())
            assertEquals(iter.next(), ints[i++]);
        assertFalse(iter.hasNext());
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
