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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.btree.UpdateFunction;

public class BTreeTest
{

    static Integer[] ints = new Integer[20];
    static
    {
        System.setProperty("cassandra.btree.fanfactor", "4");
        for (int i = 0 ; i < ints.length ; i++)
            ints[i] = new Integer(i);
    }

    static final UpdateFunction<Integer> updateF = new UpdateFunction<Integer>()
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
        {
            checkResult(i, BTree.build(seq(i), CMP, true, updateF));
            checkResult(i, BTree.build(rand(i), CMP, false, updateF));
        }
    }

    @Test
    public void testUpdate_UpdateFunctionReplacement()
    {
        for (int i = 0; i < 20 ; i++)
        {
            checkResult(i, BTree.update(BTree.build(seq(i), CMP, true, UpdateFunction.NoOp.<Integer>instance()), CMP, seq(i), true, updateF));
            checkResult(i, BTree.update(BTree.build(rand(i), CMP, false, UpdateFunction.NoOp.<Integer>instance()), CMP, rand(i), false, updateF));
        }
    }

    private static void checkResult(int count, Object[] btree)
    {
        BTreeSet<Integer> vs = new BTreeSet<>(btree, CMP);
        assert vs.size() == count;
        int i = 0;
        for (Integer j : vs)
            Assert.assertEquals(j, ints[i++]);
    }

    @Test
    public void testClearOnAbort()
    {
        final Comparator<String> cmp = new Comparator<String>()
        {
            public int compare(String o1, String o2)
            {
                return o1.compareTo(o2);
            }
        };

        Object[] btree = BTree.build(ranges(range(0, 8)), cmp, true, UpdateFunction.NoOp.<String>instance());
        BTree.update(btree, cmp, ranges(range(0, 94)), false, new AbortAfterX(90));
        btree = BTree.update(btree, cmp, ranges(range(0, 94)), false, UpdateFunction.NoOp.<String>instance());
        Assert.assertTrue(BTree.isWellFormed(btree, cmp));
    }

    private static final class AbortAfterX implements UpdateFunction<String>
    {
        int counter;
        final int abortAfter;
        private AbortAfterX(int abortAfter)
        {
            this.abortAfter = abortAfter;
        }
        public String apply(String replacing, String update)
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
        public String apply(String v)
        {
            return v;
        }
    }

    private static int[] range(int lb, int ub)
    {
        return new int[] { lb, ub };
    }

    private static List<String> ranges(int[] ... ranges)
    {

        List<String> r = new ArrayList<>();
        for (int[] range : ranges)
        {
            for (int i = range[0] ; i < range[1] ; i+=1)
                r.add(Integer.toString(i));
        }
        return r;
    }
}
