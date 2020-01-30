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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

import org.junit.Test;

import org.apache.cassandra.utils.IntegerInterval.Set;

public class IntegerIntervalsTest
{
    int[] values = new int[] { Integer.MIN_VALUE, -2, -1, 0, 5, 9, 13, Integer.MAX_VALUE };

    @Test
    public void testMake()
    {
        IntegerInterval iv;
        for (int i = 0; i < values.length; ++i)
        {
            for (int j = i; j < values.length; ++j)
            {
                iv = new IntegerInterval(values[i], values[j]);
                assertEquals(values[i], iv.lower());
                assertEquals(values[j], iv.upper());
            }
        }

        for (int i = 0; i < values.length; ++i)
        {
            for (int j = 0; j < i; ++j)
            {
                try
                {
                    iv = new IntegerInterval(values[i], values[j]);
                    fail("Assertion not thrown: " + values[i] + ", " + values[j]);
                }
                catch (AssertionError e)
                {
                    // expected
                }
            }
        }
    }

    @Test
    public void testExpandToCoverSingleThread()
    {
        IntegerInterval iv;
        for (int i = 0; i < values.length; ++i)
        {
            for (int j = i; j < values.length; ++j)
            {
                iv = new IntegerInterval(values[i], values[j]);
                int k = 0;
                for (; k < i; ++k)
                {
                    IntegerInterval v = new IntegerInterval(iv);
                    v.expandToCover(values[k]);
                    assertEquals(values[k], v.lower());
                    assertEquals(values[j], v.upper());
                }
                for (; k < j; ++k)
                {
                    IntegerInterval v = new IntegerInterval(iv);
                    v.expandToCover(values[k]);
                    assertEquals(values[i], v.lower());
                    assertEquals(values[j], v.upper());
                }
                for (; k < values.length; ++k)
                {
                    IntegerInterval v = new IntegerInterval(iv);
                    v.expandToCover(values[k]);
                    assertEquals(values[i], v.lower());
                    assertEquals(values[k], v.upper());
                }
            }
        }
    }

    @Test
    public void testExpandToCoverMultiThread() throws InterruptedException
    {
        Random r = new Random();
        int threads = 16;
        int streamSize = 1000000;
        List<Callable<Void>> tasks = new ArrayList<>(threads);
        final IntegerInterval interval = new IntegerInterval(0, 0);
        int min = 0;
        int max = 0;
        for (int i = 0; i < threads; ++i)
        {
            final int seed = r.nextInt();
            tasks.add(() -> {
                new Random(seed).ints(streamSize).forEach(v -> interval.expandToCover(v));
                return null;
            });
            min = Math.min(min, new Random(seed).ints(streamSize).min().getAsInt());
            max = Math.max(max, new Random(seed).ints(streamSize).max().getAsInt());
        }
        for (Future<?> f : Executors.newFixedThreadPool(threads).invokeAll(tasks))
            Futures.getUnchecked(f);
        assertEquals(min, interval.lower());
        assertEquals(max, interval.upper());
    }

    void testSetAdd(int l, int r, Integer... expected)
    {
        Set s = new Set();
        s.add(-3, -1);
        s.add(1, 3);
        s.add(l, r);
        assertArrayEquals(expected, s
                                    .intervals()
                                    .stream()
                                    .flatMap(x -> ImmutableList.of(x.lower(), x.upper()).stream())
                                    .toArray());
    }

    void testSetAdd(int l, int r, String expected)
    {
        Set s = new Set();
        s.add(-3, -1);
        s.add(1, 3);
        s.add(l, r);
        assertEquals(expected, s.toString());
    }

    @Test
    public void testSetAdd()
    {
        testSetAdd(Integer.MIN_VALUE, -4, Integer.MIN_VALUE, -4, -3, -1, 1, 3);
        testSetAdd(Integer.MIN_VALUE, -3, Integer.MIN_VALUE, -1, 1, 3);
        testSetAdd(Integer.MIN_VALUE, -2, Integer.MIN_VALUE, -1, 1, 3);
        testSetAdd(Integer.MIN_VALUE, -1, Integer.MIN_VALUE, -1, 1, 3);
        testSetAdd(Integer.MIN_VALUE, 0, Integer.MIN_VALUE, 0, 1, 3);
        testSetAdd(Integer.MIN_VALUE, 1, Integer.MIN_VALUE, 3);
        testSetAdd(Integer.MIN_VALUE, 2, Integer.MIN_VALUE, 3);
        testSetAdd(Integer.MIN_VALUE, 3, Integer.MIN_VALUE, 3);
        testSetAdd(Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE);

        testSetAdd(-5, -4, "[[-5,-4], [-3,-1], [1,3]]");
        testSetAdd(-5, -3, -5, -1, 1, 3);
        testSetAdd(-5, -2, -5, -1, 1, 3);
        testSetAdd(-5, -1, -5, -1, 1, 3);
        testSetAdd(-5, 0, -5, 0, 1, 3);
        testSetAdd(-5, 1, -5, 3);
        testSetAdd(-5, 2, -5, 3);
        testSetAdd(-5, 3, -5, 3);
        testSetAdd(-5, 4, -5, 4);
        testSetAdd(-5, Integer.MAX_VALUE, -5, Integer.MAX_VALUE);

        testSetAdd(-3, -3, -3, -1, 1, 3);
        testSetAdd(-3, -2, -3, -1, 1, 3);
        testSetAdd(-3, -1, -3, -1, 1, 3);
        testSetAdd(-3, 0, -3, 0, 1, 3);
        testSetAdd(-3, 1, "[[-3,3]]");
        testSetAdd(-3, 2, -3, 3);
        testSetAdd(-3, 3, -3, 3);
        testSetAdd(-3, 4, -3, 4);
        testSetAdd(-3, Integer.MAX_VALUE, -3, Integer.MAX_VALUE);

        testSetAdd(-2, -2, -3, -1, 1, 3);
        testSetAdd(-2, -1, -3, -1, 1, 3);
        testSetAdd(-2, 0, "[[-3,0], [1,3]]");
        testSetAdd(-2, 1, -3, 3);
        testSetAdd(-2, 2, -3, 3);
        testSetAdd(-2, 3, -3, 3);
        testSetAdd(-2, 4, -3, 4);
        testSetAdd(-2, Integer.MAX_VALUE, -3, Integer.MAX_VALUE);

        testSetAdd(-1, -1, -3, -1, 1, 3);
        testSetAdd(-1, 0, -3, 0, 1, 3);
        testSetAdd(-1, 1, -3, 3);
        testSetAdd(-1, 2, -3, 3);
        testSetAdd(-1, 3, -3, 3);
        testSetAdd(-1, 4, -3, 4);
        testSetAdd(-1, Integer.MAX_VALUE, -3, Integer.MAX_VALUE);

        testSetAdd(0, 0, -3, -1, 0, 0, 1, 3);
        testSetAdd(0, 1, -3, -1, 0, 3);
        testSetAdd(0, 2, -3, -1, 0, 3);
        testSetAdd(0, 3, -3, -1, 0, 3);
        testSetAdd(0, 4, -3, -1, 0, 4);
        testSetAdd(0, Integer.MAX_VALUE, -3, -1, 0, Integer.MAX_VALUE);

        testSetAdd(1, 1, -3, -1, 1, 3);
        testSetAdd(1, 2, -3, -1, 1, 3);
        testSetAdd(1, 3, -3, -1, 1, 3);
        testSetAdd(1, 4, -3, -1, 1, 4);
        testSetAdd(1, Integer.MAX_VALUE, -3, -1, 1, Integer.MAX_VALUE);

        testSetAdd(2, 2, -3, -1, 1, 3);
        testSetAdd(2, 3, -3, -1, 1, 3);
        testSetAdd(2, 4, -3, -1, 1, 4);
        testSetAdd(2, Integer.MAX_VALUE, -3, -1, 1, Integer.MAX_VALUE);

        testSetAdd(3, 3, "[[-3,-1], [1,3]]");
        testSetAdd(3, 4, -3, -1, 1, 4);
        testSetAdd(3, Integer.MAX_VALUE, -3, -1, 1, Integer.MAX_VALUE);

        testSetAdd(4, 5, -3, -1, 1, 3, 4, 5);
        testSetAdd(4, Integer.MAX_VALUE, -3, -1, 1, 3, 4, Integer.MAX_VALUE);
    }

    @Test
    public void testSetAddMultiThread() throws InterruptedException
    {
        Random r = new Random();
        int threads = 16;
        int streamSize = 10000;
        List<Callable<Void>> tasks = new ArrayList<>(threads);
        final IntegerInterval.Set st = new IntegerInterval.Set();
        final IntegerInterval.Set mt = new IntegerInterval.Set();
        for (int i = 0; i < threads; ++i)
        {
            final int seed = r.nextInt();
            tasks.add(() -> {
                new Random(seed)
                    .ints(streamSize)
                    .forEach(v -> mt.add(v, v + 5));
                return null;
            });
            new Random(seed)
                .ints(streamSize)
                .forEach(v -> st.add(v, v + 5));
        }
        for (Future<?> f : Executors.newFixedThreadPool(threads).invokeAll(tasks))
            Futures.getUnchecked(f);
        assertEquals(st, mt);
    }

    void testSetCovers(int l, int r, boolean expected)
    {
        Set s = new Set();
        s.add(-3, -1);
        s.add(1, 3);
        assertEquals(expected, s.covers(new IntegerInterval(l, r)));
    }


    @Test
    public void testSetCovers()
    {
        testSetCovers(Integer.MIN_VALUE, -4, false);
        testSetCovers(Integer.MIN_VALUE, -3, false);
        testSetCovers(Integer.MIN_VALUE, -2, false);
        testSetCovers(Integer.MIN_VALUE, -1, false);
        testSetCovers(Integer.MIN_VALUE, 0, false);
        testSetCovers(Integer.MIN_VALUE, 1, false);
        testSetCovers(Integer.MIN_VALUE, 2, false);
        testSetCovers(Integer.MIN_VALUE, 3, false);
        testSetCovers(Integer.MIN_VALUE, Integer.MAX_VALUE, false);

        testSetCovers(-5, -4, false);
        testSetCovers(-5, -3, false);
        testSetCovers(-5, -2, false);
        testSetCovers(-5, -1, false);
        testSetCovers(-5, 0, false);
        testSetCovers(-5, 1, false);
        testSetCovers(-5, 2, false);
        testSetCovers(-5, 3, false);
        testSetCovers(-5, 4, false);
        testSetCovers(-5, Integer.MAX_VALUE, false);

        testSetCovers(-3, -3, true);
        testSetCovers(-3, -2, true);
        testSetCovers(-3, -1, true);
        testSetCovers(-3, 0, false);
        testSetCovers(-3, 1, false);
        testSetCovers(-3, 2, false);
        testSetCovers(-3, 3, false);
        testSetCovers(-3, 4, false);
        testSetCovers(-3, Integer.MAX_VALUE, false);

        testSetCovers(-2, -2, true);
        testSetCovers(-2, -1, true);
        testSetCovers(-2, 0, false);
        testSetCovers(-2, 1, false);
        testSetCovers(-2, 2, false);
        testSetCovers(-2, 3, false);
        testSetCovers(-2, 4, false);
        testSetCovers(-2, Integer.MAX_VALUE, false);

        testSetCovers(-1, -1, true);
        testSetCovers(-1, 0, false);
        testSetCovers(-1, 1, false);
        testSetCovers(-1, 2, false);
        testSetCovers(-1, 3, false);
        testSetCovers(-1, 4, false);
        testSetCovers(-1, Integer.MAX_VALUE, false);

        testSetCovers(0, 0, false);
        testSetCovers(0, 1, false);
        testSetCovers(0, 2, false);
        testSetCovers(0, 3, false);
        testSetCovers(0, 4, false);
        testSetCovers(0, Integer.MAX_VALUE, false);

        testSetCovers(1, 1, true);
        testSetCovers(1, 2, true);
        testSetCovers(1, 3, true);
        testSetCovers(1, 4, false);
        testSetCovers(1, Integer.MAX_VALUE, false);

        testSetCovers(2, 2, true);
        testSetCovers(2, 3, true);
        testSetCovers(2, 4, false);
        testSetCovers(2, Integer.MAX_VALUE, false);

        testSetCovers(3, 3, true);
        testSetCovers(3, 4, false);
        testSetCovers(3, Integer.MAX_VALUE, false);

        testSetCovers(4, 5, false);
        testSetCovers(4, Integer.MAX_VALUE, false);
    }
}
