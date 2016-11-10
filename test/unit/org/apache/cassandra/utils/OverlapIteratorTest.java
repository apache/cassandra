package org.apache.cassandra.utils;
/*
 *
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
 *
 */


import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OverlapIteratorTest
{

    private static List<Interval<Integer, Integer>> randomIntervals(int range, int increment, int count)
    {
        List<Integer> a = random(range, increment, count);
        List<Integer> b = random(range, increment, count);
        List<Interval<Integer, Integer>> r = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
        {
            r.add(a.get(i) < b.get(i) ? Interval.create(a.get(i), b.get(i), i)
                                      : Interval.create(b.get(i), a.get(i), i));
        }
        return r;
    }

    private static List<Integer> random(int range, int increment, int count)
    {
        List<Integer> random = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
        {
            int base = i * increment;
            random.add(ThreadLocalRandom.current().nextInt(base, base + range));
        }
        return random;
    }

    @Test
    public void test()
    {
        for (int i = 0 ; i < 10 ; i++)
        {
            test(1000, 0, 1000);
            test(100000, 100, 1000);
            test(1000000, 0, 1000);
        }
    }

    private void test(int range, int increment, int count)
    {
        compare(randomIntervals(range, increment, count), random(range, increment, count), 1);
        compare(randomIntervals(range, increment, count), random(range, increment, count), 2);
        compare(randomIntervals(range, increment, count), random(range, increment, count), 3);
    }

    private <I extends Comparable<I>, V> void compare(List<Interval<I, V>> intervals, List<I> points, int initCount)
    {
        Collections.sort(points);
        IntervalTree<I, V, Interval<I, V>> tree = IntervalTree.build(intervals);
        OverlapIterator<I, V> iter = new OverlapIterator<>(intervals);
        int initPoint = points.size() / initCount;
        int i = 0;
        for (I point : points)
        {
            if (i++ == initPoint)
                iter = new OverlapIterator<>(intervals);
            iter.update(point);
            TreeSet<V> act = new TreeSet<>(iter.overlaps);
            TreeSet<V> exp = new TreeSet<>(tree.search(point));
            TreeSet<V> extra = new TreeSet<>(act);
            extra.removeAll(exp);
            TreeSet<V> missing = new TreeSet<>(exp);
            missing.removeAll(act);
            assertTrue(extra.isEmpty());
            assertTrue(missing.isEmpty());
        }
    }

}
