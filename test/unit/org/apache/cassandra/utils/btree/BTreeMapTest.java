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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BTreeMapTest
{
    @Test(expected = IllegalStateException.class)
    public void testDuplicates()
    {
        BTreeMap<Integer, String> map = BTreeMap.empty();

        map = map.with(1, "hello");
        map = map.with(2, "bye");
        map = map.with(1, "aaaa");
    }

    @Test
    public void keySetTest()
    {
        BTreeMap<Integer, String> map = BTreeMap.empty();

        map = map.with(1, "hello");
        map = map.with(2, "bye");

        System.out.println(map.keySet());

    }


    @Test
    public void randomTest()
    {
        long seed = 0;
        try
        {
            for (int i = 0; i < 100; i++)
            {
                seed = System.currentTimeMillis();
                Random r = new Random(seed);
                int listSize = 100 + r.nextInt(500); // todo: increase after rebase due to BTreeRemoval bug
                List<Pair<Integer, Integer>> raw = new ArrayList<>(listSize);

                for (int j = 0; j < listSize; j++)
                    raw.add(Pair.create(r.nextInt(10000), r.nextInt()));

                TreeMap<Integer, Integer> expected = new TreeMap<>();
                BTreeMap<Integer, Integer> actual = BTreeMap.empty();
                for (Pair<Integer, Integer> p : raw)
                {
                    expected.put(p.left, p.right);
                    actual = actual.withForce(p.left, p.right);
                    if (expected.size() > 5 && r.nextInt(10) < 4)
                    {
                        int toRemove = r.nextInt(expected.size());
                        expected.remove(raw.get(toRemove).left);
                        actual = actual.without(raw.get(toRemove).left);
                    }
                }
                assertEqual(expected, actual);
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("seed = "+seed, t);
        }
    }

    private void assertEqual(TreeMap<Integer, Integer> expected, BTreeMap<Integer, Integer> actual)
    {
        assertEquals(actual + "\n" + expected , expected.size(), actual.size());

        Iterator<Map.Entry<Integer, Integer>> expectedIter = expected.entrySet().iterator();
        Iterator<Map.Entry<Integer, Integer>> actualIter = actual.entrySet().iterator();
        while (expectedIter.hasNext())
        {
            assertTrue(actualIter.hasNext());
            Map.Entry<Integer, Integer> e = expectedIter.next();
            Map.Entry<Integer, Integer> a = actualIter.next();
            assertEquals(actual + "\n" + expected, e, a);
        }

        Iterator<Integer> actualKeyIter = actual.keySet().iterator();
        Iterator<Integer> expectedKeyIter = expected.keySet().iterator();
        while (expectedKeyIter.hasNext())
        {
            assertTrue(actualKeyIter.hasNext());
            assertEquals(actualKeyIter.next(), expectedKeyIter.next());
        }

        List<Integer> actualValues = new ArrayList<>(actual.values());
        actualValues.sort(Comparator.naturalOrder());
        List<Integer> expectedValues = new ArrayList<>(expected.values());
        expectedValues.sort(Comparator.naturalOrder());
        Iterator<Integer> actualValueIterator = actualValues.iterator();
        Iterator<Integer> expectedValueIter = expectedValues.iterator();
        while (actualValueIterator.hasNext())
        {
            assertTrue(expectedValueIter.hasNext());
            assertEquals(actualValueIterator.next(), expectedValueIter.next());
        }
    }
}
