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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;

public class BTreeBiMapTest
{
    @Test
    public void simpleDeleteTest()
    {
        BTreeBiMap<Integer, String> actual = BTreeBiMap.empty();
        actual = actual.with(1, "11");
        actual = actual.with(2, "22");
        actual = actual.with(3, "33");
        assertEquals(3, actual.keySet().size());
        assertEquals(3, actual.inverse().keySet().size());
        assertEquals("11", actual.get(1));
        assertEquals("22", actual.get(2));
        assertEquals("33", actual.get(3));
        actual = actual.without(2);
        assertEquals(2, actual.keySet().size());
        assertEquals(2, actual.inverse().keySet().size());
        assertEquals("11", actual.get(1));
        assertEquals("33", actual.get(3));

    }

    @Test(expected = IllegalArgumentException.class)
    public void invertDuplicateValuesTest()
    {
        BTreeBiMap<Integer, String> actual = BTreeBiMap.empty();
        actual = actual.with(1, "11");
        actual = actual.with(2, "22");
        actual = actual.with(3, "11");
    }

    @Test
    public void randomTest()
    {
        long seed = 0;
        try
        {
            for (int x = 0; x < 100; x++)
            {
                seed = System.currentTimeMillis();
                Random r = new Random(seed);

                int listSize = 100 + r.nextInt(200);
                Set<Integer> unique = new HashSet<>(listSize);

                while (unique.size() < listSize)
                    unique.add(r.nextInt(10000));

                // need unique keys and unique values;
                List<Integer> rawKeys = new ArrayList<>(unique);
                List<Integer> rawValues = new ArrayList<>(unique);
                Collections.shuffle(rawKeys);
                List<Pair<Integer, Integer>> raw = new ArrayList<>();
                for (int i = 0; i < listSize; i++)
                    raw.add(Pair.create(rawKeys.get(i), rawValues.get(i)));

                BiMap<Integer, Integer> expected = HashBiMap.create(listSize);
                BTreeBiMap<Integer, Integer> actual = BTreeBiMap.empty();

                for (Pair<Integer, Integer> p : raw)
                {
                    expected.put(p.left, p.right);
                    actual = actual.with(p.left, p.right);

                    if (expected.size() > 5 && r.nextInt(10) < 4)
                    {
                        int toRemove = r.nextInt(expected.size());
                        expected.remove(raw.get(toRemove).left);
                        actual = actual.without(raw.get(toRemove).left);
                    }
                }
                assertEqual(expected, actual);
                assertEqual(expected.inverse(), actual.inverse());
                assertEqual(expected, actual.inverse().inverse());
                assertEqual(expected.inverse().inverse(), actual);
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Seed = "+seed, t);
        }
    }

    private void assertEqual(BiMap<Integer, Integer> expected, BiMap<Integer, Integer> actual)
    {
        assertEquals(expected.size(), actual.size());

        Iterator<Map.Entry<Integer, Integer>> expectedIter = expected.entrySet().iterator();
        while (expectedIter.hasNext())
        {
            Map.Entry<Integer, Integer> e = expectedIter.next();
            assertEquals(expected + " \n " + actual, e.getValue(), actual.get(e.getKey()));
        }
    }
}
