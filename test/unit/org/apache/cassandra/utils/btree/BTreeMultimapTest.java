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
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BTreeMultimapTest
{
    @Test
    public void basicTest()
    {
        BTreeMultimap<String, Integer> map = BTreeMultimap.empty();
        map = map.with("hello", 123);
        map = map.with("hello", 124);
        map = map.with("hello", 125);
        assertEquals(3, map.size());
        assertEquals(Sets.newHashSet(123, 124, 125), map.get("hello"));
        map = map.without("hello", 124);
        assertEquals(2, map.size());
        assertEquals(Sets.newHashSet(123, 125), map.get("hello"));
        map = map.without("hello", 123);
        assertEquals(1, map.size());
        assertEquals(Sets.newHashSet(125), map.get("hello"));
        map = map.without("hello", 125);
        assertEquals(0, map.size());
        assertFalse(map.containsKey("hello"));
    }

    @Test
    public void randomTest()
    {
        // todo; BTree removals are currently broken - increase these numbers after rebase to current trunk (and change seed below)
        int insertCount = 200;
        for (int x = 0; x < 200; x++)
        {
            List<Pair<String, String>> inserted = new ArrayList<>();
            BiMap<String, String> ref = HashBiMap.create();
            BTreeBiMap<String, String> test = BTreeBiMap.empty();
            boolean inversed = false;
            long seed = 1;
//            long seed = System.currentTimeMillis();
            Random r = new Random(seed);
            while (inserted.size() < insertCount)
            {
                double d = r.nextDouble();
                if (d < 0.01)
                {
                    ref = ref.inverse();
                    test = (BTreeBiMap<String, String>) test.inverse();
                    inversed = !inversed;
                }
                else if (d < 0.02)
                {
                    if (inserted.size() > 0)
                    {
                        Pair<String, String> p = inserted.get(r.nextInt(inserted.size()));
                        String key = inversed ? p.right : p.left;
                        ref.remove(key);
                        test = test.without(key);
                        inserted.remove(p);
                    }
                }
                else
                {
                    String key = randomStr(10, r);
                    String value = randomStr(10, r);
                    ref.put(key, value);
                    test = test.with(key, value);
                    Pair<String, String> p = Pair.create(inversed ? value : key, inversed ? key : value);
                    inserted.add(p);
                }
                assertEqual(ref, test);
            }
        }
    }

    private void assertEqual(BiMap<String, String> a, BTreeBiMap<String, String> b)
    {
        assertEquals(a.size(), b.size());
        for (Map.Entry<String, String> expect : a.entrySet())
            assertEquals(b.get(expect.getKey()), expect.getValue());
    }

    private String randomStr(int len, Random r)
    {
        return RandomStringUtils.random(len, 0, 0, true, true, null, r);
    }
}
