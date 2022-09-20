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

package org.apache.cassandra.repair.asymmetric;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeMapTest
{
    @Test
    public void randomTest()
    {
        int iterCount = 0;
        while (iterCount < 10000)
        {
            RangeMap<Integer> rangeMap = new RangeMap<>();
            int cnt = 2000;
            int i = 0;
            long seed = System.currentTimeMillis();
            Random r = new Random(seed);
            Set<Range<Token>> randomRanges = random(cnt, r);
            for (Range<Token> range : randomRanges)
                rangeMap.put(range, i++);

            long a = r.nextLong() % 1000000;
            long b = r.nextLong() % 1000000;
            if (a == b) b++;

            Range<Token> intersectionRange = r(a, b);

            Set<Map.Entry<Range<Token>, Integer>> expected = new HashSet<>();
            for (Map.Entry<Range<Token>, Integer> entry : rangeMap.entrySet())
                if (intersectionRange.intersects(entry.getKey()))
                    expected.add(new RangeMap.Entry<>(entry));

            Set<Map.Entry<Range<Token>, Integer>> intersection = new HashSet<>(rangeMap.removeIntersecting(intersectionRange));

            // no intersecting ranges left in the range map:
            for (Map.Entry<Range<Token>, Integer> entry : rangeMap.entrySet())
                assertFalse("seed:"+seed, intersectionRange.intersects(entry.getKey()));

            assertEquals("seed:"+seed, expected, intersection);
            if (++iterCount % 1000 == 0)
                 System.out.println(iterCount);
        }
    }

    Set<Range<Token>> random(int cnt, Random r)
    {
        Set<Long> uniqueTokens = new HashSet<>(cnt * 2);
        for (int i = 0; i < cnt * 2; i++)
            while(!uniqueTokens.add(r.nextLong() % 1000000));
        List<Long> randomTokens = new ArrayList<>(uniqueTokens);
        randomTokens.sort(Long::compare);

        Set<Range<Token>> ranges = new HashSet<>(cnt);
        for (int i = 0; i < cnt - 1; i++)
        {
            ranges.add(r(randomTokens.get(i), randomTokens.get(i+1)));
            i++;
        }
        ranges.add(r(randomTokens.get(randomTokens.size() - 1), randomTokens.get(0) - 1));
        return ranges;
    }

    private Range<Token> r(long left, long right)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    @Test
    public void testEmpty()
    {
        RangeMap<Integer> rmap = new RangeMap<>();
        assertFalse(rmap.intersectingEntryIterator(r(1, 10)).hasNext());
    }

    @Test
    public void testWrap()
    {
        RangeMap<Integer> rangeMap = new RangeMap<>();
        rangeMap.put(r(5, 10), 1);
        rangeMap.removeIntersecting(r(100, 50));
        assertTrue(rangeMap.isEmpty());
    }
}
