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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.utils.MergeIterator;

import static org.junit.Assert.assertEquals;

/**
 * {@link IteratorMergeOrder} must give equal items in the order {@link MergeIterator} gives them
 * to its reducer.  Each source is a sorted list of ints, and many sources hold the same ints, so
 * most steps merge several sources.  Source counts go past the sorted section into the binary
 * heap, and empty or short sources exercise the removal of an exhausted source.
 */
public class IteratorMergeOrderTest
{
    private static final class Source
    {
        final int id;
        final int[] items;
        int pos = -1;

        Source(int id, int[] items)
        {
            this.id = id;
            this.items = items;
        }

        int current()
        {
            return items[pos];
        }
    }

    private static final IteratorMergeOrder.Sources<Source> SOURCES = new IteratorMergeOrder.Sources<>()
    {
        public int compare(Source a, Source b)
        {
            return Integer.compare(a.current(), b.current());
        }

        public boolean advance(Source source)
        {
            return ++source.pos < source.items.length;
        }
    };

    @Test
    public void sameOrderAsMergeIterator()
    {
        Random random = new Random(0x1EEDL);
        IteratorMergeOrder<Source> order = new IteratorMergeOrder<>(64, SOURCES);
        for (int round = 0; round < 3000; round++)
        {
            int sourceCount = 1 + random.nextInt(round % 3 == 0 ? 64 : 9);
            int range = 1 + random.nextInt(40);
            int[][] data = new int[sourceCount][];
            for (int s = 0; s < sourceCount; s++)
            {
                int density = random.nextInt(101);
                List<Integer> items = new ArrayList<>();
                for (int v = 0; v < range; v++)
                    if (random.nextInt(100) < density)
                        items.add(v);
                data[s] = items.stream().mapToInt(Integer::intValue).toArray();
            }

            List<List<Integer>> expected = mergeIteratorOrder(data);

            Source[] sources = new Source[sourceCount];
            for (int s = 0; s < sourceCount; s++)
                sources[s] = new Source(s, data[s]);
            // reuse one order across rounds, as the compaction does
            order.reset(sources, sourceCount);
            List<List<Integer>> actual = new ArrayList<>();
            Source[] into = new Source[sourceCount];
            int n;
            while ((n = order.next(into)) > 0)
            {
                List<Integer> step = new ArrayList<>();
                for (int i = 0; i < n; i++)
                    step.add(into[i].id);
                actual.add(step);
            }
            assertEquals("round " + round + ", " + sourceCount + " sources", expected, actual);
        }
    }

    /** The source index of every item the reducer gets, step by step. */
    private static List<List<Integer>> mergeIteratorOrder(int[][] data)
    {
        List<Iterator<Integer>> iterators = new ArrayList<>();
        for (int[] items : data)
        {
            List<Integer> list = new ArrayList<>();
            for (int v : items)
                list.add(v);
            iterators.add(list.iterator());
        }
        List<List<Integer>> steps = new ArrayList<>();
        MergeIterator<Integer, List<Integer>> merge = MergeIterator.get(iterators, Comparator.naturalOrder(), new MergeIterator.Reducer<>()
        {
            List<Integer> step;

            public void reduce(int idx, Integer current)
            {
                step.add(idx);
            }

            protected List<Integer> getReduced()
            {
                return step;
            }

            protected void onKeyChange()
            {
                step = new ArrayList<>();
            }
        });
        while (merge.hasNext())
            steps.add(merge.next());
        return steps;
    }
}
