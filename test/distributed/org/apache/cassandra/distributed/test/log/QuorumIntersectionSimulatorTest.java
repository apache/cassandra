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

package org.apache.cassandra.distributed.test.log;

import java.util.*;

import org.junit.Test;

public class QuorumIntersectionSimulatorTest
{
    public static class PermutationIterator<T> implements Iterator<List<T>> {

        private final List<T> arr;
        private final int[] pointers;
        private final int variants;

        public PermutationIterator(List<T> arr, int size)
        {
            this.arr = arr;
            this.pointers = new int[size];
            this.variants = arr.size() - 1;
        }

        public boolean hasNext()
        {
            for (int p : pointers)
            {
                if (p < variants)
                    return true;
            }
            return false;
        }

        public List<T> next()
        {
            for (int i = 0; i < pointers.length; i++)
            {
                if (pointers[i] < variants)
                {
                    pointers[i]++;
                    break;
                }
                else
                {
                    for (int j = 0; j <= i; j++)
                        pointers[i] = 0;
                }
            }

            List<T> res = new ArrayList<>();
            for (int pointer : pointers)
                res.add(arr.get(pointer));

            return res;
        }
    }

    @Test
    public void checkQuorumIntersections()
    {
        View view1 = new View(set("a", "b", "c"),
                              set("a", "b", "c", "d"));

        View view2 = new View(set("b", "c", "d"),
                              set("a", "b", "c", "d"));

        checkQuorums(view1, view2);

        View view3 = new View(set("b", "c", "d"),
                              set("b", "c", "d"));

        checkQuorums(view2, view3);
    }

    public void checkQuorums(View readingNode, View writingNode)
    {
        Iterator<List<String>> readQuorums = new PermutationIterator<>(readingNode.r, readingNode.r.size() / 2 + 1);
        while (readQuorums.hasNext())
        {
            List<String> readQuorum = readQuorums.next();
            Set<String> readQuorumSet = new HashSet<>(readQuorum);
            if (readQuorum.size() != readQuorumSet.size())
                continue;

            Iterator<List<String>> writeQuorums = new PermutationIterator<>(writingNode.w, writingNode.w.size() / 2 + 1);
            while (writeQuorums.hasNext())
            {
                List<String> writeQuorum = writeQuorums.next();
                Set<String> writeQuorumSet = new HashSet<>(writeQuorum);
                if (writeQuorum.size() != writeQuorumSet.size())
                    continue;

                assert hasIntersection(new HashSet<>(readQuorum), new HashSet<>(writeQuorum)) :
                String.format("No intersections between quorums %s and %s", readQuorum, writeQuorum);
            }
        }
    }

    public static <T> boolean hasIntersection(Set<T> l, Set<T> r) {
        for (T l1 : l)
        {
            for (T r1 : r)
            {
                if (l1.equals(r1))
                    return true;
            }
        }
        return false;
    }

    static class View
    {
        final List<String> r;
        final List<String> w;

        View(List<String> r, List<String> w)
        {
            this.r = r;
            this.w = w;
        }
    }

    public List<String> set(String... nodes) {
        return Arrays.asList(nodes);
    }
}
