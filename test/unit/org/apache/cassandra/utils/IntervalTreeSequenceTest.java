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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INTERVAL_TREE_EXPENSIVE_CHECKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs random sequences of add, replace and update on one tree and compares it with a brute-force model after
 * every step. This covers add after replace, replace after add, update after add and the periodic full rebuild in
 * {@link IntervalTree#add}, which the property tests in {@link IntervalTreeTest} do not combine.
 */
public class IntervalTreeSequenceTest
{
    private static final Logger logger = LoggerFactory.getLogger(IntervalTreeSequenceTest.class);

    private static final int RUNS = 3000;
    // IntervalTree.REBUILD_AT_MOD_COUNT: add() rebuilds the whole tree when the modification count reaches it
    private static final int REBUILD_AT_MOD_COUNT = 20;

    private int nextId = 0;

    @BeforeClass
    public static void beforeClass()
    {
        TEST_INTERVAL_TREE_EXPENSIVE_CHECKS.setBoolean(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRandomSequences()
    {
        assertTrue(IntervalTree.EXPENSIVE_CHECKS);
        long seed0 = System.nanoTime();
        logger.info("Initial seed {}", seed0);
        int adds = 0, replaces = 0, updates = 0, rebuilds = 0;
        for (int run = 0; run < RUNS; run++)
        {
            long seed = seed0 + run;
            Random rng = new Random(seed);
            int range = 1 + rng.nextInt(12); // small ranges give many intervals with the same bounds
            List<Interval<Integer, Integer>> model = new ArrayList<>();
            int initial = rng.nextInt(8);
            for (int i = 0; i < initial; i++)
                model.add(newInterval(rng, range));
            IntervalTree<Integer, Integer, Interval<Integer, Integer>> tree = IntervalTree.build(new ArrayList<>(model));
            int steps = 10 + rng.nextInt(60);
            for (int step = 0; step < steps; step++)
            {
                int op = rng.nextInt(10);
                String desc;
                if (op < 5)
                {
                    int k = 1 + rng.nextInt(3);
                    Interval<Integer, Integer>[] add = new Interval[k];
                    for (int i = 0; i < k; i++)
                        add[i] = newInterval(rng, range);
                    int before = tree.modCount;
                    tree = tree.add(add.clone());
                    if (before + 1 >= REBUILD_AT_MOD_COUNT && tree.modCount == 0)
                        rebuilds++;
                    Collections.addAll(model, add);
                    adds++;
                    desc = "add";
                }
                else if (op < 8)
                {
                    if (model.isEmpty())
                        continue;
                    List<Pair<Interval<Integer, Integer>, Interval<Integer, Integer>>> replacements = new ArrayList<>();
                    List<Interval<Integer, Integer>> shuffled = new ArrayList<>(model);
                    Collections.shuffle(shuffled, rng);
                    int k = 1 + rng.nextInt(shuffled.size());
                    for (int i = 0; i < k; i++)
                    {
                        Interval<Integer, Integer> old = shuffled.get(i);
                        Interval<Integer, Integer> replacement = Interval.create(old.min, old.max, nextId++);
                        replacements.add(Pair.create(old, replacement));
                        model.remove(old);
                        model.add(replacement);
                    }
                    tree = tree.replace(replacements);
                    replaces++;
                    desc = "replace";
                }
                else
                {
                    List<Interval<Integer, Integer>> shuffled = new ArrayList<>(model);
                    Collections.shuffle(shuffled, rng);
                    int r = shuffled.isEmpty() ? 0 : rng.nextInt(shuffled.size() + 1);
                    Interval<Integer, Integer>[] removals = shuffled.subList(0, r).toArray(new Interval[0]);
                    int k = rng.nextInt(3);
                    Interval<Integer, Integer>[] additions = new Interval[k];
                    for (int i = 0; i < k; i++)
                        additions[i] = newInterval(rng, range);
                    for (Interval<Integer, Integer> removal : removals)
                        model.remove(removal);
                    Collections.addAll(model, additions);
                    tree = tree.update(removals, additions);
                    updates++;
                    desc = "update";
                }
                check(tree, model, rng, range, "seed=" + seed + " step=" + step + " op=" + desc);
            }
        }
        logger.info("adds={} replaces={} updates={} rebuilds={}", adds, replaces, updates, rebuilds);
        assertTrue("the rebuild branch of add() was not exercised", rebuilds > 0);
    }

    private Interval<Integer, Integer> newInterval(Random rng, int range)
    {
        int a = rng.nextInt(2 * range + 1) - range;
        int b = rng.nextInt(2 * range + 1) - range;
        return Interval.create(Math.min(a, b), Math.max(a, b), nextId++);
    }

    private static void check(IntervalTree<Integer, Integer, Interval<Integer, Integer>> tree,
                              List<Interval<Integer, Integer>> model, Random rng, int range, String ctx)
    {
        assertEquals(ctx, model.size(), tree.intervalCount());
        Set<Interval<Integer, Integer>> iterated = new HashSet<>();
        tree.forEach(iterated::add);
        assertEquals(ctx, new HashSet<>(model), iterated);
        assertEquals(ctx, model.isEmpty(), tree.isEmpty());
        if (!model.isEmpty())
        {
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
            for (Interval<Integer, Integer> i : model)
            {
                min = Math.min(min, i.min);
                max = Math.max(max, i.max);
            }
            assertEquals(ctx, min, (int) tree.min());
            assertEquals(ctx, max, (int) tree.max());
        }
        for (int q = -range - 1; q <= range + 1; q++)
            checkQuery(tree, model, q, q, ctx);
        for (int n = 0; n < 6; n++)
        {
            int a = rng.nextInt(2 * range + 3) - range - 1;
            int b = rng.nextInt(2 * range + 3) - range - 1;
            checkQuery(tree, model, Math.min(a, b), Math.max(a, b), ctx);
        }
    }

    private static void checkQuery(IntervalTree<Integer, Integer, Interval<Integer, Integer>> tree,
                                   List<Interval<Integer, Integer>> model, int lo, int hi, String ctx)
    {
        List<Integer> expected = new ArrayList<>();
        for (Interval<Integer, Integer> i : model)
            if (i.min <= hi && i.max >= lo)
                expected.add(i.data);
        List<Integer> actual = tree.search(Interval.create(lo, hi, null));
        // no interval may be reported twice, and the result set must match brute force
        assertEquals(ctx + " query=[" + lo + ',' + hi + "] duplicates in " + actual, new HashSet<>(actual).size(), actual.size());
        assertEquals(ctx + " query=[" + lo + ',' + hi + ']', new HashSet<>(expected), new HashSet<>(actual));
    }
}
