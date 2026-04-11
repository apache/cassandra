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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;
import org.quicktheories.WithQuickTheories;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import accord.utils.Invariants;

import org.apache.cassandra.service.accord.OrderedKeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderedKeysTest implements WithQuickTheories
{
    private Gen<List<Op>> operationsGen()
    {
        Gen<Integer> sizeGen = SourceDSL.integers().between(0, 200);
        Gen<Integer> keyGen = SourceDSL.integers().between(0, 1000);
        Gen<Boolean> opGen = SourceDSL.booleans().all();
        return rng -> {
            Set<Integer> additions = new HashSet<>();
            int size = sizeGen.generate(rng);
            List<Op> ops = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                while (true)
                {
                    if (opGen.generate(rng))
                    {
                        int key = keyGen.generate(rng);
                        // Do not add same key twice
                        if (additions.contains(key))
                            continue;
                        ops.add(new AddOp(key));
                        additions.add(key);
                    }
                    else if (!additions.isEmpty())
                    {
                        int idx = 0;
                        if (additions.size() > 1)
                            idx = (int) rng.next(Constraint.between(0, additions.size() - 1));

                        Iterator<Integer> iter = additions.iterator();
                        Integer key = iter.next();
                        for (int j = 0; j < idx; j++)
                            key = iter.next();

                        ops.add(new RemoveOp(key));
                        additions.remove(key);
                    }
                    break;
                }
            }

            return ops;
        };
    }

    interface Op
    {
        void apply(OrderedKeys<Integer> orderedKeys, TreeSet<Integer> reference, Map<Integer, Object> cache);
    }

    static class AddOp implements Op
    {
        final Integer key;

        AddOp(Integer key)
        {
            this.key = key;
        }

        @Override
        public void apply(OrderedKeys<Integer> orderedKeys, TreeSet<Integer> model, Map<Integer, Object> cache)
        {
            orderedKeys.add(key);
            cache.put(key, key.toString());
            model.add(key);
        }

        @Override
        public String toString()
        {
            return "Add(" + key + ")";
        }
    }

    static class RemoveOp implements Op
    {
        final Integer key;

        RemoveOp(Integer key)
        {
            this.key = key;
        }

        @Override
        public void apply(OrderedKeys<Integer> orderedKeys, TreeSet<Integer> model, Map<Integer, Object> cache)
        {
            orderedKeys.remove(key);
            Object removed = cache.remove(key);
            Assert.assertEquals(removed, key.toString());
            Invariants.require(model.remove(key));
        }

        @Override
        public String toString()
        {
            return "Remove(" + key + ")";
        }
    }

    @Test
    public void simpleTest()
    {
        OrderedKeys<Integer> keys = new OrderedKeys<>(Integer::compareTo);
        keys.add(1);
        keys.add(5);
        keys.add(3);

        keys.flush();
        assertEquals(3, keys.size());

        Iterator<Integer> iter = keys.iterator();
        assertEquals(Integer.valueOf(1), iter.next());
        assertEquals(Integer.valueOf(3), iter.next());
        assertEquals(Integer.valueOf(5), iter.next());
        assertFalse(iter.hasNext());

        // Remove a key
        keys.remove(3);
        keys.flush();
        assertEquals(2, keys.size());

        iter = keys.iterator();
        assertEquals(Integer.valueOf(1), iter.next());
        assertEquals(Integer.valueOf(5), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testDuplicates()
    {
        OrderedKeys<Integer> keys = new OrderedKeys<>(Integer::compareTo);
        keys.add(1);
        keys.remove(1);
        keys.add(1);

        keys.flush();

        Iterator<Integer> iter = keys.iterator();
        assertEquals((Integer) 1, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testBetween()
    {
        OrderedKeys<Integer> keys = new OrderedKeys<>(Integer::compareTo);

        for (int i = 0; i < 10; i++)
            keys.add(i);
        keys.flush();

        // incl
        List<Integer> result = new ArrayList<>();
        keys.between(3, true, 7, true).forEach(result::add);
        assertEquals(List.of(3, 4, 5, 6, 7), result);

        // excl
        result.clear();
        keys.between(3, false, 7, false).forEach(result::add);
        assertEquals(List.of(4, 5, 6), result);

        // incl/excl
        result.clear();
        keys.between(3, true, 7, false).forEach(result::add);
        assertEquals(List.of(3, 4, 5, 6), result);
    }

    @Test
    public void testBuffering()
    {
        OrderedKeys<Integer> keys = new OrderedKeys<>(Integer::compareTo);

        for (int i = 0; i < 10; i++)
            keys.add(i);

        assertTrue("Buffer should have pending operations", keys.bufferSize() == 10);

        // After flush, buffer should be empty
        keys.flush();
        assertEquals(0, keys.bufferSize());
        assertEquals(10, keys.size());
    }

    @Test
    public void testFlushOnOverflow()
    {
        OrderedKeys<Integer> keys = new OrderedKeys<>(Integer::compareTo);

        // add more than 32 keys (default buffer size)
        for (int i = 0; i < 50; i++)
            keys.add(i);

        assertTrue("Buffer should have flushed", keys.bufferSize() == (50 - 32));
    }

    @Test
    public void randomOperationsMatchReference()
    {
        qt().withExamples(1000)
            .withShrinkCycles(0)
            .withFixedSeed(1)
            .forAll(operationsGen())
            .checkAssert((operations) -> {
                Map<Integer, Object> cache = new HashMap<>();
                OrderedKeys<Integer> orderedKeys = new OrderedKeys<>(Integer::compareTo);
                TreeSet<Integer> model = new TreeSet<>();

                for (int i = 0; i < operations.size(); i++)
                {
                    Op op = operations.get(i);
                    op.apply(orderedKeys, model, cache);
                    if (Math.random() < 0.1)
                    {
                        orderedKeys.flush();
                        assertMatchesReference(operations.subList(0, i), orderedKeys, model, cache, "After: " + op);
                    }
                }

                orderedKeys.flush();
                assertMatchesReference(operations, orderedKeys, model, cache, "Final state");
            });
    }

    private void assertMatchesReference(List<Op> ops, OrderedKeys<Integer> orderedKeys, TreeSet<Integer> model,  Map<Integer, Object> cache, String message)
    {
        List<Integer> actualList = new ArrayList<>();
        orderedKeys.iterator().forEachRemaining(actualList::add);
        List<Integer> expectedList = new ArrayList<>(model);

        if (!expectedList.equals(actualList))
        {
            throw new AssertionError(String.format("%s\n" +
                                                   "Ops:          %s\n" +
                                                   "Ordered Keys: %s\n" +
                                                   "Cache:        %s\n" +
                                                   "Model:        %s", message, ops, actualList, cache, model));
        }
    }
}
