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

package org.apache.cassandra.index.sai;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import org.apache.cassandra.db.memtable.TrieMemtable;

import static org.assertj.core.api.Assertions.assertThat;

public class LongVectorTest extends SAITester
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LongVectorTest.class);

    int dimension = 16; // getRandom().nextIntBetween(128, 768);

    KeySet keysInserted = new KeySet();
    private final int threadCount = 12;

    @Before
    public void setup() throws Throwable
    {
        // we don't get loaded until after TM, so we can't affect the very first memtable,
        // but this will affect all subsequent ones
        TrieMemtable.SHARD_COUNT = 4 * threadCount;
    }

    @FunctionalInterface
    private interface Op
    {
        public void run(int i) throws Throwable;
    }

    public void testConcurrentOps(Op op) throws ExecutionException, InterruptedException
    {
        createTable(String.format("CREATE TABLE %%s (key int primary key, value vector<float, %s>)", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': 'dot_product' }");
        waitForIndexQueryable();

        AtomicInteger counter = new AtomicInteger();
        long start = System.currentTimeMillis();
        var fjp = new ForkJoinPool(threadCount);
        var keys = IntStream.range(0, 10_000_000).boxed().collect(Collectors.toList());
        Collections.shuffle(keys);
        var task = fjp.submit(() -> keys.stream().parallel().forEach(i ->
        {
            wrappedOp(op, i);
            if (counter.incrementAndGet() % 10_000 == 0)
            {
                var elapsed = System.currentTimeMillis() - start;
                logger.info("{} ops in {}ms = {} ops/s", counter.get(), elapsed, counter.get() * 1000.0 / elapsed);
            }
            if (ThreadLocalRandom.current().nextDouble() < 0.001)
                flush();
        }));
        fjp.shutdown();
        task.get(); // re-throw
    }

    private static void wrappedOp(Op op, Integer i)
    {
        try
        {
            op.run(i);
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testConcurrentReadsWritesDeletes() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i -> {
            var R = ThreadLocalRandom.current();
            var v = randomVector(dimension);
            if (R.nextDouble() < 0.2 || keysInserted.isEmpty())
            {
                execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, v);
                keysInserted.add(i);
            } else if (R.nextDouble() < 0.1) {
                var key = keysInserted.getRandom();
                execute("DELETE FROM %s WHERE key = ?", key);
            } else if (R.nextDouble() < 0.5) {
                var key = keysInserted.getRandom();
                execute("SELECT * FROM %s WHERE key = ? ORDER BY value ANN OF ? LIMIT ?", key, v, R.nextInt(1, 100));
            } else {
                execute("SELECT * FROM %s ORDER BY value ANN OF ? LIMIT ?", v, R.nextInt(1, 100));
            }
        });
    }

    // like testConcurrentReadsWritesDeletes, but generates multiple rows w/ the same vector, and
    // the sub-op weights are biased more towards doing additional inserts
    @Test
    public void testMultiplePostings() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i -> {
            var R = ThreadLocalRandom.current();
            var v = sequentiallyDuplicateVector(i, dimension);
            if (R.nextDouble() < 0.8 || keysInserted.isEmpty())
            {
                execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, v);
                keysInserted.add(i);
            } else if (R.nextDouble() < 0.1) {
                var key = keysInserted.getRandom();
                execute("DELETE FROM %s WHERE key = ?", key);
            } else if (R.nextDouble() < 0.5) {
                var key = keysInserted.getRandom();
                execute("SELECT * FROM %s WHERE key = ? ORDER BY value ANN OF ? LIMIT ?", key, v, R.nextInt(1, 100));
            } else {
                execute("SELECT * FROM %s ORDER BY value ANN OF ? LIMIT ?", v, R.nextInt(1, 100));
            }
        });
    }

    @Test
    public void testConcurrentReadsWrites() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i -> {
            var R = ThreadLocalRandom.current();
            var v = randomVector(dimension);
            if (R.nextDouble() < 0.1 || keysInserted.isEmpty())
            {
                execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, v);
                keysInserted.add(i);
            } else if (R.nextDouble() < 0.5) {
                var key = keysInserted.getRandom();
                var results = execute("SELECT * FROM %s WHERE key = ? ORDER BY value ANN OF ? LIMIT ?", key, v, R.nextInt(1, 100));
                assertThat(results).hasSize(1);
            } else {
                var results = execute("SELECT * FROM %s ORDER BY value ANN OF ? LIMIT ?", v, R.nextInt(1, 100));
                assertThat(results).hasSizeGreaterThan(0); // VSTODO can we make a stronger assertion?
            }
        });
    }

    @Test
    public void testConcurrentWrites() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i -> {
            var v = randomVector(dimension);
            execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, v);
        });
    }

    /**
     * @return a normalized vector with the given dimension, where each vector from 0 .. N-1 is the same,
     * N .. 2N-1 is the same, etc., where N is the number of cores.
     */
    private static Vector<Float> sequentiallyDuplicateVector(int i, int dimension)
    {
        int j = 1 + i / Runtime.getRuntime().availableProcessors();
        var vector = new Float[dimension];
        Arrays.fill(vector, 0.0f);
        outer:
        while (true)
        {
            for (int k = 0; k < dimension; k++)
            {
                vector[k] += 1.0f;
                if (j-- <= 0)
                    break outer;
            }
        }
        normalize(vector);
        return new Vector<>(vector);
    }

    private static class KeySet
    {
        private final Map<Integer, Integer> keys = new ConcurrentHashMap<>();
        private final AtomicInteger ordinal = new AtomicInteger();

        public void add(int key)
        {
            var i = ordinal.getAndIncrement();
            keys.put(i, key);
        }

        public int getRandom()
        {
            if (isEmpty())
                throw new IllegalStateException();
            var i = ThreadLocalRandom.current().nextInt(ordinal.get());
            // in case there is race with add(key), retry another random
            return keys.containsKey(i) ? keys.get(i) : getRandom();
        }

        public boolean isEmpty()
        {
            return keys.isEmpty();
        }
    }
}
