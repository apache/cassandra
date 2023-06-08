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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import org.apache.cassandra.db.memtable.TrieMemtable;

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

    public void testConcurrentOps(Consumer<Integer> op) throws ExecutionException, InterruptedException
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
            op.accept(i);
            if (counter.incrementAndGet() % 10_000 == 0)
            {
                var elapsed = System.currentTimeMillis() - start;
                logger.info("{} ops in {}ms = {} ops/s", counter.get(), elapsed, counter.get() * 1000.0 / elapsed);
            }
        }));
        fjp.shutdown();
        task.get(); // re-throw
    }

    @Test
    public void testConcurrentReadsWritesDeletes() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i ->
                          {
                              try
                              {
                                  readWriteDeleteOp(i);
                                  if (ThreadLocalRandom.current().nextDouble() < 0.0001)
                                      flush();
                              }
                              catch (Throwable e)
                              {
                                  throw new RuntimeException(e);
                              }
                          });
    }

    @Test
    public void testConcurrentReadsWrites() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i ->
        {
            try
            {
                readWriteOp(i);
            }
            catch (Throwable e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testConcurrentWrites() throws ExecutionException, InterruptedException
    {
        testConcurrentOps(i ->
        {
            try
            {
                writeOp(i);
            }
            catch (Throwable e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private void writeOp(int i) throws Throwable
    {
        var R = ThreadLocalRandom.current();
        var v = normalizedVector(dimension);
        execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, v);
    }

    private void readWriteOp(int i) throws Throwable
    {
        var R = ThreadLocalRandom.current();
        var v = normalizedVector(dimension);
        if (R.nextDouble() < 0.1 || keysInserted.isEmpty())
        {
            execute("INSERT INTO %s (key, value) VALUES (?, ?)", i, v);
            keysInserted.add(i);
        } else if (R.nextDouble() < 0.5) {
            var key = keysInserted.getRandom();
            execute("SELECT * FROM %s WHERE key = ? ORDER BY value ANN OF ? LIMIT ?", key, v, R.nextInt(1, 100));
        } else {
            execute("SELECT * FROM %s ORDER BY value ANN OF ? LIMIT ?", v, R.nextInt(1, 100));
        }
    }

    private void readWriteDeleteOp(int i) throws Throwable
    {
        var R = ThreadLocalRandom.current();
        var v = normalizedVector(dimension);
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
    }

    private static Vector<Float> normalizedVector(int dimension)
    {
        var R = ThreadLocalRandom.current();
        var vector = new Float[dimension];
        var sum = 0.0f;
        for (int i = 0; i < dimension; i++)
        {
            vector[i] = R.nextFloat();
            sum += vector[i] * vector[i];
        }

        sum = (float) Math.sqrt(sum);
        for (int i = 0; i < dimension; i++)
            vector[i] /= sum;

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
            return keys.get(i);
        }

        public boolean isEmpty()
        {
            return keys.isEmpty();
        }
    }
}
