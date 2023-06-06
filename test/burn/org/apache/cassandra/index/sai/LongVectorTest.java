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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

public class LongVectorTest extends SAITester
{
    int dimension = 16; // getRandom().nextIntBetween(128, 768);

    KeySet keysInserted = new KeySet();

    @Test
    public void testConcurrentReadsWrites() throws ExecutionException, InterruptedException
    {
        createTable(String.format("CREATE TABLE %%s (key int primary key, value vector<float, %s>)", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        var fjp = new ForkJoinPool(12);
        var task = fjp.submit(() -> IntStream.range(0, 10_000_000).parallel().forEach(i ->
        {
            try
            {
                oneOp(i);
            }
            catch (Throwable e)
            {
                throw new RuntimeException(e);
            }
        }));
        fjp.shutdown();
        task.get(); // re-throw
    }

    private void oneOp(int i) throws Throwable
    {
        var R = ThreadLocalRandom.current();
        var v = normalizedVector(R, dimension);
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

        if (R.nextDouble() < 0.01)
            flush();

        if (R.nextDouble() < 0.0001)
            compact();
    }

    private Vector<Float> normalizedVector(ThreadLocalRandom r, int dimension)
    {
        var vector = new Float[dimension];
        var sum = 0.0f;
        for (int i = 0; i < dimension; i++)
        {
            vector[i] = r.nextFloat();
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
