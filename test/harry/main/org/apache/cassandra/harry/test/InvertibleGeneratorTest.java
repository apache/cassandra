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

package org.apache.cassandra.harry.test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.gen.InvertibleGenerator;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.junit.Assert.assertEquals;

public class InvertibleGeneratorTest
{
    private static final Logger logger = LoggerFactory.getLogger(InvertibleGeneratorTest.class);

    private static final int POPULATION = 10_000;

    @Test
    public void benchmark()
    {
        withRandom(rng -> {
            long start = System.nanoTime();
            HistoryBuilder.IndexedBijection<String> generator = InvertibleGenerator.fromType(rng, POPULATION, ColumnSpec.regularColumn("regular", ColumnSpec.asciiType));
            long buildNanos = System.nanoTime() - start;

            int n = (int) generator.population();
            long[] descriptors = new long[n];
            String[] values = new String[n];
            for (int i = 0; i < n; i++)
            {
                descriptors[i] = generator.descriptorAt(i);
                values[i] = generator.inflate(descriptors[i]);
            }

            start = System.nanoTime();
            for (int i = 0; i < n; i++)
                generator.inflate(descriptors[i]);
            long inflateNanos = System.nanoTime() - start;

            start = System.nanoTime();
            for (int i = 0; i < n; i++)
                generator.deflate(values[i]);
            long deflateNanos = System.nanoTime() - start;

            start = System.nanoTime();
            for (int i = 0; i < n; i++)
                generator.idxFor(descriptors[i]);
            long idxForNanos = System.nanoTime() - start;

            logger.info("InvertibleGenerator benchmark (population={}): build {} ms, inflate {} ms, deflate {} ms, idxFor {} ms ({} ops each)",
                        n,
                        TimeUnit.NANOSECONDS.toMillis(buildNanos),
                        TimeUnit.NANOSECONDS.toMillis(inflateNanos),
                        TimeUnit.NANOSECONDS.toMillis(deflateNanos),
                        TimeUnit.NANOSECONDS.toMillis(idxForNanos),
                        n);
        });
    }

    @Test
    public void fifoCache()
    {
        AtomicInteger computes = new AtomicInteger();
        InvertibleGenerator.Cache<String> cache = new InvertibleGenerator.FifoCache<>(2, d -> { computes.incrementAndGet(); return "v" + d; });

        assertEquals("v1", cache.get(1));   // miss -> compute
        assertEquals("v1", cache.get(1));   // hit
        assertEquals(1, computes.get());

        assertEquals("v2", cache.get(2));   // miss -> compute; cache now holds {1, 2}
        assertEquals(2, computes.get());

        assertEquals("v3", cache.get(3));   // miss -> evicts oldest (1); cache now holds {2, 3}
        assertEquals("v3", cache.get(3));   // hit
        assertEquals(3, computes.get());

        assertEquals("v1", cache.get(1));   // 1 was evicted -> recompute
        assertEquals(4, computes.get());
    }

    @Test
    public void growingCache()
    {
        AtomicInteger computes = new AtomicInteger();
        InvertibleGenerator.Cache<String> cache = new InvertibleGenerator.UnboundedCache<>(4, d -> { computes.incrementAndGet(); return "v" + d; });

        for (long d = 0; d < 100; d++)
            assertEquals("v" + d, cache.get(d));   // 100 distinct misses
        assertEquals(100, computes.get());

        for (long d = 0; d < 100; d++)
            assertEquals("v" + d, cache.get(d));   // nothing is ever evicted, so all hits
        assertEquals(100, computes.get());
    }

    @Test
    public void binarySearchPathCache()
    {
        long[] sorted = { 10, 20, 30, 40, 50, 60, 70 };
        AtomicInteger computes = new AtomicInteger();
        // depth 2 over [0, 6] visits indices {3, 1, 5} -> descriptors {40, 20, 60}, precomputed on construction.
        InvertibleGenerator.Cache<String> cache = new InvertibleGenerator.BinarySearchPathCache<>(sorted, sorted.length, 2,
                                                                                                  d -> { computes.incrementAndGet(); return "v" + d; });
        assertEquals(3, computes.get());

        // entries on the cached search path are served without recomputing
        assertEquals("v40", cache.get(40));
        assertEquals("v20", cache.get(20));
        assertEquals("v60", cache.get(60));
        assertEquals(3, computes.get());

        // entries off the path are computed on every lookup
        assertEquals("v10", cache.get(10));
        assertEquals(4, computes.get());
        assertEquals("v10", cache.get(10));
        assertEquals(5, computes.get());
    }

    @Test
    public void hierarchicalCache()
    {
        long[] sorted = { 10, 20, 30, 40, 50, 60, 70 };
        AtomicInteger pathComputes = new AtomicInteger();
        AtomicInteger fifoComputes = new AtomicInteger();
        // primary pins the depth-2 binary-search path {40, 20, 60}; backstop is a tiny FIFO
        InvertibleGenerator.Cache<String> primary = new InvertibleGenerator.BinarySearchPathCache<>(sorted, sorted.length, 2,
                                                                                                    d -> { pathComputes.incrementAndGet(); return "v" + d; });
        InvertibleGenerator.Cache<String> backstop = new InvertibleGenerator.FifoCache<>(2, d -> { fifoComputes.incrementAndGet(); return "v" + d; });
        InvertibleGenerator.Cache<String> cache = new InvertibleGenerator.HierarchicalCache<>(primary, backstop);

        assertEquals(3, pathComputes.get());   // primary precomputed its 3 path entries

        // entries on the pinned path are served by the primary; the backstop is never consulted
        assertEquals("v40", cache.get(40));
        assertEquals("v20", cache.get(20));
        assertEquals("v60", cache.get(60));
        assertEquals(3, pathComputes.get());
        assertEquals(0, fifoComputes.get());

        // a miss on the primary falls through to the FIFO backstop, which computes and caches it
        assertEquals("v10", cache.get(10));
        assertEquals(1, fifoComputes.get());
        assertEquals("v10", cache.get(10));    // now a backstop hit
        assertEquals(1, fifoComputes.get());
        assertEquals(3, pathComputes.get());   // primary still only ever computed its path
    }
}
