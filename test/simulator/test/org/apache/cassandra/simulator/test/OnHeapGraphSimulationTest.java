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

package org.apache.cassandra.simulator.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.disk.v1.vector.VectorPostings;
import org.apache.cassandra.simulator.asm.NemesisFieldKind;
import org.apache.cassandra.simulator.asm.NemesisFieldSelectors;
import org.apache.cassandra.utils.CloseableIterator;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

/**
 * Simulation test for {@link OnHeapGraph} that uses the simulator's nemesis framework to inject
 * adversarial scheduling around mutable field accesses, without modifying the source class.
 * <p>
 * This test registers fields from {@link OnHeapGraph} and {@link VectorPostings} as nemesis targets
 * via {@link NemesisFieldSelectors#register(Class, String, NemesisFieldKind)} so the bytecode
 * transformer inserts scheduling perturbation points around those field accesses.
 * <p>
 * The test exercises concurrent add + search workloads under the simulator, which exposes
 * ordering-dependent bugs that are difficult to trigger with plain threads.
 */
public class OnHeapGraphSimulationTest extends SimulationTestBase
{
    private static final int DIMENSIONS = 8;
    private static final int VECTORS_PER_THREAD = 200;
    private static final int NUM_THREADS = 4;

    @BeforeClass
    public static void registerNemesisFields()
    {
        // OnHeapGraph mutable fields
        NemesisFieldSelectors.register(OnHeapGraph.class, "hasDeletions", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "postingsMap", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "postingsByOrdinal", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "vectorsByKey", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "vectorValues", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "builder", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(OnHeapGraph.class, "nextOrdinal", NemesisFieldKind.ATOMICX);

        // VectorPostings mutable fields
        NemesisFieldSelectors.register(VectorPostings.class, "ordinal", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(VectorPostings.class, "postings", NemesisFieldKind.SIMPLE);
        NemesisFieldSelectors.register(VectorPostings.class, "rowIds", NemesisFieldKind.SIMPLE);
    }

    /**
     * Concurrent adds under nemesis scheduling: multiple threads insert vectors into the same
     * OnHeapGraph and the simulator adversarially reorders field accesses. After all inserts
     * complete, a search must return the vast majority of inserted vectors (ANN recall tolerance).
     */
    @Test
    public void testConcurrentAddsUnderNemesis()
    {
        int totalInserted = NUM_THREADS * VECTORS_PER_THREAD;

        simulate(() -> {
            OnHeapGraph<Integer> graph = createGraph();
            AtomicInteger keyCounter = new AtomicInteger(0);

            ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("writers", NUM_THREADS);

            for (int t = 0; t < NUM_THREADS; t++)
            {
                executor.submit(() -> {
                    for (int i = 0; i < VECTORS_PER_THREAD; i++)
                    {
                        int key = keyCounter.getAndIncrement();
                        ByteBuffer vector = randomVector(DIMENSIONS);
                        graph.add(vector, key, OnHeapGraph.InvalidVectorBehavior.FAIL);
                    }
                });
            }

            SharedGraphHolder.graph = graph;
            SharedGraphHolder.totalInserted = totalInserted;
        }, () -> {
            @SuppressWarnings("unchecked")
            OnHeapGraph<Integer> graph = (OnHeapGraph<Integer>) SharedGraphHolder.graph;
            if (graph == null)
                throw new AssertionError("Graph was not created");

            int total = SharedGraphHolder.totalInserted;
            int graphSize = graph.size();

            // The graph should contain ALL inserted vectors - no data loss from concurrent adds.
            if (graphSize != total)
                throw new AssertionError(String.format(
                    "Graph size %d != total inserted %d — ordinal collision detected (lost %d vectors)",
                    graphSize, total, total - graphSize));
        }, DEFAULT_ITERATIONS);
    }

    /**
     * Concurrent adds and searches under nemesis: writers insert while readers search the graph.
     * The simulator will adversarially schedule field accesses to expose races between
     * hasDeletions reads in search() and writes in add()/remove().
     */
    @Test
    public void testConcurrentAddsAndSearchesUnderNemesis()
    {
        simulate(() -> {
            OnHeapGraph<Integer> graph = createGraph();

            // Pre-seed some vectors so search always has something to traverse
            for (int i = 0; i < 50; i++)
            {
                ByteBuffer vector = randomVector(DIMENSIONS);
                graph.add(vector, -(i + 1), OnHeapGraph.InvalidVectorBehavior.FAIL);
            }

            ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("mixed", NUM_THREADS * 2);

            // Writers
            AtomicInteger keyCounter = new AtomicInteger(0);
            for (int t = 0; t < NUM_THREADS; t++)
            {
                executor.submit(() -> {
                    for (int i = 0; i < VECTORS_PER_THREAD; i++)
                    {
                        int key = keyCounter.getAndIncrement();
                        ByteBuffer vector = randomVector(DIMENSIONS);
                        graph.add(vector, key, OnHeapGraph.InvalidVectorBehavior.FAIL);
                    }
                });
            }

            // Readers: search while writers are in progress
            for (int t = 0; t < NUM_THREADS; t++)
            {
                executor.submit(() -> {
                    for (int i = 0; i < 10; i++)
                    {
                        float[] queryVector = new float[DIMENSIONS];
                        for (int d = 0; d < DIMENSIONS; d++)
                            queryVector[d] = ThreadLocalRandom.current().nextFloat();

                        // Must not throw — safety invariant
                        try (CloseableIterator<SearchResult.NodeScore> results = graph.search(queryVector, 50, new Bits.MatchAllBits(graph.size())))
                        {
                            while (results.hasNext())
                            {
                                SearchResult.NodeScore ns = results.next();
                                if (!Float.isFinite(ns.score))
                                    throw new AssertionError("Non-finite score during concurrent search: " + ns.score);
                            }
                        }
                    }
                });
            }
        }, () -> {}, DEFAULT_ITERATIONS);
    }

    /**
     * Concurrent adds and removes under nemesis: exercises the hasDeletions volatile flag
     * and the interaction between add() creating postings and remove() marking them deleted.
     */
    @Test
    public void testConcurrentAddsAndRemovesUnderNemesis()
    {
        simulate(() -> {
            OnHeapGraph<Integer> graph = createGraph();
            int insertCount = NUM_THREADS * VECTORS_PER_THREAD;

            // First, insert all vectors
            List<ByteBuffer> vectors = new ArrayList<>(insertCount);
            for (int i = 0; i < insertCount; i++)
            {
                ByteBuffer vector = randomVector(DIMENSIONS);
                vectors.add(vector);
                graph.add(vector, i, OnHeapGraph.InvalidVectorBehavior.FAIL);
            }

            ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("removers", NUM_THREADS);

            // Remove half the vectors concurrently
            AtomicInteger removeCounter = new AtomicInteger(0);
            for (int t = 0; t < NUM_THREADS; t++)
            {
                executor.submit(() -> {
                    int idx;
                    while ((idx = removeCounter.getAndIncrement()) < insertCount)
                    {
                        if (idx % 2 == 0)
                            graph.remove(vectors.get(idx), idx);
                    }
                });
            }

            // After removes, search should still work without exceptions
            float[] queryVector = new float[DIMENSIONS];
            for (int d = 0; d < DIMENSIONS; d++)
                queryVector[d] = ThreadLocalRandom.current().nextFloat();

            try (CloseableIterator<SearchResult.NodeScore> results = graph.search(queryVector, 100, new Bits.MatchAllBits(graph.size())))
            {
                while (results.hasNext())
                {
                    SearchResult.NodeScore ns = results.next();
                    if (!Float.isFinite(ns.score))
                        throw new AssertionError("Non-finite score with deletions: " + ns.score);
                }
            }
        }, () -> {}, DEFAULT_ITERATIONS);
    }

    @SuppressWarnings("unchecked")
    private static OnHeapGraph<Integer> createGraph()
    {
        VectorType<?> vectorType = VectorType.getInstance(FloatType.instance, DIMENSIONS);
        IndexWriterConfig config = new IndexWriterConfig(
            IndexWriterConfig.DEFAULT_MAXIMUM_NODE_CONNECTIONS,
            IndexWriterConfig.DEFAULT_CONSTRUCTION_BEAM_WIDTH,
            VectorSimilarityFunction.DOT_PRODUCT,
            null
        );
        // Use a JDK proxy — Mockito cannot operate inside the InstanceClassLoader.
        // OnHeapGraph only checks memtable != null and calls getClass().getSimpleName() + hashCode().
        Memtable memtable = (Memtable) java.lang.reflect.Proxy.newProxyInstance(
            Memtable.class.getClassLoader(),
            new Class<?>[]{ Memtable.class },
            (proxy, method, args) -> {
                if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
                if ("toString".equals(method.getName())) return "SimulatedMemtable";
                if ("equals".equals(method.getName())) return proxy == args[0];
                return null;
            }
        );
        return new OnHeapGraph<>(vectorType, config, memtable);
    }

    private static ByteBuffer randomVector(int dimensions)
    {
        List<Float> rawVector = new ArrayList<>(dimensions);
        for (int i = 0; i < dimensions; i++)
            rawVector.add(ThreadLocalRandom.current().nextFloat());
        return VectorType.getInstance(FloatType.instance, dimensions).getSerializer().serialize(rawVector);
    }

    /**
     * Static holder so the graph created inside the simulated classloader can be shared
     * between the action runnables and the check runnable.
     */
    public static class SharedGraphHolder
    {
        public static volatile OnHeapGraph<?> graph;
        public static volatile int totalInserted;
    }
}
