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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.github.jbellis.jvector.disk.SimpleMappedReader;
import io.github.jbellis.jvector.disk.SimpleWriter;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import static org.assertj.core.api.Assertions.assertThat;

public class CachingGraphIndexTest
{
    private static final int SIZE = 5000;
    private static final int DIMENSION = 16;
    private static final VectorSimilarityFunction SIMILARITY = VectorSimilarityFunction.COSINE;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private SimpleMappedReader.Supplier readerSupplier;
    private OnDiskGraphIndex onDiskGraph;
    private CachingGraphIndex cachingGraph;

    @Before
    public void setup() throws Exception
    {
        Random random = new Random(42);
        List<VectorFloat<?>> vectors = new ArrayList<>(SIZE);
        for (int i = 0; i < SIZE; i++)
        {
            float[] vector = new float[DIMENSION];
            for (int d = 0; d < DIMENSION; d++)
                vector[d] = random.nextFloat() - 0.5f;
            vectors.add(JVectorUtils.toVectorFloat(vector));
        }
        ListRandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectors, DIMENSION);

        Path path = temporaryFolder.newFile().toPath();
        try (GraphIndexBuilder builder = new GraphIndexBuilder(ravv, SIMILARITY, OnHeapGraph.maxDegree(16), 100, 1.2f, 1.4f, false))
        {
            for (int i = 0; i < SIZE; i++)
                builder.addGraphNode(i, ravv.getVector(i));
            builder.cleanup();

            try (SimpleWriter out = new SimpleWriter(path))
            {
                OnHeapGraph.writeGraph(builder.getGraph(), ravv, out);
            }
        }

        readerSupplier = new SimpleMappedReader.Supplier(path);
        onDiskGraph = OnDiskGraphIndex.load(readerSupplier, 0);
        cachingGraph = new CachingGraphIndex(OnDiskGraphIndex.load(readerSupplier, 0));
    }

    @After
    public void teardown() throws Exception
    {
        onDiskGraph.close();
        cachingGraph.close();
        readerSupplier.close();
    }

    @Test
    public void testCachesNodesNearEntryNode() throws Exception
    {
        // jvector 1.x filled the cache depth first, skipping neighbors already cached, so it holds fewer nodes than
        // everything within CACHE_DISTANCE hops. Cache the same ones, so that heap use per segment stays as it was.
        Set<Integer> expected = new LinkedHashSet<>();
        Set<Integer> withinDistance = new HashSet<>();
        try (OnDiskGraphIndex.View view = onDiskGraph.getView())
        {
            cacheDepthFirst(view, view.entryNode().node, CachingGraphIndex.CACHE_DISTANCE, expected);

            Set<Integer> frontier = Set.of(view.entryNode().node);
            for (int distance = 0; distance <= CachingGraphIndex.CACHE_DISTANCE; distance++)
            {
                withinDistance.addAll(frontier);
                Set<Integer> next = new HashSet<>();
                for (int node : frontier)
                {
                    NodesIterator it = view.getNeighborsIterator(0, node);
                    while (it.hasNext())
                        next.add(it.nextInt());
                }
                next.removeAll(withinDistance);
                frontier = next;
            }
        }

        assertThat(cachingGraph.cachedNodeCount()).isEqualTo(expected.size());
        assertThat(withinDistance).containsAll(expected);
        assertThat(cachingGraph.cachedNodeCount()).isGreaterThan(onDiskGraph.maxDegree()).isLessThan(SIZE);
        assertThat(cachingGraph.ramBytesUsed()).isGreaterThan(onDiskGraph.ramBytesUsed() + (long) expected.size() * DIMENSION * Float.BYTES);
    }

    private static void cacheDepthFirst(OnDiskGraphIndex.View view, int node, int distance, Set<Integer> cached)
    {
        cached.add(node);
        if (distance == 0)
            return;
        NodesIterator it = view.getNeighborsIterator(0, node);
        int[] neighbors = new int[it.size()];
        for (int i = 0; i < neighbors.length; i++)
            neighbors[i] = it.nextInt();
        for (int neighbor : neighbors)
        {
            if (!cached.contains(neighbor))
                cacheDepthFirst(view, neighbor, distance - 1, cached);
        }
    }

    @Test
    public void testViewMatchesOnDiskView() throws Exception
    {
        try (OnDiskGraphIndex.View expected = onDiskGraph.getView();
             CachingGraphIndex.CachedView actual = cachingGraph.getView())
        {
            assertThat(actual.entryNode()).isEqualTo(expected.entryNode());
            assertThat(actual.size()).isEqualTo(expected.size());
            VectorFloat<?> destination = JVectorUtils.toVectorFloat(new float[DIMENSION + 1]);
            for (int node = 0; node < SIZE; node++)
            {
                assertThat(neighbors(actual.getNeighborsIterator(0, node))).isEqualTo(neighbors(expected.getNeighborsIterator(0, node)));
                assertThat(actual.getVector(node)).isEqualTo(expected.getVector(node));
                actual.getVectorInto(node, destination, 1);
                for (int d = 0; d < DIMENSION; d++)
                    assertThat(destination.get(d + 1)).isEqualTo(expected.getVector(node).get(d));
            }
        }
    }

    @Test
    public void testSearchMatchesOnDiskGraph() throws Exception
    {
        Random random = new Random(7);
        for (int q = 0; q < 50; q++)
        {
            float[] query = new float[DIMENSION];
            for (int d = 0; d < DIMENSION; d++)
                query[d] = random.nextFloat() - 0.5f;
            VectorFloat<?> queryVector = JVectorUtils.toVectorFloat(query);

            assertThat(search(cachingGraph, queryVector)).isEqualTo(search(onDiskGraph, queryVector));
        }
    }

    private static List<SearchResult.NodeScore> search(ImmutableGraphIndex graph, VectorFloat<?> query) throws Exception
    {
        try (GraphSearcher searcher = new GraphSearcher(graph))
        {
            var view = (ImmutableGraphIndex.ScoringView) searcher.getView();
            var scoreProvider = new DefaultSearchScoreProvider(view.rerankerFor(query, SIMILARITY));
            SearchResult result = searcher.search(scoreProvider, 10, 10, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, Bits.ALL);
            return List.of(result.getNodes());
        }
    }

    private static List<Integer> neighbors(NodesIterator it)
    {
        List<Integer> neighbors = new ArrayList<>();
        while (it.hasNext())
            neighbors.add(it.nextInt());
        return neighbors;
    }
}
