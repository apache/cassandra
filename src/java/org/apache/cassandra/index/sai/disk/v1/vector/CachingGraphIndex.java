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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Int2ObjectHashMap;

import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * Keeps the neighbors and vectors of the nodes closest to the entry node of an {@link OnDiskGraphIndex} in memory.
 * Every search starts at the entry node, so these nodes are read by almost every query. jvector 1.x provided this
 * class, but later versions do not.
 */
public class CachingGraphIndex implements ImmutableGraphIndex
{
    /** Nodes within this many hops of the entry node are cached. */
    @VisibleForTesting
    static final int CACHE_DISTANCE = 3;

    private final OnDiskGraphIndex graph;
    // only written in the constructor
    private final Int2ObjectHashMap<CachedNode> cache = new Int2ObjectHashMap<>();
    private final long cacheBytesUsed;

    public CachingGraphIndex(OnDiskGraphIndex graph)
    {
        this.graph = graph;
        this.cacheBytesUsed = loadCache();
    }

    private long loadCache()
    {
        long bytesUsed = 0;
        try (OnDiskGraphIndex.View view = graph.getView())
        {
            NodeAtLevel entry = view.entryNode();
            if (entry == null)
                return 0;

            // breadth-first, so every node within CACHE_DISTANCE hops is reached by its shortest path
            ArrayDeque<int[]> queue = new ArrayDeque<>();
            queue.add(new int[]{ entry.node, 0 });
            while (!queue.isEmpty())
            {
                int[] next = queue.poll();
                int node = next[0];
                int distance = next[1];
                if (cache.containsKey(node))
                    continue;

                NodesIterator it = view.getNeighborsIterator(0, node);
                int[] neighbors = new int[it.size()];
                for (int i = 0; i < neighbors.length; i++)
                    neighbors[i] = it.nextInt();
                CachedNode cached = new CachedNode(view.getVector(node), neighbors);
                cache.put(node, cached);
                bytesUsed += RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + cached.vector.ramBytesUsed() + RamUsageEstimator.sizeOf(neighbors);

                if (distance < CACHE_DISTANCE)
                {
                    for (int neighbor : neighbors)
                    {
                        if (!cache.containsKey(neighbor))
                            queue.add(new int[]{ neighbor, distance + 1 });
                    }
                }
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        return bytesUsed;
    }

    @VisibleForTesting
    int cachedNodeCount()
    {
        return cache.size();
    }

    @Override
    public CachedView getView()
    {
        return new CachedView(graph.getView());
    }

    @Override
    public NodesIterator getNodes(int level)
    {
        return graph.getNodes(level);
    }

    @Override
    public int maxDegree()
    {
        return graph.maxDegree();
    }

    @Override
    public List<Integer> maxDegrees()
    {
        return graph.maxDegrees();
    }

    @Override
    public int getDimension()
    {
        return graph.getDimension();
    }

    @Override
    public int getIdUpperBound()
    {
        return graph.getIdUpperBound();
    }

    @Override
    public boolean isHierarchical()
    {
        return graph.isHierarchical();
    }

    @Override
    public int getMaxLevel()
    {
        return graph.getMaxLevel();
    }

    @Override
    public int getDegree(int level)
    {
        return graph.getDegree(level);
    }

    @Override
    public double getAverageDegree(int level)
    {
        return graph.getAverageDegree(level);
    }

    @Override
    public int size(int level)
    {
        return graph.size(level);
    }

    @Override
    public long ramBytesUsed()
    {
        return graph.ramBytesUsed() + cacheBytesUsed;
    }

    @Override
    public void close() throws IOException
    {
        graph.close();
    }

    private static final class CachedNode
    {
        private final VectorFloat<?> vector;
        private final int[] neighbors;

        private CachedNode(VectorFloat<?> vector, int[] neighbors)
        {
            this.vector = vector;
            this.neighbors = neighbors;
        }
    }

    /**
     * A view that answers from the cache where it can and from disk otherwise. Not thread-safe, like the
     * {@link OnDiskGraphIndex.View} it wraps.
     */
    public class CachedView implements ImmutableGraphIndex.ScoringView, RandomAccessVectorValues
    {
        private final OnDiskGraphIndex.View view;

        private CachedView(OnDiskGraphIndex.View view)
        {
            this.view = view;
        }

        @Override
        public NodesIterator getNeighborsIterator(int level, int node)
        {
            CachedNode cached = level == 0 ? cache.get(node) : null;
            if (cached != null)
                return new NodesIterator.ArrayNodesIterator(cached.neighbors, cached.neighbors.length);
            return view.getNeighborsIterator(level, node);
        }

        @Override
        public void processNeighbors(int level, int node, ScoreFunction scoreFunction, IntMarker visited, NeighborProcessor neighborProcessor)
        {
            CachedNode cached = level == 0 ? cache.get(node) : null;
            if (cached == null)
            {
                view.processNeighbors(level, node, scoreFunction, visited, neighborProcessor);
                return;
            }

            for (int neighbor : cached.neighbors)
            {
                if (visited.mark(neighbor))
                    neighborProcessor.process(neighbor, scoreFunction.similarityTo(neighbor));
            }
        }

        @Override
        public int size()
        {
            return view.size();
        }

        @Override
        public int dimension()
        {
            return view.dimension();
        }

        @Override
        public NodeAtLevel entryNode()
        {
            return view.entryNode();
        }

        @Override
        public Bits liveNodes()
        {
            return view.liveNodes();
        }

        @Override
        public int getIdUpperBound()
        {
            return view.getIdUpperBound();
        }

        @Override
        public boolean contains(int level, int node)
        {
            return view.contains(level, node);
        }

        @Override
        public VectorFloat<?> getVector(int node)
        {
            CachedNode cached = cache.get(node);
            return cached != null ? cached.vector.copy() : view.getVector(node);
        }

        @Override
        public void getVectorInto(int node, VectorFloat<?> destination, int offset)
        {
            CachedNode cached = cache.get(node);
            if (cached != null)
                destination.copyFrom(cached.vector, 0, offset, cached.vector.length());
            else
                view.getVectorInto(node, destination, offset);
        }

        @Override
        public boolean isValueShared()
        {
            return false;
        }

        @Override
        public RandomAccessVectorValues copy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScoreFunction.ExactScoreFunction rerankerFor(VectorFloat<?> queryVector, VectorSimilarityFunction vsf)
        {
            // reads vectors through getVectorInto, so cached vectors are not read from disk
            return RandomAccessVectorValues.super.rerankerFor(queryVector, vsf);
        }

        @Override
        public ScoreFunction.ApproximateScoreFunction approximateScoreFunctionFor(VectorFloat<?> queryVector, VectorSimilarityFunction vsf)
        {
            return view.approximateScoreFunctionFor(queryVector, vsf);
        }

        @Override
        public void close() throws IOException
        {
            view.close();
        }
    }
}
