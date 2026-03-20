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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.IntConsumer;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.GrowableBitSet;

/**
 * An iterator over {@link SearchResult.NodeScore} backed by a {@link SearchResult} that resumes search
 * when the backing {@link SearchResult} is exhausted.
 */
public class AutoResumingNodeScoreIterator extends AbstractIterator<SearchResult.NodeScore>
{
    private final GraphSearcher<float[]> searcher;
    private final GraphIndex.View<float[]> view;
    private final NeighborSimilarity.ScoreFunction scoreFunction;
    private final NeighborSimilarity.ReRanker<float[]> reRanker;
    private final int topK;
    private final Bits acceptBits;
    private final boolean inMemory;
    private final String source;
    private final IntConsumer nodesVisitedConsumer;
    private Iterator<SearchResult.NodeScore> nodeScores = Collections.emptyIterator();
    private int cumulativeNodesVisited;

    // Defer initialization since it is only needed if we need to resume search
    private SkipVisitedBits visited = null;
    private SearchResult.NodeScore[] previousResult = null;

    /**
     * Create a new {@link AutoResumingNodeScoreIterator} that iterates over the provided {@link SearchResult}.
     * If the {@link SearchResult} is consumed, it retrieves the next {@link SearchResult} until the search returns
     * no more results.
     * @param searcher the {@link GraphSearcher} to use to search and resume search.
     * @param nodesVisitedConsumer a consumer that accepts the total number of nodes visited
     * @param inMemory whether the graph is in memory or on disk (used for trace logging)
     * @param source the source of the search (used for trace logging)
     * @param view the view used to read from disk. It will be closed when the iterator is closed.
     */
    public AutoResumingNodeScoreIterator(GraphSearcher<float[]> searcher,
                                         NeighborSimilarity.ScoreFunction scoreFunction,
                                         NeighborSimilarity.ReRanker<float[]> reRanker,
                                         int topK,
                                         Bits acceptBits,
                                         IntConsumer nodesVisitedConsumer,
                                         boolean inMemory,
                                         String source,
                                         GraphIndex.View<float[]> view)
    {
        this.searcher = searcher;
        this.scoreFunction = scoreFunction;
        this.reRanker = reRanker;
        this.topK = topK;
        this.acceptBits = acceptBits;

        this.cumulativeNodesVisited = 0;
        this.nodesVisitedConsumer = nodesVisitedConsumer;
        this.inMemory = inMemory;
        this.source = source;
        this.view = view;
    }

    @Override
    protected SearchResult.NodeScore computeNext()
    {
        if (nodeScores.hasNext())
            return nodeScores.next();

        // Add result from previous search to visited bits
        if (previousResult != null)
        {
            if (visited == null)
                visited = new SkipVisitedBits(acceptBits, previousResult.length);
            visited.visited(previousResult);
        }
        Bits bits = visited == null ? acceptBits : visited;
        SearchResult nextResult = searcher.search(scoreFunction, reRanker, topK, bits);

        // Record metrics (we add here instead of overwriting because re-queries are expensive proportional to the
        // number of visited nodes and even though we throw away some of those results, it helps us determine the
        // right path for brute force vs. ANN)
        cumulativeNodesVisited += nextResult.getVisitedCount();

        if (Tracing.isTracing())
        {
            Tracing.trace("{} based ANN {} for topK {} visited {} nodes to return {} results from {}",
                          inMemory ? "Memory" : "Disk", previousResult == null ? "initial" : "re-query",
                          topK, nextResult.getVisitedCount(), nextResult.getNodes().length, source);
        }

        previousResult = nextResult.getNodes();
        // If the next result is empty, we are done searching.
        nodeScores = Arrays.stream(nextResult.getNodes()).iterator();
        return nodeScores.hasNext() ? nodeScores.next() : endOfData();
    }

    @Override
    public void close()
    {
        nodesVisitedConsumer.accept(cumulativeNodesVisited);
        FileUtils.closeQuietly(view);
    }

    private static class SkipVisitedBits implements Bits
    {
        private final Bits acceptBits;
        private final GrowableBitSet visited;

        SkipVisitedBits(Bits acceptBits, int initialBits)
        {
            this.acceptBits = acceptBits;
            this.visited = new GrowableBitSet(initialBits);
        }

        void visited(SearchResult.NodeScore[] nodes)
        {
            for (SearchResult.NodeScore nodeScore : nodes)
                visited.set(nodeScore.node);
        }

        @Override
        public boolean get(int i)
        {
            return acceptBits.get(i) && !visited.get(i);
        }

        @Override
        public int length()
        {
            return acceptBits.length();
        }
    }
}