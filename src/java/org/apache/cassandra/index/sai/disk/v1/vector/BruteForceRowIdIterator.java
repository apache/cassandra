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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.NeighborQueue;
import io.github.jbellis.jvector.graph.NeighborSimilarity;


/**
 * An iterator over {@link RowIdWithScore} that lazily consumes from a {@link NeighborQueue} of approximate scores.
 * <p>
 * The idea is that we maintain the same level of accuracy as we would get from a graph search, by re-ranking the top
 * `k` best approximate scores at a time with the full resolution vectors to return the top `limit`.
 * <p>
 * For example, suppose that limit=3 and k=5 and we have ten elements.  After our first re-ranking batch, we have
 *   ABDEF?????
 * We will return A, B, and D; if more elements are requested, we will re-rank another 5 (so three more, including
 * the two remaining from the first batch).  Here we uncover C, G, and H, and order them appropriately:
 *      CEFGH??
 * This illustrates that, also like a graph search, we only guarantee ordering of results within a re-ranking batch,
 * not globally.
 * <p>
 * Note that we deliberately do not fetch new items from the approximate list until the first batch of `limit`-many
 * is consumed. We do this because we expect that most often the first limit-many will pass the final verification
 * and only query more if some didn't (e.g. because the vector was deleted in a newer sstable).
 * <p>
 * As an implementation detail, we use a heap to maintain state rather than a List and sorting.
 */
@NotThreadSafe
public class BruteForceRowIdIterator extends AbstractIterator<RowIdWithScore>
{
    // We use two binary heaps (NeighborQueue) because we do not need an eager ordering of
    // these results. Depending on how many sstables the query hits and the relative scores of vectors from those
    // sstables, we may not need to return more than the first handful of scores.
    // Heap with compressed vector scores
    private final NeighborQueue approximateScoreQueue;
    private final SegmentRowIdOrdinalPairs segmentOrdinalPairs;
    // Use the jvector NeighborQueue to avoid unnecessary object allocations
    private final NeighborQueue exactScoreQueue;
    private final NeighborSimilarity.ExactScoreFunction reranker;
    private final GraphIndex.View<float[]> view;
    private final int topK;
    private final int limit;
    private int rerankedCount;

    /**
     * @param approximateScoreQueue A heap of indexes ordered by their approximate similarity scores
     * @param segmentOrdinalPairs   A mapping from the index in the approximateScoreQueue to the node's rowId and ordinal
     * @param reranker              A function that takes a graph ordinal and returns the exact similarity score
     * @param limit                 The query limit
     * @param topK                  The number of vectors to resolve and score before returning results
     * @param view                  The view of the graph, passed so we can close it when the iterator is closed
     */
    public BruteForceRowIdIterator(NeighborQueue approximateScoreQueue,
                                   SegmentRowIdOrdinalPairs segmentOrdinalPairs,
                                   NeighborSimilarity.ExactScoreFunction reranker,
                                   int limit,
                                   int topK,
                                   GraphIndex.View<float[]> view)
    {
        this.approximateScoreQueue = approximateScoreQueue;
        this.segmentOrdinalPairs = segmentOrdinalPairs;
        this.exactScoreQueue = new NeighborQueue(limit, true);
        this.reranker = reranker;
        assert topK >= limit : "topK must be greater than or equal to limit. Found: " + topK + " < " + limit;
        this.limit = limit;
        this.topK = topK;
        this.rerankedCount = topK; // placeholder to kick off computeNext
        this.view = view;
    }

    @Override
    protected RowIdWithScore computeNext()
    {
        int consumed = rerankedCount - exactScoreQueue.size();
        if (consumed >= limit)
        {
            // Refill the exactScoreQueue until it reaches topK exact scores, or the approximate score queue is empty
            while (approximateScoreQueue.size() > 0 && exactScoreQueue.size() < topK)
            {
                int segmentOrdinalIndex = approximateScoreQueue.pop();
                int rowId = segmentOrdinalPairs.getSegmentRowId(segmentOrdinalIndex);
                int ordinal = segmentOrdinalPairs.getOrdinal(segmentOrdinalIndex);
                float score = reranker.similarityTo(ordinal);
                exactScoreQueue.add(rowId, score);
            }
            rerankedCount = exactScoreQueue.size();
        }
        if (exactScoreQueue.size() == 0)
            return endOfData();

        float score = exactScoreQueue.topScore();
        int rowId = exactScoreQueue.pop();
        return new RowIdWithScore(rowId, score);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(view);
    }
}
