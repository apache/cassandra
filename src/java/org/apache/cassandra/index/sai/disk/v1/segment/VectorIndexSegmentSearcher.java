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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.VectorQueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.vector.DiskAnn;
import org.apache.cassandra.index.sai.disk.v1.vector.OptimizeFor;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeListIterator;
import org.apache.cassandra.index.sai.memory.VectorMemoryIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.IntArrayPostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.AtomicRatio;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.tracing.Tracing;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Executes ANN search against a vector graph for an individual index segment.
 */
public class VectorIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final DiskAnn graph;
    private final int globalBruteForceRows;
    private final AtomicRatio actualExpectedRatio = new AtomicRatio();
    private final ThreadLocal<SparseFixedBitSet> cachedBitSets;
    private final OptimizeFor optimizeFor;

    VectorIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                               PerColumnIndexFiles perIndexFiles,
                               SegmentMetadata segmentMetadata,
                               StorageAttachedIndex index) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, index);
        graph = new DiskAnn(segmentMetadata.componentMetadatas, perIndexFiles, index.indexWriterConfig());
        cachedBitSets = ThreadLocal.withInitial(() -> new SparseFixedBitSet(graph.size()));
        globalBruteForceRows = Integer.MAX_VALUE;
        optimizeFor = index.indexWriterConfig().getOptimizeFor();
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public KeyRangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context) throws IOException
    {
        int limit = context.vectorContext().limit();

        if (logger.isTraceEnabled())
            logger.trace(index.identifier().logMessage("Searching on expression '{}'..."), exp);

        if (exp.getIndexOperator() != Expression.IndexOperator.ANN)
            throw new IllegalArgumentException(index.identifier().logMessage("Unsupported expression during ANN index query: " + exp));

        int topK = optimizeFor.topKFor(limit);
        BitsOrPostingList bitsOrPostingList = bitsOrPostingListForKeyRange(context.vectorContext(), keyRange, topK);
        if (bitsOrPostingList.skipANN())
            return toPrimaryKeyIterator(bitsOrPostingList.postingList(), context);

        float[] queryVector = index.termType().decomposeVector(exp.lower().value.raw.duplicate());
        var vectorPostings = graph.search(queryVector, topK, limit, bitsOrPostingList.getBits());
        if (bitsOrPostingList.expectedNodesVisited >= 0)
            updateExpectedNodes(vectorPostings.getVisitedCount(), bitsOrPostingList.expectedNodesVisited);
        return toPrimaryKeyIterator(vectorPostings, context);
    }

    /**
     * Return bit set we need to search the graph; otherwise return posting list to bypass the graph
     */
    private BitsOrPostingList bitsOrPostingListForKeyRange(VectorQueryContext context, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
                return new BitsOrPostingList(context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph));

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.ceiling(keyRange.left.getToken());
            // If we didn't find the first key, we won't find the last primary key either
            if (minSSTableRowId < 0)
                return new BitsOrPostingList(PostingList.EMPTY);
            long maxSSTableRowId = getMaxSSTableRowId(primaryKeyMap, keyRange.right);

            if (minSSTableRowId > maxSSTableRowId)
                return new BitsOrPostingList(PostingList.EMPTY);

            // if it covers entire segment, skip bit set
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
                return new BitsOrPostingList(context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph));

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // If num of matches are not bigger than limit, skip ANN.
            // (nRows should not include shadowed rows, but context doesn't break those out by segment,
            // so we will live with the inaccuracy.)
            var nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            int maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(limit, nRows, graph.size()));
            logger.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                         nRows, maxBruteForceRows, graph.size(), limit);
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                          nRows, maxBruteForceRows, graph.size(), limit);
            if (nRows <= maxBruteForceRows)
            {
                IntArrayList postings = new IntArrayList(Math.toIntExact(nRows), -1);
                for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                {
                    if (context.shouldInclude(sstableRowId, primaryKeyMap))
                        postings.addInt(metadata.toSegmentRowId(sstableRowId));
                }
                return new BitsOrPostingList(new IntArrayPostingList(postings.toIntArray()));
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            SparseFixedBitSet bits = bitSetForSearch();
            boolean hasMatches = false;
            try (var ordinalsView = graph.getOrdinalsView())
            {
                for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                {
                    if (context.shouldInclude(sstableRowId, primaryKeyMap))
                    {
                        int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                        int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                        if (ordinal >= 0)
                        {
                            bits.set(ordinal);
                            hasMatches = true;
                        }
                    }
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            if (!hasMatches)
                return new BitsOrPostingList(PostingList.EMPTY);

            return new BitsOrPostingList(bits, VectorMemoryIndex.expectedNodesVisited(limit, nRows, graph.size()));
        }
    }

    private long getMaxSSTableRowId(PrimaryKeyMap primaryKeyMap, PartitionPosition right)
    {
        // if the right token is the minimum token, there is no upper bound on the keyRange and
        // we can save a lookup by using the maxSSTableRowId
        if (right.isMinimum())
            return metadata.maxSSTableRowId;

        long max = primaryKeyMap.floor(right.getToken());
        if (max < 0)
            return metadata.maxSSTableRowId;
        return max;
    }

    private SparseFixedBitSet bitSetForSearch()
    {
        var bits = cachedBitSets.get();
        bits.clear();
        return bits;
    }

    @Override
    public KeyRangeIterator limitToTopKResults(QueryContext context, List<PrimaryKey> primaryKeys, Expression expression) throws IOException
    {
        int limit = context.vectorContext().limit();
        // VSTODO would it be better to do a binary search to find the boundaries?
        List<PrimaryKey> keysInRange = primaryKeys.stream()
                                                  .dropWhile(k -> k.compareTo(metadata.minKey) < 0)
                                                  .takeWhile(k -> k.compareTo(metadata.maxKey) <= 0)
                                                  .collect(Collectors.toList());
        if (keysInRange.isEmpty())
            return KeyRangeIterator.empty();
        int topK = optimizeFor.topKFor(limit);
        if (shouldUseBruteForce(topK, limit, keysInRange.size()))
            return new KeyRangeListIterator(metadata.minKey, metadata.maxKey, keysInRange);

        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // the iterator represents keys from the whole table -- we'll only pull of those that
            // are from our own token range, so we can use row ids to order the results by vector similarity.
            var maxSegmentRowId = metadata.toSegmentRowId(metadata.maxSSTableRowId);
            SparseFixedBitSet bits = bitSetForSearch();
            var rowIds = new IntArrayList();
            try (var ordinalsView = graph.getOrdinalsView())
            {
                for (PrimaryKey primaryKey : keysInRange)
                {
                    long sstableRowId = primaryKeyMap.rowIdFromPrimaryKey(primaryKey);
                    // skip rows that are not in our segment (or more preciesely, have no vectors that were indexed)
                    // or are not in this segment (exactRowIdForPrimaryKey returns a negative value for not found)
                    if (sstableRowId < metadata.minSSTableRowId)
                        continue;

                    // if sstable row id has exceeded current ANN segment, stop
                    if (sstableRowId > metadata.maxSSTableRowId)
                        break;

                    int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                    rowIds.add(segmentRowId);
                    // VSTODO now that we know the size of keys evaluated, is it worth doing the brute
                    // force check eagerly to potentially skip the PK to sstable row id to ordinal lookup?
                    int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                    if (ordinal >= 0)
                        bits.set(ordinal);
                }
            }

            if (shouldUseBruteForce(topK, limit, rowIds.size()))
                return toPrimaryKeyIterator(new IntArrayPostingList(rowIds.toIntArray()), context);

            // else ask the index to perform a search limited to the bits we created
            float[] queryVector = index.termType().decomposeVector(expression.lower().value.raw.duplicate());
            var results = graph.search(queryVector, topK, limit, bits);
            updateExpectedNodes(results.getVisitedCount(), expectedNodesVisited(topK, maxSegmentRowId, graph.size()));
            return toPrimaryKeyIterator(results, context);
        }
    }

    private boolean shouldUseBruteForce(int topK, int limit, int numRows)
    {
        // if we have a small number of results then let TopK processor do exact NN computation
        var maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(topK, numRows, graph.size()));
        logger.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                     numRows, maxBruteForceRows, graph.size(), limit);
        Tracing.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                      numRows, maxBruteForceRows, graph.size(), limit);
        return numRows <= maxBruteForceRows;
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodes = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        // ANN index will do a bunch of extra work besides the full comparisons (performing PQ similarity for each edge);
        // brute force from sstable will also do a bunch of extra work (going through trie index to look up row).
        // VSTODO I'm not sure which one is more expensive (and it depends on things like sstable chunk cache hit ratio)
        // so I'm leaving it as a 1:1 ratio for now.
        return max(limit, expectedNodes);
    }

    private int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        var observedRatio = actualExpectedRatio.getUpdateCount() >= 10 ? actualExpectedRatio.get() : 1.0;
        return (int) (observedRatio * VectorMemoryIndex.expectedNodesVisited(limit, nPermittedOrdinals, graphSize));
    }

    private void updateExpectedNodes(int actualNodesVisited, int expectedNodesVisited)
    {
        assert expectedNodesVisited >= 0 : expectedNodesVisited;
        assert actualNodesVisited >= 0 : actualNodesVisited;
        if (actualNodesVisited >= 1000 && actualNodesVisited > 2 * expectedNodesVisited || expectedNodesVisited > 2 * actualNodesVisited)
            logger.warn("Predicted visiting {} nodes, but actually visited {}", expectedNodesVisited, actualNodesVisited);
        actualExpectedRatio.update(actualNodesVisited, expectedNodesVisited);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("index", index).toString();
    }

    @Override
    public void close() throws IOException
    {
        graph.close();
    }

    private static class BitsOrPostingList
    {
        private final Bits bits;
        private final int expectedNodesVisited;
        private final PostingList postingList;

        public BitsOrPostingList(@Nullable Bits bits, int expectedNodesVisited)
        {
            this.bits = bits;
            this.expectedNodesVisited = expectedNodesVisited;
            this.postingList = null;
        }

        public BitsOrPostingList(@Nullable Bits bits)
        {
            this.bits = bits;
            this.postingList = null;
            this.expectedNodesVisited = -1;
        }

        public BitsOrPostingList(PostingList postingList)
        {
            this.bits = null;
            this.postingList = Preconditions.checkNotNull(postingList);
            this.expectedNodesVisited = -1;
        }

        @Nullable
        public Bits getBits()
        {
            Preconditions.checkState(!skipANN());
            return bits;
        }

        public PostingList postingList()
        {
            Preconditions.checkState(skipANN());
            return postingList;
        }

        public boolean skipANN()
        {
            return postingList != null;
        }
    }
}
