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
package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.AtomicRatio;
import org.apache.cassandra.index.sai.utils.ListRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.tracing.Tracing;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

/**
 * Executes ann search against the graph for an individual index segment.
 */
public class V2VectorIndexSearcher extends IndexSearcher implements SegmentOrdering
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final JVectorLuceneOnDiskGraph graph;
    private final PrimaryKey.Factory keyFactory;
    private int globalBruteForceRows; // not final so test can inject its own setting
    private final AtomicRatio actualExpectedRatio = new AtomicRatio();
    private final ThreadLocal<SparseFixedBitSet> cachedBitSets;

    public V2VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                 PerIndexFiles perIndexFiles,
                                 SegmentMetadata segmentMetadata,
                                 IndexDescriptor indexDescriptor,
                                 IndexContext indexContext) throws IOException
    {
        this(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext, new CassandraOnDiskHnsw(segmentMetadata.componentMetadatas, perIndexFiles, indexContext));
    }

    protected V2VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                    PerIndexFiles perIndexFiles,
                                    SegmentMetadata segmentMetadata,
                                    IndexDescriptor indexDescriptor,
                                    IndexContext indexContext,
                                    JVectorLuceneOnDiskGraph graph) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);
        this.graph = graph;
        this.keyFactory = PrimaryKey.factory(indexContext.comparator(), indexContext.indexFeatureSet());
        cachedBitSets = ThreadLocal.withInitial(() -> new SparseFixedBitSet(graph.size()));

        globalBruteForceRows = Integer.MAX_VALUE;
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public RangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        PostingList results = searchPosting(context, exp, keyRange, limit);
        return toPrimaryKeyIterator(results, context);
    }

    private PostingList searchPosting(QueryContext context, Expression exp, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.ANN && exp.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        if (exp.getEuclideanSearchThreshold() > 0)
            limit = 100000;
        int topK = topKFor(limit);
        float[] queryVector = exp.lower.value.vector;

        BitsOrPostingList bitsOrPostingList = bitsOrPostingListForKeyRange(context, keyRange, queryVector, topK);
        if (bitsOrPostingList.skipANN())
            return bitsOrPostingList.postingList();

        var vectorPostings = graph.search(queryVector, topK, exp.getEuclideanSearchThreshold(), limit, bitsOrPostingList.getBits(), context);
        if (bitsOrPostingList.rawExpectedNodes >= 0)
            updateExpectedNodes(vectorPostings.getVisitedCount(), bitsOrPostingList.rawExpectedNodes);
        return vectorPostings;
    }

    /**
     * @return the topK >= `limit` results to ask the index to search for.  This allows
     * us to compensate for using lossily-compressed vectors during the search, by
     * searching deeper in the graph.
     */
    private int topKFor(int limit)
    {
        var cv = graph.getCompressedVectors();
        // uncompressed indexes don't need to over-search
        if (cv == null)
            return limit;

        // compute the factor `n` to multiply limit by to increase the number of results from the index.
        var n = 0.509 + 9.491 * pow(limit, -0.402); // f(1) = 10.0, f(100) = 2.0, f(1000) = 1.1
        // The function becomes less than 1 at limit ~= 1583.4
        n = max(1.0, n);

        // 2x results at limit=100 is enough for all our tested data sets to match uncompressed recall,
        // except for the ada002 vectors that compress at a 32x ratio.  For ada002, we need 3x results
        // with PQ, and 4x for BQ.
        if (cv instanceof BinaryQuantization)
            n *= 2;
        else if ((double) cv.getOriginalSize() / cv.getCompressedSize() > 16.0)
            n *= 1.5;

        return (int) (n * limit);
    }

    /**
     * Return bit set if needs to search HNSW; otherwise return posting list to bypass HNSW
     */
    private BitsOrPostingList bitsOrPostingListForKeyRange(QueryContext context,
                                                           AbstractBounds<PartitionPosition> keyRange,
                                                           float[] queryVector,
                                                           int topK) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
                return new BitsOrPostingList(context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph));

            PrimaryKey firstPrimaryKey = keyFactory.createTokenOnly(keyRange.left.getToken());

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.ceiling(firstPrimaryKey);
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

            // If num of matches are not bigger than topK, skip ANN.
            // (nRows should not include shadowed rows, but context doesn't break those out by segment,
            // so we will live with the inaccuracy.)
            int nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            int maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(topK, nRows));
            logAndTrace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                        nRows, maxBruteForceRows, graph.size(), topK);
            // if we have a small number of results then let TopK processor do exact NN computation
            if (nRows <= maxBruteForceRows)
            {
                var segmentRowIds = new IntArrayList(nRows, 0);
                for (long i = minSSTableRowId; i <= maxSSTableRowId; i++)
                {
                    if (context.shouldInclude(i, primaryKeyMap))
                        segmentRowIds.add(metadata.toSegmentRowId(i));
                }
                var postings = findTopApproximatePostings(queryVector, segmentRowIds, topK);
                return new BitsOrPostingList(new ArrayPostingList(postings));
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

            return new BitsOrPostingList(bits, getRawExpectedNodes(topK, nRows));
        }
    }

    private int[] findTopApproximatePostings(float[] queryVector, IntArrayList segmentRowIds, int topK) throws IOException
    {
        var cv = graph.getCompressedVectors();
        if (cv == null || segmentRowIds.size() <= topK)
            return segmentRowIds.toIntArray();

        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        var scoreFunction = cv.approximateScoreFunctionFor(queryVector, similarityFunction);

        ArrayList<SearchResult.NodeScore> pairs = new ArrayList<>(segmentRowIds.size());
        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (int i = 0; i < segmentRowIds.size(); i++)
            {
                int segmentRowId = segmentRowIds.getInt(i);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal < 0)
                    continue;

                var score = scoreFunction.similarityTo(ordinal);
                pairs.add(new SearchResult.NodeScore(segmentRowId, score));
            }
        }
        // sort descending
        pairs.sort((a, b) -> Float.compare(b.score, a.score));
        int end = Math.min(pairs.size(), topK) - 1;
        int[] postings = new int[end + 1];
        // top K ascending
        for (int i = end; i >= 0; i--)
            postings[end - i] = pairs.get(i).node;
        return postings;
    }

    private long getMaxSSTableRowId(PrimaryKeyMap primaryKeyMap, PartitionPosition right)
    {
        // if the right token is the minimum token, there is no upper bound on the keyRange and
        // we can save a lookup by using the maxSSTableRowId
        if (right.isMinimum())
            return metadata.maxSSTableRowId;

        PrimaryKey lastPrimaryKey = keyFactory.createTokenOnly(right.getToken());
        long max = primaryKeyMap.floor(lastPrimaryKey);
        if (max < 0)
            return metadata.maxSSTableRowId;
        return max;
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals)
    {
        int expectedNodes = expectedNodesVisited(limit, nPermittedOrdinals);
        // ANN index will do a bunch of extra work besides the full comparisons (performing PQ similarity for each edge);
        // brute force from sstable will also do a bunch of extra work (going through trie index to look up row).
        // VSTODO I'm not sure which one is more expensive (and it depends on things like sstable chunk cache hit ratio)
        // so I'm leaving it as a 1:1 ratio for now.
        return max(limit, expectedNodes);
    }

    private int expectedNodesVisited(int limit, int nPermittedOrdinals)
    {
        return (int) (getObservedNodesRatio() * getRawExpectedNodes(limit, nPermittedOrdinals));
    }

    /** the ratio of nodes visited by a graph search, to our estimate */
    private double getObservedNodesRatio()
    {
        return actualExpectedRatio.getUpdateCount() >= 10 ? actualExpectedRatio.get() : 1.0;
    }

    private void updateExpectedNodes(int actualNodesVisited, int rawExpectedNodes)
    {
        assert rawExpectedNodes >= 0 : rawExpectedNodes;
        assert actualNodesVisited >= 0 : actualNodesVisited;
        double ratio = getObservedNodesRatio();
        var expectedNodes = (int) (ratio * rawExpectedNodes);
        if (actualNodesVisited >= 1000 && (actualNodesVisited > 2 * expectedNodes || actualNodesVisited < 0.5 * expectedNodes))
            logger.warn("Predicted visiting {} nodes, but actually visited {} (observed:predicted ratio is {})",
                        expectedNodes, actualNodesVisited, ratio);
        actualExpectedRatio.updateAndGet(actualNodesVisited, rawExpectedNodes);
    }

    private SparseFixedBitSet bitSetForSearch()
    {
        var bits = cachedBitSets.get();
        bits.clear();
        return bits;
    }

    @Override
    public RangeIterator limitToTopResults(QueryContext context, List<PrimaryKey> keys, Expression exp, int limit) throws IOException
    {
        // create a sublist of the keys within this segment's bounds
        int minIndex = Collections.binarySearch(keys, metadata.minKey);
        minIndex = minIndex < 0 ? -minIndex - 1 : minIndex;
        int maxIndex = Collections.binarySearch(keys, metadata.maxKey);
        maxIndex = maxIndex < 0 ? -maxIndex - 1 : maxIndex + 1;
        List<PrimaryKey> keysInRange = keys.subList(minIndex, maxIndex);
        if (keysInRange.isEmpty())
            return RangeIterator.empty();

        int numRows = keysInRange.size();
        logAndTrace("SAI predicates produced {} rows out of limit {}", numRows, limit);
        if (numRows <= limit)
            return new ListRangeIterator(metadata.minKey, metadata.maxKey, keysInRange);

        int topK = topKFor(limit);
        // if we are brute forcing, we want to build a list of segment row ids, but if not,
        // we want to build a bitset of ordinals corresponding to the rows.  We won't know which
        // path to take until we have an accurate key count.
        SparseFixedBitSet bits = bitSetForSearch();
        IntArrayList rowIds = new IntArrayList();
        try (var primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
             var ordinalsView = graph.getOrdinalsView())
        {
            for (PrimaryKey primaryKey : keysInRange)
            {
                long sstableRowId = primaryKeyMap.exactRowIdForPrimaryKey(primaryKey);
                // skip rows that are not in our segment (or more preciesely, have no vectors that were indexed)
                // or are not in this segment (exactRowIdForPrimaryKey returns a negative value for not found)
                if (sstableRowId < metadata.minSSTableRowId)
                    continue;

                // if sstable row id has exceeded current ANN segment, stop
                if (sstableRowId > metadata.maxSSTableRowId)
                    break;

                int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                rowIds.add(segmentRowId);
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal >= 0)
                    bits.set(ordinal);
            }
        }

        numRows = rowIds.size();
        var maxBruteForceRows = min(globalBruteForceRows, maxBruteForceRows(topK, numRows));
        logAndTrace("{} rows relevant to current sstable; max brute force rows is {} for index with {} nodes, LIMIT {}",
                    numRows, maxBruteForceRows, graph.size(), limit);
        if (numRows == 0) {
            return RangeIterator.empty();
        }

        if (numRows <= maxBruteForceRows)
        {
            // brute force using the in-memory compressed vectors to cut down the number of results returned
            var queryVector = exp.lower.value.vector;
            var postings = findTopApproximatePostings(queryVector, rowIds, topK);
            return toPrimaryKeyIterator(new ArrayPostingList(postings), context);
        }
        // else ask the index to perform a search limited to the bits we created
        float[] queryVector = exp.lower.value.vector;
        var results = graph.search(queryVector, topK, limit, bits, context);
        updateExpectedNodes(results.getVisitedCount(), getRawExpectedNodes(topK, numRows));
        return toPrimaryKeyIterator(results, context);
    }

    private int getRawExpectedNodes(int topK, int nPermittedOrdinals)
    {
        return VectorMemtableIndex.expectedNodesVisited(topK, nPermittedOrdinals, graph.size());
    }

    private void logAndTrace(String message, Object... args)
    {
        logger.trace(message, args);
        Tracing.trace(message, args);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close() throws IOException
    {
        graph.close();
    }

    private static class BitsOrPostingList
    {
        private final Bits bits;
        private final int rawExpectedNodes;
        private final PostingList postingList;

        public BitsOrPostingList(Bits bits, int rawExpectedNodes)
        {
            this.bits = bits;
            this.rawExpectedNodes = rawExpectedNodes;
            this.postingList = null;
        }

        public BitsOrPostingList(Bits bits)
        {
            this.bits = bits;
            this.postingList = null;
            this.rawExpectedNodes = -1;
        }

        public BitsOrPostingList(PostingList postingList)
        {
            this.bits = null;
            this.postingList = Preconditions.checkNotNull(postingList);
            this.rawExpectedNodes = -1;
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
