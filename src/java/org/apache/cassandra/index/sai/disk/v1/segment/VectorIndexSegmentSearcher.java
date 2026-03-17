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
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.vector.BruteForceRowIdIterator;
import org.apache.cassandra.index.sai.disk.v1.vector.DiskAnn;
import org.apache.cassandra.index.sai.disk.v1.vector.NeighborQueueRowIdIterator;
import org.apache.cassandra.index.sai.disk.v1.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.v1.vector.OptimizeFor;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.disk.v1.vector.RowIdToPrimaryKeyWithScoreIterator;
import org.apache.cassandra.index.sai.disk.v1.vector.RowIdWithScore;
import org.apache.cassandra.index.sai.disk.v1.vector.SegmentRowIdOrdinalPairs;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.VectorMemoryIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.AtomicRatio;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.NeighborQueue;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseFixedBitSet;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Executes ANN search against a vector graph for an individual index segment.
 */
public class VectorIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // If true, use brute force. If false, use graph search. If null, use the normal logic.
    @VisibleForTesting
    public static Boolean FORCE_BRUTE_FORCE_ANN = null;

    private final DiskAnn graph;
    private final AtomicRatio actualExpectedRatio = new AtomicRatio();
    private final ThreadLocal<SparseFixedBitSet> cachedBitSets;
    private final OptimizeFor optimizeFor;
    private final ColumnMetadata column;

    VectorIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                               SSTableId sstableId,
                               PerColumnIndexFiles perIndexFiles,
                               SegmentMetadata segmentMetadata,
                               StorageAttachedIndex index) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, index);
        graph = new DiskAnn(segmentMetadata.componentMetadatas, perIndexFiles, index.indexWriterConfig(), sstableId);
        cachedBitSets = ThreadLocal.withInitial(() -> new SparseFixedBitSet(graph.size()));
        optimizeFor = index.indexWriterConfig().getOptimizeFor();
        column = index.termType().columnMetadata();
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderBy(Expression orderer, AbstractBounds<PartitionPosition> keyRange, QueryContext context) throws IOException
    {
        int limit = context.limit();

        if (logger.isTraceEnabled())
            logger.trace(index.identifier().logMessage("Searching on expression '{}'..."), orderer);

        if (orderer.getIndexOperator() != Expression.IndexOperator.ANN)
            throw new IllegalArgumentException(index.identifier().logMessage("Unsupported expression during ANN index query: " + orderer));

        int topK = optimizeFor.topKFor(limit);

        float[] queryVector = index.termType().decomposeVector(orderer.lower().value.raw.duplicate());
        CloseableIterator<RowIdWithScore> result = searchInternal(keyRange, queryVector, limit, topK);
        return toScoreSortedIterator(result);
    }

    private CloseableIterator<RowIdWithScore> searchInternal(AbstractBounds<PartitionPosition> keyRange, float[] queryVector, int limit, int topK) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
                return searchInternalUnrestricted(queryVector, limit, topK);


            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.ceiling(keyRange.left.getToken());
            // If we didn't find the first key, we won't find the last primary key either
            if (minSSTableRowId < 0)
                return CloseableIterator.empty();
            long maxSSTableRowId = getMaxSSTableRowId(primaryKeyMap, keyRange.right);

            if (minSSTableRowId > maxSSTableRowId)
                return CloseableIterator.empty();

            // if it covers entire segment, skip bit set
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
                return searchInternalUnrestricted(queryVector, limit, topK);

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // If num of matches are not bigger than limit, skip graph search and lazily sort by brute force.
            int nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            int maxBruteForceRows = maxBruteForceRows(limit, nRows, graph.size());
            logger.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                         nRows, maxBruteForceRows, graph.size(), limit);
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                          nRows, maxBruteForceRows, graph.size(), limit);
            boolean shouldBruteForce = FORCE_BRUTE_FORCE_ANN == null ? nRows <= maxBruteForceRows : FORCE_BRUTE_FORCE_ANN;
            if (shouldBruteForce)
            {
                SegmentRowIdOrdinalPairs segmentOrdinalPairs = new SegmentRowIdOrdinalPairs(Math.toIntExact(nRows));
                try (OnDiskOrdinalsMap.OrdinalsView ordinalsView = graph.getOrdinalsView())
                {
                    for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
                    {
                        int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                        int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                        if (ordinal >= 0)
                            segmentOrdinalPairs.add(segmentRowId, ordinal);
                    }
                }
                return orderByBruteForce(queryVector, segmentOrdinalPairs, limit, topK);
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            SparseFixedBitSet bits = bitSetForSearch();
            boolean hasMatches = false;
            try (var ordinalsView = graph.getOrdinalsView())
            {
                for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
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
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            if (!hasMatches)
                return CloseableIterator.empty();

            int expectedNodesVisited = expectedNodesVisited(limit, bits.cardinality(), graph.size());
            IntConsumer nodesVisitedConsumer = nodesVisited -> updateExpectedNodes(nodesVisited, expectedNodesVisited);
            return graph.search(queryVector, topK, limit, bits, nodesVisitedConsumer);
        }
    }

    private CloseableIterator<RowIdWithScore> searchInternalUnrestricted(float[] queryVector, int limit, int topK)
    {
        int expectedNodesVisited = expectedNodesVisited(limit, graph.size(), graph.size());
        IntConsumer nodesVisitedConsumer = nodesVisited -> updateExpectedNodes(nodesVisited, expectedNodesVisited);
        return graph.search(queryVector, topK, limit, new Bits.MatchAllBits(graph.size()), nodesVisitedConsumer);
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
        SparseFixedBitSet bits = cachedBitSets.get();
        bits.clear();
        return bits;
    }

    /**
     * Produces a descending score ordered iterator over the rows in the given segment. Branches depending on the number
     * of rows to consider and whether the graph has compressed vectors available for faster comparisons.
     */
    private CloseableIterator<RowIdWithScore> orderByBruteForce(float[] queryVector, SegmentRowIdOrdinalPairs segmentOrdinalPairs, int limit, int topK) throws IOException
    {
        if (segmentOrdinalPairs.size() == 0)
            return CloseableIterator.empty();

        // If we have more than topK segmentOrdinalPairs, we do a two pass partial sort by first getting the approximate
        // similarity score via the PQ vectors that are already in memory and then by hitting disk to get the full
        // precision vectors to get the full precision similarity score.
        if (graph.getCompressedVectors() != null && segmentOrdinalPairs.size() > topK)
            return orderByBruteForceTwoPass(graph.getCompressedVectors(), queryVector, segmentOrdinalPairs, limit, topK);

        try (GraphIndex.View<float[]> view = graph.getView())
        {
            NeighborSimilarity.ExactScoreFunction esf = graph.getExactScoreFunction(queryVector, view);
            NeighborQueue scoredRowIds = segmentOrdinalPairs.mapToSegmentRowIdScoreHeap(esf);
            return new NeighborQueueRowIdIterator(scoredRowIds);
        }
        catch (Exception e)
        {
            throw new IOException(e);
        }
    }

    /**
     * Materialize the compressed vectors for the given segment row ids, put them into a priority queue ordered by
     * approximate similarity score, and then pass to the {@link BruteForceRowIdIterator} to lazily resolve the
     * full resolution ordering as needed.
     */
    private CloseableIterator<RowIdWithScore> orderByBruteForceTwoPass(CompressedVectors cv,
                                                                       float[] queryVector,
                                                                       SegmentRowIdOrdinalPairs segmentOrdinalPairs,
                                                                       int limit,
                                                                       int rerankK)
    {
        NeighborSimilarity.ApproximateScoreFunction scoreFunction = graph.getApproximateScoreFunction(queryVector);
        // Store the index of the (rowId, ordinal) pair from the segmentOrdinalPairs in the NodeQueue so that we can
        // retrieve both values with O(1) lookup when we need to resolve the full resolution score in the
        // BruteForceRowIdIterator.
        NeighborQueue approximateScoreHeap = segmentOrdinalPairs.mapToIndexScoreIterator(scoreFunction);
        GraphIndex.View<float[]> view = graph.getView();
        NeighborSimilarity.ExactScoreFunction esf = graph.getExactScoreFunction(queryVector, view);
        return new BruteForceRowIdIterator(approximateScoreHeap, segmentOrdinalPairs, esf, limit, rerankK, view);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderResultsBy(QueryContext context, List<PrimaryKey> results, Expression orderer) throws IOException
    {
        int limit = context.limit();
        // VSTODO would it be better to do a binary search to find the boundaries?
        List<PrimaryKey> keysInRange = results.stream()
                                              .dropWhile(k -> k.compareTo(metadata.minKey) < 0)
                                              .takeWhile(k -> k.compareTo(metadata.maxKey) <= 0)
                                              .collect(Collectors.toList());
        if (keysInRange.isEmpty())
            return CloseableIterator.empty();

        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // the iterator represents keys from the whole table -- we'll only pull of those that
            // are from our own token range, so we can use row ids to order the results by vector similarity.
            SegmentRowIdOrdinalPairs segmentOrdinalPairs = new SegmentRowIdOrdinalPairs(keysInRange.size());
            try (OnDiskOrdinalsMap.OrdinalsView ordinalsView = graph.getOrdinalsView())
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
                    // VSTODO now that we know the size of keys evaluated, is it worth doing the brute
                    // force check eagerly to potentially skip the PK to sstable row id to ordinal lookup?
                    int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                    if (ordinal >= 0)
                        segmentOrdinalPairs.add(segmentRowId, ordinal);
                }
            }

            int topK = optimizeFor.topKFor(limit);
            float[] queryVector = index.termType().decomposeVector(orderer.lower().value.raw.duplicate());

            if (shouldUseBruteForce(topK, limit, segmentOrdinalPairs.size()))
            {
                return toScoreSortedIterator(orderByBruteForce(queryVector, segmentOrdinalPairs, limit, topK));
            }

            SparseFixedBitSet bits = bitSetForSearch();
            segmentOrdinalPairs.forEachOrdinal(bits::set);
            // else ask the index to perform a search limited to the bits we created
            int expectedNodesVisited = expectedNodesVisited(limit, segmentOrdinalPairs.size(), graph.size());
            IntConsumer nodesVisitedConsumer = nodesVisited -> updateExpectedNodes(nodesVisited, expectedNodesVisited);
            CloseableIterator<RowIdWithScore> result = graph.search(queryVector, topK, limit, bits, nodesVisitedConsumer);
            return toScoreSortedIterator(result);
        }
    }

    private boolean shouldUseBruteForce(int topK, int limit, int numRows)
    {
        // if we have a small number of results then let TopK processor do exact NN computation
        int maxBruteForceRows = maxBruteForceRows(topK, numRows, graph.size());
        logger.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                     numRows, maxBruteForceRows, graph.size(), limit);
        Tracing.trace("SAI materialized {} rows; max brute force rows is {} for sstable index with {} nodes, LIMIT {}",
                      numRows, maxBruteForceRows, graph.size(), limit);
        return FORCE_BRUTE_FORCE_ANN == null ? numRows <= maxBruteForceRows
                                             : FORCE_BRUTE_FORCE_ANN;
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodesVisited = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        int expectedComparisons = index.indexWriterConfig().getMaximumNodeConnections() * expectedNodesVisited;
        // Brute force here means reading each vector from the index file on disk, comparing the row's vector to the
        // search vector to get a score, and then putting the results into a priority queue to then iterate over.
        // Alternatively, we search the graph, which entails comparisons and disk reads. The goal is to reduce disk
        // accesses and number of comparisons.
        // VSTODO the below factor is dramatically oversimplified
        // larger dimension should increase this, because comparisons are more expensive
        double memoryToDiskFactor = 0.25;
        return (int) max(limit, memoryToDiskFactor * expectedComparisons);
    }

    private int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        double observedRatio = actualExpectedRatio.getUpdateCount() >= 10 ? actualExpectedRatio.get() : 1.0;
        return (int) (observedRatio * VectorMemoryIndex.expectedNodesVisited(limit, nPermittedOrdinals, graphSize));
    }

    private void updateExpectedNodes(int actualNodesVisited, int expectedNodesVisited)
    {
        assert expectedNodesVisited >= 0 : expectedNodesVisited;
        assert actualNodesVisited >= 0 : actualNodesVisited;
        if (actualNodesVisited >= 1000 && actualNodesVisited > 2 * expectedNodesVisited || expectedNodesVisited > 2 * actualNodesVisited)
            logger.trace("Predicted visiting {} nodes, but actually visited {}", expectedNodesVisited, actualNodesVisited);
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

    private CloseableIterator<PrimaryKeyWithScore> toScoreSortedIterator(CloseableIterator<RowIdWithScore> rowIdIterator) throws IOException
    {
        if (!rowIdIterator.hasNext())
        {
            FileUtils.closeQuietly(rowIdIterator);
            return CloseableIterator.empty();
        }

        return new RowIdToPrimaryKeyWithScoreIterator(column, primaryKeyMapFactory, rowIdIterator, metadata.rowIdOffset);
    }
}
