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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.NodeQueue;
import io.github.jbellis.jvector.quantization.CompressedVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.BitSet;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.BoundedLongHeap;
import io.github.jbellis.jvector.util.SparseBits;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.BruteForceRowIdIterator;
import org.apache.cassandra.index.sai.disk.vector.CassandraDiskAnn;
import org.apache.cassandra.index.sai.disk.vector.CloseableReranker;
import org.apache.cassandra.index.sai.disk.vector.NodeQueueRowIdIterator;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.plan.Plan.CostCoefficients;
import org.apache.cassandra.index.sai.utils.SegmentRowIdOrdinalPairs;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.RowIdWithMeta;
import org.apache.cassandra.index.sai.utils.RowIdWithScore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.LinearFit;
import org.apache.cassandra.metrics.PairedSlidingWindowReservoir;
import org.apache.cassandra.metrics.QuickSlidingWindowReservoir;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static org.apache.cassandra.index.sai.plan.Plan.hrs;

/**
 * Executes ann search against the graph for an individual index segment.
 */
public class V2VectorIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    /**
     * Only allow brute force if fewer than this many rows are involved.
     * Not final so test can inject its own setting.
     */
    @VisibleForTesting
    public static int GLOBAL_BRUTE_FORCE_ROWS = Integer.MAX_VALUE;
    /**
     * How much more expensive is brute forcing the comparisons than going through the index?
     * (brute force needs to go through the full read path to pull out the vectors from the row)
     */
    @VisibleForTesting
    public static double BRUTE_FORCE_EXPENSE_FACTOR = DatabaseDescriptor.getAnnBruteForceExpenseFactor();

    @VisibleForTesting
    public final CassandraDiskAnn graph;
    private final PrimaryKey.Factory keyFactory;
    private final PairedSlidingWindowReservoir expectedActualNodesVisited = new PairedSlidingWindowReservoir(20);
    private final ThreadLocal<SparseBits> cachedBits;
    private final ColumnQueryMetrics.VectorIndexMetrics columnQueryMetrics;

    protected V2VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                    PerIndexFiles perIndexFiles,
                                    SegmentMetadata segmentMetadata,
                                    IndexContext indexContext,
                                    CassandraDiskAnn graph)
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexContext);
        this.graph = graph;
        this.keyFactory = PrimaryKey.factory(indexContext.comparator(), indexContext.indexFeatureSet());
        this.cachedBits = ThreadLocal.withInitial(SparseBits::new);
        this.columnQueryMetrics = (ColumnQueryMetrics.VectorIndexMetrics) indexContext.getColumnQueryMetrics();
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    public VectorCompression getCompression()
    {
        return graph.getCompression();
    }

    public ProductQuantization getPQ()
    {
        return graph.getPQ();
    }

    @Override
    public KeyRangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer) throws IOException
    {
        PostingList results = searchPosting(context, exp, keyRange);
        return toPrimaryKeyIterator(results, context);
    }

    private PostingList searchPosting(QueryContext context, Expression exp, AbstractBounds<PartitionPosition> keyRange) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.BOUNDED_ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during BOUNDED_ANN index query: " + exp));

        var queryVector = vts.createFloatVector(exp.lower.value.vector);

        // this is a thresholded query, so pass graph.size() as top k to get all results satisfying the threshold
        // Threshold queries do not use pruning.
        var result = searchInternal(keyRange, context, queryVector, graph.size(), graph.size(), exp.getEuclideanSearchThreshold(), false);
        return new ReorderingPostingList(result, RowIdWithMeta::getSegmentRowId);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext context, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), orderer);

        if (!orderer.isANN())
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + orderer));

        int rerankK = orderer.rerankKFor(limit, graph.getCompression());
        var queryVector = orderer.getVectorTerm();

        var result = searchInternal(keyRange, context, queryVector, limit, rerankK, 0, orderer.usePruning());
        return toMetaSortedIterator(result, context);
    }

    /**
     * Find the closest `limit` neighbors to the given query vector, using a coarse search pass for `rerankK`
     * candidates.  May decide to use brute force instead of the index.
     * @param keyRange the key range to search
     * @param context the query context
     * @param queryVector the query vector
     * @param limit the limit for the query
     * @param rerankK the amplified limit for the query to get more accurate results
     * @param threshold the threshold for the query. When the threshold is greater than 0 and brute force logic is used,
     *                  the results will be filtered by the threshold.
     * @param usePruning whether to use pruning to speed up the ANN search
     */
    private CloseableIterator<RowIdWithScore> searchInternal(AbstractBounds<PartitionPosition> keyRange,
                                                             QueryContext context,
                                                             VectorFloat<?> queryVector,
                                                             int limit,
                                                             int rerankK,
                                                             float threshold,
                                                             boolean usePruning) throws IOException
    {
        try (PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            // not restricted
            if (RangeUtil.coversFullRing(keyRange))
            {
                var estimate = estimateCost(rerankK, graph.size());
                return graph.search(queryVector, limit, rerankK, threshold, usePruning, Bits.ALL, context, estimate::updateStatistics);
            }

            PrimaryKey firstPrimaryKey = keyFactory.createTokenOnly(keyRange.left.getToken());

            // it will return the next row id if given key is not found.
            long minSSTableRowId = primaryKeyMap.ceiling(firstPrimaryKey);
            // If we didn't find the first key, we won't find the last primary key either
            if (minSSTableRowId < 0)
                return CloseableIterator.emptyIterator();
            long maxSSTableRowId = getMaxSSTableRowId(primaryKeyMap, keyRange.right);

            if (minSSTableRowId > maxSSTableRowId)
                return CloseableIterator.emptyIterator();

            // if the range covers the entire segment, skip directly to an index search
            if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
                return graph.search(queryVector, limit, rerankK, threshold, usePruning, Bits.ALL, context, visited -> {});

            minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
            maxSSTableRowId = min(maxSSTableRowId, metadata.maxSSTableRowId);

            // Upper-bound cost based on maximum possible rows included
            int nRows = Math.toIntExact(maxSSTableRowId - minSSTableRowId + 1);
            var initialCostEstimate = estimateCost(rerankK, nRows);
            Tracing.logAndTrace(logger, "Search range covers {} rows in index of {} nodes; estimate for LIMIT {} is {}",
                                nRows, graph.size(), rerankK, initialCostEstimate);
            // if the range spans a small number of rows, then generate scores from the sstable rows instead of searching the index
            int startSegmentRowId = metadata.toSegmentRowId(minSSTableRowId);
            int endSegmentRowId = metadata.toSegmentRowId(maxSSTableRowId);
            if (initialCostEstimate.shouldUseBruteForce())
            {
                var maxSize = endSegmentRowId - startSegmentRowId + 1;
                var segmentOrdinalPairs = new SegmentRowIdOrdinalPairs(maxSize);
                try (var ordinalsView = graph.getOrdinalsView())
                {
                    ordinalsView.forEachOrdinalInRange(startSegmentRowId, endSegmentRowId, segmentOrdinalPairs::add);
                }

                // When we have a threshold, we only need to filter the results, not order them, because it means we're
                // evaluating a boolean predicate in the SAI pipeline that wants to collate by PK
                if (threshold > 0)
                    return filterByBruteForce(queryVector, segmentOrdinalPairs, threshold);
                else
                    return orderByBruteForce(queryVector, segmentOrdinalPairs, limit, rerankK);
            }

            // create a bitset of ordinals corresponding to the rows in the given key range
            final Bits bits;
            try (var ordinalsView = graph.getOrdinalsView())
            {
                bits = ordinalsView.buildOrdinalBits(startSegmentRowId, endSegmentRowId, this::bitSetForSearch);
            }
            // the set of ordinals may be empty if no rows in the range had a vector associated with them
            int cardinality = bits instanceof SparseBits ? ((SparseBits) bits).cardinality() : ((BitSet) bits).cardinality();
            if (cardinality == 0)
                return CloseableIterator.emptyIterator();
            // Rows are many-to-one wrt index ordinals, so the actual number of ordinals involved (`cardinality`)
            // could be less than the number of rows in the range (`nRows`).  In that case we should update the cost
            // so that we don't pollute the planner with incorrectly pessimistic estimates.
            //
            // Technically, we could also have another `shouldUseBruteForce` branch here, but we don't have
            // the code to generate rowids from ordinals, and it's a rare enough case that it doesn't seem worth
            // the trouble to add it.
            var betterCostEstimate = estimateCost(rerankK, cardinality);

            return graph.search(queryVector, limit, rerankK, threshold, usePruning, bits, context, betterCostEstimate::updateStatistics);
        }
    }

    private CloseableIterator<RowIdWithScore> orderByBruteForce(VectorFloat<?> queryVector, SegmentRowIdOrdinalPairs segmentOrdinalPairs, int limit, int rerankK) throws IOException
    {
        if (segmentOrdinalPairs.size() == 0)
            return CloseableIterator.emptyIterator();

        // We allow for negative rerankK, but for our cost calculations, it only makes sense to use 0 here.
        rerankK = Math.max(0, rerankK);
        // If we use compressed vectors, we still have to order rerankK results using full resolution similarity
        // scores, so only use the compressed vectors when there are enough vectors to make it worthwhile.
        double twoPassCost = segmentOrdinalPairs.size() * CostCoefficients.ANN_SIMILARITY_COST
                             + rerankK * hrs(CostCoefficients.ANN_SCORED_KEY_COST);
        double onePassCost = segmentOrdinalPairs.size() * hrs(CostCoefficients.ANN_SCORED_KEY_COST);
        if (graph.getCompressedVectors() != null && twoPassCost < onePassCost)
            return orderByBruteForce(graph.getCompressedVectors(), queryVector, segmentOrdinalPairs, limit, rerankK);
        return orderByBruteForce(queryVector, segmentOrdinalPairs);
    }

    /**
     * Materialize the compressed vectors for the given segment row ids, put them into a priority queue ordered by
     * approximate similarity score, and then pass to the {@link BruteForceRowIdIterator} to lazily resolve the
     * full resolution ordering as needed.
     */
    private CloseableIterator<RowIdWithScore> orderByBruteForce(CompressedVectors cv,
                                                                VectorFloat<?> queryVector,
                                                                SegmentRowIdOrdinalPairs segmentOrdinalPairs,
                                                                int limit,
                                                                int rerankK)
    {
        // Use the jvector NodeQueue to avoid unnecessary object allocations since this part of the code operates on
        // many rows.
        var approximateScores = new NodeQueue(new BoundedLongHeap(segmentOrdinalPairs.size()), NodeQueue.Order.MAX_HEAP);
        var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
        var scoreFunction = cv.precomputedScoreFunctionFor(queryVector, similarityFunction);
        columnQueryMetrics.onBruteForceNodesVisited(segmentOrdinalPairs.size());

        if (rerankK <= 0)
        {
            // Rerankless search, so we go straight to the NodeQueueRowIdIterator.
            var iter = segmentOrdinalPairs.mapToSegmentRowIdScoreIterator(scoreFunction);
            approximateScores.pushMany(iter, segmentOrdinalPairs.size());
            return new NodeQueueRowIdIterator(approximateScores, true);
        }

        // Store the index of the (rowId, ordinal) pair from the segmentOrdinalPairs in the NodeQueue so that we can
        // retrieve both values with O(1) lookup when we need to resolve the full resolution score in the
        // BruteForceRowIdIterator.
        var iter = segmentOrdinalPairs.mapToIndexScoreIterator(scoreFunction);
        approximateScores.pushMany(iter, segmentOrdinalPairs.size());
        var reranker = new CloseableReranker(similarityFunction, queryVector, graph.getView());
        return new BruteForceRowIdIterator(approximateScores, segmentOrdinalPairs, reranker, limit, rerankK, graph.usesNVQ(), columnQueryMetrics);
    }

    /**
     * Produces a correct ranking of the rows in the given segment. Because this graph does not have compressed
     * vectors, read all vectors and put them into a priority queue to rank them lazily. It is assumed that the whole
     * PQ will often not be needed.
     */
    private CloseableIterator<RowIdWithScore> orderByBruteForce(VectorFloat<?> queryVector, SegmentRowIdOrdinalPairs segmentOrdinalPairs) throws IOException
    {
        var scoredRowIds = new NodeQueue(new BoundedLongHeap(segmentOrdinalPairs.size()), NodeQueue.Order.MAX_HEAP);
        try (var vectorsView = graph.getView())
        {
            var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
            var esf = vectorsView.rerankerFor(queryVector, similarityFunction);
            // Because the scores are exact, we only store the rowid, score pair.
            var iter = segmentOrdinalPairs.mapToSegmentRowIdScoreIterator(esf);
            scoredRowIds.pushMany(iter, segmentOrdinalPairs.size());
            columnQueryMetrics.onBruteForceNodesReranked(segmentOrdinalPairs.size());
            return new NodeQueueRowIdIterator(scoredRowIds, graph.usesNVQ());
        }
    }

    /**
     * Materialize the full resolution vector for each row id, compute the similarity score, filter
     * out rows that do not meet the threshold, and then return them in an iterator.
     * NOTE: because the threshold is not used for ordering, the result is returned in PK order, not score order.
     */
    private CloseableIterator<RowIdWithScore> filterByBruteForce(VectorFloat<?> queryVector,
                                                                 SegmentRowIdOrdinalPairs segmentOrdinalPairs,
                                                                 float threshold) throws IOException
    {
        var results = new ArrayList<RowIdWithScore>(segmentOrdinalPairs.size());
        try (var vectorsView = graph.getView())
        {
            var similarityFunction = indexContext.getIndexWriterConfig().getSimilarityFunction();
            var esf = vectorsView.rerankerFor(queryVector, similarityFunction);
            final boolean isRerankerApproximate = graph.usesNVQ();
            segmentOrdinalPairs.forEachSegmentRowIdOrdinalPair((segmentRowId, ordinal) -> {
                var score = esf.similarityTo(ordinal);
                if (score >= threshold)
                    results.add(new RowIdWithScore(segmentRowId, score, isRerankerApproximate));
            });
            columnQueryMetrics.onBruteForceNodesReranked(segmentOrdinalPairs.size());
        }
        return CloseableIterator.wrap(results.iterator());
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

    public V5VectorPostingsWriter.Structure getPostingsStructure()
    {
        return graph.getPostingsStructure();
    }

    private class CostEstimate
    {
        private final int candidates;
        private final int rawExpectedNodesVisited;
        private final int expectedNodesVisited;

        public CostEstimate(int candidates, int rawExpectedNodesVisited, int expectedNodesVisited)
        {
            assert rawExpectedNodesVisited >= 0 : rawExpectedNodesVisited;
            assert expectedNodesVisited >= 0 : expectedNodesVisited;

            this.candidates = candidates;
            this.rawExpectedNodesVisited = rawExpectedNodesVisited;
            this.expectedNodesVisited = expectedNodesVisited;
        }

        public boolean shouldUseBruteForce()
        {
            if (candidates > GLOBAL_BRUTE_FORCE_ROWS)
                return false;
            return bruteForceCost() <= indexScanCost();
        }

        private double indexScanCost()
        {
            return expectedNodesVisited
                   * (CostCoefficients.ANN_SIMILARITY_COST + hrs(CostCoefficients.ANN_EDGELIST_COST) / graph.maxDegree());
        }

        private double bruteForceCost()
        {
            // VSTODO we don't have rerankK available here, so we only calculate the two pass cost
            // out of the options in orderByBruteForce.  (The rerank cost is roughly equal for both
            // indexScanCost and bruteForceCost so we can leave it out of both.)
            return candidates * CostCoefficients.ANN_SIMILARITY_COST;
        }

        public void updateStatistics(int actualNodesVisited)
        {
            assert actualNodesVisited >= 0 : actualNodesVisited;
            expectedActualNodesVisited.update(rawExpectedNodesVisited, actualNodesVisited);

            if (actualNodesVisited >= 1000 && (actualNodesVisited > 2 * expectedNodesVisited || actualNodesVisited < 0.5 * expectedNodesVisited))
                Tracing.logAndTrace(logger, "Predicted visiting {} nodes ({} raw), but actually visited {}",
                                    expectedNodesVisited, rawExpectedNodesVisited, actualNodesVisited);
        }

        @Override
        public String toString()
        {
            return String.format("{brute force(%d) = %.2f, index scan(%d) = %.2f}",
                                 candidates, bruteForceCost(), expectedNodesVisited, indexScanCost());
        }

        public double cost()
        {
            return min(bruteForceCost(), indexScanCost());
        }
    }

    public double estimateAnnSearchCost(int rerankK, int candidates)
    {
        var estimate = estimateCost(rerankK, candidates);
        return estimate.cost();
    }

    private CostEstimate estimateCost(int rerankK, int candidates)
    {
        int rawExpectedNodes = getRawExpectedNodes(rerankK, candidates);
        // update the raw expected value with a linear interpolation based on observed data
        var observedValues = expectedActualNodesVisited.getSnapshot().values;
        int expectedNodes;
        if (observedValues.length >= 10)
        {
            var interceptSlope = LinearFit.interceptSlopeFor(observedValues);
            expectedNodes = (int) (interceptSlope.left + interceptSlope.right * rawExpectedNodes);
        }
        else
        {
            expectedNodes = rawExpectedNodes;
        }

        int sanitizedEstimate = VectorMemtableIndex.ensureSaneEstimate(expectedNodes, rerankK, graph.size());
        return new CostEstimate(candidates, rawExpectedNodes, sanitizedEstimate);
    }

    private SparseBits bitSetForSearch()
    {
        var bits = cachedBits.get();
        bits.clear();
        return bits;
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(SSTableReader reader,
                                                                             QueryContext context,
                                                                             List<PrimaryKey> keys,
                                                                             Orderer orderer,
                                                                             int limit) throws IOException
    {
        if (keys.isEmpty())
            return CloseableIterator.emptyIterator();

        int rerankK = orderer.rerankKFor(limit, graph.getCompression());
        // Convert PKs to segment row ids and map to ordinals, skipping any that don't exist in this segment
        var segmentOrdinalPairs = flatmapPrimaryKeysToBitsAndRows(keys);
        var numRows = segmentOrdinalPairs.size();
        final CostEstimate cost = estimateCost(rerankK, numRows);
        Tracing.logAndTrace(logger, "{} relevant rows out of {} in range in index of {} nodes; estimate for LIMIT {} is {}",
                            numRows, keys.size(), graph.size(), limit, cost);
        if (numRows == 0)
            return CloseableIterator.emptyIterator();

        if (cost.shouldUseBruteForce())
        {
            // brute force using the in-memory compressed vectors to cut down the number of results returned
            var queryVector = orderer.getVectorTerm();
            return toMetaSortedIterator(this.orderByBruteForce(queryVector, segmentOrdinalPairs, limit, rerankK), context);
        }
        // Create bits from the mapping
        var bits = bitSetForSearch();
        segmentOrdinalPairs.forEachOrdinal(bits::set);
        // else ask the index to perform a search limited to the bits we created
        var queryVector = orderer.getVectorTerm();
        var results = graph.search(queryVector, limit, rerankK, 0, orderer.usePruning(), bits, context, cost::updateStatistics);
        return toMetaSortedIterator(results, context);
    }


    /**
     * Build a mapping of segment row id to ordinal for the given primary keys, skipping any that don't exist in this
     * segment.
     * @param keysInRange the primary keys to map
     * @return a mapping of segment row id to ordinal
     * @throws IOException
     */
    private SegmentRowIdOrdinalPairs flatmapPrimaryKeysToBitsAndRows(List<PrimaryKey> keysInRange) throws IOException
    {
        var segmentOrdinalPairs = new SegmentRowIdOrdinalPairs(keysInRange.size());
        int lastSegmentRowId = -1;
        try (var primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
             var ordinalsView = graph.getOrdinalsView())
        {
            // track whether we are saving comparisons by using binary search to skip ahead
            // (if most of the keys belong to this sstable, bsearch will actually be slower)
            var comparisonsSavedByBsearch = new QuickSlidingWindowReservoir(10);
            boolean preferSeqScanToBsearch = false;

            for (int i = 0; i < keysInRange.size();)
            {
                // turn the pk back into a row id, with a fast path for the case where the pk is from this sstable
                var primaryKey = keysInRange.get(i);
                long sstableRowId = primaryKeyMap.exactRowIdOrInvertedCeiling(primaryKey);

                if (sstableRowId < 0)
                {
                    // The given PK doesn't exist in this sstable, so sstableRowId is the inversion
                    // of the next-highest row ID.  Invert it back to get the ceiling row ID.
                    long ceilingRowId = ~sstableRowId;
                    if (ceilingRowId > metadata.maxSSTableRowId)
                    {
                        // The next greatest primary key is greater than all the primary keys in this segment
                        break;
                    }
                    var ceilingPrimaryKey = primaryKeyMap.primaryKeyFromRowId(ceilingRowId);

                    boolean ceilingPrimaryKeyMatchesKeyInRange = false;
                    // adaptively choose either seq scan or bsearch to skip ahead in keysInRange until
                    // we find one at least as large as the ceiling key
                    if (preferSeqScanToBsearch)
                    {
                        int keysToSkip = 1; // We already know that the PK at index i is not equal to the ceiling PK.
                        int cmp = 1; // Need to initialize. The value is irrelevant.
                        for ( ; i + keysToSkip < keysInRange.size(); keysToSkip++)
                        {
                            var nextPrimaryKey = keysInRange.get(i + keysToSkip);
                            cmp = nextPrimaryKey.compareTo(ceilingPrimaryKey);
                            if (cmp >= 0)
                                break;
                        }
                        comparisonsSavedByBsearch.update(keysToSkip - (int) ceil(logBase2(keysInRange.size() - i)));
                        i += keysToSkip;
                        ceilingPrimaryKeyMatchesKeyInRange = cmp == 0;
                    }
                    else
                    {
                        // Use a sublist to only search the remaining primary keys in range.
                        var keysRemaining = keysInRange.subList(i, keysInRange.size());
                        int nextIndexForCeiling = Collections.binarySearch(keysRemaining, ceilingPrimaryKey);
                        if (nextIndexForCeiling < 0)
                            // We got the inversion of the insertion point. Invert it to get the insertion point.
                            nextIndexForCeiling = ~nextIndexForCeiling;
                        else
                            ceilingPrimaryKeyMatchesKeyInRange = true;

                        comparisonsSavedByBsearch.update(nextIndexForCeiling - (int) ceil(logBase2(keysRemaining.size())));
                        i += nextIndexForCeiling;
                    }

                    // update our estimate
                    preferSeqScanToBsearch = comparisonsSavedByBsearch.size() >= 10
                                             && comparisonsSavedByBsearch.getMean() < 0;
                    if (ceilingPrimaryKeyMatchesKeyInRange)
                        sstableRowId = ceilingRowId;
                    else
                        continue; // without incrementing i further. ceilingPrimaryKey is less than the PK at index i.
                }
                // Increment here to simplify the sstableRowId < 0 logic.
                i++;

                // During compaction, the SegmentMetadata is written based on the rows with vector values. Therefore,
                // we can find a row that has a row id but is outside the min/max range of the segment. We can ignore
                // these rows here and skip the row id to ordinal conversion that would result in a -1 ordinal.
                if (sstableRowId < metadata.minSSTableRowId || sstableRowId > metadata.maxSSTableRowId)
                    continue;

                // convert the global row id to segment row id and from segment row id to graph ordinal
                int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                // This requirement is required by the ordinals view. There are cases where we have broken this
                // requirement, and in order to make future debugging easier, we check here and throw an exception
                // with additional detail.
                if (segmentRowId <= lastSegmentRowId)
                    throw new IllegalStateException("Row ids must ascend monotonically. Got " + segmentRowId + " after " + lastSegmentRowId
                                                    + " for " + primaryKey + " on sstable " + primaryKeyMap.getSSTableId());
                lastSegmentRowId = segmentRowId;
                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal >= 0)
                    segmentOrdinalPairs.add(segmentRowId, ordinal);
            }
        }
        return segmentOrdinalPairs;
    }

    public static double logBase2(double number) {
        return Math.log(number) / Math.log(2);
    }

    private int getRawExpectedNodes(int rerankK, int nPermittedOrdinals)
    {
        return VectorMemtableIndex.expectedNodesVisited(rerankK, nPermittedOrdinals, graph.size());
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

    public boolean containsUnitVectors()
    {
        return graph.containsUnitVectors();
    }
}
