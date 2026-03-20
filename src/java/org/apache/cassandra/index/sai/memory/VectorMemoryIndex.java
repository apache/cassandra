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

package org.apache.cassandra.index.sai.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.PriorityQueueIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class VectorMemoryIndex extends MemoryIndex
{
    private final OnHeapGraph<PrimaryKey> graph;
    private final Memtable memtable;
    private final LongAdder writeCount = new LongAdder();

    private PrimaryKey minimumKey;
    private PrimaryKey maximumKey;

    private final NavigableSet<PrimaryKey> primaryKeys = new ConcurrentSkipListSet<>();

    public VectorMemoryIndex(StorageAttachedIndex index, Memtable memtable)
    {
        super(index);
        this.graph = new OnHeapGraph<>(index.termType().indexType(), index.indexWriterConfig(), memtable);
        this.memtable = memtable;
    }

    @Override
    public synchronized long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0 || !index.validateTermSize(key, value, false, null))
            return 0;

        PrimaryKey primaryKey = index.hasClustering() ? index.keyFactory().create(key, clustering)
                                                      : index.keyFactory().create(key);
        return index(primaryKey, value);
    }

    private long index(PrimaryKey primaryKey, ByteBuffer value)
    {
        updateKeyBounds(primaryKey);

        writeCount.increment();
        primaryKeys.add(primaryKey);
        return graph.add(value, primaryKey, OnHeapGraph.InvalidVectorBehavior.FAIL);
    }

    @Override
    public long update(DecoratedKey key, Clustering<?> clustering, ByteBuffer oldValue, ByteBuffer newValue)
    {
        int oldRemaining = oldValue == null ? 0 : oldValue.remaining();
        int newRemaining = newValue == null ? 0 : newValue.remaining();
        if (oldRemaining == 0 && newRemaining == 0)
            return 0;

        boolean different;
        if (oldRemaining != newRemaining)
        {
            assert oldRemaining == 0 || newRemaining == 0; // one of them is null
            different = true;
        }
        else
        {
            different = index.termType().compare(oldValue, newValue) != 0;
        }

        long bytesUsed = 0;
        if (different)
        {
            PrimaryKey primaryKey = index.hasClustering() ? index.keyFactory().create(key, clustering)
                                                          : index.keyFactory().create(key);
            // update bounds because only rows with vectors are included in the key bounds,
            // so if the vector was null before, we won't have included it
            updateKeyBounds(primaryKey);

            // make the changes in this order, so we don't have a window where the row is not in the index at all
            if (newRemaining > 0)
                bytesUsed += graph.add(newValue, primaryKey, OnHeapGraph.InvalidVectorBehavior.FAIL);
            if (oldRemaining > 0)
                bytesUsed -= graph.remove(oldValue, primaryKey);

            // remove primary key if it's no longer indexed
            if (newRemaining <= 0 && oldRemaining > 0)
                primaryKeys.remove(primaryKey);
        }
        return bytesUsed;
    }

    private void updateKeyBounds(PrimaryKey primaryKey)
    {
        if (minimumKey == null)
            minimumKey = primaryKey;
        else if (primaryKey.compareTo(minimumKey) < 0)
            minimumKey = primaryKey;
        if (maximumKey == null)
            maximumKey = primaryKey;
        else if (primaryKey.compareTo(maximumKey) > 0)
            maximumKey = primaryKey;
    }

    @Override
    public KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderBy(QueryContext queryContext, Expression expr, AbstractBounds<PartitionPosition> keyRange)
    {
        assert expr.getIndexOperator() == Expression.IndexOperator.ANN : "Only ANN is supported for vector search, received " + expr.getIndexOperator();

        ByteBuffer buffer = expr.lower().value.raw;
        float[] qv = index.termType().decomposeVector(buffer);

        Bits bits;
        if (!RangeUtil.coversFullRing(keyRange))
        {
            // if left bound is MIN_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean leftInclusive = keyRange.left.kind() != PartitionPosition.Kind.MAX_BOUND;
            // if right bound is MAX_BOUND or KEY_BOUND, we need to include all token-only PrimaryKeys with same token
            boolean rightInclusive = keyRange.right.kind() != PartitionPosition.Kind.MIN_BOUND;
            // if right token is MAX (Long.MIN_VALUE), there is no upper bound
            boolean isMaxToken = keyRange.right.getToken().isMinimum(); // max token

            PrimaryKey left = index.keyFactory().create(keyRange.left.getToken()); // lower bound
            PrimaryKey right = isMaxToken ? null : index.keyFactory().create(keyRange.right.getToken()); // upper bound

            Set<PrimaryKey> resultKeys = isMaxToken ? primaryKeys.tailSet(left, leftInclusive) : primaryKeys.subSet(left, leftInclusive, right, rightInclusive);

            if (resultKeys.isEmpty())
                return CloseableIterator.empty();

            int bruteForceRows = maxBruteForceRows(queryContext.limit(), resultKeys.size(), graph.size());
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                          resultKeys.size(), bruteForceRows, graph.size(), queryContext.limit());
            if (resultKeys.size() < Math.max(queryContext.limit(), bruteForceRows))
                return orderByBruteForce(qv, resultKeys);
            else
                bits = new KeyRangeFilteringBits(keyRange, null);
        }
        else
        {
            // Accept all bits
            bits = new Bits.MatchAllBits(Integer.MAX_VALUE);
        }

        CloseableIterator<SearchResult.NodeScore> iterator = graph.search(qv, queryContext.limit(), bits);
        return new NodeScoreToScoredPrimaryKeyIterator(iterator);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithScore> orderResultsBy(QueryContext queryContext, List<PrimaryKey> results, Expression orderer)
    {
        if (minimumKey == null)
            // This case implies maximumKey is empty too.
            return CloseableIterator.empty();

        int limit = queryContext.limit();

        List<PrimaryKey> resultsInRange = results.stream()
                                                 .dropWhile(k -> k.compareTo(minimumKey) < 0)
                                                 .takeWhile(k -> k.compareTo(maximumKey) <= 0)
                                                 .collect(Collectors.toList());

        int maxBruteForceRows = maxBruteForceRows(limit, resultsInRange.size(), graph.size());
        Tracing.trace("SAI materialized {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                      resultsInRange.size(), maxBruteForceRows, graph.size(), limit);

        if (resultsInRange.isEmpty())
            return CloseableIterator.empty();

        ByteBuffer buffer = orderer.lower().value.raw;
        float[] qv = index.termType().decomposeVector(buffer);

        if (resultsInRange.size() <= maxBruteForceRows)
            return orderByBruteForce(qv, resultsInRange);

        // Search the graph for the topK vectors near the query
        KeyFilteringBits bits = new KeyFilteringBits(resultsInRange);
        CloseableIterator<SearchResult.NodeScore> nodeScores = graph.search(qv, limit, bits);
        return new NodeScoreToScoredPrimaryKeyIterator(nodeScores);
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodesVisited = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        // ANN index will do a bunch of extra work besides the full comparisons
        // VSTODO I'm not sure which one is more expensive, but since the graph is in memory and the vectors are
        // full precision, the goal here is simple: minimize the number of vector comparisons (aka nodes visited).
        // As such, the cost function weights them at a 1:1 ratio for now.
        return max(limit, expectedNodesVisited);
    }

    private CloseableIterator<PrimaryKeyWithScore> orderByBruteForce(float[] queryVector, Collection<PrimaryKey> keys)
    {
        VectorSimilarityFunction similarityFunction = index.indexWriterConfig().getSimilarityFunction();
        List<PrimaryKeyWithScore> scoredKeys = new ArrayList<>(keys.size());
        for (PrimaryKey key : keys)
        {
            PrimaryKeyWithScore scoredKey = scoreKey(similarityFunction, queryVector, key);
            if (scoredKey != null)
                scoredKeys.add(scoredKey);
        }
        // Because we merge iterators from all sstables and memtables, we do not need a complete sort of these
        // elements, so a priority queue provides good performance.
        return new PriorityQueueIterator<>(new PriorityQueue<>(scoredKeys));
    }

    private PrimaryKeyWithScore scoreKey(VectorSimilarityFunction similarityFunction, float[] queryVector, PrimaryKey key)
    {
        float[] vector = graph.vectorForKey(key);
        if (vector == null)
            return null;
        float score = similarityFunction.compare(queryVector, vector);
        return new PrimaryKeyWithScore(index.termType().columnMetadata(), memtable, key, score);
    }

    /**
     * All parameters must be greater than zero.  nPermittedOrdinals may be larger than graphSize.
     */
    public static int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        // constants are computed by Code Interpreter based on observed comparison counts in tests
        // https://chat.openai.com/share/2b1d7195-b4cf-4a45-8dce-1b9b2f893c75
        int sizeRestriction = min(nPermittedOrdinals, graphSize);
        int raw = (int) (0.7 * pow(log(graphSize), 2) *
                         pow(graphSize, 0.33) *
                         pow(log(limit), 2) *
                         pow(log((double) graphSize / sizeRestriction), 2) / pow(sizeRestriction, 0.13));
        // we will always visit at least min(limit, graphSize) nodes, and we can't visit more nodes than exist in the graph
        return min(max(raw, min(limit, graphSize)), graphSize);
    }

    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        // This method is only used when merging an in-memory index with a RowMapping. This is done a different
        // way with the graph using the writeData method below.
        throw new UnsupportedOperationException();
    }

    public SegmentMetadata.ComponentMetadataMap writeDirect(IndexDescriptor indexDescriptor,
                                                            IndexIdentifier indexIdentifier,
                                                            Function<PrimaryKey, Integer> postingTransformer) throws IOException
    {
        return graph.writeData(indexDescriptor, indexIdentifier, postingTransformer);
    }

    @Override
    public boolean isEmpty()
    {
        return graph.isEmpty();
    }

    @Nullable
    @Override
    public ByteBuffer getMinTerm()
    {
        return null;
    }

    @Nullable
    @Override
    public ByteBuffer getMaxTerm()
    {
        return null;
    }

    private class KeyRangeFilteringBits implements Bits
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        @Nullable
        private final Bits bits;

        public KeyRangeFilteringBits(AbstractBounds<PartitionPosition> keyRange, @Nullable Bits bits)
        {
            this.keyRange = keyRange;
            this.bits = bits;
        }

        @Override
        public boolean get(int ordinal)
        {
            if (bits != null && !bits.get(ordinal))
                return false;

            Collection<PrimaryKey> keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> keyRange.contains(k.partitionKey()));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }

    private class KeyFilteringBits implements Bits
    {
        private final List<PrimaryKey> results;

        public KeyFilteringBits(List<PrimaryKey> results)
        {
            this.results = results;
        }

        @Override
        public boolean get(int i)
        {
            Collection<PrimaryKey> pk = graph.keysFromOrdinal(i);
            return results.stream().anyMatch(pk::contains);
        }

        @Override
        public int length()
        {
            return results.size();
        }
    }

    /**
     * An iterator over {@link PrimaryKeyWithScore} sorted by score descending. The iterator converts ordinals (node ids)
     * to {@link PrimaryKey}s and pairs them with the score given by the index.
     */
    private class NodeScoreToScoredPrimaryKeyIterator extends AbstractIterator<PrimaryKeyWithScore>
    {
        private final CloseableIterator<SearchResult.NodeScore> nodeScores;
        private Iterator<PrimaryKeyWithScore> primaryKeysForNode = Collections.emptyIterator();

        NodeScoreToScoredPrimaryKeyIterator(CloseableIterator<SearchResult.NodeScore> nodeScores)
        {
            this.nodeScores = nodeScores;
        }

        @Override
        protected PrimaryKeyWithScore computeNext()
        {
            if (primaryKeysForNode.hasNext())
                return primaryKeysForNode.next();

            while (nodeScores.hasNext())
            {
                SearchResult.NodeScore nodeScore = nodeScores.next();
                primaryKeysForNode = graph.keysFromOrdinal(nodeScore.node)
                                          .stream()
                                          .map(pk -> new PrimaryKeyWithScore(index.termType().columnMetadata(), memtable, pk, nodeScore.score))
                                          .iterator();
                if (primaryKeysForNode.hasNext())
                    return primaryKeysForNode.next();
            }

            return endOfData();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(nodeScores);
        }
    }
}
