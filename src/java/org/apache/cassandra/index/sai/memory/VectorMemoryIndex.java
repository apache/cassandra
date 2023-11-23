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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import io.github.jbellis.jvector.util.Bits;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.VectorQueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.vector.OnHeapGraph;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeListIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

public class VectorMemoryIndex extends MemoryIndex
{
    private final OnHeapGraph<PrimaryKey> graph;
    private final LongAdder writeCount = new LongAdder();

    private PrimaryKey minimumKey;
    private PrimaryKey maximumKey;

    private final NavigableSet<PrimaryKey> primaryKeys = new ConcurrentSkipListSet<>();

    public VectorMemoryIndex(StorageAttachedIndex index)
    {
        super(index);
        this.graph = new OnHeapGraph<>(index.termType().indexType(), index.indexWriterConfig());
    }

    @Override
    public synchronized long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0 || !index.validateMaxTermSize(key, value, false))
            return 0;

        var primaryKey = index.hasClustering() ? index.keyFactory().create(key, clustering)
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
            different = IntStream.range(0, oldRemaining).anyMatch(i -> oldValue.get(i) != newValue.get(i));
        }

        long bytesUsed = 0;
        if (different)
        {
            var primaryKey = index.hasClustering() ? index.keyFactory().create(key, clustering)
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
    public KeyRangeIterator search(QueryContext queryContext, Expression expr, AbstractBounds<PartitionPosition> keyRange)
    {
        assert expr.getIndexOperator() == Expression.IndexOperator.ANN : "Only ANN is supported for vector search, received " + expr.getIndexOperator();

        VectorQueryContext vectorQueryContext = queryContext.vectorContext();

        var buffer = expr.lower().value.raw;
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
            if (!vectorQueryContext.getShadowedPrimaryKeys().isEmpty())
                resultKeys = resultKeys.stream().filter(pk -> !vectorQueryContext.containsShadowedPrimaryKey(pk)).collect(Collectors.toSet());

            if (resultKeys.isEmpty())
                return KeyRangeIterator.empty();

            int bruteForceRows = maxBruteForceRows(vectorQueryContext.limit(), resultKeys.size(), graph.size());
            Tracing.trace("Search range covers {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                          resultKeys.size(), bruteForceRows, graph.size(), vectorQueryContext.limit());
            if (resultKeys.size() < Math.max(vectorQueryContext.limit(), bruteForceRows))
                return new ReorderingRangeIterator(new PriorityQueue<>(resultKeys));
            else
                bits = new KeyRangeFilteringBits(keyRange, vectorQueryContext.bitsetForShadowedPrimaryKeys(graph));
        }
        else
        {
            // partition/range deletion won't trigger index update, so we have to filter shadow primary keys in memtable index
            bits = queryContext.vectorContext().bitsetForShadowedPrimaryKeys(graph);
        }

        var keyQueue = graph.search(qv, queryContext.vectorContext().limit(), bits);
        if (keyQueue.isEmpty())
            return KeyRangeIterator.empty();
        return new ReorderingRangeIterator(keyQueue);
    }

    @Override
    public KeyRangeIterator limitToTopResults(List<PrimaryKey> primaryKeys, Expression expression, int limit)
    {
        if (minimumKey == null)
            // This case implies maximumKey is empty too.
            return KeyRangeIterator.empty();

        List<PrimaryKey> results = primaryKeys.stream()
                                              .dropWhile(k -> k.compareTo(minimumKey) < 0)
                                              .takeWhile(k -> k.compareTo(maximumKey) <= 0)
                                              .collect(Collectors.toList());

        int maxBruteForceRows = maxBruteForceRows(limit, results.size(), graph.size());
        Tracing.trace("SAI materialized {} rows; max brute force rows is {} for memtable index with {} nodes, LIMIT {}",
                      results.size(), maxBruteForceRows, graph.size(), limit);
        if (results.size() <= maxBruteForceRows)
        {
            if (results.isEmpty())
                return KeyRangeIterator.empty();
            return new KeyRangeListIterator(minimumKey, maximumKey, results);
        }

        ByteBuffer buffer = expression.lower().value.raw;
        float[] qv = index.termType().decomposeVector(buffer);
        var bits = new KeyFilteringBits(results);
        var keyQueue = graph.search(qv, limit, bits);
        if (keyQueue.isEmpty())
            return KeyRangeIterator.empty();
        return new ReorderingRangeIterator(keyQueue);
    }

    private int maxBruteForceRows(int limit, int nPermittedOrdinals, int graphSize)
    {
        int expectedNodesVisited = expectedNodesVisited(limit, nPermittedOrdinals, graphSize);
        int expectedComparisons = index.indexWriterConfig().getMaximumNodeConnections() * expectedNodesVisited;
        // in-memory comparisons are cheaper than pulling a row off disk and then comparing
        // VSTODO this is dramatically oversimplified
        // larger dimension should increase this, because comparisons are more expensive
        // lower chunk cache hit ratio should decrease this, because loading rows is more expensive
        double memoryToDiskFactor = 0.25;
        return (int) max(limit, memoryToDiskFactor * expectedComparisons);
    }

    /**
     * All parameters must be greater than zero.  nPermittedOrdinals may be larger than graphSize.
     */
    public static int expectedNodesVisited(int limit, int nPermittedOrdinals, int graphSize)
    {
        // constants are computed by Code Interpreter based on observed comparison counts in tests
        // https://chat.openai.com/share/2b1d7195-b4cf-4a45-8dce-1b9b2f893c75
        var sizeRestriction = min(nPermittedOrdinals, graphSize);
        var raw = (int) (0.7 * pow(log(graphSize), 2) *
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

            var keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> keyRange.contains(k.partitionKey()));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }

    private class ReorderingRangeIterator extends KeyRangeIterator
    {
        private final PriorityQueue<PrimaryKey> keyQueue;

        ReorderingRangeIterator(PriorityQueue<PrimaryKey> keyQueue)
        {
            super(minimumKey, maximumKey, keyQueue.size());
            this.keyQueue = keyQueue;
        }

        @Override
        // VSTODO maybe we can abuse "current" to avoid having to pop and re-add the last skipped key
        protected void performSkipTo(PrimaryKey nextKey)
        {
            while (!keyQueue.isEmpty() && keyQueue.peek().compareTo(nextKey) < 0)
                keyQueue.poll();
        }

        @Override
        public void close() {}

        @Override
        protected PrimaryKey computeNext()
        {
            if (keyQueue.isEmpty())
                return endOfData();
            return keyQueue.poll();
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
            var pk = graph.keysFromOrdinal(i);
            return results.stream().anyMatch(pk::contains);
        }

        @Override
        public int length()
        {
            return results.size();
        }
    }
}
