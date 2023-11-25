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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * This is an in-memory index using the {@link InMemoryTrie} to store a {@link ByteComparable}
 * representation of the indexed values. Data is stored on-heap or off-heap and follows the
 * settings of the {@link TrieMemtable} to determine where.
 */
public class TrieMemoryIndex extends MemoryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemoryIndex.class);
    private static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    private final InMemoryTrie<PrimaryKeys> data;
    private final PrimaryKeysReducer primaryKeysReducer;
    private final boolean isLiteral;

    private ByteBuffer minTerm;
    private ByteBuffer maxTerm;

    public TrieMemoryIndex(StorageAttachedIndex index)
    {
        super(index);
        this.data = new InMemoryTrie<>(TrieMemtable.BUFFER_TYPE);
        this.primaryKeysReducer = new PrimaryKeysReducer();
        // The use of the analyzer is within a synchronized block so can be considered thread-safe
        this.isLiteral = index.termType().isLiteral();
    }

    /**
     * Adds an index value to the in-memory index
     *
     * @param key partition key for the indexed value
     * @param clustering clustering for the indexed value
     * @param value indexed value
     * @return amount of heap allocated by the new value
     */
    @Override
    public synchronized long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        value = index.termType().asIndexBytes(value);
        final PrimaryKey primaryKey = index.hasClustering() ? index.keyFactory().create(key, clustering)
                                                            : index.keyFactory().create(key);
        final long initialSizeOnHeap = data.sizeOnHeap();
        final long initialSizeOffHeap = data.sizeOffHeap();
        final long reducerHeapSize = primaryKeysReducer.heapAllocations();

        if (index.hasAnalyzer())
        {
            AbstractAnalyzer analyzer = index.analyzer();
            try
            {
                analyzer.reset(value);
                while (analyzer.hasNext())
                {
                    addTerm(primaryKey, analyzer.next());
                }
            }
            finally
            {
                analyzer.end();
            }
        }
        else
        {
            addTerm(primaryKey, value);
        }
        long onHeap = data.sizeOnHeap();
        long offHeap = data.sizeOffHeap();
        long heapAllocations = primaryKeysReducer.heapAllocations();
        return (onHeap - initialSizeOnHeap) + (offHeap - initialSizeOffHeap) + (heapAllocations - reducerHeapSize);
    }

    @Override
    public long update(DecoratedKey key, Clustering<?> clustering, ByteBuffer oldValue, ByteBuffer newValue)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Search for an expression in the in-memory index within the {@link AbstractBounds} defined
     * by keyRange. This can either be an exact match or a range match.
     * <p>
     * @param expression the {@link Expression} to search for
     * @param keyRange the {@link AbstractBounds} containing the key range to restrict the search to
     * @return a {@link KeyRangeIterator} containing the search results
     */
    public KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        if (logger.isTraceEnabled())
            logger.trace("Searching memtable index on expression '{}'...", expression);

        switch (expression.getIndexOperator())
        {
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return exactMatch(expression, keyRange);
            case RANGE:
                return rangeMatch(expression, keyRange);
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }

    /**
     * Returns an {@link Iterator} over the entire dataset contained in the trie. This is used
     * when the index is flushed to disk.
     *
     * @return the iterator containing the trie data
     */
    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        Iterator<Map.Entry<ByteComparable, PrimaryKeys>> iterator = data.entrySet().iterator();
        return new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Pair<ByteComparable, PrimaryKeys> next()
            {
                Map.Entry<ByteComparable, PrimaryKeys> entry = iterator.next();
                return Pair.create(decode(entry.getKey()), entry.getValue());
            }
        };
    }

    @Override
    public SegmentMetadata.ComponentMetadataMap writeDirect(IndexDescriptor indexDescriptor,
                                                            IndexIdentifier indexIdentifier,
                                                            Function<PrimaryKey, Integer> postingTransformer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty()
    {
        return minTerm == null;
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        return maxTerm;
    }

    private void addTerm(PrimaryKey primaryKey, ByteBuffer term)
    {
        if (index.validateMaxTermSize(primaryKey.partitionKey(), term, false))
        {
            setMinMaxTerm(term.duplicate());

            final ByteComparable comparableBytes = asComparableBytes(term);

            try
            {
                if (term.limit() <= MAX_RECURSIVE_KEY_LENGTH)
                {
                    data.putRecursive(comparableBytes, primaryKey, primaryKeysReducer);
                }
                else
                {
                    data.apply(Trie.singleton(comparableBytes, primaryKey), primaryKeysReducer);
                }
            }
            catch (InMemoryTrie.SpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private void setMinMaxTerm(ByteBuffer term)
    {
        assert term != null;

        minTerm = index.termType().min(term, minTerm);
        maxTerm = index.termType().max(term, maxTerm);
    }

    private ByteComparable asComparableBytes(ByteBuffer input)
    {
        return isLiteral ? version -> terminated(ByteSource.of(input, version))
                         : version -> index.termType().asComparableBytes(input, version);
    }

    private ByteComparable decode(ByteComparable term)
    {
        return isLiteral ? version -> ByteSourceInverse.unescape(ByteSource.peekable(term.asComparableBytes(version)))
                         : term;
    }

    private ByteSource terminated(ByteSource src)
    {
        return new ByteSource()
        {
            boolean done = false;

            @Override
            public int next()
            {
                if (done)
                    return END_OF_STREAM;
                int n = src.next();
                if (n != END_OF_STREAM)
                    return n;

                done = true;
                return ByteSource.TERMINATOR;
            }
        };
    }

    private KeyRangeIterator exactMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        ByteComparable comparableMatch = expression.lower() == null ? ByteComparable.EMPTY
                                                                    : asComparableBytes(expression.lower().value.encoded);
        PrimaryKeys primaryKeys = data.get(comparableMatch);
        return primaryKeys == null ? KeyRangeIterator.empty()
                                   : new FilteringInMemoryKeyRangeIterator(primaryKeys.keys(), keyRange);
    }

    private static class Collector
    {
        private static final int MINIMUM_QUEUE_SIZE = 128;

        // Maintain the last queue size used on this index to use for the next range match.
        // This allows for receiving a stream of wide range queries where the queue size
        // is larger than we would want to default the size to.
        // TODO Investigate using a decaying histogram here to avoid the effect of outliers.
        private static final FastThreadLocal<Integer> lastQueueSize = new FastThreadLocal<>()
        {
            protected Integer initialValue()
            {
                return MINIMUM_QUEUE_SIZE;
            }
        };

        PrimaryKey minimumKey = null;
        PrimaryKey maximumKey = null;
        final PriorityQueue<PrimaryKey> mergedKeys = new PriorityQueue<>(lastQueueSize.get());

        final AbstractBounds<PartitionPosition> keyRange;

        public Collector(AbstractBounds<PartitionPosition> keyRange)
        {
            this.keyRange = keyRange;
        }

        public void processContent(PrimaryKeys keys)
        {
            if (keys.isEmpty())
                return;

            SortedSet<PrimaryKey> primaryKeys = keys.keys();

            // shortcut to avoid generating iterator
            if (primaryKeys.size() == 1)
            {
                processKey(primaryKeys.first());
                return;
            }

            // skip entire partition keys if they don't overlap
            if (!keyRange.right.isMinimum() && primaryKeys.first().partitionKey().compareTo(keyRange.right) > 0
                || primaryKeys.last().partitionKey().compareTo(keyRange.left) < 0)
                return;

            primaryKeys.forEach(this::processKey);
        }

        public void updateLastQueueSize()
        {
            lastQueueSize.set(Math.max(MINIMUM_QUEUE_SIZE, mergedKeys.size()));
        }

        private void processKey(PrimaryKey key)
        {
            if (keyRange.contains(key.partitionKey()))
            {
                mergedKeys.add(key);

                minimumKey = minimumKey == null ? key : key.compareTo(minimumKey) < 0 ? key : minimumKey;
                maximumKey = maximumKey == null ? key : key.compareTo(maximumKey) > 0 ? key : maximumKey;
            }
        }
    }

    private KeyRangeIterator rangeMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        ByteComparable lowerBound, upperBound;
        boolean lowerInclusive, upperInclusive;
        if (expression.lower() != null)
        {
            lowerBound = asComparableBytes(expression.lower().value.encoded);
            lowerInclusive = expression.lower().inclusive;
        }
        else
        {
            lowerBound = ByteComparable.EMPTY;
            lowerInclusive = false;
        }

        if (expression.upper() != null)
        {
            upperBound = asComparableBytes(expression.upper().value.encoded);
            upperInclusive = expression.upper().inclusive;
        }
        else
        {
            upperBound = null;
            upperInclusive = false;
        }

        Collector cd = new Collector(keyRange);

        data.subtrie(lowerBound, lowerInclusive, upperBound, upperInclusive)
            .values()
            .forEach(cd::processContent);

        if (cd.mergedKeys.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        cd.updateLastQueueSize();

        return new InMemoryKeyRangeIterator(cd.minimumKey, cd.maximumKey, cd.mergedKeys);
    }

    private static class PrimaryKeysReducer implements InMemoryTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
    {
        private final LongAdder heapAllocations = new LongAdder();

        @Override
        public PrimaryKeys apply(PrimaryKeys existing, PrimaryKey neww)
        {
            if (existing == null)
            {
                existing = new PrimaryKeys();
                heapAllocations.add(existing.unsharedHeapSize());
            }
            heapAllocations.add(existing.add(neww));
            return existing;
        }

        long heapAllocations()
        {
            return heapAllocations.longValue();
        }
    }
}
