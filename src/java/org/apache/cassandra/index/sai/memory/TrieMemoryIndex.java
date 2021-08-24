/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class TrieMemoryIndex extends MemoryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemoryIndex.class);
    private static final int MINIMUM_QUEUE_SIZE = 128;
    private static final int MAX_RECURSIVE_KEY_LENGTH = 128;


    private final MemtableTrie<PrimaryKeys> data;
    private final ClusteringComparator clusteringComparator;
    private final PrimaryKeysReducer primaryKeysReducer;
    private final AbstractAnalyzer.AnalyzerFactory analyzerFactory;
    private final AbstractType<?> validator;
    private final boolean isLiteral;
    private final Object writeLock = new Object();

    private static final FastThreadLocal<Integer> lastQueueSize = new FastThreadLocal<Integer>()
    {
        protected Integer initialValue()
        {
            return MINIMUM_QUEUE_SIZE;
        }
    };


    public TrieMemoryIndex(ColumnContext columnContext)
    {
        super(columnContext);
        //TODO Do we need to follow a setting for this?
        this.data = new MemtableTrie<>(BufferType.OFF_HEAP);
        this.clusteringComparator = columnContext.clusteringComparator();
        this.primaryKeysReducer = new PrimaryKeysReducer();
        // MemoryIndex is per-core, so analyzer should be thread-safe..
        this.analyzerFactory = columnContext.getAnalyzerFactory();
        this.validator = columnContext.getValidator();
        this.isLiteral = TypeUtil.isLiteral(validator);
    }

    @Override
    public long add(DecoratedKey key, Clustering clustering, ByteBuffer value)
    {
        synchronized (writeLock)
        {
            AbstractAnalyzer analyzer = analyzerFactory.create();
            try
            {
                value = TypeUtil.encode(value, validator);
                analyzer.reset(value.duplicate());
                final PrimaryKey primaryKey = PrimaryKey.of(key, clustering);
                final long initialSizeOnHeap = data.sizeOnHeap();
                final long initialSizeOffHeap = data.sizeOffHeap();
                final long reducerHeapSize = primaryKeysReducer.heapAllocations();

                while (analyzer.hasNext())
                {
                    final ByteBuffer term = analyzer.next();

                    setMinMaxTerm(term.duplicate());

                    final ByteComparable encodedTerm = encode(term.duplicate());

                    try
                    {
                        if (term.limit() <= MAX_RECURSIVE_KEY_LENGTH)
                        {
                            data.putRecursive(encodedTerm, primaryKey, primaryKeysReducer);
                        }
                        else
                        {
                            data.apply(Trie.singleton(encodedTerm, primaryKey), primaryKeysReducer);
                        }
                    }
                    catch (MemtableTrie.SpaceExhaustedException e)
                    {
                        //TODO Handle this properly
                        throw new RuntimeException(e);
                    }
                }

                return (data.sizeOnHeap() - initialSizeOnHeap) + (data.sizeOffHeap() - initialSizeOffHeap) + (primaryKeysReducer.heapAllocations() - reducerHeapSize);
            }
            finally
            {
                analyzer.end();
            }
        }
    }

    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        if (logger.isTraceEnabled())
            logger.trace("Searching memtable index on expression '{}'...", expression);

        switch (expression.getOp())
        {
            case MATCH:
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
                return exactMatch(expression);
            case RANGE:
                return rangeMatch(expression, keyRange);
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }

    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        Iterator<Map.Entry<ByteComparable, PrimaryKeys>> iterator = data.entrySet().iterator();
        return new Iterator<Pair<ByteComparable, PrimaryKeys>>()
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

    private ByteComparable encode(ByteBuffer input)
    {
        return isLiteral ? version -> append(ByteSource.of(input, version), ByteSource.TERMINATOR)
                         : version -> TypeUtil.asComparableBytes(input, validator, version);
    }

    private ByteComparable decode(ByteComparable term)
    {
        return isLiteral ? version -> ByteSourceInverse.unescape(ByteSource.peekable(term.asComparableBytes(version)))
                         : term;

    }

    private ByteSource append(ByteSource src, int lastByte)
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
                return lastByte;
            }
        };
    }

    private RangeIterator exactMatch(Expression expression)
    {
        final ByteComparable prefix = expression.lower == null ? ByteComparable.EMPTY : encode(expression.lower.value.encoded);
        final PrimaryKeys primaryKeys = data.get(prefix);
        if (primaryKeys == null)
        {
            return RangeIterator.empty();
        }
        return new KeyRangeIterator(primaryKeys.partitionKeys());
    }

    public static class Collector
    {
        long minimumTokenValue = Long.MAX_VALUE;
        long maximumTokenValue = Long.MIN_VALUE;
        PriorityQueue<DecoratedKey> mergedKeys = new PriorityQueue<>(lastQueueSize.get(), DecoratedKey.comparator);

        AbstractBounds<PartitionPosition> keyRange;

        public Collector(AbstractBounds<PartitionPosition> keyRange)
        {
            this.keyRange = keyRange;
        }

        public void processContent(PrimaryKeys keys)
        {
            if (keys.isEmpty())
                return;

            SortedSet<DecoratedKey> partitionKeys = keys.partitionKeys();

            // shortcut to avoid generating iterator
            if (partitionKeys.size() == 1)
            {
                DecoratedKey first = partitionKeys.first();
                if (keyRange.contains(first))
                {
                    mergedKeys.add(first);

                    long currentTokenValue = first.getToken().getLongValue();
                    minimumTokenValue = Math.min(minimumTokenValue, currentTokenValue);
                    maximumTokenValue = Math.max(maximumTokenValue, currentTokenValue);
                }

                return;
            }

            // skip entire partition keys if they don't overlap
            if (!keyRange.right.isMinimum() && partitionKeys.first().compareTo(keyRange.right) > 0
                    || partitionKeys.last().compareTo(keyRange.left) < 0)
                return;

            for (DecoratedKey key : partitionKeys)
            {
                if (keyRange.contains(key))
                {
                    mergedKeys.add(key);

                    long currentTokenValue = key.getToken().getLongValue();
                    minimumTokenValue = Math.min(minimumTokenValue, currentTokenValue);
                    maximumTokenValue = Math.max(maximumTokenValue, currentTokenValue);
                }
            }
            return;
        }
    }

    private RangeIterator rangeMatch(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        ByteComparable lowerBound, upperBound;
        boolean lowerInclusive, upperInclusive;
        if (expression.lower != null)
        {
            lowerBound = encode(expression.lower.value.encoded);
            lowerInclusive = expression.lower.inclusive;
        }
        else
        {
            lowerBound = ByteComparable.EMPTY;
            lowerInclusive = false;
        }

        if (expression.upper != null)
        {
            upperBound = encode(expression.upper.value.encoded);
            upperInclusive = expression.upper.inclusive;
        }
        else
        {
            upperBound = null;
            upperInclusive = false;
        }

        Collector cd = new Collector(keyRange);

        data.subtrie(lowerBound, lowerInclusive, upperBound, upperInclusive).values().forEach(pk -> cd.processContent(pk));

        if (cd.mergedKeys.isEmpty())
        {
            return RangeIterator.empty();
        }

        lastQueueSize.set(Math.max(MINIMUM_QUEUE_SIZE, cd.mergedKeys.size()));
        return new KeyRangeIterator(cd.minimumTokenValue, cd.maximumTokenValue, cd.mergedKeys);
    }

    private class PrimaryKeysReducer implements MemtableTrie.UpsertTransformer<PrimaryKeys, PrimaryKey>
    {
        private final LongAdder heapAllocations = new LongAdder();

        @Override
        public PrimaryKeys apply(PrimaryKeys existing, PrimaryKey neww)
        {
            if (existing == null)
            {
                existing = PrimaryKeys.create(clusteringComparator);
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
