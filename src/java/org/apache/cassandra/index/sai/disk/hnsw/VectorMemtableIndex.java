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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.ReadWriteLockedList;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.hnsw.ConcurrentHnswGraphBuilder;
import org.apache.lucene.util.hnsw.ConcurrentHnswGraphFactory;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class VectorMemtableIndex implements MemtableIndex
{
    private final IndexContext indexContext;
    private final ByteBufferVectorValues vectorValues = new ByteBufferVectorValues();
    private final List<PrimaryKey> keys = ReadWriteLockedList.wrap(new ArrayList<>());
    private final ConcurrentHnswGraphBuilder<float[]> builder;
    private final LongAdder writeCount = new LongAdder();

    private final AtomicInteger cachedDimensions = new AtomicInteger();

    private static final Token.KeyBound MIN_KEY_BOUND = DatabaseDescriptor.getPartitioner().getMinimumToken().minKeyBound();

    private PrimaryKey minimumKey;
    private PrimaryKey maximumKey;

    public VectorMemtableIndex(IndexContext indexContext) {
        this.indexContext = indexContext;
        try
        {
            ConcurrentHnswGraphFactory factory = ConcurrentHnswGraphFactory.instance;
            builder = factory.createBuilder(vectorValues,
                                            VectorEncoding.FLOAT32,
                                            indexContext.getIndexWriterConfig().getSimilarityFunction(),
                                            indexContext.getIndexWriterConfig().getMaximumNodeConnections(),
                                            indexContext.getIndexWriterConfig().getConstructionBeamWidth(),
                                            ThreadLocalRandom.current().nextLong());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    // TODO either we need to create a concurrent graph builder (possible!), or
    // do sharding in the memtable with brute force search, followed by building the actual graph on flush
    @Override
    public long index(DecoratedKey key, Clustering clustering, ByteBuffer value)
    {
        var primaryKey = indexContext.keyFactory().create(key, clustering);
        if (minimumKey == null)
            minimumKey = primaryKey;
        else if (primaryKey.compareTo(minimumKey) < 0)
            minimumKey = primaryKey;
        if (maximumKey == null)
            maximumKey = primaryKey;
        else if (primaryKey.compareTo(maximumKey) > 0)
            maximumKey = primaryKey;
        keys.add(primaryKey);
        var vector = vectorValues.add(value);
        writeCount.increment();
        try
        {
            builder.addGraphNode(vectorValues.size() - 1, vector);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return 0;
    }

    @Override
    public KeyRangeIterator search(Expression expr, AbstractBounds<PartitionPosition> keyRange, int limit)
    {
        assert expr.getOp() == Expression.IndexOperator.ANN : "Only ANN is supported for vector search, received " + expr.getOp();

        var buffer = expr.lower.value.raw;
        float[] qv = (float[])indexContext.getValidator().getSerializer().deserialize(buffer);

        return new BatchKeyRangeIterator(qv, limit, keyRange);
    }

    private static boolean coversFullRing(AbstractBounds<PartitionPosition> keyRange)
    {
        return keyRange.left.equals(MIN_KEY_BOUND) && keyRange.right.equals(MIN_KEY_BOUND);
    }

    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long writeCount()
    {
        return writeCount.longValue();
    }

    @Override
    public long estimatedMemoryUsed()
    {
        return vectorValues.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    @Override
    public boolean isEmpty()
    {
        return vectorValues.size() == 0;
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

        public KeyRangeFilteringBits(AbstractBounds<PartitionPosition> keyRange)
        {
            this.keyRange = keyRange;
        }

        @Override
        public boolean get(int index)
        {
            PrimaryKey key = keys.get(index);
            return keyRange.contains(key.partitionKey());
        }

        @Override
        public int length()
        {
            return keys.size();
        }
    }

    private class BatchKeyRangeIterator extends KeyRangeIterator
    {
        private final float[] queryVector;
        private int limit;

        private Bits bits;
        private PriorityQueue<PrimaryKey> keyQueue = new PriorityQueue<>();

        BatchKeyRangeIterator(float[] queryVector, int limit, AbstractBounds<PartitionPosition> keyRange)
        {
            super(minimumKey, maximumKey, writeCount.longValue());
            this.queryVector = queryVector;
            this.limit = limit;
            // key range doesn't full token ring, we need to filter keys inside ANN search
            if (!keys.isEmpty() && !coversFullRing(keyRange))
                bits = new KeyRangeFilteringBits(keyRange);
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            PrimaryKey key;
            while ((key = doComputeNext()) != null)
            {
                if (key.compareTo(nextKey) >= 0)
                    break;
                keyQueue.poll();
            }
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        protected PrimaryKey computeNext()
        {
            return doComputeNext() == null ? null : keyQueue.poll();
        }

        private PrimaryKey doComputeNext()
        {
            if (keyQueue.isEmpty())
            {
                readBatch();
                if (keyQueue.isEmpty())
                    return null;
            }
            return keyQueue.peek();
        }

        private void readBatch()
        {
            try
            {
                NeighborQueue neighborQueue = HnswGraphSearcher.search(queryVector,
                                                                       limit,
                                                                       vectorValues,
                                                                       VectorEncoding.FLOAT32,
                                                                       indexContext.getIndexWriterConfig().getSimilarityFunction(),
                                                                       builder.getGraph(),
                                                                       bits,
                                                                       Integer.MAX_VALUE);
                if (bits == null || bits instanceof KeyRangeFilteringBits)
                    bits = new InvertedFilteringBits(bits);

                for (int node : neighborQueue.nodes())
                {
                    ((InvertedFilteringBits)bits).set(node);
                    keyQueue.add(keys.get(node));
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private class InvertedFilteringBits implements Bits
    {
        private final BitSet ignoredBits = new SparseFixedBitSet(writeCount.intValue());
        private final Bits rangeBits;

        InvertedFilteringBits(Bits rangeBits)
        {
            this.rangeBits = rangeBits;
        }

        public void set(int index)
        {
            ignoredBits.set(index);
        }

        @Override
        public boolean get(int index)
        {
            return (rangeBits == null || rangeBits.get(index)) && !ignoredBits.get(index);
        }

        @Override
        public int length()
        {
            return ignoredBits.length();
        }
    }

    private class ByteBufferVectorValues implements RandomAccessVectorValues<float[]>
    {
        private final List<ByteBuffer> buffers = ReadWriteLockedList.wrap(new ArrayList<>());

        public ByteBufferVectorValues() {}

        @Override
        public int size()
        {
            return buffers.size();
        }

        @Override
        public int dimension()
        {
            // if cached dimensions is 0, then this is being called for the first time;
            // compute it from the current vector length
            int i = cachedDimensions.get();
            if (i == 0)
            {
                i = vectorValue(0).length;
                cachedDimensions.set(i);
            }
            return i;
        }

        @Override
        public float[] vectorValue(int i)
        {
            return (float[])indexContext.getValidator().getSerializer().deserialize(buffers.get(i));
        }

        public float[] add(ByteBuffer buffer) {
            buffers.add(buffer);
            return vectorValue(buffers.size() - 1);
        }

        @Override
        public RandomAccessVectorValues<float[]> copy()
        {
            return this;
        }

        public long ramBytesUsed()
        {
            return ObjectSizes.measure(buffers) + buffers.size() * (4L * dimension());
        }
    }
}
