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
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.memory.FilteringKeyRangeIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class VectorMemtableIndex implements MemtableIndex
{
    private final IndexContext indexContext;
    private final ByteBufferVectorValues vectorValues = new ByteBufferVectorValues();
    private final ArrayList<PrimaryKey> keys = new ArrayList<>();
    private final HnswGraphBuilder<float[]> builder;
    private final LongAdder writeCount = new LongAdder();

    private final AtomicInteger cachedDimensions = new AtomicInteger();

    private static final Token.KeyBound MIN_KEY_BOUND = DatabaseDescriptor.getPartitioner().getMinimumToken().minKeyBound();

    public VectorMemtableIndex(IndexContext indexContext) {
        this.indexContext = indexContext;
        try
        {
            builder = HnswGraphBuilder.create(vectorValues,
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
    public synchronized void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        var primaryKey = indexContext.keyFactory().create(key, clustering);
        keys.add(primaryKey);
        var vector = vectorValues.add(value);
        try
        {
            builder.addGraphNode(vectorValues.size() - 1, vector);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized RangeIterator search(Expression expr, AbstractBounds<PartitionPosition> keyRange, int limit)
    {
        assert expr.getOp() == Expression.Op.ANN : "Only ANN is supported for vector search, received " + expr.getOp();

        var buffer = expr.lower.value.raw;
        var qv = VectorType.Serializer.instance.deserialize(buffer);
        NeighborQueue nn;
        try
        {
            Bits bits = null;
            // key range doesn't full token ring, we need to filter keys inside ANN search
            if (!keys.isEmpty() && !coversFullRing(keyRange))
                bits = new KeyRangeFilteringBits(keyRange);

            nn = HnswGraphSearcher.search(qv,
                                          limit,
                                          vectorValues,
                                          VectorEncoding.FLOAT32,
                                          indexContext.getIndexWriterConfig().getSimilarityFunction(),
                                          builder.getGraph(),
                                          bits,
                                          Integer.MAX_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        TreeSet<PrimaryKey> keys = Arrays.stream(nn.nodes())
                         .mapToObj(this.keys::get)
                         .collect(Collectors.toCollection(TreeSet::new));

        return keys.isEmpty() ? RangeIterator.empty() : new FilteringKeyRangeIterator(keys, keyRange);
    }

    private static boolean coversFullRing(AbstractBounds<PartitionPosition> keyRange)
    {
        return keyRange.left.equals(MIN_KEY_BOUND) && keyRange.right.equals(MIN_KEY_BOUND);
    }

    @Override
    public Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public long writeCount()
    {
        return writeCount.longValue();
    }

    @Override
    public long estimatedOnHeapMemoryUsed()
    {
        return vectorValues.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    @Override
    public long estimatedOffHeapMemoryUsed()
    {
        return 0;
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
    private class ByteBufferVectorValues implements RandomAccessVectorValues<float[]>
    {
        private final ArrayList<ByteBuffer> buffers = new ArrayList<>();

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
            return VectorType.Serializer.instance.deserialize(buffers.get(i));
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
            return ObjectSizes.measure(buffers) + buffers.size() * (4 + 4L * dimension());
        }
    }
}
