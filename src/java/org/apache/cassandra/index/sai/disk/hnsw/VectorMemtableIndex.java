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
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.LongAdder;
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
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;

public class VectorMemtableIndex implements MemtableIndex
{
    private final IndexContext indexContext;
    private final CassandraOnHeapHnsw graph;
    private final LongAdder writeCount = new LongAdder();

    private static final Token.KeyBound MIN_KEY_BOUND = DatabaseDescriptor.getPartitioner().getMinimumToken().minKeyBound();

    private PrimaryKey minimumKey;
    private PrimaryKey maximumKey;

    public VectorMemtableIndex(IndexContext indexContext) {
        this.indexContext = indexContext;
        this.graph = new CassandraOnHeapHnsw(indexContext);
    }

    // FIXME horrible no good hack that compacts in-memory
    public void index(PrimaryKey key, float[] vector)
    {
        graph.put(key, VectorType.Serializer.getByteBuffer(vector));
    }

    @Override
    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        var primaryKey = indexContext.keyFactory().create(key, clustering);
        index(primaryKey, value);
    }

    private void index(PrimaryKey primaryKey, ByteBuffer value)
    {
        if (minimumKey == null)
            minimumKey = primaryKey;
        else if (primaryKey.compareTo(minimumKey) < 0)
            minimumKey = primaryKey;
        if (maximumKey == null)
            maximumKey = primaryKey;
        else if (primaryKey.compareTo(maximumKey) > 0)
            maximumKey = primaryKey;

        writeCount.increment();
        graph.put(primaryKey, value);
    }

    @Override
    public RangeIterator search(Expression expr, AbstractBounds<PartitionPosition> keyRange, int limit)
    {
        assert expr.getOp() == Expression.Op.ANN : "Only ANN is supported for vector search, received " + expr.getOp();

        var buffer = expr.lower.value.raw;
        float[] qv = (float[])indexContext.getValidator().getSerializer().deserialize(buffer);

        return new BatchKeyRangeIterator(qv, limit, keyRange);
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
        return graph.ramBytesUsed();
    }

    @Override
    public long estimatedOffHeapMemoryUsed()
    {
        return 0;
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

    public void writeData(IndexDescriptor descriptor, IndexContext context, Map<PrimaryKey, Integer> keyToRowId) throws IOException
    {
        graph.write(descriptor, context, keyToRowId);
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
            var keys = graph.keysFromOrdinal(index);
            return keys.stream().anyMatch(k -> keyRange.contains(k.partitionKey()));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }

    private class BatchKeyRangeIterator extends RangeIterator
    {
        private final float[] queryVector;
        private final int limit;

        private Bits bits;
        private final PriorityQueue<PrimaryKey> keyQueue = new PriorityQueue<>();

        BatchKeyRangeIterator(float[] queryVector, int limit, AbstractBounds<PartitionPosition> keyRange)
        {
            super(minimumKey, maximumKey, writeCount.longValue());
            this.queryVector = queryVector;
            this.limit = limit;
            // key range doesn't full token ring, we need to filter keys inside ANN search
            if (!graph.isEmpty() && !coversFullRing(keyRange))
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
        public void close()
        {
        }

        @Override
        protected PrimaryKey computeNext()
        {
            if (doComputeNext() == null) {
                return endOfData();
            }
            return keyQueue.poll();
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
            var results = graph.search(queryVector, limit, bits, Integer.MAX_VALUE);
            if (bits == null || bits instanceof KeyRangeFilteringBits)
                bits = new InvertedFilteringBits(bits);

            while (results.hasNext())
            {
                var r = results.next();
                ((InvertedFilteringBits)bits).set(r.vectorOrdinal);
                keyQueue.addAll(r.keys);
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
}
