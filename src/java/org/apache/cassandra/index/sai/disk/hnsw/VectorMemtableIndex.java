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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.Bits;

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
    public long index(DecoratedKey key, Clustering clustering, ByteBuffer value)
    {
        var primaryKey = indexContext.keyFactory().create(key, clustering);
        return index(primaryKey, value);
    }

    private int index(PrimaryKey primaryKey, ByteBuffer value)
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
        return 0;
    }

    @Override
    public KeyRangeIterator search(Expression expr, AbstractBounds<PartitionPosition> keyRange, int limit)
    {
        assert expr.getOp() == Expression.IndexOperator.ANN : "Only ANN is supported for vector search, received " + expr.getOp();

        var buffer = expr.lower.value.raw;
        float[] qv = (float[])indexContext.getValidator().getSerializer().deserialize(buffer);

        Bits bits = null;
        // key range doesn't full token ring, we need to filter keys inside ANN search
        if (!graph.isEmpty() && !coversFullRing(keyRange))
            bits = new KeyRangeFilteringBits(keyRange);

        return new AnnKeyRangeIterator(qv, limit, bits);
    }

    @Override
    public KeyRangeIterator reorderOneComponent(QueryContext context, KeyRangeIterator iterator, Expression exp, int limit)
    {
        Set<PrimaryKey> results = new HashSet<>();
        while (iterator.hasNext())
        {
            var key = iterator.next();
            results.add(key);
        }
        ByteBuffer buffer = exp.lower.value.raw;
        float[] qv = (float[])indexContext.getValidator().getSerializer().deserialize(buffer.duplicate());
        var bits = new KeyFilteringBits(results);
        return new AnnKeyRangeIterator(qv, limit, bits);
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
        return graph.ramBytesUsed();
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

    private class AnnKeyRangeIterator extends KeyRangeIterator
    {
        private final PriorityQueue<PrimaryKey> keyQueue;

        AnnKeyRangeIterator(float[] queryVector, int limit, Bits toAccept)
        {
            super(minimumKey, maximumKey, writeCount.longValue());
            keyQueue = graph.search(queryVector, limit, toAccept, Integer.MAX_VALUE);
        }

        @Override
        // REVIEWME
        // (it's inefficient, but is it correct?)
        // (maybe we can abuse "current" to make it efficient)
        protected void performSkipTo(PrimaryKey nextKey)
        {
            PrimaryKey lastSkipped = null;
            while (!keyQueue.isEmpty() && keyQueue.peek().compareTo(nextKey) < 0)
                lastSkipped = keyQueue.poll();
            if (lastSkipped != null)
                keyQueue.add(lastSkipped);
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
        private final Set<PrimaryKey> results;

        public KeyFilteringBits(Set<PrimaryKey> results)
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
