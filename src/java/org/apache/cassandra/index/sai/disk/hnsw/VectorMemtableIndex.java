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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Float32DenseVectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.Item;
import com.github.jelmerk.knn.hnsw.HnswIndex;

public class VectorMemtableIndex implements MemtableIndex
{
    private final IndexContext indexContext;
    private final AtomicReference<HnswIndex<DecoratedKey, float[], VectorItem, Float>> hnswRef = new AtomicReference<>();
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedOnHeapMemoryUsed = new LongAdder();

    private final int universeSize = 1_000_000; // TODO make hnsw growable instead of hardcoding size
    private final AtomicInteger cachedDimensions = new AtomicInteger();

    public VectorMemtableIndex(IndexContext indexContext) {
        this.indexContext = indexContext;
    }

    @Override
    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        var hnsw = hnswRef.get();
        if (hnsw == null)
        {
            // hnsw wants to know the dimensionality of its vectors, but we don't know that until we add the first one
            var firstVector = Float32DenseVectorType.Serializer.instance.deserialize(value);
            HnswIndex<DecoratedKey, float[], VectorItem, Float> newHnsw = HnswIndex
                                                                          .newBuilder(firstVector.length, DistanceFunctions.FLOAT_INNER_PRODUCT, universeSize)
                                                                          .withM(16) // TODO
                                                                          .withEf(200) // TODO
                                                                          .withEfConstruction(200) // TODO
                                                                          .build();
            hnsw = hnswRef.compareAndExchange(null, newHnsw);
        }
        var item = new VectorItem(key, value);
        hnsw.add(item);
        writeCount.increment();
        estimatedOnHeapMemoryUsed.add(ObjectSizes.measureDeep(item));
    }

    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        // FIXME
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        return null;
    }

    @Override
    public long writeCount()
    {
        return writeCount.longValue();
    }

    @Override
    public long estimatedOnHeapMemoryUsed()
    {
        return estimatedOnHeapMemoryUsed.longValue();
    }

    @Override
    public long estimatedOffHeapMemoryUsed()
    {
        return 0;
    }

    @Override
    public boolean isEmpty()
    {
        var hnsw = hnswRef.get();
        return hnsw == null || hnsw.size() == 0;
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

    private class VectorItem implements Item<DecoratedKey, float[]>
    {
        private final DecoratedKey key;
        private final ByteBuffer buffer;

        public VectorItem(DecoratedKey key, ByteBuffer buffer)
        {
            this.key = key;
            this.buffer = buffer;
        }

        @Override
        public DecoratedKey id()
        {
            return key;
        }

        @Override
        public float[] vector()
        {
            return Float32DenseVectorType.Serializer.instance.deserialize(buffer);
        }

        @Override
        public int dimensions()
        {
            // if cached dimensions is 0, then this is being called for the first time;
            // compute it from the current vector length
            int i = cachedDimensions.get();
            if (i == 0)
            {
                i = vector().length;
                cachedDimensions.set(i);
            }
            return i;
        }
    }
}
