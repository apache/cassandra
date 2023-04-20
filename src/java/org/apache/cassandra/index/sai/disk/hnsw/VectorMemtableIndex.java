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
import org.github.jamm.MemoryMeter;

public class VectorMemtableIndex implements MemtableIndex
{
    private final IndexContext indexContext;
    private final HnswIndex<DecoratedKey, float[], VectorItem, Float> hnsw;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedOnHeapMemoryUsed = new LongAdder();

    private final int universeSize = 1_000_000; // TODO make hnsw growable instead of hardcoding size

    public VectorMemtableIndex(IndexContext indexContext) {
        hnsw = HnswIndex
               .newBuilder(vectorDimensions, DistanceFunctions.FLOAT_INNER_PRODUCT, universeSize)
               .withM(16) // TODO
               .withEf(200) // TODO
               .withEfConstruction(200) // TODO
               .build();
        this.indexContext = indexContext;
    }

    @Override
    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        var item = new VectorItem(key, Float32DenseVectorType.Serializer.instance.deserialize(value));
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
        return hnsw.size() > 1;
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

    private static class VectorItem implements Item<DecoratedKey, float[]>
    {
        private final DecoratedKey key;
        private final float[] vector;

        public VectorItem(DecoratedKey key, float[] vector)
        {
            this.key = key;
            this.vector = vector;
        }

        @Override
        public DecoratedKey id()
        {
            return key;
        }

        @Override
        public float[] vector()
        {
            return vector;
        }

        @Override
        public int dimensions()
        {
            return vector.length;
        }
    }
}
