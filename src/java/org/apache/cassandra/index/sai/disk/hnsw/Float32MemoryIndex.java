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
import java.util.function.LongConsumer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;

public class Float32MemoryIndex extends MemoryIndex
{
    @Override
    public void add(DecoratedKey key, Clustering clustering, ByteBuffer value, LongConsumer onHeapAllocationsTracker, LongConsumer offHeapAllocationsTracker)
    {
        int seed = (int) (Math.random() * Integer.MAX_VALUE);
        // TODO expose M and beamWidth as configurable parameters
        var builder = HnswGraphBuilder.create(vectors, VectorEncoding.FLOAT32, VectorSimilarityFunction.COSINE, 16, 100, seed);
        hnsw = builder.build(vectors.copy());

    }

    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        return null;
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        return null;
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        return null;
    }

    @Override
    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        return null;
    }
}
