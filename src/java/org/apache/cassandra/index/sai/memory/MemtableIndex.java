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
import java.util.function.Function;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface MemtableIndex extends MemtableOrdering
{
    long writeCount();

    long estimatedMemoryUsed();

    boolean isEmpty();

    Memtable getMemtable();

    ByteBuffer getMinTerm();

    ByteBuffer getMaxTerm();

    long index(DecoratedKey key, Clustering<?> clustering, ByteBuffer value);

    /** Implementation only for Vector Indexes */
    default long update(DecoratedKey key, Clustering<?> clustering, ByteBuffer oldValue, ByteBuffer newValue)
    {
        throw new UnsupportedOperationException();
    }

    KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange);

    /**
     * Returns an iterator over the in-memory index in ascending term order.
     * <p>
     * Returned data may contain keys outside [minKey, maxKey]. The bounds
     * are used for shard selection only; implementations without sharding may
     * ignore them.
     * <p>
     * Callers should invoke this method only when the source Memtable is no longer accepting
     * writes. Otherwise, concurrent inserts may be observed partway through iteration.
     */
    Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator(DecoratedKey min, DecoratedKey max);

    /** Implementation only for Vector Indexes */
    default SegmentMetadata.ComponentMetadataMap writeDirect(IndexDescriptor indexDescriptor,
                                                             IndexIdentifier indexIdentifier,
                                                             Function<PrimaryKey, Integer> postingTransformer) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default CloseableIterator<PrimaryKeyWithScore> orderBy(QueryContext queryContext, Expression orderer, AbstractBounds<PartitionPosition> keyRange)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default CloseableIterator<PrimaryKeyWithScore> orderResultsBy(QueryContext queryContext, List<PrimaryKey> results, Expression orderer)
    {
        throw new UnsupportedOperationException();
    }
}
