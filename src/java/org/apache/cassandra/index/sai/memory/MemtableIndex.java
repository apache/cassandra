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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class MemtableIndex implements MemtableOrdering
{
    private final MemoryIndex memoryIndex;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedMemoryUsed = new LongAdder();

    public MemtableIndex(StorageAttachedIndex index)
    {
        this.memoryIndex = index.termType().isVector() ? new VectorMemoryIndex(index) : new TrieMemoryIndex(index);
    }

    public long writeCount()
    {
        return writeCount.sum();
    }

    public long estimatedMemoryUsed()
    {
        return estimatedMemoryUsed.sum();
    }

    public boolean isEmpty()
    {
        return memoryIndex.isEmpty();
    }

    public ByteBuffer getMinTerm()
    {
        return memoryIndex.getMinTerm();
    }

    public ByteBuffer getMaxTerm()
    {
        return memoryIndex.getMaxTerm();
    }

    public long index(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return 0;

        long ram = memoryIndex.add(key, clustering, value);
        writeCount.increment();
        estimatedMemoryUsed.add(ram);
        return ram;
    }

    public long update(DecoratedKey key, Clustering<?> clustering, ByteBuffer oldValue, ByteBuffer newValue)
    {
        return memoryIndex.update(key, clustering, oldValue, newValue);
    }

    public KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        return memoryIndex.search(queryContext, expression, keyRange);
    }

    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        return memoryIndex.iterator();
    }

    public SegmentMetadata.ComponentMetadataMap writeDirect(IndexDescriptor indexDescriptor,
                                                            IndexIdentifier indexIdentifier,
                                                            Function<PrimaryKey, Integer> postingTransformer) throws IOException
    {
        return memoryIndex.writeDirect(indexDescriptor, indexIdentifier, postingTransformer);
    }

    @Override
    public KeyRangeIterator limitToTopResults(List<PrimaryKey> primaryKeys, Expression expression, int limit)
    {
        return memoryIndex.limitToTopResults(primaryKeys, expression, limit);
    }
}
