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
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class TrieMemtableIndex implements MemtableIndex
{
    private final TrieMemoryIndex index;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedMemoryUsed = new LongAdder();

    public TrieMemtableIndex(IndexContext indexContext)
    {
        this.index = new TrieMemoryIndex(indexContext);
    }

    @Override
    public long writeCount()
    {
        return writeCount.sum();
    }

    public long estimatedMemoryUsed()
    {
        return estimatedMemoryUsed.sum();
    }

    @Override
    public boolean isEmpty()
    {
        return getMinTerm() == null;
    }

    @Override
    @Nullable
    public ByteBuffer getMinTerm()
    {
        return index.getMinTerm();
    }

    @Override
    @Nullable
    public ByteBuffer getMaxTerm()
    {
        return index.getMaxTerm();
    }

    @Override
    public long index(DecoratedKey key, Clustering clustering, ByteBuffer value)
    {
        if (value == null || value.remaining() == 0)
            return 0;

        long ram = index.add(key, clustering, value);
        writeCount.increment();
        estimatedMemoryUsed.add(ram);
        return ram;
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, int limit)
    {
        return index.search(expression, keyRange);
    }

    @Override
    public KeyRangeIterator reorderOneComponent(QueryContext context, KeyRangeIterator iterator, Expression exp, int limit)
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    public Iterator<Pair<ByteComparable, PrimaryKeys>> iterator()
    {
        return index.iterator();
    }
}
