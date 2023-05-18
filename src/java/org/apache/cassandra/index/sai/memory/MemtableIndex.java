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

import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.hnsw.VectorMemtableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.SegementOrdering;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface MemtableIndex extends SegementOrdering
{
    long writeCount();

    long estimatedMemoryUsed();

    boolean isEmpty();

    @Nullable
    ByteBuffer getMinTerm();

    @Nullable
    ByteBuffer getMaxTerm();

    /**
     * @return ram used by the new entry
     */
    long index(DecoratedKey key, Clustering clustering, ByteBuffer value);

    KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, int limit);

    @Override
    // memtable version does not throw IOException
    KeyRangeIterator reorderOneComponent(QueryContext context, KeyRangeIterator iterator, Expression exp, int limit);

    Iterator<Pair<ByteComparable, PrimaryKeys>> iterator();

    static MemtableIndex createIndex(IndexContext indexContext)
    {
        return indexContext.isVector() ? new VectorMemtableIndex(indexContext) : new TrieMemtableIndex(indexContext);
    }
}
