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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Each segment represents an on-disk index structure (balanced tree/terms/postings) flushed by memory limit or token boundaries.
 * It also helps to reduce resource consumption for read requests as only segments that intersect with read request data
 * range need to be loaded.
 */
public class Segment implements SegmentOrdering, Closeable
{
    private final Token.KeyBound minKeyBound;
    private final Token.KeyBound maxKeyBound;

    // per sstable
    final PrimaryKeyMap.Factory primaryKeyMapFactory;
    // per-segment
    public final SegmentMetadata metadata;

    private final IndexSegmentSearcher index;

    public Segment(StorageAttachedIndex index, SSTableContext sstableContext, PerColumnIndexFiles indexFiles, SegmentMetadata metadata) throws IOException
    {
        this.minKeyBound = metadata.minKey.token().minKeyBound();
        this.maxKeyBound = metadata.maxKey.token().maxKeyBound();

        this.primaryKeyMapFactory = sstableContext.primaryKeyMapFactory;
        this.metadata = metadata;

        this.index = IndexSegmentSearcher.open(primaryKeyMapFactory, indexFiles, metadata, index);
    }

    @VisibleForTesting
    public Segment(Token minKey, Token maxKey)
    {
        this.primaryKeyMapFactory = null;
        this.metadata = null;
        this.minKeyBound = minKey.minKeyBound();
        this.maxKeyBound = maxKey.maxKeyBound();
        this.index = null;
    }

    /**
     * @return true if current segment intersects with query key range
     */
    public boolean intersects(AbstractBounds<PartitionPosition> keyRange)
    {
        if (keyRange instanceof Range && ((Range<?>)keyRange).isWrapAround())
            return keyRange.contains(minKeyBound) || keyRange.contains(maxKeyBound);

        int cmp = keyRange.right.compareTo(minKeyBound);
        // if right is minimum, it means right is the max token and bigger than maxKey.
        // if right bound is less than minKeyBound, no intersection
        if (!keyRange.right.isMinimum() && (!keyRange.inclusiveRight() && cmp == 0 || cmp < 0))
            return false;

        cmp = keyRange.left.compareTo(maxKeyBound);
        // if left bound is bigger than maxKeyBound, no intersection
        return (keyRange.isStartInclusive() || cmp != 0) && cmp <= 0;
    }

    public long indexFileCacheSize()
    {
        return index == null ? 0 : index.indexFileCacheSize();
    }

    /**
     * Search on-disk index synchronously
     *
     * @param expression to filter on disk index
     * @param context to track per sstable cache and per query metrics

     * @return range iterator that matches given expression
     */
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext context) throws IOException
    {
        return index.search(expression, keyRange, context);
    }

    @Override
    public KeyRangeIterator limitToTopKResults(QueryContext context, List<PrimaryKey> primaryKeys, Expression expression) throws IOException
    {
        return index.limitToTopKResults(context, primaryKeys, expression);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(index);
    }

    @Override
    public String toString()
    {
        return String.format("Segment{metadata=%s}", metadata);
    }
}
