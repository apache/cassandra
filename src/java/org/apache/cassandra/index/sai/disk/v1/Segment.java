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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import org.slf4j.Logger;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Each segment represents an on-disk index structure (kdtree/terms/postings) flushed by memory limit or token boundaries,
 * or max segment rowId limit, because of lucene's limitation on 2B(Integer.MAX_VALUE). It also helps to reduce resource
 * consumption for read requests as only segments that intersect with read request data range need to be loaded.
 */
public class Segment implements Closeable, SegmentOrdering
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Segment.class);

    private final Token.KeyBound minKeyBound;
    private final Token.KeyBound maxKeyBound;

    // per sstable
    final PrimaryKeyMap.Factory primaryKeyMapFactory;
    // per-index
    public final PerIndexFiles indexFiles;
    // per-segment
    public final SegmentMetadata metadata;

    private final IndexSearcher index;

    public Segment(IndexContext indexContext, SSTableContext sstableContext, PerIndexFiles indexFiles, SegmentMetadata metadata) throws IOException
    {
        this.minKeyBound = metadata.minKey.token().minKeyBound();
        this.maxKeyBound = metadata.maxKey.token().maxKeyBound();

        this.primaryKeyMapFactory = sstableContext.primaryKeyMapFactory();
        this.indexFiles = indexFiles;
        this.metadata = metadata;

        var version = sstableContext.indexDescriptor.version;
        // FIXME we only have one IndexDescriptor + Version per sstable, so this is a hack
        // to support indexes at different versions.  Vectors are the only types impacted by multiple versions so far.
        IndexSearcher searcher;
        try
        {
            searcher = version.onDiskFormat().newIndexSearcher(sstableContext, indexContext, indexFiles, metadata);
        }
        catch (Throwable e) // there's multiple things that can go wrong w/ version mismatch, so catch all of them
        {
            if (!List.of(Version.BA, Version.CA).contains(version))
            {
                // we're only trying to recover from BA/CA confusion, this is something else
                throw e;
            }
            // opening with the global format didn't work.  that means that (unless it's actually corrupt)
            // the correct version is whichever one the global format is not set to
            version = version == Version.CA ? Version.BA : Version.CA;
            searcher = version.onDiskFormat().newIndexSearcher(sstableContext, indexContext, indexFiles, metadata);
        }
        logger.info("Opened searcher {} for segment {} at version {}",
                    searcher.getClass().getName(), sstableContext.descriptor(), version);
        this.index = searcher;
    }

    @VisibleForTesting
    public Segment(PrimaryKeyMap.Factory primaryKeyMapFactory,
                   PerIndexFiles indexFiles,
                   SegmentMetadata metadata,
                   AbstractType<?> columnType)
    {
        this.primaryKeyMapFactory = primaryKeyMapFactory;
        this.indexFiles = indexFiles;
        this.metadata = metadata;
        this.minKeyBound = null;
        this.maxKeyBound = null;
        this.index = null;
    }

    @VisibleForTesting
    public Segment(Token minKey, Token maxKey)
    {
        this.primaryKeyMapFactory = null;
        this.indexFiles = null;
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
     * @param keyRange   key range specific in read command, used by ANN index
     * @param context    to track per sstable cache and per query metrics
     * @param defer      create the iterator in a deferred state
     * @param limit      the num of rows to returned, used by ANN index
     * @return range iterator of {@link PrimaryKey} that matches given expression
     */
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        return index.search(expression, keyRange, context, defer, limit);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Segment segment = (Segment) o;
        return Objects.equal(metadata, segment.metadata);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(metadata);
    }

    @Override
    public RangeIterator limitToTopResults(QueryContext context, List<PrimaryKey> keys, Expression exp, int limit) throws IOException
    {
        return index.limitToTopResults(context, keys, exp, limit);
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
