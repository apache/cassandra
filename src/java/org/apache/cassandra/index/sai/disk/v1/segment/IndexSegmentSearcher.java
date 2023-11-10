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

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingListRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PeekablePostingList;
import org.apache.cassandra.index.sai.postings.PostingList;

/**
 * Abstract reader for individual segments of an on-disk index.
 * <p>
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSegmentSearcher implements SegmentOrdering, Closeable
{
    final PrimaryKeyMap.Factory primaryKeyMapFactory;
    final PerColumnIndexFiles indexFiles;
    final SegmentMetadata metadata;
    final StorageAttachedIndex index;

    IndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                         PerColumnIndexFiles perIndexFiles,
                         SegmentMetadata segmentMetadata,
                         StorageAttachedIndex index)
    {
        this.primaryKeyMapFactory = primaryKeyMapFactory;
        this.indexFiles = perIndexFiles;
        this.metadata = segmentMetadata;
        this.index = index;
    }

    public static IndexSegmentSearcher open(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                            PerColumnIndexFiles indexFiles,
                                            SegmentMetadata segmentMetadata,
                                            StorageAttachedIndex index) throws IOException
    {
        if (index.termType().isVector())
            return new VectorIndexSegmentSearcher(primaryKeyMapFactory, indexFiles, segmentMetadata, index);
        else if (index.termType().isLiteral())
            return new LiteralIndexSegmentSearcher(primaryKeyMapFactory, indexFiles, segmentMetadata, index);
        else
            return new NumericIndexSegmentSearcher(primaryKeyMapFactory, indexFiles, segmentMetadata, index);
    }

    /**
     * @return memory usage of underlying on-disk data structure
     */
    public abstract long indexFileCacheSize();

    /**
     * Search on-disk index synchronously.
     *
     * @param expression to filter on disk index
     * @param queryContext to track per sstable cache and per query metrics
     *
     * @return {@link KeyRangeIterator} with matches for the given expression
     */
    public abstract KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext) throws IOException;

    KeyRangeIterator toPrimaryKeyIterator(PostingList postingList, QueryContext queryContext) throws IOException
    {
        if (postingList == null || postingList.size() == 0)
            return KeyRangeIterator.empty();

        IndexSegmentSearcherContext searcherContext = new IndexSegmentSearcherContext(metadata.minKey,
                                                                                      metadata.maxKey,
                                                                                      metadata.rowIdOffset,
                                                                                      queryContext,
                                                                                      PeekablePostingList.makePeekable(postingList));

        return new PostingListRangeIterator(index.identifier(), primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(), searcherContext);
    }
}
