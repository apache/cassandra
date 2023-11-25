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

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeReader;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.lucene.index.CorruptIndexException;

import static org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeQueries.balancedTreeQueryFrom;

/**
 * Executes {@link Expression}s against the balanced tree for an individual index segment.
 */
public class NumericIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BlockBalancedTreeReader treeReader;
    private final QueryEventListener.BalancedTreeEventListener perColumnEventListener;

    NumericIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                PerColumnIndexFiles perIndexFiles,
                                SegmentMetadata segmentMetadata,
                                StorageAttachedIndex index) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, index);

        final long treePosition = metadata.getIndexRoot(IndexComponent.BALANCED_TREE);
        if (treePosition < 0)
            throw new CorruptIndexException(index.identifier().logMessage("The tree position is less than zero."), IndexComponent.BALANCED_TREE.name);
        final long postingsPosition = metadata.getIndexRoot(IndexComponent.POSTING_LISTS);
        if (postingsPosition < 0)
            throw new CorruptIndexException(index.identifier().logMessage("The postings position is less than zero."), IndexComponent.BALANCED_TREE.name);

        treeReader = new BlockBalancedTreeReader(index.identifier(),
                                                 indexFiles.balancedTree(),
                                                 treePosition,
                                                 indexFiles.postingLists(),
                                                 postingsPosition);
        perColumnEventListener = (QueryEventListener.BalancedTreeEventListener)index.columnQueryMetrics();
    }

    @Override
    public long indexFileCacheSize()
    {
        return treeReader.memoryUsage();
    }

    @Override
    public KeyRangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(index.identifier().logMessage("Searching on expression '{}'..."), exp);

        if (exp.getIndexOperator().isEqualityOrRange())
        {
            final BlockBalancedTreeReader.IntersectVisitor query = balancedTreeQueryFrom(exp, treeReader.getBytesPerValue());
            QueryEventListener.BalancedTreeEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            return toPrimaryKeyIterator(treeReader.intersect(query, listener, context), context);
        }
        else
        {
            throw new IllegalArgumentException(index.identifier().logMessage("Unsupported expression during index query: " + exp));
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("index", index)
                          .add("count", treeReader.getPointCount())
                          .add("bytesPerValue", treeReader.getBytesPerValue())
                          .toString();
    }

    @Override
    public void close()
    {
        treeReader.close();
    }
}
