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

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeReader;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;

import static org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeQueries.balancedTreeQueryFrom;

/**
 * Executes {@link Expression}s against the balanced tree for an individual index segment.
 */
public class NumericIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BlockBalancedTreeReader balancedTreeReader;
    private final QueryEventListener.BlockBalancedTreeEventListener perColumnEventListener;

    NumericIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                PerColumnIndexFiles perIndexFiles,
                                SegmentMetadata segmentMetadata,
                                IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexContext);

        final long balancedTreePosition = metadata.getIndexRoot(IndexComponent.BALANCED_TREE);
        assert balancedTreePosition >= 0;
        final long postingsPosition = metadata.getIndexRoot(IndexComponent.POSTING_LISTS);
        assert postingsPosition >= 0;

        balancedTreeReader = new BlockBalancedTreeReader(indexContext,
                                                         indexFiles.balancedTree(),
                                                         balancedTreePosition,
                                                         indexFiles.postingLists(),
                                                         postingsPosition);
        perColumnEventListener = (QueryEventListener.BlockBalancedTreeEventListener)indexContext.getColumnQueryMetrics();
    }

    @Override
    public long indexFileCacheSize()
    {
        return balancedTreeReader.memoryUsage();
    }

    @Override
    @SuppressWarnings({"resource", "RedundantSuppression"})
    public KeyRangeIterator search(Expression exp, QueryContext context) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp().isEqualityOrRange())
        {
            final BlockBalancedTreeReader.IntersectVisitor query = balancedTreeQueryFrom(exp, balancedTreeReader.getBytesPerValue());
            QueryEventListener.BlockBalancedTreeEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            PostingList postingList = balancedTreeReader.intersect(query, listener, context);
            return toIterator(postingList, context);
        }
        else
        {
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during index query: " + exp));
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .add("count", balancedTreeReader.getPointCount())
                          .add("bytesPerValue", balancedTreeReader.getBytesPerValue())
                          .toString();
    }

    @Override
    public void close()
    {
        balancedTreeReader.close();
    }
}
