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
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReader;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;

import static org.apache.cassandra.index.sai.disk.v1.kdtree.BKDQueries.bkdQueryFrom;

/**
 * Executes {@link Expression}s against the kd-tree for an individual index segment.
 */
public class NumericIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BKDReader bkdReader;
    private final QueryEventListener.BKDIndexEventListener perColumnEventListener;

    NumericIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                PerColumnIndexFiles perIndexFiles,
                                SegmentMetadata segmentMetadata,
                                IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexContext);

        final long bkdPosition = metadata.getIndexRoot(IndexComponent.KD_TREE);
        assert bkdPosition >= 0;
        final long postingsPosition = metadata.getIndexRoot(IndexComponent.POSTING_LISTS);
        assert postingsPosition >= 0;

        bkdReader = new BKDReader(indexContext,
                                  indexFiles.kdtree(),
                                  bkdPosition,
                                  indexFiles.postingLists(),
                                  postingsPosition);
        perColumnEventListener = (QueryEventListener.BKDIndexEventListener)indexContext.getColumnQueryMetrics();
    }

    @Override
    public long indexFileCacheSize()
    {
        return bkdReader.memoryUsage();
    }

    @Override
    @SuppressWarnings({"resource", "RedundantSuppression"})
    public KeyRangeIterator search(Expression exp, QueryContext context) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp().isEqualityOrRange())
        {
            final BKDReader.IntersectVisitor query = bkdQueryFrom(exp, bkdReader.getBytesPerValue());
            QueryEventListener.BKDIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            PostingList postingList = bkdReader.intersect(query, listener, context);
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
                          .add("count", bkdReader.getPointCount())
                          .add("bytesPerValue", bkdReader.getBytesPerValue())
                          .toString();
    }

    @Override
    public void close()
    {
        bkdReader.close();
    }
}
