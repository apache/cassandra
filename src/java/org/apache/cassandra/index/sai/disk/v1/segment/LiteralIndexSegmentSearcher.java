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
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class LiteralIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(LiteralIndexSegmentSearcher.class);

    private final LiteralIndexSegmentTermsReader reader;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;

    LiteralIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                PerColumnIndexFiles perIndexFiles,
                                SegmentMetadata segmentMetadata,
                                IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexContext);

        long root = metadata.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        perColumnEventListener = (QueryEventListener.TrieIndexEventListener)indexContext.getColumnQueryMetrics();

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        reader = new LiteralIndexSegmentTermsReader(indexContext,
                                                    indexFiles.termsData(),
                                                    indexFiles.postingLists(),
                                                    root, footerPointer);
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        return 0;
    }

    @Override
    @SuppressWarnings({"resource", "RedundantSuppression"})
    public KeyRangeIterator search(Expression expression, QueryContext queryContext) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), expression);

        if (!expression.getOp().isEquality())
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + expression));

        final ByteComparable term = ByteComparable.fixedLength(expression.lower.value.encoded);
        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(queryContext, perColumnEventListener);
        PostingList postingList = reader.exactMatch(term, listener, queryContext);
        return toIterator(postingList, queryContext);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close()
    {
        reader.close();
    }
}
