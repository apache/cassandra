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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class InvertedIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final TermsReader reader;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;

    InvertedIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                          PerIndexFiles perIndexFiles,
                          SegmentMetadata segmentMetadata,
                          IndexDescriptor indexDescriptor,
                          IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);

        long root = metadata.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        perColumnEventListener = (QueryEventListener.TrieIndexEventListener)indexContext.getColumnQueryMetrics();

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        reader = new TermsReader(indexContext,
                                 indexFiles.termsData().sharedCopy(),
                                 indexFiles.postingLists().sharedCopy(),
                                 root, footerPointer);
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        // TODO: Is this still the case now the trie isn't using the chunk cache?
        return 0;
    }

    @SuppressWarnings("resource")
    public RangeIterator<Long> search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        PostingList postingList = searchPosting(exp, context);
        return toSSTableRowIdsIterator(postingList, context);
    }

    private PostingList searchPosting(Expression exp, QueryContext context)
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (!exp.getOp().isEquality())
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + exp));

        final ByteComparable term = ByteComparable.fixedLength(exp.lower.value.encoded);
        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
        return reader.exactMatch(term, listener, context);
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
