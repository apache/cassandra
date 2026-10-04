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
import java.nio.ByteBuffer;
import java.util.Map;

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
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.utils.ByteBufferUtil;
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
                                StorageAttachedIndex index) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, index);

        long root = metadata.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        perColumnEventListener = (QueryEventListener.TrieIndexEventListener)index.columnQueryMetrics();

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);
        boolean isV2 = LiteralIndexWriter.POSTINGS_FORMAT_V2.equals(map.get(LiteralIndexWriter.POSTINGS_FORMAT));

        reader = new LiteralIndexSegmentTermsReader(index.identifier(), indexFiles.termsData(), indexFiles.postingLists(), root, footerPointer, isV2);
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        return 0;
    }

    @Override
    public KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(index.identifier().logMessage("Searching on expression '{}'..."), expression);

        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(queryContext, perColumnEventListener);

        if (expression.getIndexOperator() == Expression.IndexOperator.LIKE_PREFIX)
        {
            ByteBuffer prefixValue = expression.lower().value.encoded;
            ByteComparable start = v -> index.termType().asComparableBytes(prefixValue, v);
            ByteBuffer successor = prefixSuccessor(prefixValue);
            ByteComparable end = successor == null ? null : v -> index.termType().asComparableBytes(successor, v);
            return toPrimaryKeyIterator(reader.prefixMatch(start, end, listener, queryContext), queryContext);
        }

        if (!expression.getIndexOperator().isEquality())
            throw new IllegalArgumentException(index.identifier().logMessage("Unsupported expression: " + expression));

        ByteComparable term = v -> index.termType().asComparableBytes(expression.lower().value.encoded, v);
        return toPrimaryKeyIterator(reader.exactMatch(term, listener, queryContext), queryContext);
    }

    /**
     * Computes the lexicographic successor of the given raw prefix bytes: the byte array with its last non-{@code 0xFF}
     * byte incremented and any trailing {@code 0xFF} bytes removed. Returns null (an unbounded upper bound) when every
     * byte is {@code 0xFF}.
     */
    private static ByteBuffer prefixSuccessor(ByteBuffer prefix)
    {
        byte[] bytes = ByteBufferUtil.getArray(prefix);
        int last = bytes.length - 1;
        while (last >= 0 && (bytes[last] & 0xFF) == 0xFF)
            last--;

        if (last < 0)
            return null;

        byte[] successor = java.util.Arrays.copyOf(bytes, last + 1);
        successor[last]++;
        return ByteBuffer.wrap(successor);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("index", index).toString();
    }

    @Override
    public void close()
    {
        reader.close();
    }
}
