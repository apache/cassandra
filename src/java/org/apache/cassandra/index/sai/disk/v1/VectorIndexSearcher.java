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
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Executes ann search against the HNSW graph for an individual index segment.
 */
public class VectorIndexSearcher extends IndexSearcher implements SegmentOrdering
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CassandraOnDiskHnsw graph;
    private final PrimaryKey.Factory keyFactory;
    private final PrimaryKeyMap primaryKeyMap;

    VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                        PerIndexFiles perIndexFiles,
                        SegmentMetadata segmentMetadata,
                        IndexDescriptor indexDescriptor,
                        IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);
        graph = new CassandraOnDiskHnsw(indexDescriptor, indexContext);
        this.keyFactory = PrimaryKey.factory(indexContext.comparator(), indexContext.indexFeatureSet());
        this.primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    @SuppressWarnings("resource")
    public RangeIterator<PrimaryKey> search(Expression exp, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context, boolean defer, int limit) throws IOException
    {
        ReorderingPostingList results = searchPosting(context, exp, keyRange, limit);
        return toPrimaryKeyIterator(results, context);
    }

    @Override
    public RangeIterator<Long> searchSSTableRowIds(Expression exp, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context, boolean defer, int limit) throws IOException
    {
        ReorderingPostingList results = searchPosting(context, exp, keyRange, limit);
        return toSSTableRowIdsIterator(results, context);
    }

    private ReorderingPostingList searchPosting(SSTableQueryContext context, Expression exp, AbstractBounds<PartitionPosition> keyRange, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.Op.ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        Bits bits = bitsetForKeyRange(context, keyRange);

        ByteBuffer buffer = exp.lower.value.raw;
        float[] queryVector = TypeUtil.decomposeVector(indexContext, buffer.duplicate());
        return graph.search(queryVector, limit, bits, Integer.MAX_VALUE);
    }

    private Bits bitsetForKeyRange(SSTableQueryContext context, AbstractBounds<PartitionPosition> keyRange) throws IOException
    {
        // not restricted
        if (RangeUtil.coversFullRing(keyRange))
            return context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph::getOrdinal);

        PrimaryKey firstPrimaryKey = keyFactory.createTokenOnly(keyRange.left.getToken());
        PrimaryKey lastPrimaryKey = keyFactory.createTokenOnly(keyRange.right.getToken());

        // it will return the next row id if given key is not found.
        long minSSTableRowId = primaryKeyMap.firstRowIdFromPrimaryKey(firstPrimaryKey);
        long maxSSTableRowId = primaryKeyMap.lastRowIdFromPrimaryKey(lastPrimaryKey);

        // if it covers entire segment, skip bit set
        if (minSSTableRowId <= metadata.minSSTableRowId && maxSSTableRowId >= metadata.maxSSTableRowId)
            return context.bitsetForShadowedPrimaryKeys(metadata, primaryKeyMap, graph::getOrdinal);

        minSSTableRowId = Math.max(minSSTableRowId, metadata.minSSTableRowId);
        maxSSTableRowId = Math.min(maxSSTableRowId, metadata.maxSSTableRowId);

        SparseFixedBitSet bits = new SparseFixedBitSet(1 + metadata.segmentedRowId(metadata.maxSSTableRowId));
        for (long sstableRowId = minSSTableRowId; sstableRowId <= maxSSTableRowId; sstableRowId++)
        {
            try
            {
                int segmentRowId = metadata.segmentedRowId(sstableRowId);
                int ordinal = graph.getOrdinal(segmentRowId);
                if (ordinal >= 0)
                {
                    if (context.shouldInclude(sstableRowId, primaryKeyMap))
                        bits.set(ordinal);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        return bits;
    }

    @Override
    public RangeIterator<PrimaryKey> limitToTopResults(SSTableQueryContext context, RangeIterator<Long> iterator, Expression exp, int limit) throws IOException
    {
        // materialize the underlying iterator as a bitset, then ask hnsw to search.
        // the iterator represents keys from the same sstable segment as us,
        // so we can use row ids to order the results by vector similarity
        SparseFixedBitSet bits = new SparseFixedBitSet(1 + metadata.segmentedRowId(metadata.maxSSTableRowId));
        int maxBruteForceRows = Math.max(limit, (int)(indexContext.getIndexWriterConfig().getMaximumNodeConnections() * Math.log(graph.size())));
        int[] bruteForceRows = new int[maxBruteForceRows];
        int n = 0;
        while (iterator.hasNext())
        {
            Long sstableRowId = iterator.peek();
            // if sstable row id has exceeded current ANN segment, stop
            if (sstableRowId > metadata.maxSSTableRowId)
                break;

            iterator.next();

            int segmentRowId = metadata.segmentedRowId(sstableRowId);
            if (n < maxBruteForceRows)
                bruteForceRows[n] = segmentRowId;
            n++;

            int ordinal = graph.getOrdinal(segmentRowId);
            if (ordinal >= 0)
            {
                if (context.shouldInclude(sstableRowId, primaryKeyMap))
                    bits.set(ordinal);
            }
        }

        // if we have a small number of results then let TopK processor do exact NN computation
        if (n <= maxBruteForceRows)
        {
            var results = new ReorderingPostingList(Arrays.stream(bruteForceRows, 0, n).iterator(), n);
            return toPrimaryKeyIterator(results, context);
        }

        ByteBuffer buffer = exp.lower.value.raw;
        float[] queryVector = (float[])indexContext.getValidator().getSerializer().deserialize(buffer);
        var results = graph.search(queryVector, limit, bits, Integer.MAX_VALUE);
        return toPrimaryKeyIterator(results, context);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close() throws IOException
    {
        graph.close();
        primaryKeyMap.close();
    }
}
