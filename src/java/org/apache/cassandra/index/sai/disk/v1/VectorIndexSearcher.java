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

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.SegementOrdering;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Executes ann search against the HNSW graph for an individual index segment.
 */
public class VectorIndexSearcher extends IndexSegmentSearcher implements SegementOrdering
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CassandraOnDiskHnsw graph;

    public VectorIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                               PerColumnIndexFiles perIndexFiles, // TODO not used for now because lucene has different file extensions
                               SegmentMetadata segmentMetadata,
                               IndexDescriptor indexDescriptor,
                               IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexContext);
        graph = new CassandraOnDiskHnsw(indexDescriptor, indexContext);
    }

    @Override
    public long indexFileCacheSize()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public KeyRangeIterator search(Expression exp, QueryContext context, int limit) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp() != Expression.IndexOperator.ANN)
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during ANN index query: " + exp));

        ByteBuffer buffer = exp.lower.value.raw;
        float[] queryVector = (float[])indexContext.getValidator().getSerializer().deserialize(buffer.duplicate());
        var results = graph.search(queryVector, limit, null, Integer.MAX_VALUE);
        return toIterator(results, context);
    }

    @Override
    public KeyRangeIterator reorderOneComponent(QueryContext context, KeyRangeIterator iterator, Expression exp, int limit) throws IOException
    {
        // materialize the underlying iterator as a bitset, then ask hnsw to search.
        // the iterator represents keys from the same sstable segment as us,
        // so we can use row ids to order the results by vector similarity
        SparseFixedBitSet bits;
        try (var mapper = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap())
        {
            bits = new SparseFixedBitSet(1 + metadata.segmentedRowId(metadata.maxSSTableRowId));
            while (iterator.hasNext())
            {
                var pk = iterator.next();
                var segmentRowId = metadata.segmentedRowId(mapper.rowIdFromPrimaryKey(pk));
                var ordinal = graph.getOrdinal(segmentRowId);
                bits.set(ordinal);
            }
        }
        ByteBuffer buffer = exp.lower.value.raw;
        float[] queryVector = (float[])indexContext.getValidator().getSerializer().deserialize(buffer.duplicate());
        var results = graph.search(queryVector, limit, bits, Integer.MAX_VALUE);
        return toIterator(results, context);
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
    }
}
