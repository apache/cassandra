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
import java.util.PriorityQueue;

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
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Executes ann search against the HNSW graph for an individual index segment.
 */
public class VectorIndexSearcher extends IndexSegmentSearcher
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

        String field = indexContext.getIndexName();

        ByteBuffer buffer = exp.lower.value.raw;
        float[] queryVector = (float[])indexContext.getValidator().getSerializer().deserialize(buffer.duplicate());

        return toIterator(new BatchPostingList(field, queryVector, limit), context);
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

    private class BatchPostingList implements PostingList
    {
        private final float[] queryVector;
        private final PriorityQueue<Long> queue;

        private int limit;
        private BitSet bitset;

        BatchPostingList(String field, float[] queryVector, int limit)
        {
            this.queryVector = queryVector;
            this.limit = limit;
            this.queue = new PriorityQueue<>();
        }

        @Override
        public long nextPosting() throws IOException
        {
            return computeNextPosting();
        }

        @Override
        public long size()
        {
            // TODO Figure out what this should be
            return limit;
        }

        @Override
        public long advance(long targetRowID) throws IOException
        {
            long rowId = computeNextPosting();
            while (rowId < targetRowID)
                rowId = computeNextPosting();

            return rowId;
        }

        private long computeNextPosting() throws IOException
        {
            if (queue.isEmpty())
            {
                readBatch();
                if (queue.isEmpty())
                    return PostingList.END_OF_STREAM;
            }

            return queue.poll();
        }

        private void readBatch() throws IOException
        {
            var results = graph.search(queryVector, limit, new InvertedBits(bitset), Integer.MAX_VALUE);
            if (bitset == null)
                bitset = new SparseFixedBitSet(graph.size());
            while (results.hasNext())
            {
                var r = results.next();
                bitset.set(r.vectorOrdinal);
                for (var rowId : r.rowIds) {
                    queue.offer((long) rowId);
                }
            }
        }
    }

    private static class InvertedBits implements Bits
    {
        private final Bits wrapped;

        InvertedBits(Bits wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public boolean get(int i)
        {
            return wrapped == null ? true : !wrapped.get(i);
        }

        @Override
        public int length()
        {
            return wrapped == null ? 0 : wrapped.length();
        }
    }
}
