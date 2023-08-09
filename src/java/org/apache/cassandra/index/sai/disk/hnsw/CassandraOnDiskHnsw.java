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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.stream.IntStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class CassandraOnDiskHnsw implements AutoCloseable
{
    private final int vectorDimension;
    private final Function<QueryContext, OnDiskVectors> vectorsSupplier;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final OnDiskHnswGraph hnsw;
    private final VectorSimilarityFunction similarityFunction;
    private final VectorCache vectorCache;

    private static final int OFFSET_CACHE_MIN_BYTES = 100_000;

    public CassandraOnDiskHnsw(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        long vectorsSegmentOffset = componentMetadatas.get(IndexComponent.VECTOR).offset;
        vectorsSupplier = (qc) -> new OnDiskVectors(indexFiles.vectors(), vectorsSegmentOffset, qc);

        SegmentMetadata.ComponentMetadata postingListsMetadata = componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
        hnsw = new OnDiskHnswGraph(indexFiles.termsData(), termsMetadata.offset, termsMetadata.length, OFFSET_CACHE_MIN_BYTES);
        var mockContext = new QueryContext();
        try (var vectors = vectorsSupplier.apply(mockContext))
        {
            vectorDimension = vectors.dimension();
            vectorCache = VectorCache.load(hnsw.getView(mockContext), vectors, CassandraRelevantProperties.SAI_HNSW_VECTOR_CACHE_BYTES.getInt());
        }
    }

    public long ramBytesUsed()
    {
        return hnsw.getCacheSizeInBytes() + vectorCache.ramBytesUsed();
    }

    public int size()
    {
        return hnsw.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    // VSTODO make this return something with a size
    public ReorderingPostingList search(float[] queryVector, int topK, Bits acceptBits, int vistLimit, QueryContext context)
    {
        CassandraOnHeapHnsw.validateIndexable(queryVector, similarityFunction);

        NeighborQueue queue;
        try (var vectors = vectorsSupplier.apply(context); var view = hnsw.getView(context))
        {
            queue = HnswGraphSearcher.search(queryVector,
                                             topK,
                                             vectors,
                                             VectorEncoding.FLOAT32,
                                             similarityFunction,
                                             view,
                                             ordinalsMap.ignoringDeleted(acceptBits),
                                             vistLimit);
            return annRowIdsToPostings(queue);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private class RowIdIterator implements PrimitiveIterator.OfInt, AutoCloseable
    {
        private final NeighborQueue queue;
        private final OnDiskOrdinalsMap.RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

        private PrimitiveIterator.OfInt segmentRowIdIterator = IntStream.empty().iterator();

        public RowIdIterator(NeighborQueue queue)
        {
            this.queue = queue;
        }

        @Override
        public boolean hasNext() {
            while (!segmentRowIdIterator.hasNext() && queue.size() > 0) {
                try
                {
                    var ordinal = queue.pop();
                    segmentRowIdIterator = Arrays.stream(rowIdsView.getSegmentRowIdsMatching(ordinal)).iterator();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return segmentRowIdIterator.hasNext();
        }

        @Override
        public int nextInt() {
            if (!hasNext())
                throw new NoSuchElementException();
            return segmentRowIdIterator.nextInt();
        }

        @Override
        public void close() throws IOException
        {
            rowIdsView.close();
        }
    }

    private ReorderingPostingList annRowIdsToPostings(NeighborQueue queue) throws IOException
    {
        int originalSize = queue.size();
        try (var iterator = new RowIdIterator(queue))
        {
            return new ReorderingPostingList(iterator, originalSize);
        }
    }

    public void close()
    {
        ordinalsMap.close();
        hnsw.close();
    }

    public OnDiskOrdinalsMap.OrdinalsView getOrdinalsView() throws IOException
    {
        return ordinalsMap.getOrdinalsView();
    }

    @NotThreadSafe
    class OnDiskVectors implements RandomAccessVectorValues<float[]>, AutoCloseable
    {
        private final RandomAccessReader reader;
        private final long segmentOffset;
        private final int dimension;
        private final int size;
        private final float[] vector;
        private final QueryContext queryContext;

        public OnDiskVectors(FileHandle fh, long segmentOffset, QueryContext queryContext)
        {
            this.queryContext = queryContext;
            try
            {
                this.reader = fh.createReader();
                reader.seek(segmentOffset);
                this.segmentOffset = segmentOffset;

                this.size = reader.readInt();
                this.dimension = reader.readInt();
                this.vector = new float[dimension];
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error initializing OnDiskVectors at segment offset" + segmentOffset, e);
            }
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public int dimension()
        {
            return dimension;
        }

        @Override
        public float[] vectorValue(int i) throws IOException
        {
            queryContext.hnswVectorsAccessed++;
            var cached = vectorCache.get(i);
            if (cached != null)
            {
                queryContext.hnswVectorCacheHits++;
                return cached;
            }

            readVector(i, vector);
            return vector;
        }

        void readVector(int i, float[] v) throws IOException
        {
            reader.readFloatsAt(segmentOffset + 8L + i * dimension * 4L, v);
        }

        @Override
        public RandomAccessVectorValues<float[]> copy()
        {
            // this is only necessary if we need to build a new graph from this vector source.
            // (the idea is that if you are re-using float[] between calls, like we are here,
            //  you can make a copy of the source so you can compare different neighbors' scores
            //  as you build the graph.)
            // since we only build new graphs during insert and compaction, when we get the vectors from the source rows,
            // we don't need to worry about this.
            throw new UnsupportedOperationException();
        }

        public void close()
        {
            reader.close();
        }
    }
}
