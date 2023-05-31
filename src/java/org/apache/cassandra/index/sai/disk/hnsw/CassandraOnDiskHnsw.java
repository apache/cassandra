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
import java.util.stream.IntStream;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class CassandraOnDiskHnsw
{
    private final OnDiskVectors vectorValues;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final OnDiskHnswGraph hnsw;
    private final VectorSimilarityFunction similarityFunction;

    public CassandraOnDiskHnsw(IndexDescriptor descriptor, IndexContext context) throws IOException
    {
        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();
        vectorValues = new OnDiskVectors(descriptor.fileFor(IndexComponent.VECTOR, context));
        ordinalsMap = new OnDiskOrdinalsMap(descriptor.fileFor(IndexComponent.POSTING_LISTS, context));
        hnsw = new OnDiskHnswGraph(descriptor.fileFor(IndexComponent.TERMS_DATA, context),
                                   CassandraRelevantProperties.SAI_VECTOR_SEARCH_HNSW_CACHE_BYTES.getInt());
    }

    public long ramBytesUsed()
    {
        return hnsw.getCacheSizeInBytes();
    }

    public int size()
    {
        return vectorValues.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    // TODO make this return something with a size
    public ReorderingPostingList search(float[] queryVector, int topK, Bits acceptBits, int vistLimit)
    {
        NeighborQueue queue;
        try
        {
            queue = HnswGraphSearcher.search(queryVector,
                                             topK,
                                             vectorValues,
                                             VectorEncoding.FLOAT32,
                                             similarityFunction,
                                             hnsw,
                                             acceptBits,
                                             vistLimit);
            return annRowIdsToPostings(queue);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private ReorderingPostingList annRowIdsToPostings(NeighborQueue queue) throws IOException
    {
        int originalSize = queue.size();
        PrimitiveIterator.OfInt iterator = new PrimitiveIterator.OfInt() {
            private PrimitiveIterator.OfInt segmentRowIdIterator = IntStream.empty().iterator();

            @Override
            public boolean hasNext() {
                while (!segmentRowIdIterator.hasNext() && queue.size() > 0) {
                    try
                    {
                        segmentRowIdIterator = Arrays.stream(ordinalsMap.getSegmentRowIdsMatching(queue.pop())).iterator();
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
        };

        return new ReorderingPostingList(iterator, originalSize);
    }

    public void close()
    {
        vectorValues.close();
        ordinalsMap.close();
        hnsw.close();
    }

    public int getOrdinal(int segmentRowId) throws IOException
    {
        return ordinalsMap.getOrdinalForRowId(segmentRowId);
    }

    private static class OnDiskVectors implements RandomAccessVectorValues<float[]>
    {
        private final RandomAccessReader reader;
        private final int dimension;
        private final int size;

        public OnDiskVectors(File file) throws IOException
        {
            this.reader = RandomAccessReader.open(file);
            this.size = reader.readInt();
            this.dimension = reader.readInt();
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
            reader.seek(8L + i * dimension * 4L);
            float[] vector = new float[dimension];
            for (int j = 0; j < dimension; j++)
            {
                vector[j] = reader.readFloat();
            }
            return vector;
        }

        @Override
        public RandomAccessVectorValues<float[]> copy() throws IOException
        {
            return new OnDiskVectors(reader.getFile());
        }

        public void close()
        {
            reader.close();
        }
    }
}
