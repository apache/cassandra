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
import java.util.Iterator;

import com.google.common.base.Preconditions;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
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
        ordinalsMap = new OnDiskOrdinalsMap(descriptor.fileFor(IndexComponent.POSTING_LISTS, context), vectorValues.dimension());
        hnsw = new OnDiskHnswGraph(descriptor.fileFor(IndexComponent.TERMS_DATA, context));
    }

    public long ramBytesUsed()
    {
        return 0; // FIXME
    }

    public int size()
    {
        return vectorValues.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    public Iterator<AnnResultRowId> search(float[] queryVector, int topK, Bits acceptBits, int vistLimit)
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
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return new Iterator<>()
        {
            int remaining = queue.size();

            @Override
            public boolean hasNext()
            {
                return remaining > 0;
            }

            @Override
            public AnnResultRowId next()
            {
                remaining--;
                int ordinal = queue.pop();
                try
                {
                    return new AnnResultRowId(ordinal, ordinalsMap.getRowIdsMatching(ordinal));
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public void close()
    {
        vectorValues.close();
        ordinalsMap.close();
        hnsw.close();
    }

    private static class OnDiskOrdinalsMap
    {
        private final RandomAccessReader reader;
        private final int dimension;
        private final int size;

        public OnDiskOrdinalsMap(File file, int dimension) throws IOException
        {
            this.reader = RandomAccessReader.open(file);
            this.size = reader.readInt();
            this.dimension = dimension;
        }

        // TODO in the degenerate case it is not a good idea to read all the ordinals into memory
        public int[] getRowIdsMatching(int vectorOrdinal) throws IOException
        {
            Preconditions.checkArgument(vectorOrdinal < size, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, size);

            reader.seek(4L + vectorOrdinal * 8L);
            long offset = reader.readLong();
            reader.seek(offset);
            int postingsSize = reader.readInt();
            int[] ordinals = new int[postingsSize];
            for (int i = 0; i < ordinals.length; i++)
            {
                ordinals[i] = reader.readInt();
            }
            return ordinals;
        }

        public void close()
        {
            reader.close();
        }
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
            return new OnDiskVectors(new File(reader.getPath()));
        }

        public void close()
        {
            reader.close();
        }
    }

    public static class AnnResultRowId
    {
        public final int vectorOrdinal;
        public final int[] rowIds;

        public AnnResultRowId(int vectorOrdinal, int[] rowIds)
        {
            this.vectorOrdinal = vectorOrdinal;
            this.rowIds = rowIds;
        }
    }
}
