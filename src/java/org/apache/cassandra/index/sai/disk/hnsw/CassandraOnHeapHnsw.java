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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.ConcurrentHnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;

public class CassandraOnHeapHnsw<T>
{
    private final ConcurrentVectorValues vectorValues;
    private final ConcurrentHnswGraphBuilder<float[]> builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    final Map<float[], VectorPostings<T>> postingsMap;
    private final AtomicInteger nextOrdinal = new AtomicInteger();

    public CassandraOnHeapHnsw(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig)
    {
        serializer = (VectorType.VectorSerializer)termComparator.getSerializer();
        vectorValues = new ConcurrentVectorValues(((VectorType)termComparator).dimension);
        similarityFunction = indexWriterConfig.getSimilarityFunction();
        postingsMap = new ConcurrentHashMap<>();

        try
        {
            builder = ConcurrentHnswGraphBuilder.create(vectorValues,
                                                        VectorEncoding.FLOAT32,
                                                        similarityFunction,
                                                        indexWriterConfig.getMaximumNodeConnections(),
                                                        indexWriterConfig.getConstructionBeamWidth());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public int size()
    {
        return vectorValues.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public long add(ByteBuffer term, T key)
    {
        assert term != null && term.remaining() != 0;

        var initialBytesUsed = ramBytesUsed();

        var vector = serializer.deserializeFloatArray(term, ByteBufferAccessor.instance);
        var postings = postingsMap.computeIfAbsent(vector, v -> {
            var ordinal = nextOrdinal.getAndIncrement();
            vectorValues.add(ordinal, vector);
            try
            {
                builder.addGraphNode(ordinal, vectorValues);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return new VectorPostings<>(ordinal);
        });
        postings.append(key);

        // hnsw is too much of a black box for us to be able to estimate how many additional bytes are used other than this way
        return ramBytesUsed() - initialBytesUsed;
    }

    public Collection<T> keysFromOrdinal(int node)
    {
        return postingsMap.get(vectorValues.vectorValue(node)).postings;
    }

    /**
     * @return keys (PrimaryKey or segment row id) associated with the topK vectors near the query
     */
    public PriorityQueue<T> search(float[] queryVector, int limit, Bits toAccept, int visitedLimit)
    {
        // search() errors out when an empty graph is passed to it
        if (vectorValues.size() == 0)
            return new PriorityQueue<>();

        NeighborQueue queue;
        try
        {
            queue = HnswGraphSearcher.search(queryVector,
                                             limit,
                                             vectorValues,
                                             VectorEncoding.FLOAT32,
                                             similarityFunction,
                                             builder.getGraph().getView(),
                                             toAccept,
                                             visitedLimit);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        PriorityQueue<T> keyQueue = new PriorityQueue<>();
        while (queue.size() > 0)
        {
            keyQueue.addAll(keysFromOrdinal(queue.pop()));
        }
        return keyQueue;
    }

    public void writeData(IndexDescriptor indexDescriptor, IndexContext indexContext, Function<T, Integer> postingTransformer) throws IOException
    {
        try (var vectorsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.VECTOR, indexContext));
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext)))
        {
            vectorValues.write(vectorsOutput.asSequentialWriter());
            new VectorPostingsWriter<T>().writePostings(postingsOutput.asSequentialWriter(), vectorValues, postingsMap, postingTransformer);
            new HnswGraphWriter(new ExtendedConcurrentHnswGraph(builder.getGraph())).write(indexDescriptor.fileFor(IndexComponent.TERMS_DATA, indexContext));
        }
    }

    public long ramBytesUsed()
    {
        return postingsBytesUsed() + vectorValues.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    private long postingsBytesUsed()
    {
        // looping through vectorPostings entries is expensive, we assume there's one per entry
        return RamEstimation.concurrentHashMapRamUsed(postingsMap.size())
               + postingsMap.size() * VectorPostings.bytesPerPosting();
    }

    private long exactRamBytesUsed()
    {
        return ObjectSizes.measureDeep(this);
    }
}
