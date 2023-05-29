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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
        // We need to be able to inexpensively distinguish different vectors, with a slower path
        // that identifies vectors that are equal but not the same reference.  A comparison-
        // based Map (which only needs to look at vector elements until a difference is found)
        // is thus a better option than hash-based (which has to look at all elements to compute the hash).
        postingsMap = new ConcurrentSkipListMap<>(Arrays::compare);

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

        var vector = serializer.deserializeFloatArray(term, ByteBufferAccessor.instance);
        var bytesUsed = new AtomicLong(VectorPostings.bytesPerPosting()); // the new posting
        var postings = postingsMap.computeIfAbsent(vector, v -> {
            var bytes = RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
            var ordinal = nextOrdinal.getAndIncrement();
            bytes += vectorValues.add(ordinal, vector);
            try
            {
                bytes += builder.addGraphNode(ordinal, vectorValues);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            bytes += VectorPostings.emptyBytesUsed();
            bytesUsed.addAndGet(bytes);
            return new VectorPostings<>(ordinal);
        });
        postings.append(key);

        return bytesUsed.get();
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
        return postingsMap.values().stream().mapToLong(VectorPostings::ramBytesUsed).sum();
    }

    private long exactRamBytesUsed()
    {
        return ObjectSizes.measureDeep(this);
    }
}
