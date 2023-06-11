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
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;

public class CassandraOnHeapHnsw<T>
{
    private final RamAwareVectorValues vectorValues;
    private final CassandraHnswGraphBuilder<float[]> builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    final Map<float[], VectorPostings<T>> postingsMap;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private final Set<Integer> deletedOrdinals = ConcurrentHashMap.newKeySet();

    /**
     * @param termComparator the vector type
     * @param indexWriterConfig
     *
     * Will create a concurrent object.
     */
    public CassandraOnHeapHnsw(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig)
    {
        this(termComparator, indexWriterConfig, true);
    }

    /**
     * @param termComparator the vector type
     * @param indexWriterConfig
     * @param concurrent should be true for memtables, false for compaction.  Concurrent allows us to search
     *                   while building the graph; non-concurrent allows us to avoid synchronization costs.
     */
    public CassandraOnHeapHnsw(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig, boolean concurrent)
    {
        serializer = (VectorType.VectorSerializer)termComparator.getSerializer();
        vectorValues = concurrent
                       ? new ConcurrentVectorValues(((VectorType) termComparator).dimension)
                       : new CompactionVectorValues(((VectorType<Float>) termComparator));
        similarityFunction = indexWriterConfig.getSimilarityFunction();
        // We need to be able to inexpensively distinguish different vectors, with a slower path
        // that identifies vectors that are equal but not the same reference.  A comparison-
        // based Map (which only needs to look at vector elements until a difference is found)
        // is thus a better option than hash-based (which has to look at all elements to compute the hash).
        postingsMap = new ConcurrentSkipListMap<>(Arrays::compare);

        builder = concurrent
                  ? new CassandraHnswGraphBuilder.ConcurrentBuilder<>(vectorValues,
                                                                      VectorEncoding.FLOAT32,
                                                                      similarityFunction,
                                                                      indexWriterConfig.getMaximumNodeConnections(),
                                                                      indexWriterConfig.getConstructionBeamWidth())
                  : new CassandraHnswGraphBuilder.SerialBuilder<>(vectorValues,
                                                                  VectorEncoding.FLOAT32,
                                                                  similarityFunction,
                                                                  indexWriterConfig.getMaximumNodeConnections(),
                                                                  indexWriterConfig.getConstructionBeamWidth());
    }

    public int size()
    {
        return vectorValues.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    /**
     * @return the incremental bytes ysed by adding the given vector to the index
     */
    public long add(ByteBuffer term, T key, InvalidVectorBehavior behavior)
    {
        assert term != null && term.remaining() != 0;

        var vector = serializer.deserializeFloatArray(term);
        var invalidMessage = validateIndexable(vector);
        if (invalidMessage != null)
        {
            switch (behavior)
            {
                case IGNORE:
                    return 0;
                case FAIL:
                    throw new InvalidRequestException(invalidMessage);
            }
        }

        var bytesUsed = new AtomicLong();
        var newVector = new AtomicBoolean();
        // if the vector is already in the graph, all that happens is that the postings list is updated
        // otherwise, we add the vector in this order:
        // 1. to the vectorValues
        // 2. to the postingsMap
        // 3. to the graph
        // This way, concurrent searches of the graph won't see the vector until it's visible
        // in the other structures as well.
        var postings = postingsMap.computeIfAbsent(vector, v -> {
            var bytes = RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
            var ordinal = nextOrdinal.getAndIncrement();
            bytes += (vectorValues instanceof ConcurrentVectorValues)
                     ? ((ConcurrentVectorValues) vectorValues).add(ordinal, vector)
                     : ((CompactionVectorValues) vectorValues).add(ordinal, term);
            bytes += VectorPostings.emptyBytesUsed();
            bytesUsed.addAndGet(bytes);
            newVector.set(true);
            return new VectorPostings<>(ordinal);
        });
        if (postings.add(key))
        {
            bytesUsed.addAndGet(VectorPostings.bytesPerPosting());
            deletedOrdinals.remove(postings.getOrdinal());
            if (newVector.get()) {
                try
                {
                    bytesUsed.addAndGet(builder.addGraphNode(postings.getOrdinal(), vectorValues));
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
        return bytesUsed.get();
    }

    private String validateIndexable(float[] vector)
    {
        for (int i = 0; i < vector.length; i++)
        {
            if (vector[i] != 0)
                return null;
        }

        return "Zero vectors cannot be indexed";
    }

    public Collection<T> keysFromOrdinal(int node)
    {
        return postingsMap.get(vectorValues.vectorValue(node)).getPostings();
    }

    public void remove(ByteBuffer term, T key)
    {
        assert term != null && term.remaining() != 0;

        var vector = serializer.deserializeFloatArray(term);
        var postings = postingsMap.get(vector);
        if (postings == null)
        {
            // it's possible for this to be called against a different memtable than the one
            // the value was originally added to, in which case we do not expect to find
            // the key among the postings for this vector
            return;
        }

        postings.remove(key);
        if (postings.isEmpty())
            deletedOrdinals.add(postings.getOrdinal());
    }

    /**
     * @return keys (PrimaryKey or segment row id) associated with the topK vectors near the query
     */
    public PriorityQueue<T> search(float[] queryVector, int limit, Bits toAccept, int visitedLimit)
    {
        assert builder.isConcurrent();

        // search() errors out when an empty graph is passed to it
        if (vectorValues.size() == 0)
            return new PriorityQueue<>();

        NeighborQueue queue;
        try
        {
            queue = HnswGraphSearcher.searchConcurrent(queryVector,
                                                       limit,
                                                       vectorValues,
                                                       VectorEncoding.FLOAT32,
                                                       similarityFunction,
                                                       builder.getGraph(),
                                                       BitsUtil.bitsIgnoringDeleted(toAccept, deletedOrdinals),
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

    public SegmentMetadata.ComponentMetadataMap writeData(IndexDescriptor indexDescriptor, IndexContext indexContext, Function<T, Integer> postingTransformer) throws IOException
    {
        try (var vectorsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.VECTOR, indexContext), true);
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext), true);
             var indexOutputWriter = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.TERMS_DATA, indexContext), true))
        {
            long vectorOffset = vectorsOutput.getFilePointer();
            long vectorPosition = vectorValues.write(vectorsOutput.asSequentialWriter());
            long vectorLength = vectorPosition - vectorOffset;

            // remove ordinals that don't have corresponding row ids due to partition/range deletion
            for (VectorPostings<T> vectorPostings : postingsMap.values())
            {
                vectorPostings.computeRowIds(postingTransformer);
                if (vectorPostings.shouldAppendDeletedOrdinal())
                    deletedOrdinals.add(vectorPostings.getOrdinal());
            }

            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition = new VectorPostingsWriter<T>().writePostings(postingsOutput.asSequentialWriter(), vectorValues, postingsMap, deletedOrdinals);
            long postingsLength = postingsPosition - postingsOffset;

            long termsOffset = indexOutputWriter.getFilePointer();
            long termsPosition = new HnswGraphWriter(builder.getGraph()).write(indexOutputWriter);
            long termsLength = termsPosition - termsOffset;

            SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();

            metadataMap.put(IndexComponent.TERMS_DATA, -1, termsOffset, termsLength, Map.of());
            metadataMap.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength, Map.of());

            Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(StringHelper.randomId())));
            metadataMap.put(IndexComponent.VECTOR, -1, vectorOffset, vectorLength, vectorConfigs);

            return metadataMap;
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

    public enum InvalidVectorBehavior
    {
        IGNORE,
        FAIL
    }
}
