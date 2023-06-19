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
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(CassandraOnHeapHnsw.class);

    private final RamAwareVectorValues vectorValues;
    private final CassandraHnswGraphBuilder<float[]> builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final Map<float[], VectorPostings<T>> postingsMap;
    private final NonBlockingHashMapLong<VectorPostings<T>> postingsByOrdinal;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private volatile boolean hasDeletions;

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
        postingsByOrdinal = new NonBlockingHashMapLong<>();

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
        if (behavior == InvalidVectorBehavior.IGNORE)
        {
            try
            {
                validateIndexable(vector, similarityFunction);
            }
            catch (InvalidRequestException e)
            {
                logger.trace("Ignoring invalid vector during index build against existing data: {}", e);
                return 0;
            }
        }
        else
        {
            assert behavior == InvalidVectorBehavior.FAIL;
            validateIndexable(vector, similarityFunction);
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
            var vp = new VectorPostings<T>(ordinal);
            postingsByOrdinal.put(ordinal, vp);
            return vp;
        });
        if (postings.add(key))
        {
            bytesUsed.addAndGet(VectorPostings.bytesPerPosting());
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

    // copied out of a Lucene PR -- hopefully committed soon
    public static final float MAX_FLOAT32_COMPONENT = 1E17f;
    public static float[] checkInBounds(float[] v) {
        for (int i = 0; i < v.length; i++) {
            if (!Float.isFinite(v[i])) {
                throw new IllegalArgumentException("non-finite value at vector[" + i + "]=" + v[i]);
            }

            if (Math.abs(v[i]) > MAX_FLOAT32_COMPONENT) {
                throw new IllegalArgumentException("Out-of-bounds value at vector[" + i + "]=" + v[i]);
            }
        }
        return v;
    }

    static void validateIndexable(float[] vector, VectorSimilarityFunction similarityFunction)
    {
        try
        {
            checkInBounds(vector);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        if (similarityFunction == VectorSimilarityFunction.COSINE)
        {
            for (int i = 0; i < vector.length; i++)
            {
                if (vector[i] != 0)
                    return;
            }
            throw new InvalidRequestException("Zero vectors cannot be indexed or queried with cosine similarity");
        }
    }

    public Collection<T> keysFromOrdinal(int node)
    {
        return postingsByOrdinal.get(node).getPostings();
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

        hasDeletions = true;
        postings.remove(key);
    }

    /**
     * @return keys (PrimaryKey or segment row id) associated with the topK vectors near the query
     */
    public PriorityQueue<T> search(float[] queryVector, int limit, Bits toAccept, int visitedLimit)
    {
        assert builder.isConcurrent();
        validateIndexable(queryVector, similarityFunction);

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
                                                       hasDeletions ? BitsUtil.bitsIgnoringDeleted(toAccept, postingsByOrdinal) : toAccept,
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
        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert nextOrdinal.get() == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                              nextOrdinal.get(), builder.getGraph().size());
        assert vectorValues.size() == builder.getGraph().size() : String.format("vector count %d != graph size %d",
                                                                                vectorValues.size(), builder.getGraph().size());
        assert postingsMap.keySet().size() == vectorValues.size() : String.format("postings map entry count %d != vector count %d",
                                                                                  postingsMap.keySet().size(), vectorValues.size());
        logger.debug("Writing graph with {} rows and {} distinct vectors", postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), vectorValues.size());

        try (var vectorsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.VECTOR, indexContext), true);
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext), true);
             var indexOutputWriter = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.TERMS_DATA, indexContext), true))
        {
            // write vectors
            long vectorOffset = vectorsOutput.getFilePointer();
            long vectorPosition = vectorValues.write(vectorsOutput.asSequentialWriter());
            long vectorLength = vectorPosition - vectorOffset;

            var deletedOrdinals = new HashSet<Integer>();
            postingsMap.values().stream().filter(VectorPostings::isEmpty).forEach(vectorPostings -> deletedOrdinals.add(vectorPostings.getOrdinal()));
            // remove ordinals that don't have corresponding row ids due to partition/range deletion
            for (VectorPostings<T> vectorPostings : postingsMap.values())
            {
                vectorPostings.computeRowIds(postingTransformer);
                if (vectorPostings.shouldAppendDeletedOrdinal())
                    deletedOrdinals.add(vectorPostings.getOrdinal());
            }
            // write postings
            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition = new VectorPostingsWriter<T>().writePostings(postingsOutput.asSequentialWriter(), vectorValues, postingsMap, deletedOrdinals);
            long postingsLength = postingsPosition - postingsOffset;

            // write the graph
            long termsOffset = indexOutputWriter.getFilePointer();
            long termsPosition = new HnswGraphWriter(builder.getGraph()).write(indexOutputWriter);
            long termsLength = termsPosition - termsOffset;

            // add components to the metadata map
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
