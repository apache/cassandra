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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.util.StringHelper;

public class OnHeapGraph<T>
{
    private static final Logger logger = LoggerFactory.getLogger(OnHeapGraph.class);

    private final RamAwareVectorValues vectorValues;
    private final GraphIndexBuilder<float[]> builder;
    private final VectorType<?> vectorType;
    private final VectorSimilarityFunction similarityFunction;
    private final ConcurrentMap<float[], VectorPostings<T>> postingsMap;
    private final NonBlockingHashMapLong<VectorPostings<T>> postingsByOrdinal;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private volatile boolean hasDeletions;

    /**
     * @param termComparator the vector type
     * @param indexWriterConfig
     *
     * Will create a concurrent object.
     */
    public OnHeapGraph(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig)
    {
        this(termComparator, indexWriterConfig, true);
    }

    /**
     * @param termComparator the vector type
     * @param indexWriterConfig the {@link IndexWriterConfig} for the graph
     * @param concurrent should be true for memtables, false for compaction.  Concurrent allows us to search
     *                   while building the graph; non-concurrent allows us to avoid synchronization costs.
     */
    @SuppressWarnings("unchecked")
    public OnHeapGraph(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig, boolean concurrent)
    {
        this.vectorType = (VectorType<?>) termComparator;
        vectorValues = concurrent
                       ? new ConcurrentVectorValues(((VectorType<?>) termComparator).dimension)
                       : new CompactionVectorValues(((VectorType<Float>) termComparator));
        similarityFunction = indexWriterConfig.getSimilarityFunction();
        // We need to be able to inexpensively distinguish different vectors, with a slower path
        // that identifies vectors that are equal but not the same reference.  A comparison
        // based Map (which only needs to look at vector elements until a difference is found)
        // is thus a better option than hash-based (which has to look at all elements to compute the hash).
        postingsMap = new ConcurrentSkipListMap<>(Arrays::compare);
        postingsByOrdinal = new NonBlockingHashMapLong<>();

        builder = new GraphIndexBuilder<>(vectorValues,
                                          VectorEncoding.FLOAT32,
                                          similarityFunction,
                                          indexWriterConfig.getMaximumNodeConnections(),
                                          indexWriterConfig.getConstructionBeamWidth(),
                                          1.2f,
                                          1.4f);
    }

    public int size()
    {
        return vectorValues.size();
    }

    public boolean isEmpty()
    {
        return postingsMap.values().stream().allMatch(VectorPostings::isEmpty);
    }

    /**
     * @return the incremental bytes ysed by adding the given vector to the index
     */
    public long add(ByteBuffer term, T key, InvalidVectorBehavior behavior)
    {
        assert term != null && term.remaining() != 0;

        var vector = vectorType.composeAsFloat(term);
        if (behavior == InvalidVectorBehavior.IGNORE)
        {
            try
            {
                validateIndexable(vector, similarityFunction);
            }
            catch (InvalidRequestException e)
            {
                logger.trace("Ignoring invalid vector during index build against existing data: {}", vector, e);
                return 0;
            }
        }
        else
        {
            assert behavior == InvalidVectorBehavior.FAIL;
            validateIndexable(vector, similarityFunction);
        }

        var bytesUsed = 0L;
        VectorPostings<T> postings = postingsMap.get(vector);
        // if the vector is already in the graph, all that happens is that the postings list is updated
        // otherwise, we add the vector in this order:
        // 1. to the postingsMap
        // 2. to the vectorValues
        // 3. to the graph
        // This way, concurrent searches of the graph won't see the vector until it's visible
        // in the other structures as well.
        if (postings == null)
        {
            postings = new VectorPostings<>(key);
            // since we are using ConcurrentSkipListMap, it is NOT correct to use computeIfAbsent here
            if (postingsMap.putIfAbsent(vector, postings) == null)
            {
                // we won the race to add the new entry; assign it an ordinal and add to the other structures
                var ordinal = nextOrdinal.getAndIncrement();
                postings.setOrdinal(ordinal);
                bytesUsed += RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
                bytesUsed += (vectorValues instanceof ConcurrentVectorValues)
                             ? ((ConcurrentVectorValues) vectorValues).add(ordinal, vector)
                             : ((CompactionVectorValues) vectorValues).add(ordinal, term);
                bytesUsed += VectorPostings.emptyBytesUsed() + VectorPostings.bytesPerPosting();
                postingsByOrdinal.put(ordinal, postings);
                bytesUsed += builder.addGraphNode(ordinal, vectorValues);
                return bytesUsed;
            }
            else
            {
                postings = postingsMap.get(vector);
            }
        }
        // postings list already exists, just add the new key (if it's not already in the list)
        if (postings.add(key))
        {
            bytesUsed += VectorPostings.bytesPerPosting();
        }

        return bytesUsed;
    }

    // copied out of a Lucene PR -- hopefully committed soon
    public static final float MAX_FLOAT32_COMPONENT = 1E17f;

    public static void checkInBounds(float[] v)
    {
        for (int i = 0; i < v.length; i++)
        {
            if (!Float.isFinite(v[i]))
            {
                throw new IllegalArgumentException("non-finite value at vector[" + i + "]=" + v[i]);
            }

            if (Math.abs(v[i]) > MAX_FLOAT32_COMPONENT)
            {
                throw new IllegalArgumentException("Out-of-bounds value at vector[" + i + "]=" + v[i]);
            }
        }
    }

    public static void validateIndexable(float[] vector, VectorSimilarityFunction similarityFunction)
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

    public long remove(ByteBuffer term, T key)
    {
        assert term != null && term.remaining() != 0;

        var vector = vectorType.composeAsFloat(term);
        var postings = postingsMap.get(vector);
        if (postings == null)
        {
            // it's possible for this to be called against a different memtable than the one
            // the value was originally added to, in which case we do not expect to find
            // the key among the postings for this vector
            return 0;
        }

        hasDeletions = true;
        return postings.remove(key);
    }

    /**
     * @return keys (PrimaryKey or segment row id) associated with the topK vectors near the query
     */
    public PriorityQueue<T> search(float[] queryVector, int limit, Bits toAccept)
    {
        validateIndexable(queryVector, similarityFunction);

        // search() errors out when an empty graph is passed to it
        if (vectorValues.size() == 0)
            return new PriorityQueue<>();

        Bits bits = hasDeletions ? BitsUtil.bitsIgnoringDeleted(toAccept, postingsByOrdinal) : toAccept;
        GraphIndex<float[]> graph = builder.getGraph();
        var searcher = new GraphSearcher.Builder<>(graph.getView()).withConcurrentUpdates().build();
        NeighborSimilarity.ExactScoreFunction scoreFunction = node2 -> vectorCompareFunction(queryVector, node2);
        var result = searcher.search(scoreFunction, null, limit, bits);
        Tracing.trace("ANN search visited {} in-memory nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
        var a = result.getNodes();
        PriorityQueue<T> keyQueue = new PriorityQueue<>();
        for (int i = 0; i < a.length; i++)
            keyQueue.addAll(keysFromOrdinal(a[i].node));
        return keyQueue;
    }

    public SegmentMetadata.ComponentMetadataMap writeData(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier, Function<T, Integer> postingTransformer) throws IOException
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

        try (var pqOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.COMPRESSED_VECTORS, indexIdentifier), true);
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexIdentifier), true);
             var indexOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.TERMS_DATA, indexIdentifier), true))
        {
            SAICodecUtils.writeHeader(pqOutput);
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(indexOutput);

            // compute and write PQ
            long pqOffset = pqOutput.getFilePointer();
            long pqPosition = writePQ(pqOutput.asSequentialWriter());
            long pqLength = pqPosition - pqOffset;

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

            // complete (internal clean up) and write the graph
            builder.complete();
            long termsOffset = indexOutput.getFilePointer();
            OnDiskGraphIndex.write(builder.getGraph(), vectorValues, indexOutput.asSequentialWriter());
            long termsLength = indexOutput.getFilePointer() - termsOffset;

            // write footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);
            SAICodecUtils.writeFooter(indexOutput);

            // add components to the metadata map
            SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();
            metadataMap.put(IndexComponent.TERMS_DATA, -1, termsOffset, termsLength, Map.of());
            metadataMap.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength, Map.of());
            Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(StringHelper.randomId())));
            metadataMap.put(IndexComponent.COMPRESSED_VECTORS, -1, pqOffset, pqLength, vectorConfigs);
            return metadataMap;
        }
    }

    private float vectorCompareFunction(float[] queryVector, int node)
    {
        return similarityFunction.compare(queryVector, ((RandomAccessVectorValues<float[]>) vectorValues).vectorValue(node));
    }

    private long writePQ(SequentialWriter writer) throws IOException
    {
        // don't bother with PQ if there are fewer than 1K vectors
        int M = vectorValues.dimension() / 2;
        writer.writeBoolean(vectorValues.size() >= 1024);
        if (vectorValues.size() < 1024)
        {
            logger.debug("Skipping PQ for only {} vectors", vectorValues.size());
            return writer.position();
        }

        logger.debug("Computing PQ for {} vectors", vectorValues.size());
        // limit the PQ computation and encoding to one index at a time -- goal during flush is to
        // evict from memory ASAP so better to do the PQ build (in parallel) one at a time
        ProductQuantization pq;
        byte[][] encoded;
        synchronized (OnHeapGraph.class)
        {
            // train PQ and encode
            pq = ProductQuantization.compute(vectorValues, M, false);
            assert !vectorValues.isValueShared();
            encoded = IntStream.range(0, vectorValues.size())
                               .parallel()
                               .mapToObj(i -> pq.encode(vectorValues.vectorValue(i)))
                               .toArray(byte[][]::new);
        }
        var cv = new CompressedVectors(pq, encoded);
        // save
        cv.write(writer);
        return writer.position();
    }

    public enum InvalidVectorBehavior
    {
        IGNORE,
        FAIL
    }
}
