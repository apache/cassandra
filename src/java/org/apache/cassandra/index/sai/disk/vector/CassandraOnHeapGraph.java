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

package org.apache.cassandra.index.sai.disk.vector;

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
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.pq.VectorCompressor;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.util.StringHelper;

import static java.lang.Math.max;

public class CassandraOnHeapGraph<T>
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOnHeapGraph.class);

    private final RamAwareVectorValues vectorValues;
    private final GraphIndexBuilder<float[]> builder;
    private final VectorType.VectorSerializer serializer;
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
    public CassandraOnHeapGraph(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig)
    {
        this(termComparator, indexWriterConfig, true);
    }

    /**
     * @param termComparator the vector type
     * @param indexWriterConfig
     * @param concurrent should be true for memtables, false for compaction.  Concurrent allows us to search
     *                   while building the graph; non-concurrent allows us to avoid synchronization costs.
     */
    public CassandraOnHeapGraph(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig, boolean concurrent)
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
            postings = new VectorPostings<T>(key);
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
    public PriorityQueue<T> search(float[] queryVector, int limit, Bits toAccept)
    {
        validateIndexable(queryVector, similarityFunction);

        // search() errors out when an empty graph is passed to it
        if (vectorValues.size() == 0)
            return new PriorityQueue<>();

        Bits bits = hasDeletions ? BitsUtil.bitsIgnoringDeleted(toAccept, postingsByOrdinal) : toAccept;
        // VSTODO re-use searcher objects
        GraphIndex<float[]> graph = builder.getGraph();
        var searcher = new GraphSearcher.Builder<>(graph.getView()).withConcurrentUpdates().build();
        NeighborSimilarity.ExactScoreFunction scoreFunction = node2 -> {
            return similarityFunction.compare(queryVector, ((RandomAccessVectorValues<float[]>) vectorValues).vectorValue(node2));
        };
        var result = searcher.search(scoreFunction, null, limit, bits);
        Tracing.trace("ANN search visited {} in-memory nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
        var a = result.getNodes();
        PriorityQueue<T> keyQueue = new PriorityQueue<>();
        for (int i = 0; i < a.length; i++)
        {
            int node = a[i].node;
            Collection<T> keys = keysFromOrdinal(node);
            keyQueue.addAll(keys);
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

        try (var pqOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.PQ, indexContext), true);
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext), true);
             var indexOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.TERMS_DATA, indexContext), true))
        {
            SAICodecUtils.writeHeader(pqOutput);
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(indexOutput);

            var deletedOrdinals = new HashSet<Integer>();
            postingsMap.values().stream().filter(VectorPostings::isEmpty).forEach(vectorPostings -> deletedOrdinals.add(vectorPostings.getOrdinal()));
            // remove ordinals that don't have corresponding row ids due to partition/range deletion
            for (VectorPostings<T> vectorPostings : postingsMap.values())
            {
                vectorPostings.computeRowIds(postingTransformer);
                if (vectorPostings.shouldAppendDeletedOrdinal())
                    deletedOrdinals.add(vectorPostings.getOrdinal());
            }

            // map of existing ordinal to rowId (aka new ordinal if remapping is possible)
            // null if remapping is not possible
            final BiMap <Integer, Integer> ordinalMap = deletedOrdinals.isEmpty() ? buildOrdinalMap() : null;

            boolean canFastFindRows = false; // ordinalMap != null;
            IntUnaryOperator ordinalMapper = canFastFindRows
                                                ? x -> ordinalMap.getOrDefault(x, x)
                                                : x -> x;
            IntUnaryOperator reverseOrdinalMapper = canFastFindRows
                                                        ? x -> ordinalMap.inverse().getOrDefault(x, x)
                                                        : x -> x;

            // compute and write PQ
            long pqOffset = pqOutput.getFilePointer();
            long pqPosition = writePQ(pqOutput.asSequentialWriter(), reverseOrdinalMapper);
            long pqLength = pqPosition - pqOffset;

            // write postings
            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition = new VectorPostingsWriter<T>(canFastFindRows, reverseOrdinalMapper)
                                            .writePostings(postingsOutput.asSequentialWriter(),
                                                           vectorValues, postingsMap, deletedOrdinals);
            long postingsLength = postingsPosition - postingsOffset;

            // complete (internal clean up) and write the graph
            builder.cleanup();
            long termsOffset = indexOutput.getFilePointer();

            OnDiskGraphIndex.write(new RemappingOnDiskGraphIndex<>(builder.getGraph(), ordinalMapper, reverseOrdinalMapper),
                                   new RemappingRamAwareVectorValues(vectorValues, reverseOrdinalMapper),
                                   indexOutput.asSequentialWriter());
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
            metadataMap.put(IndexComponent.PQ, -1, pqOffset, pqLength, vectorConfigs);
            return metadataMap;
        }
    }

    private BiMap <Integer, Integer> buildOrdinalMap()
    {
        BiMap <Integer, Integer> ordinalMap = HashBiMap.create();
        int minRow = Integer.MAX_VALUE;
        int maxRow = Integer.MIN_VALUE;
        for (VectorPostings<T> vectorPostings : postingsMap.values())
        {
            if (vectorPostings.getRowIds().size() != 1)
            {
                return null;
            }
            int rowId = vectorPostings.getRowIds().getInt(0);
            int ordinal = vectorPostings.getOrdinal();
            minRow = Math.min(minRow, rowId);
            maxRow = Math.max(maxRow, rowId);
            if (ordinalMap.containsKey(ordinal))
            {
                return null;
            } else {
                ordinalMap.put(ordinal, rowId);
            }
        }

        if (minRow != 0 || maxRow != postingsMap.values().size() - 1)
        {
            return null;
        }
        return ordinalMap;
    }

    private long writePQ(SequentialWriter writer, IntUnaryOperator reverseOrdinalMapper) throws IOException
    {
        VectorCompression compressionType;
        if (vectorValues.dimension() >= 1536)
            compressionType = VectorCompression.BINARY_QUANTIZATION;
        else if (vectorValues.size() < 1024)
            compressionType = VectorCompression.NONE;
        else
            compressionType = VectorCompression.PRODUCT_QUANTIZATION;
        writer.writeByte(compressionType.ordinal());
        if (compressionType == VectorCompression.NONE)
        {
            if (logger.isDebugEnabled()) logger.debug("Skipping compression for only {} vectors", vectorValues.size());
            return writer.position();
        }

        int bytesPerVector = getBytesPerVector(vectorValues.dimension());
        logger.debug("Computing PQ for {} vectors at {} bytes per vector (originally {}-D)",
                     vectorValues.size(), bytesPerVector, vectorValues.dimension());

        // train PQ and encode
        VectorCompressor<?> compressor;
        Object encoded; // byte[][], or long[][]
        // limit the PQ computation and encoding to one index at a time -- goal during flush is to
        // evict from memory ASAP so better to do the PQ build (in parallel) one at a time
        synchronized (CassandraOnHeapGraph.class)
        {
            if (vectorValues.dimension() >= 1536)
                compressor = BinaryQuantization.compute(vectorValues);
            else
                compressor = ProductQuantization.compute(vectorValues, bytesPerVector, false);
            assert !vectorValues.isValueShared();
            encoded = compressVectors(reverseOrdinalMapper, compressor);
        }

        // save (outside the synchronized block, this is io-bound not CPU)
        CompressedVectors cv;
        if (compressor instanceof BinaryQuantization)
            cv = new BQVectors((BinaryQuantization) compressor, (long[][]) encoded);
        else
            cv = new PQVectors((ProductQuantization) compressor, (byte[][]) encoded);
        cv.write(writer);
        return writer.position();
    }

    private Object compressVectors(IntUnaryOperator reverseOrdinalMapper, VectorCompressor<?> compressor)
    {
        if (compressor instanceof ProductQuantization)
            return IntStream.range(0, vectorValues.size()).parallel()
                       .mapToObj(i -> ((ProductQuantization) compressor).encode(vectorValues.vectorValue(reverseOrdinalMapper.applyAsInt(i))))
                       .toArray(byte[][]::new);
        else if (compressor instanceof BinaryQuantization)
            return IntStream.range(0, vectorValues.size()).parallel()
                            .mapToObj(i -> ((BinaryQuantization) compressor).encode(vectorValues.vectorValue(reverseOrdinalMapper.applyAsInt(i))))
                            .toArray(long[][]::new);
        throw new UnsupportedOperationException("Unrecognized compressor " + compressor.getClass());
    }

    /**
     * Compute bytes per vector for PQ using piecewise linear interpolation.
     *
     * @param dimension Dimension of the original vector
     * @return Approximate number of bytes after compression
     */
    private int getBytesPerVector(int dimension) {
        // the idea here is that higher dimensions compress well, but not so well that we should use fewer bits
        // than a lower-dimension vector, which is what you could get with cutoff points to switch between (e.g.)
        // D*0.5 and D*0.25.  Thus, the following ensures that bytes per vector is strictly increasing with D.
        if (dimension <= 32) {
            // We are compressing from 4-byte floats to single-byte codebook indexes,
            // so this represents compression of 4x
            // * GloVe-25 needs 25 BPV to achieve good recall
            return dimension;
        }
        if (dimension <= 64) {
            // * GloVe-50 performs fine at 25
            return 32;
        }
        if (dimension <= 200) {
            // * GloVe-100 and -200 perform well at 50 and 100 BPV, respectively
            return (int) (dimension * 0.5);
        }
        if (dimension <= 400) {
            // * NYTimes-256 actually performs fine at 64 BPV but we'll be conservative
            //   since we don't want BPV to decrease
            return 100;
        }
        if (dimension <= 768) {
            // allow BPV to increase linearly up to 192
            return (int) (dimension * 0.25);
        }
        if (dimension <= 1536) {
            // * ada002 vectors have good recall even at 192 BPV = compression of 32x
            return 192;
        }
        // We have not tested recall with larger vectors than this, let's let it increase linearly
        return (int) (dimension * 0.125);
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
