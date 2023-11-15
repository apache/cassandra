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

package org.apache.cassandra.index.sai.disk.v3;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NodeSimilarity;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.SearchResult.NodeScore;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.VectorPostingList;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.disk.vector.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.vector.OrdinalsView;
import org.apache.cassandra.index.sai.disk.vector.RowIdsView;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.tracing.Tracing;

public class CassandraDiskAnn extends JVectorLuceneOnDiskGraph
{
    private static final Logger logger = Logger.getLogger(CassandraDiskAnn.class.getName());

    private final FileHandle graphHandle;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final CachingGraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;
    @Nullable
    private final CompressedVectors compressedVectors;

    public CassandraDiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        super(componentMetadatas, indexFiles);

        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = getComponentMetadata(IndexComponent.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        graph = new CachingGraphIndex(new OnDiskGraphIndex<>(graphHandle::createReader, termsMetadata.offset));

        long pqSegmentOffset = getComponentMetadata(IndexComponent.PQ).offset;
        try (var pqFile = indexFiles.pq();
             var reader = pqFile.createReader())
        {
            reader.seek(pqSegmentOffset);
            VectorCompression compressionType = VectorCompression.values()[reader.readByte()];
            if (compressionType == VectorCompression.PRODUCT_QUANTIZATION)
                compressedVectors = PQVectors.load(reader, reader.getFilePointer());
            else if (compressionType == VectorCompression.BINARY_QUANTIZATION)
                compressedVectors = BQVectors.load(reader, reader.getFilePointer());
            else
                compressedVectors = null;
        }

        SegmentMetadata.ComponentMetadata postingListsMetadata = getComponentMetadata(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
    }

    @Override
    public long ramBytesUsed()
    {
        return graph.ramBytesUsed();
    }

    @Override
    public int size()
    {
        return graph.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     *
     * @param queryVector the query vector
     * @param topK the number of results to look for in the index (>= limit)
     * @param limit the maximum number of results to return
     * @param acceptBits a Bits indicating which row IDs are acceptable, or null if no constraints
     * @param context unused (vestige from HNSW, retained in signature to allow calling both easily)
     */
    @Override
    public VectorPostingList search(float[] queryVector, int topK, int limit, Bits acceptBits, QueryContext context)
    {
        return search(queryVector, topK, 0, limit, acceptBits, context);
    }

    /**
     * @return Row IDs associated with the topK vectors near the query. If a threshold is specified, only vectors with
     * a similarity score >= threshold will be returned.
     * @param queryVector the query vector
     * @param topK the number of results to look for in the index (>= limit)
     * @param threshold the minimum similarity score to accept
     * @param limit the maximum number of results to return
     * @param acceptBits a Bits indicating which row IDs are acceptable, or null if no constraints
     * @param context unused (vestige from HNSW, retained in signature to allow calling both easily)
     * @return
     */
    @Override
    public VectorPostingList search(float[] queryVector, int topK, float threshold, int limit, Bits acceptBits, QueryContext context)
    {
        CassandraOnHeapGraph.validateIndexable(queryVector, similarityFunction);

        var view = graph.getView();
        var searcher = new GraphSearcher.Builder<>(view).build();
        NodeSimilarity.ScoreFunction scoreFunction;
        NodeSimilarity.ReRanker<float[]> reRanker;
        if (compressedVectors == null)
        {
            scoreFunction = (NodeSimilarity.ExactScoreFunction)
                            i -> similarityFunction.compare(queryVector, view.getVector(i));
            reRanker = null;
        }
        else
        {
            scoreFunction = compressedVectors.approximateScoreFunctionFor(queryVector, similarityFunction);
            reRanker = (i, map) -> similarityFunction.compare(queryVector, map.get(i));
        }
        var result = searcher.search(scoreFunction,
                                     reRanker,
                                     topK,
                                     threshold,
                                     ordinalsMap.ignoringDeleted(acceptBits));
        Tracing.trace("DiskANN search visited {} nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
        return annRowIdsToPostings(result, limit);
    }

    @Override
    public CompressedVectors getCompressedVectors()
    {
        return compressedVectors;
    }

    private class RowIdIterator implements PrimitiveIterator.OfInt, AutoCloseable
    {
        private final Iterator<NodeScore> it;
        private final RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

        private OfInt segmentRowIdIterator = IntStream.empty().iterator();

        public RowIdIterator(NodeScore[] results)
        {
            this.it = Arrays.stream(results).iterator();
        }

        @Override
        public boolean hasNext() {
            while (!segmentRowIdIterator.hasNext() && it.hasNext()) {
                try
                {
                    NodeScore result = it.next();
                    var ordinal = result.node;
                    int[] rowIds = rowIdsView.getSegmentRowIdsMatching(ordinal);
                    segmentRowIdIterator = Arrays.stream(rowIds).iterator();
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
        public void close()
        {
            rowIdsView.close();
        }
    }

    private VectorPostingList annRowIdsToPostings(SearchResult results, int limit)
    {
        try (var iterator = new RowIdIterator(results.getNodes()))
        {
            return new VectorPostingList(iterator, limit, results.getVisitedCount());
        }
    }

    @Override
    public void close() throws IOException
    {
        ordinalsMap.close();
        graph.close();
        graphHandle.close();
    }

    @Override
    public OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }
}
