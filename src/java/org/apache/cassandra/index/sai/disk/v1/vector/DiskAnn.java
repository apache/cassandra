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
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.SearchResult.NodeScore;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.postings.VectorPostingList;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.tracing.Tracing;

public class DiskAnn implements AutoCloseable
{
    private final FileHandle graphHandle;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final CachingGraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;

    // only one of these will be not null
    private final CompressedVectors compressedVectors;

    public DiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerColumnIndexFiles indexFiles, IndexWriterConfig config) throws IOException
    {
        similarityFunction = config.getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        graph = new CachingGraphIndex(new OnDiskGraphIndex<>(RandomAccessReaderAdapter.createSupplier(graphHandle), termsMetadata.offset));

        long pqSegmentOffset = componentMetadatas.get(IndexComponent.COMPRESSED_VECTORS).offset;
        try (var pqFileHandle = indexFiles.compressedVectors(); var reader = new RandomAccessReaderAdapter(pqFileHandle))
        {
            reader.seek(pqSegmentOffset);
            var containsCompressedVectors = reader.readBoolean();
            if (containsCompressedVectors)
                compressedVectors = CompressedVectors.load(reader, reader.getFilePointer());
            else
                compressedVectors = null;
        }

        SegmentMetadata.ComponentMetadata postingListsMetadata = componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
    }

    public long ramBytesUsed()
    {
        return graph.ramBytesUsed();
    }

    public int size()
    {
        return graph.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    public VectorPostingList search(float[] queryVector, int topK, int limit, Bits acceptBits)
    {
        OnHeapGraph.validateIndexable(queryVector, similarityFunction);

        var view = graph.getView();
        var searcher = new GraphSearcher.Builder<>(view).build();
        NeighborSimilarity.ScoreFunction scoreFunction;
        NeighborSimilarity.ReRanker<float[]> reRanker;
        if (compressedVectors == null)
        {
            scoreFunction = (NeighborSimilarity.ExactScoreFunction)
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
                                     ordinalsMap.ignoringDeleted(acceptBits));
        Tracing.trace("DiskANN search visited {} nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
        return annRowIdsToPostings(result, limit);
    }

    private class RowIdIterator implements PrimitiveIterator.OfInt, AutoCloseable
    {
        private final Iterator<NodeScore> it;
        private final OnDiskOrdinalsMap.RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

        private OfInt segmentRowIdIterator = IntStream.empty().iterator();

        public RowIdIterator(NodeScore[] results)
        {
            this.it = Arrays.stream(results).iterator();
        }

        @Override
        public boolean hasNext()
        {
            while (!segmentRowIdIterator.hasNext() && it.hasNext())
            {
                try
                {
                    var ordinal = it.next().node;
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

    public OnDiskOrdinalsMap.OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }
}
