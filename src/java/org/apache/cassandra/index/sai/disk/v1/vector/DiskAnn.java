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
import java.util.function.IntConsumer;

import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

public class DiskAnn implements AutoCloseable
{
    private final FileHandle graphHandle;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final CachingGraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;
    private final String source;

    // only one of these will be not null
    private final CompressedVectors compressedVectors;

    public DiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerColumnIndexFiles indexFiles, IndexWriterConfig config, SSTableId sstableId) throws IOException
    {
        similarityFunction = config.getSimilarityFunction();
        source = sstableId.toString();

        SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
        graphHandle = indexFiles.termsData();
        graph = new CachingGraphIndex(new OnDiskGraphIndex<>(RandomAccessReaderAdapter.createSupplier(graphHandle), termsMetadata.offset));

        long pqSegmentOffset = componentMetadatas.get(IndexComponent.COMPRESSED_VECTORS).offset;
        try (var pqFileHandle = indexFiles.compressedVectors(); var reader = new RandomAccessReaderAdapter(pqFileHandle))
        {
            reader.seek(pqSegmentOffset);
            boolean containsCompressedVectors = reader.readBoolean();
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

    public CompressedVectors getCompressedVectors()
    {
        return compressedVectors;
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    public CloseableIterator<RowIdWithScore> search(float[] queryVector, int topK, int limit, Bits acceptBits, IntConsumer nodesVisitedConsumer)
    {
        OnHeapGraph.validateIndexable(queryVector, similarityFunction);

        GraphIndex.View<float[]> view = graph.getView();
        try
        {
            GraphSearcher<float[]> searcher = new GraphSearcher.Builder<>(view).build();
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
            Bits acceptedBits = ordinalsMap.ignoringDeleted(acceptBits);
            // Search is done within the iterator to keep track of visited nodes. The resulting iterator
            // searches until the graph is exhausted.
            AutoResumingNodeScoreIterator nodeScoreIterator = new AutoResumingNodeScoreIterator(searcher, scoreFunction, reRanker, topK, acceptedBits, nodesVisitedConsumer, false, source, view);
            return new NodeScoreToRowIdWithScoreIterator(nodeScoreIterator, ordinalsMap.getRowIdsView());
        }
        catch (Throwable e)
        {
            FileUtils.closeQuietly(view);
            throw Throwables.unchecked(e);
        }
    }

    public NeighborSimilarity.ApproximateScoreFunction getApproximateScoreFunction(float[] queryVector)
    {
        return compressedVectors.approximateScoreFunctionFor(queryVector, similarityFunction);
    }

    public NeighborSimilarity.ExactScoreFunction getExactScoreFunction(float[] queryVector, GraphIndex.View<float[]> view)
    {
        return i -> similarityFunction.compare(queryVector, view.getVector(i));
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

    /**
     * Get the graph view, callers must close the view.
     * @return
     */
    public GraphIndex.View<float[]> getView()
    {
        return graph.getView();
    }
}
