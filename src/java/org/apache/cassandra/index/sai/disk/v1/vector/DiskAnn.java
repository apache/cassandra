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

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.quantization.CompressedVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;

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
        graph = new CachingGraphIndex(OnDiskGraphIndex.load(RandomAccessReaderAdapter.createSupplier(graphHandle), termsMetadata.offset));

        long pqSegmentOffset = componentMetadatas.get(IndexComponent.COMPRESSED_VECTORS).offset;
        try (var pqFileHandle = indexFiles.compressedVectors(); var reader = new RandomAccessReaderAdapter(pqFileHandle))
        {
            reader.seek(pqSegmentOffset);
            boolean containsCompressedVectors = reader.readBoolean();
            if (containsCompressedVectors)
                compressedVectors = PQVectors.load(reader, reader.getFilePointer());
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

        GraphSearcher searcher = new GraphSearcher(graph);
        try
        {
            VectorFloat<?> query = JVectorUtils.toVectorFloat(queryVector);
            ScoreFunction.ExactScoreFunction exactScoreFunction = getExactScoreFunction(query, (CachingGraphIndex.CachedView) searcher.getView());
            DefaultSearchScoreProvider scoreProvider = compressedVectors == null
                                                       ? new DefaultSearchScoreProvider(exactScoreFunction)
                                                       : new DefaultSearchScoreProvider(compressedVectors.precomputedScoreFunctionFor(query, similarityFunction),
                                                                                        exactScoreFunction);
            Bits acceptedBits = ordinalsMap.ignoringDeleted(acceptBits);
            // Search is done within the iterator to keep track of visited nodes. The resulting iterator
            // searches until the graph is exhausted.
            AutoResumingNodeScoreIterator nodeScoreIterator = new AutoResumingNodeScoreIterator(searcher, scoreProvider, topK, acceptedBits, nodesVisitedConsumer, false, source);
            return new NodeScoreToRowIdWithScoreIterator(nodeScoreIterator, ordinalsMap.getRowIdsView());
        }
        catch (Throwable e)
        {
            FileUtils.closeQuietly(searcher);
            throw Throwables.unchecked(e);
        }
    }

    public ScoreFunction.ApproximateScoreFunction getApproximateScoreFunction(float[] queryVector)
    {
        return compressedVectors.precomputedScoreFunctionFor(JVectorUtils.toVectorFloat(queryVector), similarityFunction);
    }

    public ScoreFunction.ExactScoreFunction getExactScoreFunction(float[] queryVector, CachingGraphIndex.CachedView view)
    {
        return getExactScoreFunction(JVectorUtils.toVectorFloat(queryVector), view);
    }

    private ScoreFunction.ExactScoreFunction getExactScoreFunction(VectorFloat<?> queryVector, CachingGraphIndex.CachedView view)
    {
        return view.rerankerFor(queryVector, similarityFunction);
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
    public CachingGraphIndex.CachedView getView()
    {
        return graph.getView();
    }
}
