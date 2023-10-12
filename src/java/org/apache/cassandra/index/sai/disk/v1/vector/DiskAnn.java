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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.github.jbellis.jvector.disk.CachingGraphIndex;
import io.github.jbellis.jvector.disk.CompressedVectors;
import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.NeighborSimilarity;
import io.github.jbellis.jvector.graph.SearchResult.NodeScore;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;

public class DiskAnn implements AutoCloseable
{
    private final OnDiskOrdinalsMap ordinalsMap;
    private final CachingGraphIndex graph;
    private final VectorSimilarityFunction similarityFunction;

    // only one of these will be not null
    private final CompressedVectors compressedVectors;
    private final ListRandomAccessVectorValues uncompressedVectors;

    public DiskAnn(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerColumnIndexFiles indexFiles, IndexContext context) throws IOException
    {
        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
        graph = new CachingGraphIndex(new OnDiskGraphIndex<>(new TermsReaderSupplier(indexFiles.termsData()), termsMetadata.offset));

        long pqSegmentOffset = componentMetadatas.get(IndexComponent.VECTORS).offset;
        try (var fileHandle = indexFiles.vectors(); var reader = new RandomAccessReaderAdapter(fileHandle.createReader()))
        {
            reader.seek(pqSegmentOffset);
            var containsCompressedVectors = reader.readBoolean();
            if (containsCompressedVectors) {
                compressedVectors = CompressedVectors.load(reader, reader.getFilePointer());
                uncompressedVectors = null;
            }
            else
            {
                compressedVectors = null;
                var vectors = cacheOriginalVectors(graph);
                uncompressedVectors = new ListRandomAccessVectorValues(vectors, vectors.get(0).length);
            }
        }

        SegmentMetadata.ComponentMetadata postingListsMetadata = componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);
    }

    private static List<float[]> cacheOriginalVectors(GraphIndex<float[]> graph)
    {
        var view = graph.getView();
        return IntStream.range(0, graph.size()).mapToObj(view::getVector).collect(Collectors.toList());
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
    // VSTODO make this return something with a size
    public ReorderingPostingList search(float[] queryVector, int topK, Bits acceptBits)
    {
        OnHeapGraph.validateIndexable(queryVector, similarityFunction);

        var searcher = new GraphSearcher.Builder<>(graph.getView()).build();
        NeighborSimilarity.ScoreFunction scoreFunction;
        NeighborSimilarity.ReRanker<float[]> reRanker;
        if (compressedVectors == null)
        {
            assert uncompressedVectors != null;
            scoreFunction = (NeighborSimilarity.ExactScoreFunction)
                            i -> similarityFunction.compare(queryVector, uncompressedVectors.vectorValue(i));
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
        return annRowIdsToPostings(result.getNodes());
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
        public boolean hasNext() {
            while (!segmentRowIdIterator.hasNext() && it.hasNext()) {
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

    private ReorderingPostingList annRowIdsToPostings(NodeScore[] results)
    {
        try (var iterator = new RowIdIterator(results))
        {
            return new ReorderingPostingList(iterator, results.length);
        }
    }

    @Override
    public void close() throws IOException
    {
        ordinalsMap.close();
        graph.close();
    }

    public OnDiskOrdinalsMap.OrdinalsView getOrdinalsView()
    {
        return ordinalsMap.getOrdinalsView();
    }

    private static class TermsReaderSupplier implements ReaderSupplier
    {
        private final FileHandle fileHandle;

        private TermsReaderSupplier(FileHandle fileHandle)
        {
            this.fileHandle = fileHandle;
        }

        @Override
        public io.github.jbellis.jvector.disk.RandomAccessReader get()
        {
            return new RandomAccessReaderAdapter(fileHandle.createReader());
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(fileHandle);
        }
    }

    private static class RandomAccessReaderAdapter implements io.github.jbellis.jvector.disk.RandomAccessReader
    {
        private final RandomAccessReader reader;

        private RandomAccessReaderAdapter(RandomAccessReader reader)
        {
            this.reader = reader;
        }

        public boolean readBoolean() throws IOException
        {
            return reader.readBoolean();
        }

        public long getFilePointer()
        {
            return reader.getFilePointer();
        }

        @Override
        public void seek(long offset)
        {
            reader.seek(offset);
        }

        @Override
        public int readInt() throws IOException
        {
            return reader.readInt();
        }

        @Override
        public void readFully(byte[] bytes) throws IOException
        {
            reader.readFully(bytes);
        }

        @Override
        public void readFully(float[] floats) throws IOException
        {
            reader.readFully(floats);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(reader);
        }
    }
}
