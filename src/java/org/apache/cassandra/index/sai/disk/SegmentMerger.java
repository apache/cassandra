/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.InvertedIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * Responsible for merging index segments into a single segment during initial index build.
 */
public interface SegmentMerger extends Closeable
{
    void addSegment(ColumnContext context, SegmentMetadata segment, SSTableIndex.PerIndexFiles indexFiles) throws IOException;

    boolean isEmpty();

    SegmentMetadata merge(ColumnContext context, IndexComponents components, DecoratedKey minKey, DecoratedKey maxKey, long maxSSTableRowId) throws IOException;

    @SuppressWarnings("resource")
    static SegmentMerger newSegmentMerger(boolean literal)
    {
        return literal ? new LiteralSegmentMerger() : new NumericSegmentMerger();
    }

    class LiteralSegmentMerger implements SegmentMerger
    {
        final List<TermsReader> readers = new ArrayList<>();
        final List<TermsIterator> segmentTermsIterators = new ArrayList<>();

        @Override
        public void addSegment(ColumnContext context, SegmentMetadata segment, SSTableIndex.PerIndexFiles indexFiles) throws IOException
        {
            segmentTermsIterators.add(createTermsIterator(segment, indexFiles));
        }

        @Override
        public boolean isEmpty()
        {
            return segmentTermsIterators.isEmpty();
        }

        @Override
        public SegmentMetadata merge(ColumnContext context, IndexComponents components, DecoratedKey minKey, DecoratedKey maxKey, long maxSSTableRowId) throws IOException
        {
            try (final TermsIteratorMerger merger = new TermsIteratorMerger(segmentTermsIterators.toArray(new TermsIterator[0]), context.getValidator()))
            {

                SegmentMetadata.ComponentMetadataMap indexMetas;
                long numRows;

                try (InvertedIndexWriter indexWriter = new InvertedIndexWriter(components, false))
                {
                    indexMetas = indexWriter.writeAll(merger);
                    numRows = indexWriter.getPostingsCount();
                }
                return new SegmentMetadata(0,
                                           numRows,
                                           merger.minSSTableRowId,
                                           merger.maxSSTableRowId,
                                           minKey,
                                           maxKey,
                                           merger.getMinTerm(),
                                           merger.getMaxTerm(),
                                           indexMetas);
            }
        }

        @Override
        public void close() throws IOException
        {
            readers.forEach(TermsReader::close);
        }

        @SuppressWarnings("resource")
        private TermsIterator createTermsIterator(SegmentMetadata segment, SSTableIndex.PerIndexFiles indexFiles) throws IOException
        {
            final long root = segment.getIndexRoot(indexFiles.components().termsData);
            assert root >= 0;

            final Map<String, String> map = segment.componentMetadatas.get(IndexComponents.NDIType.TERMS_DATA).attributes;
            final String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
            final long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

            final TermsReader termsReader = new TermsReader(indexFiles.components(),
                                                            indexFiles.termsData().sharedCopy(),
                                                            indexFiles.postingLists().sharedCopy(),
                                                            root,
                                                            footerPointer);
            readers.add(termsReader);
            return termsReader.allTerms(segment.segmentRowIdOffset, QueryEventListeners.NO_OP_TRIE_LISTENER);
        }
    }

    class NumericSegmentMerger implements SegmentMerger
    {
        final List<BKDReader.IteratorState> segmentIterators = new ArrayList<>();
        final List<BKDReader> readers = new ArrayList<>();

        ByteBuffer minTerm = null, maxTerm = null;

        @Override
        public void addSegment(ColumnContext context, SegmentMetadata segment, SSTableIndex.PerIndexFiles indexFiles) throws IOException
        {
            minTerm = TypeUtil.min(segment.minTerm, minTerm, context.getValidator());
            maxTerm = TypeUtil.max(segment.maxTerm, maxTerm, context.getValidator());

            segmentIterators.add(createIteratorState(segment, indexFiles));
        }

        @Override
        public boolean isEmpty()
        {
            return segmentIterators.isEmpty();
        }

        @Override
        public SegmentMetadata merge(ColumnContext context, IndexComponents components, DecoratedKey minKey, DecoratedKey maxKey, long maxSSTableRowId) throws IOException
        {
            final MergeOneDimPointValues merger = new MergeOneDimPointValues(segmentIterators, context.getValidator());

            final SegmentMetadata.ComponentMetadataMap componentMetadataMap;
            try (NumericIndexWriter indexWriter = new NumericIndexWriter(components,
                                                                         TypeUtil.fixedSizeOf(context.getValidator()),
                                                                         maxSSTableRowId,
                                                                         Integer.MAX_VALUE,
                                                                         context.getIndexWriterConfig(),
                                                                         false))
            {
                componentMetadataMap = indexWriter.writeAll(merger);
            }
            return new SegmentMetadata(0,
                                       merger.getNumRows(),
                                       merger.getMinRowID(),
                                       merger.getMaxRowID(),
                                       minKey,
                                       maxKey,
                                       minTerm,
                                       maxTerm,
                                       componentMetadataMap);
        }

        @Override
        public void close() throws IOException
        {
            segmentIterators.forEach(BKDReader.IteratorState::close);
            readers.forEach(BKDReader::close);
        }

        @SuppressWarnings("resource")
        private BKDReader.IteratorState createIteratorState(SegmentMetadata segment, SSTableIndex.PerIndexFiles indexFiles) throws IOException
        {
            final long bkdPosition = segment.getIndexRoot(indexFiles.components().kdTree);
            assert bkdPosition >= 0;
            final long postingsPosition = segment.getIndexRoot(indexFiles.components().kdTreePostingLists);
            assert postingsPosition >= 0;

            final BKDReader bkdReader = new BKDReader(indexFiles.components(),
                                                      indexFiles.kdtree().sharedCopy(),
                                                      bkdPosition,
                                                      indexFiles.kdtreePostingLists().sharedCopy(),
                                                      postingsPosition);
            readers.add(bkdReader);
            return bkdReader.iteratorState(rowid -> rowid + segment.segmentRowIdOffset);
        }
    }
}

