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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.MutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * Specialized writer for 1-dim point values, that builds them into a BKD tree with auxiliary posting lists on eligible
 * tree levels.
 *
 * Given sorted input {@link MutablePointValues}, 1-dim case allows to optimise flush process, because we don't need to
 * buffer all point values to sort them.
 */
public class NumericIndexWriter implements Closeable
{
    public static final int MAX_POINTS_IN_LEAF_NODE = BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
    private final BKDWriter writer;
    private final IndexComponents indexComponents;
    private final int bytesPerDim;
    private final boolean segmented;

    private final IndexWriterConfig config;

    /**
     * @param maxSegmentRowId maximum possible segment row ID, used to create `maxDoc` for kd-tree
     * @param numRows must be greater than number of added rowIds, only used for validation.
     */
    public NumericIndexWriter(IndexComponents indexComponents, int bytesPerDim, long maxSegmentRowId, long numRows, IndexWriterConfig config, boolean segmented) throws IOException
    {
        this(indexComponents, MAX_POINTS_IN_LEAF_NODE, bytesPerDim, maxSegmentRowId, numRows, config, segmented);
    }

    public NumericIndexWriter(IndexComponents indexComponents, int maxPointsInLeafNode, int bytesPerDim, long maxSegmentRowId, long numRows, IndexWriterConfig config, boolean segmented) throws IOException
    {
        checkArgument(maxSegmentRowId >= 0,
                      "[%s] maxRowId must be non-negative value, but got %s",
                      config.getIndexName(), maxSegmentRowId);

        checkArgument(numRows >= 0,
                      "[$s] numRows must be non-negative value, but got %s",
                      config.getIndexName(), numRows);

        this.indexComponents = indexComponents;
        this.bytesPerDim = bytesPerDim;
        this.config = config;
        this.writer = new BKDWriter(maxSegmentRowId + 1,
                                    1,
                                    bytesPerDim,
                                    maxPointsInLeafNode,
                                    BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP,
                                    numRows,
                                    true, null);
        this.segmented = segmented;
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.close(writer);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("bytesPerDim", bytesPerDim)
                          .add("bufferedPoints", writer.getPointCount())
                          .toString();
    }

    public static class LeafCallback implements BKDWriter.OneDimensionBKDWriterCallback
    {
        final List<PackedLongValues> postings = new ArrayList<>();

        public int numLeaves()
        {
            return postings.size();
        }

        @Override
        public void writeLeafDocs(int leafNum, BKDWriter.RowIDAndIndex[] sortedByRowID, int offset, int count)
        {
            final PackedLongValues.Builder builder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);

            for (int i = offset; i < count; ++i)
            {
                builder.add(sortedByRowID[i].rowID);
            }
            postings.add(builder.build());
        }
    }

    /**
     * Writes a k-d tree and posting lists from a {@link MutablePointValues}.
     *
     * @param values points to write
     *
     * @return metadata describing the location and size of this kd-tree in the overall SSTable kd-tree component file
     */
    public SegmentMetadata.ComponentMetadataMap writeAll(MutableOneDimPointValues values) throws IOException
    {
        long bkdPosition;
        final SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        final LeafCallback leafCallback = new LeafCallback();

        try (IndexOutput bkdOutput = indexComponents.createOutput(indexComponents.kdTree, true, segmented))
        {
            // The SSTable kd-tree component file is opened in append mode, so our offset is the current file pointer.
            final long bkdOffset = bkdOutput.getFilePointer();

            bkdPosition = writer.writeField(bkdOutput, values, leafCallback);

            // If the bkdPosition is less than 0 then we didn't write any values out
            // and the index is empty
            if (bkdPosition < 0)
                return components;

            final long bkdLength = bkdOutput.getFilePointer() - bkdOffset;

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("max_points_in_leaf_node", Integer.toString(writer.maxPointsInLeafNode));
            attributes.put("num_leaves", Integer.toString(leafCallback.numLeaves()));
            attributes.put("num_points", Long.toString(writer.pointCount));
            attributes.put("bytes_per_dim", Long.toString(writer.bytesPerDim));
            attributes.put("num_dims", Long.toString(writer.numDims));

            components.put(IndexComponents.NDIType.KD_TREE, bkdPosition, bkdOffset, bkdLength, attributes);
        }

        try (TraversingBKDReader reader = new TraversingBKDReader(indexComponents, indexComponents.createFileHandle(indexComponents.kdTree, segmented), bkdPosition);
             IndexOutput postingsOutput = indexComponents.createOutput(indexComponents.kdTreePostingLists, true, segmented))
        {
            final long postingsOffset = postingsOutput.getFilePointer();

            final OneDimBKDPostingsWriter postingsWriter = new OneDimBKDPostingsWriter(leafCallback.postings, config, indexComponents);
            reader.traverse(postingsWriter);

            // The kd-tree postings writer already writes its own header & footer.
            final long postingsPosition = postingsWriter.finish(postingsOutput);

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("num_leaf_postings", Integer.toString(postingsWriter.numLeafPostings));
            attributes.put("num_non_leaf_postings", Integer.toString(postingsWriter.numNonLeafPostings));

            long postingsLength = postingsOutput.getFilePointer() - postingsOffset;
            components.put(IndexComponents.NDIType.KD_TREE_POSTING_LISTS, postingsPosition, postingsOffset, postingsLength, attributes);
        }

        return components;
    }

    /**
     * @return number of points added
     */
    public long getPointCount()
    {
        return writer.getPointCount();
    }
}
