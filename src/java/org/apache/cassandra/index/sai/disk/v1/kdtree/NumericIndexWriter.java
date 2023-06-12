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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.store.IndexOutput;
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
public class NumericIndexWriter
{
    public static final int MAX_POINTS_IN_LEAF_NODE = BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE;

    private final BKDWriter writer;
    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;
    private final int bytesPerValue;

    /**
     * @param maxSegmentRowId maximum possible segment row ID, used to create `maxDoc` for kd-tree
     */
    public NumericIndexWriter(IndexDescriptor indexDescriptor,
                              IndexContext indexContext,
                              int bytesPerValue,
                              long maxSegmentRowId)
    {
        this(indexDescriptor, indexContext, MAX_POINTS_IN_LEAF_NODE, bytesPerValue, maxSegmentRowId);
    }

    @VisibleForTesting
    public NumericIndexWriter(IndexDescriptor indexDescriptor,
                              IndexContext indexContext,
                              int maxPointsInLeafNode,
                              int bytesPerValue,
                              long maxSegmentRowId)
    {
        checkArgument(maxSegmentRowId >= 0, "[%s] maxRowId must be non-negative value, but got %s", indexContext.getIndexName(), maxSegmentRowId);

        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.bytesPerValue = bytesPerValue;
        this.writer = new BKDWriter(maxSegmentRowId + 1, bytesPerValue, maxPointsInLeafNode);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("bytesPerDim", bytesPerValue)
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
        public void writeLeafDocs(BKDWriter.RowIDAndIndex[] sortedByRowID, int offset, int count)
        {
            PackedLongValues.Builder builder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);

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
        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        LeafCallback leafCallback = new LeafCallback();

        try (IndexOutput bkdOutput = indexDescriptor.openPerIndexOutput(IndexComponent.KD_TREE, indexContext, true))
        {
            // The SSTable kd-tree component file is opened in append mode, so our offset is the current file pointer.
            long bkdOffset = bkdOutput.getFilePointer();

            bkdPosition = writer.writeField(bkdOutput, values, leafCallback);

            // If the bkdPosition is less than 0 then we didn't write any values out
            // and the index is empty
            if (bkdPosition < 0)
                return components;

            long bkdLength = bkdOutput.getFilePointer() - bkdOffset;

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("max_points_in_leaf_node", Integer.toString(writer.getMaxPointsInLeafNode()));
            attributes.put("num_leaves", Integer.toString(leafCallback.numLeaves()));
            attributes.put("num_points", Long.toString(writer.getPointCount()));
            attributes.put("bytes_per_dim", Long.toString(writer.getBytesPerValue()));

            components.put(IndexComponent.KD_TREE, bkdPosition, bkdOffset, bkdLength, attributes);
        }

        try (TraversingBKDReader reader = new TraversingBKDReader(indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext), bkdPosition);
             IndexOutput postingsOutput = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexContext, true))
        {
            long postingsOffset = postingsOutput.getFilePointer();

            OneDimBKDPostingsWriter postingsWriter = new OneDimBKDPostingsWriter(leafCallback.postings, indexContext);
            reader.traverse(postingsWriter);

            // The kd-tree postings writer already writes its own header & footer.
            long postingsPosition = postingsWriter.finish(postingsOutput);

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("num_leaf_postings", Integer.toString(postingsWriter.numLeafPostings));
            attributes.put("num_non_leaf_postings", Integer.toString(postingsWriter.numNonLeafPostings));

            long postingsLength = postingsOutput.getFilePointer() - postingsOffset;
            components.put(IndexComponent.POSTING_LISTS, postingsPosition, postingsOffset, postingsLength, attributes);
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
