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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

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
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Specialized writer for values, that builds them into a {@link BlockBalancedTreeWriter} with auxiliary
 * posting lists on eligible tree levels.
 * <p>
 * Given a sorted input {@link BlockBalancedTreeIterator}, the flush process is optimised because we don't need to
 * buffer all point values to sort them.
 */
public class NumericIndexWriter
{
    public static final int MAX_POINTS_IN_LEAF_NODE = BlockBalancedTreeWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE;
    private static final int DEFAULT_POSTINGS_SIZE = 128;

    private final BlockBalancedTreeWriter writer;
    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;
    private final int bytesPerValue;

    /**
     * @param maxSegmentRowId maximum possible segment row ID, used to create `maxRows` for the balanced tree
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
        checkArgument(maxSegmentRowId >= 0, "[%s] maxSegmentRowId must be non-negative value, but got %s", indexContext.getIndexName(), maxSegmentRowId);

        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.bytesPerValue = bytesPerValue;
        this.writer = new BlockBalancedTreeWriter(bytesPerValue, maxPointsInLeafNode);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .add("bytesPerValue", bytesPerValue)
                          .toString();
    }

    private static class LeafCallback implements BlockBalancedTreeWriter.Callback
    {
        final List<PackedLongValues> leafPostings = new ArrayList<>(DEFAULT_POSTINGS_SIZE);

        public int numLeaves()
        {
            return leafPostings.size();
        }

        @Override
        public void writeLeafPostings(BlockBalancedTreeWriter.RowIDAndIndex[] leafPostings, int offset, int count)
        {
            PackedLongValues.Builder builder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);

            for (int i = offset; i < count; ++i)
            {
                builder.add(leafPostings[i].rowID);
            }
            this.leafPostings.add(builder.build());
        }
    }

    /**
     * Writes a balanced tree and posting lists from a {@link BlockBalancedTreeIterator}.
     *
     * @param values sorted {@link BlockBalancedTreeIterator} values to write
     *
     * @return metadata describing the location and size of this balanced tree in the overall SSTable balanced tree component file
     */
    public SegmentMetadata.ComponentMetadataMap writeCompleteSegment(BlockBalancedTreeIterator values) throws IOException
    {
        long treePosition;

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        LeafCallback leafCallback = new LeafCallback();

        try (IndexOutput treeOutput = indexDescriptor.openPerIndexOutput(IndexComponent.BALANCED_TREE, indexContext, true))
        {
            // The SSTable balanced tree component file is opened in append mode, so our offset is the current file pointer.
            long treeOffset = treeOutput.getFilePointer();

            treePosition = writer.write(treeOutput, values, leafCallback);

            // If the treePosition is less than 0 then we didn't write any values out and the index is empty
            if (treePosition < 0)
                return components;

            long treeLength = treeOutput.getFilePointer() - treeOffset;

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("max_points_in_leaf_node", Integer.toString(writer.getMaxPointsInLeafNode()));
            attributes.put("num_leaves", Integer.toString(leafCallback.numLeaves()));
            attributes.put("num_values", Long.toString(writer.getValueCount()));
            attributes.put("bytes_per_value", Long.toString(writer.getBytesPerValue()));

            components.put(IndexComponent.BALANCED_TREE, treePosition, treeOffset, treeLength, attributes);
        }

        try (BlockBalancedTreeWalker reader = new BlockBalancedTreeWalker(indexDescriptor.createPerIndexFileHandle(IndexComponent.BALANCED_TREE, indexContext, null), treePosition);
             IndexOutputWriter postingsOutput = indexDescriptor.openPerIndexOutput(IndexComponent.POSTING_LISTS, indexContext, true))
        {
            long postingsOffset = postingsOutput.getFilePointer();

            BlockBalancedTreePostingsWriter postingsWriter = new BlockBalancedTreePostingsWriter();
            reader.traverse(postingsWriter);

            // The balanced tree postings writer already writes its own header & footer.
            long postingsPosition = postingsWriter.finish(postingsOutput, leafCallback.leafPostings, indexContext);

            Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put("num_leaf_postings", Integer.toString(postingsWriter.numLeafPostings));
            attributes.put("num_non_leaf_postings", Integer.toString(postingsWriter.numNonLeafPostings));

            long postingsLength = postingsOutput.getFilePointer() - postingsOffset;
            components.put(IndexComponent.POSTING_LISTS, postingsPosition, postingsOffset, postingsLength, attributes);
        }

        return components;
    }

    /**
     * @return number of values added
     */
    public long getValueCount()
    {
        return writer.getValueCount();
    }
}
