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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.disk.v1.postings.FilteringPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PeekablePostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Handles intersection of a point or point range with a block balanced tree previously written with
 * {@link BlockBalancedTreeWriter}.
 */
public class BlockBalancedTreeReader extends BlockBalancedTreeWalker implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Comparator<PeekablePostingList> COMPARATOR = Comparator.comparingLong(PeekablePostingList::peek);

    private final IndexIdentifier indexIdentifier;
    private final FileHandle postingsFile;
    private final BlockBalancedTreePostingsIndex postingsIndex;
    private final int leafOrderMapBitsRequired;
    /**
     * Performs a blocking read.
     */
    public BlockBalancedTreeReader(IndexIdentifier indexIdentifier,
                                   FileHandle treeIndexFile,
                                   long treeIndexRoot,
                                   FileHandle postingsFile,
                                   long treePostingsRoot) throws IOException
    {
        super(treeIndexFile, treeIndexRoot);
        this.indexIdentifier = indexIdentifier;
        this.postingsFile = postingsFile;
        this.postingsIndex = new BlockBalancedTreePostingsIndex(postingsFile, treePostingsRoot);
        leafOrderMapBitsRequired = DirectWriter.unsignedBitsRequired(maxValuesInLeafNode - 1);
    }

    public int getBytesPerValue()
    {
        return bytesPerValue;
    }

    public long getPointCount()
    {
        return valueCount;
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(postingsFile);
    }

    public PostingList intersect(IntersectVisitor visitor, QueryEventListener.BalancedTreeEventListener listener, QueryContext context)
    {
        Relation relation = visitor.compare(minPackedValue, maxPackedValue);

        if (relation == Relation.CELL_OUTSIDE_QUERY)
        {
            listener.onIntersectionEarlyExit();
            return null;
        }

        listener.onSegmentHit();
        IndexInput treeInput = IndexFileUtils.instance.openInput(treeIndexFile);
        IndexInput postingsInput = IndexFileUtils.instance.openInput(postingsFile);
        IndexInput postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);

        Intersection intersection = relation == Relation.CELL_INSIDE_QUERY
                                    ? new Intersection(treeInput, postingsInput, postingsSummaryInput, listener, context)
                                    : new FilteringIntersection(treeInput, postingsInput, postingsSummaryInput, visitor, listener, context);

        return intersection.execute();
    }

    /**
     * Synchronous intersection of a point or point range with a block balanced tree previously written
     * with {@link BlockBalancedTreeWriter}.
     */
    private class Intersection
    {
        private final Stopwatch queryExecutionTimer = Stopwatch.createStarted();
        final QueryContext context;

        final TraversalState state;
        final IndexInput treeInput;
        final IndexInput postingsInput;
        final IndexInput postingsSummaryInput;
        final QueryEventListener.BalancedTreeEventListener listener;
        final PriorityQueue<PeekablePostingList> postingLists;

        Intersection(IndexInput treeInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                     QueryEventListener.BalancedTreeEventListener listener, QueryContext context)
        {
            this.state = newTraversalState();
            this.treeInput = treeInput;
            this.postingsInput = postingsInput;
            this.postingsSummaryInput = postingsSummaryInput;
            this.listener = listener;
            this.context = context;
            postingLists = new PriorityQueue<>(numLeaves, COMPARATOR);
        }

        public PostingList execute()
        {
            try
            {
                executeInternal();

                FileUtils.closeQuietly(treeInput);

                return mergePostings();
            }
            catch (Throwable t)
            {
                if (!(t instanceof QueryCancelledException))
                    logger.error(indexIdentifier.logMessage("Balanced tree intersection failed on {}"), treeIndexFile.path(), t);

                closeOnException();
                throw Throwables.cleaned(t);
            }
        }

        protected void executeInternal() throws IOException
        {
            collectPostingLists();
        }

        protected void closeOnException()
        {
            FileUtils.closeQuietly(treeInput);
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        protected PostingList mergePostings()
        {
            final long elapsedMicros = queryExecutionTimer.stop().elapsed(TimeUnit.MICROSECONDS);

            listener.onIntersectionComplete(elapsedMicros, TimeUnit.MICROSECONDS);
            listener.postingListsHit(postingLists.size());

            if (postingLists.isEmpty())
            {
                FileUtils.closeQuietly(postingsInput);
                FileUtils.closeQuietly(postingsSummaryInput);
                return null;
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace(indexIdentifier.logMessage("[{}] Intersection completed in {} microseconds. {} leaf and internal posting lists hit."),
                                 treeIndexFile.path(), elapsedMicros, postingLists.size());
                return MergePostingList.merge(postingLists, () -> FileUtils.close(postingsInput, postingsSummaryInput));
            }
        }

        private void collectPostingLists() throws IOException
        {
            context.checkpoint();

            // This will return true if the node is a child leaf that has postings or if there is postings for the
            // entire subtree under a leaf
            if (postingsIndex.exists(state.nodeID))
            {
                postingLists.add(initPostingReader(postingsIndex.getPostingsFilePointer(state.nodeID)));
                return;
            }

            if (state.atLeafNode())
                throw new CorruptIndexException(indexIdentifier.logMessage(String.format("Leaf node %s does not have balanced tree postings.", state.nodeID)), "");

            // Recurse on left subtree:
            state.pushLeft();
            collectPostingLists();
            state.pop();

            // Recurse on right subtree:
            state.pushRight();
            collectPostingLists();
            state.pop();
        }

        private PeekablePostingList initPostingReader(long offset) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);
            return PeekablePostingList.makePeekable(new PostingsReader(postingsInput, summary, listener.postingListEventListener()));
        }
    }

    private class FilteringIntersection extends Intersection
    {
        private final IntersectVisitor visitor;
        private final byte[] packedValue;
        private final short[] origIndex;

        FilteringIntersection(IndexInput treeInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                              IntersectVisitor visitor, QueryEventListener.BalancedTreeEventListener listener, QueryContext context)
        {
            super(treeInput, postingsInput, postingsSummaryInput, listener, context);
            this.visitor = visitor;
            this.packedValue = new byte[bytesPerValue];
            this.origIndex = new short[maxValuesInLeafNode];
        }

        @Override
        public void executeInternal() throws IOException
        {
            collectPostingLists(minPackedValue, maxPackedValue);
        }

        private void collectPostingLists(byte[] minPackedValue, byte[] maxPackedValue) throws IOException
        {
            context.checkpoint();

            final Relation r = visitor.compare(minPackedValue, maxPackedValue);

            // This value range is fully outside the query shape: stop recursing
            if (r == Relation.CELL_OUTSIDE_QUERY)
                return;

            if (r == Relation.CELL_INSIDE_QUERY)
            {
                // This value range is fully inside the query shape: recursively add all points from this node without filtering
                super.collectPostingLists();
                return;
            }

            if (state.atLeafNode())
            {
                if (state.nodeExists())
                    filterLeaf();
                return;
            }

            visitNode(minPackedValue, maxPackedValue);
        }

        private void filterLeaf() throws IOException
        {
            treeInput.seek(state.getLeafBlockFP());

            int count = treeInput.readVInt();
            int orderMapLength = treeInput.readVInt();
            long orderMapPointer = treeInput.getFilePointer();

            SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(treeInput);
            LongValues leafOrderMapReader = DirectReader.getInstance(randomAccessInput, leafOrderMapBitsRequired, orderMapPointer);
            for (int index = 0; index < count; index++)
            {
                origIndex[index] = (short) Math.toIntExact(leafOrderMapReader.get(index));
            }

            // seek beyond the ordermap
            treeInput.seek(orderMapPointer + orderMapLength);

            FixedBitSet fixedBitSet = buildPostingsFilter(treeInput, count, visitor, origIndex);

            if (postingsIndex.exists(state.nodeID) && fixedBitSet.cardinality() > 0)
            {
                long pointer = postingsIndex.getPostingsFilePointer(state.nodeID);
                postingLists.add(initFilteringPostingReader(pointer, fixedBitSet));
            }
        }

        void visitNode(byte[] minPackedValue, byte[] maxPackedValue) throws IOException
        {
            assert !state.atLeafNode() : "Cannot recurse down tree because nodeID " + state.nodeID + " is a leaf node";

            byte[] splitValue = state.getSplitValue();

            if (BlockBalancedTreeWriter.DEBUG)
            {
                // make sure cellMin <= splitValue <= cellMax:
                assert ByteArrayUtil.compareUnsigned(minPackedValue, 0, splitValue, 0, bytesPerValue) <= 0 :"bytesPerValue=" + bytesPerValue;
                assert ByteArrayUtil.compareUnsigned(maxPackedValue, 0, splitValue, 0, bytesPerValue) >= 0 : "bytesPerValue=" + bytesPerValue;
            }

            // Recurse on left subtree:
            state.pushLeft();
            collectPostingLists(minPackedValue, splitValue);
            state.pop();

            // Recurse on right subtree:
            state.pushRight();
            collectPostingLists(splitValue, maxPackedValue);
            state.pop();
        }

        private PeekablePostingList initFilteringPostingReader(long offset, FixedBitSet filter) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);
            PostingsReader postingsReader = new PostingsReader(postingsInput, summary, listener.postingListEventListener());
            return PeekablePostingList.makePeekable(new FilteringPostingList(filter, postingsReader));
        }

        private FixedBitSet buildPostingsFilter(IndexInput in, int count, IntersectVisitor visitor, short[] origIndex) throws IOException
        {
            int commonPrefixLength = readCommonPrefixLength(in);
            return commonPrefixLength == bytesPerValue ? buildPostingsFilterForSingleValueLeaf(count, visitor, origIndex)
                                                       : buildPostingsFilterForMultiValueLeaf(commonPrefixLength, in, count, visitor, origIndex);
        }

        private FixedBitSet buildPostingsFilterForMultiValueLeaf(int commonPrefixLength,
                                                                 IndexInput in,
                                                                 int count,
                                                                 IntersectVisitor visitor,
                                                                 short[] origIndex) throws IOException
        {
            // the byte at `compressedByteOffset` is compressed using run-length compression,
            // other suffix bytes are stored verbatim
            int compressedByteOffset = commonPrefixLength;
            commonPrefixLength++;
            int i;

            FixedBitSet fixedBitSet = new FixedBitSet(maxValuesInLeafNode);

            for (i = 0; i < count; )
            {
                packedValue[compressedByteOffset] = in.readByte();
                final int runLen = Byte.toUnsignedInt(in.readByte());
                for (int j = 0; j < runLen; ++j)
                {
                    in.readBytes(packedValue, commonPrefixLength, bytesPerValue - commonPrefixLength);
                    final int rowIDIndex = origIndex[i + j];
                    if (visitor.contains(packedValue))
                        fixedBitSet.set(rowIDIndex);
                }
                i += runLen;
            }
            if (i != count)
                throw new CorruptIndexException(String.format("Expected %d sub-blocks but read %d.", count, i), in);

            return fixedBitSet;
        }

        private FixedBitSet buildPostingsFilterForSingleValueLeaf(int count, IntersectVisitor visitor, final short[] origIndex)
        {
            FixedBitSet fixedBitSet = new FixedBitSet(maxValuesInLeafNode);

            // All the values in the leaf are the same, so we only
            // need to visit once then set the bits for the relevant indexes
            if (visitor.contains(packedValue))
            {
                for (int i = 0; i < count; ++i)
                    fixedBitSet.set(origIndex[i]);
            }
            return fixedBitSet;
        }

        private int readCommonPrefixLength(IndexInput in) throws IOException
        {
            int prefixLength = in.readVInt();
            if (prefixLength > 0)
                in.readBytes(packedValue, 0, prefixLength);
            return prefixLength;
        }
    }

    /**
     * We recurse the balanced tree, using a provided instance of this to guide the recursion.
     */
    public interface IntersectVisitor
    {
        /**
         * Called for all values in a leaf cell that crosses the query.  The consumer should scrutinize the packedValue
         * to decide whether to accept it. Values are visited in increasing order, and in the case of ties,
         * in increasing order by segment row ID.
         */
        boolean contains(byte[] packedValue);

        /**
         * Called for non-leaf cells to test how the cell relates to the query, to
         * determine how to further recurse down the tree.
         */
        Relation compare(byte[] minPackedValue, byte[] maxPackedValue);
    }
}
