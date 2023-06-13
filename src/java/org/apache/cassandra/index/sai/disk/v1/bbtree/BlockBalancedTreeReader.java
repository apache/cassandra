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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.postings.FilteringPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PeekablePostingList;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Handles intersection of a multidimensional shape in byte[] space with a block KD-tree previously written with
 * {@link BlockBalancedTreeWriter}.
 */
public class BlockBalancedTreeReader extends TraversingBlockBalancedTreeReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Comparator<PeekablePostingList> COMPARATOR = Comparator.comparingLong(PeekablePostingList::peek);

    private final IndexContext indexContext;
    private final FileHandle postingsFile;
    private final BlockBalancedTreePostingsIndex postingsIndex;
    private final DirectReaders.Reader leafOrderMapReader;

    /**
     * Performs a blocking read.
     */
    public BlockBalancedTreeReader(IndexContext indexContext,
                                   FileHandle balancedTreeFile,
                                   long balancedTreeIndexRoot,
                                   FileHandle postingsFile,
                                   long balancedTreePostingsRoot) throws IOException
    {
        super(balancedTreeFile, balancedTreeIndexRoot);
        this.indexContext = indexContext;
        this.postingsFile = postingsFile;
        this.postingsIndex = new BlockBalancedTreePostingsIndex(postingsFile, balancedTreePostingsRoot);
        leafOrderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(maxPointsInLeafNode - 1));
    }

    public int getBytesPerValue()
    {
        return bytesPerValue;
    }

    public long getPointCount()
    {
        return pointCount;
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(postingsFile);
    }

    @SuppressWarnings({"resource", "RedundantSuppression"})
    public PostingList intersect(IntersectVisitor visitor, QueryEventListener.BlockBalancedTreeEventListener listener, QueryContext context)
    {
        Relation relation = visitor.compare(minPackedValue, maxPackedValue);

        if (relation == Relation.CELL_OUTSIDE_QUERY)
        {
            listener.onIntersectionEarlyExit();
            return null;
        }

        listener.onSegmentHit();
        IndexInput balancedTreeInput = IndexFileUtils.instance.openInput(indexFile);
        IndexInput postingsInput = IndexFileUtils.instance.openInput(postingsFile);
        IndexInput postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);
        PackedIndexTree index = new PackedIndexTree();

        Intersection intersection = relation == Relation.CELL_INSIDE_QUERY
                                    ? new Intersection(balancedTreeInput, postingsInput, postingsSummaryInput, index, listener, context)
                                    : new FilteringIntersection(balancedTreeInput, postingsInput, postingsSummaryInput, index, visitor, listener, context);

        return intersection.execute();
    }

    /**
     * Synchronous intersection of a multidimensional shape in byte[] space with a block KD-tree
     * previously written with {@link BlockBalancedTreeWriter}.
     */
    private class Intersection
    {
        private final Stopwatch queryExecutionTimer = Stopwatch.createStarted();
        final QueryContext context;

        final IndexInput balancedTreeInput;
        final IndexInput postingsInput;
        final IndexInput postingsSummaryInput;
        final PackedIndexTree index;
        final QueryEventListener.BlockBalancedTreeEventListener listener;

        Intersection(IndexInput balancedTreeInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                     PackedIndexTree index, QueryEventListener.BlockBalancedTreeEventListener listener, QueryContext context)
        {
            this.balancedTreeInput = balancedTreeInput;
            this.postingsInput = postingsInput;
            this.postingsSummaryInput = postingsSummaryInput;
            this.index = index;
            this.listener = listener;
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                PriorityQueue<PeekablePostingList> postingLists = new PriorityQueue<>(100, COMPARATOR);

                executeInternal(postingLists);

                FileUtils.closeQuietly(balancedTreeInput);

                return mergePostings(postingLists);
            }
            catch (Throwable t)
            {
                if (!(t instanceof QueryCancelledException))
                    logger.error(indexContext.logMessage("Balanced tree intersection failed on {}"), indexFile.path(), t);

                closeOnException();
                throw Throwables.cleaned(t);
            }
        }

        protected void executeInternal(final PriorityQueue<PeekablePostingList> postingLists) throws IOException
        {
            collectPostingLists(postingLists);
        }

        protected void closeOnException()
        {
            FileUtils.closeQuietly(balancedTreeInput);
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        protected PostingList mergePostings(PriorityQueue<PeekablePostingList> postingLists)
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
                    logger.trace(indexContext.logMessage("[{}] Intersection completed in {} microseconds. {} leaf and internal posting lists hit."),
                                 indexFile.path(), elapsedMicros, postingLists.size());
                return MergePostingList.merge(postingLists, () -> FileUtils.close(postingsInput, postingsSummaryInput));
            }
        }

        private void collectPostingLists(PriorityQueue<PeekablePostingList> postingLists) throws IOException
        {
            context.checkpoint();

            final int nodeID = index.getNodeID();

            // if there is pre-built posting for entire subtree
            if (postingsIndex.exists(nodeID))
            {
                postingLists.add(initPostingReader(postingsIndex.getPostingsFilePointer(nodeID)));
                return;
            }

            Preconditions.checkState(!index.isLeafNode(), "Leaf node %s does not have balanced tree postings.", index.getNodeID());

            // Recurse on left subtree:
            index.pushLeft();
            collectPostingLists(postingLists);
            index.pop();

            // Recurse on right subtree:
            index.pushRight();
            collectPostingLists(postingLists);
            index.pop();
        }

        @SuppressWarnings({"resource", "RedundantSuppression"})
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

        FilteringIntersection(IndexInput balancedTreeInput, IndexInput postingsInput, IndexInput postingsSummaryInput,
                              PackedIndexTree index, IntersectVisitor visitor,
                              QueryEventListener.BlockBalancedTreeEventListener listener, QueryContext context)
        {
            super(balancedTreeInput, postingsInput, postingsSummaryInput, index, listener, context);
            this.visitor = visitor;
            this.packedValue = new byte[packedBytesLength];
            this.origIndex = new short[maxPointsInLeafNode];
        }

        @Override
        public void executeInternal(final PriorityQueue<PeekablePostingList> postingLists) throws IOException
        {
            collectPostingLists(postingLists, minPackedValue, maxPackedValue);
        }

        private void collectPostingLists(PriorityQueue<PeekablePostingList> postingLists, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException
        {
            context.checkpoint();

            final Relation r = visitor.compare(cellMinPacked, cellMaxPacked);

            if (r == Relation.CELL_OUTSIDE_QUERY)
            {
                // This cell is fully outside the query shape: stop recursing
                return;
            }

            if (r == Relation.CELL_INSIDE_QUERY)
            {
                // This cell is fully inside the query shape: recursively add all points in this cell without filtering
                super.collectPostingLists(postingLists);
                return;
            }

            if (index.isLeafNode())
            {
                if (index.nodeExists())
                    filterLeaf(postingLists);
                return;
            }

            visitNode(postingLists, cellMinPacked, cellMaxPacked);
        }

        private void filterLeaf(PriorityQueue<PeekablePostingList> postingLists) throws IOException
        {
            balancedTreeInput.seek(index.getLeafBlockFP());

            int count = balancedTreeInput.readVInt();

            // loading doc ids occurred here prior

            int orderMapLength = balancedTreeInput.readVInt();

            long orderMapPointer = balancedTreeInput.getFilePointer();

            SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(balancedTreeInput);
            for (int x = 0; x < count; x++)
            {
                origIndex[x] = (short) LeafOrderMap.getValue(randomAccessInput, orderMapPointer, x, leafOrderMapReader);
            }

            // seek beyond the ordermap
            balancedTreeInput.seek(orderMapPointer + orderMapLength);

            FixedBitSet fixedBitSet = visitDocValues(balancedTreeInput, count, visitor, origIndex);

            int nodeID = index.getNodeID();

            if (postingsIndex.exists(nodeID) && fixedBitSet.cardinality() > 0)
            {
                long pointer = postingsIndex.getPostingsFilePointer(nodeID);
                postingLists.add(initFilteringPostingReader(pointer, fixedBitSet));
            }
        }

        void visitNode(PriorityQueue<PeekablePostingList> postingLists, byte[] cellMinPacked, byte[] cellMaxPacked) throws IOException
        {
            int splitDim = index.getSplitDim();
            assert splitDim >= 0 : "splitDim=" + splitDim;
            assert splitDim < 1;

            byte[] splitPackedValue = index.getSplitPackedValue();
            BytesRef splitDimValue = index.getSplitDimValue();
            assert splitDimValue.length == bytesPerValue;

            // make sure cellMin <= splitValue <= cellMax:
            assert FutureArrays.compareUnsigned(cellMinPacked, 0, bytesPerValue, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerValue) <= 0 : "bytesPerDim=" + bytesPerValue + " splitDim=" + splitDim + " numDims=" + 1;
            assert FutureArrays.compareUnsigned(cellMaxPacked, 0, bytesPerValue, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerValue) >= 0 : "bytesPerDim=" + bytesPerValue + " splitDim=" + splitDim + " numDims=" + 1;

            // Recurse on left subtree:
            System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, 0, bytesPerValue);

            index.pushLeft();
            collectPostingLists(postingLists, cellMinPacked, splitPackedValue);
            index.pop();

            // Restore the split dim value since it may have been overwritten while recursing:
            System.arraycopy(splitPackedValue, 0, splitDimValue.bytes, splitDimValue.offset, bytesPerValue);
            // Recurse on right subtree:
            System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, 0, bytesPerValue);
            index.pushRight();
            collectPostingLists(postingLists, splitPackedValue, cellMaxPacked);
            index.pop();
        }

        @SuppressWarnings({"resource", "RedundantSuppression"})
        private PeekablePostingList initFilteringPostingReader(long offset, FixedBitSet filter) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);
            PostingsReader postingsReader = new PostingsReader(postingsInput, summary, listener.postingListEventListener());
            return PeekablePostingList.makePeekable(new FilteringPostingList(filter, postingsReader));
        }

        private FixedBitSet visitDocValues(IndexInput in, int count, IntersectVisitor visitor, short[] origIndex) throws IOException
        {
            int commonPrefixLength = readCommonPrefixLength(in);
            int compressedDimension = readCompressedDimension(in);
            // If the compressed dimension is -1 it means that all values in the block are equal
            if (compressedDimension == -1)
                return visitRawDocValues(count, visitor, origIndex);
            else
                return visitCompressedDocValues(commonPrefixLength, in, count, visitor, origIndex);
        }

        private int readCompressedDimension(IndexInput in) throws IOException
        {
            int compressedDimension = in.readByte();
            if (compressedDimension != -1 && compressedDimension != 0)
                throw new CorruptIndexException(String.format("Dimension should be in the range [-1, 0], but was %d.", compressedDimension), in);
            return compressedDimension;
        }

        private FixedBitSet visitCompressedDocValues(int commonPrefixLength,
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

            FixedBitSet fixedBitSet = new FixedBitSet(maxPointsInLeafNode);

            for (i = 0; i < count; )
            {
                packedValue[compressedByteOffset] = in.readByte();
                final int runLen = Byte.toUnsignedInt(in.readByte());
                for (int j = 0; j < runLen; ++j)
                {
                    in.readBytes(packedValue, commonPrefixLength, bytesPerValue - commonPrefixLength);
                    final int rowIDIndex = origIndex[i + j];
                    if (visitor.visit(packedValue))
                        fixedBitSet.set(rowIDIndex);
                }
                i += runLen;
            }
            if (i != count)
                throw new CorruptIndexException(String.format("Expected %d sub-blocks but read %d.", count, i), in);

            return fixedBitSet;
        }

        private FixedBitSet visitRawDocValues(int count, IntersectVisitor visitor, final short[] origIndex)
        {
            FixedBitSet fixedBitSet = new FixedBitSet(maxPointsInLeafNode);

            // In our one-dimensional case, all the values in the leaf are the same, so we only
            // need to visit once then set the bits for the relevant indexes
            if (visitor.visit(packedValue))
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
        boolean visit(byte[] packedValue);

        /**
         * Called for non-leaf cells to test how the cell relates to the query, to
         * determine how to further recurse down the tree.
         */
        Relation compare(byte[] minPackedValue, byte[] maxPackedValue);
    }
}
