/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.index.sai.disk.MutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.Sorter;
import org.apache.lucene.util.bkd.MutablePointsReaderUtils;

// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/**
 * Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller
 * and smaller N-dim rectangles (cells) until the number of points in a given
 * rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 * fully balanced, which means the leaf nodes will have between 50% and 100% of
 * the requested <code>maxPointsInLeafNode</code>.  Values that fall exactly
 * on a cell boundary may be in either cell.
 *
 * <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 * <p>
 * See <a href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for details.
 *
 * <p>This consumes heap during writing: it allocates a <code>LongBitSet(numPoints)</code>,
 * and then uses up to the specified {@code maxMBSortInHeap} heap space for writing.
 *
 * <p>
 * <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> total points.
 *
 * @lucene.experimental
 */

public class BKDWriter implements Closeable
{
    /** How many bytes each docs takes in the fixed-width offline format */
    private final int bytesPerDoc;

    /** Default maximum number of point in each leaf block */
    public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

    /** Default maximum heap to use, before spilling to (slower) disk */
    public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

    /** Maximum number of dimensions */
    public static final int MAX_DIMS = 8;

    /** How many dimensions we are indexing */
    protected final int numDims;

    /** How many bytes each value in each dimension takes. */
    protected final int bytesPerDim;

    /** numDims * bytesPerDim */
    protected final int packedBytesLength;

    final BytesRef scratchBytesRef1 = new BytesRef();
    final int[] commonPrefixLengths;

    protected final LongBitSet docsSeen;

    protected final int maxPointsInLeafNode;
    private final int maxPointsSortInHeap;

    /** Minimum per-dim values, packed */
    protected final byte[] minPackedValue;

    /** Maximum per-dim values, packed */
    protected final byte[] maxPackedValue;

    protected long pointCount;

    /** true if we have so many values that we must write ords using long (8 bytes) instead of int (4 bytes) */
    protected final boolean longOrds;

    /** An upper bound on how many points the caller will add (includes deletions) */
    private final long totalPointCount;

    private final long maxDoc;

    private final ICompressor compressor;

    public BKDWriter(long maxDoc, int numDims, int bytesPerDim,
            int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount, boolean singleValuePerDoc,
            ICompressor compressor) throws IOException
    {
        this(maxDoc, numDims, bytesPerDim, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount, singleValuePerDoc,
             totalPointCount > Integer.MAX_VALUE, compressor);
    }

    protected BKDWriter(long maxDoc, int numDims, int bytesPerDim,
                        int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount,
                        boolean singleValuePerDoc, boolean longOrds, ICompressor compressor) throws IOException
    {
        verifyParams(numDims, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount);
        // We use tracking dir to deal with removing files on exception, so each place that
        // creates temp files doesn't need crazy try/finally/sucess logic:
        this.maxPointsInLeafNode = maxPointsInLeafNode;
        this.numDims = numDims;
        this.bytesPerDim = bytesPerDim;
        this.totalPointCount = totalPointCount;
        this.maxDoc = maxDoc;
        this.compressor = compressor;
        docsSeen = new LongBitSet(maxDoc);
        packedBytesLength = numDims * bytesPerDim;

        commonPrefixLengths = new int[numDims];

        minPackedValue = new byte[packedBytesLength];
        maxPackedValue = new byte[packedBytesLength];

        // If we may have more than 1+Integer.MAX_VALUE values, then we must encode ords with long (8 bytes), else we can use int (4 bytes).
        this.longOrds = longOrds;

        // dimensional values (numDims * bytesPerDim) + ord (int or long) + docID (int)
        if (singleValuePerDoc)
        {
            // Lucene only supports up to 2.1 docs, so we better not need longOrds in this case:
            assert longOrds == false;
            bytesPerDoc = packedBytesLength + Integer.BYTES;
        }
        else if (longOrds)
        {
            bytesPerDoc = packedBytesLength + Long.BYTES + Integer.BYTES;
        }
        else
        {
            bytesPerDoc = packedBytesLength + Integer.BYTES + Integer.BYTES;
        }

        // As we recurse, we compute temporary partitions of the data, halving the
        // number of points at each recursion.  Once there are few enough points,
        // we can switch to sorting in heap instead of offline (on disk).  At any
        // time in the recursion, we hold the number of points at that level, plus
        // all recursive halves (i.e. 16 + 8 + 4 + 2) so the memory usage is 2X
        // what that level would consume, so we multiply by 0.5 to convert from
        // bytes to points here.  Each dimension has its own sorted partition, so
        // we must divide by numDims as wel.

        maxPointsSortInHeap = (int) (0.5 * (maxMBSortInHeap * 1024 * 1024) / (bytesPerDoc * numDims));

        // Finally, we must be able to hold at least the leaf node in heap during build:
        if (maxPointsSortInHeap < maxPointsInLeafNode)
        {
            throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
        }
    }

    public static void verifyParams(int numDims, int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount)
    {
        // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for it now, in case we want to use
        // remaining 4 bits for another purpose later
        if (numDims < 1 || numDims > MAX_DIMS)
        {
            throw new IllegalArgumentException("numDims must be 1 .. " + MAX_DIMS + " (got: " + numDims + ")");
        }
        if (maxPointsInLeafNode <= 0)
        {
            throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
        }
        if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH)
        {
            throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);
        }
        if (maxMBSortInHeap < 0.0)
        {
            throw new IllegalArgumentException("maxMBSortInHeap must be >= 0.0 (got: " + maxMBSortInHeap + ")");
        }
        if (totalPointCount < 0)
        {
            throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
        }
    }

    /** How many points have been added so far */
    public long getPointCount()
    {
        return pointCount;
    }

    /**
     * Write a field from a {@link MutablePointValues}. This way of writing
     * points is faster than regular writes with BKDWriter#add since
     * there is opportunity for reordering points before writing them to
     * disk. This method does not use transient disk in order to reorder points.
     */
    public long writeField(IndexOutput out, MutableOneDimPointValues reader,
                           final OneDimensionBKDWriterCallback callback) throws IOException
    {
        if (numDims == 1)
        {
            SAICodecUtils.writeHeader(out);
            final long fp = writeField1Dim(out, reader, callback);
            SAICodecUtils.writeFooter(out);
            return fp;
        }
        else
        {
            throw new IllegalArgumentException("Only 1 dimension is supported.");
        }
    }

    /* In the 1D case, we can simply sort points in ascending order and use the
     * same writing logic as we use at merge time. */
    private long writeField1Dim(IndexOutput out, MutableOneDimPointValues reader,
                                OneDimensionBKDWriterCallback callback) throws IOException
    {
        // TODO: cast to int
        if (reader.size() > 1)
            MutablePointsReaderUtils.sort(Math.toIntExact(maxDoc), packedBytesLength, reader, 0, Math.toIntExact(reader.size()));

        final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out, callback);

        reader.intersect((docID, packedValue) -> oneDimWriter.add(packedValue, docID));

        return oneDimWriter.finish();
    }

    // reused when writing leaf blocks
    private final GrowableByteArrayDataOutput scratchOut = new GrowableByteArrayDataOutput(32 * 1024);

    private final GrowableByteArrayDataOutput scratchOut2 = new GrowableByteArrayDataOutput(2 * 1024);

    interface OneDimensionBKDWriterCallback
    {
        void writeLeafDocs(int leafNum, RowIDAndIndex[] leafDocs, int offset, int count);
    }

    public static class RowIDAndIndex
    {
        public int valueOrderIndex;
        public long rowID;

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("valueOrderIndex", valueOrderIndex)
                              .add("rowID", rowID)
                              .toString();
        }
    }

    private class OneDimensionBKDWriter
    {

        final IndexOutput out;
        final List<Long> leafBlockFPs = new ArrayList<>();
        final List<byte[]> leafBlockStartValues = new ArrayList<>();
        final byte[] leafValues = new byte[maxPointsInLeafNode * packedBytesLength];
        final long[] leafDocs = new long[maxPointsInLeafNode];
        private long valueCount;
        private int leafCount;
        final RowIDAndIndex[] rowIDAndIndexes = new RowIDAndIndex[maxPointsInLeafNode];
        final int[] orderIndex = new int[maxPointsInLeafNode];
        final OneDimensionBKDWriterCallback callback;

        {
            for (int x = 0; x < rowIDAndIndexes.length; x++)
            {
                rowIDAndIndexes[x] = new RowIDAndIndex();
            }
        }

        OneDimensionBKDWriter(IndexOutput out, OneDimensionBKDWriterCallback callback)
        {
            if (numDims != 1)
            {
                throw new UnsupportedOperationException("numDims must be 1 but got " + numDims);
            }
            if (pointCount != 0)
            {
                throw new IllegalStateException("cannot mix add and merge");
            }

            this.out = out;
            this.callback = callback;

            lastPackedValue = new byte[packedBytesLength];
        }

        // for asserts
        final byte[] lastPackedValue;
        private long lastDocID;

        void add(byte[] packedValue, long docID) throws IOException
        {
            assert valueInOrder(valueCount + leafCount,
                                0, lastPackedValue, packedValue, 0, docID, lastDocID);

            System.arraycopy(packedValue, 0, leafValues, leafCount * packedBytesLength, packedBytesLength);
            leafDocs[leafCount] = docID;
            docsSeen.set(docID);
            leafCount++;

            if (valueCount > totalPointCount)
            {
                throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + pointCount + " values");
            }

            if (leafCount == maxPointsInLeafNode)
            {
                // We write a block once we hit exactly the max count ... this is different from
                // when we write N > 1 dimensional points where we write between max/2 and max per leaf block
                writeLeafBlock();
                leafCount = 0;
            }

            assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
        }

        public long finish() throws IOException
        {
            if (leafCount > 0)
            {
                writeLeafBlock();
                leafCount = 0;
            }

            if (valueCount == 0)
            {
                return -1;
            }

            pointCount = valueCount;

            long indexFP = out.getFilePointer();

            int numInnerNodes = leafBlockStartValues.size();

            //System.out.println("BKDW: now rotate numInnerNodes=" + numInnerNodes + " leafBlockStarts=" + leafBlockStartValues.size());

            byte[] index = new byte[(1 + numInnerNodes) * (1 + bytesPerDim)];
            rotateToTree(1, 0, numInnerNodes, index, leafBlockStartValues);
            long[] arr = new long[leafBlockFPs.size()];
            for (int i = 0; i < leafBlockFPs.size(); i++)
            {
                arr[i] = leafBlockFPs.get(i);
            }
            writeIndex(out, maxPointsInLeafNode, arr, index);
            return indexFP;
        }

        private void writeLeafBlock() throws IOException
        {
            assert leafCount != 0;
            if (valueCount == 0)
            {
                System.arraycopy(leafValues, 0, minPackedValue, 0, packedBytesLength);
            }
            System.arraycopy(leafValues, (leafCount - 1) * packedBytesLength, maxPackedValue, 0, packedBytesLength);

            valueCount += leafCount;

            if (leafBlockFPs.size() > 0)
            {
                // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
                leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength));
            }
            leafBlockFPs.add(out.getFilePointer());
            checkMaxLeafNodeCount(leafBlockFPs.size());

            // Find per-dim common prefix:
            int prefix = bytesPerDim;
            int offset = (leafCount - 1) * packedBytesLength;
            for (int j = 0; j < bytesPerDim; j++)
            {
                if (leafValues[j] != leafValues[offset + j])
                {
                    prefix = j;
                    break;
                }
            }

            commonPrefixLengths[0] = prefix;

            assert scratchOut.getPosition() == 0;

            out.writeVInt(leafCount);

            for (int x = 0; x < leafCount; x++)
            {
                rowIDAndIndexes[x].valueOrderIndex = x;
                rowIDAndIndexes[x].rowID = leafDocs[x];
            }

            final Sorter sorter = new IntroSorter()
            {
                RowIDAndIndex pivot;

                @Override
                protected void swap(int i, int j)
                {
                    RowIDAndIndex o = rowIDAndIndexes[i];
                    rowIDAndIndexes[i] = rowIDAndIndexes[j];
                    rowIDAndIndexes[j] = o;
                }

                @Override
                protected void setPivot(int i)
                {
                    pivot = rowIDAndIndexes[i];
                }

                @Override
                protected int comparePivot(int j)
                {
                    return Long.compare(pivot.rowID, rowIDAndIndexes[j].rowID);
                }
            };

            sorter.sort(0, leafCount);

            // write leaf rowID -> orig index
            scratchOut2.reset();

            // iterate in row ID order to get the row ID index for the given value order index
            // place into an array to be written as packed ints
            for (int x = 0; x < leafCount; x++)
            {
                final int valueOrderIndex = rowIDAndIndexes[x].valueOrderIndex;
                orderIndex[valueOrderIndex] = x;
            }

            LeafOrderMap.write(orderIndex, leafCount, maxPointsInLeafNode - 1, scratchOut2);

            out.writeVInt(scratchOut2.getPosition());
            out.writeBytes(scratchOut2.getBytes(), 0, scratchOut2.getPosition());

            if (callback != null) callback.writeLeafDocs(leafBlockFPs.size() - 1, rowIDAndIndexes, 0, leafCount);

            writeCommonPrefixes(scratchOut, commonPrefixLengths, leafValues);

            scratchBytesRef1.length = packedBytesLength;
            scratchBytesRef1.bytes = leafValues;

            final IntFunction<BytesRef> packedValues = (i) -> {
                    scratchBytesRef1.offset = packedBytesLength * i;
                    return scratchBytesRef1;
            };
            assert valuesInOrderAndBounds(leafCount, 0, ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength),
                                          ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * packedBytesLength, leafCount * packedBytesLength),
                                          packedValues, leafDocs, 0);

            writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, leafCount, 0, packedValues);

            if (compressor == null)
            {
                out.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());
            }
            else
            {
                CryptoUtils.compress(new BytesRef(scratchOut.getBytes(), 0, scratchOut.getPosition()), scratchBytesRef, out, compressor);
            }
            scratchOut.reset();
        }
    }

    private final BytesRef scratchBytesRef = new BytesRef(new byte[128]);

    // TODO: there must be a simpler way?
    private void rotateToTree(int nodeID, int offset, int count, byte[] index, List<byte[]> leafBlockStartValues)
    {
        //System.out.println("ROTATE: nodeID=" + nodeID + " offset=" + offset + " count=" + count + " bpd=" + bytesPerDim + " index.length=" + index.length);
        if (count == 1)
        {
            // Leaf index node
            //System.out.println("  leaf index node");
            //System.out.println("  index[" + nodeID + "] = blockStartValues[" + offset + "]");
            System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
        }
        else if (count > 1)
        {
            // Internal index node: binary partition of count
            int countAtLevel = 1;
            int totalCount = 0;
            while (true)
            {
                int countLeft = count - totalCount;
                //System.out.println("    cycle countLeft=" + countLeft + " coutAtLevel=" + countAtLevel);
                if (countLeft <= countAtLevel)
                {
                    // This is the last level, possibly partially filled:
                    int lastLeftCount = Math.min(countAtLevel / 2, countLeft);
                    assert lastLeftCount >= 0;
                    int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

                    int rootOffset = offset + leftHalf;
          /*
          System.out.println("  last left count " + lastLeftCount);
          System.out.println("  leftHalf " + leftHalf + " rightHalf=" + (count-leftHalf-1));
          System.out.println("  rootOffset=" + rootOffset);
          */

                    System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
                    //System.out.println("  index[" + nodeID + "] = blockStartValues[" + rootOffset + "]");

                    // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
                    // under here, to save this while loop on each recursion

                    // Recurse left
                    rotateToTree(2 * nodeID, offset, leftHalf, index, leafBlockStartValues);

                    // Recurse right
                    rotateToTree(2 * nodeID + 1, rootOffset + 1, count - leftHalf - 1, index, leafBlockStartValues);
                    return;
                }
                totalCount += countAtLevel;
                countAtLevel *= 2;
            }
        }
        else
        {
            assert count == 0;
        }
    }

    // useful for debugging:
  /*
  private void printPathSlice(String desc, PathSlice slice, int dim) throws IOException {
    System.out.println("    " + desc + " dim=" + dim + " count=" + slice.count + ":");    
    try(PointReader r = slice.writer.getReader(slice.start, slice.count)) {
      int count = 0;
      while (r.next()) {
        byte[] v = r.packedValue();
        System.out.println("      " + count + ": " + new BytesRef(v, dim*bytesPerDim, bytesPerDim));
        count++;
        if (count == slice.count) {
          break;
        }
      }
    }
  }
  */

    private void checkMaxLeafNodeCount(int numLeaves)
    {
        if ((1 + bytesPerDim) * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH)
        {
            throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
        }
    }

    /** Packs the two arrays, representing a balanced binary tree, into a compact byte[] structure. */
    @SuppressWarnings("resource")
    private byte[] packIndex(long[] leafBlockFPs, byte[] splitPackedValues) throws IOException
    {

        int numLeaves = leafBlockFPs.length;

        // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree (only happens
        // if it was created by OneDimensionBKDWriter).  In this case the leaf nodes may straddle the two bottom
        // levels of the binary tree:
        if (numDims == 1 && numLeaves > 1)
        {
            int levelCount = 2;
            while (true)
            {
                if (numLeaves >= levelCount && numLeaves <= 2 * levelCount)
                {
                    int lastLevel = 2 * (numLeaves - levelCount);
                    assert lastLevel >= 0;
                    if (lastLevel != 0)
                    {
                        // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
                        // at read-time, so that we can still delta code them on disk at write:
                        long[] newLeafBlockFPs = new long[numLeaves];
                        System.arraycopy(leafBlockFPs, lastLevel, newLeafBlockFPs, 0, leafBlockFPs.length - lastLevel);
                        System.arraycopy(leafBlockFPs, 0, newLeafBlockFPs, leafBlockFPs.length - lastLevel, lastLevel);
                        leafBlockFPs = newLeafBlockFPs;
                    }
                    break;
                }

                levelCount *= 2;
            }
        }

        /** Reused while packing the index */
        // TODO: replace with RAMIndexOutput because RAMOutputStream has synchronized/monitor locks
        RAMOutputStream writeBuffer = new RAMOutputStream();

        // This is the "file" we append the byte[] to:
        List<byte[]> blocks = new ArrayList<>();
        byte[] lastSplitValues = new byte[bytesPerDim * numDims];
        //System.out.println("\npack index");
        int totalSize = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, 0l, blocks, 1, lastSplitValues, new boolean[numDims], false);

        // Compact the byte[] blocks into single byte index:
        byte[] index = new byte[totalSize];
        int upto = 0;
        for (byte[] block : blocks)
        {
            System.arraycopy(block, 0, index, upto, block.length);
            upto += block.length;
        }
        assert upto == totalSize;

        return index;
    }

    /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
    private int appendBlock(RAMOutputStream writeBuffer, List<byte[]> blocks) throws IOException
    {
        int pos = Math.toIntExact(writeBuffer.getFilePointer());
        byte[] bytes = new byte[pos];
        writeBuffer.writeTo(bytes, 0);
        writeBuffer.reset();
        blocks.add(bytes);
        return pos;
    }

    /**
     * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each
     * inner node
     */
    private int recursePackIndex(RAMOutputStream writeBuffer, long[] leafBlockFPs, byte[] splitPackedValues, long minBlockFP, List<byte[]> blocks,
                                 int nodeID, byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft) throws IOException
    {
        if (nodeID >= leafBlockFPs.length)
        {
            int leafID = nodeID - leafBlockFPs.length;
            //System.out.println("recursePack leaf nodeID=" + nodeID);

            // In the unbalanced case it's possible the left most node only has one child:
            if (leafID < leafBlockFPs.length)
            {
                long delta = leafBlockFPs[leafID] - minBlockFP;
                if (isLeft)
                {
                    assert delta == 0;
                    return 0;
                }
                else
                {
                    assert nodeID == 1 || delta > 0 : "nodeID=" + nodeID;
                    writeBuffer.writeVLong(delta);
                    return appendBlock(writeBuffer, blocks);
                }
            }
            else
            {
                return 0;
            }
        }
        else
        {
            long leftBlockFP;
            if (isLeft == false)
            {
                leftBlockFP = getLeftMostLeafBlockFP(leafBlockFPs, nodeID);
                long delta = leftBlockFP - minBlockFP;
                assert nodeID == 1 || delta > 0;
                writeBuffer.writeVLong(delta);
            }
            else
            {
                // The left tree's left most leaf block FP is always the minimal FP:
                leftBlockFP = minBlockFP;
            }

            int address = nodeID * (1 + bytesPerDim);
            int splitDim = splitPackedValues[address++] & 0xff;

            //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

            // find common prefix with last split value in this dim:
            int prefix = 0;
            for (; prefix < bytesPerDim; prefix++)
            {
                if (splitPackedValues[address + prefix] != lastSplitValues[splitDim * bytesPerDim + prefix])
                {
                    break;
                }
            }

            //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " bytesPerDim=" + bytesPerDim + " prefix=" + prefix);

            int firstDiffByteDelta;
            if (prefix < bytesPerDim)
            {
                //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
                firstDiffByteDelta = (splitPackedValues[address + prefix] & 0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix] & 0xFF);
                if (negativeDeltas[splitDim])
                {
                    firstDiffByteDelta = -firstDiffByteDelta;
                }
                //System.out.println("  delta=" + firstDiffByteDelta);
                assert firstDiffByteDelta > 0;
            }
            else
            {
                firstDiffByteDelta = 0;
            }

            // pack the prefix, splitDim and delta first diff byte into a single vInt:
            int code = (firstDiffByteDelta * (1 + bytesPerDim) + prefix) * numDims + splitDim;

            //System.out.println("  code=" + code);
            //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

            writeBuffer.writeVInt(code);

            // write the split value, prefix coded vs. our parent's split value:
            int suffix = bytesPerDim - prefix;
            byte[] savSplitValue = new byte[suffix];
            if (suffix > 1)
            {
                writeBuffer.writeBytes(splitPackedValues, address + prefix + 1, suffix - 1);
            }

            byte[] cmp = lastSplitValues.clone();

            System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix);

            // copy our split value into lastSplitValues for our children to prefix-code against
            System.arraycopy(splitPackedValues, address + prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            int numBytes = appendBlock(writeBuffer, blocks);

            // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
            // quickly seek to its starting point
            int idxSav = blocks.size();
            blocks.add(null);

            boolean savNegativeDelta = negativeDeltas[splitDim];
            negativeDeltas[splitDim] = true;

            int leftNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2 * nodeID, lastSplitValues, negativeDeltas, true);

            if (nodeID * 2 < leafBlockFPs.length)
            {
                writeBuffer.writeVInt(leftNumBytes);
            }
            else
            {
                assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
            }
            int numBytes2 = Math.toIntExact(writeBuffer.getFilePointer());
            byte[] bytes2 = new byte[numBytes2];
            writeBuffer.writeTo(bytes2, 0);
            writeBuffer.reset();
            // replace our placeholder:
            blocks.set(idxSav, bytes2);

            negativeDeltas[splitDim] = false;
            int rightNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2 * nodeID + 1, lastSplitValues, negativeDeltas, false);

            negativeDeltas[splitDim] = savNegativeDelta;

            // restore lastSplitValues to what caller originally passed us:
            System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            assert Arrays.equals(lastSplitValues, cmp);

            return numBytes + numBytes2 + leftNumBytes + rightNumBytes;
        }
    }

    private long getLeftMostLeafBlockFP(long[] leafBlockFPs, int nodeID)
    {
        // TODO: can we do this cheaper, e.g. a closed form solution instead of while loop?  Or
        // change the recursion while packing the index to return this left-most leaf block FP
        // from each recursion instead?
        //
        // Still, the overall cost here is minor: this method's cost is O(log(N)), and while writing
        // we call it O(N) times (N = number of leaf blocks)
        while (nodeID < leafBlockFPs.length)
        {
            nodeID *= 2;
        }
        int leafID = nodeID - leafBlockFPs.length;
        long result = leafBlockFPs[leafID];
        if (result < 0)
        {
            throw new AssertionError(result + " for leaf " + leafID);
        }
        return result;
    }

    private void writeIndex(IndexOutput out, int countPerLeaf, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException
    {
        byte[] packedIndex = packIndex(leafBlockFPs, splitPackedValues);
        writeIndex(out, countPerLeaf, leafBlockFPs.length, packedIndex);
    }

    private void writeIndex(IndexOutput out, int countPerLeaf, int numLeaves, byte[] packedIndex) throws IOException
    {
        out.writeVInt(numDims);
        out.writeVInt(countPerLeaf);
        out.writeVInt(bytesPerDim);

        assert numLeaves > 0;
        out.writeVInt(numLeaves);

        if (compressor != null)
        {
            RAMIndexOutput ramOut = new RAMIndexOutput("");
            ramOut.writeBytes(minPackedValue, 0, packedBytesLength);
            ramOut.writeBytes(maxPackedValue, 0, packedBytesLength);

            CryptoUtils.compress(new BytesRef(ramOut.getBytes(), 0, (int)ramOut.getFilePointer()), out, compressor);
        }
        else
        {
            out.writeBytes(minPackedValue, 0, packedBytesLength);
            out.writeBytes(maxPackedValue, 0, packedBytesLength);
        }

        out.writeVLong(pointCount);
        //TODO Changing disk format
        out.writeVLong(docsSeen.cardinality());

        if (compressor != null)
        {
            CryptoUtils.compress(new BytesRef(packedIndex, 0, packedIndex.length), out, compressor);
        }
        else
        {
            out.writeVInt(packedIndex.length);
            out.writeBytes(packedIndex, 0, packedIndex.length);
        }
    }

    private void writeLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues) throws IOException
    {
        int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();
        if (prefixLenSum == packedBytesLength)
        {
            // all values in this block are equal
            out.writeByte((byte) -1);
        }
        else
        {
            assert numDims == 1;

            assert commonPrefixLengths[sortedDim] < bytesPerDim;
            out.writeByte((byte) sortedDim);
            int compressedByteOffset = sortedDim * bytesPerDim + commonPrefixLengths[sortedDim];
            commonPrefixLengths[sortedDim]++;
            for (int i = 0; i < count; )
            {
                // do run-length compression on the byte at compressedByteOffset
                int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
                assert runLen <= 0xff;
                BytesRef first = packedValues.apply(i);
                byte prefixByte = first.bytes[first.offset + compressedByteOffset];
                out.writeByte(prefixByte);
                out.writeByte((byte) runLen);
                writeLeafBlockPackedValuesRange(out, commonPrefixLengths, i, i + runLen, packedValues);
                i += runLen;
                assert i <= count;
            }
        }
    }

    /**
     * Return an array that contains the min and max values for the [offset, offset+length] interval
     * of the given {@link BytesRef}s.
     */
    private static BytesRef[] computeMinMax(int count, IntFunction<BytesRef> packedValues, int offset, int length)
    {
        assert length > 0;
        BytesRefBuilder min = new BytesRefBuilder();
        BytesRefBuilder max = new BytesRefBuilder();
        BytesRef first = packedValues.apply(0);
        min.copyBytes(first.bytes, first.offset + offset, length);
        max.copyBytes(first.bytes, first.offset + offset, length);
        for (int i = 1; i < count; ++i)
        {
            BytesRef candidate = packedValues.apply(i);
            if (FutureArrays.compareUnsigned(min.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) > 0)
            {
                min.copyBytes(candidate.bytes, candidate.offset + offset, length);
            }
            else if (FutureArrays.compareUnsigned(max.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) < 0)
            {
                max.copyBytes(candidate.bytes, candidate.offset + offset, length);
            }
        }
        return new BytesRef[]{ min.get(), max.get() };
    }

    private void writeLeafBlockPackedValuesRange(DataOutput out, int[] commonPrefixLengths, int start, int end, IntFunction<BytesRef> packedValues) throws IOException
    {
        for (int i = start; i < end; ++i)
        {
            BytesRef ref = packedValues.apply(i);
            assert ref.length == packedBytesLength;

            for (int dim = 0; dim < numDims; dim++)
            {
                int prefix = commonPrefixLengths[dim];
                out.writeBytes(ref.bytes, ref.offset + dim * bytesPerDim + prefix, bytesPerDim - prefix);
            }
        }
    }

    private static int runLen(IntFunction<BytesRef> packedValues, int start, int end, int byteOffset)
    {
        BytesRef first = packedValues.apply(start);
        byte b = first.bytes[first.offset + byteOffset];
        for (int i = start + 1; i < end; ++i)
        {
            BytesRef ref = packedValues.apply(i);
            byte b2 = ref.bytes[ref.offset + byteOffset];
            assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
            if (b != b2)
            {
                return i - start;
            }
        }
        return end - start;
    }

    private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, byte[] packedValue) throws IOException
    {
        for (int dim = 0; dim < numDims; dim++)
        {
            out.writeVInt(commonPrefixes[dim]);
            //System.out.println(commonPrefixes[dim] + " of " + bytesPerDim);
            out.writeBytes(packedValue, dim * bytesPerDim, commonPrefixes[dim]);
        }
    }

    @Override
    public void close() throws IOException
    {

    }

    /** Called only in assert */
    private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue)
    {
        for (int dim = 0; dim < numDims; dim++)
        {
            int offset = bytesPerDim * dim;
            if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0)
            {
                return false;
            }
            if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0)
            {
                return false;
            }
        }

        return true;
    }

    // only called from assert
    private boolean valuesInOrderAndBounds(int count, int sortedDim, byte[] minPackedValue, byte[] maxPackedValue,
                                           IntFunction<BytesRef> values, long[] docs, int docsOffset) throws IOException
    {
        byte[] lastPackedValue = new byte[packedBytesLength];
        long lastDoc = -1;
        for (int i = 0; i < count; i++)
        {
            BytesRef packedValue = values.apply(i);
            assert packedValue.length == packedBytesLength;
            assert valueInOrder(i, sortedDim, lastPackedValue, packedValue.bytes, packedValue.offset,
                                docs[docsOffset + i], lastDoc);
            lastDoc = docs[docsOffset + i];

            // Make sure this value does in fact fall within this leaf cell:
            assert valueInBounds(packedValue, minPackedValue, maxPackedValue);
        }
        return true;
    }

    // only called from assert
    private boolean valueInOrder(long ord, int sortedDim, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset,
                                 long doc, long lastDoc)
    {
        int dimOffset = sortedDim * bytesPerDim;
        if (ord > 0)
        {
            int cmp = FutureArrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + bytesPerDim);
            if (cmp > 0)
            {
                throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
            }
            if (cmp == 0 && doc < lastDoc)
            {
                throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
            }
        }
        System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, packedBytesLength);
        return true;
    }
}
