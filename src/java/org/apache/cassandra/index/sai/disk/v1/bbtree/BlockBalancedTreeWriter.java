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
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.disk.ResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.Sorter;
import org.apache.lucene.util.bkd.BKDWriter;

import static org.apache.cassandra.index.sai.postings.PostingList.END_OF_STREAM;

/**
 * This is a specialisation of the Lucene {@link BKDWriter} that only writes a single dimension
 * balanced tree.
 * <p>
 * Recursively builds a block balanced tree to assign all incoming points to smaller
 * and smaller rectangles (cells) until the number of points in a given
 * rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 * fully balanced, which means the leaf nodes will have between 50% and 100% of
 * the requested <code>maxPointsInLeafNode</code>.  Values that fall exactly
 * on a cell boundary may be in either cell.
 * <p>
 * Visual representation of the disk format:
 * <pre>
 *
 * +========+=======================================+==================+========+
 * | HEADER | LEAF BLOCK LIST                       | BALANCED TREE    | FOOTER |
 * +========+================+=====+================+==================+========+
 *          | LEAF BLOCK (0) | ... | LEAF BLOCK (N) | VALUES PER LEAF  |
 *          +----------------+-----+----------------+------------------|
 *          | ORDER INDEX    |                      | BYTES PER VALUE  |
 *          +----------------+                      +------------------+
 *          | PREFIX         |                      | NUMBER OF LEAVES |
 *          +----------------+                      +------------------+
 *          | VALUES         |                      | MINIMUM VALUE    |
 *          +----------------+                      +------------------+
 *                                                  | MAXIMUM VALUE    |
 *                                                  +------------------+
 *                                                  | TOTAL VALUES     |
 *                                                  +------------------+
 *                                                  | INDEX TREE       |
 *                                                  +--------+---------+
 *                                                  | LENGTH | BYTES   |
 *                                                  +--------+---------+
 *  </pre>
 *
 * <p>
 * <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> total points.
 * <p>
 * @see BKDWriter
 */
@NotThreadSafe
public class BlockBalancedTreeWriter
{
    // Enable to check that values are added to the tree in correct order and within bounds
    public static final boolean DEBUG = CassandraRelevantProperties.SAI_TEST_BALANCED_TREE_DEBUG_ENABLED.getBoolean();

    // Default maximum number of point in each leaf block
    public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

    private final int bytesPerValue;
    private final int maxPointsInLeafNode;
    private final byte[] minPackedValue;
    private final byte[] maxPackedValue;
    private long valueCount;

    public BlockBalancedTreeWriter(int bytesPerValue, int maxPointsInLeafNode)
    {
        if (maxPointsInLeafNode <= 0)
            throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
        if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH)
            throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " +
                                               ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);

        this.maxPointsInLeafNode = maxPointsInLeafNode;
        this.bytesPerValue = bytesPerValue;

        minPackedValue = new byte[bytesPerValue];
        maxPackedValue = new byte[bytesPerValue];
    }

    public long getValueCount()
    {
        return valueCount;
    }

    public int getBytesPerValue()
    {
        return bytesPerValue;
    }

    public int getMaxPointsInLeafNode()
    {
        return maxPointsInLeafNode;
    }

    /**
     * Write the sorted values from a {@link BlockBalancedTreeIterator}.
     * <p>
     * @param treeOutput The {@link IndexOutput} to write the balanced tree to
     * @param values The {@link BlockBalancedTreeIterator} containing the values and rowIDs to be written
     * @param callback The {@link Callback} used to record the leaf postings for each leaf
     *
     * @return The file pointer to the beginning of the balanced tree
     */
    public long write(IndexOutput treeOutput, BlockBalancedTreeIterator values, final Callback callback) throws IOException
    {
        SAICodecUtils.writeHeader(treeOutput);

        LeafWriter leafWriter = new LeafWriter(treeOutput, callback);

        while (values.hasNext())
        {
            Pair<byte[], PostingList> pair = values.next();
            long segmentRowId;
            while ((segmentRowId = pair.right.nextPosting()) != END_OF_STREAM)
                leafWriter.add(pair.left, segmentRowId);
        }

        valueCount = leafWriter.finish();

        long treeFilePointer = valueCount == 0 ? -1 : treeOutput.getFilePointer();

        // There is only any point in writing the balanced tree if any values were added
        if (treeFilePointer >= 0)
            writeBalancedTree(treeOutput, maxPointsInLeafNode, leafWriter.leafBlockStartValues, leafWriter.leafBlockFilePointers);

        SAICodecUtils.writeFooter(treeOutput);

        return treeFilePointer;
    }

    private void writeBalancedTree(IndexOutput out, int countPerLeaf, List<byte[]> leafBlockStartValues, List<Long> leafBlockFilePointer) throws IOException
    {
        int numInnerNodes = leafBlockStartValues.size();
        byte[] splitValues = new byte[(1 + numInnerNodes) * bytesPerValue];
        int treeDepth = recurseBalanceTree(1, 0, numInnerNodes, 1, splitValues, leafBlockStartValues);
        long[] leafBlockFPs = leafBlockFilePointer.stream().mapToLong(l -> l).toArray();
        byte[] packedIndex = packIndex(leafBlockFPs, splitValues);

        out.writeVInt(countPerLeaf);
        out.writeVInt(bytesPerValue);

        out.writeVInt(leafBlockFPs.length);
        out.writeVInt(Math.min(treeDepth, leafBlockFPs.length));

        out.writeBytes(minPackedValue, 0, bytesPerValue);
        out.writeBytes(maxPackedValue, 0, bytesPerValue);

        out.writeVLong(valueCount);

        out.writeVInt(packedIndex.length);
        out.writeBytes(packedIndex, 0, packedIndex.length);
    }

    /**
     * This can, potentially, be removed in the future by CASSANDRA-18597
     */
    private int recurseBalanceTree(int nodeID, int offset, int count, int treeDepth, byte[] splitValues, List<byte[]> leafBlockStartValues)
    {
        if (count == 1)
        {
            treeDepth++;
            // Leaf index node
            System.arraycopy(leafBlockStartValues.get(offset), 0, splitValues, nodeID * bytesPerValue, bytesPerValue);
        }
        else if (count > 1)
        {
            treeDepth++;
            // Internal index node: binary partition of count
            int countAtLevel = 1;
            int totalCount = 0;
            while (true)
            {
                int countLeft = count - totalCount;
                if (countLeft <= countAtLevel)
                {
                    // This is the last level, possibly partially filled:
                    int lastLeftCount = Math.min(countAtLevel / 2, countLeft);
                    assert lastLeftCount >= 0;
                    int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

                    int rootOffset = offset + leftHalf;

                    System.arraycopy(leafBlockStartValues.get(rootOffset), 0, splitValues, nodeID * bytesPerValue, bytesPerValue);

                    // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
                    // under here, to save this while loop on each recursion

                    // Recurse left
                    int leftTreeDepth = recurseBalanceTree(2 * nodeID, offset, leftHalf, treeDepth, splitValues, leafBlockStartValues);

                    // Recurse right
                    int rightTreeDepth = recurseBalanceTree(2 * nodeID + 1, rootOffset + 1, count - leftHalf - 1, treeDepth, splitValues, leafBlockStartValues);
                    return Math.max(leftTreeDepth, rightTreeDepth);
                }
                totalCount += countAtLevel;
                countAtLevel *= 2;
            }
        }
        else
        {
            assert count == 0;
        }
        return treeDepth;
    }

    // Packs the two arrays, representing a balanced binary tree, into a compact byte[] structure.
    private byte[] packIndex(long[] leafBlockFPs, byte[] splitValues) throws IOException
    {
        int numLeaves = leafBlockFPs.length;

        // Possibly rotate the leaf block FPs, if the index is not a fully balanced binary tree (only happens
        // if it was created by TreeWriter).  In this case the leaf nodes may straddle the two bottom
        // levels of the binary tree:
        if (numLeaves > 1)
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

        // Reused while packing the index
        try (ResettableByteBuffersIndexOutput writeBuffer = new ResettableByteBuffersIndexOutput("PackedIndex"))
        {
            // This is the "file" we append the byte[] to:
            List<byte[]> blocks = new ArrayList<>();
            byte[] lastSplitValue = new byte[bytesPerValue];
            int totalSize = recursePackIndex(writeBuffer, leafBlockFPs, splitValues, 0, blocks, 1, lastSplitValue, false);
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
    }

    /**
     * lastSplitValue is the split value previously seen; we use this to prefix-code the split byte[] on each
     * inner node
     */
    private int recursePackIndex(ResettableByteBuffersIndexOutput writeBuffer, long[] leafBlockFPs, byte[] splitValues,
                                 long minBlockFP, List<byte[]> blocks, int nodeID, byte[] lastSplitValue, boolean isLeft) throws IOException
    {
        if (nodeID >= leafBlockFPs.length)
        {
            int leafID = nodeID - leafBlockFPs.length;

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
                throw new IllegalStateException("Unbalanced tree");
            }
        }
        else
        {
            long leftBlockFP;
            if (!isLeft)
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

            int address = nodeID * bytesPerValue;

            // find common prefix with last split value in this dim:
            int prefix = 0;
            for (; prefix < bytesPerValue; prefix++)
            {
                if (splitValues[address + prefix] != lastSplitValue[prefix])
                {
                    break;
                }
            }

            int firstDiffByteDelta;
            if (prefix < bytesPerValue)
            {
                firstDiffByteDelta = (splitValues[address + prefix] & 0xFF) - (lastSplitValue[prefix] & 0xFF);
                // If this is left then we need to negate the delta
                if (isLeft)
                    firstDiffByteDelta = -firstDiffByteDelta;
                assert firstDiffByteDelta > 0;
            }
            else
            {
                firstDiffByteDelta = 0;
            }

            // pack the prefix and delta first diff byte into a single vInt:
            int code = (firstDiffByteDelta * (1 + bytesPerValue) + prefix);

            writeBuffer.writeVInt(code);

            // write the split value, prefix coded vs. our parent's split value:
            int suffix = bytesPerValue - prefix;
            byte[] savSplitValue = new byte[suffix];
            if (suffix > 1)
            {
                writeBuffer.writeBytes(splitValues, address + prefix + 1, suffix - 1);
            }

            byte[] cmp = lastSplitValue.clone();

            System.arraycopy(lastSplitValue, prefix, savSplitValue, 0, suffix);

            // copy our split value into lastSplitValue for our children to prefix-code against
            System.arraycopy(splitValues, address + prefix, lastSplitValue, prefix, suffix);

            int numBytes = appendBlock(writeBuffer, blocks);

            // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into
            // the right subtree we can quickly seek to its starting point
            int idxSav = blocks.size();
            blocks.add(null);

            int leftNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitValues, leftBlockFP, blocks, 2 * nodeID, lastSplitValue, true);

            if (nodeID * 2 < leafBlockFPs.length)
            {
                writeBuffer.writeVInt(leftNumBytes);
            }
            else
            {
                assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
            }
            int numBytes2 = Math.toIntExact(writeBuffer.getFilePointer());
            byte[] bytes2 = writeBuffer.toArrayCopy();
            writeBuffer.reset();
            // replace our placeholder:
            blocks.set(idxSav, bytes2);

            int rightNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitValues, leftBlockFP, blocks, 2 * nodeID + 1, lastSplitValue, false);

            // restore lastSplitValue to what caller originally passed us:
            System.arraycopy(savSplitValue, 0, lastSplitValue, prefix, suffix);

            assert Arrays.equals(lastSplitValue, cmp);

            return numBytes + numBytes2 + leftNumBytes + rightNumBytes;
        }
    }

    /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
    private int appendBlock(ResettableByteBuffersIndexOutput writeBuffer, List<byte[]> blocks)
    {
        int pos = Math.toIntExact(writeBuffer.getFilePointer());
        byte[] bytes = writeBuffer.toArrayCopy();
        writeBuffer.reset();
        blocks.add(bytes);
        return pos;
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

    interface Callback
    {
        void writeLeafPostings(RowIDAndIndex[] leafPostings, int offset, int count);
    }

    static class RowIDAndIndex
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

    /**
     * Responsible for writing the leaf blocks at the beginning of the balanced tree index.
     */
    private class LeafWriter
    {
        private final IndexOutput treeOutput;
        private final List<Long> leafBlockFilePointers = new ArrayList<>();
        private final List<byte[]> leafBlockStartValues = new ArrayList<>();
        private final byte[] leafValues = new byte[maxPointsInLeafNode * bytesPerValue];
        private final long[] leafRowIDs = new long[maxPointsInLeafNode];
        private final RowIDAndIndex[] rowIDAndIndexes = new RowIDAndIndex[maxPointsInLeafNode];
        private final int[] orderIndex = new int[maxPointsInLeafNode];
        private final Callback callback;
        private final ByteBuffersDataOutput leafOrderIndexOutput = new ByteBuffersDataOutput(2 * 1024);
        private final ByteBuffersDataOutput leafBlockOutput = new ByteBuffersDataOutput(32 * 1024);
        private final byte[] lastPackedValue = new byte[bytesPerValue];

        private long valueCount;
        private int leafValueCount;
        private long lastRowID;

        LeafWriter(IndexOutput treeOutput, Callback callback)
        {
            assert callback != null : "Callback cannot be null in TreeWriter";

            this.treeOutput = treeOutput;
            this.callback = callback;

            for (int x = 0; x < rowIDAndIndexes.length; x++)
            {
                rowIDAndIndexes[x] = new RowIDAndIndex();
            }
        }

        /**
         * Adds a value and row ID to the current leaf block. If the leaf block is full after the addition
         * the current leaf block is written to disk.
         */
        void add(byte[] packedValue, long rowID) throws IOException
        {
            if (DEBUG)
                valueInOrder(valueCount + leafValueCount, lastPackedValue, packedValue, 0, rowID, lastRowID);

            System.arraycopy(packedValue, 0, leafValues, leafValueCount * bytesPerValue, bytesPerValue);
            leafRowIDs[leafValueCount] = rowID;
            leafValueCount++;

            if (leafValueCount == maxPointsInLeafNode)
            {
                // We write a block once we hit exactly the max count
                writeLeafBlock();
                leafValueCount = 0;
            }

            if (DEBUG)
                if ((lastRowID = rowID) < 0)
                    throw new AssertionError("row id must be >= 0; got " + rowID);
        }

        /**
         * Write a leaf block if we have unwritten values and return the total number of values added
         */
        public long finish() throws IOException
        {
            if (leafValueCount > 0)
                writeLeafBlock();

            return valueCount;
        }

        private void writeLeafBlock() throws IOException
        {
            assert leafValueCount != 0;
            if (valueCount == 0)
            {
                System.arraycopy(leafValues, 0, minPackedValue, 0, bytesPerValue);
            }
            System.arraycopy(leafValues, (leafValueCount - 1) * bytesPerValue, maxPackedValue, 0, bytesPerValue);

            valueCount += leafValueCount;

            if (leafBlockFilePointers.size() > 0)
            {
                // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
                leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, bytesPerValue));
            }
            leafBlockFilePointers.add(treeOutput.getFilePointer());
            checkMaxLeafNodeCount(leafBlockFilePointers.size());

            // Find the common prefix between the first and last values in the block
            int commonPrefixLength = bytesPerValue;
            int offset = (leafValueCount - 1) * bytesPerValue;
            for (int j = 0; j < bytesPerValue; j++)
            {
                if (leafValues[j] != leafValues[offset + j])
                {
                    commonPrefixLength = j;
                    break;
                }
            }

            treeOutput.writeVInt(leafValueCount);

            for (int x = 0; x < leafValueCount; x++)
            {
                rowIDAndIndexes[x].valueOrderIndex = x;
                rowIDAndIndexes[x].rowID = leafRowIDs[x];
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

            sorter.sort(0, leafValueCount);

            // write the leaf order index: leaf rowID -> orig index
            leafOrderIndexOutput.reset();

            // iterate in row ID order to get the row ID index for the given value order index
            // place into an array to be written as packed ints
            for (int x = 0; x < leafValueCount; x++)
                orderIndex[rowIDAndIndexes[x].valueOrderIndex] = x;

            LeafOrderMap.write(orderIndex, leafValueCount, maxPointsInLeafNode - 1, leafOrderIndexOutput);

            treeOutput.writeVInt((int) leafOrderIndexOutput.size());
            leafOrderIndexOutput.copyTo(treeOutput);

            callback.writeLeafPostings(rowIDAndIndexes, 0, leafValueCount);

            // Write the common prefix for the leaf block
            writeCommonPrefix(treeOutput, commonPrefixLength, leafValues);

            // Write the run length encoded packed values for the leaf block
            leafBlockOutput.reset();

            if (DEBUG)
                valuesInOrderAndBounds(leafValueCount,
                                       ArrayUtil.copyOfSubArray(leafValues, 0, bytesPerValue),
                                       ArrayUtil.copyOfSubArray(leafValues, (leafValueCount - 1) * bytesPerValue, leafValueCount * bytesPerValue),
                                       leafRowIDs);

            writeLeafBlockPackedValues(leafBlockOutput, commonPrefixLength, leafValueCount);

            leafBlockOutput.copyTo(treeOutput);
        }

        private void checkMaxLeafNodeCount(int numLeaves)
        {
            if (bytesPerValue * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH)
            {
                throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
            }
        }

        private void writeCommonPrefix(DataOutput treeOutput, int commonPrefixLength, byte[] packedValue) throws IOException
        {
            treeOutput.writeVInt(commonPrefixLength);
            if (commonPrefixLength > 0)
                treeOutput.writeBytes(packedValue, 0, commonPrefixLength);
        }

        private void writeLeafBlockPackedValues(DataOutput out, int commonPrefixLength, int count) throws IOException
        {
            // If all the values are the same (e.g. the common prefix length == bytes per value) then we don't
            // need to write anything. Otherwise, we run length compress the values to disk.
            if (commonPrefixLength != bytesPerValue)
            {
                int compressedByteOffset = commonPrefixLength;
                commonPrefixLength++;
                for (int i = 0; i < count; )
                {
                    // do run-length compression on the byte at compressedByteOffset
                    int runLen = runLen(i, Math.min(i + 0xff, count), compressedByteOffset);
                    assert runLen <= 0xff;
                    byte prefixByte = leafValues[i * bytesPerValue + compressedByteOffset];
                    out.writeByte(prefixByte);
                    out.writeByte((byte) runLen);
                    writeLeafBlockPackedValuesRange(out, commonPrefixLength, i, i + runLen);
                    i += runLen;
                    assert i <= count;
                }
            }
        }

        private void writeLeafBlockPackedValuesRange(DataOutput out, int commonPrefixLength, int start, int end) throws IOException
        {
            for (int i = start; i < end; ++i)
            {
                out.writeBytes(leafValues, i * bytesPerValue + commonPrefixLength, bytesPerValue - commonPrefixLength);
            }
        }

        private int runLen(int start, int end, int byteOffset)
        {
            byte b = leafValues[start * bytesPerValue + byteOffset];
            for (int i = start + 1; i < end; ++i)
            {
                byte b2 = leafValues[i * bytesPerValue + byteOffset];
                assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
                if (b != b2)
                {
                    return i - start;
                }
            }
            return end - start;
        }

        // The following 3 methods are only used when DEBUG is true:

        private void valueInBounds(byte[] packedValue, int packedValueOffset, byte[] minPackedValue, byte[] maxPackedValue)
        {
            if (ByteArrayUtil.compareUnsigned(packedValue,
                                              packedValueOffset,
                                              minPackedValue,
                                              0,
                                              bytesPerValue) < 0)
            {
                throw new AssertionError("value=" + new BytesRef(packedValue, packedValueOffset, bytesPerValue) +
                                         " is < minPackedValue=" + new BytesRef(minPackedValue));
            }

            if (ByteArrayUtil.compareUnsigned(packedValue,
                                              packedValueOffset,
                                              maxPackedValue, 0,
                                              bytesPerValue) > 0)
            {
                throw new AssertionError("value=" + new BytesRef(packedValue, packedValueOffset, bytesPerValue) +
                                         " is > maxPackedValue=" + new BytesRef(maxPackedValue));
            }
        }

        private void valuesInOrderAndBounds(int count, byte[] minPackedValue, byte[] maxPackedValue, long[] rowIds)
        {
            byte[] lastPackedValue = new byte[bytesPerValue];
            long lastRowId = -1;
            for (int i = 0; i < count; i++)
            {
                valueInOrder(i, lastPackedValue, leafValues, i * bytesPerValue, rowIds[i], lastRowId);
                lastRowId = rowIds[i];

                // Make sure this value does in fact fall within this leaf cell:
                valueInBounds(leafValues, i * bytesPerValue, minPackedValue, maxPackedValue);
            }
        }

        private void valueInOrder(long ord, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset, long rowId, long lastRowId)
        {
            if (ord > 0)
            {
                int cmp = ByteArrayUtil.compareUnsigned(lastPackedValue, 0, packedValue, packedValueOffset, bytesPerValue);
                if (cmp > 0)
                {
                    throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) +
                                             " current value=" + new BytesRef(packedValue, packedValueOffset, bytesPerValue) +
                                             " ord=" + ord);
                }
                if (cmp == 0 && rowId < lastRowId)
                {
                    throw new AssertionError("row IDs out of order: last rowID=" + lastRowId + " current rowID=" + rowId + " ord=" + ord);
                }
            }
            System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, bytesPerValue);
        }
    }
}
