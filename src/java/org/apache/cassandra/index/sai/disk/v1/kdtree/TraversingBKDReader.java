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

import java.io.Closeable;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.MathUtil;

/**
 * Base reader for a block KD-tree previously written with {@link BKDWriter}.
 *
 * Holds index tree on heap and enables its traversal via {@link #traverse(IndexTreeTraversalCallback)}.
 */
public class TraversingBKDReader implements Closeable
{
    final FileHandle indexFile;
    final int bytesPerValue;
    final int numLeaves;
    final byte[] minPackedValue;
    final byte[] maxPackedValue;
    // Packed array of byte[] holding all split values in the full binary tree:
    final byte[] packedIndex;
    final long pointCount;
    final int leafNodeOffset;
    final int maxPointsInLeafNode;
    final int packedBytesLength;

    TraversingBKDReader(FileHandle indexFile, long root)
    {
        this.indexFile = indexFile;

        try (RandomAccessReader reader = indexFile.createReader();
             IndexInputReader in = IndexInputReader.create(reader))
        {
            SAICodecUtils.validate(in);
            in.seek(root);

            maxPointsInLeafNode = in.readVInt();
            bytesPerValue = in.readVInt();
            packedBytesLength = bytesPerValue;

            // Read index:
            numLeaves = in.readVInt();
            assert numLeaves > 0;
            leafNodeOffset = numLeaves;

            minPackedValue = new byte[packedBytesLength];
            maxPackedValue = new byte[packedBytesLength];

            in.readBytes(minPackedValue, 0, packedBytesLength);
            in.readBytes(maxPackedValue, 0, packedBytesLength);

            if (FutureArrays.compareUnsigned(minPackedValue, 0, bytesPerValue, maxPackedValue, 0, bytesPerValue) > 0)
            {
                String message = String.format("Min packed value %s is > max packed value %s.",
                                               new BytesRef(minPackedValue), new BytesRef(maxPackedValue));
                throw new CorruptIndexException(message, in);
            }

            pointCount = in.readVLong();

            // docCount, unused
            in.readVInt();

            int numBytes = in.readVInt();
            packedIndex = new byte[numBytes];
            in.readBytes(packedIndex, 0, numBytes);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFile);
            throw Throwables.unchecked(t);
        }
    }

    public long memoryUsage()
    {
        return ObjectSizes.sizeOfArray(packedIndex)
               + ObjectSizes.sizeOfArray(minPackedValue)
               + ObjectSizes.sizeOfArray(maxPackedValue);
    }

    @Override
    public void close()
    {
        indexFile.close();
    }

    void traverse(IndexTreeTraversalCallback callback)
    {
        traverse(callback, new PackedIndexTree(), new IntArrayList());
    }

    private void traverse(IndexTreeTraversalCallback callback, PackedIndexTree index, IntArrayList pathToRoot)
    {
        if (index.isLeafNode())
        {
            // In the unbalanced case it's possible the left most node only has one child:
            if (index.nodeExists())
            {
                callback.onLeaf(index.getNodeID(), index.getLeafBlockFP(), pathToRoot);
            }
        }
        else
        {
            int nodeID = index.getNodeID();
            IntArrayList currentPath = new IntArrayList();
            currentPath.addAll(pathToRoot);
            currentPath.add(nodeID);

            index.pushLeft();
            traverse(callback, index, currentPath);
            index.pop();

            index.pushRight();
            traverse(callback, index, currentPath);
            index.pop();
        }
    }

    /**
     * Copy of BKDReader#getTreeDepth()
     */
    private int getTreeDepth()
    {
        // First +1 because all the non-leave nodes makes another power
        // of 2; e.g. to have a fully balanced tree with 4 leaves you
        // need a depth=3 tree:

        // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
        // with 5 leaves you need a depth=4 tree:
        return MathUtil.log(numLeaves, 2) + 2;
    }

    interface IndexTreeTraversalCallback
    {
        void onLeaf(int leafNodeID, long leafBlockFP, IntArrayList pathToRoot);
    }

    final class PackedIndexTree
    {
        private final byte[][] splitPackedValueStack;

        private int nodeID;
        // level is 1-based so that we can do level-1 w/o checking each time:
        private int level;
        private int splitDim;

        // used to read the packed byte[]
        private final ByteArrayDataInput in;
        // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
        private final long[] leafBlockFPStack;
        // holds the address, in the packed byte[] index, of the left-node of each level:
        private final int[] leftNodePositions;
        // holds the address, in the packed byte[] index, of the right-node of each level:
        private final int[] rightNodePositions;
        // holds the splitDim for each level:
        private final int[] splitDims;
        // true if the per-dim delta we read for the node at this level is a negative offset vs. the last split on this dim; this is a packed
        // 2D array, i.e. to access array[level][dim] you read from negativeDeltas[level*numDims+dim].  this will be true if the last time we
        // split on this dimension, we next pushed to the left subtree:
        private final boolean[] negativeDeltas;
        // holds the packed per-level split values; the run method uses this to save the cell min/max as it recurses:
        private final byte[][] splitValuesStack;
        // scratch value to return from getPackedValue:
        private final BytesRef scratch;

        PackedIndexTree()
        {
            int treeDepth = getTreeDepth();
            splitPackedValueStack = new byte[treeDepth + 1][];
            nodeID = 1;
            level = 1;
            splitPackedValueStack[level] = new byte[packedBytesLength];
            leafBlockFPStack = new long[treeDepth + 1];
            leftNodePositions = new int[treeDepth + 1];
            rightNodePositions = new int[treeDepth + 1];
            splitValuesStack = new byte[treeDepth + 1][];
            splitDims = new int[treeDepth + 1];
            negativeDeltas = new boolean[treeDepth + 1];

            in = new ByteArrayDataInput(packedIndex);
            splitValuesStack[0] = new byte[packedBytesLength];
            readNodeData(false);
            scratch = new BytesRef();
            scratch.length = bytesPerValue;
        }

        public void pushLeft()
        {
            int nodePosition = leftNodePositions[level];
            nodeID *= 2;
            level++;
            if (splitPackedValueStack[level] == null)
                splitPackedValueStack[level] = new byte[packedBytesLength];
            System.arraycopy(negativeDeltas, level - 1, negativeDeltas, level, 1);
            assert splitDim != -1;
            negativeDeltas[level + splitDim] = true;
            in.setPosition(nodePosition);
            readNodeData(true);
        }

        public void pushRight()
        {
            int nodePosition = rightNodePositions[level];
            nodeID = nodeID * 2 + 1;
            level++;
            if (splitPackedValueStack[level] == null)
                splitPackedValueStack[level] = new byte[packedBytesLength];
            System.arraycopy(negativeDeltas, level - 1, negativeDeltas, level, 1);
            assert splitDim != -1;
            negativeDeltas[level + splitDim] = false;
            in.setPosition(nodePosition);
            readNodeData(false);
        }

        public void pop()
        {
            nodeID /= 2;
            level--;
            splitDim = splitDims[level];
        }

        public boolean isLeafNode()
        {
            return nodeID >= leafNodeOffset;
        }

        public boolean nodeExists()
        {
            return nodeID - leafNodeOffset < leafNodeOffset;
        }

        public int getNodeID()
        {
            return nodeID;
        }

        public byte[] getSplitPackedValue()
        {
            assert !isLeafNode();
            assert splitPackedValueStack[level] != null : "level=" + level;
            return splitPackedValueStack[level];
        }

        /** Only valid after pushLeft or pushRight, not pop! */
        public int getSplitDim()
        {
            assert !isLeafNode();
            return splitDim;
        }

        public long getLeafBlockFP()
        {
            assert isLeafNode() : "nodeID=" + nodeID + " is not a leaf";
            return leafBlockFPStack[level];
        }

        public BytesRef getSplitDimValue()
        {
            assert !isLeafNode();
            scratch.bytes = splitValuesStack[level];
            scratch.offset = splitDim * bytesPerValue;
            return scratch;
        }

        private void readNodeData(boolean isLeft)
        {
            leafBlockFPStack[level] = leafBlockFPStack[level - 1];

            // read leaf block FP delta
            if (!isLeft)
                leafBlockFPStack[level] += in.readVLong();

            if (isLeafNode())
                splitDim = -1;
            else
            {
                // read split dim, prefix, firstDiffByteDelta encoded as int:
                int code = in.readVInt();
                splitDim = 0;
                splitDims[level] = splitDim;
                int prefix = code % (1 + bytesPerValue);
                int suffix = bytesPerValue - prefix;

                if (splitValuesStack[level] == null)
                {
                    splitValuesStack[level] = new byte[packedBytesLength];
                }
                System.arraycopy(splitValuesStack[level - 1], 0, splitValuesStack[level], 0, packedBytesLength);
                if (suffix > 0)
                {
                    int firstDiffByteDelta = code / (1 + bytesPerValue);
                    if (negativeDeltas[level + splitDim])
                        firstDiffByteDelta = -firstDiffByteDelta;
                    int oldByte = splitValuesStack[level][splitDim * bytesPerValue + prefix] & 0xFF;
                    splitValuesStack[level][splitDim * bytesPerValue + prefix] = (byte) (oldByte + firstDiffByteDelta);
                    in.readBytes(splitValuesStack[level], splitDim * bytesPerValue + prefix + 1, suffix - 1);
                }

                int leftNumBytes = nodeID * 2 < leafNodeOffset ? in.readVInt() : 0;

                leftNodePositions[level] = in.getPosition();
                rightNodePositions[level] = leftNodePositions[level] + leftNumBytes;
            }
        }
    }
}
