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
 * Base reader for a block balanced tree previously written with {@link BlockBalancedTreeWriter}.
 * <p>
 * Holds the index tree on heap and enables its traversal via {@link #traverse(TraversalCallback)}.
 */
public class BlockBalancedTreeWalker implements Closeable
{
    final FileHandle treeIndexFile;
    final int bytesPerValue;
    final int numLeaves;
    final byte[] minPackedValue;
    final byte[] maxPackedValue;
    // Packed array of byte[] holding all split values in the full binary tree:
    final byte[] packedIndex;
    final long pointCount;
    final int maxPointsInLeafNode;

    BlockBalancedTreeWalker(FileHandle treeIndexFile, long treeIndexRoot)
    {
        this.treeIndexFile = treeIndexFile;

        try (RandomAccessReader reader = treeIndexFile.createReader();
             IndexInputReader in = IndexInputReader.create(reader))
        {
            SAICodecUtils.validate(in);
            in.seek(treeIndexRoot);

            maxPointsInLeafNode = in.readVInt();
            bytesPerValue = in.readVInt();

            // Read index:
            numLeaves = in.readVInt();
            assert numLeaves > 0;

            minPackedValue = new byte[bytesPerValue];
            maxPackedValue = new byte[bytesPerValue];

            in.readBytes(minPackedValue, 0, bytesPerValue);
            in.readBytes(maxPackedValue, 0, bytesPerValue);

            if (FutureArrays.compareUnsigned(minPackedValue, 0, bytesPerValue, maxPackedValue, 0, bytesPerValue) > 0)
            {
                String message = String.format("Min packed value %s is > max packed value %s.",
                                               new BytesRef(minPackedValue), new BytesRef(maxPackedValue));
                throw new CorruptIndexException(message, in);
            }

            pointCount = in.readVLong();

            int numBytes = in.readVInt();
            packedIndex = new byte[numBytes];
            in.readBytes(packedIndex, 0, numBytes);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(treeIndexFile);
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
        FileUtils.closeQuietly(treeIndexFile);
    }

    void traverse(TraversalCallback callback)
    {
        traverse(callback, new PackedIndexTree(packedIndex, bytesPerValue, numLeaves), new IntArrayList());
    }

    private void traverse(TraversalCallback callback, PackedIndexTree index, IntArrayList pathToRoot)
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

    interface TraversalCallback
    {
        void onLeaf(int leafNodeID, long leafBlockFP, IntArrayList pathToRoot);
    }

    final static class PackedIndexTree
    {
        private final int bytesPerValue;
        private final int numLeaves;

        private final byte[][] splitPackedValueStack;
        // used to read the packed index byte[]
        private final ByteArrayDataInput in;
        // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
        private final long[] leafBlockFPStack;
        // holds the address, in the packed byte[] index, of the left-node of each level:
        private final int[] leftNodePositions;
        // holds the address, in the packed byte[] index, of the right-node of each level:
        private final int[] rightNodePositions;
        // true if the delta we read for the node at this level is a negative offset vs. the last split;
        // this will be true if the last time we split, we next pushed to the left subtree:
        private final boolean[] negativeDeltas;
        // holds the packed per-level split values; the run method uses this to save the cell min/max as it recurses:
        private final byte[][] splitValuesStack;

        private int nodeID;
        // level is 1-based so that we can do level-1 w/o checking each time:
        private int level;
        private boolean leafNode;

        PackedIndexTree(byte[] packedIndex, int bytesPerValue, int numLeaves)
        {
            this.bytesPerValue = bytesPerValue;
            this.numLeaves = numLeaves;
            int treeDepth = getTreeDepth();
            splitPackedValueStack = new byte[treeDepth + 1][];
            nodeID = 1;
            level = 1;
            splitPackedValueStack[level] = new byte[bytesPerValue];
            leafBlockFPStack = new long[treeDepth + 1];
            leftNodePositions = new int[treeDepth + 1];
            rightNodePositions = new int[treeDepth + 1];
            splitValuesStack = new byte[treeDepth + 1][];
            negativeDeltas = new boolean[treeDepth + 1];

            in = new ByteArrayDataInput(packedIndex);
            splitValuesStack[0] = new byte[bytesPerValue];
            readNodeData(false);
        }

        public void pushLeft()
        {
            int nodePosition = leftNodePositions[level];
            nodeID *= 2;
            level++;
            if (splitPackedValueStack[level] == null)
                splitPackedValueStack[level] = new byte[bytesPerValue];
            System.arraycopy(negativeDeltas, level - 1, negativeDeltas, level, 1);
            assert !leafNode;
            negativeDeltas[level] = true;
            in.setPosition(nodePosition);
            readNodeData(true);
        }

        public void pushRight()
        {
            int nodePosition = rightNodePositions[level];
            nodeID = nodeID * 2 + 1;
            level++;
            if (splitPackedValueStack[level] == null)
                splitPackedValueStack[level] = new byte[bytesPerValue];
            System.arraycopy(negativeDeltas, level - 1, negativeDeltas, level, 1);
            assert !leafNode;
            negativeDeltas[level] = false;
            in.setPosition(nodePosition);
            readNodeData(false);
        }

        public void pop()
        {
            nodeID /= 2;
            level--;
            leafNode = false;
        }

        public boolean isLeafNode()
        {
            return nodeID >= numLeaves;
        }

        public boolean nodeExists()
        {
            return nodeID - numLeaves < numLeaves;
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

        public long getLeafBlockFP()
        {
            assert isLeafNode() : "nodeID=" + nodeID + " is not a leaf";
            return leafBlockFPStack[level];
        }

        public byte[] getSplitValue()
        {
            assert !isLeafNode();
            return splitValuesStack[level];
        }

        private void readNodeData(boolean isLeft)
        {
            leafBlockFPStack[level] = leafBlockFPStack[level - 1];

            // read leaf block FP delta
            if (!isLeft)
                leafBlockFPStack[level] += in.readVLong();

            leafNode = isLeafNode();
            if (!leafNode)
            {
                // read prefix, firstDiffByteDelta encoded as int:
                int code = in.readVInt();
                int prefix = code % (1 + bytesPerValue);
                int suffix = bytesPerValue - prefix;

                if (splitValuesStack[level] == null)
                {
                    splitValuesStack[level] = new byte[bytesPerValue];
                }
                System.arraycopy(splitValuesStack[level - 1], 0, splitValuesStack[level], 0, bytesPerValue);
                if (suffix > 0)
                {
                    int firstDiffByteDelta = code / (1 + bytesPerValue);
                    if (negativeDeltas[level])
                        firstDiffByteDelta = -firstDiffByteDelta;
                    int oldByte = splitValuesStack[level][prefix] & 0xFF;
                    splitValuesStack[level][prefix] = (byte) (oldByte + firstDiffByteDelta);
                    in.readBytes(splitValuesStack[level], prefix + 1, suffix - 1);
                }

                int leftNumBytes = nodeID * 2 < numLeaves ? in.readVInt() : 0;

                leftNodePositions[level] = in.getPosition();
                rightNodePositions[level] = leftNodePositions[level] + leftNumBytes;
            }
        }

        private int getTreeDepth()
        {
            // First +1 because all the non-leave nodes makes another power
            // of 2; e.g. to have a fully balanced tree with 4 leaves you
            // need a depth=3 tree:

            // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
            // with 5 leaves you need a depth=4 tree:
            return MathUtil.log(numLeaves, 2) + 2;
        }
    }
}
