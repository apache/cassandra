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
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

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
    final int treeDepth;
    final byte[] minPackedValue;
    final byte[] maxPackedValue;
    final long valueCount;
    final int maxValuesInLeafNode;
    final byte[] packedIndex;
    final long memoryUsage;

    BlockBalancedTreeWalker(FileHandle treeIndexFile, long treeIndexRoot)
    {
        this.treeIndexFile = treeIndexFile;

        try (RandomAccessReader reader = treeIndexFile.createReader();
             IndexInput indexInput = IndexInputReader.create(reader))
        {
            SAICodecUtils.validate(indexInput);
            indexInput.seek(treeIndexRoot);

            maxValuesInLeafNode = indexInput.readVInt();
            bytesPerValue = indexInput.readVInt();

            // Read index:
            numLeaves = indexInput.readVInt();
            assert numLeaves > 0;
            treeDepth = indexInput.readVInt();
            minPackedValue = new byte[bytesPerValue];
            maxPackedValue = new byte[bytesPerValue];

            indexInput.readBytes(minPackedValue, 0, bytesPerValue);
            indexInput.readBytes(maxPackedValue, 0, bytesPerValue);

            if (ByteArrayUtil.compareUnsigned(minPackedValue, 0, maxPackedValue, 0, bytesPerValue) > 0)
            {
                String message = String.format("Min packed value %s is > max packed value %s.",
                                               new BytesRef(minPackedValue), new BytesRef(maxPackedValue));
                throw new CorruptIndexException(message, indexInput);
            }

            valueCount = indexInput.readVLong();

            int numBytes = indexInput.readVInt();
            packedIndex = new byte[numBytes];
            indexInput.readBytes(packedIndex, 0, numBytes);

            memoryUsage = ObjectSizes.sizeOfArray(packedIndex) +
                          ObjectSizes.sizeOfArray(minPackedValue) +
                          ObjectSizes.sizeOfArray(maxPackedValue);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(treeIndexFile);
            throw Throwables.unchecked(t);
        }
    }

    @VisibleForTesting
    public BlockBalancedTreeWalker(DataInput indexInput, long treeIndexRoot) throws IOException
    {
        treeIndexFile = null;

        indexInput.skipBytes(treeIndexRoot);

        maxValuesInLeafNode = indexInput.readVInt();
        bytesPerValue = indexInput.readVInt();

        // Read index:
        numLeaves = indexInput.readVInt();
        assert numLeaves > 0;
        treeDepth = indexInput.readVInt();
        minPackedValue = new byte[bytesPerValue];
        maxPackedValue = new byte[bytesPerValue];

        indexInput.readBytes(minPackedValue, 0, bytesPerValue);
        indexInput.readBytes(maxPackedValue, 0, bytesPerValue);

        if (ByteArrayUtil.compareUnsigned(minPackedValue, 0, maxPackedValue, 0, bytesPerValue) > 0)
        {
            String message = String.format("Min packed value %s is > max packed value %s.",
                                           new BytesRef(minPackedValue), new BytesRef(maxPackedValue));
            throw new CorruptIndexException(message, indexInput);
        }

        valueCount = indexInput.readVLong();

        int numBytes = indexInput.readVInt();
        packedIndex = new byte[numBytes];
        indexInput.readBytes(packedIndex, 0, numBytes);

        memoryUsage = ObjectSizes.sizeOfArray(packedIndex) +
                      ObjectSizes.sizeOfArray(minPackedValue) +
                      ObjectSizes.sizeOfArray(maxPackedValue);
    }

    public long memoryUsage()
    {
        return memoryUsage;
    }

    public TraversalState newTraversalState()
    {
        return new TraversalState();
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(treeIndexFile);
    }

    void traverse(TraversalCallback callback)
    {
        traverse(newTraversalState(), callback, new IntArrayList());
    }

    private void traverse(TraversalState state, TraversalCallback callback, IntArrayList pathToRoot)
    {
        if (state.atLeafNode())
        {
            // In the unbalanced case it's possible the left most node only has one child:
            if (state.nodeExists())
            {
                callback.onLeaf(state.nodeID, state.getLeafBlockFP(), pathToRoot);
            }
        }
        else
        {
            IntArrayList currentPath = new IntArrayList();
            currentPath.addAll(pathToRoot);
            currentPath.add(state.nodeID);

            state.pushLeft();
            traverse(state, callback, currentPath);
            state.pop();

            state.pushRight();
            traverse(state, callback, currentPath);
            state.pop();
        }
    }

    interface TraversalCallback
    {
        void onLeaf(int leafNodeID, long leafBlockFP, IntArrayList pathToRoot);
    }

    /**
     * This maintains the state for a traversal of the packed index. It is loaded once and can be resused
     * by calling the reset method.
     * <p>
     * The packed index is a packed representation of a balanced tree and takes the form of a packed array of
     * file pointer / split value pairs. Both the file pointers and split values are prefix compressed by tree level
     * requiring us to maintain a stack of values for each level in the tree. The stack size is always the tree depth.
     * <p>
     * The tree is traversed by recursively following the left and then right subtrees under the current node. For the
     * following tree (split values in square brackets):
     * <pre>
     *        1[16]
     *       / \
     *      /   \
     *     2[8]  3[24]
     *    / \   / \
     *   4   5 6   7
     * </pre>
     * The traversal will be 1 -> 2 -> 4 -> 5 -> 3 -> 6 -> 7 with nodes 4, 5, 6 & 7 being leaf nodes.
     * <p>
     * Assuming the full range of values in the tree is 0 -> 32, the non-leaf nodes will represent the following
     * values:
     * <pre>
     *         1[0-32]
     *        /      \
     *    2[0-16]   3[16-32]
     * </pre>
     */
    @NotThreadSafe
    final class TraversalState
    {
        // used to read the packed index byte[]
        final ByteArrayDataInput dataInput;
        // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
        final long[] leafBlockFPStack;
        // holds the address, in the packed byte[] index, of the left-node of each level:
        final int[] leftNodePositions;
        // holds the address, in the packed byte[] index, of the right-node of each level:
        final int[] rightNodePositions;
        // holds the packed per-level split values; the run method uses this to save the cell min/max as it recurses:
        final byte[][] splitValuesStack;

        int nodeID;
        int level;
        @VisibleForTesting
        int maxLevel;

        private TraversalState()
        {
            nodeID = 1;
            level = 0;
            leafBlockFPStack = new long[treeDepth];
            leftNodePositions = new int[treeDepth];
            rightNodePositions = new int[treeDepth];
            splitValuesStack = new byte[treeDepth][];
            this.dataInput = new ByteArrayDataInput(packedIndex);
            readNodeData(false);
        }

        public void pushLeft()
        {
            int nodePosition = leftNodePositions[level];
            nodeID *= 2;
            level++;
            maxLevel = Math.max(maxLevel, level);
            dataInput.setPosition(nodePosition);
            readNodeData(true);
        }

        public void pushRight()
        {
            int nodePosition = rightNodePositions[level];
            nodeID = nodeID * 2 + 1;
            level++;
            maxLevel = Math.max(maxLevel, level);
            dataInput.setPosition(nodePosition);
            readNodeData(false);
        }

        public void pop()
        {
            nodeID /= 2;
            level--;
        }

        public boolean atLeafNode()
        {
            return nodeID >= numLeaves;
        }

        public boolean nodeExists()
        {
            return nodeID - numLeaves < numLeaves;
        }

        public long getLeafBlockFP()
        {
            return leafBlockFPStack[level];
        }

        public byte[] getSplitValue()
        {
            assert !atLeafNode();
            return splitValuesStack[level];
        }

        private void readNodeData(boolean isLeft)
        {
            leafBlockFPStack[level] = level == 0 ? 0 : leafBlockFPStack[level - 1];

            // read leaf block FP delta
            if (!isLeft)
                leafBlockFPStack[level] += dataInput.readVLong();

            if (!atLeafNode())
            {
                // read prefix, firstDiffByteDelta encoded as int:
                int code = dataInput.readVInt();
                int prefix = code % (1 + bytesPerValue);
                int suffix = bytesPerValue - prefix;

                pushSplitValueStack();
                if (suffix > 0)
                {
                    int firstDiffByteDelta = code / (1 + bytesPerValue);
                    // If we are pushing to the left subtree then the delta will be negative
                    if (isLeft)
                        firstDiffByteDelta = -firstDiffByteDelta;
                    int oldByte = splitValuesStack[level][prefix] & 0xFF;
                    splitValuesStack[level][prefix] = (byte) (oldByte + firstDiffByteDelta);
                    dataInput.readBytes(splitValuesStack[level], prefix + 1, suffix - 1);
                }

                int leftNumBytes = nodeID * 2 < numLeaves ? dataInput.readVInt() : 0;

                leftNodePositions[level] = dataInput.getPosition();
                rightNodePositions[level] = leftNodePositions[level] + leftNumBytes;
            }
        }

        private void pushSplitValueStack()
        {
            if (splitValuesStack[level] == null)
                splitValuesStack[level] = new byte[bytesPerValue];
            if (level == 0)
                Arrays.fill(splitValuesStack[level], (byte) 0);
            else
                System.arraycopy(splitValuesStack[level - 1], 0, splitValuesStack[level], 0, bytesPerValue);
        }
    }
}
