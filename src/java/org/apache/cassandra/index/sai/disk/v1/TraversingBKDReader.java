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

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
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
 * Holds index tree on heap and enables it's traversal via {@link #traverse(IndexTreeTraversalCallback)}.
 */
public class TraversingBKDReader implements Closeable
{
    final IndexComponents indexComponents;
    final FileHandle indexFile;
    final int bytesPerDim;
    final int numLeaves;
    final byte[] minPackedValue;
    final byte[] maxPackedValue;
    // Packed array of byte[] holding all split values in the full binary tree:
    final byte[] packedIndex;
    final long pointCount;
    final int leafNodeOffset;
    final int numDims;
    final int maxPointsInLeafNode;
    final int packedBytesLength;

    @SuppressWarnings("resource")
    TraversingBKDReader(IndexComponents indexComponents, FileHandle indexFile, long root)
    {
        this.indexComponents = indexComponents;
        this.indexFile = indexFile;

        try (final RandomAccessReader reader = indexFile.createReader())
        {
            final IndexInputReader in = IndexInputReader.create(reader);
            SAICodecUtils.validate(in);
            in.seek(root);

            numDims = in.readVInt();
            maxPointsInLeafNode = in.readVInt();
            bytesPerDim = in.readVInt();
            packedBytesLength = numDims * bytesPerDim;

            // Read index:
            numLeaves = in.readVInt();
            assert numLeaves > 0;
            leafNodeOffset = numLeaves;

            minPackedValue = new byte[packedBytesLength];
            maxPackedValue = new byte[packedBytesLength];

//            if (indexComponents.getEncryptionCompressor() != null)
//            {
//                IndexInput cryptoInput = CryptoUtils.uncompress(in, indexComponents.getEncryptionCompressor());
//                cryptoInput.readBytes(minPackedValue, 0, packedBytesLength);
//                cryptoInput.readBytes(maxPackedValue, 0, packedBytesLength);
//            }
//            else
//            {
                in.readBytes(minPackedValue, 0, packedBytesLength);
                in.readBytes(maxPackedValue, 0, packedBytesLength);
//            }

            for (int dim = 0; dim < numDims; dim++)
            {
                if (FutureArrays.compareUnsigned(minPackedValue, dim * bytesPerDim, dim * bytesPerDim + bytesPerDim, maxPackedValue, dim * bytesPerDim, dim * bytesPerDim + bytesPerDim) > 0)
                {
                    String message = String.format("Min packed value %s is > max packed value %s for dimension %d.",
                                                   new BytesRef(minPackedValue), new BytesRef(maxPackedValue), dim);
                    throw new CorruptIndexException(message, in);
                }
            }

            pointCount = in.readVLong();

            // docCount, unused
            in.readVInt();

//            ICompressor compressor = indexComponents.getEncryptionCompressor();
//            if (compressor != null)
//            {
//                // TODO: there's extra byte[] allocation here
//                IndexInput input = CryptoUtils.uncompress(in, compressor);
//
//                packedIndex = new byte[(int)input.length()];
//                input.readBytes(packedIndex, 0, (int)input.length());
//            }
//            else
//            {
                int numBytes = in.readVInt();
                packedIndex = new byte[numBytes];
                in.readBytes(packedIndex, 0, numBytes);
//            }
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFile);
            throw Throwables.unchecked(t);
        }
    }

    public long getMinLeafBlockFP()
    {
        if (packedIndex != null)
        {
            return new ByteArrayDataInput(packedIndex).readVLong();
        }
        else
        {
            throw new IllegalStateException();
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

    interface IndexTreeTraversalCallback
    {
        void onLeaf(int leafNodeID, long leafBlockFP, IntArrayList pathToRoot);
    }

    /**
     * Copy of BKDReader.IndexTree
     */
    abstract class IndexTree implements Cloneable
    {
        protected int nodeID;
        // level is 1-based so that we can do level-1 w/o checking each time:
        protected int level;
        protected int splitDim;
        protected final byte[][] splitPackedValueStack;

        protected IndexTree()
        {
            int treeDepth = getTreeDepth();
            splitPackedValueStack = new byte[treeDepth + 1][];
            nodeID = 1;
            level = 1;
            splitPackedValueStack[level] = new byte[packedBytesLength];
        }

        public void pushLeft()
        {
            nodeID *= 2;
            level++;
            if (splitPackedValueStack[level] == null)
            {
                splitPackedValueStack[level] = new byte[packedBytesLength];
            }
        }

        /** Clone, but you are not allowed to pop up past the point where the clone happened. */
        public abstract IndexTree clone();

        public void pushRight()
        {
            nodeID = nodeID * 2 + 1;
            level++;
            if (splitPackedValueStack[level] == null)
            {
                splitPackedValueStack[level] = new byte[packedBytesLength];
            }
        }

        public void pop()
        {
            nodeID /= 2;
            level--;
            splitDim = -1;
            //System.out.println("  pop nodeID=" + nodeID);
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

        /** Only valid after pushLeft or pushRight, not pop! */
        public abstract BytesRef getSplitDimValue();

        /** Only valid after pushLeft or pushRight, not pop! */
        public abstract long getLeafBlockFP();
    }


    /**
     * Copy of BKDReader.PackedIndexTree
     */
    final class PackedIndexTree extends IndexTree
    {
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
        // split on this dimension, we next pushed to the left sub-tree:
        private final boolean[] negativeDeltas;
        // holds the packed per-level split values; the run method uses this to save the cell min/max as it recurses:
        private final byte[][] splitValuesStack;
        // scratch value to return from getPackedValue:
        private final BytesRef scratch;

        PackedIndexTree()
        {
            int treeDepth = getTreeDepth();
            leafBlockFPStack = new long[treeDepth + 1];
            leftNodePositions = new int[treeDepth + 1];
            rightNodePositions = new int[treeDepth + 1];
            splitValuesStack = new byte[treeDepth + 1][];
            splitDims = new int[treeDepth + 1];
            negativeDeltas = new boolean[numDims * (treeDepth + 1)];

            in = new ByteArrayDataInput(packedIndex);
            splitValuesStack[0] = new byte[packedBytesLength];
            readNodeData(false);
            scratch = new BytesRef();
            scratch.length = bytesPerDim;
        }

        @Override
        public PackedIndexTree clone()
        {
            PackedIndexTree index = new PackedIndexTree();
            index.nodeID = nodeID;
            index.level = level;
            index.splitDim = splitDim;
            index.leafBlockFPStack[level] = leafBlockFPStack[level];
            index.leftNodePositions[level] = leftNodePositions[level];
            index.rightNodePositions[level] = rightNodePositions[level];
            index.splitValuesStack[index.level] = splitValuesStack[index.level].clone();
            System.arraycopy(negativeDeltas, level * numDims, index.negativeDeltas, level * numDims, numDims);
            index.splitDims[level] = splitDims[level];
            return index;
        }

        @Override
        public void pushLeft()
        {
            int nodePosition = leftNodePositions[level];
            super.pushLeft();
            System.arraycopy(negativeDeltas, (level - 1) * numDims, negativeDeltas, level * numDims, numDims);
            assert splitDim != -1;
            negativeDeltas[level * numDims + splitDim] = true;
            in.setPosition(nodePosition);
            readNodeData(true);
        }

        @Override
        public void pushRight()
        {
            int nodePosition = rightNodePositions[level];
            super.pushRight();
            System.arraycopy(negativeDeltas, (level - 1) * numDims, negativeDeltas, level * numDims, numDims);
            assert splitDim != -1;
            negativeDeltas[level * numDims + splitDim] = false;
            in.setPosition(nodePosition);
            readNodeData(false);
        }

        @Override
        public void pop()
        {
            super.pop();
            splitDim = splitDims[level];
        }

        @Override
        public long getLeafBlockFP()
        {
            assert isLeafNode() : "nodeID=" + nodeID + " is not a leaf";
            return leafBlockFPStack[level];
        }

        @Override
        public BytesRef getSplitDimValue()
        {
            assert !isLeafNode();
            scratch.bytes = splitValuesStack[level];
            scratch.offset = splitDim * bytesPerDim;
            return scratch;
        }

        private void readNodeData(boolean isLeft)
        {

            leafBlockFPStack[level] = leafBlockFPStack[level - 1];

            // read leaf block FP delta
            if (!isLeft)
            {
                leafBlockFPStack[level] += in.readVLong();
            }

            if (isLeafNode())
            {
                splitDim = -1;
            }
            else
            {

                // read split dim, prefix, firstDiffByteDelta encoded as int:
                int code = in.readVInt();
                splitDim = code % numDims;
                splitDims[level] = splitDim;
                code /= numDims;
                int prefix = code % (1 + bytesPerDim);
                int suffix = bytesPerDim - prefix;

                if (splitValuesStack[level] == null)
                {
                    splitValuesStack[level] = new byte[packedBytesLength];
                }
                System.arraycopy(splitValuesStack[level - 1], 0, splitValuesStack[level], 0, packedBytesLength);
                if (suffix > 0)
                {
                    int firstDiffByteDelta = code / (1 + bytesPerDim);
                    if (negativeDeltas[level * numDims + splitDim])
                    {
                        firstDiffByteDelta = -firstDiffByteDelta;
                    }
                    int oldByte = splitValuesStack[level][splitDim * bytesPerDim + prefix] & 0xFF;
                    splitValuesStack[level][splitDim * bytesPerDim + prefix] = (byte) (oldByte + firstDiffByteDelta);
                    in.readBytes(splitValuesStack[level], splitDim * bytesPerDim + prefix + 1, suffix - 1);
                }
                else
                {
                    // our split value is == last split value in this dim, which can happen when there are many duplicate values
                }

                int leftNumBytes;
                if (nodeID * 2 < leafNodeOffset)
                {
                    leftNumBytes = in.readVInt();
                }
                else
                {
                    leftNumBytes = 0;
                }

                leftNodePositions[level] = in.getPosition();
                rightNodePositions[level] = leftNodePositions[level] + leftNumBytes;
            }
        }
    }


    void traverse(IndexTreeTraversalCallback callback)
    {
        traverse(callback,
                 new PackedIndexTree(),
                 new IntArrayList());
    }

    private void traverse(IndexTreeTraversalCallback callback,
                          IndexTree index,
                          IntArrayList pathToRoot)
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
            final int nodeID = index.getNodeID();
            final IntArrayList currentPath = new IntArrayList();
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
}
