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

import java.util.function.IntFunction;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.NumericUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BlockBalancedTreeTest extends SAIRandomizedTester
{
    private ByteBuffersDataOutput dataOutput;

    @Before
    public void setupDataOutput()
    {
        dataOutput = new ByteBuffersDataOutput();
    }

    @Test
    public void testEmptyTree() throws Exception
    {
        long treeFilePointer = writeBalancedTree(0, 100, rowID -> rowID);

        assertEquals(-1, treeFilePointer);
    }

    @Test
    public void testSingleLeaf() throws Exception
    {
        try (BlockBalancedTreeWalker walker = generateBalancedTree(100, 100, rowID -> rowID))
        {
            assertEquals(1, walker.numLeaves);
            assertEquals(1, walker.treeDepth);
            assertEquals(100, walker.valueCount);

            BlockBalancedTreeWalker.TraversalState state = walker.newTraversalState();

            assertTrue(state.atLeafNode());

            recursiveAssertTraversal(state, -1);

            assertEquals(walker.treeDepth, state.maxLevel + 1);
        }
    }

    @Test
    public void testTreeWithSameValue() throws Exception
    {
        try (BlockBalancedTreeWalker walker = generateBalancedTree(100, 4, rowID -> 1))
        {
            BlockBalancedTreeWalker.TraversalState state = walker.newTraversalState();

            recursiveAssertTraversal(state, -1);

            assertEquals(walker.treeDepth, state.maxLevel + 1);
        }
    }

    @Test
    public void testTreeDepthNeverMoreThanNumberOfLeaves() throws Exception
    {
        int leafSize = 4;
        for (int numLeaves = 1; numLeaves < 1000; numLeaves++)
        {
            int numRows = leafSize * numLeaves;

            try (BlockBalancedTreeWalker walker = generateBalancedTree(numRows, leafSize, rowID -> rowID))
            {
                assertEquals(numLeaves, walker.numLeaves);
                assertTrue(walker.treeDepth <= walker.numLeaves);

                BlockBalancedTreeWalker.TraversalState state = walker.newTraversalState();

                recursiveAssertTraversal(state, -1);

                assertEquals(walker.treeDepth, state.maxLevel + 1);
            }
        }
    }

    @Test
    public void randomisedTreeTest() throws Exception
    {
        int loops = nextInt(10, 1000);

        for (int loop = 0; loop < loops; loop++)
        {
            int leafSize = nextInt(2, 512);
            int numRows = nextInt(1000, 10000);

            try (BlockBalancedTreeWalker walker = generateBalancedTree(numRows, leafSize, rowID -> nextInt(0, numRows / 2)))
            {
                BlockBalancedTreeWalker.TraversalState state = walker.newTraversalState();

                recursiveAssertTraversal(state, -1);

                assertEquals(walker.treeDepth, state.maxLevel + 1);
            }
        }
    }

    private long recursiveAssertTraversal(BlockBalancedTreeWalker.TraversalState state, long lastLeafBlockFP)
    {
        if (state.atLeafNode())
        {
            assertTrue(state.nodeExists());
            assertTrue(state.getLeafBlockFP() > lastLeafBlockFP);
            return state.getLeafBlockFP();
        }
        else
        {
            state.pushLeft();
            lastLeafBlockFP = recursiveAssertTraversal(state, lastLeafBlockFP);
            state.pop();

            state.pushRight();
            lastLeafBlockFP = recursiveAssertTraversal(state, lastLeafBlockFP);
            state.pop();

            return lastLeafBlockFP;
        }
    }

    private BlockBalancedTreeWalker generateBalancedTree(int numRows, int leafSize, IntFunction<Integer> valueProvider) throws Exception
    {
        long treeOffset = writeBalancedTree(numRows, leafSize, valueProvider);

        DataInput input = dataOutput.toDataInput();

        return new BlockBalancedTreeWalker(input, treeOffset);
    }

    private long writeBalancedTree(int numRows, int leafSize, IntFunction<Integer> valueProvider) throws Exception
    {
        final BlockBalancedTreeRamBuffer buffer = new BlockBalancedTreeRamBuffer(Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int rowID = 0; rowID < numRows; rowID++)
        {
            NumericUtils.intToSortableBytes(valueProvider.apply(rowID), scratch, 0);
            buffer.add(rowID, scratch);
        }

        BlockBalancedTreeWriter writer = new BlockBalancedTreeWriter(4, leafSize);
        ByteBuffersIndexOutput output = new ByteBuffersIndexOutput(dataOutput, "test", "test");
        return writer.write(output, buffer.iterator(), (leafPostings, offset, count) -> {});
    }
}
