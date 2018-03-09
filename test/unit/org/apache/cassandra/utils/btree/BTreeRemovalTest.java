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
package org.apache.cassandra.utils.btree;

import static org.apache.cassandra.utils.btree.BTreeRemoval.remove;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.google.common.collect.Iterables;

public class BTreeRemovalTest
{
    static
    {
        System.setProperty("cassandra.btree.fanfactor", "8");
    }

    private static final Comparator<Integer> CMP = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    private static Object[] copy(final Object[] btree)
    {
        final Object[] result = new Object[btree.length];
        System.arraycopy(btree, 0, result, 0, btree.length);
        if (!BTree.isLeaf(btree))
        {
            for (int i = BTree.getChildStart(btree); i < BTree.getChildEnd(btree); ++i)
                result[i] = copy((Object[]) btree[i]);
            final int[] sizeMap = BTree.getSizeMap(btree);
            final int[] resultSizeMap = new int[sizeMap.length];
            System.arraycopy(sizeMap, 0, resultSizeMap, 0, sizeMap.length);
            result[result.length - 1] = resultSizeMap;
        }
        return result;
    }

    private static Object[] assertRemove(final Object[] btree, final int key)
    {
        final Object[] btreeBeforeRemoval = copy(btree);
        final Object[] result = remove(btree, CMP, key);
        assertBTree(btreeBeforeRemoval, btree);
        assertTrue(BTree.isWellFormed(result, CMP));
        assertEquals(BTree.size(btree) - 1, BTree.size(result));
        assertNull(BTree.find(result, CMP, key));

        for (Integer k : BTree.<Integer>iterable(btree))
            if (k != key)
                assertNotNull(BTree.find(result, CMP, k));

        return result;
    }

    private static void assertBTree(final Object[] expected, final Object[] result)
    {
        assertEquals(BTree.isEmpty(expected), BTree.isEmpty(result));
        assertEquals(BTree.isLeaf(expected), BTree.isLeaf(result));
        assertEquals(expected.length, result.length);
        if (BTree.isLeaf(expected))
        {
            assertArrayEquals(expected, result);
        }
        else
        {
            for (int i = 0; i < BTree.getBranchKeyEnd(expected); ++i)
                assertEquals(expected[i], result[i]);
            for (int i = BTree.getChildStart(expected); i < BTree.getChildEnd(expected); ++i)
                assertBTree((Object[]) expected[i], (Object[]) result[i]);
            assertArrayEquals(BTree.getSizeMap(expected), BTree.getSizeMap(result));
        }
    }

    private static Object[] generateLeaf(int from, int size)
    {
        final Object[] result = new Object[(size & 1) == 1 ? size : size + 1];
        for (int i = 0; i < size; ++i)
            result[i] = from + i;
        return result;
    }

    private static Object[] generateBranch(int[] keys, Object[][] children)
    {
        assert keys.length > 0;
        assert children.length > 1;
        assert children.length == keys.length + 1;
        final Object[] result = new Object[keys.length + children.length + 1];
        for (int i = 0; i < keys.length; ++i)
            result[i] = keys[i];
        for (int i = 0; i < children.length; ++i)
            result[keys.length + i] = children[i];
        final int[] sizeMap = new int[children.length];
        sizeMap[0] = BTree.size(children[0]);
        for (int i = 1; i < children.length; ++i)
            sizeMap[i] = sizeMap[i - 1] + BTree.size(children[i]) + 1;
        result[result.length - 1] = sizeMap;
        return result;
    }

    private static Object[] generateSampleTwoLevelsTree(final int[] leafSizes)
    {
        assert leafSizes.length > 1;
        final Object[][] leaves = new Object[leafSizes.length][];
        for (int i = 0; i < leaves.length; ++i)
            leaves[i] = generateLeaf(10 * i + 1, leafSizes[i]);
        final int[] keys = new int[leafSizes.length - 1];
        for (int i = 0; i < keys.length; ++i)
            keys[i] = 10 * (i + 1);
        final Object[] btree = generateBranch(keys, leaves);
        assertTrue(BTree.isWellFormed(btree, CMP));
        return btree;
    }

    private static Object[] generateSampleThreeLevelsTree(final int[] middleNodeSizes)
    {
        assert middleNodeSizes.length > 1;
        final Object[][] middleNodes = new Object[middleNodeSizes.length][];
        for (int i = 0; i < middleNodes.length; ++i)
        {
            final Object[][] leaves = new Object[middleNodeSizes[i]][];
            for (int j = 0; j < middleNodeSizes[i]; ++j)
                leaves[j] = generateLeaf(100 * i + 10 * j + 1, 4);
            final int[] keys = new int[middleNodeSizes[i] - 1];
            for (int j = 0; j < keys.length; ++j)
                keys[j] = 100 * i + 10 * (j + 1);
            middleNodes[i] = generateBranch(keys, leaves);
        }
        final int[] keys = new int[middleNodeSizes.length - 1];
        for (int i = 0; i < keys.length; ++i)
            keys[i] = 100 * (i + 1);
        final Object[] btree = generateBranch(keys, middleNodes);
        assertTrue(BTree.isWellFormed(btree, CMP));
        return btree;
    }

    @Test
    public void testRemoveFromEmpty()
    {
        assertBTree(BTree.empty(), remove(BTree.empty(), CMP, 1));
    }

    @Test
    public void testRemoveNonexistingElement()
    {
        final Object[] btree = new Object[] {1, 2, 3, 4, null};
        assertBTree(btree, remove(btree, CMP, 5));
    }

    @Test
    public void testRemoveLastElement()
    {
        final Object[] btree = new Object[] {1};
        assertBTree(BTree.empty(), remove(btree, CMP, 1));
    }

    @Test
    public void testRemoveFromRootWhichIsALeaf()
    {
        for (int size = 1; size < 9; ++size)
        {
            final Object[] btree = new Object[(size & 1) == 1 ? size : size + 1];
            for (int i = 0; i < size; ++i)
                btree[i] = i + 1;
            for (int i = 0; i < size; ++i)
            {
                final Object[] result = remove(btree, CMP, i + 1);
                assertTrue("size " + size, BTree.isWellFormed(result, CMP));
                for (int j = 0; j < i; ++j)
                    assertEquals("size " + size + "elem " + j, btree[j], result[j]);
                for (int j = i; j < size - 1; ++j)
                    assertEquals("size " + size + "elem " + j, btree[j + 1], result[j]);
                for (int j = size - 1; j < result.length; ++j)
                    assertNull("size " + size + "elem " + j, result[j]);
            }

            {
                final Object[] result = remove(btree, CMP, 0);
                assertTrue("size " + size, BTree.isWellFormed(result, CMP));
                assertBTree(btree, result);
            }

            {
                final Object[] result = remove(btree, CMP, size + 1);
                assertTrue("size " + size, BTree.isWellFormed(result, CMP));
                assertBTree(btree, result);
            }
        }
    }

    @Test
    public void testRemoveFromNonMinimalLeaf()
    {
        for (int size = 5; size < 9; ++size)
        {
            final Object[] btree = generateSampleTwoLevelsTree(new int[] {size, 4, 4, 4, 4});

            for (int i = 1; i < size + 1; ++i)
                assertRemove(btree, i);
        }
    }

    @Test
    public void testRemoveFromMinimalLeafRotateLeft()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 5, 5, 5, 5});

        for (int i = 11; i < 15; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafRotateRight1()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 5, 5, 5, 5});

        for (int i = 1; i < 5; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafRotateRight2()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 4, 5, 5, 5});

        for (int i = 11; i < 15; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafMergeWithLeft1()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 4, 4, 4, 4});

        for (int i = 11; i < 15; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafMergeWithLeft2()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 4, 4, 4, 4});

        for (int i = 41; i < 45; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafMergeWithRight()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 4, 4, 4, 4});

        for (int i = 1; i < 5; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWhenSingleKeyRootMergeWithLeft()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 4});

        for (int i = 1; i < 5; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWhenSingleKeyRootMergeWithRight()
    {
        final Object[] btree = generateSampleTwoLevelsTree(new int[] {4, 4});

        for (int i = 11; i < 15; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWithBranchLeftRotation()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {6, 5, 5, 5, 5});
        for (int i = 101; i < 105; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWithBranchRightRotation1()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {5, 6, 5, 5, 5});
        for (int i = 1; i < 5; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWithBranchRightRotation2()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {5, 5, 6, 5, 5});
        for (int i = 101; i < 105; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWithBranchMergeWithLeft1()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {5, 5, 5, 5, 5});
        for (int i = 101; i < 105; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWithBranchMergeWithLeft2()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {5, 5, 5, 5, 5});
        for (int i = 401; i < 405; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMinimalLeafWithBranchMergeWithRight()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {5, 5, 5, 5, 5});
        for (int i = 1; i < 5; ++i)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromMiddleBranch()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {5, 5, 5, 5, 5});
        for (int i = 10; i < 50; i += 10)
            assertRemove(btree, i);
    }

    @Test
    public void testRemoveFromRootBranch()
    {
        final Object[] btree = generateSampleThreeLevelsTree(new int[] {5, 5, 5, 5, 5});
        for (int i = 100; i < 500; i += 100)
            assertRemove(btree, i);
    }

    @Test
    public void randomizedTest()
    {
        Random rand = new Random(2);
        SortedSet<Integer> data = new TreeSet<>();
        for (int i = 0; i < 1000; ++i)
            data.add(rand.nextInt());
        Object[] btree = BTree.build(data, UpdateFunction.<Integer>noOp());

        assertTrue(BTree.isWellFormed(btree, CMP));
        assertTrue(Iterables.elementsEqual(data, BTree.iterable(btree)));
        while (btree != BTree.empty())
        {
            int idx = rand.nextInt(BTree.size(btree));
            Integer val = BTree.findByIndex(btree, idx);
            assertTrue(data.remove(val));
            btree = assertRemove(btree, val);
        }
    }
}
