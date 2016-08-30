/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.btree;

import java.util.Arrays;
import java.util.Comparator;

public class BTreeRemoval
{
    /**
     * Remove |elem| from |btree|. If it's not present then return |btree| itself.
     */
    public static <V> Object[] remove(final Object[] btree, final Comparator<? super V> comparator, final V elem)
    {
        if (BTree.isEmpty(btree))
            return btree;
        int index = -1;
        V elemToSwap = null;
        int lb = 0;
        Object[] node = btree;
        while (true)
        {
            int keyEnd = BTree.getKeyEnd(node);
            int i = Arrays.binarySearch((V[]) node, 0, keyEnd, elem, comparator);

            if (i >= 0)
            {
                if (BTree.isLeaf(node))
                    index = lb + i;
                else
                {
                    final int indexInNode = BTree.getSizeMap(node)[i];
                    index = lb + indexInNode - 1;
                    elemToSwap = BTree.findByIndex(node, indexInNode - 1);
                }
                break;
            }
            if (BTree.isLeaf(node))
                return btree;

            i = -1 - i;
            if (i > 0)
                lb += BTree.getSizeMap(node)[i - 1] + 1;

            node = (Object[]) node[keyEnd + i];
        }
        if (BTree.size(btree) == 1)
            return BTree.empty();
        Object[] result = removeFromLeaf(btree, index);
        if (elemToSwap != null)
            BTree.replaceInSitu(result, index, elemToSwap);
        return result;
    }

    /**
     * Remove |elem| from |btree|. It has to be present and it has to reside in a leaf node.
     */
    private static Object[] removeFromLeaf(Object[] node, int index)
    {
        Object[] result = null;
        Object[] prevNode = null;
        int prevI = -1;
        boolean needsCopy = true;
        while (!BTree.isLeaf(node))
        {
            final int keyEnd = BTree.getBranchKeyEnd(node);
            int i = -1 - Arrays.binarySearch(BTree.getSizeMap(node), index);
            if (i > 0)
                index -= (1 + BTree.getSizeMap(node)[i - 1]);
            Object[] nextNode = (Object[]) node[keyEnd + i];
            boolean nextNodeNeedsCopy = true;
            if (BTree.getKeyEnd(nextNode) > BTree.MINIMAL_NODE_SIZE)
                node = copyIfNeeded(node, needsCopy);
            else if (i > 0 && BTree.getKeyEnd((Object[]) node[keyEnd + i - 1]) > BTree.MINIMAL_NODE_SIZE)
            {
                node = copyIfNeeded(node, needsCopy);
                final Object[] leftNeighbour = (Object[]) node[keyEnd + i - 1];
                index++;
                if (!BTree.isLeaf(leftNeighbour))
                    index += BTree.size((Object[])leftNeighbour[BTree.getChildEnd(leftNeighbour) - 1]);
                nextNode = rotateLeft(node, i);
            }
            else if (i < keyEnd && BTree.getKeyEnd((Object[]) node[keyEnd + i + 1]) > BTree.MINIMAL_NODE_SIZE)
            {
                node = copyIfNeeded(node, needsCopy);
                nextNode = rotateRight(node, i);
            }
            else
            {
                nextNodeNeedsCopy = false;
                if (i > 0)
                {
                    final Object[] leftNeighbour = (Object[]) node[keyEnd + i - 1];
                    final Object nodeKey = node[i - 1];
                    node = keyEnd == 1 ? null : copyWithKeyAndChildRemoved(node, i - 1, i - 1, false);
                    nextNode = merge(leftNeighbour, nextNode, nodeKey);
                    i = i - 1;
                    index += BTree.size(leftNeighbour) + 1;
                }
                else
                {
                    final Object[] rightNeighbour = (Object[]) node[keyEnd + i + 1];
                    final Object nodeKey = node[i];
                    node = keyEnd == 1 ? null : copyWithKeyAndChildRemoved(node, i, i, false);
                    nextNode = merge(nextNode, rightNeighbour, nodeKey);
                }
            }

            if (node != null)
            {
                final int[] sizeMap = BTree.getSizeMap(node);
                for (int j = i; j < sizeMap.length; ++j)
                    sizeMap[j] -= 1;
                if (prevNode != null)
                    prevNode[prevI] = node;
                else
                    result = node;
                prevNode = node;
                prevI = BTree.getChildStart(node) + i;
            }

            node = nextNode;
            needsCopy = nextNodeNeedsCopy;
        }
        final int keyEnd = BTree.getLeafKeyEnd(node);
        final Object[] newLeaf = new Object[(keyEnd & 1) == 1 ? keyEnd : keyEnd - 1];
        copyKeys(node, newLeaf, 0, index);
        if (prevNode != null)
            prevNode[prevI] = newLeaf;
        else
            result = newLeaf;
        return result;
    }

    private static Object[] rotateRight(final Object[] node, final int i)
    {
        final int keyEnd = BTree.getBranchKeyEnd(node);
        final Object[] nextNode = (Object[]) node[keyEnd + i];
        final Object[] rightNeighbour = (Object[]) node[keyEnd + i + 1];
        final boolean leaves = BTree.isLeaf(nextNode);
        final int nextKeyEnd = BTree.getKeyEnd(nextNode);
        final Object[] newChild = leaves ? null : (Object[]) rightNeighbour[BTree.getChildStart(rightNeighbour)];
        final Object[] newNextNode =
                copyWithKeyAndChildInserted(nextNode, nextKeyEnd, node[i], BTree.getChildCount(nextNode), newChild);
        node[i] = rightNeighbour[0];
        node[keyEnd + i + 1] = copyWithKeyAndChildRemoved(rightNeighbour, 0, 0, true);
        BTree.getSizeMap(node)[i] +=
                leaves ? 1 : 1 + BTree.size((Object[]) newNextNode[BTree.getChildEnd(newNextNode) - 1]);
        return newNextNode;
    }

    private static Object[] rotateLeft(final Object[] node, final int i)
    {
        final int keyEnd = BTree.getBranchKeyEnd(node);
        final Object[] nextNode = (Object[]) node[keyEnd + i];
        final Object[] leftNeighbour = (Object[]) node[keyEnd + i - 1];
        final int leftNeighbourEndKey = BTree.getKeyEnd(leftNeighbour);
        final boolean leaves = BTree.isLeaf(nextNode);
        final Object[] newChild = leaves ? null : (Object[]) leftNeighbour[BTree.getChildEnd(leftNeighbour) - 1];
        final Object[] newNextNode = copyWithKeyAndChildInserted(nextNode, 0, node[i - 1], 0, newChild);
        node[i - 1] = leftNeighbour[leftNeighbourEndKey - 1];
        node[keyEnd + i - 1] = copyWithKeyAndChildRemoved(leftNeighbour, leftNeighbourEndKey - 1, leftNeighbourEndKey, true);
        BTree.getSizeMap(node)[i - 1] -= leaves ? 1 : 1 + BTree.getSizeMap(newNextNode)[0];
        return newNextNode;
    }

    private static <V> Object[] copyWithKeyAndChildInserted(final Object[] node, final int keyIndex, final V key, final int childIndex, final Object[] child)
    {
        final boolean leaf = BTree.isLeaf(node);
        final int keyEnd = BTree.getKeyEnd(node);
        final Object[] copy;
        if (leaf)
            copy = new Object[keyEnd + ((keyEnd & 1) == 1 ? 2 : 1)];
        else
            copy = new Object[node.length + 2];

        if (keyIndex > 0)
            System.arraycopy(node, 0, copy, 0, keyIndex);
        copy[keyIndex] = key;
        if (keyIndex < keyEnd)
            System.arraycopy(node, keyIndex, copy, keyIndex + 1, keyEnd - keyIndex);

        if (!leaf)
        {
            if (childIndex > 0)
                System.arraycopy(node,
                                 BTree.getChildStart(node),
                                 copy,
                                 keyEnd + 1,
                                 childIndex);
            copy[keyEnd + 1 + childIndex] = child;
            if (childIndex <= keyEnd)
                System.arraycopy(node,
                                 BTree.getChildStart(node) + childIndex,
                                 copy,
                                 keyEnd + childIndex + 2,
                                 keyEnd - childIndex + 1);
            final int[] sizeMap = BTree.getSizeMap(node);
            final int[] newSizeMap = new int[sizeMap.length + 1];
            if (childIndex > 0)
                System.arraycopy(sizeMap, 0, newSizeMap, 0, childIndex);
            final int childSize = BTree.size(child);
            newSizeMap[childIndex] = childSize + ((childIndex == 0) ? 0 : newSizeMap[childIndex - 1] + 1);
            for (int i = childIndex + 1; i < newSizeMap.length; ++i)
                newSizeMap[i] = sizeMap[i - 1] + childSize + 1;
            copy[copy.length - 1] = newSizeMap;
        }
        return copy;
    }

    private static Object[] copyWithKeyAndChildRemoved(final Object[] node, final int keyIndex, final int childIndex, final boolean substractSize)
    {
        final boolean leaf = BTree.isLeaf(node);
        final Object[] newNode;
        if (leaf)
        {
            final int keyEnd = BTree.getKeyEnd(node);
            newNode = new Object[keyEnd - ((keyEnd & 1) == 1 ? 0 : 1)];
        }
        else
        {
            newNode = new Object[node.length - 2];
        }
        int offset = copyKeys(node, newNode, 0, keyIndex);
        if (!leaf)
        {
            offset = copyChildren(node, newNode, offset, childIndex);
            final int[] nodeSizeMap = BTree.getSizeMap(node);
            final int[] newNodeSizeMap = new int[nodeSizeMap.length - 1];
            int pos = 0;
            final int sizeToRemove = BTree.size((Object[])node[BTree.getChildStart(node) + childIndex]) + 1;
            for (int i = 0; i < nodeSizeMap.length; ++i)
                if (i != childIndex)
                    newNodeSizeMap[pos++] = nodeSizeMap[i] -
                        ((substractSize && i > childIndex) ? sizeToRemove : 0);
            newNode[offset] = newNodeSizeMap;
        }
        return newNode;
    }

    private static <V> Object[] merge(final Object[] left, final Object[] right, final V nodeKey)
    {
        assert BTree.getKeyEnd(left) == BTree.MINIMAL_NODE_SIZE;
        assert BTree.getKeyEnd(right) == BTree.MINIMAL_NODE_SIZE;
        final boolean leaves = BTree.isLeaf(left);
        final Object[] result;
        if (leaves)
            result = new Object[BTree.MINIMAL_NODE_SIZE * 2 + 1];
        else
            result = new Object[left.length + right.length];
        int offset = 0;
        offset = copyKeys(left, result, offset);
        result[offset++] = nodeKey;
        offset = copyKeys(right, result, offset);
        if (!leaves)
        {
            offset = copyChildren(left, result, offset);
            offset = copyChildren(right, result, offset);
            final int[] leftSizeMap = BTree.getSizeMap(left);
            final int[] rightSizeMap = BTree.getSizeMap(right);
            final int[] newSizeMap = new int[leftSizeMap.length + rightSizeMap.length];
            offset = 0;
            offset = copySizeMap(leftSizeMap, newSizeMap, offset, 0);
            offset = copySizeMap(rightSizeMap, newSizeMap, offset, leftSizeMap[leftSizeMap.length - 1] + 1);
            result[result.length - 1] = newSizeMap;
        }
        return result;
    }

    private static int copyKeys(final Object[] from, final Object[] to, final int offset)
    {
        final int keysCount = BTree.getKeyEnd(from);
        System.arraycopy(from, 0, to, offset, keysCount);
        return offset + keysCount;
    }

    private static int copyKeys(final Object[] from, final Object[] to, final int offset, final int skipIndex)
    {
        final int keysCount = BTree.getKeyEnd(from);
        if (skipIndex > 0)
            System.arraycopy(from, 0, to, offset, skipIndex);
        if (skipIndex + 1 < keysCount)
            System.arraycopy(from, skipIndex + 1, to, offset + skipIndex, keysCount - skipIndex - 1);
        return offset + keysCount - 1;
    }

    private static int copyChildren(final Object[] from, final Object[] to, final int offset)
    {
        assert !BTree.isLeaf(from);
        final int start = BTree.getChildStart(from);
        final int childCount = BTree.getChildCount(from);
        System.arraycopy(from, start, to, offset, childCount);
        return offset + childCount;
    }

    private static int copyChildren(final Object[] from, final Object[] to, final int offset, final int skipIndex)
    {
        assert !BTree.isLeaf(from);
        final int start = BTree.getChildStart(from);
        final int childCount = BTree.getChildCount(from);
        if (skipIndex > 0)
            System.arraycopy(from, start, to, offset, skipIndex);
        if (skipIndex + 1 <= childCount)
            System.arraycopy(from, start + skipIndex + 1, to, offset + skipIndex, childCount - skipIndex - 1);
        return offset + childCount - 1;
    }

    private static int copySizeMap(final int[] from, final int[] to, final int offset, final int extra)
    {
        for (int i = 0; i < from.length; ++i)
            to[offset + i] = from[i] + extra;
        return offset + from.length;
    }

    private static Object[] copyIfNeeded(final Object[] node, boolean needCopy)
    {
        if (!needCopy) return node;
        final Object[] copy = new Object[node.length];
        System.arraycopy(node, 0, copy, 0, node.length);
        if (!BTree.isLeaf(node))
        {
            final int[] sizeMap = BTree.getSizeMap(node);
            final int[] copySizeMap = new int[sizeMap.length];
            System.arraycopy(sizeMap, 0, copySizeMap, 0, sizeMap.length);
            copy[copy.length - 1] = copySizeMap;
        }
        return copy;
    }
}
