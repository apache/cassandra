/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import static org.apache.cassandra.utils.btree.BTree.*;

/**
 * A class for searching within one node of a btree: a linear chain (stack) of these is built of tree height
 * to form a Cursor. Some corollaries of the basic building block operations in TreeCursor (moveOne and seekTo),
 * along with some other methods for helping implement movement between two NodeCursor
 *
 * The behaviour is not dissimilar to that of NodeBuilder and TreeBuilder, wherein functions that may move
 * us to a different node pass us the node we should move to, from which we continue our operations.
 * @param <K>
 */
class NodeCursor<K>
{
    // TODO: consider splitting forwards from backwards
    final NodeCursor<K> parent, child;
    final Comparator<? super K> comparator;

    boolean inChild;
    // if !inChild, this is the key position we are currently on;
    // if inChild, this is the child position we are currently descending into
    int position;
    Object[] node;
    int nodeOffset;

    NodeCursor(Object[] node, NodeCursor<K> parent, Comparator<? super K> comparator)
    {
        this.node = node;
        this.parent = parent;
        this.comparator = comparator;
        // a well formed b-tree (text book, or ours) must be balanced, so by building a stack following the left-most branch
        // we have a stack capable of visiting any path in the tree
        this.child = BTree.isLeaf(node) ? null : new NodeCursor<>((Object[]) node[getChildStart(node)], this, comparator);
    }

    void resetNode(Object[] node, int nodeOffset)
    {
        this.node = node;
        this.nodeOffset = nodeOffset;
    }

    /**
     * adapt child position to key position within branch, knowing it is safe to do so
     */
    void safeAdvanceIntoBranchFromChild(boolean forwards)
    {
        if (!forwards)
            --position;
    }

    /**
     * adapt child position to key position within branch, and return if this was successful or we're now out of bounds
     */
    boolean advanceIntoBranchFromChild(boolean forwards)
    {
        return forwards ? position < getBranchKeyEnd(node) : --position >= 0;
    }

    boolean advanceLeafNode(boolean forwards)
    {
        return forwards ? ++position < getLeafKeyEnd(node)
                        : --position >= 0;
    }

    /**
     * @return the upper/lower bound of the child we are currently descended in
     */
    K bound(boolean upper)
    {
        return (K) node[position - (upper ? 0 : 1)];
    }

    /**
     * The parent that covers a range wider than ourselves, either ascending or descending,
     * i.e. that defines the upper or lower bound on the subtree rooted at our node
     * @param upper
     * @return the NodeCursor parent that can tell us the upper/lower bound of ourselves
     */
    NodeCursor<K> boundIterator(boolean upper)
    {
        NodeCursor<K> bound = this.parent;
        while (bound != null && (upper ? bound.position >= getChildCount(bound.node) - 1
                                       : bound.position <= 0))
            bound = bound.parent;
        return bound;
    }

    /**
     * look for the provided key in this node, in the specified direction:
     * forwards => ceil search; otherwise floor
     *
     * we require that the node's "current" key (including the relevant bound if we are a parent we have ascended into)
     * be already excluded by the search. this is useful for the following reasons:
     *   1: we must ensure we never go backwards, so excluding that key from our binary search prevents our
     *      descending into a child we have already visited (without any further checks)
     *   2: we already check the bounds as we search upwards for our natural parent;
     *   3: we want to cheaply check sequential access, so we always check the first key we're on anyway (if it can be done easily)
     */
    boolean seekInNode(K key, boolean forwards)
    {
        int position = this.position;
        int lb, ub;
        if (forwards)
        {
            lb = position + 1;
            ub = getKeyEnd(node);
        }
        else
        {
            ub = position;
            lb = 0;
        }

        int find = Arrays.binarySearch((K[]) node, lb, ub, key, comparator);
        if (find >= 0)
        {
            // exact key match, so we're in the correct node already. return success
            this.position = find;
            inChild = false;
            return true;
        }

        // if we are a branch, and we are an inequality match, the direction of travel doesn't matter
        // so we only need to modify if we are going backwards on a leaf node, to produce floor semantics
        int delta = isLeaf() & !forwards ? -1 : 0;
        this.position = delta -1 -find;
        return false;
    }

    NodeCursor<K> descendToFirstChild(boolean forwards)
    {
        if (isLeaf())
        {
            position = forwards ? 0 : getLeafKeyEnd(node) - 1;
            return null;
        }
        inChild = true;
        position = forwards ? 0 : getChildCount(node) - 1;
        return descend();
    }

    // descend into the child at "position"
    NodeCursor<K> descend()
    {
        Object[] childNode = (Object[]) node[position + getChildStart(node)];
        int childOffset = nodeOffset + treeIndexOffsetOfChild(node, position);
        child.resetNode(childNode, childOffset);
        inChild = true;
        return child;
    }

    boolean isLeaf()
    {
        return child == null;
    }

    int globalIndex()
    {
        return nodeOffset + treeIndexOfKey(node, position);
    }

    int globalLeafIndex()
    {
        return nodeOffset + treeIndexOfLeafKey(position);
    }

    int globalBranchIndex()
    {
        return nodeOffset + treeIndexOfBranchKey(node, position);
    }

    K value()
    {
        return (K) node[position];
    }
}