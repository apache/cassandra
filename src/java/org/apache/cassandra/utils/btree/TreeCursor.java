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
 * Supports two basic operations for moving around a BTree, either forwards or backwards:
 * moveOne(), and seekTo()
 *
 * These two methods, along with movement to the start/end, permit us to construct any desired
 * movement around a btree, without much cognitive burden.
 *
 * This TreeCursor is (and has its methods) package private. This is to avoid polluting the BTreeSearchIterator
 * that extends it, and uses its functionality. If this class is useful for wider consumption, a public extension
 * class can be provided, that just makes all of its methods public.
 */
class TreeCursor<K> extends NodeCursor<K>
{
    // TODO: spend some time optimising compiler inlining decisions: many of these methods have only one primary call-site

    NodeCursor<K> cur;

    TreeCursor(Comparator<? super K> comparator, Object[] node)
    {
        super(node, null, comparator);
    }

    /**
     * Move the cursor to either the first or last item in the btree
     * @param start true if should move to the first item; false moves to the last
     */
    void reset(boolean start)
    {
        cur = root();
        root().inChild = false;
        // this is a corrupt position, but we ensure we never use it except to start our search from
        root().position = start ? -1 : getKeyEnd(root().node);
    }

    /**
     * move the Cursor one item, either forwards or backwards
     * @param forwards direction of travel
     * @return false iff the cursor is exhausted in the direction of travel
     */
    int moveOne(boolean forwards)
    {
        NodeCursor<K> cur = this.cur;
        if (cur.isLeaf())
        {
            // if we're a leaf, we try to step forwards inside ourselves
            if (cur.advanceLeafNode(forwards))
                return cur.globalLeafIndex();

            // if we fail, we just find our bounding parent
            this.cur = cur = moveOutOfLeaf(forwards, cur, root());
            return cur.globalIndex();
        }

        // otherwise we descend directly into our next child
        if (forwards)
            ++cur.position;
        cur = cur.descend();

        // and go to its first item
        NodeCursor<K> next;
        while ( null != (next = cur.descendToFirstChild(forwards)) )
            cur = next;

        this.cur = cur;
        return cur.globalLeafIndex();
    }

    /**
     * seeks from the current position, forwards or backwards, for the provided key
     * while the direction could be inferred (or ignored), it is required so that (e.g.) we do not infinitely loop on bad inputs
     * if there is no such key, it moves to the key that would naturally follow/succeed it (i.e. it behaves as ceil when ascending; floor when descending)
     */
    boolean seekTo(K key, boolean forwards, boolean skipOne)
    {
        NodeCursor<K> cur = this.cur;

        /**
         * decide if we will "try one" value by itself, as a sequential access;
         * we actually *require* that we try the "current key" for any node before we call seekInNode on it.
         *
         * if we are already on a value, we just check it irregardless of if it is a leaf or not;
         * if we are not, we have already excluded it (as we have consumed it), so:
         *    if we are on a branch we consider that good enough;
         *    otherwise, we move onwards one, and we try the new value
         *
         */
        boolean tryOne = !skipOne;
        if ((!tryOne & cur.isLeaf()) && !(tryOne = (cur.advanceLeafNode(forwards) || (cur = moveOutOfLeaf(forwards, cur, null)) != null)))
        {
            // we moved out of the tree; return out-of-bounds
            this.cur = root();
            return false;
        }

        if (tryOne)
        {
            // we're presently on a value we can (and *must*) cheaply test
            K test = cur.value();

            int cmp;
            if (key == test) cmp = 0; // check object identity first, since we utilise that in some places and it's very cheap
            else cmp = comparator.compare(test, key); // order of provision matters for asymmetric comparators
            if (forwards ? cmp >= 0 : cmp <= 0)
            {
                // we've either matched, or excluded the value from being present
                this.cur = cur;
                return cmp == 0;
            }
        }

        // if we failed to match with the cheap test, first look to see if we're even in the correct sub-tree
        while (cur != root())
        {
            NodeCursor<K> bound = cur.boundIterator(forwards);
            if (bound == null)
                break; // we're all that's left

            int cmpbound = comparator.compare(bound.bound(forwards), key); // order of provision matters for asymmetric comparators
            if (forwards ? cmpbound > 0 : cmpbound < 0)
                break; //  already in correct sub-tree

            // bound is on-or-before target, so ascend to that bound and continue looking upwards
            cur = bound;
            cur.safeAdvanceIntoBranchFromChild(forwards);
            if (cmpbound == 0) // it was an exact match, so terminate here
            {
                this.cur = cur;
                return true;
            }
        }

        // we must now be able to find our target in the sub-tree rooted at cur
        boolean match;
        while (!(match = cur.seekInNode(key, forwards)) && !cur.isLeaf())
        {
            cur = cur.descend();
            cur.position = forwards ? -1 : getKeyEnd(cur.node);
        }

        if (!match)
            cur = ensureValidLocation(forwards, cur);

        this.cur = cur;
        assert !cur.inChild;
        return match;
    }

    /**
     * ensures a leaf node we have seeked in, is not positioned outside of its bounds,
     * by moving us into its parents (if any); if it is the root, we're permitted to be out-of-bounds
     * as this indicates exhaustion
     */
    private NodeCursor<K> ensureValidLocation(boolean forwards, NodeCursor<K> cur)
    {
        assert cur.isLeaf();
        int position = cur.position;
        // if we're out of bounds of the leaf, move once in direction of travel
        if ((position < 0) | (position >= getLeafKeyEnd(cur.node)))
            cur = moveOutOfLeaf(forwards, cur, root());
        return cur;
    }

    /**
     * move out of a leaf node that is currently out of (its own) bounds
     * @return null if we're now out-of-bounds of the whole tree
     */
    private <K> NodeCursor<K> moveOutOfLeaf(boolean forwards, NodeCursor<K> cur, NodeCursor<K> ifFail)
    {
        while (true)
        {
            cur = cur.parent;
            if (cur == null)
            {
                root().inChild = false;
                return ifFail;
            }
            if (cur.advanceIntoBranchFromChild(forwards))
                break;
        }
        cur.inChild = false;
        return cur;
    }

    /**
     * resets the cursor and seeks to the specified position; does not assume locality or take advantage of the cursor's current position
     */
    void seekTo(int index)
    {
        if ((index < 0) | (index >= BTree.size(rootNode())))
        {
            if ((index < -1) | (index > BTree.size(rootNode())))
                throw new IndexOutOfBoundsException(index + " not in range [0.." + BTree.size(rootNode()) + ")");
            reset(index == -1);
            return;
        }

        NodeCursor<K> cur = root();
        assert cur.nodeOffset == 0;
        while (true)
        {
            int relativeIndex = index - cur.nodeOffset; // index within subtree rooted at cur
            Object[] node = cur.node;

            if (cur.isLeaf())
            {
                assert relativeIndex < getLeafKeyEnd(node);
                cur.position = relativeIndex;
                this.cur = cur;
                return;
            }

            int[] sizeMap = getSizeMap(node);
            int boundary = Arrays.binarySearch(sizeMap, relativeIndex);
            if (boundary >= 0)
            {
                // exact match, in this branch node
                assert boundary < sizeMap.length - 1;
                cur.position = boundary;
                cur.inChild = false;
                this.cur = cur;
                return;
            }

            cur.inChild = true;
            cur.position = -1 -boundary;
            cur = cur.descend();
        }
    }

    private NodeCursor<K> root()
    {
        return this;
    }

    Object[] rootNode()
    {
        return this.node;
    }

    K currentValue()
    {
        return cur.value();
    }
}
