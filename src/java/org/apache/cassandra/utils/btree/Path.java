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

import java.util.Comparator;

import static org.apache.cassandra.utils.btree.BTree.NEGATIVE_INFINITY;
import static org.apache.cassandra.utils.btree.BTree.POSITIVE_INFINITY;
import static org.apache.cassandra.utils.btree.BTree.getBranchKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getLeafKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;

/**
 * An internal class for searching and iterating through a tree.  As it traverses the tree,
 * it adds the nodes visited to a stack.  This allows us to backtrack from a child node
 * to its parent.
 *
 * As we navigate the tree, we destructively modify this stack.
 *
 * Path is only intended to be used via Cursor.
 */
public class Path<V>
{
    // operations corresponding to the ones in NavigableSet
    static enum Op
    {
        CEIL,   // the least element greater than or equal to the given element
        FLOOR,  // the greatest element less than or equal to the given element
        HIGHER, // the least element strictly greater than the given element
        LOWER   // the greatest element strictly less than the given element
    }

    // the path to the searched-for key
    Object[][] path;
    // the index within the node of our path at a given depth
    byte[] indexes;
    // current depth.  nothing in path[i] for i > depth is valid.
    byte depth;

    Path() { }
    Path(int depth, Object[] btree)
    {
        this.path = new Object[depth][];
        this.indexes = new byte[depth];
        this.path[0] = btree;
    }

    void init(Object[] btree)
    {
        int depth = BTree.depth(btree);
        if (path == null || path.length < depth)
        {
            path = new Object[depth][];
            indexes = new byte[depth];
        }
        path[0] = btree;
    }

    void moveEnd(Object[] node, boolean forwards)
    {
        push(node, getKeyEnd(node));
        if (!forwards)
            predecessor();
    }

    void moveStart(Object[] node, boolean forwards)
    {
        push(node, -1);
        if (forwards)
            successor();
    }

    /**
     * Find the provided key in the tree rooted at node, and store the root to it in the path
     *
     * @param comparator the comparator defining the order on the tree
     * @param target     the key to search for
     * @param mode       the type of search to perform
     * @param forwards   if the path should be setup for forward or backward iteration
     * @param <K>
     */
    <K> boolean find(Comparator<K> comparator, Object target, Op mode, boolean forwards)
    {
        // TODO : should not require parameter 'forwards' - consider modifying index to represent both
        // child and key position, as opposed to just key position (which necessitates a different value depending
        // on which direction you're moving in. Prerequisite for making Path public and using to implement general
        // search

        Object[] node = path[depth];
        int lb = indexes[depth];
        assert lb == 0 || forwards;
        pop();

        if (target instanceof BTree.Special)
        {
            if (target == POSITIVE_INFINITY)
                moveEnd(node, forwards);
            else if (target == NEGATIVE_INFINITY)
                moveStart(node, forwards);
            else
                throw new AssertionError();
            return false;
        }

        while (true)
        {
            int keyEnd = getKeyEnd(node);

            // search for the target in the current node
            int i = BTree.find(comparator, target, node, lb, keyEnd);
            lb = 0;
            if (i >= 0)
            {
                // exact match. transform exclusive bounds into the correct index by moving back or forwards one
                push(node, i);
                switch (mode)
                {
                    case HIGHER:
                        successor();
                        break;
                    case LOWER:
                        predecessor();
                }
                return true;
            }
            i = -i - 1;

            // traverse into the appropriate child
            if (!isLeaf(node))
            {
                push(node, forwards ? i - 1 : i);
                node = (Object[]) node[keyEnd + i];
                continue;
            }

            // bottom of the tree and still not found.  pick the right index to satisfy Op
            switch (mode)
            {
                case FLOOR:
                case LOWER:
                    i--;
            }

            if (i < 0)
            {
                push(node, 0);
                predecessor();
            }
            else if (i >= keyEnd)
            {
                push(node, keyEnd - 1);
                successor();
            }
            else
            {
                push(node, i);
            }

            return false;
        }
    }

    boolean isRoot()
    {
        return depth == 0;
    }

    void pop()
    {
        depth--;
    }

    Object[] currentNode()
    {
        return path[depth];
    }

    byte currentIndex()
    {
        return indexes[depth];
    }

    void push(Object[] node, int index)
    {
        path[++depth] = node;
        indexes[depth] = (byte) index;
    }

    void setIndex(int index)
    {
        indexes[depth] = (byte) index;
    }

    byte findSuccessorParentDepth()
    {
        byte depth = this.depth;
        depth--;
        while (depth >= 0)
        {
            int ub = indexes[depth] + 1;
            Object[] node = path[depth];
            if (ub < getBranchKeyEnd(node))
                return depth;
            depth--;
        }
        return -1;
    }

    // move to the next key in the tree
    void successor()
    {
        Object[] node = currentNode();
        int i = currentIndex();

        if (!isLeaf(node))
        {
            // if we're on a key in a branch, we MUST have a descendant either side of us,
            // so we always go down the left-most child until we hit a leaf
            node = (Object[]) node[getBranchKeyEnd(node) + i + 1];
            while (!isLeaf(node))
            {
                push(node, -1);
                node = (Object[]) node[getBranchKeyEnd(node)];
            }
            push(node, 0);
            return;
        }

        // if we haven't reached the end of this leaf, just increment our index and return
        i += 1;
        if (i < getLeafKeyEnd(node))
        {
            // moved to the next key in the same leaf
            setIndex(i);
            return;
        }

        // we've reached the end of this leaf,
        // so go up until we reach something we've not finished visiting
        while (!isRoot())
        {
            pop();
            i = currentIndex() + 1;
            node = currentNode();
            if (i < getKeyEnd(node))
            {
                setIndex(i);
                return;
            }
        }

        // we've visited the last key in the root node, so we're done
        setIndex(getKeyEnd(node));
    }

    // move to the previous key in the tree
    void predecessor()
    {
        Object[] node = currentNode();
        int i = currentIndex();

        if (!isLeaf(node))
        {
            // if we're on a key in a branch, we MUST have a descendant either side of us
            // so we always go down the right-most child until we hit a leaf
            node = (Object[]) node[getBranchKeyEnd(node) + i];
            while (!isLeaf(node))
            {
                i = getBranchKeyEnd(node);
                push(node, i);
                node = (Object[]) node[i * 2];
            }
            push(node, getLeafKeyEnd(node) - 1);
            return;
        }

        // if we haven't reached the beginning of this leaf, just decrement our index and return
        i -= 1;
        if (i >= 0)
        {
            setIndex(i);
            return;
        }

        // we've reached the beginning of this leaf,
        // so go up until we reach something we've not finished visiting
        while (!isRoot())
        {
            pop();
            i = currentIndex() - 1;
            if (i >= 0)
            {
                setIndex(i);
                return;
            }
        }

        // we've visited the last key in the root node, so we're done
        setIndex(-1);
    }

    Object currentKey()
    {
        return currentNode()[currentIndex()];
    }

    int compareTo(Path<V> that, boolean forwards)
    {
        int d = Math.min(this.depth, that.depth);
        for (int i = 0; i <= d; i++)
        {
            int c = this.indexes[i] - that.indexes[i];
            if (c != 0)
                return c;
        }
        // identical indices up to depth, so if somebody is lower depth they are on a later item if iterating forwards
        // and an earlier item if iterating backwards, as the node at max common depth must be a branch if they are
        // different depths, and branches that are currently descended into lag the child index they are in when iterating forwards,
        // i.e. if they are in child 0 they record an index of -1 forwards, or 0 when backwards
        d = this.depth - that.depth;
        return forwards ? d : -d;
    }
}

