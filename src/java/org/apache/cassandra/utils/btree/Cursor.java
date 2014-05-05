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
import java.util.Iterator;

import static org.apache.cassandra.utils.btree.BTree.NEGATIVE_INFINITY;
import static org.apache.cassandra.utils.btree.BTree.POSITIVE_INFINITY;
import static org.apache.cassandra.utils.btree.BTree.getLeafKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;

/**
 * An extension of Path which provides a public interface for iterating over or counting a subrange of the tree
 *
 * @param <V>
 */
public final class Cursor<K, V extends K> extends Path implements Iterator<V>
{
    /*
     * Conceptually, a Cursor derives two Paths, one for the first object in the slice requested (inclusive),
     * and one for the last (exclusive).  Then hasNext just checks, have we reached the last yet, and next
     * calls successor() to get to the next item in the Tree.
     *
     * To optimize memory use, we summarize the last Path as just endNode/endIndex, and inherit from Path for
     *
     * the first one.
     */

    // the last node covered by the requested range
    private Object[] endNode;
    // the index within endNode that signals we're finished -- that is, endNode[endIndex] is NOT part of the Cursor
    private byte endIndex;

    private boolean forwards;

    /**
     * Reset this cursor for the provided tree, to iterate over its entire range
     *
     * @param btree    the tree to iterate over
     * @param forwards if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, boolean forwards)
    {
        _reset(btree, null, NEGATIVE_INFINITY, false, POSITIVE_INFINITY, false, forwards);
    }

    /**
     * Reset this cursor for the provided tree, to iterate between the provided start and end
     *
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param lowerBound the first item to include, inclusive
     * @param upperBound the last item to include, exclusive
     * @param forwards   if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, Comparator<K> comparator, K lowerBound, K upperBound, boolean forwards)
    {
        _reset(btree, comparator, lowerBound, true, upperBound, false, forwards);
    }

    /**
     * Reset this cursor for the provided tree, to iterate between the provided start and end
     *
     * @param btree               the tree to iterate over
     * @param comparator          the comparator that defines the ordering over the items in the tree
     * @param lowerBound          the first item to include
     * @param inclusiveLowerBound should include start in the iterator, if present in the tree
     * @param upperBound          the last item to include
     * @param inclusiveUpperBound should include end in the iterator, if present in the tree
     * @param forwards            if false, the cursor will start at the end and move backwards
     */
    public void reset(Object[] btree, Comparator<K> comparator, K lowerBound, boolean inclusiveLowerBound, K upperBound, boolean inclusiveUpperBound, boolean forwards)
    {
        _reset(btree, comparator, lowerBound, inclusiveLowerBound, upperBound, inclusiveUpperBound, forwards);
    }

    private void _reset(Object[] btree, Comparator<K> comparator, Object lowerBound, boolean inclusiveLowerBound, Object upperBound, boolean inclusiveUpperBound, boolean forwards)
    {
        init(btree);
        if (lowerBound == null)
            lowerBound = NEGATIVE_INFINITY;
        if (upperBound == null)
            upperBound = POSITIVE_INFINITY;

        this.forwards = forwards;

        Path findLast = new Path(this.path.length, btree);
        if (forwards)
        {
            findLast.find(comparator, upperBound, inclusiveUpperBound ? Op.HIGHER : Op.CEIL, true);
            find(comparator, lowerBound, inclusiveLowerBound ? Op.CEIL : Op.HIGHER, true);
        }
        else
        {
            findLast.find(comparator, lowerBound, inclusiveLowerBound ? Op.LOWER : Op.FLOOR, false);
            find(comparator, upperBound, inclusiveUpperBound ? Op.FLOOR : Op.LOWER, false);
        }
        int c = this.compareTo(findLast, forwards);
        if (forwards ? c > 0 : c < 0)
        {
            endNode = currentNode();
            endIndex = currentIndex();
        }
        else
        {
            endNode = findLast.currentNode();
            endIndex = findLast.currentIndex();
        }
    }

    public boolean hasNext()
    {
        return path[depth] != endNode || indexes[depth] != endIndex;
    }

    public V next()
    {
        Object r = currentKey();
        if (forwards)
            successor();
        else
            predecessor();
        return (V) r;
    }

    public int count()
    {
        if (!forwards)
            throw new IllegalStateException("Count can only be run on forward cursors");
        int count = 0;
        int next;
        while ((next = consumeNextLeaf()) >= 0)
            count += next;
        return count;
    }

    /**
     * @return the number of objects consumed by moving out of the next (possibly current) leaf
     */
    private int consumeNextLeaf()
    {
        Object[] node = currentNode();
        int r = 0;

        if (!isLeaf(node))
        {
            // if we're not in a leaf, then calling successor once will take us to a leaf, since the next
            // key will be in the leftmost subtree of whichever branch is next.  For instance, if we
            // are in the root node of the tree depicted by http://cis.stvincent.edu/html/tutorials/swd/btree/btree1.gif,
            // successor() will take us to the leaf containing N and O.
            int i = currentIndex();
            if (node == endNode && i == endIndex)
                return -1;
            r = 1;
            successor();
            node = currentNode();
        }

        if (node == endNode)
        {
            // only count up to endIndex, and don't call successor()
            if (currentIndex() == endIndex)
                return r > 0 ? r : -1;
            r += endIndex - currentIndex();
            setIndex(endIndex);
            return r;
        }

        // count the remaining objects in this leaf
        int keyEnd = getLeafKeyEnd(node);
        r += keyEnd - currentIndex();
        setIndex(keyEnd);
        successor();
        return r;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
