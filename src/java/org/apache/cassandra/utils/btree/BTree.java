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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Queue;

import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.utils.btree.UpdateFunction.NoOp;

public class BTree
{
    /**
     * Leaf Nodes are a raw array of values: Object[V1, V1, ...,].
     *
     * Branch Nodes: Object[V1, V2, ..., child[&lt;V1.key], child[&lt;V2.key], ..., child[&lt; Inf]], where
     * each child is another node, i.e., an Object[].  Thus, the value elements in a branch node are the
     * first half of the array, rounding down.  In our implementation, each value must include its own key;
     * we access these via Comparator, rather than directly. 
     *
     * So we can quickly distinguish between leaves and branches, we require that leaf nodes are always even number
     * of elements (padded with a null, if necessary), and branches are always an odd number of elements.
     *
     * BTrees are immutable; updating one returns a new tree that reuses unmodified nodes.
     *
     * There are no references back to a parent node from its children.  (This would make it impossible to re-use
     * subtrees when modifying the tree, since the modified tree would need new parent references.)
     * Instead, we store these references in a Path as needed when navigating the tree.
     */

    // The maximum fan factor used for B-Trees
    static final int FAN_SHIFT;
    static
    {
        int fanfactor = 32;
        if (System.getProperty("cassandra.btree.fanfactor") != null)
            fanfactor = Integer.parseInt(System.getProperty("cassandra.btree.fanfactor"));
        int shift = 1;
        while (1 << shift < fanfactor)
            shift += 1;
        FAN_SHIFT = shift;
    }
    // NB we encode Path indexes as Bytes, so this needs to be less than Byte.MAX_VALUE / 2
    static final int FAN_FACTOR = 1 << FAN_SHIFT;

    // An empty BTree Leaf - which is the same as an empty BTree
    static final Object[] EMPTY_LEAF = new Object[0];

    // An empty BTree branch - used only for internal purposes in Modifier
    static final Object[] EMPTY_BRANCH = new Object[1];

    /**
     * Returns an empty BTree
     *
     * @return
     */
    public static Object[] empty()
    {
        return EMPTY_LEAF;
    }

    public static <V> Object[] build(Collection<V> source, Comparator<V> comparator, boolean sorted, UpdateFunction<V> updateF)
    {
        return build(source, source.size(), comparator, sorted, updateF);
    }

    /**
     * Creates a BTree containing all of the objects in the provided collection
     *
     * @param source     the items to build the tree with
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param sorted     if false, the collection will be copied and sorted to facilitate construction
     * @param <V>
     * @return
     */
    public static <V> Object[] build(Iterable<V> source, int size, Comparator<V> comparator, boolean sorted, UpdateFunction<V> updateF)
    {
        if (size < FAN_FACTOR)
        {
            // pad to even length to match contract that all leaf nodes are even
            V[] values = (V[]) new Object[size + (size & 1)];
            {
                int i = 0;
                for (V v : source)
                    values[i++] = v;
            }

            // inline sorting since we're already calling toArray
            if (!sorted)
                Arrays.sort(values, 0, size, comparator);

            // if updateF is specified
            if (updateF != null)
            {
                for (int i = 0 ; i < size ; i++)
                    values[i] = updateF.apply(values[i]);
                updateF.allocated(ObjectSizes.sizeOfArray(values));
            }
            return values;
        }

        if (!sorted)
            source = sorted(source, comparator, size);

        Queue<Builder> queue = modifier.get();
        Builder builder = queue.poll();
        if (builder == null)
            builder = new Builder();
        Object[] btree = builder.build(source, updateF, size);
        queue.add(builder);
        return btree;
    }

    /**
     * Returns a new BTree with the provided set inserting/replacing as necessary any equal items
     *
     * @param btree              the tree to update
     * @param comparator         the comparator that defines the ordering over the items in the tree
     * @param updateWith         the items to either insert / update
     * @param updateWithIsSorted if false, updateWith will be copied and sorted to facilitate construction
     * @param <V>
     * @return
     */
    public static <V> Object[] update(Object[] btree, Comparator<V> comparator, Collection<V> updateWith, boolean updateWithIsSorted)
    {
        return update(btree, comparator, updateWith, updateWithIsSorted, NoOp.<V>instance());
    }

    public static <V> Object[] update(Object[] btree,
                                      Comparator<V> comparator,
                                      Collection<V> updateWith,
                                      boolean updateWithIsSorted,
                                      UpdateFunction<V> updateF)
    {
        return update(btree, comparator, updateWith, updateWith.size(), updateWithIsSorted, updateF);
    }

    /**
     * Returns a new BTree with the provided set inserting/replacing as necessary any equal items
     *
     * @param btree              the tree to update
     * @param comparator         the comparator that defines the ordering over the items in the tree
     * @param updateWith         the items to either insert / update
     * @param updateWithIsSorted if false, updateWith will be copied and sorted to facilitate construction
     * @param updateF            the update function to apply to any pairs we are swapping, and maybe abort early
     * @param <V>
     * @return
     */
    public static <V> Object[] update(Object[] btree,
                                      Comparator<V> comparator,
                                      Iterable<V> updateWith,
                                      int updateWithLength,
                                      boolean updateWithIsSorted,
                                      UpdateFunction<V> updateF)
    {
        if (btree.length == 0)
            return build(updateWith, updateWithLength, comparator, updateWithIsSorted, updateF);

        if (!updateWithIsSorted)
            updateWith = sorted(updateWith, comparator, updateWithLength);

        Queue<Builder> queue = modifier.get();
        Builder builder = queue.poll();
        if (builder == null)
            builder = new Builder();
        btree = builder.update(btree, comparator, updateWith, updateF);
        queue.add(builder);
        return btree;
    }

    /**
     * Returns an Iterator over the entire tree
     *
     * @param btree    the tree to iterate over
     * @param forwards if false, the iterator will start at the end and move backwards
     * @param <V>
     * @return
     */
    public static <V> Cursor<V, V> slice(Object[] btree, boolean forwards)
    {
        Cursor<V, V> r = new Cursor<>();
        r.reset(btree, forwards);
        return r;
    }

    /**
     * Returns an Iterator over a sub-range of the tree
     *
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param start      the first item to include
     * @param end        the last item to include
     * @param forwards   if false, the iterator will start at end and move backwards
     * @param <V>
     * @return
     */
    public static <K, V extends K> Cursor<K, V> slice(Object[] btree, Comparator<K> comparator, K start, K end, boolean forwards)
    {
        Cursor<K, V> r = new Cursor<>();
        r.reset(btree, comparator, start, end, forwards);
        return r;
    }

    /**
     * Returns an Iterator over a sub-range of the tree
     *
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param start      the first item to include
     * @param end        the last item to include
     * @param forwards   if false, the iterator will start at end and move backwards
     * @param <V>
     * @return
     */
    public static <K, V extends K> Cursor<K, V> slice(Object[] btree, Comparator<K> comparator, K start, boolean startInclusive, K end, boolean endInclusive, boolean forwards)
    {
        Cursor<K, V> r = new Cursor<>();
        r.reset(btree, comparator, start, startInclusive, end, endInclusive, forwards);
        return r;
    }

    public static <V> V find(Object[] node, Comparator<V> comparator, V find)
    {
        while (true)
        {
            int keyEnd = getKeyEnd(node);
            int i = BTree.find(comparator, find, node, 0, keyEnd);
            if (i >= 0)
            {
                return (V) node[i];
            }
            else if (!isLeaf(node))
            {
                i = -i - 1;
                node = (Object[]) node[keyEnd + i];
            }
            else
            {
                return null;
            }
        }
    }


    // UTILITY METHODS

    // same basic semantics as Arrays.binarySearch, but delegates to compare() method to avoid
    // wrapping generic Comparator with support for Special +/- infinity sentinels
    static <V> int find(Comparator<V> comparator, Object key, Object[] a, final int fromIndex, final int toIndex)
    {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high)
        {
            int mid = (low + high) / 2;
            int cmp = comparator.compare((V) key, (V) a[mid]);

            if (cmp > 0)
                low = mid + 1;
            else if (cmp < 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    // get the upper bound we should search in for keys in the node
    static int getKeyEnd(Object[] node)
    {
        if (isLeaf(node))
            return getLeafKeyEnd(node);
        else
            return getBranchKeyEnd(node);
    }

    // get the last index that is non-null in the leaf node
    static int getLeafKeyEnd(Object[] node)
    {
        int len = node.length;
        if (len == 0)
            return 0;
        else if (node[len - 1] == null)
            return len - 1;
        else
            return len;
    }

    // return the boundary position between keys/children for the branch node
    static int getBranchKeyEnd(Object[] node)
    {
        return node.length / 2;
    }

    // returns true if the provided node is a leaf, false if it is a branch
    static boolean isLeaf(Object[] node)
    {
        return (node.length & 1) == 0;
    }

    public static boolean isEmpty(Object[] tree)
    {
        return tree.length == 0;
    }

    public static int depth(Object[] tree)
    {
        int depth = 1;
        while (!isLeaf(tree))
        {
            depth++;
            tree = (Object[]) tree[getKeyEnd(tree)];
        }
        return depth;
    }

    // Special class for making certain operations easier, so we can define a +/- Inf
    static interface Special extends Comparable<Object> { }
    static final Special POSITIVE_INFINITY = new Special()
    {
        public int compareTo(Object o)
        {
            return o == this ? 0 : 1;
        }
    };
    static final Special NEGATIVE_INFINITY = new Special()
    {
        public int compareTo(Object o)
        {
            return o == this ? 0 : -1;
        }
    };

    private static final ThreadLocal<Queue<Builder>> modifier = new ThreadLocal<Queue<Builder>>()
    {
        @Override
        protected Queue<Builder> initialValue()
        {
            return new ArrayDeque<>();
        }
    };

    // return a sorted collection
    private static <V> Collection<V> sorted(Iterable<V> source, Comparator<V> comparator, int size)
    {
        V[] vs = (V[]) new Object[size];
        int i = 0;
        for (V v : source)
            vs[i++] = v;
        Arrays.sort(vs, comparator);
        return Arrays.asList(vs);
    }

    /** simple static wrapper to calls to cmp.compare() which checks if either a or b are Special (i.e. represent an infinity) */
    // TODO : cheaper to check for POSITIVE/NEGATIVE infinity in callers, rather than here
    static <V> int compare(Comparator<V> cmp, Object a, Object b)
    {
        if (a instanceof Special)
            return ((Special) a).compareTo(b);
        if (b instanceof Special)
            return -((Special) b).compareTo(a);
        return cmp.compare((V) a, (V) b);
    }

    public static boolean isWellFormed(Object[] btree, Comparator<? extends Object> cmp)
    {
        return isWellFormed(cmp, btree, true, NEGATIVE_INFINITY, POSITIVE_INFINITY);
    }

    private static boolean isWellFormed(Comparator<?> cmp, Object[] node, boolean isRoot, Object min, Object max)
    {
        if (cmp != null && !isNodeWellFormed(cmp, node, min, max))
            return false;

        if (isLeaf(node))
        {
            if (isRoot)
                return node.length <= FAN_FACTOR;
            return node.length >= FAN_FACTOR / 2 && node.length <= FAN_FACTOR;
        }

        int type = 0;
        int childOffset = getBranchKeyEnd(node);
        // compare each child node with the branch element at the head of this node it corresponds with
        for (int i = childOffset; i < node.length; i++)
        {
            Object[] child = (Object[]) node[i];
            Object localmax = i < node.length - 1 ? node[i - childOffset] : max;
            if (!isWellFormed(cmp, child, false, min, localmax))
                return false;
            type |= isLeaf(child) ? 1 : 2;
            min = localmax;
        }
        return type < 3; // either all leaves or all branches but not a mix
    }

    private static boolean isNodeWellFormed(Comparator<?> cmp, Object[] node, Object min, Object max)
    {
        Object previous = min;
        int end = getKeyEnd(node);
        for (int i = 0; i < end; i++)
        {
            Object current = node[i];
            if (compare(cmp, previous, current) >= 0)
                return false;
            previous = current;
        }
        return compare(cmp, previous, max) < 0;
    }
}
