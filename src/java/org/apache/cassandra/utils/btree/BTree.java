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

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;

import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.caching.TinyThreadLocalPool;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.cassandra.config.CassandraRelevantProperties.BTREE_BRANCH_SHIFT;

public class BTree
{
    /**
     * The {@code BRANCH_FACTOR} is defined as the maximum number of children of each branch, with between
     * BRANCH_FACTOR/2-1 and BRANCH_FACTOR-1 keys being stored in every node. This yields a minimum tree size of
     * {@code (BRANCH_FACTOR/2)^height - 1} and a maximum tree size of {@code BRANCH_FACTOR^height - 1}.
     * <p>
     * Branches differ from leaves only in that they contain a suffix region containing the child nodes that occur
     * either side of the keys, and a sizeMap in the last position, permitting seeking by index within the tree.
     * Nodes are disambiguated by the length of the array that represents them: an even number is a branch, odd a leaf.
     * <p>
     * Leaf Nodes are represented by an odd-length array of keys, with the final element possibly null, i.e.
     * Object[V1, V2, ...,null?]
     * <p>
     * Branch nodes: Object[V1, V2, ..., child[&lt;V1.key], child[&lt;V2.key], ..., child[&lt; Inf], sizeMap]
     * Each child is either a branch or leaf, i.e., always an Object[].
     * The key elements in a branch node occupy the first half of the array (minus one)
     * <p>
     * BTrees are immutable; updating one returns a new tree that reuses unmodified nodes.
     * <p>
     * There are no references back to a parent node from its children (this would make it impossible to re-use
     * subtrees when modifying the tree, since the modified tree would need new parent references).
     * Instead, we store these references in a Path as needed when navigating the tree.
     */
    public static final int BRANCH_SHIFT = BTREE_BRANCH_SHIFT.getInt();

    private static final int BRANCH_FACTOR = 1 << BRANCH_SHIFT;
    public static final int MIN_KEYS = BRANCH_FACTOR / 2 - 1;
    public static final int MAX_KEYS = BRANCH_FACTOR - 1;

    // An empty BTree Leaf - which is the same as an empty BTree
    private static final Object[] EMPTY_LEAF = new Object[1];

    private static final int[][] DENSE_SIZE_MAPS = buildBalancedSizeMaps(BRANCH_SHIFT);
    private static final long[] PERFECT_DENSE_SIZE_ON_HEAP = sizeOnHeapOfPerfectTrees(BRANCH_SHIFT);

    /**
     * Represents the direction of iteration.
     */
    public enum Dir
    {
        ASC, DESC;

        public Dir invert()
        {
            return this == ASC ? DESC : ASC;
        }

        public static Dir desc(boolean desc)
        {
            return desc ? DESC : ASC;
        }
    }

    /**
     * Returns an empty BTree
     *
     * @return an empty BTree
     */
    public static Object[] empty()
    {
        return EMPTY_LEAF;
    }

    /**
     * Create a BTree containing only the specified object
     *
     * @return an new BTree containing only the specified object
     */
    public static Object[] singleton(Object value)
    {
        return new Object[]{ value };
    }

    /** @deprecated See CASSANDRA-15510 */
    @Deprecated(since = "4.0")
    public static <C, K extends C, V extends C> Object[] build(Collection<K> source)
    {
        return build(source, UpdateFunction.noOp());
    }

    /** @deprecated See CASSANDRA-15510 */
    @Deprecated(since = "4.0")
    public static <C, K extends C, V extends C> Object[] build(Collection<K> source, UpdateFunction<K, V> updateF)
    {
        return build(BulkIterator.of(source.iterator()), source.size(), updateF);
    }

    public static <C, I extends C, O extends C> Object[] build(BulkIterator<I> source, int size, UpdateFunction<I, O> updateF)
    {
        assert size >= 0;
        if (size == 0)
            return EMPTY_LEAF;

        if (size <= MAX_KEYS)
            return buildLeaf(source, size, updateF);

        return buildRoot(source, size, updateF);
    }

    /**
     * Build a leaf with {@code size} elements taken in bulk from {@code insert}, and apply {@code updateF} to these elements
     */
    private static <C, I extends C, O extends C> Object[] buildLeaf(BulkIterator<I> insert,
                                                                    int size,
                                                                    UpdateFunction<I, O> updateF)
    {
        Object[] values = new Object[size | 1]; // ensure that we have an odd-length array
        insert.fetch(values, 0, size);
        if (!isSimple(updateF))
        {
            updateF.onAllocatedOnHeap(ObjectSizes.sizeOfReferenceArray(values.length));
            for (int i = 0; i < size; i++)
                values[i] = updateF.insert((I) values[i]);
        }
        return values;
    }

    /**
     * Build a leaf with {@code size} elements taken in bulk from {@code insert}, and apply {@code updateF} to these elements
     * Do not invoke {@code updateF.onAllocated}.  Used by {@link #buildPerfectDenseWithoutSizeTracking} which
     * track the size for the entire tree they build in order to save on work.
     */
    private static <C, I extends C, O extends C> Object[] buildLeafWithoutSizeTracking(BulkIterator<I> insert, int size, UpdateFunction<I, O> updateF)
    {
        Object[] values = new Object[size | 1]; // ensure that we have an odd-length array
        insert.fetch(values, 0, size);
        if (!isSimple(updateF))
        {
            for (int i = 0; i < size; i++)
                values[i] = updateF.insert((I) values[i]);
        }
        return values;
    }

    /**
     * Build a root node from the first {@code size} elements from {@code source}, applying {@code updateF} to those elements.
     * A root node is permitted to have as few as two children, if a branch (i.e. if {@code size > MAX_SIZE}.
     */
    private static <C, I extends C, O extends C> Object[] buildRoot(BulkIterator<I> source, int size, UpdateFunction<I, O> updateF)
    {
        // first calculate the minimum height needed for this size of tree
        int height = minHeight(size);
        assert height > 1;
        assert height * BRANCH_SHIFT < 32;

        int denseChildSize = denseSize(height - 1);
        // Divide the size by the child size + 1, adjusting size by +1 to compensate for not having an upper key on the
        // last child and rounding up, i.e. (size + 1 + div - 1) / div == size / div + 1 where div = childSize + 1
        int childCount = size / (denseChildSize + 1) + 1;

        return buildMaximallyDense(source, childCount, size, height, updateF);
    }

    /**
     * Build a tree containing only dense nodes except at most two on any level. This matches the structure that
     * a FastBuilder would create, with some optimizations in constructing the dense nodes.
     * <p>
     * We do this by repeatedly constructing fully dense children until we reach a threshold, chosen so that we would
     * not be able to create another child with fully dense children and at least MIN_KEYS keys. After the threshold,
     * the remainder may fit a single node, or is otherwise split roughly halfway to create one child with at least
     * MIN_KEYS+1 fully dense children, and one that has at least MIN_KEYS-1 fully dense and up to two non-dense.
     */
    private static <C, I extends C, O extends C> Object[] buildMaximallyDense(BulkIterator<I> source,
                                                                              int childCount,
                                                                              int size,
                                                                              int height,
                                                                              UpdateFunction<I, O> updateF)
    {
        assert childCount <= MAX_KEYS + 1;

        int keyCount = childCount - 1;
        int[] sizeMap = new int[childCount];
        Object[] branch = new Object[childCount * 2];

        if (height == 2)
        {
            // we use the _exact same logic_ as below, only we invoke buildLeaf
            int remaining = size;
            int threshold = MAX_KEYS + 1 + MIN_KEYS;
            int i = 0;
            while (remaining >= threshold)
            {
                branch[keyCount + i] = buildLeaf(source, MAX_KEYS, updateF);
                branch[i] = isSimple(updateF) ? source.next() : updateF.insert(source.next());
                remaining -= MAX_KEYS + 1;
                sizeMap[i++] = size - remaining - 1;
            }
            if (remaining > MAX_KEYS)
            {
                int childSize = remaining / 2;
                branch[keyCount + i] = buildLeaf(source, childSize, updateF);
                branch[i] = isSimple(updateF) ? source.next() : updateF.insert(source.next());
                remaining -= childSize + 1;
                sizeMap[i++] = size - remaining - 1;
            }
            branch[keyCount + i] = buildLeaf(source, remaining, updateF);
            sizeMap[i++] = size;
            assert i == childCount;
        }
        else
        {
            --height;
            int denseChildSize = denseSize(height);
            int denseGrandChildSize = denseSize(height - 1);
            // The threshold is the point after which we can't add a dense child and still add another child with
            // at least MIN_KEYS fully dense children plus at least one more key.
            int threshold = denseChildSize + 1 + MIN_KEYS * (denseGrandChildSize + 1);
            int remaining = size;
            int i = 0;
            // Add dense children until we reach the threshold.
            while (remaining >= threshold)
            {
                branch[keyCount + i] = buildPerfectDense(source, height, updateF);
                branch[i] = isSimple(updateF) ? source.next() : updateF.insert(source.next());
                remaining -= denseChildSize + 1;
                sizeMap[i++] = size - remaining - 1;
            }
            // At this point the remainder either fits in one child, or too much for one but too little for one
            // perfectly dense and a second child with enough grandchildren to be valid. In the latter case, the
            // remainder should be split roughly in half, where the first child only has dense grandchildren.
            if (remaining > denseChildSize)
            {
                int grandChildCount = remaining / ((denseGrandChildSize + 1) * 2);
                assert grandChildCount >= MIN_KEYS + 1;
                int childSize = grandChildCount * (denseGrandChildSize + 1) - 1;
                branch[keyCount + i] = buildMaximallyDense(source, grandChildCount, childSize, height, updateF);
                branch[i] = isSimple(updateF) ? source.next() : updateF.insert(source.next());
                remaining -= childSize + 1;
                sizeMap[i++] = size - remaining - 1;
            }
            // Put the remainder in the last child, it is now guaranteed to fit and have the required minimum of children.
            int grandChildCount = remaining / (denseGrandChildSize + 1) + 1;
            assert grandChildCount >= MIN_KEYS + 1;
            int childSize = remaining;
            branch[keyCount + i] = buildMaximallyDense(source, grandChildCount, childSize, height, updateF);
            sizeMap[i++] = size;
            assert i == childCount;
        }

        branch[2 * keyCount + 1] = sizeMap;
        if (!isSimple(updateF))
            updateF.onAllocatedOnHeap(ObjectSizes.sizeOfArray(branch) + ObjectSizes.sizeOfArray(sizeMap));

        return branch;
    }

    private static <C, I extends C, O extends C> Object[] buildPerfectDense(BulkIterator<I> source, int height, UpdateFunction<I, O> updateF)
    {
        Object[] result = buildPerfectDenseWithoutSizeTracking(source, height, updateF);
        updateF.onAllocatedOnHeap(PERFECT_DENSE_SIZE_ON_HEAP[height]);
        return result;
    }

    /**
     * Build a tree of size precisely {@code branchFactor^height - 1}
     */
    private static <C, I extends C, O extends C> Object[] buildPerfectDenseWithoutSizeTracking(BulkIterator<I> source, int height, UpdateFunction<I, O> updateF)
    {
        int keyCount = (1 << BRANCH_SHIFT) - 1;
        Object[] node = new Object[(1 << BRANCH_SHIFT) * 2];

        if (height == 2)
        {
            int childSize = treeSize2n(1, BRANCH_SHIFT);
            for (int i = 0; i < keyCount; i++)
            {
                node[keyCount + i] = buildLeafWithoutSizeTracking(source, childSize, updateF);
                node[i] = isSimple(updateF) ? source.next() : updateF.insert(source.next());
            }
            node[2 * keyCount] = buildLeafWithoutSizeTracking(source, childSize, updateF);
        }
        else
        {
            for (int i = 0; i < keyCount; i++)
            {
                Object[] child = buildPerfectDenseWithoutSizeTracking(source, height - 1, updateF);
                node[keyCount + i] = child;
                node[i] = isSimple(updateF) ? source.next() : updateF.insert(source.next());
            }
            node[2 * keyCount] = buildPerfectDenseWithoutSizeTracking(source, height - 1, updateF);
        }
        node[keyCount * 2 + 1] = DENSE_SIZE_MAPS[height - 2];

        return node;
    }

    public static <Compare> Object[] update(Object[] toUpdate, Object[] insert, Comparator<? super Compare> comparator)
    {
        return BTree.<Compare, Compare, Compare>update(toUpdate, insert, comparator, UpdateFunction.noOp());
    }

    /**
     * Inserts {@code insert} into {@code update}, applying {@code updateF} to each new item in {@code insert},
     * as well as any matched items in {@code update}.
     * <p>
     * Note that {@code UpdateFunction.noOp} is assumed to indicate a lack of interest in which value survives.
     */
    public static <Compare, Existing extends Compare, Insert extends Compare> Object[] update(Object[] toUpdate,
                                                                                              Object[] insert,
                                                                                              Comparator<? super Compare> comparator,
                                                                                              UpdateFunction<Insert, Existing> updateF)
    {
        // perform some initial obvious optimisations
        if (isEmpty(insert))
            return toUpdate; // do nothing if update is empty


        if (isEmpty(toUpdate))
        {
            if (isSimple(updateF))
                return insert; // if update is empty and updateF is trivial, return our new input

            // if update is empty and updateF is non-trivial, perform a simple fast transformation of the input tree
            insert = BTree.transform(insert, updateF::insert);
            updateF.onAllocatedOnHeap(sizeOnHeapOf(insert));
            return insert;
        }

        if (isLeaf(toUpdate) && isLeaf(insert))
        {
            // if both are leaves, perform a tight-loop leaf variant of update
            // possibly flipping the input order if sizes suggest and updateF permits
            if (updateF == (UpdateFunction) UpdateFunction.noOp && toUpdate.length < insert.length)
            {
                Object[] tmp = toUpdate;
                toUpdate = insert;
                insert = tmp;
            }
            Object[] merged = updateLeaves(toUpdate, insert, comparator, updateF);
            updateF.onAllocatedOnHeap(sizeOnHeapOf(merged) - sizeOnHeapOf(toUpdate));
            return merged;
        }

        if (!isLeaf(insert) && isSimple(updateF))
        {
            // consider flipping the order of application, if update is much larger than insert and applying unary no-op
            int updateSize = size(toUpdate);
            int insertSize = size(insert);
            int scale = Integer.numberOfLeadingZeros(updateSize) - Integer.numberOfLeadingZeros(insertSize);
            if (scale >= 4)
            {
                // i.e. at roughly 16x the size, or one tier deeper - very arbitrary, should pick more carefully
                // experimentally, at least at 64x the size the difference in performance is ~10x
                Object[] tmp = insert;
                insert = toUpdate;
                toUpdate = tmp;
                if (updateF != (UpdateFunction) UpdateFunction.noOp)
                    updateF = ((UpdateFunction.Simple) updateF).flip();
            }
        }

        try (Updater<Compare, Existing, Insert> updater = Updater.get())
        {
            return updater.update(toUpdate, insert, comparator, updateF);
        }
    }

    /**
     * A fast tight-loop variant of updating one btree with another, when both are leaves.
     */
    public static <Compare, Existing extends Compare, Insert extends Compare> Object[] updateLeaves(Object[] unode,
                                                                                                    Object[] inode,
                                                                                                    Comparator<? super Compare> comparator,
                                                                                                    UpdateFunction<Insert, Existing> updateF)
    {
        int upos = -1, usz = sizeOfLeaf(unode);
        Existing uk = (Existing) unode[0];
        int ipos = 0, isz = sizeOfLeaf(inode);
        Insert ik = (Insert) inode[0];

        Existing merged = null;
        int c = -1;
        while (c <= 0) // optimistic: find the first point in the original leaf that is modified (if any)
        {
            if (c < 0)
            {
                upos = exponentialSearch(comparator, unode, upos + 1, usz, ik);
                c = upos < 0 ? 1 : 0; // positive or zero
                if (upos < 0)
                    upos = -(1 + upos);
                if (upos == usz)
                    break;
                uk = (Existing) unode[upos];
            }
            else // c == 0
            {
                merged = updateF.merge(uk, ik);
                if (merged != uk)
                    break;
                if (++ipos == isz)
                    return unode;
                if (++upos == usz)
                    break;
                c = comparator.compare(uk = (Existing) unode[upos], ik = (Insert) inode[ipos]);
            }
        }

        // exit conditions: c == 0 && merged != uk
        //              or: c >  0
        //              or: upos == usz

        try (FastBuilder<Existing> builder = fastBuilder())
        {
            if (upos > 0)
            {
                // copy any initial section that is unmodified
                builder.leaf().copy(unode, 0, upos);
            }

            // handle prior loop's exit condition
            // we always have either an ik, or an ik merged with uk, to handle
            if (upos < usz)
            {
                if (c == 0)
                {
                    builder.add(merged);
                    if (++upos < usz)
                        uk = (Existing) unode[upos];
                }
                else // c > 0
                {
                    builder.add(updateF.insert(ik));
                }
                if (++ipos < isz)
                    ik = (Insert) inode[ipos];

                if (upos < usz && ipos < isz)
                {
                    // note: this code is _identical_ to equivalent section in FastUpdater
                    c = comparator.compare(uk, ik);
                    while (true)
                    {
                        if (c == 0)
                        {
                            builder.leaf().addKey(updateF.merge(uk, ik));
                            ++upos;
                            ++ipos;
                            if (upos == usz || ipos == isz)
                                break;
                            c = comparator.compare(uk = (Existing) unode[upos], ik = (Insert) inode[ipos]);
                        }
                        else if (c < 0)
                        {
                            int until = exponentialSearch(comparator, unode, upos + 1, usz, ik);
                            c = until < 0 ? 1 : 0; // must find greater or equal; set >= 0 (equal) to 0; set < 0 (greater) to c=+ve
                            if (until < 0)
                                until = -(1 + until);
                            builder.leaf().copy(unode, upos, until - upos);
                            if ((upos = until) == usz)
                                break;
                            uk = (Existing) unode[upos];
                        }
                        else
                        {
                            int until = exponentialSearch(comparator, inode, ipos + 1, isz, uk);
                            c = until & 0x80000000; // must find less or equal; set >= 0 (equal) to 0, otherwise leave intact
                            if (until < 0)
                                until = -(1 + until);
                            builder.leaf().copy(inode, ipos, until - ipos, updateF);
                            if ((ipos = until) == isz)
                                break;
                            ik = (Insert) inode[ipos];
                        }
                    }
                }
                if (upos < usz)
                {
                    // ipos == isz
                    builder.leaf().copy(unode, upos, usz - upos);
                }
            }
            if (ipos < isz)
            {
                // upos == usz
                builder.leaf().copy(inode, ipos, isz - ipos, updateF);
            }
            return builder.build();
        }
    }

    public static void reverseInSitu(Object[] tree)
    {
        reverseInSitu(tree, height(tree), true);
    }

    /**
     * The internal implementation of {@link #reverseInSitu(Object[])}.
     * Takes two arguments that help minimise garbage generation, by testing sizeMaps against
     * known globallyl shared sizeMap for dense nodes that do not need to be modified, and
     * for permitting certain users (namely FastBuilder) to declare that non-matching sizeMap
     * can be mutated directly without allocating {@code new int[]}
     *
     * @param tree         the tree to reverse in situ
     * @param height       the height of the tree
     * @param copySizeMaps whether or not to copy any non-globally-shared sizeMap before reversing them
     */
    private static void reverseInSitu(Object[] tree, int height, boolean copySizeMaps)
    {
        if (isLeaf(tree))
        {
            reverse(tree, 0, sizeOfLeaf(tree));
        }
        else
        {
            int keyCount = shallowSizeOfBranch(tree);
            reverse(tree, 0, keyCount);
            reverse(tree, keyCount, keyCount * 2 + 1);
            for (int i = keyCount; i <= keyCount * 2; ++i)
                reverseInSitu((Object[]) tree[i], height - 1, copySizeMaps);

            int[] sizeMap = (int[]) tree[2 * keyCount + 1];
            if (sizeMap != DENSE_SIZE_MAPS[height - 2]) // no need to reverse a dense map; same in both directions
            {
                if (copySizeMaps)
                    sizeMap = sizeMap.clone();
                sizeMapToSizes(sizeMap);
                reverse(sizeMap, 0, sizeMap.length);
                sizesToSizeMap(sizeMap);
            }
        }
    }

    public static <V> Iterator<V> iterator(Object[] btree)
    {
        return iterator(btree, Dir.ASC);
    }

    public static <V> Iterator<V> iterator(Object[] btree, Dir dir)
    {
        return isLeaf(btree) ? new LeafBTreeSearchIterator<>(btree, null, dir)
                             : new FullBTreeSearchIterator<>(btree, null, dir);
    }

    public static <V> Iterator<V> iterator(Object[] btree, int lb, int ub, Dir dir)
    {
        return isLeaf(btree) ? new LeafBTreeSearchIterator<>(btree, null, dir, lb, ub)
                             : new FullBTreeSearchIterator<>(btree, null, dir, lb, ub);
    }

    public static <V> Iterable<V> iterable(Object[] btree)
    {
        return iterable(btree, Dir.ASC);
    }

    public static <V> Iterable<V> iterable(Object[] btree, Dir dir)
    {
        return () -> iterator(btree, dir);
    }

    public static <V> Iterable<V> iterable(Object[] btree, int lb, int ub, Dir dir)
    {
        return () -> iterator(btree, lb, ub, dir);
    }

    /**
     * Returns an Iterator over the entire tree
     *
     * @param btree the tree to iterate over
     * @param dir   direction of iteration
     * @param <V>
     * @return
     */
    public static <K, V> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, Dir dir)
    {
        return isLeaf(btree) ? new LeafBTreeSearchIterator<>(btree, comparator, dir)
                             : new FullBTreeSearchIterator<>(btree, comparator, dir);
    }

    /**
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param start      the beginning of the range to return, inclusive (in ascending order)
     * @param end        the end of the range to return, exclusive (in ascending order)
     * @param dir        if false, the iterator will start at the last item and move backwards
     * @return an Iterator over the defined sub-range of the tree
     */
    public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, K start, K end, Dir dir)
    {
        return slice(btree, comparator, start, true, end, false, dir);
    }

    /**
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param startIndex the start index of the range to return, inclusive
     * @param endIndex   the end index of the range to return, inclusive
     * @param dir        if false, the iterator will start at the last item and move backwards
     * @return an Iterator over the defined sub-range of the tree
     */
    public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, int startIndex, int endIndex, Dir dir)
    {
        return isLeaf(btree) ? new LeafBTreeSearchIterator<>(btree, comparator, dir, startIndex, endIndex)
                             : new FullBTreeSearchIterator<>(btree, comparator, dir, startIndex, endIndex);
    }

    /**
     * @param btree          the tree to iterate over
     * @param comparator     the comparator that defines the ordering over the items in the tree
     * @param start          low bound of the range
     * @param startInclusive inclusivity of lower bound
     * @param end            high bound of the range
     * @param endInclusive   inclusivity of higher bound
     * @param dir            direction of iteration
     * @return an Iterator over the defined sub-range of the tree
     */
    public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, K start, boolean startInclusive, K end, boolean endInclusive, Dir dir)
    {
        int inclusiveLowerBound = max(0,
                                      start == null ? Integer.MIN_VALUE
                                                    : startInclusive ? ceilIndex(btree, comparator, start)
                                                                     : higherIndex(btree, comparator, start));
        int inclusiveUpperBound = min(size(btree) - 1,
                                      end == null ? Integer.MAX_VALUE
                                                  : endInclusive ? floorIndex(btree, comparator, end)
                                                                 : lowerIndex(btree, comparator, end));
        return isLeaf(btree) ? new LeafBTreeSearchIterator<>(btree, comparator, dir, inclusiveLowerBound, inclusiveUpperBound)
                             : new FullBTreeSearchIterator<>(btree, comparator, dir, inclusiveLowerBound, inclusiveUpperBound);
    }

    /**
     * @return the item in the tree that sorts as equal to the search argument, or null if no such item
     */
    public static <V> V find(Object[] node, Comparator<? super V> comparator, V find)
    {
        while (true)
        {
            int keyEnd = getKeyEnd(node);
            int i = Arrays.binarySearch((V[]) node, 0, keyEnd, find, comparator);

            if (i >= 0)
                return (V) node[i];

            if (isLeaf(node))
                return null;

            i = -1 - i;
            node = (Object[]) node[keyEnd + i];
        }
    }

    /**
     * Modifies the provided btree directly. THIS SHOULD NOT BE USED WITHOUT EXTREME CARE as BTrees are meant to be immutable.
     * Finds and replaces the item provided by index in the tree.
     */
    public static <V> void replaceInSitu(Object[] tree, int index, V replace)
    {
        // WARNING: if semantics change, see also InternalCursor.seekTo, which mirrors this implementation
        if ((index < 0) | (index >= size(tree)))
            throw new IndexOutOfBoundsException(index + " not in range [0.." + size(tree) + ")");

        while (!isLeaf(tree))
        {
            final int[] sizeMap = getSizeMap(tree);
            int boundary = Arrays.binarySearch(sizeMap, index);
            if (boundary >= 0)
            {
                // exact match, in this branch node
                assert boundary < sizeMap.length - 1;
                tree[boundary] = replace;
                return;
            }

            boundary = -1 - boundary;
            if (boundary > 0)
            {
                assert boundary < sizeMap.length;
                index -= (1 + sizeMap[boundary - 1]);
            }
            tree = (Object[]) tree[getChildStart(tree) + boundary];
        }
        assert index < getLeafKeyEnd(tree);
        tree[index] = replace;
    }

    /**
     * Modifies the provided btree directly. THIS SHOULD NOT BE USED WITHOUT EXTREME CARE as BTrees are meant to be immutable.
     * Finds and replaces the provided item in the tree. Both should sort as equal to each other (although this is not enforced)
     */
    public static <V> void replaceInSitu(Object[] node, Comparator<? super V> comparator, V find, V replace)
    {
        while (true)
        {
            int keyEnd = getKeyEnd(node);
            int i = Arrays.binarySearch((V[]) node, 0, keyEnd, find, comparator);

            if (i >= 0)
            {
                assert find == node[i];
                node[i] = replace;
                return;
            }

            if (isLeaf(node))
                throw new NoSuchElementException();

            i = -1 - i;
            node = (Object[]) node[keyEnd + i];
        }
    }

    /**
     * Honours result semantics of {@link Arrays#binarySearch}, as though it were performed on the tree flattened into an array
     *
     * @return index of item in tree, or <tt>(-(<i>insertion point</i>) - 1)</tt> if not present
     */
    public static <V> int findIndex(Object[] node, Comparator<? super V> comparator, V find)
    {
        int lb = 0;
        while (true)
        {
            int keyEnd = getKeyEnd(node);
            int i = Arrays.binarySearch((V[]) node, 0, keyEnd, find, comparator);
            boolean exact = i >= 0;

            if (isLeaf(node))
                return exact ? lb + i : i - lb;

            if (!exact)
                i = -1 - i;

            int[] sizeMap = getSizeMap(node);
            if (exact)
                return lb + sizeMap[i];
            else if (i > 0)
                lb += sizeMap[i - 1] + 1;

            node = (Object[]) node[keyEnd + i];
        }
    }

    /**
     * @return the value at the index'th position in the tree, in tree order
     */
    public static <V> V findByIndex(Object[] tree, int index)
    {
        // WARNING: if semantics change, see also InternalCursor.seekTo, which mirrors this implementation
        if ((index < 0) | (index >= size(tree)))
            throw new IndexOutOfBoundsException(index + " not in range [0.." + size(tree) + ")");

        Object[] node = tree;
        while (true)
        {
            if (isLeaf(node))
            {
                int keyEnd = getLeafKeyEnd(node);
                assert index < keyEnd;
                return (V) node[index];
            }

            int[] sizeMap = getSizeMap(node);
            int boundary = Arrays.binarySearch(sizeMap, index);
            if (boundary >= 0)
            {
                // exact match, in this branch node
                assert boundary < sizeMap.length - 1;
                return (V) node[boundary];
            }

            boundary = -1 - boundary;
            if (boundary > 0)
            {
                assert boundary < sizeMap.length;
                index -= (1 + sizeMap[boundary - 1]);
            }
            node = (Object[]) node[getChildStart(node) + boundary];
        }
    }

    /* since we have access to binarySearch semantics within indexOf(), we can use this to implement
     * lower/upper/floor/higher very trivially
     *
     * this implementation is *not* optimal; it requires two logarithmic traversals, although the second is much cheaper
     * (having less height, and operating over only primitive arrays), and the clarity is compelling
     */

    public static <V> int lowerIndex(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = findIndex(btree, comparator, find);
        if (i < 0)
            i = -1 - i;
        return i - 1;
    }

    public static <V> V lower(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = lowerIndex(btree, comparator, find);
        return i >= 0 ? findByIndex(btree, i) : null;
    }

    public static <V> int floorIndex(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = findIndex(btree, comparator, find);
        if (i < 0)
            i = -2 - i;
        return i;
    }

    public static <V> V floor(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = floorIndex(btree, comparator, find);
        return i >= 0 ? findByIndex(btree, i) : null;
    }

    public static <V> int higherIndex(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = findIndex(btree, comparator, find);
        if (i < 0)
            i = -1 - i;
        else
            i++;
        return i;
    }

    public static <V> V higher(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = higherIndex(btree, comparator, find);
        return i < size(btree) ? findByIndex(btree, i) : null;
    }

    public static <V> int ceilIndex(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = findIndex(btree, comparator, find);
        if (i < 0)
            i = -1 - i;
        return i;
    }

    public static <V> V ceil(Object[] btree, Comparator<? super V> comparator, V find)
    {
        int i = ceilIndex(btree, comparator, find);
        return i < size(btree) ? findByIndex(btree, i) : null;
    }

    // UTILITY METHODS

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
        return node[len - 1] == null ? len - 1 : len;
    }

    // return the boundary position between keys/children for the branch node
    // == number of keys, as they are indexed from zero
    static int getBranchKeyEnd(Object[] branchNode)
    {
        return (branchNode.length / 2) - 1;
    }

    /**
     * @return first index in a branch node containing child nodes
     */
    static int getChildStart(Object[] branchNode)
    {
        return getBranchKeyEnd(branchNode);
    }

    /**
     * @return last index + 1 in a branch node containing child nodes
     */
    static int getChildEnd(Object[] branchNode)
    {
        return branchNode.length - 1;
    }

    /**
     * @return number of children in a branch node
     */
    static int getChildCount(Object[] branchNode)
    {
        return branchNode.length / 2;
    }

    /**
     * @return the size map for the branch node
     */
    static int[] getSizeMap(Object[] branchNode)
    {
        return (int[]) branchNode[getChildEnd(branchNode)];
    }

    /**
     * @return the size map for the branch node
     */
    static int lookupSizeMap(Object[] branchNode, int index)
    {
        return getSizeMap(branchNode)[index];
    }

    // get the size from the btree's index (fails if not present)
    public static int size(Object[] tree)
    {
        if (isLeaf(tree))
            return getLeafKeyEnd(tree);
        int length = tree.length;
        // length - 1 == getChildEnd == getPositionOfSizeMap
        // (length / 2) - 1 == getChildCount - 1 == position of full tree size
        // hard code this, as will be used often;
        return ((int[]) tree[length - 1])[(length / 2) - 1];
    }

    public static long sizeOfStructureOnHeap(Object[] tree)
    {
        if (tree == EMPTY_LEAF)
            return 0;

        long size = ObjectSizes.sizeOfArray(tree);
        if (isLeaf(tree))
            return size;
        for (int i = getChildStart(tree); i < getChildEnd(tree); i++)
            size += sizeOfStructureOnHeap((Object[]) tree[i]);
        return size;
    }

    /**
     * Checks is the node is a leaf.
     *
     * @return {@code true} if the provided node is a leaf, {@code false} if it is a branch.
     */
    public static boolean isLeaf(Object[] node)
    {
        // Nodes are disambiguated by the length of the array that represents them: an even number is a branch, odd a leaf
        return (node.length & 1) == 1;
    }

    public static boolean isEmpty(Object[] tree)
    {
        return tree == EMPTY_LEAF;
    }

    // get the upper bound we should search in for keys in the node
    static int shallowSize(Object[] node)
    {
        if (isLeaf(node))
            return sizeOfLeaf(node);
        else
            return shallowSizeOfBranch(node);
    }

    static int sizeOfLeaf(Object[] leaf)
    {
        int len = leaf.length;
        return leaf[len - 1] == null ? len - 1 : len;
    }

    // return the boundary position between keys/children for the branch node
    // == number of keys, as they are indexed from zero
    static int shallowSizeOfBranch(Object[] branch)
    {
        return (branch.length / 2) - 1;
    }

    /**
     * @return first index in a branch node containing child nodes
     */
    static int childOffset(Object[] branch)
    {
        return shallowSizeOfBranch(branch);
    }

    /**
     * @return last index + 1 in a branch node containing child nodes
     */
    static int childEndOffset(Object[] branch)
    {
        return branch.length - 1;
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

    /**
     * Fill the target array with the contents of the provided subtree, in ascending order, starting at targetOffset
     *
     * @param tree         source
     * @param target       array
     * @param targetOffset offset in target array
     * @return number of items copied (size of tree)
     */
    public static int toArray(Object[] tree, Object[] target, int targetOffset)
    {
        return toArray(tree, 0, size(tree), target, targetOffset);
    }

    public static int toArray(Object[] tree, int treeStart, int treeEnd, Object[] target, int targetOffset)
    {
        if (isLeaf(tree))
        {
            int count = treeEnd - treeStart;
            System.arraycopy(tree, treeStart, target, targetOffset, count);
            return count;
        }

        int newTargetOffset = targetOffset;
        int childCount = getChildCount(tree);
        int childOffset = getChildStart(tree);
        for (int i = 0; i < childCount; i++)
        {
            int childStart = treeIndexOffsetOfChild(tree, i);
            int childEnd = treeIndexOfBranchKey(tree, i);
            if (childStart <= treeEnd && childEnd >= treeStart)
            {
                newTargetOffset += toArray((Object[]) tree[childOffset + i], max(0, treeStart - childStart), min(childEnd, treeEnd) - childStart,
                                           target, newTargetOffset);
                if (treeStart <= childEnd && treeEnd > childEnd) // this check will always fail for the non-existent key
                    target[newTargetOffset++] = tree[i];
            }
        }
        return newTargetOffset - targetOffset;
    }

    /**
     * An efficient transformAndFilter implementation suitable for a tree consisting of a single leaf root
     * NOTE: codewise *identical* to {@link #transformAndFilterLeaf(Object[], BiFunction, Object)}
     */
    private static <I, O> Object[] transformAndFilterLeaf(Object[] leaf, Function<? super I, ? extends O> apply)
    {
        int i = 0, sz = sizeOfLeaf(leaf);
        I in;
        O out;
        do // optimistic loop, looking for first point transformation modifies the input (if any)
        {
            in = (I) leaf[i];
            out = apply.apply(in);
        } while (in == out && ++i < sz);

        // in == out -> i == sz
        // otherwise   in == leaf[i]
        int identicalUntil = i;

        if (out == null && ++i < sz)
        {
            // optimistic loop, looking for first key {@code apply} modifies without removing it (if any)
            do
            {
                in = (I) leaf[i];
                out = apply.apply(in);
            } while (null == out && ++i < sz);
        }
        // out == null -> i == sz
        // otherwise      out == apply.apply(leaf[i])

        if (i == sz)
        {
            // if we have reached the end of the input, we're either:
            //   1) returning input unmodified; or
            //   2) copying some (possibly empty) prefix of it

            if (identicalUntil == sz)
                return leaf;

            if (identicalUntil == 0)
                return empty();

            Object[] copy = new Object[identicalUntil | 1];
            System.arraycopy(leaf, 0, copy, 0, identicalUntil);
            return copy;
        }

        try (FastBuilder<O> builder = fastBuilder())
        {
            // otherwise copy the initial part that was unmodified, insert the non-null modified key, and continue
            if (identicalUntil > 0)
                builder.leaf().copyNoOverflow(leaf, 0, identicalUntil);
            builder.leaf().addKeyNoOverflow(out);

            while (++i < sz)
            {
                in = (I) leaf[i];
                out = apply.apply(in);
                if (out != null)
                    builder.leaf().addKeyNoOverflow(out);
            }

            return builder.build();
        }
    }

    /**
     * Takes a tree and transforms it using the provided function, filtering out any null results.
     * The result of any transformation must sort identically as their originals, wrt other results.
     * <p>
     * If no modifications are made, the original is returned.
     * NOTE: codewise *identical* to {@link #transformAndFilter(Object[], Function)}
     */
    public static <I, I2, O> Object[] transformAndFilter(Object[] tree, BiFunction<? super I, ? super I2, ? extends O> apply, I2 param)
    {
        if (isEmpty(tree))
            return tree;

        if (isLeaf(tree))
            return transformAndFilterLeaf(tree, apply, param);

        try (BiTransformer<I, I2, O> transformer = BiTransformer.get(apply, param))
        {
            return transformer.apply(tree);
        }
    }

    /**
     * Takes a tree and transforms it using the provided function, filtering out any null results.
     * The result of any transformation must sort identically as their originals, wrt other results.
     * <p>
     * If no modifications are made, the original is returned.
     * <p>
     * An efficient transformAndFilter implementation suitable for a tree consisting of a single leaf root
     * NOTE: codewise *identical* to {@link #transformAndFilter(Object[], BiFunction, Object)}
     */
    public static <I, O> Object[] transformAndFilter(Object[] tree, Function<? super I, ? extends O> apply)
    {
        if (isEmpty(tree))
            return tree;

        if (isLeaf(tree))
            return transformAndFilterLeaf(tree, apply);

        try (Transformer<I, O> transformer = Transformer.get(apply))
        {
            return transformer.apply(tree);
        }
    }

    /**
     * An efficient transformAndFilter implementation suitable for a tree consisting of a single leaf root
     * NOTE: codewise *identical* to {@link #transformAndFilterLeaf(Object[], Function)}
     */
    private static <I, I2, O> Object[] transformAndFilterLeaf(Object[] leaf, BiFunction<? super I, ? super I2, ? extends O> apply, I2 param)
    {
        int i = 0, sz = sizeOfLeaf(leaf);
        I in;
        O out;
        do // optimistic loop, looking for first point transformation modifies the input (if any)
        {
            in = (I) leaf[i];
            out = apply.apply(in, param);
        } while (in == out && ++i < sz);

        // in == out -> i == sz
        // otherwise   in == leaf[i]
        int identicalUntil = i;

        if (out == null && ++i < sz)
        {
            // optimistic loop, looking for first key {@code apply} modifies without removing it (if any)
            do
            {
                in = (I) leaf[i];
                out = apply.apply(in, param);
            } while (null == out && ++i < sz);
        }
        // out == null -> i == sz
        // otherwise      out == apply.apply(leaf[i])

        if (i == sz)
        {
            // if we have reached the end of the input, we're either:
            //   1) returning input unmodified; or
            //   2) copying some (possibly empty) prefix of it

            if (identicalUntil == sz)
                return leaf;

            if (identicalUntil == 0)
                return empty();

            Object[] copy = new Object[identicalUntil | 1];
            System.arraycopy(leaf, 0, copy, 0, identicalUntil);
            return copy;
        }

        try (FastBuilder<O> builder = fastBuilder())
        {
            // otherwise copy the initial part that was unmodified, insert the non-null modified key, and continue
            if (identicalUntil > 0)
                builder.leaf().copyNoOverflow(leaf, 0, identicalUntil);
            builder.leaf().addKeyNoOverflow(out);

            while (++i < sz)
            {
                in = (I) leaf[i];
                out = apply.apply(in, param);
                if (out != null)
                    builder.leaf().addKeyNoOverflow(out);
            }

            return builder.build();
        }
    }

    /**
     * Takes a tree and transforms it using the provided function.
     * The result of any transformation must sort identically as their originals, wrt other results.
     * <p>
     * If no modifications are made, the original is returned.
     */
    public static <I, O> Object[] transform(Object[] tree, Function<? super I, ? extends O> function)
    {
        if (isEmpty(tree)) // isEmpty determined by identity; must return input
            return tree;

        if (isLeaf(tree)) // escape hatch for fast leaf transformation
            return transformLeaf(tree, function);

        Object[] result = tree; // optimistically assume we'll return our input unmodified
        int keyCount = shallowSizeOfBranch(tree);
        for (int i = 0; i < keyCount; ++i)
        {
            // operate on a pair of (child,key) each loop
            Object[] curChild = (Object[]) tree[keyCount + i];
            Object[] updChild = transform(curChild, function);
            Object curKey = tree[i];
            Object updKey = function.apply((I) curKey);
            if (result == tree)
            {
                if (curChild == updChild && curKey == updKey)
                    continue; // if output still same as input, loop

                // otherwise initialise output to a copy of input up to this point
                result = transformCopyBranchHelper(tree, keyCount, i, i);
            }
            result[keyCount + i] = updChild;
            result[i] = updKey;
        }
        // final unrolled copy of loop for last child only (unbalanced with keys)
        Object[] curChild = (Object[]) tree[2 * keyCount];
        Object[] updChild = transform(curChild, function);
        if (result == tree)
        {
            if (curChild == updChild)
                return tree;
            result = transformCopyBranchHelper(tree, keyCount, keyCount, keyCount);
        }
        result[2 * keyCount] = updChild;
        result[2 * keyCount + 1] = tree[2 * keyCount + 1]; // take the original sizeMap, as we are exactly the same shape
        return result;
    }

    // create a copy of a branch, with the exact same size, copying the specified number of keys and children
    private static Object[] transformCopyBranchHelper(Object[] branch, int keyCount, int copyKeyCount, int copyChildCount)
    {
        Object[] result = new Object[branch.length];
        System.arraycopy(branch, 0, result, 0, copyKeyCount);
        System.arraycopy(branch, keyCount, result, keyCount, copyChildCount);
        return result;
    }

    // an efficient transformAndFilter implementation suitable for a tree consisting of a single leaf root
    private static <I, O> Object[] transformLeaf(Object[] leaf, Function<? super I, ? extends O> apply)
    {
        Object[] result = leaf; // optimistically assume we'll return our input unmodified
        int size = sizeOfLeaf(leaf);
        for (int i = 0; i < size; ++i)
        {
            Object current = leaf[i];
            Object updated = apply.apply((I) current);
            if (result == leaf)
            {
                if (current == updated)
                    continue; // if output still same as input, loop

                // otherwise initialise output to a copy of input up to this point
                result = new Object[leaf.length];
                System.arraycopy(leaf, 0, result, 0, i);
            }
            result[i] = updated;
        }
        return result;
    }

    public static boolean equals(Object[] a, Object[] b)
    {
        return size(a) == size(b) && Iterators.elementsEqual(iterator(a), iterator(b));
    }

    public static int hashCode(Object[] btree)
    {
        // we can't just delegate to Arrays.deepHashCode(),
        // because two equivalent trees may be represented by differently shaped trees
        int result = 1;
        for (Object v : iterable(btree))
            result = 31 * result + Objects.hashCode(v);
        return result;
    }

    public static String toString(Object[] btree)
    {
        return appendBranchOrLeaf(new StringBuilder().append('['), btree).append(']').toString();
    }

    private static StringBuilder appendBranchOrLeaf(StringBuilder builder, Object[] node)
    {
        return isLeaf(node) ? appendLeaf(builder, node) : appendBranch(builder, node);
    }

    private static StringBuilder appendBranch(StringBuilder builder, Object[] branch)
    {
        int childCount = branch.length / 2;
        int keyCount = childCount - 1;
        // add keys
        for (int i = 0; i < keyCount; i++)
        {
            if (i != 0)
                builder.append(", ");
            builder.append(branch[i]);
        }
        // add children
        for (int i = keyCount, m = branch.length - 1; i < m; i++)
        {
            builder.append(", ");
            appendBranchOrLeaf(builder, (Object[]) branch[i]);
        }
        // add sizeMap
        builder.append(", ").append(Arrays.toString((int[]) branch[branch.length - 1]));
        return builder;
    }

    private static StringBuilder appendLeaf(StringBuilder builder, Object[] leaf)
    {
        return builder.append(Arrays.toString(leaf));
    }

    /**
     * tree index => index of key wrt all items in the tree laid out serially
     * <p>
     * This version of the method permits requesting out-of-bounds indexes, -1 and size
     *
     * @param root     to calculate tree index within
     * @param keyIndex root-local index of key to calculate tree-index
     * @return the number of items preceding the key in the whole tree of root
     */
    public static int treeIndexOfKey(Object[] root, int keyIndex)
    {
        if (isLeaf(root))
            return keyIndex;
        int[] sizeMap = getSizeMap(root);
        if ((keyIndex >= 0) & (keyIndex < sizeMap.length))
            return sizeMap[keyIndex];
        // we support asking for -1 or size, so that we can easily use this for iterator bounds checking
        if (keyIndex < 0)
            return -1;
        return sizeMap[keyIndex - 1] + 1;
    }

    /**
     * @param keyIndex node-local index of the key to calculate index of
     * @return keyIndex; this method is here only for symmetry and clarity
     */
    public static int treeIndexOfLeafKey(int keyIndex)
    {
        return keyIndex;
    }

    /**
     * @param root     to calculate tree-index within
     * @param keyIndex root-local index of key to calculate tree-index of
     * @return the number of items preceding the key in the whole tree of root
     */
    public static int treeIndexOfBranchKey(Object[] root, int keyIndex)
    {
        return lookupSizeMap(root, keyIndex);
    }

    /**
     * @param root       to calculate tree-index within
     * @param childIndex root-local index of *child* to calculate tree-index of
     * @return the number of items preceding the child in the whole tree of root
     */
    public static int treeIndexOffsetOfChild(Object[] root, int childIndex)
    {
        if (childIndex == 0)
            return 0;
        return 1 + lookupSizeMap(root, childIndex - 1);
    }

    public static <V> Builder<V> builder(Comparator<? super V> comparator)
    {
        return new Builder<>(comparator);
    }

    public static <V> Builder<V> builder(Comparator<? super V> comparator, int initialCapacity)
    {
        return new Builder<>(comparator, initialCapacity);
    }

    public static class Builder<V>
    {
        // a user-defined bulk resolution, to be applied manually via resolve()
        public static interface Resolver
        {
            // can return a different output type to input, so long as sort order is maintained
            // if a resolver is present, this method will be called for every sequence of equal inputs
            // even those with only one item
            Object resolve(Object[] array, int lb, int ub);
        }

        // a user-defined resolver that is applied automatically on encountering two duplicate values
        public static interface QuickResolver<V>
        {
            // can return a different output type to input, so long as sort order is maintained
            // if a resolver is present, this method will be called for every sequence of equal inputs
            // even those with only one item
            V resolve(V a, V b);
        }

        Comparator<? super V> comparator;
        Object[] values;
        int count;
        boolean detected = true; // true if we have managed to cheaply ensure sorted (+ filtered, if resolver == null) as we have added
        boolean auto = true; // false if the user has promised to enforce the sort order and resolve any duplicates
        QuickResolver<V> quickResolver;

        protected Builder(Comparator<? super V> comparator)
        {
            this(comparator, 16);
        }

        protected Builder(Comparator<? super V> comparator, int initialCapacity)
        {
            if (initialCapacity == 0)
                initialCapacity = 16;
            this.comparator = comparator;
            this.values = new Object[initialCapacity];
        }

        @VisibleForTesting
        public Builder()
        {
            this.values = new Object[16];
        }

        private Builder(Builder<V> builder)
        {
            this.comparator = builder.comparator;
            this.values = Arrays.copyOf(builder.values, builder.values.length);
            this.count = builder.count;
            this.detected = builder.detected;
            this.auto = builder.auto;
            this.quickResolver = builder.quickResolver;
        }

        /**
         * Creates a copy of this {@code Builder}.
         *
         * @return a copy of this {@code Builder}.
         */
        public Builder<V> copy()
        {
            return new Builder<>(this);
        }

        public Builder<V> setQuickResolver(QuickResolver<V> quickResolver)
        {
            this.quickResolver = quickResolver;
            return this;
        }

        public void reuse()
        {
            reuse(comparator);
        }

        public void reuse(Comparator<? super V> comparator)
        {
            this.comparator = comparator;
            Arrays.fill(values, null);
            count = 0;
            detected = true;
        }

        public Builder<V> auto(boolean auto)
        {
            this.auto = auto;
            return this;
        }

        public Builder<V> add(V v)
        {
            if (count == values.length)
                values = Arrays.copyOf(values, count * 2);

            Object[] values = this.values;
            int prevCount = this.count++;
            values[prevCount] = v;

            if (auto && detected && prevCount > 0)
            {
                V prev = (V) values[prevCount - 1];
                int c = comparator.compare(prev, v);
                if (c == 0 && auto)
                {
                    count = prevCount;
                    if (quickResolver != null)
                        values[prevCount - 1] = quickResolver.resolve(prev, v);
                }
                else if (c > 0)
                {
                    detected = false;
                }
            }

            return this;
        }

        public Builder<V> addAll(Collection<V> add)
        {
            if (auto && add instanceof SortedSet && equalComparators(comparator, ((SortedSet) add).comparator()))
            {
                // if we're a SortedSet, permit quick order-preserving addition of items
                // if we collect all duplicates, don't bother as merge will necessarily be more expensive than sorting at end
                return mergeAll(add, add.size());
            }
            detected = false;
            if (values.length < count + add.size())
                values = Arrays.copyOf(values, max(count + add.size(), count * 2));
            for (V v : add)
                values[count++] = v;
            return this;
        }

        private static boolean equalComparators(Comparator<?> a, Comparator<?> b)
        {
            return a == b || (isNaturalComparator(a) && isNaturalComparator(b));
        }

        private static boolean isNaturalComparator(Comparator<?> a)
        {
            return a == null || a == Comparator.naturalOrder() || a == Ordering.natural();
        }

        // iter must be in sorted order!
        private Builder<V> mergeAll(Iterable<V> add, int addCount)
        {
            assert auto;
            // ensure the existing contents are in order
            autoEnforce();

            int curCount = count;
            // we make room for curCount * 2 + addCount, so that we can copy the current values to the end
            // if necessary for continuing the merge, and have the new values directly after the current value range
            if (values.length < curCount * 2 + addCount)
                values = Arrays.copyOf(values, max(curCount * 2 + addCount, curCount * 3));

            if (add instanceof BTreeSet)
            {
                // use btree set's fast toArray method, to append directly
                ((BTreeSet) add).toArray(values, curCount);
            }
            else
            {
                // consider calling toArray() and System.arraycopy
                int i = curCount;
                for (V v : add)
                    values[i++] = v;
            }
            return mergeAll(addCount);
        }

        private Builder<V> mergeAll(int addCount)
        {
            Object[] a = values;
            int addOffset = count;

            int i = 0, j = addOffset;
            int curEnd = addOffset, addEnd = addOffset + addCount;

            // save time in cases where we already have a subset, by skipping dir
            while (i < curEnd && j < addEnd)
            {
                V ai = (V) a[i], aj = (V) a[j];
                // in some cases, such as Columns, we may have identity supersets, so perform a cheap object-identity check
                int c = ai == aj ? 0 : comparator.compare(ai, aj);
                if (c > 0)
                    break;
                else if (c == 0)
                {
                    if (quickResolver != null)
                        a[i] = quickResolver.resolve(ai, aj);
                    j++;
                }
                i++;
            }

            if (j == addEnd)
                return this; // already a superset of the new values

            // otherwise, copy the remaining existing values to the very end, freeing up space for merge result
            int newCount = i;
            System.arraycopy(a, i, a, addEnd, count - i);
            curEnd = addEnd + (count - i);
            i = addEnd;

            while (i < curEnd && j < addEnd)
            {
                V ai = (V) a[i];
                V aj = (V) a[j];
                // could avoid one comparison if we cared, but would make this ugly
                int c = comparator.compare(ai, aj);
                if (c == 0)
                {
                    Object newValue = quickResolver == null ? ai : quickResolver.resolve(ai, aj);
                    a[newCount++] = newValue;
                    i++;
                    j++;
                }
                else
                {
                    a[newCount++] = c < 0 ? a[i++] : a[j++];
                }
            }

            // exhausted one of the inputs; fill in remainder of the other
            if (i < curEnd)
            {
                System.arraycopy(a, i, a, newCount, curEnd - i);
                newCount += curEnd - i;
            }
            else if (j < addEnd)
            {
                if (j != newCount)
                    System.arraycopy(a, j, a, newCount, addEnd - j);
                newCount += addEnd - j;
            }
            count = newCount;
            return this;
        }

        public boolean isEmpty()
        {
            return count == 0;
        }

        public Builder<V> reverse()
        {
            assert !auto;
            int mid = count / 2;
            for (int i = 0; i < mid; i++)
            {
                Object t = values[i];
                values[i] = values[count - (1 + i)];
                values[count - (1 + i)] = t;
            }
            return this;
        }

        public Builder<V> sort()
        {
            Arrays.sort((V[]) values, 0, count, comparator);
            return this;
        }

        // automatically enforce sorted+filtered
        private void autoEnforce()
        {
            if (!detected && count > 1)
            {
                sort();
                int prevIdx = 0;
                V prev = (V) values[0];
                for (int i = 1; i < count; i++)
                {
                    V next = (V) values[i];
                    if (comparator.compare(prev, next) != 0)
                        values[++prevIdx] = prev = next;
                    else if (quickResolver != null)
                        values[prevIdx] = prev = quickResolver.resolve(prev, next);
                }
                count = prevIdx + 1;
            }
            detected = true;
        }

        public Builder<V> resolve(Resolver resolver)
        {
            if (count > 0)
            {
                int c = 0;
                int prev = 0;
                for (int i = 1; i < count; i++)
                {
                    if (comparator.compare((V) values[i], (V) values[prev]) != 0)
                    {
                        values[c++] = resolver.resolve((V[]) values, prev, i);
                        prev = i;
                    }
                }
                values[c++] = resolver.resolve((V[]) values, prev, count);
                count = c;
            }
            return this;
        }

        public Object[] build()
        {
            if (auto)
                autoEnforce();
            try (BulkIterator<V> iterator = BulkIterator.of(values, 0))
            {
                return BTree.build(iterator, count, UpdateFunction.noOp());
            }
        }
    }

    private static <V, A> void applyValue(V value, BiConsumer<A, V> function, A argument)
    {
        function.accept(argument, value);
    }

    public static <V, A> void applyLeaf(Object[] btree, BiConsumer<A, V> function, A argument)
    {
        Preconditions.checkArgument(isLeaf(btree));
        int limit = getLeafKeyEnd(btree);
        for (int i = 0; i < limit; i++)
            applyValue((V) btree[i], function, argument);
    }

    /**
     * Simple method to walk the btree forwards and apply a function till a stop condition is reached
     * <p>
     * Private method
     *
     * @param btree
     * @param function
     */
    public static <V, A> void apply(Object[] btree, BiConsumer<A, V> function, A argument)
    {
        if (isLeaf(btree))
        {
            applyLeaf(btree, function, argument);
            return;
        }

        int childOffset = getChildStart(btree);
        int limit = btree.length - 1 - childOffset;
        for (int i = 0; i < limit; i++)
        {

            apply((Object[]) btree[childOffset + i], function, argument);

            if (i < childOffset)
                applyValue((V) btree[i], function, argument);
        }
    }

    /**
     * Simple method to walk the btree forwards and apply a function till a stop condition is reached
     * <p>
     * Private method
     *
     * @param btree
     * @param function
     */
    public static <V> void apply(Object[] btree, Consumer<V> function)
    {
        BTree.<V, Consumer<V>>apply(btree, Consumer::accept, function);
    }

    private static <V> int find(Object[] btree, V from, Comparator<V> comparator)
    {
        // find the start index in iteration order
        Preconditions.checkNotNull(comparator);
        int keyEnd = getKeyEnd(btree);
        return Arrays.binarySearch((V[]) btree, 0, keyEnd, from, comparator);
    }

    private static boolean isStopSentinel(long v)
    {
        return v == Long.MAX_VALUE;
    }

    private static <V, A> long accumulateLeaf(Object[] btree, BiLongAccumulator<A, V> accumulator, A arg, Comparator<V> comparator, V from, long initialValue)
    {
        Preconditions.checkArgument(isLeaf(btree));
        long value = initialValue;
        int limit = getLeafKeyEnd(btree);

        int startIdx = 0;
        if (from != null)
        {
            int i = find(btree, from, comparator);
            boolean isExact = i >= 0;
            startIdx = isExact ? i : (-1 - i);
        }

        for (int i = startIdx; i < limit; i++)
        {
            value = accumulator.apply(arg, (V) btree[i], value);

            if (isStopSentinel(value))
                break;
        }
        return value;
    }

    /**
     * Walk the btree and accumulate a long value using the supplied accumulator function. Iteration will stop if the
     * accumulator function returns the sentinel values Long.MIN_VALUE or Long.MAX_VALUE
     * <p>
     * If the optional from argument is not null, iteration will start from that value (or the one after it's insertion
     * point if an exact match isn't found)
     */
    public static <V, A> long accumulate(Object[] btree, BiLongAccumulator<A, V> accumulator, A arg, Comparator<V> comparator, V from, long initialValue)
    {
        if (isLeaf(btree))
            return accumulateLeaf(btree, accumulator, arg, comparator, from, initialValue);

        long value = initialValue;
        int childOffset = getChildStart(btree);

        int startChild = 0;
        if (from != null)
        {
            int i = find(btree, from, comparator);
            boolean isExact = i >= 0;

            startChild = isExact ? i + 1 : -1 - i;

            if (isExact)
            {
                value = accumulator.apply(arg, (V) btree[i], value);
                if (isStopSentinel(value))
                    return value;
                from = null;
            }
        }

        int limit = btree.length - 1 - childOffset;
        for (int i = startChild; i < limit; i++)
        {
            value = accumulate((Object[]) btree[childOffset + i], accumulator, arg, comparator, from, value);

            if (isStopSentinel(value))
                break;

            if (i < childOffset)
            {
                value = accumulator.apply(arg, (V) btree[i], value);
                // stop if a sentinel stop value was returned
                if (isStopSentinel(value))
                    break;
            }

            if (from != null)
                from = null;
        }
        return value;
    }

    public static <V> long accumulate(Object[] btree, LongAccumulator<V> accumulator, Comparator<V> comparator, V from, long initialValue)
    {
        return accumulate(btree, LongAccumulator::apply, accumulator, comparator, from, initialValue);
    }

    public static <V> long accumulate(Object[] btree, LongAccumulator<V> accumulator, long initialValue)
    {
        return accumulate(btree, accumulator, null, null, initialValue);
    }

    public static <V, A> long accumulate(Object[] btree, BiLongAccumulator<A, V> accumulator, A arg, long initialValue)
    {
        return accumulate(btree, accumulator, arg, null, null, initialValue);
    }

    /**
     * Calculate the minimum height needed for this size of tree
     *
     * @param size the tree size
     * @return the minimum height needed for this size of tree
     */
    private static int minHeight(int size)
    {
        return heightAtSize2n(size, BRANCH_SHIFT);
    }

    private static int heightAtSize2n(int size, int branchShift)
    {
        //    branch factor                 =  1 <<  branchShift
        // => full size at height            = (1 << (branchShift * height)) - 1
        // => full size at height + 1        =  1 << (branchShift * height)
        // => shift(full size at height + 1) = branchShift * height
        // => shift(full size at height + 1) / branchShift = height
        int lengthInBinary = 64 - Long.numberOfLeadingZeros(size);
        return (branchShift - 1 + lengthInBinary) / branchShift;
    }

    private static int[][] buildBalancedSizeMaps(int branchShift)
    {
        int count = (32 / branchShift) - 1;
        int childCount = 1 << branchShift;
        int[][] sizeMaps = new int[count][childCount];
        for (int height = 0; height < count; ++height)
        {
            int childSize = treeSize2n(height + 1, branchShift);
            int size = 0;
            int[] sizeMap = sizeMaps[height];
            for (int i = 0; i < childCount; ++i)
            {
                sizeMap[i] = size += childSize;
                size += 1;
            }
        }
        return sizeMaps;
    }

    // simply utility to reverse the contents of array[from..to)
    private static void reverse(Object[] array, int from, int to)
    {
        int mid = (from + to) / 2;
        for (int i = from; i < mid; i++)
        {
            int j = to - (1 + i - from);
            Object tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }
    }

    // simply utility to reverse the contents of array[from..to)
    private static void reverse(int[] array, int from, int to)
    {
        int mid = (from + to) / 2;
        for (int i = from; i < mid; i++)
        {
            int j = to - (1 + i - from);
            int tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }
    }

    /**
     * Mutate an array of child sizes into a cumulative sizeMap, returning the total size
     */
    private static int sizesToSizeMap(int[] sizeMap)
    {
        int total = sizeMap[0];
        for (int i = 1; i < sizeMap.length; ++i)
            sizeMap[i] = total += 1 + sizeMap[i];
        return total;
    }

    private static int sizesToSizeMap(int[] sizes, int count)
    {
        int total = sizes[0];
        for (int i = 1; i < count; ++i)
            sizes[i] = total += 1 + sizes[i];
        return total;
    }

    /**
     * Mutate an array of child sizes into a cumulative sizeMap, returning the total size
     */
    private static void sizeMapToSizes(int[] sizeMap)
    {
        for (int i = sizeMap.length; i > 1; --i)
            sizeMap[i] -= 1 + sizeMap[i - 1];
    }

    /**
     * A simple utility method to handle a null upper bound that we treat as infinity
     */
    private static <Compare> int compareWithMaybeInfinity(Comparator<? super Compare> comparator, Compare key, Compare ub)
    {
        if (ub == null)
            return -1;
        return comparator.compare(key, ub);
    }

    /**
     * Perform {@link #exponentialSearch} on {@code in[from..to)}, treating a {@code find} of {@code null} as infinity.
     */
    static <Compare> int exponentialSearchForMaybeInfinity(Comparator<? super Compare> comparator, Object[] in, int from, int to, Compare find)
    {
        if (find == null)
            return -(1 + to);
        return exponentialSearch(comparator, in, from, to, find);
    }

    /**
     * Equivalent to {@link Arrays#binarySearch}, only more efficient algorithmically for linear merges.
     * Binary search has worst case complexity {@code O(n.lg n)} for a linear merge, whereas exponential search
     * has a worst case of {@code O(n)}. However compared to a simple linear merge, the best case for exponential
     * search is {@code O(lg(n))} instead of {@code O(n)}.
     */
    private static <Compare> int exponentialSearch(Comparator<? super Compare> comparator, Object[] in, int from, int to, Compare find)
    {
        int step = 0;
        while (from + step < to)
        {
            int i = from + step;
            int c = comparator.compare(find, (Compare) in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c == 0)
                return i;
            from = i + 1;
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return Arrays.binarySearch((Compare[]) in, from, to, find, comparator);
    }

    /**
     * Perform {@link #exponentialSearch} on {@code in[from..to)}; if the value falls outside of the range of these
     * elements, test against {@code ub} as though it occurred at position {@code to}
     *
     * @return same as {@link Arrays#binarySearch} if {@code find} occurs in the range {@code [in[from]..in[to])};
     * otherwise the insertion position {@code -(1+to)} if {@code find} is less than {@code ub}, and {@code -(2+t)}
     * if it is greater than or equal to.
     * <p>
     * {@code ub} may be {@code null}, representing infinity.
     */
    static <Compare> int exponentialSearchWithUpperBound(Comparator<? super Compare> comparator, Object[] in, int from, int to, Compare ub, Compare find)
    {
        int step = 0;
        while (true)
        {
            int i = from + step;
            if (i >= to)
            {
                int c = compareWithMaybeInfinity(comparator, find, ub);
                if (c >= 0)
                    return -(2 + to);
                break;
            }
            int c = comparator.compare(find, (Compare) in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c == 0)
                return i;
            from = i + 1;
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return Arrays.binarySearch((Compare[]) in, from, to, find, comparator);
    }

    /**
     * Compute the size-in-bytes of full trees of cardinality {@code branchFactor^height - 1}
     */
    private static long[] sizeOnHeapOfPerfectTrees(int branchShift)
    {
        long[] result = new long[heightAtSize2n(Integer.MAX_VALUE, branchShift)];
        int branchFactor = 1 << branchShift;
        result[0] = branchFactor - 1;
        for (int i = 1; i < result.length; ++i)
            result[i] = sizeOnHeapOfPerfectTree(i + 1, branchFactor);
        return result;
    }

    /**
     * Compute the size-in-bytes of a full tree of cardinality {@code branchFactor^height - 1}
     * TODO: test
     */
    private static long sizeOnHeapOfPerfectTree(int height, int branchShift)
    {
        int branchFactor = 1 << branchShift;
        long branchSize = ObjectSizes.sizeOfReferenceArray(branchFactor * 2);
        int branchCount = height == 2 ? 1 : 2 + treeSize2n(height - 2, branchShift);
        long leafSize = ObjectSizes.sizeOfReferenceArray((branchFactor - 1) | 1);
        int leafCount = 1 + treeSize2n(height - 1, branchShift);
        return (branchSize * branchCount) + (leafSize * leafCount);
    }

    /**
     * @return the actual height of {@code tree}
     */
    public static int height(Object[] tree)
    {
        if (isLeaf(tree))
            return 1;

        int height = 1;
        while (!isLeaf(tree))
        {
            height++;
            tree = (Object[]) tree[shallowSizeOfBranch(tree)];
        }
        return height;
    }

    /**
     * @return the maximum representable size at {@code height}.
     */
    private static int denseSize(int height)
    {
        return treeSize2n(height, BRANCH_SHIFT);
    }

    /**
     * @return the maximum representable size at {@code height}.
     */
    private static int checkedDenseSize(int height)
    {
        assert height * BRANCH_SHIFT < 32;
        return denseSize(height);
    }

    /**
     * Computes the number of nodes in a full tree of height {@code height}
     * and with {@code 2^branchShift} branch factor.
     * i.e. computes {@code (2^branchShift)^height - 1}
     */
    private static int treeSize2n(int height, int branchShift)
    {
        return (1 << (branchShift * height)) - 1;
    }

    // TODO: test
    private static int maxRootHeight(int size)
    {
        if (size <= BRANCH_FACTOR)
            return 1;
        return 1 + heightAtSize2n((size - 1) / 2, BRANCH_SHIFT - 1);
    }

    private static int sizeOfBranch(Object[] branch)
    {
        int length = branch.length;
        // length - 1 == getChildEnd == getPositionOfSizeMap
        // (length / 2) - 1 == getChildCount - 1 == position of full tree size
        // hard code this, as will be used often;
        return ((int[]) branch[length - 1])[(length / 2) - 1];
    }

    /**
     * Checks if the UpdateFunction is an instance of {@code UpdateFunction.Simple}.
     */
    private static boolean isSimple(UpdateFunction<?, ?> updateF)
    {
        return updateF instanceof UpdateFunction.Simple;
    }

    /**
     * @return the size map for the branch node
     */
    static int[] sizeMap(Object[] branch)
    {
        return (int[]) branch[branch.length - 1];
    }

    public static long sizeOnHeapOf(Object[] tree)
    {
        if (isEmpty(tree))
            return 0;

        long size = ObjectSizes.sizeOfArray(tree);
        if (isLeaf(tree))
            return size;
        for (int i = childOffset(tree); i < childEndOffset(tree); i++)
            size += sizeOnHeapOf((Object[]) tree[i]);
        size += ObjectSizes.sizeOfArray(sizeMap(tree)); // may overcount, since we share size maps
        return size;
    }

    private static long sizeOnHeapOfLeaf(Object[] tree)
    {
        if (isEmpty(tree))
            return 0;

        return ObjectSizes.sizeOfArray(tree);
    }

    // Arbitrary boundaries
    private static Object POSITIVE_INFINITY = new Object();
    private static Object NEGATIVE_INFINITY = new Object();

    /**
     * simple static wrapper to calls to cmp.compare() which checks if either a or b are Special (i.e. represent an infinity)
     */
    private static <V> int compareWellFormed(Comparator<V> cmp, Object a, Object b)
    {
        if (a == b)
            return 0;
        if (a == NEGATIVE_INFINITY | b == POSITIVE_INFINITY)
            return -1;
        if (b == NEGATIVE_INFINITY | a == POSITIVE_INFINITY)
            return 1;
        return cmp.compare((V) a, (V) b);
    }

    public static boolean isWellFormed(Object[] btree, Comparator<?> cmp)
    {
        return isWellFormedReturnHeight(cmp, btree, true, NEGATIVE_INFINITY, POSITIVE_INFINITY) >= 0;
    }

    private static int isWellFormedReturnHeight(Comparator<?> cmp, Object[] node, boolean isRoot, Object min, Object max)
    {
        if (isEmpty(node))
            return 0;

        if (cmp != null && !isNodeWellFormed(cmp, node, min, max))
            return -1;

        int keyCount = shallowSize(node);
        if (keyCount < 1)
            return -1;
        if (!isRoot && keyCount < BRANCH_FACTOR / 2 - 1)
            return -1;
        if (keyCount >= BRANCH_FACTOR)
            return -1;

        if (isLeaf(node))
            return 0;

        int[] sizeMap = sizeMap(node);
        int size = 0;
        int childHeight = -1;
        // compare each child node with the branch element at the head of this node it corresponds with
        for (int i = childOffset(node); i < childEndOffset(node); i++)
        {
            Object[] child = (Object[]) node[i];
            Object localmax = i < node.length - 2 ? node[i - childOffset(node)] : max;
            int height = isWellFormedReturnHeight(cmp, child, false, min, localmax);
            if (height == -1)
                return -1;
            if (childHeight == -1)
                childHeight = height;
            if (childHeight != height)
                return -1;

            min = localmax;
            size += size(child);
            if (sizeMap[i - childOffset(node)] != size)
                return -1;
            size += 1;
        }

        return childHeight + 1;
    }

    private static boolean isNodeWellFormed(Comparator<?> cmp, Object[] node, Object min, Object max)
    {
        Object previous = min;
        int end = shallowSize(node);
        for (int i = 0; i < end; i++)
        {
            Object current = node[i];
            if (compareWellFormed(cmp, previous, current) >= 0)
                return false;

            previous = current;
        }
        return compareWellFormed(cmp, previous, max) < 0;
    }

    /**
     * Build a tree of unknown size, in order.
     * <p>
     * Can be used with {@link #reverseInSitu} to build a tree in reverse.
     */
    public static <V> FastBuilder<V> fastBuilder()
    {
        TinyThreadLocalPool.TinyPool<FastBuilder<?>> pool = FastBuilder.POOL.get();
        FastBuilder<V> builder = (FastBuilder<V>) pool.poll();
        if (builder == null)
            builder = new FastBuilder<>();
        builder.pool = pool;
        return builder;
    }

    /**
     * Base class for AbstractFastBuilder.BranchBuilder, LeafBuilder and AbstractFastBuilder,
     * containing shared behaviour and declaring some useful abstract methods.
     */
    private static abstract class LeafOrBranchBuilder
    {
        final int height;
        final LeafOrBranchBuilder child;
        BranchBuilder parent;

        /**
         * The current buffer contents (if any) of the leaf or branch - always sized to contain a complete
         * node of the form being constructed.  Always non-null, except briefly during overflow.
         */
        Object[] buffer;
        /**
         * The number of keys in our buffer, whether or not we are building a leaf or branch; if we are building
         * a branch, we will ordinarily have the same number of children as well, except temporarily when finishing
         * the construction of the node.
         */
        int count;

        /**
         * either
         * 1) an empty leftover buffer from a past usage, which can be used when we exhaust {@code buffer}; or
         * 2) a full {@code buffer} that has been parked until we next overflow, so we can steal some back
         * if we finish before reaching MIN_KEYS in {@code buffer}
         */
        Object[] savedBuffer;
        /**
         * The key we overflowed on when populating savedBuffer.  If null, {@link #savedBuffer} is logically empty.
         */
        Object savedNextKey;

        LeafOrBranchBuilder(LeafOrBranchBuilder child)
        {
            this.height = child == null ? 1 : 1 + child.height;
            this.child = child;
        }

        /**
         * Do we have enough keys in the builder to construct at least one balanced node?
         * We could have enough to build two.
         */
        final boolean isSufficient()
        {
            return hasOverflow() || count >= MIN_KEYS;
        }

        /**
         * Do we have an already constructed node saved, that we can propagate or redistribute?
         * This implies we are building two nodes, since {@link #savedNextKey} would overflow {@link #savedBuffer}
         */
        final boolean hasOverflow()
        {
            return savedNextKey != null;
        }

        /**
         * Do we have an already constructed node saved AND insufficient keys in our buffer, so
         * that we need to share the contents of {@link #savedBuffer} with {@link #buffer} to construct
         * our results?
         */
        final boolean mustRedistribute()
        {
            return hasOverflow() && count < MIN_KEYS;
        }

        /**
         * Are we empty, i.e. we have no contents in either {@link #buffer} or {@link #savedBuffer}
         */
        final boolean isEmpty()
        {
            return count == 0 && savedNextKey == null;
        }

        /**
         * Drain the contents of this builder and build up to two nodes, as necessary.
         * If {@code unode != null} and we are building a single node that is identical to it, use {@code unode} instead.
         * If {@code propagateTo != null} propagate any nodes we build to it.
         *
         * @return the last node we construct
         */
        abstract Object[] drainAndPropagate(Object[] unode, BranchBuilder propagateTo);

        /**
         * Drain the contents of this builder and build at most one node.
         * Requires {@code !hasOverflow()}
         *
         * @return the node we construct
         */
        abstract Object[] drain();

        /**
         * Complete the build. Drains the node and any used or newly-required parent and returns the root of the
         * resulting tree.
         *
         * @return the root of the constructed tree.
         */
        public Object[] completeBuild()
        {
            LeafOrBranchBuilder level = this;
            while (true)
            {
                if (!level.hasOverflow())
                    return level.drain();

                BranchBuilder parent = level.ensureParent();
                level.drainAndPropagate(null, parent);
                if (level.savedBuffer != null)
                    Arrays.fill(level.savedBuffer, null);
                level = parent;
            }
        }

        /**
         * Takes a node that would logically occur directly preceding the current buffer contents,
         * and the key that would separate them in a parent node, and prepends their contents
         * to the current buffer's contents.  This can be used to redistribute already-propagated
         * contents to a parent in cases where this is convenient (i.e. when transforming)
         *
         * @param predecessor        directly preceding node
         * @param predecessorNextKey key that would have separated predecessor from buffer contents
         */
        abstract void prepend(Object[] predecessor, Object predecessorNextKey);

        /**
         * Indicates if this builder produces dense nodes, i.e. those that are populated with MAX_KEYS
         * at every level.  Only the last two children of any branch may be non-dense, and in some cases only
         * the last two nodes in any tier of the tree.
         * <p>
         * This flag switches whether or not we maintain a buffer of sizes, or use the globally shared contents of
         * DENSE_SIZE_MAPS.
         */
        abstract boolean producesOnlyDense();

        /**
         * Ensure there is a {@code branch.parent}, and return it
         */
        final BranchBuilder ensureParent()
        {
            if (parent == null)
                parent = new BranchBuilder(this);
            parent.inUse = true;
            return parent;
        }

        /**
         * Mark a branch builder as utilised, so that we must clear it when resetting any {@link AbstractFastBuilder}
         *
         * @return {@code branch}
         */
        static BranchBuilder markUsed(BranchBuilder branch)
        {
            branch.inUse = true;
            return branch;
        }

        /**
         * A utility method for comparing a range of two arrays
         */
        static boolean areIdentical(Object[] a, int aOffset, Object[] b, int bOffset, int count)
        {
            for (int i = 0; i < count; ++i)
            {
                if (a[i + aOffset] != b[i + bOffset])
                    return false;
            }
            return true;
        }

        /**
         * A utility method for comparing a range of two arrays
         */
        static boolean areIdentical(int[] a, int aOffset, int[] b, int bOffset, int count)
        {
            for (int i = 0; i < count; ++i)
            {
                if (a[i + aOffset] != b[i + bOffset])
                    return false;
            }
            return true;
        }
    }

    /**
     * LeafBuilder for methods pertaining specifically to building a leaf in an {@link AbstractFastBuilder}.
     * Note that {@link AbstractFastBuilder} extends this class directly, however it is convenient to maintain
     * distinct classes in the hierarchy for clarity of behaviour and intent.
     */
    private static abstract class LeafBuilder extends LeafOrBranchBuilder
    {
        long allocated;

        LeafBuilder()
        {
            super(null);
            buffer = new Object[MAX_KEYS];
        }

        /**
         * Add {@code nextKey} to the buffer, overflowing if necessary
         */
        public void addKey(Object nextKey)
        {
            if (count == MAX_KEYS)
                overflow(nextKey);
            else
                buffer[count++] = nextKey;
        }

        /**
         * Add {@code nextKey} to the buffer; the caller specifying overflow is unnecessary
         */
        public void addKeyNoOverflow(Object nextKey)
        {
            buffer[count++] = nextKey;
        }

        /**
         * Add {@code nextKey} to the buffer; the caller specifying overflow is unnecessary
         */
        public void maybeAddKeyNoOverflow(Object nextKey)
        {
            buffer[count] = nextKey;
            count += nextKey != null ? 1 : 0;
        }

        /**
         * Add {@code nextKey} to the buffer; the caller specifying overflow is unnecessary
         */
        public void maybeAddKey(Object nextKey)
        {
            if (count == MAX_KEYS)
            {
                if (nextKey != null)
                    overflow(nextKey);
            }
            else
            {
                buffer[count] = nextKey;
                count += nextKey != null ? 1 : 0;
            }
        }

        /**
         * Copy the contents of {@code source[from..to)} to {@code buffer}, overflowing as necessary.
         */
        void copy(Object[] source, int offset, int length)
        {
            if (count + length > MAX_KEYS)
            {
                int copy = MAX_KEYS - count;
                System.arraycopy(source, offset, buffer, count, copy);
                offset += copy;
//              implicitly:  count = MAX_KEYS;
                overflow(source[offset++]);
                length -= 1 + copy;
            }
            System.arraycopy(source, offset, buffer, count, length);
            count += length;
        }

        /**
         * Copy the contents of {@code source[from..to)} to {@code buffer}; the caller specifying overflow is unnecessary
         */
        void copyNoOverflow(Object[] source, int offset, int length)
        {
            System.arraycopy(source, offset, buffer, count, length);
            count += length;
        }

        /**
         * Copy the contents of the data to {@code buffer}, overflowing as necessary.
         */
        <Insert, Existing> void copy(Object[] source, int offset, int length, UpdateFunction<Insert, Existing> apply)
        {
            if (isSimple(apply))
            {
                copy(source, offset, length);
                return;
            }

            if (count + length > MAX_KEYS)
            {
                int copy = MAX_KEYS - count;
                for (int i = 0; i < copy; ++i)
                    buffer[count + i] = apply.insert((Insert) source[offset + i]);
                offset += copy;
//              implicitly:  leaf().count = MAX_KEYS;
                overflow(apply.insert((Insert) source[offset++]));
                length -= 1 + copy;
            }

            for (int i = 0; i < length; ++i)
                buffer[count + i] = apply.insert((Insert) source[offset + i]);
            count += length;

        }

        /**
         * {@link #buffer} is full, and we need to make room either by populating {@link #savedBuffer},
         * propagating its current contents, if any, to {@link #parent}
         */
        void overflow(Object nextKey)
        {
            if (hasOverflow())
                propagateOverflow();

            // precondition: count == MAX_KEYS and savedNextKey == null

            Object[] newBuffer = savedBuffer;
            if (newBuffer == null)
                newBuffer = new Object[MAX_KEYS];

            savedBuffer = buffer;
            savedNextKey = nextKey;
            buffer = newBuffer;
            count = 0;
        }

        /**
         * Redistribute the contents of {@link #savedBuffer} into {@link #buffer}, finalise {@link #savedBuffer} and flush upwards.
         * Invoked when we are building from {@link #buffer}, have insufficient values but a complete leaf in {@link #savedBuffer}
         *
         * @return the size of the leaf we flushed to our parent from {@link #savedBuffer}
         */
        Object[] redistributeOverflowAndDrain()
        {
            Object[] newLeaf = redistributeAndDrain(savedBuffer, MAX_KEYS, savedNextKey);
            savedNextKey = null;
            return newLeaf;
        }

        /**
         * Redistribute the contents of {@link #buffer} and an immediate predecessor into a new leaf,
         * then construct a new predecessor with the remaining contents and propagate up to our parent
         * Invoked when we are building from {@link #buffer}, have insufficient values but either a complete
         * leaf in {@link #savedBuffer} or can exfiltrate one from our parent to redistribute.
         *
         * @return the second of the two new leaves
         */
        Object[] redistributeAndDrain(Object[] pred, int predSize, Object predNextKey)
        {
            // precondition: savedLeafCount == MAX_KEYS && leaf().count < MIN_KEYS
            // ensure we have at least MIN_KEYS in leaf().buffer
            // first shift leaf().buffer and steal some keys from leaf().savedBuffer and leaf().savedBufferNextKey
            int steal = MIN_KEYS - count;
            Object[] newLeaf = new Object[MIN_KEYS];
            System.arraycopy(pred, predSize - (steal - 1), newLeaf, 0, steal - 1);
            newLeaf[steal - 1] = predNextKey;
            System.arraycopy(buffer, 0, newLeaf, steal, count);

            // then create a leaf out of the remainder of savedBuffer
            int newPredecessorCount = predSize - steal;
            Object[] newPredecessor = new Object[newPredecessorCount | 1];
            System.arraycopy(pred, 0, newPredecessor, 0, newPredecessorCount);
            if (allocated >= 0)
                allocated += ObjectSizes.sizeOfReferenceArray(newPredecessorCount | 1);
            ensureParent().addChildAndNextKey(newPredecessor, newPredecessorCount, pred[newPredecessorCount]);
            return newLeaf;
        }

        /**
         * Invoked to fill our {@link #buffer} to >= MIN_KEYS with data ocurring before {@link #buffer};
         * possibly instead fills {@link #savedBuffer}
         *
         * @param pred        directly preceding node
         * @param predNextKey key that would have separated predecessor from buffer contents
         */
        void prepend(Object[] pred, Object predNextKey)
        {
            assert !hasOverflow();
            int predSize = sizeOfLeaf(pred);
            int newKeys = 1 + predSize;
            if (newKeys + count <= MAX_KEYS)
            {
                System.arraycopy(buffer, 0, buffer, newKeys, count);
                System.arraycopy(pred, 0, buffer, 0, predSize);
                buffer[predSize] = predNextKey;
                count += newKeys;
            }
            else
            {
                if (savedBuffer == null)
                    savedBuffer = new Object[MAX_KEYS];
                System.arraycopy(pred, 0, savedBuffer, 0, predSize);
                if (predSize == MAX_KEYS)
                {
                    savedNextKey = predNextKey;
                }
                else
                {
                    int removeKeys = MAX_KEYS - predSize;
                    count -= removeKeys;
                    savedBuffer[predSize] = predNextKey;
                    System.arraycopy(buffer, 0, savedBuffer, predSize + 1, MAX_KEYS - newKeys);
                    savedNextKey = buffer[MAX_KEYS - newKeys];
                    System.arraycopy(buffer, removeKeys, buffer, 0, count);
                }
            }
        }

        /**
         * Invoked when we want to add a key to the leaf buffer, but it is full
         */
        void propagateOverflow()
        {
            // propagate the leaf we have saved in savedBuffer
            // precondition: savedLeafCount == MAX_KEYS
            if (allocated >= 0)
                allocated += ObjectSizes.sizeOfReferenceArray(MAX_KEYS);
            ensureParent().addChildAndNextKey(savedBuffer, MAX_KEYS, savedNextKey);
            savedBuffer = null;
            savedNextKey = null;
        }

        /**
         * Construct a new leaf from the contents of {@link #buffer}, unless the contents have not changed
         * from {@code unode}, in which case return {@code unode} to avoid allocating unnecessary objects.
         *
         * This is only called when we have enough data to complete the node, i.e. we have MIN_KEYS or more items added
         * or the node is the BTree's root.
         */
        Object[] drainAndPropagate(Object[] unode, BranchBuilder propagateTo)
        {
            Object[] leaf;
            int sizeOfLeaf;
            if (mustRedistribute())
            {
                // we have too few items, so spread the two buffers across two new nodes
                leaf = redistributeOverflowAndDrain();
                sizeOfLeaf = MIN_KEYS;
            }
            else if (!hasOverflow() && unode != null && count == sizeOfLeaf(unode) && areIdentical(buffer, 0, unode, 0, count))
            {
                // we have exactly the same contents as the original node, so reuse it
                leaf = unode;
                sizeOfLeaf = count;
            }
            else
            {
                // we have maybe one saved full buffer, and one buffer with sufficient contents to copy
                if (hasOverflow())
                    propagateOverflow();

                sizeOfLeaf = count;
                leaf = drain();
                if (allocated >= 0 && sizeOfLeaf > 0)
                    allocated += ObjectSizes.sizeOfReferenceArray(sizeOfLeaf | 1) - (unode == null ? 0 : sizeOnHeapOfLeaf(unode));
            }

            count = 0;
            if (propagateTo != null)
                propagateTo.addChild(leaf, sizeOfLeaf);
            return leaf;
        }

        /**
         * Construct a new leaf from the contents of {@code leaf().buffer}, assuming that the node does not overflow.
         */
        Object[] drain()
        {
            // the number of children here may be smaller than MIN_KEYS if this is the root node
            assert !hasOverflow();
            if (count == 0)
                return empty();

            Object[] newLeaf = new Object[count | 1];
            System.arraycopy(buffer, 0, newLeaf, 0, count);
            count = 0;
            return newLeaf;
        }
    }

    static class BranchBuilder extends LeafOrBranchBuilder
    {
        final LeafBuilder leaf;

        /**
         * sizes of the children in {@link #buffer}. If null, we only produce dense nodes.
         */
        int[] sizes;
        /**
         * sizes of the children in {@link #savedBuffer}
         */
        int[] savedSizes;
        /**
         * marker to limit unnecessary work with unused levels, esp. on reset
         */
        boolean inUse;

        BranchBuilder(LeafOrBranchBuilder child)
        {
            super(child);
            buffer = new Object[2 * (MAX_KEYS + 1)];
            if (!child.producesOnlyDense())
                sizes = new int[MAX_KEYS + 1];
            this.leaf = child instanceof LeafBuilder ? (LeafBuilder) child : ((BranchBuilder) child).leaf;
        }

        /**
         * Ensure there is room to add another key to {@code branchBuffers[branchIndex]}, and add it;
         * invoke {@link #overflow} if necessary
         */
        void addKey(Object key)
        {
            if (count == MAX_KEYS)
                overflow(key);
            else
                buffer[count++] = key;
        }

        /**
         * To be invoked when there's a key already inserted to the buffer that requires a corresponding
         * right-hand child, for which the buffers are sized to ensure there is always room.
         */
        void addChild(Object[] child, int sizeOfChild)
        {
            buffer[MAX_KEYS + count] = child;
            recordSizeOfChild(sizeOfChild);
        }

        void recordSizeOfChild(int sizeOfChild)
        {
            if (sizes != null)
                sizes[count] = sizeOfChild;
        }

        /**
         * See {@link BranchBuilder#addChild(Object[], int)}
         */
        void addChild(Object[] child)
        {
            addChild(child, sizes == null ? 0 : size(child));
        }

        /**
         * Insert a new child into a parent branch, when triggered by {@code overflowLeaf} or {@code overflowBranch}
         */
        void addChildAndNextKey(Object[] newChild, int newChildSize, Object nextKey)
        {
            // we should always have room for a child to the right of any key we have previously inserted
            buffer[MAX_KEYS + count] = newChild;
            recordSizeOfChild(newChildSize);
            // but there may not be room for another key
            addKey(nextKey);
        }

        /**
         * Invoked when we want to add a key to the leaf buffer, but it is full
         */
        void propagateOverflow()
        {
            // propagate the leaf we have saved in leaf().savedBuffer
            if (leaf.allocated >= 0)
                leaf.allocated += ObjectSizes.sizeOfReferenceArray(2 * (1 + MAX_KEYS));
            int size = setOverflowSizeMap(savedBuffer, MAX_KEYS);
            ensureParent().addChildAndNextKey(savedBuffer, size, savedNextKey);
            savedBuffer = null;
            savedNextKey = null;
        }

        /**
         * Invoked when a branch already contains {@code MAX_KEYS}, and another child is ready to be added.
         * Creates a new neighbouring node containing MIN_KEYS items, shifting back the remaining MIN_KEYS+1
         * items to the start of the buffer(s).
         */
        void overflow(Object nextKey)
        {
            if (hasOverflow())
                propagateOverflow();

            Object[] restoreBuffer = savedBuffer;
            int[] restoreSizes = savedSizes;

            savedBuffer = buffer;
            savedSizes = sizes;
            savedNextKey = nextKey;

            sizes = restoreSizes == null && savedSizes != null ? new int[MAX_KEYS + 1] : restoreSizes;
            buffer = restoreBuffer == null ? new Object[2 * (MAX_KEYS + 1)] : restoreBuffer;
            count = 0;
        }

        /**
         * Redistribute the contents of branch.savedBuffer into branch.buffer, finalise savedBuffer and flush upwards.
         * Invoked when we are building from branch, have insufficient values but a complete branch in savedBuffer.
         *
         * @return the size of the branch we flushed to our parent from savedBuffer
         */
        Object[] redistributeOverflowAndDrain()
        {
            // now ensure we have at least MIN_KEYS in buffer
            // both buffer and savedBuffer should be balanced, so that we have count+1 and MAX_KEYS+1 children respectively
            // we need to utilise savedNextKey, so  we want to take {@code steal-1} keys from savedBuffer, {@code steal) children
            // and the dangling key we use in place of savedNextKey for our parent key.
            int steal = MIN_KEYS - count;
            Object[] newBranch = new Object[2 * (MIN_KEYS + 1)];
            System.arraycopy(savedBuffer, MAX_KEYS - (steal - 1), newBranch, 0, steal - 1);
            newBranch[steal - 1] = savedNextKey;
            System.arraycopy(buffer, 0, newBranch, steal, count);
            System.arraycopy(savedBuffer, 2 * MAX_KEYS + 1 - steal, newBranch, MIN_KEYS, steal);
            System.arraycopy(buffer, MAX_KEYS, newBranch, MIN_KEYS + steal, count + 1);
            setRedistributedSizeMap(newBranch, steal);

            // then create a branch out of the remainder of savedBuffer
            int savedBranchCount = MAX_KEYS - steal;
            Object[] savedBranch = new Object[2 * (savedBranchCount + 1)];
            System.arraycopy(savedBuffer, 0, savedBranch, 0, savedBranchCount);
            System.arraycopy(savedBuffer, MAX_KEYS, savedBranch, savedBranchCount, savedBranchCount + 1);
            int savedBranchSize = setOverflowSizeMap(savedBranch, savedBranchCount);
            if (leaf.allocated >= 0)
                leaf.allocated += ObjectSizes.sizeOfReferenceArray(2 * (1 + savedBranchCount));
            ensureParent().addChildAndNextKey(savedBranch, savedBranchSize, savedBuffer[savedBranchCount]);
            savedNextKey = null;

            return newBranch;
        }

        /**
         * See {@link LeafOrBranchBuilder#prepend(Object[], Object)}
         */
        void prepend(Object[] pred, Object predNextKey)
        {
            assert !hasOverflow();
            // assumes sizes != null, since only makes sense to use this method in that context

            int predKeys = shallowSizeOfBranch(pred);
            int[] sizeMap = (int[]) pred[2 * predKeys + 1];
            int newKeys = 1 + predKeys;
            if (newKeys + count <= MAX_KEYS)
            {
                System.arraycopy(buffer, 0, buffer, newKeys, count);
                System.arraycopy(sizes, 0, sizes, newKeys, count + 1);
                System.arraycopy(buffer, MAX_KEYS, buffer, MAX_KEYS + newKeys, count + 1);

                System.arraycopy(pred, 0, buffer, 0, predKeys);
                buffer[predKeys] = predNextKey;
                System.arraycopy(pred, predKeys, buffer, MAX_KEYS, predKeys + 1);
                copySizeMapToSizes(sizeMap, 0, sizes, 0, predKeys + 1);
                count += newKeys;
            }
            else
            {
                if (savedBuffer == null)
                {
                    savedBuffer = new Object[2 * (1 + MAX_KEYS)];
                    savedSizes = new int[1 + MAX_KEYS];
                }

                System.arraycopy(pred, 0, savedBuffer, 0, predKeys);
                System.arraycopy(pred, predKeys, savedBuffer, MAX_KEYS, predKeys + 1);
                copySizeMapToSizes(sizeMap, 0, savedSizes, 0, predKeys + 1);
                if (newKeys == MAX_KEYS + 1)
                {
                    savedNextKey = predNextKey;
                }
                else
                {
                    int removeKeys = (1 + MAX_KEYS - newKeys);
                    int remainingKeys = count - removeKeys;

                    savedBuffer[predKeys] = predNextKey;
                    System.arraycopy(buffer, 0, savedBuffer, newKeys, MAX_KEYS - newKeys);
                    savedNextKey = buffer[MAX_KEYS - newKeys];
                    System.arraycopy(sizes, 0, savedSizes, newKeys, MAX_KEYS + 1 - newKeys);
                    System.arraycopy(buffer, MAX_KEYS, savedBuffer, MAX_KEYS + newKeys, MAX_KEYS + 1 - newKeys);
                    System.arraycopy(buffer, removeKeys, buffer, 0, remainingKeys);
                    System.arraycopy(buffer, MAX_KEYS + removeKeys, buffer, MAX_KEYS, remainingKeys + 1);
                    System.arraycopy(sizes, removeKeys, sizes, 0, remainingKeys + 1);
                    count = remainingKeys;
                }
            }
        }

        boolean producesOnlyDense()
        {
            return sizes == null;
        }

        /**
         * Construct a new branch from the contents of {@code branchBuffers[branchIndex]}, unless the contents have
         * not changed from {@code unode}, in which case return {@code unode} to avoid allocating unnecessary objects.
         *
         * This is only called when we have enough data to complete the node, i.e. we have MIN_KEYS or more items added
         * or the node is the BTree's root.
         */
        Object[] drainAndPropagate(Object[] unode, BranchBuilder propagateTo)
        {
            int sizeOfBranch;
            Object[] branch;
            if (mustRedistribute())
            {
                branch = redistributeOverflowAndDrain();
                sizeOfBranch = sizeOfBranch(branch);
            }
            else
            {
                int usz = unode != null ? shallowSizeOfBranch(unode) : -1;
                if (!hasOverflow() && usz == count
                    && areIdentical(buffer, 0, unode, 0, usz)
                    && areIdentical(buffer, MAX_KEYS, unode, usz, usz + 1))
                {
                    branch = unode;
                    sizeOfBranch = sizeOfBranch(branch);
                }
                else
                {
                    if (hasOverflow())
                        propagateOverflow();

                    // the number of children here may be smaller than MIN_KEYS if this is the root node, but there must
                    // be at least one key / two children.
                    assert count > 0;
                    branch = new Object[2 * (count + 1)];
                    System.arraycopy(buffer, 0, branch, 0, count);
                    System.arraycopy(buffer, MAX_KEYS, branch, count, count + 1);
                    sizeOfBranch = setDrainSizeMap(unode, usz, branch, count);
                }
            }

            count = 0;
            if (propagateTo != null)
                propagateTo.addChild(branch, sizeOfBranch);

            return branch;
        }

        /**
         * Construct a new branch from the contents of {@code buffer}, assuming that the node does not overflow.
         */
        Object[] drain()
        {
            assert !hasOverflow();
            int keys = count;
            count = 0;

            Object[] branch = new Object[2 * (keys + 1)];
            if (keys == MAX_KEYS)
            {
                Object[] tmp = buffer;
                buffer = branch;
                branch = tmp;
            }
            else
            {
                System.arraycopy(buffer, 0, branch, 0, keys);
                System.arraycopy(buffer, MAX_KEYS, branch, keys, keys + 1);
            }
            setDrainSizeMap(null, -1, branch, keys);
            return branch;
        }

        /**
         * Compute (or fetch from cache) and set the sizeMap in {@code branch}, knowing that it
         * was constructed from for the contents of {@code buffer}.
         * <p>
         * For {@link FastBuilder} these are mostly the same, so they are fetched from a global cache and
         * resized accordingly, but for {@link AbstractUpdater} we maintain a buffer of sizes.
         */
        int setDrainSizeMap(Object[] original, int keysInOriginal, Object[] branch, int keysInBranch)
        {
            if (producesOnlyDense())
                return setImperfectSizeMap(branch, keysInBranch);

            // first convert our buffer contents of sizes to represent a sizeMap
            int size = sizesToSizeMap(this.sizes, keysInBranch + 1);
            // then attempt to reuse the sizeMap from the original node, by comparing the buffer's contents with it
            int[] sizeMap;
            if (keysInOriginal != keysInBranch || !areIdentical(sizeMap = sizeMap(original), 0, this.sizes, 0, keysInBranch + 1))
            {
                // if we cannot, then we either take the buffer wholesale and replace its buffer, or copy a prefix
                sizeMap = this.sizes;
                if (keysInBranch < MAX_KEYS)
                    sizeMap = Arrays.copyOf(sizeMap, keysInBranch + 1);
                else
                    this.sizes = new int[MAX_KEYS + 1];
            }
            branch[2 * keysInBranch + 1] = sizeMap;
            return size;
        }

        /**
         * Compute (or fetch from cache) and set the sizeMap in {@code branch}, knowing that it
         * was constructed from for the contents of {@code savedBuffer}.
         * <p>
         * For {@link FastBuilder} these are always the same size, so they are fetched from a global cache,
         * but for {@link AbstractUpdater} we maintain a buffer of sizes.
         *
         * @return the size of {@code branch}
         */
        int setOverflowSizeMap(Object[] branch, int keys)
        {
            if (producesOnlyDense())
            {
                int[] sizeMap = DENSE_SIZE_MAPS[height - 2];
                if (keys < MAX_KEYS)
                    sizeMap = Arrays.copyOf(sizeMap, keys + 1);
                branch[2 * keys + 1] = sizeMap;
                return keys < MAX_KEYS ? sizeMap[keys] : checkedDenseSize(height + 1);
            }
            else
            {
                int[] sizes = savedSizes;
                if (keys < MAX_KEYS)
                    sizes = Arrays.copyOf(sizes, keys + 1);
                else
                    savedSizes = null;
                branch[2 * keys + 1] = sizes;
                return sizesToSizeMap(sizes);
            }
        }

        /**
         * Compute (or fetch from cache) and set the sizeMap in {@code branch}, knowing that it
         * was constructed from the contents of both {@code savedBuffer} and {@code buffer}
         * <p>
         * For {@link FastBuilder} these are mostly the same size, so they are fetched from a global cache
         * and only the last items updated, but for {@link AbstractUpdater} we maintain a buffer of sizes.
         */
        void setRedistributedSizeMap(Object[] branch, int steal)
        {
            if (producesOnlyDense())
            {
                setImperfectSizeMap(branch, MIN_KEYS);
            }
            else
            {
                int[] sizeMap = new int[MIN_KEYS + 1];
                System.arraycopy(sizes, 0, sizeMap, steal, count + 1);
                System.arraycopy(savedSizes, MAX_KEYS + 1 - steal, sizeMap, 0, steal);
                branch[2 * MIN_KEYS + 1] = sizeMap;
                sizesToSizeMap(sizeMap);
            }
        }

        /**
         * Like {@link #setOverflowSizeMap}, but used for building the sizeMap of a node whose
         * last two children may have had their contents redistributed; uses the perfect size map
         * for all but the final two children, and queries the size of the last children directly
         */
        private int setImperfectSizeMap(Object[] branch, int keys)
        {
            int[] sizeMap = Arrays.copyOf(DENSE_SIZE_MAPS[height - 2], keys + 1);
            int size = keys == 1 ? 0 : 1 + sizeMap[keys - 2];
            sizeMap[keys - 1] = size += size((Object[]) branch[2 * keys - 1]);
            sizeMap[keys] = size += 1 + size((Object[]) branch[2 * keys]);
            branch[2 * keys + 1] = sizeMap;
            return size;
        }

        /**
         * Copy the contents of {@code unode} into {@code branchBuffers[branchIndex]},
         * starting at the child before key with index {@code offset} up to and
         * including the key with index {@code offset + length - 1}.
         */
        void copyPreceding(Object[] unode, int usz, int offset, int length)
        {
            int[] uszmap = sizeMap(unode);
            if (count + length > MAX_KEYS)
            {
                // we will overflow, so copy to MAX_KEYS and trigger overflow
                int copy = MAX_KEYS - count;
                copyPrecedingNoOverflow(unode, usz, uszmap, offset, copy);
                offset += copy;

                // copy last child that fits
                buffer[MAX_KEYS + MAX_KEYS] = unode[usz + offset];
                sizes[MAX_KEYS] = uszmap[offset] - (offset > 0 ? (1 + uszmap[offset - 1]) : 0);

                overflow(unode[offset]);

                length -= 1 + copy;
                ++offset;
            }

            copyPrecedingNoOverflow(unode, usz, uszmap, offset, length);
        }

        /**
         * Copy the contents of {@code unode} into {@code branchBuffers[branchIndex]},
         * between keys {@code from} and {@code to}, with the caller declaring overflow is unnecessary.
         * {@code from} may be {@code -1}, representing the first child only;
         * all other indices represent the key/child pairs that follow (i.e. a key and its right-hand child).
         */
        private void copyPrecedingNoOverflow(Object[] unode, int usz, int[] uszmap, int offset, int length)
        {
            if (length <= 1)
            {
                if (length == 0)
                    return;

                buffer[count] = unode[offset];
                buffer[MAX_KEYS + count] = unode[usz + offset];
                sizes[count] = uszmap[offset] - (offset > 0 ? (1 + uszmap[offset - 1]) : 0);
                ++count;
            }
            else
            {
                System.arraycopy(unode, offset, buffer, count, length);
                System.arraycopy(unode, usz + offset, buffer, MAX_KEYS + count, length);
                copySizeMapToSizes(uszmap, offset, sizes, count, length);
                count += length;
            }
        }

        /**
         * Copy a region of a cumulative sizeMap into an array of plain sizes
         */
        static void copySizeMapToSizes(int[] in, int inOffset, int[] out, int outOffset, int count)
        {
            assert count > 0;
            if (inOffset == 0)
            {
                // we don't need to subtract anything from the first node, so just copy it so we can keep the rest of the loop simple
                out[outOffset++] = in[inOffset++];
                --count;
            }
            for (int i = 0; i < count; ++i)
                out[outOffset + i] = in[inOffset + i] - (1 + in[inOffset + i - 1]);
        }
    }

    /**
     * Shared parent of {@link FastBuilder} and {@link Updater}, both of which
     * construct their trees in order without knowing the resultant size upfront.
     * <p>
     * Maintains a simple stack of buffers that we provide utilities to navigate and update.
     */
    private static abstract class AbstractFastBuilder extends LeafBuilder
    {
        final boolean producesOnlyDense()
        {
            return getClass() == FastBuilder.class;
        }

        /**
         * An aesthetic convenience for declaring when we are interacting with the leaf, instead of invoking {@code this} directly
         */
        final LeafBuilder leaf()
        {
            return this;
        }

        /**
         * Clear any references we might still retain, to avoid holding onto memory.
         * <p>
         * While this method is not strictly  necessary, it exists to
         * ensure the implementing classes are aware they must handle it.
         */
        abstract void reset();
    }

    /**
     * A pooled builder for constructing a tree in-order, and without needing any reconciliation.
     * <p>
     * Constructs whole nodes in place, so that a flush of a complete node can take its buffer entirely.
     * Since we build trees of a predictable shape (i.e. perfectly dense) we do not construct a size map.
     */
    public static class FastBuilder<V> extends AbstractFastBuilder implements AutoCloseable
    {
        private static final TinyThreadLocalPool<FastBuilder<?>> POOL = new TinyThreadLocalPool<>();
        private TinyThreadLocalPool.TinyPool<FastBuilder<?>> pool;

        FastBuilder()
        {
            allocated = -1;
        } // disable allocation tracking

        public void add(V value)
        {
            leaf().addKey(value);
        }

        public void add(Object[] from, int offset, int count)
        {
            leaf().copy(from, offset, count);
        }

        public Object[] build()
        {
            return leaf().completeBuild();
        }

        public Object[] buildReverse()
        {
            Object[] result = build();
            reverseInSitu(result, height(result), false);
            return result;
        }

        @Override
        public void close()
        {
            reset();
            pool.offer(this);
            pool = null;
        }

        @Override
        void reset()
        {
            // we clear precisely to leaf().count and branch.count because, in the case of a builder,
            // if we ever fill the buffer we will consume it entirely for the tree we are building
            // so the last count should match the number of non-null entries
            Arrays.fill(leaf().buffer, 0, leaf().count, null);
            leaf().count = 0;
            BranchBuilder branch = leaf().parent;
            while (branch != null && branch.inUse)
            {
                Arrays.fill(branch.buffer, 0, branch.count, null);
                Arrays.fill(branch.buffer, MAX_KEYS, MAX_KEYS + 1 + branch.count, null);
                branch.count = 0;
                branch.inUse = false;
                branch = branch.parent;
            }
        }
    }

    private static abstract class AbstractUpdater extends AbstractFastBuilder implements AutoCloseable
    {
        void reset()
        {
            assert leaf().count == 0;
            clearLeafBuffer(leaf().buffer);
            if (leaf().savedBuffer != null)
                Arrays.fill(leaf().savedBuffer, null);

            BranchBuilder branch = leaf().parent;
            while (branch != null && branch.inUse)
            {
                assert branch.count == 0;
                clearBranchBuffer(branch.buffer);
                if (branch.savedBuffer != null && branch.savedBuffer[0] != null)
                    Arrays.fill(branch.savedBuffer, null); // by definition full, if non-empty
                branch.inUse = false;
                branch = branch.parent;
            }
        }

        /**
         * Clear the contents of a branch buffer, aborting once we encounter a null entry
         * to save time on small trees
         */
        private void clearLeafBuffer(Object[] array)
        {
            if (array[0] == null)
                return;
            // find first null entry; loop from beginning, to amortise cost over size of working set
            int i = 1;
            while (i < array.length && array[i] != null)
                ++i;
            Arrays.fill(array, 0, i, null);
        }

        /**
         * Clear the contents of a branch buffer, aborting once we encounter a null entry
         * to save time on small trees
         */
        private void clearBranchBuffer(Object[] array)
        {
            if (array[0] == null)
                return;

            // find first null entry; loop from beginning, to amortise cost over size of working set
            int i = 1;
            while (i < MAX_KEYS && array[i] != null)
                ++i;
            Arrays.fill(array, 0, i, null);
            Arrays.fill(array, MAX_KEYS, MAX_KEYS + i + 1, null);
        }
    }

    /**
     * A pooled object for modifying an existing tree with a new (typically smaller) tree.
     * <p>
     * Constructs the new tree around the shape of the existing tree, as though we had performed inserts in
     * order, copying as much of the original tree as possible.  We achieve this by simply merging leaf nodes
     * up to the immediately following key in an ancestor, maintaining up to two complete nodes in a buffer until
     * this happens, and flushing any nodes we produce in excess of this immediately into the parent buffer.
     * <p>
     * We construct whole nodes in place, except the size map, so that a flush of a complete node can take its buffer
     * entirely.
     * <p>
     * Searches within both trees to accelerate the process of modification, instead of performing a simple
     * iteration over the new tree.
     */
    private static class Updater<Compare, Existing extends Compare, Insert extends Compare> extends AbstractUpdater implements AutoCloseable
    {
        static final TinyThreadLocalPool<Updater> POOL = new TinyThreadLocalPool<>();
        TinyThreadLocalPool.TinyPool<Updater> pool;

        // the new tree we navigate linearly, and are always on a key or at the end
        final SimpleTreeKeysIterator<Compare, Insert> insert = new SimpleTreeKeysIterator<>();

        Comparator<? super Compare> comparator;
        UpdateFunction<Insert, Existing> updateF;


        static <Compare, Existing extends Compare, Insert extends Compare> Updater<Compare, Existing, Insert> get()
        {
            TinyThreadLocalPool.TinyPool<Updater> pool = POOL.get();
            Updater<Compare, Existing, Insert> updater = pool.poll();
            if (updater == null)
                updater = new Updater<>();
            updater.pool = pool;
            return updater;
        }

        /**
         * Precondition: {@code update} should not be empty.
         * <p>
         * Inserts {@code insert} into {@code update}, after applying {@code updateF} to each item, or matched item pairs.
         */
        Object[] update(Object[] update, Object[] insert, Comparator<? super Compare> comparator, UpdateFunction<Insert, Existing> updateF)
        {
            this.insert.init(insert);
            this.updateF = updateF;
            this.comparator = comparator;
            this.allocated = isSimple(updateF) ? -1 : 0;
            int leafDepth = BTree.depth(update) - 1;
            LeafOrBranchBuilder builder = leaf();
            for (int i = 0; i < leafDepth; ++i)
                builder = builder.ensureParent();

            Insert ik = this.insert.next();
            ik = updateRecursive(ik, update, null, builder);
            assert ik == null;
            Object[] result = builder.completeBuild();

            if (allocated > 0)
                updateF.onAllocatedOnHeap(allocated);

            return result;
        }

        /**
         * Merge a BTree recursively with the contents of {@code insert} up to the given upper bound.
         *
         * @param ik      The next key from the inserted data.
         * @param unode   The source branch to update.
         * @param uub     The branch's upper bound
         * @param builder The builder that will receive the data. It needs to be at the same level of the hierarchy
         *                as the source unode.
         * @return The next key from the inserted data, >= uub.
         */
        private Insert updateRecursive(Insert ik, Object[] unode, Existing uub, LeafOrBranchBuilder builder)
        {
            return builder == leaf()
                   ? updateRecursive(ik, unode, uub, (LeafBuilder) builder)
                   : updateRecursive(ik, unode, uub, (BranchBuilder) builder);
        }

        private Insert updateRecursive(Insert ik, Object[] unode, Existing uub, BranchBuilder builder)
        {
            int upos = 0;
            int usz = shallowSizeOfBranch(unode);

            while (ik != null)
            {
                int find = exponentialSearchWithUpperBound(comparator, unode, upos, usz, uub, ik);
                int c = searchResultToComparison(find);
                if (find < 0)
                    find = -1 - find;

                if (find > usz)
                    break;  // nothing else needs to be inserted in this branch
                if (find > upos)
                    builder.copyPreceding(unode, usz, upos, find - upos);

                final Existing nextUKey = find < usz ? (Existing) unode[find] : uub;
                final Object[] childUNode = (Object[]) unode[find + usz];

                // process next child
                if (c < 0)
                {
                    // ik fall inside it -- recursively merge the child with the update, using next key as an upper bound
                    LeafOrBranchBuilder childBuilder = builder.child;
                    ik = updateRecursive(ik, childUNode, nextUKey, childBuilder);
                    childBuilder.drainAndPropagate(childUNode, builder);
                    if (find == usz)    // this was the right-most child, branch is complete and we can return immediately
                        return ik;
                    c = ik != null ? comparator.compare(nextUKey, ik) : -1;
                }
                else
                    builder.addChild(childUNode);

                // process next key
                if (c == 0)
                {
                    // ik matches next key
                    builder.addKey(updateF.merge(nextUKey, ik));
                    ik = insert.next();
                }
                else
                    builder.addKey(nextUKey);

                upos = find + 1;
            }
            // copy the rest of the branch and exit
            if (upos <= usz)
            {
                builder.copyPreceding(unode, usz, upos, usz - upos);
                builder.addChild((Object[]) unode[usz + usz]);
            }

            return ik;
        }

        private Insert updateRecursive(Insert ik, Object[] unode, Existing uub, LeafBuilder builder)
        {
            int upos = 0;
            int usz = sizeOfLeaf(unode);
            Existing uk = (Existing) unode[upos];
            int c = comparator.compare(uk, ik);

            while (true)
            {
                if (c == 0)
                {
                    leaf().addKey(updateF.merge(uk, ik));
                    if (++upos < usz)
                        uk = (Existing) unode[upos];
                    ik = insert.next();
                    if (ik == null)
                    {
                        builder.copy(unode, upos, usz - upos);
                        return null;
                    }
                    if (upos == usz)
                        break;
                    c = comparator.compare(uk, ik);
                }
                else if (c < 0)
                {
                    int ulim = exponentialSearch(comparator, unode, upos + 1, usz, ik);
                    c = -searchResultToComparison(ulim); // 0 if match, 1 otherwise
                    if (ulim < 0)
                        ulim = -(1 + ulim);
                    builder.copy(unode, upos, ulim - upos);
                    if ((upos = ulim) == usz)
                        break;
                    uk = (Existing) unode[upos];
                }
                else
                {
                    builder.addKey(isSimple(updateF) ? ik : updateF.insert(ik));
                    c = insert.copyKeysSmallerThan(uk, comparator, builder, updateF); // 0 on match, -1 otherwise
                    ik = insert.next();
                    if (ik == null)
                    {
                        builder.copy(unode, upos, usz - upos);
                        return null;
                    }
                }
            }
            if (uub == null || comparator.compare(ik, uub) < 0)
            {
                builder.addKey(isSimple(updateF) ? ik : updateF.insert(ik));
                insert.copyKeysSmallerThan(uub, comparator, builder, updateF); // 0 on match, -1 otherwise
                ik = insert.next();
            }
            return ik;
        }


        public void close()
        {
            reset();
            pool.offer(this);
            pool = null;
        }

        void reset()
        {
            super.reset();
            insert.reset();
        }
    }

    static int searchResultToComparison(int searchResult)
    {
        return searchResult >> 31;
    }

    /**
     * Attempts to perform a clean transformation of the original tree into a new tree,
     * by replicating its original shape as far as possible.
     * <p>
     * We do this by attempting to flush our buffers whenever we finish a source-branch at the given level;
     * if there are too few contents, we wait until we finish another node at the same level.
     * <p>
     * This way, we are always resetting at the earliest point we might be able to reuse more parts of the original
     * tree, maximising potential reuse.
     * <p>
     * This can permit us to build unbalanced right-most nodes at each level, in which case we simply rebalance
     * when done.
     * <p>
     * The approach taken here hopefully balances simplicity, garbage generation and execution time.
     */
    private static abstract class AbstractTransformer<I, O> extends AbstractUpdater implements AutoCloseable
    {
        /**
         * An iterator over the tree we are updating
         */
        final SimpleTreeIterator update = new SimpleTreeIterator();

        /**
         * A queue of nodes from update that we are ready to "finish" if we have buffered enough data from them
         * The stack pointer is maintained inside of {@link #apply()}
         */
        Object[][] queuedToFinish = new Object[1][];

        AbstractTransformer()
        {
            allocated = -1;
            ensureParent();
            parent.inUse = false;
        }

        abstract O apply(I v);

        Object[] apply(Object[] update)
        {
            int height = this.update.init(update);
            if (queuedToFinish.length < height - 1)
                queuedToFinish = new Object[height - 1][];
            return apply();
        }

        /**
         * We base our operation on the shape of {@code update}, trying to steal as much of the original tree as
         * possible for our new tree
         */
        private Object[] apply()
        {
            Object[] unode = update.node();
            int upos = update.position(), usz = sizeOfLeaf(unode);

            while (true)
            {
                // we always start the loop on a leaf node, for both input and output
                boolean propagatedOriginalLeaf = false;
                if (leaf().count == 0)
                {
                    if (upos == 0)
                    {   // fast path - buffer is empty and input unconsumed, so may be able to propagate original
                        I in;
                        O out;
                        do
                        {   // optimistic loop - find first point the transformation modified our input
                            in = (I) unode[upos];
                            out = apply(in);
                        } while (in == out && ++upos < usz);

                        if ((propagatedOriginalLeaf = (upos == usz)))
                        {
                            // if input is unmodified by transformation, propagate the input node
                            markUsed(parent).addChild(unode, usz);
                        }
                        else
                        {
                            // otherwise copy up to the first modified portion,
                            // and fall-through to our below condition for transforming the remainder
                            leaf().copyNoOverflow(unode, 0, upos++);
                            if (out != null)
                                leaf().addKeyNoOverflow(out);
                        }
                    }

                    if (!propagatedOriginalLeaf)
                        transformLeafNoOverflow(unode, upos, usz);
                }
                else
                {
                    transformLeaf(unode, upos, usz);
                }

                // we've finished a leaf, and have to hand it to a parent alongside its right-hand key
                // so now we try to do two things:
                //    1) find the next unfiltered key from our unfinished parent
                //    2) determine how many parents are "finished" and whose buffers we should also attempt to propagate
                // we do (1) unconditionally, because:
                //    a) we need to handle the branch keys somewhere, and it may as well happen in one place
                //    b) we either need more keys for our incomplete leaf; or
                //    c) we need a key to go after our last propagated node in any unfinished parent

                int finishToHeight = 0;
                O nextKey;
                do
                {
                    update.ascendToParent(); // always have a node above leaf level, else we'd invoke transformLeaf
                    BranchBuilder level = parent;
                    unode = update.node();
                    upos = update.position();
                    usz = shallowSizeOfBranch(unode);

                    while (upos == usz)
                    {
                        queuedToFinish[level.height - 2] = unode;
                        finishToHeight = max(finishToHeight, level.height);

                        if (!update.ascendToParent())
                            return finishAndDrain(propagatedOriginalLeaf);

                        level = level.ensureParent();
                        unode = update.node();
                        upos = update.position();
                        usz = shallowSizeOfBranch(unode);
                    }

                    nextKey = apply((I) unode[upos]);
                    if (nextKey == null && leaf().count > MIN_KEYS) // if we don't have a key, try to steal from leaf().buffer
                        nextKey = (O) leaf().buffer[--leaf().count];

                    update.descendIntoNextLeaf(unode, upos, usz);
                    unode = update.node();
                    upos = update.position();
                    usz = sizeOfLeaf(unode);

                    // nextKey might have been filtered, so we may need to look in this next leaf for it
                    while (nextKey == null && upos < usz)
                        nextKey = apply((I) unode[upos++]);

                    // if we still found no key loop and try again on the next parent, leaf, parent... ad infinitum
                } while (nextKey == null);

                // we always end with unode a leaf, though it may be that upos == usz and that we will do nothing with it

                // we've found a non-null key, now decide what to do with it:
                //   1) if we have insufficient keys in our leaf, simply append to the leaf and continue;
                //   2) otherwise, walk our parent branches finishing those *before* {@code finishTo}
                //   2a) if any cannot be finished, append our new key to it and stop finishing further parents; they
                //   will be finished the next time we ascend to their level with a complete chain of finishable branches
                //   2b) otherwise, add our new key to {@code finishTo}

                if (!propagatedOriginalLeaf && !finish(leaf(), null))
                {
                    leaf().addKeyNoOverflow(nextKey);
                    continue;
                }

                BranchBuilder finish = parent;
                while (true)
                {
                    if (finish.height <= finishToHeight)
                    {
                        Object[] originalNode = queuedToFinish[finish.height - 2];
                        if (finish(finish, originalNode))
                        {
                            finish = finish.parent;
                            continue;
                        }
                    }

                    // add our key to the last unpropagated parent branch buffer
                    finish.addKey(nextKey);
                    break;
                }
            }
        }

        private void transformLeafNoOverflow(Object[] unode, int upos, int usz)
        {
            while (upos < usz)
            {
                O v = apply((I) unode[upos++]);
                leaf().maybeAddKeyNoOverflow(v);
            }
        }

        private void transformLeaf(Object[] unode, int upos, int usz)
        {
            while (upos < usz)
            {
                O v = apply((I) unode[upos++]);
                leaf().maybeAddKey(v);
            }
        }

        /**
         * Invoked when we are finished transforming a branch.  If the buffer contains insufficient elements,
         * we refuse to construct a leaf and return null.  Otherwise we propagate the branch to its parent's buffer
         * and return the branch we have constructed.
         */
        private boolean finish(LeafOrBranchBuilder level, Object[] unode)
        {
            if (!level.isSufficient())
                return false;

            level.drainAndPropagate(unode, level.ensureParent());
            return true;
        }

        /**
         * Invoked once we have consumed all input.
         * <p>
         * Completes all unfinished buffers. If they do not contain enough keys, data is stolen from the preceding
         * node to the left on the same level. This is easy if our parent already contains a completed child; if it
         * does not, we recursively apply the stealing procedure to obtain a non-empty parent. If this process manages
         * to reach the root and still find no preceding branch, this will result in making this branch the new root.
         */
        private Object[] finishAndDrain(boolean skipLeaf)
        {
            LeafOrBranchBuilder level = leaf();
            if (skipLeaf)
            {
                level = nonEmptyParentMaybeSteal(level);
                // handle an edge case, where we have propagated a single complete leaf but have no other contents in any parent
                if (level == null)
                    return (Object[]) leaf().parent.buffer[MAX_KEYS];
            }
            while (true)
            {
                BranchBuilder parent = nonEmptyParentMaybeSteal(level);
                if (parent != null && !level.isSufficient())
                {
                    Object[] result = stealAndMaybeRepropagate(level, parent);
                    if (result != null)
                        return result;
                }
                else
                {

                    Object[] originalNode = level == leaf() ? null : queuedToFinish[level.height - 2];
                    Object[] result = level.drainAndPropagate(originalNode, parent);
                    if (parent == null)
                        return result;
                }
                level = parent;
            }
        }

        BranchBuilder nonEmptyParentMaybeSteal(LeafOrBranchBuilder level)
        {
            if (level.hasOverflow())
                return level.ensureParent();
            BranchBuilder parent = level.parent;
            if (parent == null || !parent.inUse || (parent.isEmpty() && !tryPrependFromParent(parent)))
                return null;
            return parent;
        }

        /**
         * precondition: {@code fill.parentInUse()} must return {@code fill.parent}
         * <p>
         * Steal some data from our ancestors, if possible.
         * 1) If no ancestor has any data to steal, simply drain and return the current contents.
         * 2) If we exhaust all of our ancestors, and are not now ourselves overflowing, drain and return
         * 3) Otherwise propagate the redistributed contents to our parent and return null, indicating we can continue to parent
         *
         * @return {@code null} if {@code parent} is still logicallly in use after we execute;
         * otherwise the return value is the final result
         */
        private Object[] stealAndMaybeRepropagate(LeafOrBranchBuilder fill, BranchBuilder parent)
        {
            // parent already stole, we steal one from it
            prependFromParent(fill, parent);

            // if we've emptied our parent, attempt to restore it from our grandparent,
            // this is so that we can determine an accurate exhausted status
            boolean exhausted = !fill.hasOverflow() && parent.isEmpty() && !tryPrependFromParent(parent);
            if (exhausted)
                return fill.drain();

            fill.drainAndPropagate(null, parent);
            return null;
        }

        private boolean tryPrependFromParent(BranchBuilder parent)
        {
            BranchBuilder grandparent = nonEmptyParentMaybeSteal(parent);
            if (grandparent == null)
                return false;
            prependFromParent(parent, grandparent);
            return true;
        }

        // should only be invoked with parent = parentIfStillInUse(fill), if non-null result
        private void prependFromParent(LeafOrBranchBuilder fill, BranchBuilder parent)
        {
            assert !parent.isEmpty();

            Object[] predecessor;
            Object predecessorNextKey;
            // parent will have same number of children as shallow key count (and may be empty)
            if (parent.count == 0 && parent.hasOverflow())
            {
                // use the saved buffer instead of going to our parent
                predecessorNextKey = parent.savedNextKey;
                predecessor = (Object[]) parent.savedBuffer[2 * MAX_KEYS];
                Object[] tmpBuffer = parent.savedBuffer;
                int[] tmpSizes = parent.savedSizes;
                parent.savedBuffer = parent.buffer;
                parent.savedSizes = parent.sizes;
                parent.buffer = tmpBuffer;
                parent.sizes = tmpSizes;
                parent.savedNextKey = null;
                parent.count = MAX_KEYS;
                // end with MAX_KEYS keys and children in parent, having stolen MAX_KEYS+1 child and savedNextKey
            }
            else
            {
                --parent.count;
                predecessor = (Object[]) parent.buffer[MAX_KEYS + parent.count];
                predecessorNextKey = parent.buffer[parent.count];
            }

            fill.prepend(predecessor, predecessorNextKey);
        }

        void reset()
        {
            Arrays.fill(queuedToFinish, 0, update.leafDepth, null);
            update.reset();
            super.reset();
        }
    }


    private static class Transformer<I, O> extends AbstractTransformer<I, O>
    {
        static final TinyThreadLocalPool<Transformer> POOL = new TinyThreadLocalPool<>();
        TinyThreadLocalPool.TinyPool<Transformer> pool;

        Function<? super I, ? extends O> apply;

        O apply(I v)
        {
            return apply.apply(v);
        }

        static <I, O> Transformer<I, O> get(Function<? super I, ? extends O> apply)
        {
            TinyThreadLocalPool.TinyPool<Transformer> pool = POOL.get();
            Transformer<I, O> transformer = pool.poll();
            if (transformer == null)
                transformer = new Transformer<>();
            transformer.pool = pool;
            transformer.apply = apply;
            return transformer;
        }

        public void close()
        {
            apply = null;
            reset();
            pool.offer(this);
            pool = null;
        }
    }

    private static class BiTransformer<I, I2, O> extends AbstractTransformer<I, O>
    {
        static final TinyThreadLocalPool<BiTransformer> POOL = new TinyThreadLocalPool<>();

        BiFunction<? super I, ? super I2, ? extends O> apply;
        I2 i2;
        TinyThreadLocalPool.TinyPool<BiTransformer> pool;

        O apply(I i1)
        {
            return apply.apply(i1, i2);
        }

        static <I, I2, O> BiTransformer<I, I2, O> get(BiFunction<? super I, ? super I2, ? extends O> apply, I2 i2)
        {
            TinyThreadLocalPool.TinyPool<BiTransformer> pool = POOL.get();
            BiTransformer<I, I2, O> transformer = pool.poll();
            if (transformer == null)
                transformer = new BiTransformer<>();
            transformer.pool = pool;
            transformer.apply = apply;
            transformer.i2 = i2;
            return transformer;
        }

        public void close()
        {
            apply = null;
            i2 = null;
            reset();
            pool.offer(this);
            pool = null;
        }
    }

    /**
     * A base class for very simple walks of a tree without recursion, supporting reuse
     */
    private static abstract class SimpleTreeStack
    {
        // stack we have descended, with 0 the root node
        Object[][] nodes;
        /**
         * the child node we are in, if at lower height, or the key we are on otherwise
         * can be < 0, indicating we have not yet entered the contents of the node, and are deliberating
         * whether we descend or consume the contents without descending
         */
        int[] positions;
        int depth, leafDepth;

        void reset()
        {
            Arrays.fill(nodes, 0, leafDepth + 1, null);
            // positions gets zero'd during descent
        }

        Object[] node()
        {
            return nodes[depth];
        }

        int position()
        {
            return positions[depth];
        }
    }

    // Similar to SimpleTreeNavigator, but visits values eagerly
    // (the exception being ascendToParent(), which permits iterating through finished parents).
    // Begins by immediately descending to first leaf; if empty terminates immediately.
    private static class SimpleTreeIterator extends SimpleTreeStack
    {
        int init(Object[] tree)
        {
            int maxHeight = maxRootHeight(size(tree));
            if (positions == null || maxHeight >= positions.length)
            {
                positions = new int[maxHeight + 1];
                nodes = new Object[maxHeight + 1][];
            }
            nodes[0] = tree;
            if (isEmpty(tree))
            {
                // already done
                leafDepth = 0;
                depth = -1;
            }
            else
            {
                depth = 0;
                positions[0] = 0;
                while (!isLeaf(tree))
                {
                    tree = (Object[]) tree[shallowSizeOfBranch(tree)];
                    nodes[++depth] = tree;
                    positions[depth] = 0;
                }
                leafDepth = depth;
            }
            return leafDepth + 1;
        }

        void descendIntoNextLeaf(Object[] node, int pos, int sz)
        {
            positions[depth] = ++pos;
            ++depth;
            nodes[depth] = node = (Object[]) node[sz + pos];
            positions[depth] = 0;
            while (depth < leafDepth)
            {
                ++depth;
                nodes[depth] = node = (Object[]) node[shallowSizeOfBranch(node)];
                positions[depth] = 0;
            }
        }

        boolean ascendToParent()
        {
            if (depth < 0)
                return false;
            return --depth >= 0;
        }
    }


    private static class SimpleTreeKeysIterator<Compare, Insert extends Compare>
    {
        int leafSize;
        int leafPos;
        Object[] leaf;
        Object[][] nodes;
        int[] positions;
        int depth;

        void init(Object[] tree)
        {
            int maxHeight = maxRootHeight(size(tree));
            if (positions == null || maxHeight >= positions.length)
            {
                positions = new int[maxHeight + 1];
                nodes = new Object[maxHeight + 1][];
            }
            depth = 0;
            descendToLeaf(tree);
        }

        void reset()
        {
            leaf = null;
            Arrays.fill(nodes, 0, nodes.length, null);
        }

        Insert next()
        {
            if (leafPos < leafSize) // fast path
                return (Insert) leaf[leafPos++];

            if (depth == 0)
                return null;

            Object[] node = nodes[depth - 1];
            final int position = positions[depth - 1];
            Insert result = (Insert) node[position];
            advanceBranch(node, position + 1);
            return result;
        }

        private void advanceBranch(Object[] node, int position)
        {
            int count = shallowSizeOfBranch(node);
            if (position < count)
                positions[depth - 1] = position;
            else
                --depth;  // no more children in this branch, remove from stack
            descendToLeaf((Object[]) node[count + position]);
        }

        void descendToLeaf(Object[] node)
        {
            while (!isLeaf(node))
            {
                nodes[depth] = node;
                positions[depth] = 0;
                node = (Object[]) node[shallowSizeOfBranch(node)];
                ++depth;
            }
            leaf = node;
            leafPos = 0;
            leafSize = sizeOfLeaf(node);
        }

        <Update> int copyKeysSmallerThan(Compare bound, Comparator<? super Compare> comparator, LeafBuilder builder, UpdateFunction<Insert, Update> transformer)
        {
            while (true)
            {
                int lim = exponentialSearchForMaybeInfinity(comparator, leaf, leafPos, leafSize, bound);
                int end = lim >= 0 ? lim : -1 - lim;
                if (end > leafPos)
                {
                    builder.copy(leaf, leafPos, end - leafPos, transformer);
                    leafPos = end;
                }
                if (end < leafSize)
                    return searchResultToComparison(lim);    // 0 if next is a match for bound, -1 otherwise

                if (depth == 0)
                    return -1;

                Object[] node = nodes[depth - 1];
                final int position = positions[depth - 1];
                Insert branchKey = (Insert) node[position];
                int cmp = compareWithMaybeInfinity(comparator, branchKey, bound);
                if (cmp >= 0)
                    return -cmp;
                builder.addKey(isSimple(transformer) ? branchKey : transformer.insert(branchKey));
                advanceBranch(node, position + 1);
            }
        }
    }
}
