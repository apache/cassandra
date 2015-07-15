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

import com.google.common.collect.Ordering;

import org.apache.cassandra.utils.ObjectSizes;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class BTree
{
    /**
     * Leaf Nodes are a raw array of values: Object[V1, V1, ...,].
     *
     * Branch Nodes: Object[V1, V2, ..., child[&lt;V1.key], child[&lt;V2.key], ..., child[&lt; Inf], size], where
     * each child is another node, i.e., an Object[].  Thus, the value elements in a branch node are the
     * first half of the array (minus one).  In our implementation, each value must include its own key;
     * we access these via Comparator, rather than directly. 
     *
     * So we can quickly distinguish between leaves and branches, we require that leaf nodes are always an odd number
     * of elements (padded with a null, if necessary), and branches are always an even number of elements.
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
    static final Object[] EMPTY_LEAF = new Object[1];

    // An empty BTree branch - used only for internal purposes in Modifier
    static final Object[] EMPTY_BRANCH = new Object[] { null, new int[0] };

    /**
     * Returns an empty BTree
     *
     * @return
     */
    public static Object[] empty()
    {
        return EMPTY_LEAF;
    }

    public static Object[] singleton(Object value)
    {
        return new Object[] { value };
    }

    public static <C, K extends C, V extends C> Object[] build(Collection<K> source, UpdateFunction<K, V> updateF)
    {
        return build(source, source.size(), updateF);
    }

    /**
     * Creates a BTree containing all of the objects in the provided collection
     *
     * @param source  the items to build the tree with. MUST BE IN STRICTLY ASCENDING ORDER.
     * @return        a btree representing the contents of the provided iterable
     */
    public static <C, K extends C, V extends C> Object[] build(Iterable<K> source, int size, UpdateFunction<K, V> updateF)
    {
        if (size < FAN_FACTOR)
        {
            if (size == 0)
                return EMPTY_LEAF;
            // pad to odd length to match contract that all leaf nodes are odd
            V[] values = (V[]) new Object[size | 1];
            {
                int i = 0;
                for (K k : source)
                    values[i++] = updateF.apply(k);
            }
            updateF.allocated(ObjectSizes.sizeOfArray(values));
            return values;
        }

        Queue<TreeBuilder> queue = modifier.get();
        TreeBuilder builder = queue.poll();
        if (builder == null)
            builder = new TreeBuilder();
        Object[] btree = builder.build(source, updateF, size);
        queue.add(builder);
        return btree;
    }

    public static <C, K extends C, V extends C> Object[] update(Object[] btree,
                                                                Comparator<C> comparator,
                                                                Collection<K> updateWith,
                                                                UpdateFunction<K, V> updateF)
    {
        return update(btree, comparator, updateWith, updateWith.size(), updateF);
    }

    /**
     * Returns a new BTree with the provided collection inserting/replacing as necessary any equal items
     *
     * @param btree              the tree to update
     * @param comparator         the comparator that defines the ordering over the items in the tree
     * @param updateWith         the items to either insert / update. MUST BE IN STRICTLY ASCENDING ORDER.
     * @param updateWithLength   then number of elements in updateWith
     * @param updateF            the update function to apply to any pairs we are swapping, and maybe abort early
     * @param <V>
     * @return
     */
    public static <C, K extends C, V extends C> Object[] update(Object[] btree,
                                                                Comparator<C> comparator,
                                                                Iterable<K> updateWith,
                                                                int updateWithLength,
                                                                UpdateFunction<K, V> updateF)
    {
        if (isEmpty(btree))
            return build(updateWith, updateWithLength, updateF);

        Queue<TreeBuilder> queue = modifier.get();
        TreeBuilder builder = queue.poll();
        if (builder == null)
            builder = new TreeBuilder();
        btree = builder.update(btree, comparator, updateWith, updateF);
        queue.add(builder);
        return btree;
    }

    public static <K> Object[] merge(Object[] tree1, Object[] tree2, Comparator<K> comparator)
    {
        if (size(tree1) < size(tree2))
        {
            Object[] tmp = tree1;
            tree1 = tree2;
            tree2 = tmp;
        }
        return update(tree1, comparator, new BTreeSet<K>(tree2, comparator), UpdateFunction.<K>noOp());
    }

    /**
     * Returns an Iterator over the entire tree
     *
     * @param btree    the tree to iterate over
     * @param forwards if false, the iterator will start at the end and move backwards
     * @param <V>
     * @return
     */
    public static <V> BTreeSearchIterator<V, V> slice(Object[] btree, Comparator<? super V> comparator, boolean forwards)
    {
        return new BTreeSearchIterator<>(btree, comparator, forwards);
    }

    /**
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param start      the beginning of the range to return, inclusive (in ascending order)
     * @param end        the end of the range to return, exclusive (in ascending order)
     * @param forwards   if false, the iterator will start at the last item and move backwards
     * @return           an Iterator over the defined sub-range of the tree
     */
    public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, K start, K end, boolean forwards)
    {
        return slice(btree, comparator, start, true, end, false, forwards);
    }

    /**
     * @param btree          the tree to iterate over
     * @param comparator     the comparator that defines the ordering over the items in the tree
     * @param start          low bound of the range
     * @param startInclusive inclusivity of lower bound
     * @param end            high bound of the range
     * @param endInclusive   inclusivity of higher bound
     * @param forwards       if false, the iterator will start at end and move backwards
     * @return           an Iterator over the defined sub-range of the tree
     */
    public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, K start, boolean startInclusive, K end, boolean endInclusive, boolean forwards)
    {
        int inclusiveLowerBound = max(0,
                                      start == null ? Integer.MIN_VALUE
                                                    : startInclusive ? ceilIndex(btree, comparator, start)
                                                                     : higherIndex(btree, comparator, start));
        int inclusiveUpperBound = min(size(btree) - 1,
                                      end == null ? Integer.MAX_VALUE
                                                  : endInclusive ? floorIndex(btree, comparator, end)
                                                                 : lowerIndex(btree, comparator, end));
        return new BTreeSearchIterator<>(btree, comparator, forwards, inclusiveLowerBound, inclusiveUpperBound);
    }

    /**
     * Honours result semantics of {@link Arrays#binarySearch}, as though it were performed on the tree flattened into an array
     * @return index of item in tree, or <tt>(-(<i>insertion point</i>) - 1)</tt> if not present
     */
    public static <V> int findIndex(Object[] node, Comparator<V> comparator, V find)
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

            boundary = -1 -boundary;
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
            i = -1 -i;
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
            i = -2 -i;
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
        if (i < 0) i = -1 -i;
        else i++;
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
            i = -1 -i;
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

    // returns true if the provided node is a leaf, false if it is a branch
    static boolean isLeaf(Object[] node)
    {
        return (node.length & 1) == 1;
    }

    public static boolean isEmpty(Object[] tree)
    {
        return tree == EMPTY_LEAF;
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
     * @param tree source
     * @param target array
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
        for (int i = 0 ; i < childCount ; i++)
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
     * tree index => index of key wrt all items in the tree laid out serially
     *
     * This version of the method permits requesting out-of-bounds indexes, -1 and size
     * @param root to calculate tree index within
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
     * @param root to calculate tree-index within
     * @param keyIndex root-local index of key to calculate tree-index of
     * @return the number of items preceding the key in the whole tree of root
     */
    public static int treeIndexOfBranchKey(Object[] root, int keyIndex)
    {
        return lookupSizeMap(root, keyIndex);
    }

    /**
     * @param root to calculate tree-index within
     * @param childIndex root-local index of *child* to calculate tree-index of
     * @return the number of items preceding the child in the whole tree of root
     */
    public static int treeIndexOffsetOfChild(Object[] root, int childIndex)
    {
        if (childIndex == 0)
            return 0;
        return 1 + lookupSizeMap(root, childIndex - 1);
    }

    private static final ThreadLocal<Queue<TreeBuilder>> modifier = new ThreadLocal<Queue<TreeBuilder>>()
    {
        @Override
        protected Queue<TreeBuilder> initialValue()
        {
            return new ArrayDeque<>();
        }
    };

    public static <V> Builder<V> builder(Comparator<? super V> comparator)
    {
        return new Builder<>(comparator);
    }

    public static class Builder<V>
    {

        final Comparator<? super V> comparator;
        Object[] values = new Object[10];
        int count;
        boolean detected; // true if we have managed to cheaply ensure sorted + filtered as we have added

        protected Builder(Comparator<? super V> comparator)
        {
            this.comparator = comparator;
        }

        public Builder<V> add(V v)
        {
            if (count == values.length)
                values = Arrays.copyOf(values, count * 2);
            values[count++] = v;

            if (detected && count > 1)
            {
                int c = comparator.compare((V) values[count - 2], (V) values[count - 1]);
                if (c == 0) count--;
                else if (c > 0) detected = false;
            }

            return this;
        }

        public Builder<V> addAll(Collection<V> add)
        {
            if (add instanceof SortedSet && equalComparators(comparator, ((SortedSet) add).comparator()))
            {
                // if we're a SortedSet, permit quick order-preserving addition of items
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
        public Builder<V> mergeAll(Iterable<V> add, int addCount)
        {
            // ensure the existing contents are in order
            sortAndFilter();

            int curCount = count;
            // we make room for curCount * 2 + addCount, so that we can copy the current values to the end
            // if necessary for continuing the merge, and have the new values directly after the current value range
            // i.e. []
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

        // iter must be in sorted order!
        private Builder<V> mergeAll(int addCount)
        {
            // start optimistically by assuming new values are superset of current, and just run until this fails to hold
            Object[] a = values;
            int addOffset = count;

            int i = 0, j = addOffset;
            int curEnd = addOffset, addEnd = addOffset + addCount;
            while (i < curEnd && j < addEnd)
            {
                int c = comparator.compare((V) a[i], (V) a[j]);
                if (c > 0)
                    break;
                else if (c == 0)
                    j++;
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
                // could avoid one comparison if we cared, but would make this ugly
                int c = comparator.compare((V) a[i], (V) a[j]);
                if (c == 0)
                {
                    a[newCount++] = a[i];
                    i++;
                    j++;
                }
                else
                {
                    a[newCount++] =  c < 0 ? a[i++] : a[j++];
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

        private void sortAndFilter()
        {
            if (!detected && count > 1)
            {
                Arrays.sort((V[]) values, 0, count, comparator);
                int c = 1;
                for (int i = 1 ; i < count ; i++)
                    if (comparator.compare((V) values[i], (V) values[i - 1]) != 0)
                        values[c++] = values[i];
                count = c;
            }
            detected = true;
        }

        public Object[] build()
        {
            sortAndFilter();
            return BTree.build(Arrays.asList(values).subList(0, count), UpdateFunction.noOp());
        }
    }

    /** simple static wrapper to calls to cmp.compare() which checks if either a or b are Special (i.e. represent an infinity) */
    static <V> int compare(Comparator<V> cmp, Object a, Object b)
    {
        if (a == b)
            return 0;
        if (a == NEGATIVE_INFINITY | b == POSITIVE_INFINITY)
            return -1;
        if (b == NEGATIVE_INFINITY | a == POSITIVE_INFINITY)
            return 1;
        return cmp.compare((V) a, (V) b);
    }

    static Object POSITIVE_INFINITY = new Object();
    static Object NEGATIVE_INFINITY = new Object();

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
                return node.length <= FAN_FACTOR + 1;
            return node.length >= FAN_FACTOR / 2 && node.length <= FAN_FACTOR + 1;
        }

        int type = 0;
        // compare each child node with the branch element at the head of this node it corresponds with
        for (int i = getChildStart(node); i < getChildEnd(node) ; i++)
        {
            Object[] child = (Object[]) node[i];
            Object localmax = i < node.length - 2 ? node[i - getChildStart(node)] : max;
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
