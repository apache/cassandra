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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;

import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
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

    static final int MINIMAL_NODE_SIZE = FAN_FACTOR >> 1;

    // An empty BTree Leaf - which is the same as an empty BTree
    static final Object[] EMPTY_LEAF = new Object[1];

    // An empty BTree branch - used only for internal purposes in Modifier
    static final Object[] EMPTY_BRANCH = new Object[] { null, new int[0] };

    // direction of iteration
    public static enum Dir
    {
        ASC, DESC;
        public Dir invert() { return this == ASC ? DESC : ASC; }
        public static Dir asc(boolean asc) { return asc ? ASC : DESC; }
        public static Dir desc(boolean desc) { return desc ? DESC : ASC; }
    }

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
        return buildInternal(source, source.size(), updateF);
    }

    public static <C, K extends C, V extends C> Object[] build(Iterable<K> source, UpdateFunction<K, V> updateF)
    {
        return buildInternal(source, -1, updateF);
    }

    /**
     * Creates a BTree containing all of the objects in the provided collection
     *
     * @param source  the items to build the tree with. MUST BE IN STRICTLY ASCENDING ORDER.
     * @param size    the size of the source iterable
     * @return        a btree representing the contents of the provided iterable
     */
    public static <C, K extends C, V extends C> Object[] build(Iterable<K> source, int size, UpdateFunction<K, V> updateF)
    {
        if (size < 0)
            throw new IllegalArgumentException(Integer.toString(size));
        return buildInternal(source, size, updateF);
    }

    /**
     * As build(), except:
     * @param size    < 0 if size is unknown
     */
    private static <C, K extends C, V extends C> Object[] buildInternal(Iterable<K> source, int size, UpdateFunction<K, V> updateF)
    {
        if ((size >= 0) & (size < FAN_FACTOR))
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

    public static <K> Object[] merge(Object[] tree1, Object[] tree2, Comparator<? super K> comparator, UpdateFunction<K, K> updateF)
    {
        if (size(tree1) < size(tree2))
        {
            Object[] tmp = tree1;
            tree1 = tree2;
            tree2 = tmp;
        }
        return update(tree1, comparator, new BTreeSet<K>(tree2, comparator), updateF);
    }

    public static <V> Iterator<V> iterator(Object[] btree)
    {
        return iterator(btree, Dir.ASC);
    }

    public static <V> Iterator<V> iterator(Object[] btree, Dir dir)
    {
        return new BTreeSearchIterator<V, V>(btree, null, dir);
    }

    public static <V> Iterator<V> iterator(Object[] btree, int lb, int ub, Dir dir)
    {
        return new BTreeSearchIterator<V, V>(btree, null, dir, lb, ub);
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
     * @param btree  the tree to iterate over
     * @param dir    direction of iteration
     * @param <V>
     * @return
     */
    public static <K, V> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, Dir dir)
    {
        return new BTreeSearchIterator<>(btree, comparator, dir);
    }

    /**
     * @param btree      the tree to iterate over
     * @param comparator the comparator that defines the ordering over the items in the tree
     * @param start      the beginning of the range to return, inclusive (in ascending order)
     * @param end        the end of the range to return, exclusive (in ascending order)
     * @param dir   if false, the iterator will start at the last item and move backwards
     * @return           an Iterator over the defined sub-range of the tree
     */
    public static <K, V extends K> BTreeSearchIterator<K, V> slice(Object[] btree, Comparator<? super K> comparator, K start, K end, Dir dir)
    {
        return slice(btree, comparator, start, true, end, false, dir);
    }

    /**
     * @param btree          the tree to iterate over
     * @param comparator     the comparator that defines the ordering over the items in the tree
     * @param start          low bound of the range
     * @param startInclusive inclusivity of lower bound
     * @param end            high bound of the range
     * @param endInclusive   inclusivity of higher bound
     * @param dir            direction of iteration
     * @return               an Iterator over the defined sub-range of the tree
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
        return new BTreeSearchIterator<>(btree, comparator, dir, inclusiveLowerBound, inclusiveUpperBound);
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

            boundary = -1 -boundary;
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

    public static long sizeOfStructureOnHeap(Object[] tree)
    {
        long size = ObjectSizes.sizeOfArray(tree);
        if (isLeaf(tree))
            return size;
        for (int i = getChildStart(tree) ; i < getChildEnd(tree) ; i++)
            size += sizeOfStructureOnHeap((Object[]) tree[i]);
        return size;
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

    // simple class for avoiding duplicate transformation work
    private static class FiltrationTracker<V> implements Function<V, V>
    {
        final Function<? super V, ? extends V> wrapped;
        int index;
        boolean failed;

        private FiltrationTracker(Function<? super V, ? extends V> wrapped)
        {
            this.wrapped = wrapped;
        }

        public V apply(V i)
        {
            V o = wrapped.apply(i);
            if (o != null) index++;
            else failed = true;
            return o;
        }
    }

    /**
     * Takes a btree and transforms it using the provided function, filtering out any null results.
     * The result of any transformation must sort identically wrt the other results as their originals
     */
    public static <V> Object[] transformAndFilter(Object[] btree, Function<? super V, ? extends V> function)
    {
        if (isEmpty(btree))
            return btree;

        // TODO: can be made more efficient
        FiltrationTracker<V> wrapped = new FiltrationTracker<>(function);
        Object[] result = transformAndFilter(btree, wrapped);
        if (!wrapped.failed)
            return result;

        // take the already transformed bits from the head of the partial result
        Iterable<V> head = iterable(result, 0, wrapped.index - 1, Dir.ASC);
        // and concatenate with remainder of original tree, with transformation applied
        Iterable<V> remainder = iterable(btree, wrapped.index + 1, size(btree) - 1, Dir.ASC);
        remainder = filter(transform(remainder, function), (x) -> x != null);
        Iterable<V> build = concat(head, remainder);

        return buildInternal(build, -1, UpdateFunction.<V>noOp());
    }

    private static <V> Object[] transformAndFilter(Object[] btree, FiltrationTracker<V> function)
    {
        Object[] result = btree;
        boolean isLeaf = isLeaf(btree);
        int childOffset = isLeaf ? Integer.MAX_VALUE : getChildStart(btree);
        int limit = isLeaf ? getLeafKeyEnd(btree) : btree.length - 1;
        for (int i = 0 ; i < limit ; i++)
        {
            // we want to visit in iteration order, so we visit our key nodes inbetween our children
            int idx = isLeaf ? i : (i / 2) + (i % 2 == 0 ? childOffset : 0);
            Object current = btree[idx];
            Object updated = idx < childOffset ? function.apply((V) current) : transformAndFilter((Object[]) current, function);
            if (updated != current)
            {
                if (result == btree)
                    result = btree.clone();
                result[idx] = updated;
            }
            if (function.failed)
                return result;
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

    public static <V> Builder<V> builder(Comparator<? super V> comparator, int initialCapacity)
    {
        return new Builder<>(comparator);
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
            this.comparator = comparator;
            this.values = new Object[initialCapacity];
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

        public Builder<V> reverse()
        {
            assert !auto;
            int mid = count / 2;
            for (int i = 0 ; i < mid ; i++)
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
                for (int i = 1 ; i < count ; i++)
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
                for (int i = 1 ; i < count ; i++)
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

        final int keyCount = getBranchKeyEnd(node);
        if ((!isRoot && keyCount < FAN_FACTOR / 2) || keyCount > FAN_FACTOR + 1)
            return false;

        int type = 0;
        int size = -1;
        int[] sizeMap = getSizeMap(node);
        // compare each child node with the branch element at the head of this node it corresponds with
        for (int i = getChildStart(node); i < getChildEnd(node) ; i++)
        {
            Object[] child = (Object[]) node[i];
            size += size(child) + 1;
            if (sizeMap[i - getChildStart(node)] != size)
                return false;
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
