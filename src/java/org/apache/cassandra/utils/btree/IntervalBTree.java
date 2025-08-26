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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nullable;

import accord.utils.AsymmetricComparator;
import accord.utils.Invariants;
import accord.utils.QuadFunction;
import accord.utils.SortedArrays;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.utils.caching.TinyThreadLocalPool;

import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FLOOR;
import static org.apache.cassandra.utils.btree.BTree.BRANCH_FACTOR;
import static org.apache.cassandra.utils.btree.BTree.getBranchKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getChildCount;
import static org.apache.cassandra.utils.btree.BTree.getChildStart;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getLeafKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isEmpty;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;
import static org.apache.cassandra.utils.btree.BTree.size;
import static org.apache.cassandra.utils.btree.BTree.sizeMap;

/**
 * A very simple extension to BTree to provide an Augmented Interval BTree.
 *
 * TODO (desired): there are a number of obvious performance improvements that should be pursued at the earliest suitable opportunity:
 *      - IntervalMaxIndex should include an index over any direct child leaves to bound worst case to O(lgN + m) rather than O(lgN + m.B)
 *      - IntervalMaxIndex should include the immediately preceding self-key in any max key, so we can avoid the O(N) loop and simply pre-visit the self-key
 *      - IntervalMaxIndex should omit entirely keys/nodes that do not extend past the following key
 *      - Updater/Subtraction should re-use parts of IntervalMaxIndex where appropriate
 */
public class IntervalBTree
{
    /**
     * Defines additional SORT comparators that must be symmetric for building/maintaining an interval tree
     */
    public interface IntervalComparators<I> extends WithIntervalComparators<I, I>
    {
        Comparator<I> totalOrder();
        Comparator<I> endWithEndSorter();
    }

    /**
     * Defines search comparators for finding an element in an IntervalBTree.
     *
     * A search for an element K will potentially include any interval I
     * for which any comparator yields compare(K, I) == 0.
     *
     * These comparators DO NOT need to be symmetric in either type OR comparison result,
     * and should knowingly impose the inclusive/exclusive criteria of the interval
     * by returning non-zero answers as appropriate.
     *
     * Specifically, compare(a, a) should not return 0 if the relevant bound is exclusive.
     */
    public interface WithIntervalComparators<K, I>
    {
        AsymmetricComparator<K, I> startWithStartSeeker();
        AsymmetricComparator<K, I> startWithEndSeeker();
        AsymmetricComparator<K, I> endWithStartSeeker();
    }

    public static class InclusiveEndHelper
    {
        public static int keyStartWithStart(int c) { return equalsMeansAfter(c); }
        public static int keyStartWithEnd(int c) { return c; }
        public static int keyEndWithStart(int c) { return equalsMeansBefore(c); }
        public static int startWithStart(int c) { return equalsMeansAfter(c); }
        public static int startWithEnd(int c) { return equalsMeansAfter(c); }
        public static int endWithStart(int c) { return equalsMeansBefore(c); }
        private static int equalsMeansAfter(int c) { return c == 0 ? 1 : c; }
        private static int equalsMeansBefore(int c) { return c == 0 ? -1 : c; }
    }

    /**
     * Apply the accumulation function over all intersecting intervals in the tree
     */
    public static <Find, Interval, P1, P2, V> V accumulate(Object[] btree, WithIntervalComparators<Find, Interval> comparators, Find find, QuadFunction<P1, P2, Interval, V, V> function, P1 p1, P2 p2, V accumulate)
    {
        if (isLeaf(btree))
        {
            AsymmetricComparator<Find, Interval> startWithEnd = comparators.startWithEndSeeker();
            AsymmetricComparator<Find, Interval> endWithStart = comparators.endWithStartSeeker();
            int keyEnd = getLeafKeyEnd(btree);
            for (int i = 0; i < keyEnd; ++i)
            {
                Interval interval = (Interval) btree[i];
                if (endWithStart.compare(find, interval) >= 0 && startWithEnd.compare(find, interval) <= 0)
                    accumulate = function.apply(p1, p2, interval, accumulate);
            }
        }
        else
        {
            // start/end represent those keys/children we are guaranteed to intersect (and so may visit without any further comparisons)
            int startKey = SortedArrays.binarySearch(btree, 0, getKeyEnd(btree), find, (AsymmetricComparator)comparators.startWithStartSeeker(), CEIL);
            int startChild;

            if (startKey >= 0) startChild = startKey + 1;
            else startChild = (startKey = -1 - startKey);
            int endKey = SortedArrays.binarySearch(btree, startKey, getKeyEnd(btree), find, (AsymmetricComparator)comparators.endWithStartSeeker(), FLOOR);
            int endChild;
            if (endKey >= 0) { if (endKey < getKeyEnd(btree)) ++endKey; endChild = 1 + endKey; }
            else endChild = 1 + (endKey = -1 - endKey);

            {   // descend anything with a max that overlaps us that we won't already visit
                if (startChild > 0 || startKey > 0)
                    accumulate = accumulateMaxOnly(startChild + (startChild == endChild ? 1 : 0), startKey, btree, comparators, find, function, p1, p2, accumulate);
            }

            int childOffset = getChildStart(btree);
            if (startChild == startKey && startChild < endChild)
                accumulate = accumulate((Object[]) btree[childOffset + startChild++], comparators, find, function, p1, p2, accumulate);

            if (startKey < startChild && startKey < endKey)
                accumulate = function.apply(p1, p2, (Interval) btree[startKey++], accumulate);

            while (startChild < endChild - 1)
            {
                Invariants.require(startKey == startChild);
                accumulate = accumulate((Object[]) btree[childOffset + startChild++], function, p1, p2, accumulate);
                accumulate = function.apply(p1, p2, (Interval) btree[startKey++], accumulate);
            }
            if (startKey < endKey)
                accumulate = function.apply(p1, p2, (Interval) btree[startKey], accumulate);
            if (startChild < endChild)
                accumulate = accumulate((Object[]) btree[childOffset + startChild], comparators, find, function, p1, p2, accumulate);
        }
        return accumulate;
    }

    public static <R, P1, P2, V> V accumulate(Object[] btree, QuadFunction<P1, P2, R, V, V> function, P1 p1, P2 p2, V accumulate)
    {
        if (isLeaf(btree))
        {
            for (int i = 0, maxi = getLeafKeyEnd(btree); i < maxi; ++i)
                accumulate = function.apply(p1, p2, (R) btree[i], accumulate);
        }
        else
        {
            int keyEnd = getBranchKeyEnd(btree);
            for (int i = 0; i < keyEnd; ++i)
            {
                accumulate = accumulate((Object[]) btree[keyEnd + i], function, p1, p2, accumulate);
                accumulate = function.apply(p1, p2, (R) btree[i], accumulate);
            }
            accumulate = accumulate((Object[]) btree[2 * keyEnd], function, p1, p2, accumulate);
        }
        return accumulate;
    }

    private static <Find, Interval, P1, P2, V> V accumulateMaxOnly(int ifChildBefore, int ifKeyBefore, Object[] btree, WithIntervalComparators<Find, Interval> comparators, Find find, QuadFunction<P1, P2, Interval, V, V> function, P1 p1, P2 p2, V accumulate)
    {
        AsymmetricComparator<Find, Interval> startWithEnd = comparators.startWithEndSeeker();
        AsymmetricComparator<Find, Interval> endWithStart = comparators.endWithStartSeeker();
        if (isLeaf(btree))
        {
            Invariants.require(ifChildBefore == Integer.MAX_VALUE);
            for (int i = 0, maxi = getLeafKeyEnd(btree); i < maxi; ++i)
            {
                Interval interval = (Interval) btree[i];
                if (startWithEnd.compare(find, interval) <= 0 && endWithStart.compare(find, interval) >= 0)
                    accumulate = function.apply(p1, p2, interval, accumulate);
            }
        }
        else
        {
            int keyEnd = getChildStart(btree);
            ifKeyBefore = Math.min(ifKeyBefore, keyEnd);

            IntervalMaxIndex intervalMaxIndex = getIntervalMaxIndex(btree);
            int descendMaxStart = SortedArrays.binarySearch((Interval[]) intervalMaxIndex.sortedByEnd, 0, intervalMaxIndex.sortedByEnd.length, find, comparators.startWithEndSeeker(), CEIL);
            if (descendMaxStart < 0)
                descendMaxStart = -1 - descendMaxStart;
            int[] sortedByEndIndex = intervalMaxIndex.sortedByEndIndex;
            for (int i = descendMaxStart; i < sortedByEndIndex.length; ++i)
            {
                int index = sortedByEndIndex[i];
                if (index < ifKeyBefore)
                {
                    Interval interval = (Interval) btree[index];
                    if (startWithEnd.compare(find, interval) <= 0 && endWithStart.compare(find, interval) >= 0)
                        accumulate = function.apply(p1, p2, interval, accumulate);
                }
                if (index < ifChildBefore)
                    accumulate = accumulateMaxOnly(Integer.MAX_VALUE, Integer.MAX_VALUE, (Object[]) btree[keyEnd + index], comparators, find, function, p1, p2, accumulate);
            }
        }
        return accumulate;
    }

    static class IntervalIndexAdapter implements Comparator<IntervalIndexAdapter.SortEntry>
    {
        static class SortEntry
        {
            Object sort;
            int index;
        }

        Object[] unpackedIndex;
        SortEntry[] sort = new SortEntry[BRANCH_FACTOR];
        Comparator endSorter, totalOrder;

        void setComparators(IntervalComparators comparators)
        {
            this.endSorter = comparators.endWithEndSorter();
            this.totalOrder = comparators.totalOrder();
        }

        void reset()
        {
            this.endSorter = this.totalOrder = null;
            for (int i = 0 ; i < sort.length && sort[i] != null ; i++)
                sort[i].sort = null;
        }

        public void override(Object[] branch, @Nullable List<Object[]> sourceNodes)
        {
            int childStart = getChildStart(branch), childCount = getChildCount(branch);
            for (int i = childCount - 1 ; i >= 0 && sort[i] == null ; --i)
                sort[i] = new SortEntry();

            if (sourceNodes != null && sourceNodes.isEmpty())
                sourceNodes = null;

            if (sourceNodes != null)
            {
                int maxUnpackedSize = 0;
                if (unpackedIndex == null)
                    unpackedIndex = new Object[BRANCH_FACTOR];

                int i = 0;
                for (Object[] sn : sourceNodes)
                {
                    IntervalMaxIndex index = getIntervalMaxIndex(sn);
                    int snStart = getChildStart(sn), snCount = getChildCount(sn);
                    for (int j = 0 ; j < index.sortedByEnd.length ; ++j)
                        unpackedIndex[index.sortedByEndIndex[j]] = index.sortedByEnd[j];

                    int j = 0;
                    while (i < childCount && j < snCount)
                    {
                        if (branch[childStart + i] == sn[snStart + j]) // child nodes are the same so we can copy max
                        {
                            sort[i].index = i;
                            if (j == snStart)
                            {
                                // in either case, the max we have represents the max of the child;
                                // must only compare with new branch key (if any) and take the branch if ends later
                                sort[i].sort = i == childStart || endSorter.compare(branch[i], unpackedIndex[j]) <= 0
                                               ? unpackedIndex[j] : branch[i];
                            }
                            else if (unpackedIndex[j] != sn[j])
                            {
                                // previous max was not the branch, so must have been within child
                                // like prior condition, must only compare with new branch key (if any) and take the branch if ends later
                                // however, we have a chance to save a comparison here if we also copied the key from the source node
                                sort[i].sort = i == childStart || branch[i] == sn[j] || endSorter.compare(branch[i], unpackedIndex[j]) <= 0
                                               ? unpackedIndex[j] : branch[i];
                            }
                            else if (i != childStart && endSorter.compare(sn[j], branch[i]) <= 0)
                            {
                                // previous max was previous branch key; new branch key has higher end so can simply use it
                                sort[i].sort = branch[i];
                            }
                            else
                            {
                                // otherwise we cannot infer anything directly
                                sort[i].sort = null;
                            }

                            ++i;
                            ++j;
                        }
                        else
                        {
                            if (i == childStart || j == snStart)
                                break; // if we've finished iteration of one or the other, break and continue with any further source nodes (if any)

                            int c = totalOrder.compare(branch[i], sn[j]);
                            if (c <= 0) sort[i++].sort = null;
                            if (c >= 0) ++j;
                        }
                    }

                    maxUnpackedSize = Math.max(maxUnpackedSize, snCount);
                }
                while (i < childCount)
                    sort[i++].sort = null;
                Arrays.fill(unpackedIndex, 0, maxUnpackedSize, null);
            }
            for (int i = 0; i < childCount; ++i)
            {
                if (sourceNodes != null && sort[i].sort != null)
                    continue;

                Object[] child = (Object[]) branch[i + childStart];
                Object max = maxChild(child);
                if (i < childStart && endSorter.compare(branch[i], max) > 0)
                    max = branch[i];
                sort[i].index = i;
                sort[i].sort = max;
            }
            Arrays.sort(sort, 0, childCount, this);
            Object[] sortedByEnd = new Object[childCount];
            int[] sortedByEndIndex = new int[childCount];
            for (int i = 0; i < childCount; ++i)
            {
                sortedByEnd[i] = sort[i].sort;
                sortedByEndIndex[i] = sort[i].index;
            }

            int[] sizeMap = sizeMap(branch);
            branch[branch.length - 1] = new IntervalMaxIndex(sortedByEnd, sortedByEndIndex, sizeMap);
        }

        private Object maxChild(Object[] child)
        {
            if (isLeaf(child))
            {
                Object max = child[0];
                for (int i = 1, end = getLeafKeyEnd(child); i < end; ++i)
                {
                    if (endSorter.compare(child[i], max) > 0)
                        max = child[i];
                }
                return max;
            }
            else
            {
                IntervalMaxIndex index = getIntervalMaxIndex(child);
                return index.sortedByEnd[index.sortedByEnd.length - 1];
            }
        }

        @Override
        public int compare(SortEntry o1, SortEntry o2)
        {
            return endSorter.compare(o1.sort, o2.sort);
        }
    }

    static class IntervalBranchBuilder extends BTree.BranchBuilder
    {
        List<Object[]> sourceNodes;
        IntervalIndexAdapter adapter;

        IntervalBranchBuilder(BTree.LeafOrBranchBuilder child)
        {
            super(child);
            sourceNodes = child.height == 1 ? new ArrayList<>() : null;
        }

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this);
        }

        @Override
        void initParent()
        {
            super.initParent();
            ((IntervalBranchBuilder) parent).init(adapter);
        }

        private IntervalBranchBuilder init(IntervalIndexAdapter adapter)
        {
            this.adapter = adapter;
            return this;
        }

        @Override
        int setDrainSizeMap(Object[] original, int keysInOriginal, Object[] branch, int keysInBranch)
        {
            int result = super.setDrainSizeMap(original, keysInOriginal, branch, keysInBranch);
            adapter.override(branch, sourceNodes);
            return result;
        }

        @Override
        void setRedistributedSizeMap(Object[] branch, int steal)
        {
            super.setRedistributedSizeMap(branch, steal);
            adapter.override(branch, sourceNodes);
        }

        @Override
        int setOverflowSizeMap(Object[] branch, int keys)
        {
            int result = super.setOverflowSizeMap(branch, keys);
            adapter.override(branch, sourceNodes);
            return result;
        }

        @Override
        void setSourceNode(Object[] sourceNode)
        {
            if (sourceNodes != null)
                sourceNodes.add(sourceNode);
            super.setSourceNode(sourceNode);
        }

        @Override
        void clearSourceNode()
        {
            super.clearSourceNode();
            if (sourceNodes != null)
                sourceNodes.clear();
        }
    }

    public static class FastIntervalTreeBuilder<V> extends BTree.FastBuilder<V>
    {
        private static final TinyThreadLocalPool<FastIntervalTreeBuilder<?>> POOL = new TinyThreadLocalPool<>();
        final IntervalIndexAdapter adapter = new IntervalIndexAdapter();

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this);
        }

        @Override
        void initParent()
        {
            super.initParent();
            ((IntervalBranchBuilder) parent).init(adapter);
        }

        @Override
        public void close()
        {
            super.close();
            adapter.reset();
        }
    }

    static class IntervalUpdater<Compare, Existing extends Compare, Insert extends Compare> extends BTree.Updater<Compare, Existing, Insert>
    {
        static final TinyThreadLocalPool<IntervalUpdater> POOL = new TinyThreadLocalPool<>();
        private final IntervalIndexAdapter adapter = new IntervalIndexAdapter();

        static <Compare, Existing extends Compare, Insert extends Compare> IntervalUpdater<Compare, Existing, Insert> get(IntervalComparators<Compare> comparators)
        {
            TinyThreadLocalPool.TinyPool<IntervalUpdater> pool = POOL.get();
            IntervalUpdater<Compare, Existing, Insert> updater = pool.poll();
            if (updater == null)
                updater = new IntervalUpdater<>();
            updater.pool = pool;
            updater.adapter.setComparators(comparators);
            return updater;
        }

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this);
        }

        @Override
        void initParent()
        {
            super.initParent();
            ((IntervalBranchBuilder) parent).init(adapter);
        }

        @Override
        public void close()
        {
            // TODO (required): validate this in IntervalBTreeTest
            super.close();
            adapter.reset();
        }
    }

    public static Object[] empty()
    {
        return BTree.empty();
    }

    public static Object[] singleton(Object value)
    {
        return BTree.singleton(value);
    }

    static class Subtraction<K, T extends K> extends BTree.AbstractSubtraction<K, T>
    {
        static final FastThreadLocal<Subtraction> SHARED = new FastThreadLocal<>();
        private final IntervalIndexAdapter adapter = new IntervalIndexAdapter();

        static <K, T extends K> Subtraction<K, T> get(IntervalComparators<K> comparators)
        {
            Subtraction subtraction = SHARED.get();
            if (subtraction == null)
                SHARED.set(subtraction = new Subtraction());
            subtraction.comparator = comparators.totalOrder();
            subtraction.adapter.setComparators(comparators);
            return subtraction;
        }

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this);
        }

        @Override
        void initParent()
        {
            super.initParent();
            ((IntervalBranchBuilder) parent).init(adapter);
        }

        @Override
        void reset()
        {
            super.reset();
            adapter.reset();
        }
    }

    /**
     * Subtracts {@code insert} into {@code update}, applying {@code updateF} to each new item in {@code insert},
     * as well as any matched items in {@code update}.
     * <p>
     * Note that {@code UpdateFunction.noOp} is assumed to indicate a lack of interest in which value survives.
     */
    public static <Compare> Object[] subtract(Object[] toUpdate, Object[] subtract, IntervalComparators<Compare> comparators)
    {
        try (Subtraction subtraction = Subtraction.get(comparators))
        {
            return subtraction.subtract(toUpdate, subtract);
        }
    }

    /**
     * Inserts {@code insert} into {@code update}, applying {@code updateF} to each new item in {@code insert},
     * as well as any matched items in {@code update}.
     * <p>
     * Note that {@code UpdateFunction.noOp} is assumed to indicate a lack of interest in which value survives.
     */
    public static <Compare, Existing extends Compare, Insert extends Compare> Object[] update(Object[] existing,
                                                                                              Object[] insert,
                                                                                              IntervalComparators<Compare> comparators)
    {
        // perform some initial obvious optimisations
        if (isEmpty(insert))
            return existing; // do nothing if update is empty

        if (isEmpty(existing))
            return insert;

        if (isLeaf(insert))
        {
            // consider flipping the order of application, if update is much larger than insert and applying unary no-op
            int updateSize = size(existing);
            int insertSize = size(insert);
            int scale = Integer.numberOfLeadingZeros(updateSize) - Integer.numberOfLeadingZeros(insertSize);
            if (scale >= 4)
            {
                // i.e. at roughly 16x the size, or one tier deeper - very arbitrary, should pick more carefully
                // experimentally, at least at 64x the size the difference in performance is ~10x
                Object[] tmp = insert;
                insert = existing;
                existing = tmp;
            }
        }

        try (IntervalUpdater<Compare, Existing, Insert> updater = IntervalUpdater.get(comparators))
        {
            return updater.update(existing, insert, comparators.totalOrder(), (UpdateFunction) UpdateFunction.noOp);
        }
    }

    public static <V> Object[] build(Collection<V> build, IntervalComparators<V> comparators)
    {
        try (FastIntervalTreeBuilder<V> builder = IntervalBTree.fastBuilder(comparators))
        {
            for (V v : build)
                builder.add(v);
            return builder.build();
        }
    }

    /**
     * Build a tree of unknown size, in order.
     */
    public static <V> FastIntervalTreeBuilder<V> fastBuilder(IntervalComparators<V> comparators)
    {
        TinyThreadLocalPool.TinyPool<FastIntervalTreeBuilder<?>> pool = FastIntervalTreeBuilder.POOL.get();
        FastIntervalTreeBuilder<V> builder = (FastIntervalTreeBuilder<V>) pool.poll();
        if (builder == null)
            builder = new FastIntervalTreeBuilder<>();
        builder.pool = pool;
        builder.adapter.setComparators(comparators);
        return builder;
    }

    // TODO (desired): index leaf nodes
    static class IntervalMaxIndex
    {
        final Object[] sortedByEnd;
        final int[] sortedByEndIndex;
        final int[] sizeMap;

        IntervalMaxIndex(Object[] sortedByEnd, int[] sortedByEndIndex, int[] sizeMap)
        {
            this.sortedByEnd = sortedByEnd;
            this.sortedByEndIndex = sortedByEndIndex;
            this.sizeMap = sizeMap;
        }
    }

    /**
     * @return the size map for the branch node
     */
    static IntervalMaxIndex getIntervalMaxIndex(Object[] branchNode)
    {
        return (IntervalMaxIndex) branchNode[branchNode.length - 1];
    }
}
