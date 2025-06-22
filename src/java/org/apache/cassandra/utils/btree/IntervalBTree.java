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

import java.util.Arrays;
import java.util.Comparator;

import accord.utils.Invariants;
import accord.utils.QuadFunction;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.utils.caching.TinyThreadLocalPool;

import static org.apache.cassandra.utils.btree.BTree.BRANCH_FACTOR;
import static org.apache.cassandra.utils.btree.BTree.Dir.ASC;
import static org.apache.cassandra.utils.btree.BTree.getBranchKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getChildCount;
import static org.apache.cassandra.utils.btree.BTree.getChildStart;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.getLeafKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isEmpty;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;
import static org.apache.cassandra.utils.btree.BTree.size;
import static org.apache.cassandra.utils.btree.BTree.sizeMap;
import static org.apache.cassandra.utils.btree.BTree.slice;

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
    public interface IntervalComparators<V>
    {
        Comparator<V> totalOrder();
        Comparator<V> startWithStartComparator();
        Comparator<V> startWithEndComparator();
        Comparator<V> endWithStartComparator();
        Comparator<V> endWithEndComparator();
    }

    public static class InclusiveEndKeyComparatorHelper
    {
        public static int keyStartWithIntervalStart(int c) { return equalsMeansBefore(c); }
        public static int intervalStartWithKeyStart(int c) { return equalsMeansAfter(c); }
        public static int keyStartWithIntervalEnd(int c) { return equalsMeansBefore(c); }
        public static int intervalStartWithKeyEnd(int c) { return equalsMeansAfter(c); }
        public static int keyEndWithIntervalStart(int c) { return equalsMeansAfter(c); }
        public static int intervalEndWithKeyStart(int c) { return equalsMeansAfter(c); }
        public static int intervalEndWithKeyEnd(int c) { return equalsMeansAfter(c); }
        public static int keyEndWithIntervalEnd(int c) { return equalsMeansBefore(c); }
        private static int equalsMeansAfter(int c) { return c == 0 ? 1 : c; }
        private static int equalsMeansBefore(int c) { return c == 0 ? -1 : c; }
    }

    /**
     * Apply the accumulation function over all intersecting intervals in the tree
     */
    public static <R, P1, P2, V> V accumulate(Object[] btree, IntervalComparators<R> comparators, R intersects, QuadFunction<P1, P2, R, V, V> function, P1 p1, P2 p2, V accumulate)
    {
        if (isLeaf(btree))
        {
            Comparator comparator = comparators.startWithEndComparator();
            int keyEnd = getLeafKeyEnd(btree);
            for (int i = 0; i < keyEnd; ++i)
            {
                R v = (R) btree[i];
                if (comparator.compare(v, intersects) < 0 && comparator.compare(intersects, v) < 0)
                    accumulate = function.apply(p1, p2, v, accumulate);
            }
        }
        else
        {
            int startKey = Arrays.binarySearch(btree, 0, getKeyEnd(btree), intersects, (Comparator) comparators.startWithStartComparator());
            int startChild;
            if (startKey >= 0) startChild = startKey + 1;
            else startChild = (startKey = -1 - startKey);
            int endKey = Arrays.binarySearch(btree, startKey, getKeyEnd(btree), intersects, (Comparator) comparators.startWithEndComparator());
            int endChild;
            if (endKey >= 0) endChild = 1 + endKey;
            else endChild = 1 + (endKey = -1 - endKey);

            {   // descend anything with a max that overlaps us that we won't already visit
                if (startChild > 0 || startKey > 0)
                    accumulate = accumulateMaxOnly(startChild + (startChild == endChild ? 1 : 0), startKey, btree, comparators, intersects, function, p1, p2, accumulate);
            }

            int childOffset = getChildStart(btree);
            if (startChild == startKey && startChild < endChild)
                accumulate = accumulate((Object[]) btree[childOffset + startChild++], comparators, intersects, function, p1, p2, accumulate);

            if (startKey < startChild && startKey < endKey)
                accumulate = function.apply(p1, p2, (R) btree[startKey++], accumulate);

            while (startChild < endChild - 1)
            {
                Invariants.require(startKey == startChild);
                accumulate = accumulate((Object[]) btree[childOffset + startChild++], function, p1, p2, accumulate);
                accumulate = function.apply(p1, p2, (R) btree[startKey++], accumulate);
            }
            if (startKey < endKey)
                accumulate = function.apply(p1, p2, (R) btree[startKey], accumulate);
            if (startChild < endChild)
                accumulate = accumulate((Object[]) btree[childOffset + startChild], comparators, intersects, function, p1, p2, accumulate);
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

    private static <R, P1, P2, V> V accumulateMaxOnly(int ifChildBefore, int ifKeyBefore, Object[] btree, IntervalComparators<R> comparators, R intersects, QuadFunction<P1, P2, R, V, V> function, P1 p1, P2 p2, V accumulate)
    {
        if (isLeaf(btree))
        {
            Invariants.require(ifChildBefore == Integer.MAX_VALUE);
            Comparator comparator = comparators.startWithEndComparator();
            for (int i = 0, maxi = getLeafKeyEnd(btree); i < maxi; ++i)
            {
                R v = (R) btree[i];
                if (comparator.compare(intersects, v) < 0)
                {
                    Invariants.paranoid(comparator.compare(v, intersects) < 0);
                    accumulate = function.apply(p1, p2, v, accumulate);
                }
            }
        }
        else
        {
            int keyEnd = getChildStart(btree);
            IntervalMaxIndex intervalMaxIndex = getIntervalMaxIndex(btree);
            int descendMaxStart = Arrays.binarySearch(intervalMaxIndex.sortedByEnd, intersects, (Comparator) comparators.endWithStartComparator());
            if (descendMaxStart < 0)
                descendMaxStart = -1 - descendMaxStart;
            int[] sortedByEndIndex = intervalMaxIndex.sortedByEndIndex;
            for (int i = descendMaxStart; i < sortedByEndIndex.length; ++i)
            {
                int index = sortedByEndIndex[i];
                if (index < ifChildBefore)
                    accumulate = accumulateMaxOnly(Integer.MAX_VALUE, Integer.MAX_VALUE, (Object[]) btree[keyEnd + index], comparators, intersects, function, p1, p2, accumulate);
            }
            Comparator comparator = comparators.startWithEndComparator();
            for (int i = 0, maxi = Math.min(ifKeyBefore, keyEnd); i < maxi; ++i)
            {
                R v = (R) btree[i];
                if (comparator.compare(intersects, v) < 0)
                {
                    Invariants.paranoid(comparator.compare(v, intersects) < 0);
                    accumulate = function.apply(p1, p2, v, accumulate);
                }
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

        SortEntry[] sort = new SortEntry[BRANCH_FACTOR];
        Comparator endSorter;

        public void override(Object[] branch)
        {
            int[] sizeMap = sizeMap(branch);
            int childStart = getChildStart(branch), childCount = getChildCount(branch);
            for (int i = 0; i < childCount; ++i)
            {
                Object[] child = (Object[]) branch[i + childStart];
                Object max = maxChild(child);
                if (sort[i] == null)
                    sort[i] = new SortEntry();
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
            branch[branch.length - 1] = new IntervalMaxIndex(sortedByEnd, sortedByEndIndex, sizeMap);
        }

        private Object maxChild(Object[] child)
        {
            Object max = child[0];
            for (int j = 1, jend = getKeyEnd(child); j < jend; ++j)
            {
                if (endSorter.compare(child[j], max) > 0)
                    max = child[j];
            }
            if (!isLeaf(child))
            {
                IntervalMaxIndex childIndex = getIntervalMaxIndex(child);
                Object maxChild = childIndex.sortedByEnd[childIndex.sortedByEnd.length - 1];
                if (endSorter.compare(maxChild, max) > 0)
                    max = maxChild;
            }
            return max;
        }

        @Override
        public int compare(SortEntry o1, SortEntry o2)
        {
            return endSorter.compare(o1.sort, o2.sort);
        }
    }

    static class IntervalBranchBuilder extends BTree.BranchBuilder
    {
        IntervalIndexAdapter adapter;

        IntervalBranchBuilder(BTree.LeafOrBranchBuilder child)
        {
            super(child);
        }

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this).init(adapter);
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
            adapter.override(branch);
            return result;
        }

        @Override
        void setRedistributedSizeMap(Object[] branch, int steal)
        {
            super.setRedistributedSizeMap(branch, steal);
            adapter.override(branch);
        }

        @Override
        int setOverflowSizeMap(Object[] branch, int keys)
        {
            int result = super.setOverflowSizeMap(branch, keys);
            adapter.override(branch);
            return result;
        }
    }

    public static class FastInteralTreeBuilder<V> extends BTree.FastBuilder<V>
    {
        private static final TinyThreadLocalPool<FastInteralTreeBuilder<?>> POOL = new TinyThreadLocalPool<>();
        final IntervalIndexAdapter adapter = new IntervalIndexAdapter();

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this).init(adapter);
        }

        @Override
        void initParent()
        {
            super.initParent();
            ((IntervalBranchBuilder) parent).init(adapter);
        }
    }

    static class IntervalUpdater<Compare, Existing extends Compare, Insert extends Compare> extends BTree.Updater<Compare, Existing, Insert>
    {
        static final TinyThreadLocalPool<IntervalUpdater> POOL = new TinyThreadLocalPool<>();
        private final IntervalIndexAdapter adapter = new IntervalIndexAdapter();

        static <Compare, Existing extends Compare, Insert extends Compare> IntervalUpdater<Compare, Existing, Insert> get(Comparator<Compare> compareEnds)
        {
            TinyThreadLocalPool.TinyPool<IntervalUpdater> pool = POOL.get();
            IntervalUpdater<Compare, Existing, Insert> updater = pool.poll();
            if (updater == null)
                updater = new IntervalUpdater<>();
            updater.pool = pool;
            updater.adapter.endSorter = compareEnds;
            return updater;
        }

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this).init(adapter);
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
            adapter.endSorter = null;
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

    static class Subtraction<K, T extends K> extends BTree.Subtraction<K, T>
    {
        static final FastThreadLocal<Subtraction> SHARED = new FastThreadLocal<>();
        private final IntervalIndexAdapter adapter = new IntervalIndexAdapter();

        Subtraction()
        {
            ((IntervalBranchBuilder)parent).adapter = adapter;
        }

        static <K, T extends K> Subtraction<K, T> get(IntervalComparators<K> comparators)
        {
            Subtraction subtraction = SHARED.get();
            if (subtraction == null)
                SHARED.set(subtraction = new Subtraction());
            subtraction.comparator = comparators.totalOrder();
            subtraction.adapter.endSorter = comparators.endWithEndComparator();
            return subtraction;
        }

        @Override
        BTree.BranchBuilder allocateParent()
        {
            return new IntervalBranchBuilder(this).init(adapter);
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
            reset();
            comparator = null;
            adapter.endSorter = null;
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
        try (Subtraction subtraction = IntervalBTree.Subtraction.get(comparators))
        {
            return subtraction.apply(toUpdate, slice(subtract, comparators.totalOrder(), ASC));
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

        try (IntervalUpdater<Compare, Existing, Insert> updater = IntervalUpdater.get(comparators.endWithEndComparator()))
        {
            return updater.update(existing, insert, comparators.totalOrder(), (UpdateFunction) UpdateFunction.noOp);
        }
    }


    /**
     * Build a tree of unknown size, in order.
     */
    public static <V> FastInteralTreeBuilder<V> fastBuilder(Comparator<V> endSorter)
    {
        TinyThreadLocalPool.TinyPool<FastInteralTreeBuilder<?>> pool = FastInteralTreeBuilder.POOL.get();
        FastInteralTreeBuilder<V> builder = (FastInteralTreeBuilder<V>) pool.poll();
        if (builder == null)
            builder = new FastInteralTreeBuilder<>();
        builder.pool = pool;
        builder.adapter.endSorter = endSorter;
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
