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
package org.apache.cassandra.utils;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.AsymmetricOrdering.Op;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INTERVAL_TREE_EXPENSIVE_CHECKS;

public class IntervalTree<C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> implements Iterable<I>
{
    public static final boolean EXPENSIVE_CHECKS = TEST_INTERVAL_TREE_EXPENSIVE_CHECKS.getBoolean();
    private static final int REBUILD_AT_MOD_COUNT = 20;
    private static final String OUT_OF_ORDER_MESSAGE = "%s and %s out of order";

    private static final Logger logger = LoggerFactory.getLogger(IntervalTree.class);

    @SuppressWarnings("rawtypes")
    public static final Interval[] EMPTY_ARRAY = new Interval[0];

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final IntervalTree EMPTY_TREE = new IntervalTree(null);

    private final IntervalNode head;

    /**
     * Add can potentially unbalance the interval tree each time so force a rebuild after a certain number
     * of adds to bound how unbalanced the worst path in the tree can become.
     *
     * In practice it's likely the tree will have been rebuilt anyways long before it hits mod count, but it's not
     * good to leave it unbounded.
     *
     * Napkin math is a 100k interval tree is a large tree and lg2(100k) is 16 (lg2(1million) is 20) so by bounding it at 20 then
     * the worst possible imbalance is a bit more than double a balanced tree.
     */
    protected final int modCount;

    private final I[] intervalsByMinOrder;
    private final I[] intervalsByMaxOrder;

    @SuppressWarnings("unchecked")
    protected IntervalTree(Collection<I> intervals)
    {
        this.modCount = 0;
        if (intervals == null || intervals.isEmpty())
        {
            this.head = null;
            intervalsByMinOrder = intervalsByMaxOrder = (I[])EMPTY_ARRAY;
        }
        else if (intervals.size() == 1)
        {
            intervalsByMinOrder = intervalsByMaxOrder = (I[])new Interval[] { intervals.iterator().next() };
            this.head = new IntervalNode(intervals);
        }
        else
        {
            intervalsByMinOrder = intervals.toArray((I[])EMPTY_ARRAY);
            Arrays.sort(intervalsByMinOrder, Interval.minOrdering());
            intervalsByMaxOrder = intervals.toArray((I[])EMPTY_ARRAY);
            Arrays.sort(intervalsByMaxOrder, Interval.maxOrdering());
            this.head = new IntervalNode(Arrays.asList(intervalsByMinOrder), Arrays.asList(intervalsByMaxOrder));
        }
        if (EXPENSIVE_CHECKS)
            checkSortedArrays(false);
    }

    /**
     * This constructor will not modify minSortedIntervals and maxSortedIntervals, but it also won't
     * make defensive copies and will keep the originals.
     */
    @SuppressWarnings("unchecked")
    protected IntervalTree(I[] minSortedIntervals, I[] maxSortedIntervals)
    {
        this.modCount = 0;
        if (minSortedIntervals == null || minSortedIntervals.length == 0)
        {
            this.head = null;
            intervalsByMinOrder = intervalsByMaxOrder = (I[])EMPTY_ARRAY;
        }
        else if (minSortedIntervals.length == 1)
        {
            intervalsByMinOrder = intervalsByMaxOrder = minSortedIntervals;
            List<I> intervals = Collections.singletonList(minSortedIntervals[0]);
            this.head = new IntervalNode(intervals, intervals);
        }
        else
        {
            intervalsByMinOrder = minSortedIntervals;
            intervalsByMaxOrder = maxSortedIntervals;
            this.head = new IntervalNode(Arrays.asList(minSortedIntervals), Arrays.asList(maxSortedIntervals));
        }

        if (EXPENSIVE_CHECKS)
            checkSortedArrays(true);
    }

    private void checkSortedArrays(boolean strict)
    {
        checkOrdered(intervalsByMinOrder, Interval.<C, D>minOrdering(), strict);
        checkOrdered(intervalsByMaxOrder, Interval.<C, D>maxOrdering(), strict);
    }

    protected IntervalTree(IntervalNode head, int modCount, I[] minSortedIntervals, I[] maxSortedIntervals)
    {
        checkNotNull(minSortedIntervals, "minSortedIntervals is null");
        checkNotNull(maxSortedIntervals, "maxSortedIntervals is null");
        this.head = head;
        this.modCount = modCount;
        this.intervalsByMinOrder = minSortedIntervals;
        this.intervalsByMaxOrder = maxSortedIntervals;
        if (EXPENSIVE_CHECKS)
            checkSortedArrays(true);
    }

    protected IntervalTree<C, D, I> create(IntervalNode head, int modCount, @Nullable I[] minSortedIntervals, @Nullable I[] maxSortedIntervals)
    {
        return new IntervalTree<>(head, modCount, minSortedIntervals, maxSortedIntervals);
    }

    protected IntervalTree<C, D, I> create(I[] minOrder, I[] maxOrder)
    {
        return new IntervalTree<>(minOrder, maxOrder);
    }

    protected IntervalTree<C, D, I> create(Collection<I> intervals)
    {
        return new IntervalTree<>(intervals);
    }

    public static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> IntervalTree<C, D, I> build(Collection<I> intervals)
    {
        if (intervals == null || intervals.isEmpty())
            return emptyTree();

        return new IntervalTree<>(intervals);
    }

    @SuppressWarnings("unchecked")
    public static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> IntervalTree<C, D, I> emptyTree()
    {
        return EMPTY_TREE;
    }

    public int intervalCount()
    {
        return intervalsByMinOrder.length;
    }

    public boolean isEmpty()
    {
        return head == null;
    }

    public C max()
    {
        if (head == null)
            throw new IllegalStateException();

        return head.high;
    }

    public C min()
    {
        if (head == null)
            throw new IllegalStateException();

        return head.low;
    }

    public List<D> search(Interval<C, D> searchInterval)
    {
        if (head == null)
            return Collections.emptyList();

        List<D> results = new ArrayList<>();
        head.searchInternal(searchInterval, results);
        return results;
    }

    public List<D> search(C point)
    {
        return search(Interval.create(point, point, null));
    }

    @SuppressWarnings("unchecked")
    private I[] buildUpdatedArrayForUpdate(I[] existingSorted,
                                           I[] removalsSorted,
                                           I[] additionsSorted,
                                           AsymmetricOrdering<Interval<C, D>, C> cmp)
    {
        if (EXPENSIVE_CHECKS)
            checkOrdered(existingSorted, cmp, true);

        I[] result = mergeSorted(removeSorted(existingSorted, removalsSorted, cmp), additionsSorted, cmp);

        if (EXPENSIVE_CHECKS)
            checkOrdered(result, cmp, true);

        return result;
    }

    @SuppressWarnings("unchecked")
    private I[] removeSorted(I[] existingSorted, I[] removalsSorted, AsymmetricOrdering<Interval<C, D>, C> cmp)
    {
        if (removalsSorted.length == 0)
            return existingSorted;

        if (removalsSorted.length > existingSorted.length)
            throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[existingSorted.length]);
        I[] result = (I[]) new Interval[existingSorted.length - removalsSorted.length];
        int removalsIndex = 0;
        int resultIndex = 0;
        for (I existing : existingSorted)
        {
            if (removalsIndex < removalsSorted.length)
            {
                int c = cmp.compare(removalsSorted[removalsIndex], existing);
                if (c < 0)
                    throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[removalsIndex]);
                if (c == 0)
                {
                    checkState(removalsSorted[removalsIndex].data == existing.data, "Comparator does not implement identity");
                    removalsIndex++;
                    continue;
                }
            }
            if (resultIndex == result.length)
                throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[removalsIndex]);
            result[resultIndex++] = existing;
        }

        if (removalsIndex < removalsSorted.length)
            throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[removalsIndex]);

        return result;
    }

    @SuppressWarnings("unchecked")
    private I[] mergeSorted(I[] existingSorted, I[] additionsSorted, AsymmetricOrdering<Interval<C, D>, C> cmp)
    {
        if (additionsSorted.length == 0)
            return existingSorted;

        I[] result = (I[]) new Interval[existingSorted.length + additionsSorted.length];
        int existingIndex = 0;
        int additionsIndex = 0;
        int resultIndex = 0;
        while (existingIndex < existingSorted.length && additionsIndex < additionsSorted.length)
        {
            int c = cmp.compare(additionsSorted[additionsIndex], existingSorted[existingIndex]);
            if (c == 0)
                throw new IllegalStateException("Attempting to add duplicate interval: " + additionsSorted[additionsIndex]);
            result[resultIndex++] = c < 0 ? additionsSorted[additionsIndex++] : existingSorted[existingIndex++];
        }

        System.arraycopy(existingSorted, existingIndex, result, resultIndex, existingSorted.length - existingIndex);
        System.arraycopy(additionsSorted, additionsIndex, result, resultIndex + existingSorted.length - existingIndex, additionsSorted.length - additionsIndex);
        return result;
    }

    private static <T> void checkOrdered(T[] intervals, Comparator<? super T> ordering, boolean strict)
    {
        for (int i = 1; i < intervals.length; i++)
        {
            int c = ordering.compare(intervals[i - 1], intervals[i]);
            checkState(strict ? c < 0 : c <= 0, OUT_OF_ORDER_MESSAGE, intervals[i - 1], intervals[i]);
        }
    }

    /**
     * The input arrays aren't defensively copied and will be sorted. This update method doesn't allow duplicates or elements to be removed
     * to be missing and this differs from creating the tree from scratch using {@link #build(Collection) build(Collection&lt;I&gt;)} method which allows duplicates.
     *
     * There is also the requirement that D will implement Comparable&lt;D&gt; and that comparator will implement identity
     * which is not part of the normal contract of Comparable&lt;D&gt;. That means that if a.compareTo(b) == 0 then a == b;
     *
     * It made more sense for update to be stricter because it is tracking removals and additions explicitly instead of building
     * a list from scratch and in the targeted use case of a list of SSTables there are no duplicates. At a given point in time
     * an sstable represents exactly one interval (although it may switch via removal and addition as in early open).
     */
    @SuppressWarnings("unchecked")
    public IntervalTree<C, D, I> update(I[] removals, I[] additions)
    {
        if ((removals == null || removals.length == 0) && (additions == null || additions.length == 0))
            return this;

        if (removals == null)
            removals = (I[])EMPTY_ARRAY;
        if (additions == null)
            additions = (I[])EMPTY_ARRAY;

        Arrays.sort(removals, Interval.minOrdering());
        Arrays.sort(additions, Interval.minOrdering());

        if (EXPENSIVE_CHECKS)
        {
            for (int i = 1; i < additions.length; i++)
                checkState(Interval.<C, D>minOrdering().compare(additions[i], additions[i - 1]) != 0, "Duplicate interval in additions %s", additions[i]);
        }

        I[] newByMin = buildUpdatedArrayForUpdate(
        intervalsByMinOrder,
        removals,
        additions,
        Interval.minOrdering()
        );

        Arrays.sort(removals, Interval.maxOrdering());
        Arrays.sort(additions, Interval.maxOrdering());

        I[] newByMax = buildUpdatedArrayForUpdate(
        intervalsByMaxOrder,
        removals,
        additions,
        Interval.maxOrdering()
        );

        return create(newByMin, newByMax);
    }

    /**
     * The in practice use case here is flush which only adds one interval so do binary search
     */
    @SuppressWarnings("unchecked")
    private I[] buildUpdatedArrayForAdd(I[] addIntervals, I[] existingIntervals, AsymmetricOrdering<Interval<C, D>, C> ordering)
    {
        int newSize = existingIntervals.length + addIntervals.length;
        Arrays.sort(addIntervals, ordering);
        I[] newIntervals = (I[])new Interval[newSize];
        int newIndex = 0;
        int existingIndex = 0;

        int i = 0;
        for (; i < addIntervals.length; i++)
        {
            if (existingIndex >= existingIntervals.length)
                break;
            I addInterval = addIntervals[i];
            int insertionPoint = Arrays.binarySearch(existingIntervals, addInterval, ordering);
            checkState(insertionPoint < 0, "Interval being added should not already be present");
            insertionPoint = -1 - insertionPoint;
            if (insertionPoint > existingIndex)
            {
                int toCopy = insertionPoint - existingIndex;
                System.arraycopy(existingIntervals, existingIndex, newIntervals, newIndex, toCopy);
                newIndex += toCopy;
                existingIndex += toCopy;
            }
            newIntervals[newIndex++] = addInterval;
        }

        if (i < addIntervals.length)
            System.arraycopy(addIntervals, i, newIntervals, newIndex, addIntervals.length - i);

        if (existingIndex < existingIntervals.length)
            System.arraycopy(existingIntervals, existingIndex, newIntervals, newIndex, existingIntervals.length - existingIndex);

        return newIntervals;
    }

    public IntervalTree<C, D, I> add(I[] intervals)
    {
        if (head == null)
            return create(Arrays.asList(intervals));
        if (intervals.length == 0)
            return this;
        if (modCount + 1 >= REBUILD_AT_MOD_COUNT)
        {
            return create(new AbstractCollection<>()
            {
                @Override
                public Iterator<I> iterator()
                {
                    return Iterators.concat(IntervalTree.this.iterator(), Iterators.forArray(intervals));
                }

                @Override
                public int size()
                {
                    return intervalsByMinOrder.length + intervals.length;
                }
            });
        }

        // Add does not preserve iteration order, not even by interval bounds, so it's necessary to compute the arrays that preserve the minOrder
        // Or pay to sort and build them later
        I[] sortableIntervals = Arrays.copyOf(intervals, intervals.length);
        I[] newIntervalsByMinOrder = buildUpdatedArrayForAdd(sortableIntervals,
                                                             intervalsByMinOrder,
                                                             Interval.minOrdering());
        I[] newIntervalsByMaxOrder = buildUpdatedArrayForAdd(sortableIntervals,
                                                             intervalsByMaxOrder,
                                                             Interval.maxOrdering());

        return create(head.add(Arrays.asList(intervals)), modCount + 1, newIntervalsByMinOrder, newIntervalsByMaxOrder);
    }

    @Override
    public Iterator<I> iterator()
    {
        if (head == null)
            return Collections.emptyIterator();

        return Iterators.forArray(intervalsByMinOrder);
    }

    @Override
    public String toString()
    {
        return '<' + Joiner.on(", ").join(Iterables.limit(this, 100)) + '>';
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof IntervalTree))
            return false;
        IntervalTree<C, D, I> that = (IntervalTree<C, D, I>)o;
        return Iterators.elementsEqual(iterator(), that.iterator());
    }

    @Override
    public final int hashCode()
    {
        int result = 0;
        for (Interval<C, D> interval : this)
            result = 31 * result + interval.hashCode();
        return result;
    }

    private I[] buildUpdatedArrayForReplace(I[] existingSorted,
                                            List<Pair<I, I>> replacements,
                                            AsymmetricOrdering<Interval<C, D>, C> cmp)
    {
        I[] replacementArray = Arrays.copyOf(existingSorted, existingSorted.length);
        for (Pair<I, I> replacement : replacements)
        {
            I existingInterval = replacement.left;
            I newInterval = replacement.right;

            int removalIdx = Arrays.binarySearch(replacementArray, existingInterval, cmp);
            if (removalIdx < 0)
                throw new IllegalStateException("Interval to replace not found in the existing tree: " + existingInterval);
            checkState(existingInterval.data == replacementArray[removalIdx].data, "Comparator does not implement identity");

            int insertionIdx = Arrays.binarySearch(replacementArray, newInterval, cmp);
            checkState(insertionIdx < 0, "Value to be inserted already exists");
            insertionIdx = -1 - insertionIdx;

            if (insertionIdx > removalIdx)
            {
                // Shift everything from insertionIdx and left down to removalIdx
                System.arraycopy(replacementArray, removalIdx + 1, replacementArray, removalIdx, insertionIdx - removalIdx - 1);
                replacementArray[insertionIdx - 1] = newInterval;
            }
            else if (insertionIdx < removalIdx)
            {
                // Shift everything from insertionIdx and onward right to removalIdx
                System.arraycopy(replacementArray, insertionIdx, replacementArray, insertionIdx + 1, removalIdx - insertionIdx);
                replacementArray[Math.min(replacementArray.length, insertionIdx)] = newInterval;
            }
            else
            {
                replacementArray[insertionIdx] = newInterval;
            }
        }

        if (EXPENSIVE_CHECKS)
            checkOrdered(replacementArray, cmp, true);

        return replacementArray;
    }

    /**
     * This replace method doesn't work correctly with duplicates. If the tree already has duplicates each replacement (or duplicate replacement)
     * will replace one instance in the tree.
     *
     * There is also the requirement that D will implement Comparable&lt;D&gt; and that comparator will implement identity
     * which is not part of the normal contract of Comparable&lt;D&gt;. That means that if a.compareTo(b) == 0 then a == b;
     */
    public IntervalTree<C, D, I> replace(List<Pair<I, I>> replacements)
    {
        if (head == null)
        {
            checkArgument(replacements.isEmpty(), "Can't replace intervals in an empty tree");
            return this;
        }

        if (replacements.isEmpty())
            return this;

        List<Pair<I, I>> sortableReplacements = new ArrayList<>(replacements);
        I[] newIntervalsByMinOrder = buildUpdatedArrayForReplace(intervalsByMinOrder, sortableReplacements, Interval.minOrdering());
        I[] newIntervalsByMaxOrder = buildUpdatedArrayForReplace(intervalsByMaxOrder, sortableReplacements, Interval.maxOrdering());

        checkState(newIntervalsByMinOrder.length == newIntervalsByMaxOrder.length);
        if (EXPENSIVE_CHECKS)
            checkReplacementsApplied(newIntervalsByMinOrder, newIntervalsByMaxOrder, replacements);

        return create(head.replace(head, replacements),
                      modCount,
                      newIntervalsByMinOrder,
                      newIntervalsByMaxOrder);
    }

    private void checkReplacementsApplied(I[] newIntervalsByMinOrder, I[] newIntervalsByMaxOrder, List<Pair<I, I>> replacements)
    {
        boolean[] foundMinOrderReplacement = new boolean[replacements.size()];
        boolean[] foundMaxOrderReplacement = new boolean[replacements.size()];
        for (int i = 0; i < newIntervalsByMinOrder.length; i++)
        {
            markReplacementsFound(newIntervalsByMinOrder[i], replacements, foundMinOrderReplacement);
            markReplacementsFound(newIntervalsByMaxOrder[i], replacements, foundMaxOrderReplacement);
        }
        for (int i = 0; i < foundMaxOrderReplacement.length; i++)
            checkState(foundMinOrderReplacement[i] && foundMaxOrderReplacement[i], "Didn't find replacement value that should be present");
    }

    private void markReplacementsFound(I interval, List<Pair<I, I>> replacements, boolean[] found)
    {
        for (int j = 0; j < replacements.size(); j++)
        {
            Pair<I, I> replacement = replacements.get(j);
            if (interval.min.equals(replacement.left.min) && interval.max.equals(replacement.right.max))
            {
                checkState(interval.data != replacement.left.data);
                if (interval.data == replacement.right.data)
                {
                    checkState(!found[j], "Replacement value appears more than once");
                    found[j] = true;
                }
            }
        }
    }

    private static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> boolean replaceInSorted(List<I> original,
                                                                                                                                        List<I> updated,
                                                                                                                                        AsymmetricOrdering<Interval<C, D>, C> ordering,
                                                                                                                                        C key,
                                                                                                                                        I toRemove,
                                                                                                                                        I toAdd)
    {
        for (int i = ordering.binarySearchAsymmetric(original, key, Op.CEIL); i < original.size(); i++)
        {
            if (original.get(i).equals(toRemove))
            {
                updated.set(i, toAdd);
                return true;
            }
        }
        return false;
    }

    private static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> void insertSorted(List<I> sorted,
                                                                                                                                  I interval,
                                                                                                                                  AsymmetricOrdering<Interval<C, D>, C> ordering)
    {
        int idx = Collections.binarySearch(sorted, interval, ordering);
        checkState(idx < 0, "Should not add the same interval twice");
        sorted.add(-1 - idx, interval);
    }

    /**
     * Walks the min-ordered and max-ordered intervals as if merging all the endpoints, stopping after as many steps
     * as there are intervals. Returns the number of min endpoints and max endpoints consumed, which identify the median
     * endpoint used as the node center.
     */
    private static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> int[] findMedianPosition(List<I> minOrder, List<I> maxOrder)
    {
        int i = 0;
        int j = 0;
        for (int count = 0; count < minOrder.size(); count++)
        {
            if (i < minOrder.size() && (j >= maxOrder.size() || minOrder.get(i).min.compareTo(maxOrder.get(j).max) <= 0))
                i++;
            else
                j++;
        }
        return new int[] { i, j };
    }

    private static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> void checkCenter(List<I> intervals, C center)
    {
        List<C> allEndpoints = new ArrayList<>(intervals.size() * 2);
        for (I interval : intervals)
        {
            allEndpoints.add(interval.min);
            allEndpoints.add(interval.max);
        }

        Collections.sort(allEndpoints);
        C expectedCenter = allEndpoints.get(intervals.size());
        checkState(expectedCenter.equals(center));
    }

    private static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> void partitionAroundCenter(List<I> intervals,
                                                                                                                                           C center,
                                                                                                                                           List<I> leftOfCenter,
                                                                                                                                           List<I> rightOfCenter,
                                                                                                                                           List<I> intersectingCenter)
    {
        for (I candidate : intervals)
        {
            if (candidate.max.compareTo(center) < 0)
                leftOfCenter.add(candidate);
            else if (candidate.min.compareTo(center) > 0)
                rightOfCenter.add(candidate);
            else
                intersectingCenter.add(candidate);
        }
    }

    private static <T extends Comparable<? super T>> T lesser(T a, T b)
    {
        return a.compareTo(b) < 0 ? a : b;
    }

    private static <T extends Comparable<? super T>> T greater(T a, T b)
    {
        return a.compareTo(b) > 0 ? a : b;
    }

    protected class IntervalNode
    {
        final C center;
        final C low;
        final C high;

        final List<I> intersectsLeft;
        final List<I> intersectsRight;

        final IntervalNode left;
        final IntervalNode right;

        public IntervalNode(Collection<I> toBisect)
        {
            assert toBisect.size() == 1;
            I interval = toBisect.iterator().next();
            low = interval.min;
            center = interval.max;
            high = interval.max;
            List<I> l = Collections.singletonList(interval);
            intersectsLeft = l;
            intersectsRight = l;
            left = null;
            right = null;
        }

        public IntervalNode(List<I> minOrder, List<I> maxOrder)
        {
            assert !minOrder.isEmpty();
            logger.trace("Creating IntervalNode from {}", minOrder);

            // Building IntervalTree with one interval will be a reasonably
            // common case for range tombstones, so it's worth optimizing
            if (minOrder.size() == 1)
            {
                I interval = minOrder.iterator().next();
                low = interval.min;
                center = interval.max;
                high = interval.max;
                List<I> l = Collections.singletonList(interval);
                intersectsLeft = l;
                intersectsRight = l;
                left = null;
                right = null;
                return;
            }

            low = minOrder.get(0).min;
            high = maxOrder.get(maxOrder.size() - 1).max;

            int[] medianPosition = findMedianPosition(minOrder, maxOrder);
            int i = medianPosition[0];
            int j = medianPosition[1];

            if (i < minOrder.size() && (j >= maxOrder.size() || minOrder.get(i).min.compareTo(maxOrder.get(j).max) < 0))
                center = minOrder.get(i).min;
            else
                center = maxOrder.get(j).max;

            if (EXPENSIVE_CHECKS)
                checkCenter(minOrder, center);

            // Separate interval in intersecting center, left of center and right of center
            int initialIntersectionSize = i - j + 1;
            intersectsLeft = new ArrayList<>(initialIntersectionSize);
            intersectsRight = new ArrayList<>(initialIntersectionSize);
            int initialChildSize = Math.min(i, j);
            List<I> leftSegmentMinOrder = new ArrayList<>(initialChildSize);
            List<I> leftSegmentMaxOrder = new ArrayList<>(initialChildSize);
            List<I> rightSegmentMinOrder = new ArrayList<>(initialChildSize);
            List<I> rightSegmentMaxOrder = new ArrayList<>(initialChildSize);

            partitionAroundCenter(minOrder, center, leftSegmentMinOrder, rightSegmentMinOrder, intersectsLeft);
            partitionAroundCenter(maxOrder, center, leftSegmentMaxOrder, rightSegmentMaxOrder, intersectsRight);

            left = leftSegmentMinOrder.isEmpty() ? null : new IntervalNode(leftSegmentMinOrder, leftSegmentMaxOrder);
            right = rightSegmentMinOrder.isEmpty() ? null : new IntervalNode(rightSegmentMinOrder, rightSegmentMaxOrder);

            assert (intersectsLeft.size() == intersectsRight.size());
            if ((intersectsLeft.size() + leftSegmentMinOrder.size() + rightSegmentMinOrder.size()) != minOrder.size())
                throw new IllegalStateException("intersects (" + intersectsLeft.size() +
                                                ") + leftSegment (" + leftSegmentMinOrder.size() +
                                                ") + rightSegment (" + rightSegmentMinOrder.size() +
                                                ") != toBisect (" + minOrder.size() + ')');
        }

        public IntervalNode(C center, C low, C high, List<I> intersectsLeft, List<I> intersectsRight, IntervalNode left, IntervalNode right)
        {
            this.center = center;
            this.low = low;
            this.high = high;
            this.intersectsLeft = intersectsLeft;
            this.intersectsRight = intersectsRight;
            this.left = left;
            this.right = right;
        }

        void searchInternal(Interval<C, D> searchInterval, List<D> results)
        {
            if (center.compareTo(searchInterval.min) < 0)
            {
                int i = Interval.<C, D>maxOrdering().binarySearchAsymmetric(intersectsRight, searchInterval.min, Op.CEIL);
                if (i == intersectsRight.size() && high.compareTo(searchInterval.min) < 0)
                    return;

                while (i < intersectsRight.size())
                    results.add(intersectsRight.get(i++).data);

                if (right != null)
                    right.searchInternal(searchInterval, results);
            }
            else if (center.compareTo(searchInterval.max) > 0)
            {
                int j = Interval.<C, D>minOrdering().binarySearchAsymmetric(intersectsLeft, searchInterval.max, Op.HIGHER);
                if (j == 0 && low.compareTo(searchInterval.max) > 0)
                    return;

                for (int i = 0 ; i < j ; i++)
                    results.add(intersectsLeft.get(i).data);

                if (left != null)
                    left.searchInternal(searchInterval, results);
            }
            else
            {
                // Adds every interval contained in this node to the result set then search left and right for further
                // overlapping intervals
                for (Interval<C, D> interval : intersectsLeft)
                    results.add(interval.data);

                if (left != null)
                    left.searchInternal(searchInterval, results);
                if (right != null)
                    right.searchInternal(searchInterval, results);
            }
        }


        private IntervalNode replace(IntervalNode node, List<Pair<I, I>> replacements)
        {
            if (node == null || replacements.isEmpty())
                return node;

            List<Pair<I, I>> leftSegment = new ArrayList<>();
            List<Pair<I, I>> rightSegment = new ArrayList<>();
            List<I> newIntersectsLeft = null;
            List<I> newIntersectsRight = null;
            int updated = 0;

            for (Pair<I, I> entry : replacements)
            {
                I intervalToRemove = entry.left;
                I intervalToAdd = entry.right;
                if (node.center.compareTo(intervalToRemove.min) < 0)
                {
                    rightSegment.add(entry);
                }
                else if (node.center.compareTo(intervalToRemove.max) > 0)
                {
                    leftSegment.add(entry);
                }
                else
                {
                    // only init once if any interval resides in current node
                    if (newIntersectsLeft == null)
                    {
                        newIntersectsLeft = new ArrayList<>(node.intersectsLeft);
                        newIntersectsRight = new ArrayList<>(node.intersectsRight);
                    }
                    boolean leftUpdated = replaceInSorted(node.intersectsLeft, newIntersectsLeft, Interval.minOrdering(), intervalToRemove.min, intervalToRemove, intervalToAdd);
                    boolean rightUpdated = replaceInSorted(node.intersectsRight, newIntersectsRight, Interval.maxOrdering(), intervalToRemove.max, intervalToRemove, intervalToAdd);
                    assert leftUpdated && rightUpdated : "leftupdated = " + leftUpdated + ", rightupdated = " + rightUpdated;
                    updated++;
                }
            }

            assert leftSegment.size() + rightSegment.size() + updated == replacements.size() :
            "leftSegment size (" + leftSegment.size() + ") + rightSegment size (" + rightSegment.size() +
            ") + updated (" + updated + ") != replacementMap size (" + replacements.size() + ')';
            return new IntervalNode(node.center,
                                    node.low,
                                    node.high,
                                    newIntersectsLeft != null ? newIntersectsLeft : node.intersectsLeft,
                                    newIntersectsRight != null ? newIntersectsRight : node.intersectsRight,
                                    replace(node.left, leftSegment),
                                    replace(node.right, rightSegment));
        }

        private IntervalNode add(Collection<I> intervals)
        {
            return add(this, intervals);
        }

        private IntervalNode add(IntervalNode root, Collection<I> intervals)
        {
            if (intervals.isEmpty())
                return root;

            if (root == null)
            {
                List<I> minSortedIntervals = new ArrayList<>(intervals);
                Collections.sort(minSortedIntervals, Interval.minOrdering());
                List<I> maxSortedIntervals = new ArrayList<>(intervals);
                Collections.sort(maxSortedIntervals, Interval.maxOrdering());
                return new IntervalNode(minSortedIntervals, maxSortedIntervals);
            }

            List<I> leftSegment = new ArrayList<>();
            List<I> rightSegment = new ArrayList<>();
            C newLow = root.low;
            C newHigh = root.high;
            List<I> newIntersectsLeft = null;
            List<I> newIntersectsRight = null;
            for (I i : intervals)
            {
                newLow = lesser(newLow, i.min);
                newHigh = greater(newHigh, i.max);
                if (i.max.compareTo(root.center) < 0)
                {
                    leftSegment.add(i);
                }
                else if (i.min.compareTo(root.center) > 0)
                {
                    rightSegment.add(i);
                }
                else
                {
                    if (newIntersectsLeft == null)
                    {
                        newIntersectsLeft = new ArrayList<>(root.intersectsLeft);
                        newIntersectsRight = new ArrayList<>(root.intersectsRight);
                    }
                    insertSorted(newIntersectsLeft, i, Interval.minOrdering());
                    insertSorted(newIntersectsRight, i, Interval.maxOrdering());
                }
            }

            return new IntervalNode(root.center,
                                    newLow,
                                    newHigh,
                                    newIntersectsLeft != null ? newIntersectsLeft : root.intersectsLeft,
                                    newIntersectsRight != null ? newIntersectsRight : root.intersectsRight,
                                    add(root.left, leftSegment),
                                    add(root.right, rightSegment));
        }
    }

    public static class Builder<C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>>
    {
        private final List<I> intervals = new ArrayList<>();

        public Builder<C, D, I> addAll(IntervalTree<C, D, I> other)
        {
            other.forEach(intervals::add);
            return this;
        }

        public Builder<C, D, I> add(I interval)
        {
            intervals.add(interval);
            return this;
        }

        public Builder<C, D, I> removeIf(BiPredicate<C, C> predicate)
        {
            intervals.removeIf(i -> predicate.test(i.min, i.max));
            return this;
        }

        public Builder<C, D, I> removeIf(Predicate<D> predicate)
        {
            intervals.removeIf(i -> predicate.test(i.data));
            return this;
        }

        public IntervalTree<C, D, I> build()
        {
            return IntervalTree.build(intervals);
        }

        @Override
        public String toString()
        {
            return intervals.toString();
        }
    }
}