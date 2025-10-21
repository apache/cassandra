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
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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
    public IntervalTree(Collection<I> intervals)
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
        {
            if (intervalsByMinOrder.length > 1)
                for (int i = 1; i < intervalsByMinOrder.length; i++)
                    checkState(Interval.<C, D>minOrdering().compare(intervalsByMinOrder[i - 1], intervalsByMinOrder[i]) <= 0, "%s and %s out of order", intervalsByMinOrder[i-1], intervalsByMinOrder[i]);
            if (intervalsByMaxOrder.length > 1)
                for (int i = 1; i < intervalsByMaxOrder.length; i++)
                    checkState(Interval.<C, D>maxOrdering().compare(intervalsByMaxOrder[i - 1], intervalsByMaxOrder[i]) <= 0, "%s and %s out of order", intervalsByMaxOrder[i-1], intervalsByMaxOrder[i]);
        }
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
        {
            if (intervalsByMinOrder.length > 1)
                for (int i = 1; i < intervalsByMinOrder.length; i++)
                    checkState(Interval.<C, D>minOrdering().compare(intervalsByMinOrder[i - 1], intervalsByMinOrder[i]) < 0, "%s and %s out of order", intervalsByMinOrder[i-1], intervalsByMinOrder[i]);
            if (intervalsByMaxOrder.length > 1)
                for (int i = 1; i < intervalsByMaxOrder.length; i++)
                    checkState(Interval.<C, D>maxOrdering().compare(intervalsByMaxOrder[i - 1], intervalsByMaxOrder[i]) < 0, "%s and %s out of order", intervalsByMaxOrder[i-1], intervalsByMaxOrder[i]);
        }
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
        {
            if (intervalsByMinOrder.length > 1)
                for (int i = 1; i < intervalsByMinOrder.length; i++)
                    checkState(Interval.<C, D>minOrdering().compare(intervalsByMinOrder[i - 1], intervalsByMinOrder[i]) < 0, "%s and %s out of order", intervalsByMinOrder[i-1], intervalsByMinOrder[i]);
            if (intervalsByMaxOrder.length > 1)
                for (int i = 1; i < intervalsByMaxOrder.length; i++)
                    checkState(Interval.<C, D>maxOrdering().compare(intervalsByMaxOrder[i - 1], intervalsByMaxOrder[i]) < 0, "%s and %s out of order", intervalsByMaxOrder[i-1], intervalsByMaxOrder[i]);
        }
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

    public static <C extends Comparable<? super C>, D extends Comparable<? super D>, I extends Interval<C, D>> Builder<C, D, I> builder()
    {
        return new Builder<>();
    }

    public Builder<C, D, I> unbuild()
    {
        return new Builder<C, D, I>().addAll(this);
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

    public List<Interval<C, D>> matches(Interval<C, D> searchInterval)
    {
        if (head == null)
            return Collections.emptyList();

        List<Interval<C, D>> results = new ArrayList<>();
        head.searchInternal(searchInterval, results::add);
        return results;
    }

    public List<Interval<C, D>> matches(C point)
    {
        return matches(Interval.create(point, point, null));
    }

    public List<D> search(Interval<C, D> searchInterval)
    {
        if (head == null)
            return Collections.emptyList();

        List<D> results = new ArrayList<>();
        head.searchInternal(searchInterval, i -> results.add(i.data));
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
        {
            if (existingSorted.length > 1)
                for (int i = 1; i < existingSorted.length; i++)
                    checkState(cmp.compare(existingSorted[i - 1], existingSorted[i]) < 0, "%s and %s out of order", existingSorted[i-1], existingSorted[i]);
        }

        int finalSize = existingSorted.length + additionsSorted.length - removalsSorted.length;
        I[] result = (I[]) new Interval[finalSize];

        int existingIndex = 0;
        int removalsIndex  = 0;
        int additionsIndex = 0;
        int resultIndex    = 0;

        while (existingIndex < existingSorted.length)
        {
            I currentExisting = existingSorted[existingIndex];

            int c;
            while (removalsIndex < removalsSorted.length
                   && (c = cmp.compare(removalsSorted[removalsIndex], currentExisting)) <= 0)
            {
                if (c < 0)
                {
                    throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[removalsIndex]);
                }
                else
                {
                    checkState(removalsSorted[removalsIndex].data == currentExisting.data, "Comparator does not implement identity");
                    existingIndex++;
                    removalsIndex++;

                    if (existingIndex >= existingSorted.length)
                        break;
                    currentExisting = existingSorted[existingIndex];
                }
            }

            if (existingIndex >= existingSorted.length)
                break;

            while (additionsIndex < additionsSorted.length)
            {
                int additionCmp = cmp.compare(additionsSorted[additionsIndex], currentExisting);
                if (additionCmp == 0)
                    throw new IllegalStateException("Attempting to add duplicate interval: " + additionsSorted[additionsIndex]);
                else if (additionCmp < 0)
                    result[resultIndex++] = additionsSorted[additionsIndex++];
                else
                    break;
            }

            result[resultIndex++] = currentExisting;
            existingIndex++;
        }

        if (removalsIndex < removalsSorted.length)
            throw new IllegalStateException("Removal interval not found in the existing tree: " + removalsSorted[removalsIndex]);

        while (additionsIndex < additionsSorted.length)
            result[resultIndex++] = additionsSorted[additionsIndex++];

        if (EXPENSIVE_CHECKS)
        {
            if (result.length > 1)
                for (int i = 1; i < result.length; i++)
                    checkState(cmp.compare(result[i - 1], result[i]) < 0, "%s and %s out of order", result[i-1], result[i]);
        }

        return result;
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

    public Stream<I> stream()
    {
        return StreamSupport.stream(spliterator(), false);
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
        {
            if (replacementArray.length > 1)
                for (int i = 1; i < replacementArray.length; i++)
                    checkState(cmp.compare(replacementArray[i - 1], replacementArray[i]) < 0, "%s and %s out of order", replacementArray[i-1], replacementArray[i]);
        }

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
        {
            boolean[] foundMinOrderReplacement = new boolean[replacements.size()];
            boolean[] foundMaxOrderReplacement = new boolean[replacements.size()];
            for (int i = 0; i < newIntervalsByMinOrder.length; i++)
            {
                for (int j = 0; j < replacements.size(); j++)
                {
                    Pair<I, I> replacement = replacements.get(j);
                    if (newIntervalsByMinOrder[i].min.equals(replacement.left.min) && newIntervalsByMinOrder[i].max.equals(replacement.right.max))
                    {
                        checkState(newIntervalsByMinOrder[i].data != replacement.left.data);
                        if (newIntervalsByMinOrder[i].data == replacement.right.data)
                        {
                            checkState(!foundMinOrderReplacement[j], "Replacement value appears more than once");
                            foundMinOrderReplacement[j] = true;
                        }
                    }

                    if (newIntervalsByMaxOrder[i].min.equals(replacement.left.min) && newIntervalsByMaxOrder[i].max.equals(replacement.right.max))
                    {
                        checkState(newIntervalsByMaxOrder[i].data != replacement.left.data);
                        if (newIntervalsByMaxOrder[i].data == replacement.right.data)
                        {
                            checkState(!foundMaxOrderReplacement[j], "Replacement value appears more than once");
                            foundMaxOrderReplacement[j] = true;
                        }
                    }
                }
            }
            for (int i = 0; i < foundMaxOrderReplacement.length; i++)
                checkState(foundMinOrderReplacement[i] && foundMaxOrderReplacement[i], "Didn't find replacement value that should be present");
        }

        return create(head.replace(head, replacements),
                      modCount,
                      newIntervalsByMinOrder,
                      newIntervalsByMaxOrder);
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

            int i = 0, j = 0, count = 0;
            while (count < minOrder.size())
            {
                if (i < minOrder.size() && (j >= maxOrder.size() || minOrder.get(i).min.compareTo(maxOrder.get(j).max) <= 0))
                    i++;
                else
                    j++;
                count++;
            }

            if (i < minOrder.size() && (j >= maxOrder.size() || minOrder.get(i).min.compareTo(maxOrder.get(j).max) < 0))
                center = minOrder.get(i).min;
            else
                center = maxOrder.get(j).max;

            if (EXPENSIVE_CHECKS)
            {
                List<C> allEndpoints = new ArrayList<>(minOrder.size() * 2);
                for (I interval : minOrder)
                {
                    allEndpoints.add(interval.min);
                    allEndpoints.add(interval.max);
                }

                Collections.sort(allEndpoints);
                C expectedCenter = allEndpoints.get(minOrder.size());
                checkState(expectedCenter.equals(center));
            }

            // Separate interval in intersecting center, left of center and right of center
            int initialIntersectionSize = i - j + 1;
            intersectsLeft = new ArrayList<>(initialIntersectionSize);
            intersectsRight = new ArrayList<>(initialIntersectionSize);
            int initialChildSize = Math.min(i, j);
            List<I> leftSegmentMinOrder = new ArrayList<>(initialChildSize);
            List<I> leftSegmentMaxOrder = new ArrayList<>(initialChildSize);
            List<I> rightSegmentMinOrder = new ArrayList<>(initialChildSize);
            List<I> rightSegmentMaxOrder = new ArrayList<>(initialChildSize);

            for (I candidate : minOrder)
            {
                if (candidate.max.compareTo(center) < 0)
                    leftSegmentMinOrder.add(candidate);
                else if (candidate.min.compareTo(center) > 0)
                    rightSegmentMinOrder.add(candidate);
                else
                    intersectsLeft.add(candidate);
            }

            for (I candidate : maxOrder)
            {
                if (candidate.max.compareTo(center) < 0)
                    leftSegmentMaxOrder.add(candidate);
                else if (candidate.min.compareTo(center) > 0)
                    rightSegmentMaxOrder.add(candidate);
                else
                    intersectsRight.add(candidate);
            }

            left = leftSegmentMinOrder.isEmpty() ? null : new IntervalNode(leftSegmentMinOrder, leftSegmentMaxOrder);
            right = rightSegmentMinOrder.isEmpty() ? null : new IntervalNode(rightSegmentMinOrder, rightSegmentMaxOrder);

            assert (intersectsLeft.size() == intersectsRight.size());
            assert (intersectsLeft.size() + leftSegmentMinOrder.size() + rightSegmentMinOrder.size()) == minOrder.size() :
            "intersects (" + intersectsLeft.size() +
            ") + leftSegment (" + leftSegmentMinOrder.size() +
            ") + rightSegment (" + rightSegmentMinOrder.size() +
            ") != toBisect (" + minOrder.size() + ')';
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

        void searchInternal(Interval<C, D> searchInterval, Consumer<Interval<C, D>> results)
        {
            if (center.compareTo(searchInterval.min) < 0)
            {
                int i = Interval.<C, D>maxOrdering().binarySearchAsymmetric(intersectsRight, searchInterval.min, Op.CEIL);
                if (i == intersectsRight.size() && high.compareTo(searchInterval.min) < 0)
                    return;

                while (i < intersectsRight.size())
                    results.accept(intersectsRight.get(i++));

                if (right != null)
                    right.searchInternal(searchInterval, results);
            }
            else if (center.compareTo(searchInterval.max) > 0)
            {
                int j = Interval.<C, D>minOrdering().binarySearchAsymmetric(intersectsLeft, searchInterval.max, Op.HIGHER);
                if (j == 0 && low.compareTo(searchInterval.max) > 0)
                    return;

                for (int i = 0 ; i < j ; i++)
                    results.accept(intersectsLeft.get(i));

                if (left != null)
                    left.searchInternal(searchInterval, results);
            }
            else
            {
                // Adds every interval contained in this node to the result set then search left and right for further
                // overlapping intervals
                for (Interval<C, D> interval : intersectsLeft)
                    results.accept(interval);

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
                    boolean leftUpdated = false;
                    boolean rightUpdated = false;

                    int i = Interval.<C, D>minOrdering().binarySearchAsymmetric(node.intersectsLeft, intervalToRemove.min, Op.CEIL);
                    while (i < node.intersectsLeft.size())
                    {
                        if (node.intersectsLeft.get(i).equals(intervalToRemove))
                        {
                            newIntersectsLeft.set(i, intervalToAdd);
                            leftUpdated = true;
                            break;
                        }
                        i++;
                    }

                    int j = Interval.<C, D>maxOrdering().binarySearchAsymmetric(node.intersectsRight, intervalToRemove.max, Op.CEIL);
                    while (j < node.intersectsRight.size())
                    {
                        if (node.intersectsRight.get(j).equals(intervalToRemove))
                        {
                            newIntersectsRight.set(j, intervalToAdd);
                            rightUpdated = true;
                            break;
                        }
                        j++;
                    }
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
                newLow = newLow.compareTo(i.min) < 0 ? newLow : i.min;
                newHigh = newHigh.compareTo(i.max) > 0 ? newHigh : i.max;
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
                    int leftIdx = Collections.binarySearch(newIntersectsLeft, i, Interval.minOrdering());
                    checkState(leftIdx < 0, "Should not add the same interval twice");
                    leftIdx = -1 - leftIdx;
                    newIntersectsLeft.add(leftIdx, i);

                    int rightIdx = Collections.binarySearch(newIntersectsRight, i, Interval.maxOrdering());
                    checkState(rightIdx < 0, "Should not add the same interval twice");
                    rightIdx = -1 - rightIdx;
                    newIntersectsRight.add(rightIdx, i);
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

        public Builder<C, D, I> removeIf(TriPredicate<C, C, D> predicate)
        {
            intervals.removeIf(i -> predicate.test(i.min, i.max, i.data));
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