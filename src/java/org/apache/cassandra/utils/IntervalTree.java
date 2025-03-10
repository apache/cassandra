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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AsymmetricOrdering.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IntervalTree<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements Iterable<I>
{
    private static final Logger logger = LoggerFactory.getLogger(IntervalTree.class);

    @SuppressWarnings("unchecked")
    private static final IntervalTree EMPTY_TREE = new IntervalTree(null);

    private final IntervalNode head;
    private final int count;
    // Tracks updates to ensure rebalancing occurs before the tree becomes too imbalanced
    private final int updateCount;

    protected IntervalTree(int count, IntervalNode head, int cnt)
    {
        this.head = head;
        this.count = count;
        this.updateCount = cnt;
    }

    protected IntervalTree(Collection<I> intervals)
    {
        // rebuild
        this.updateCount = 0;
        if (DatabaseDescriptor.isDaemonInitialized() &&
            DatabaseDescriptor.getUseNewBehaviorForIntervalTreeBuild())
        {
            // CASSANDRA-19596
            if (intervals == null || intervals.isEmpty())
            {
                this.head = null;
                this.count = 0;
            }
            else if (intervals.size() == 1)
            {
                this.head = new IntervalNode(intervals.iterator().next());
                this.count = intervals.size();
            }
            else
            {
                List<I> minSortedIntervals = new ArrayList<>(intervals);
                Collections.sort(minSortedIntervals, Interval.minOrdering());
                List<I> maxSortedIntervals = new ArrayList<>(intervals);
                Collections.sort(maxSortedIntervals, Interval.maxOrdering());
                this.head = new IntervalNode(minSortedIntervals, minSortedIntervals, maxSortedIntervals);
                this.count = intervals.size();
            }
        }
        else
        {
            this.head = intervals == null || intervals.isEmpty() ? null : new IntervalNode(intervals);
            this.count = intervals == null ? 0 : intervals.size();
        }
    }

    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> build(Collection<I> intervals)
    {
        if (intervals == null || intervals.isEmpty())
            return emptyTree();

        return new IntervalTree<C, D, I>(intervals);
    }

    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> Serializer<C, D, I> serializer(ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, Constructor<I> constructor)
    {
        return new Serializer<>(pointSerializer, dataSerializer, constructor);
    }

    @SuppressWarnings("unchecked")
    public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> emptyTree()
    {
        return EMPTY_TREE;
    }

    public int intervalCount()
    {
        return count;
    }

    public int updateCount()
    {
        return updateCount;
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
            return Collections.<D>emptyList();

        List<D> results = new ArrayList<D>();
        head.searchInternal(searchInterval, results);
        return results;
    }

    public List<D> search(C point)
    {
        return search(Interval.<C, D>create(point, point, null));
    }

    public Iterator<I> iterator()
    {
        if (head == null)
            return Collections.emptyIterator();

        return new TreeIterator(head);
    }

    @Override
    public String toString()
    {
        return "<" + Joiner.on(", ").join(this) + ">";
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof IntervalTree))
            return false;
        IntervalTree that = (IntervalTree)o;
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

    protected IntervalNode copyAndReplace(Map<I, I> replacementMap)
    {
        return head.copyAndReplaceHelper(head, replacementMap);
    }

    protected IntervalNode copyAndAddIntervals(List<I> sstables)
    {
        return head.copyAndAddIntervalsHelper(head, sstables);
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

        public IntervalNode(I interval)
        {
            low = interval.min;
            center = interval.max;
            high = interval.max;
            List<I> l = Collections.singletonList(interval);
            intersectsLeft = l;
            intersectsRight = l;
            left = null;
            right = null;
        }

        public IntervalNode(Collection<I> toBisect)
        {
            assert !toBisect.isEmpty();
            logger.trace("Creating IntervalNode from {}", toBisect);

            // Building IntervalTree with one interval will be a reasonably
            // common case for range tombstones, so it's worth optimizing
            if (toBisect.size() == 1)
            {
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
            else
            {
                // Find min, median and max
                List<C> allEndpoints = new ArrayList<C>(toBisect.size() * 2);
                for (I interval : toBisect)
                {
                    allEndpoints.add(interval.min);
                    allEndpoints.add(interval.max);
                }

                Collections.sort(allEndpoints);

                low = allEndpoints.get(0);
                center = allEndpoints.get(toBisect.size());
                high = allEndpoints.get(allEndpoints.size() - 1);

                // Separate interval in intersecting center, left of center and right of center
                List<I> intersects = new ArrayList<I>();
                List<I> leftSegment = new ArrayList<I>();
                List<I> rightSegment = new ArrayList<I>();

                for (I candidate : toBisect)
                {
                    if (candidate.max.compareTo(center) < 0)
                        leftSegment.add(candidate);
                    else if (candidate.min.compareTo(center) > 0)
                        rightSegment.add(candidate);
                    else
                        intersects.add(candidate);
                }

                intersectsLeft = Interval.<C, D>minOrdering().sortedCopy(intersects);
                intersectsRight = Interval.<C, D>maxOrdering().sortedCopy(intersects);
                left = leftSegment.isEmpty() ? null : new IntervalNode(leftSegment);
                right = rightSegment.isEmpty() ? null : new IntervalNode(rightSegment);

                assert (intersects.size() + leftSegment.size() + rightSegment.size()) == toBisect.size() :
                "intersects (" + String.valueOf(intersects.size()) +
                ") + leftSegment (" + String.valueOf(leftSegment.size()) +
                ") + rightSegment (" + String.valueOf(rightSegment.size()) +
                ") != toBisect (" + String.valueOf(toBisect.size()) + ")";
            }
        }

        public IntervalNode(List<I> toBisect, List<I> minOrder, List<I> maxOrder)
        {
            assert !toBisect.isEmpty();
            logger.trace("Creating IntervalNode from {}", toBisect);

            // Building IntervalTree with one interval will be a reasonably
            // common case for range tombstones, so it's worth optimizing
            if (toBisect.size() == 1)
            {
                I interval = toBisect.iterator().next();
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

            int totalPoints = minOrder.size() * 2;
            int midIndex = totalPoints / 2;
            int i = 0, j = 0, count = 0;
            while (count < midIndex)
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

            // Separate interval in intersecting center, left of center and right of center
            intersectsLeft = new ArrayList<I>();
            intersectsRight = new ArrayList<I>();
            List<I> leftSegmentMinOrder = new ArrayList<I>();
            List<I> leftSegmentMaxOrder = new ArrayList<>();
            List<I> rightSegmentMinOrder = new ArrayList<I>();
            List<I> rightSegmentMaxOrder = new ArrayList<>();

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

            left = leftSegmentMinOrder.isEmpty() ? null : new IntervalNode(leftSegmentMinOrder, leftSegmentMinOrder, leftSegmentMaxOrder);
            right = rightSegmentMinOrder.isEmpty() ? null : new IntervalNode(rightSegmentMinOrder, rightSegmentMinOrder, rightSegmentMaxOrder);

            assert (intersectsLeft.size() == intersectsRight.size());
            assert (intersectsLeft.size() + leftSegmentMinOrder.size() + rightSegmentMinOrder.size()) == toBisect.size() :
            "intersects (" + String.valueOf(intersectsLeft.size()) +
            ") + leftSegment (" + String.valueOf(leftSegmentMinOrder.size()) +
            ") + rightSegment (" + String.valueOf(rightSegmentMinOrder.size()) +
            ") != toBisect (" + String.valueOf(toBisect.size()) + ")";
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

        private IntervalNode copyAndReplaceHelper(IntervalNode node, Map<I, I> replacementMap)
        {
            if (node == null || replacementMap.isEmpty())
                return node;

            Map<I, I> leftSegment = new HashMap<>();
            Map<I, I> rightSegment = new HashMap<>();
            List<I> newIntersectsLeft = null;
            List<I> newIntersectsRight = null;
            int updated = 0;

            for (Map.Entry<I, I> entry : replacementMap.entrySet())
            {
                I intervalToRemove = entry.getKey();
                I intervalToAdd = entry.getValue();
                if (node.center.compareTo(intervalToRemove.min) < 0)
                {
                    rightSegment.put(intervalToRemove, intervalToAdd);
                }
                else if (node.center.compareTo(intervalToRemove.max) > 0)
                {
                    leftSegment.put(intervalToRemove, intervalToAdd);
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

            assert leftSegment.size() + rightSegment.size() + updated == replacementMap.size() :
            "leftSegment size (" + leftSegment.size() + ") + rightSegment size (" + rightSegment.size() +
            ") + updated (" + updated + ") != replacementMap size (" + replacementMap.size() + ')';
            return new IntervalNode(node.center,
                                    node.low,
                                    node.high,
                                    newIntersectsLeft != null ? newIntersectsLeft : node.intersectsLeft,
                                    newIntersectsRight != null ? newIntersectsRight : node.intersectsRight,
                                    copyAndReplaceHelper(node.left, leftSegment),
                                    copyAndReplaceHelper(node.right, rightSegment));
        }

        private IntervalNode copyAndAddIntervalsHelper(IntervalNode node, List<I> intervals)
        {
            if (intervals.isEmpty())
                return node;
            if (node == null)
                return new IntervalNode(intervals);

            List<I> leftSegment = new ArrayList<>();
            List<I> rightSegment = new ArrayList<>();
            C newLow = node.low;
            C newHigh = node.high;
            List<I> newIntersectsLeft = null;
            List<I> newIntersectsRight = null;
            for (I i : intervals)
            {
                newLow = newLow.compareTo(i.min) < 0 ? newLow : i.min;
                newHigh = newHigh.compareTo(i.max) > 0 ? newHigh : i.max;
                if (i.max.compareTo(node.center) < 0)
                {
                    leftSegment.add(i);
                }
                else if (i.min.compareTo(node.center) > 0)
                {
                    rightSegment.add(i);
                }
                else
                {
                    // only init once if any interval resides in current node
                    if (newIntersectsLeft == null)
                    {
                        newIntersectsLeft = new ArrayList<>(node.intersectsLeft);
                        newIntersectsRight = new ArrayList<>(node.intersectsRight);
                    }
                    int leftIdx = Interval.<C, D>minOrdering().binarySearchAsymmetric(newIntersectsLeft, i.min, Op.CEIL);
                    newIntersectsLeft.add(leftIdx, i);

                    int rightIdx = Interval.<C, D>maxOrdering().binarySearchAsymmetric(newIntersectsRight, i.max, Op.HIGHER);
                    newIntersectsRight.add(rightIdx, i);
                }
            }
            return new IntervalNode(node.center,
                                    newLow,
                                    newHigh,
                                    newIntersectsLeft != null ? newIntersectsLeft : node.intersectsLeft,
                                    newIntersectsRight != null ? newIntersectsRight : node.intersectsRight,
                                    copyAndAddIntervalsHelper(node.left, leftSegment),
                                    copyAndAddIntervalsHelper(node.right, rightSegment));
        }
    }

    private class TreeIterator extends AbstractIterator<I>
    {
        private final Deque<IntervalNode> stack = new ArrayDeque<IntervalNode>();
        private Iterator<I> current;

        TreeIterator(IntervalNode node)
        {
            super();
            gotoMinOf(node);
        }

        protected I computeNext()
        {
            while (true)
            {
                if (current != null && current.hasNext())
                    return current.next();

                IntervalNode node = stack.pollFirst();
                if (node == null)
                    return endOfData();

                current = node.intersectsLeft.iterator();

                // We know this is the smaller not returned yet, but before doing
                // its parent, we must do everyone on it's right.
                gotoMinOf(node.right);
            }
        }

        private void gotoMinOf(IntervalNode node)
        {
            while (node != null)
            {
                stack.offerFirst(node);
                node = node.left;
            }

        }
    }

    public static class Serializer<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements IVersionedSerializer<IntervalTree<C, D, I>>
    {
        private final ISerializer<C> pointSerializer;
        private final ISerializer<D> dataSerializer;
        private final Constructor<I> constructor;

        private Serializer(ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, Constructor<I> constructor)
        {
            this.pointSerializer = pointSerializer;
            this.dataSerializer = dataSerializer;
            this.constructor = constructor;
        }

        public void serialize(IntervalTree<C, D, I> it, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(it.count);
            for (Interval<C, D> interval : it)
            {
                pointSerializer.serialize(interval.min, out);
                pointSerializer.serialize(interval.max, out);
                dataSerializer.serialize(interval.data, out);
            }
        }

        /**
         * Deserialize an IntervalTree whose keys use the natural ordering.
         * Use deserialize(DataInput, int, Comparator) instead if the interval
         * tree is to use a custom comparator, as the comparator is *not*
         * serialized.
         */
        public IntervalTree<C, D, I> deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in, version, null);
        }

        public IntervalTree<C, D, I> deserialize(DataInputPlus in, int version, Comparator<C> comparator) throws IOException
        {
            try
            {
                int count = in.readInt();
                List<I> intervals = new ArrayList<I>(count);
                for (int i = 0; i < count; i++)
                {
                    C min = pointSerializer.deserialize(in);
                    C max = pointSerializer.deserialize(in);
                    D data = dataSerializer.deserialize(in);
                    intervals.add(constructor.newInstance(min, max, data));
                }
                return new IntervalTree<C, D, I>(intervals);
            }
            catch (InstantiationException | InvocationTargetException | IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        }

        public long serializedSize(IntervalTree<C, D, I> it, int version)
        {
            long size = TypeSizes.sizeof(0);
            for (Interval<C, D> interval : it)
            {
                size += pointSerializer.serializedSize(interval.min);
                size += pointSerializer.serializedSize(interval.max);
                size += dataSerializer.serializedSize(interval.data);
            }
            return size;
        }
    }
}
