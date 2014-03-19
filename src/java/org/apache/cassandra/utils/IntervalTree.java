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

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class IntervalTree<C, D, I extends Interval<C, D>> implements Iterable<I>
{
    private static final Logger logger = LoggerFactory.getLogger(IntervalTree.class);

    @SuppressWarnings("unchecked")
    private static final IntervalTree EMPTY_TREE = new IntervalTree(null, null);

    private final IntervalNode head;
    private final int count;
    private final Comparator<C> comparator;

    final Ordering<I> minOrdering;
    final Ordering<I> maxOrdering;

    protected IntervalTree(Collection<I> intervals, Comparator<C> comparator)
    {
        this.comparator = comparator;

        final IntervalTree it = this;
        this.minOrdering = new Ordering<I>()
        {
            public int compare(I interval1, I interval2)
            {
                return it.comparePoints(interval1.min, interval2.min);
            }
        };
        this.maxOrdering = new Ordering<I>()
        {
            public int compare(I interval1, I interval2)
            {
                return it.comparePoints(interval1.max, interval2.max);
            }
        };

        this.head = intervals == null || intervals.isEmpty() ? null : new IntervalNode(intervals);
        this.count = intervals == null ? 0 : intervals.size();
    }

    public static <C, D, I extends Interval<C, D>> IntervalTree<C, D, I> build(Collection<I> intervals, Comparator<C> comparator)
    {
        if (intervals == null || intervals.isEmpty())
            return emptyTree();

        return new IntervalTree<C, D, I>(intervals, comparator);
    }

    public static <C extends Comparable<C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> build(Collection<I> intervals)
    {
        if (intervals == null || intervals.isEmpty())
            return emptyTree();

        return new IntervalTree<C, D, I>(intervals, null);
    }

    public static <C, D, I extends Interval<C, D>> Serializer<C, D, I> serializer(ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, Constructor<I> constructor)
    {
        return new Serializer<>(pointSerializer, dataSerializer, constructor);
    }

    @SuppressWarnings("unchecked")
    public static <C, D, I extends Interval<C, D>> IntervalTree<C, D, I> emptyTree()
    {
        return (IntervalTree<C, D, I>)EMPTY_TREE;
    }

    public Comparator<C> comparator()
    {
        return comparator;
    }

    public int intervalCount()
    {
        return count;
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
            return Iterators.<I>emptyIterator();

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
        int result = comparator.hashCode();
        for (Interval<C, D> interval : this)
            result = 31 * result + interval.hashCode();
        return result;
    }

    private int comparePoints(C point1, C point2)
    {
        if (comparator != null)
        {
            return comparator.compare(point1, point2);
        }
        else
        {
            assert point1 instanceof Comparable;
            assert point2 instanceof Comparable;
            return ((Comparable<C>)point1).compareTo(point2);
        }
    }

    private boolean encloses(Interval<C, D> enclosing, Interval<C, D> enclosed)
    {
        return comparePoints(enclosing.min, enclosed.min) <= 0
            && comparePoints(enclosing.max, enclosed.max) >= 0;
    }

    private boolean contains(Interval<C, D> interval, C point)
    {
        return comparePoints(interval.min, point) <= 0
            && comparePoints(interval.max, point) >= 0;
    }

    private boolean intersects(Interval<C, D> interval1, Interval<C, D> interval2)
    {
        return contains(interval1, interval2.min) || contains(interval1, interval2.max);
    }

    private class IntervalNode
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
                    assert (comparator == null ? ((Comparable)interval.min).compareTo(interval.max)
                                               : comparator.compare(interval.min, interval.max)) <= 0 : "Interval min > max";
                    allEndpoints.add(interval.min);
                    allEndpoints.add(interval.max);
                }
                if (comparator != null)
                    Collections.sort(allEndpoints, comparator);
                else
                    Collections.sort((List<Comparable>)allEndpoints);

                low = allEndpoints.get(0);
                center = allEndpoints.get(toBisect.size());
                high = allEndpoints.get(allEndpoints.size() - 1);

                // Separate interval in intersecting center, left of center and right of center
                List<I> intersects = new ArrayList<I>();
                List<I> leftSegment = new ArrayList<I>();
                List<I> rightSegment = new ArrayList<I>();

                for (I candidate : toBisect)
                {
                    if (comparePoints(candidate.max, center) < 0)
                        leftSegment.add(candidate);
                    else if (comparePoints(candidate.min, center) > 0)
                        rightSegment.add(candidate);
                    else
                        intersects.add(candidate);
                }

                intersectsLeft = minOrdering.sortedCopy(intersects);
                intersectsRight = maxOrdering.reverse().sortedCopy(intersects);
                left = leftSegment.isEmpty() ? null : new IntervalNode(leftSegment);
                right = rightSegment.isEmpty() ? null : new IntervalNode(rightSegment);

                assert (intersects.size() + leftSegment.size() + rightSegment.size()) == toBisect.size() :
                        "intersects (" + String.valueOf(intersects.size()) +
                        ") + leftSegment (" + String.valueOf(leftSegment.size()) +
                        ") + rightSegment (" + String.valueOf(rightSegment.size()) +
                        ") != toBisect (" + String.valueOf(toBisect.size()) + ")";
            }
        }

        void searchInternal(Interval<C, D> searchInterval, List<D> results)
        {
            if (comparePoints(searchInterval.max, low) < 0 || comparePoints(searchInterval.min, high) > 0)
                return;

            if (contains(searchInterval, center))
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
            else if (comparePoints(center, searchInterval.min) < 0)
            {
                // Adds intervals i in intersects right as long as i.max >= searchInterval.min
                // then search right
                for (Interval<C, D> interval : intersectsRight)
                {
                    if (comparePoints(interval.max, searchInterval.min) >= 0)
                        results.add(interval.data);
                    else
                        break;
                }
                if (right != null)
                    right.searchInternal(searchInterval, results);
            }
            else
            {
                assert comparePoints(center, searchInterval.max) > 0;
                // Adds intervals i in intersects left as long as i.min >= searchInterval.max
                // then search left
                for (Interval<C, D> interval : intersectsLeft)
                {
                    if (comparePoints(interval.min, searchInterval.max) <= 0)
                        results.add(interval.data);
                    else
                        break;
                }
                if (left != null)
                    left.searchInternal(searchInterval, results);
            }
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
            if (current != null && current.hasNext())
                return current.next();

            IntervalNode node = stack.pollFirst();
            if (node == null)
                return endOfData();

            current = node.intersectsLeft.iterator();

            // We know this is the smaller not returned yet, but before doing
            // its parent, we must do everyone on it's right.
            gotoMinOf(node.right);

            return computeNext();
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

    public static class Serializer<C, D, I extends Interval<C, D>> implements IVersionedSerializer<IntervalTree<C, D, I>>
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
        public IntervalTree<C, D, I> deserialize(DataInput in, int version) throws IOException
        {
            return deserialize(in, version, null);
        }

        public IntervalTree<C, D, I> deserialize(DataInput in, int version, Comparator<C> comparator) throws IOException
        {
            try
            {
                int count = in.readInt();
                List<Interval<C, D>> intervals = new ArrayList<Interval<C, D>>(count);
                for (int i = 0; i < count; i++)
                {
                    C min = pointSerializer.deserialize(in);
                    C max = pointSerializer.deserialize(in);
                    D data = dataSerializer.deserialize(in);
                    intervals.add(constructor.newInstance(min, max, data));
                }
                return new IntervalTree(intervals, comparator);
            }
            catch (InstantiationException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e)
            {
                throw new RuntimeException(e);
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        }

        public long serializedSize(IntervalTree<C, D, I> it, TypeSizes typeSizes, int version)
        {
            long size = typeSizes.sizeof(0);
            for (Interval<C, D> interval : it)
            {
                size += pointSerializer.serializedSize(interval.min, typeSizes);
                size += pointSerializer.serializedSize(interval.max, typeSizes);
                size += dataSerializer.serializedSize(interval.data, typeSizes);
            }
            return size;
        }

        public long serializedSize(IntervalTree<C, D, I> it, int version)
        {
            return serializedSize(it, TypeSizes.NATIVE, version);
        }
    }
}
