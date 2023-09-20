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
package org.apache.cassandra.index.sai.utils;

import java.io.Closeable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.FileUtils;

/**
 * Modified from {@link org.apache.cassandra.index.sasi.utils.RangeIterator} to support:
 * 1. no generic type to reduce allocation
 * 2. CONCAT iterator type
 */
public abstract class RangeIterator<T extends Comparable<T>> extends AbstractIterator<T> implements Closeable
{
    private static final Builder.EmptyRangeIterator<Long> EMPTY_LONGS = new Builder.EmptyRangeIterator<>();
    private static final Builder.EmptyRangeIterator<PrimaryKey> EMPTY_KEYS = new Builder.EmptyRangeIterator<>();

    private final T min, max;
    private final long count;
    private T current;

    protected RangeIterator(Builder.Statistics<T> statistics)
    {
        this(statistics.min, statistics.max, statistics.tokenCount);
    }

    public RangeIterator(RangeIterator<T> range)
    {
        this(range == null ? null : range.min, range == null ? null : range.max, range == null ? -1 : range.count);
    }

    public RangeIterator(T min, T max, long count)
    {
        if (min == null || max == null || count == 0)
            assert min == null && max == null && (count == 0 || count == -1) : min + " - " + max + " " + count;

        this.min = min;
        this.current = min;
        this.max = max;
        this.count = count;
    }

    public final T getMinimum()
    {
        return min;
    }

    public final T getCurrent()
    {
        return current;
    }

    public final T getMaximum()
    {
        return max;
    }

    public final long getCount()
    {
        return count;
    }

    /**
     * When called, this iterators current position should
     * be skipped forwards until finding either:
     *   1) an element equal to or bigger than next
     *   2) the end of the iterator
     *
     * @param nextToken value to skip the iterator forward until matching
     *
     * @return The next current key after the skip was performed.
     * This key will also be the next key returned by next(), i.e.,
     * we are "peeking" at the next key as part of the skip.
     */
    public final T skipTo(T nextToken)
    {
        if (min == null || max == null)
            return endOfData();

        // In the case of deferred iterators the current value may not accurately
        // reflect the next value so we need to check that as well
        if (current.compareTo(nextToken) >= 0)
        {
            next = next == null ? recomputeNext() : next;
            if (next == null)
                return endOfData();
            else if (next.compareTo(nextToken) >= 0)
                return next;
        }

        if (max.compareTo(nextToken) < 0)
            return endOfData();

        performSkipTo(nextToken);
        return recomputeNext();
    }

    /**
     * Skip up to nextKey, but leave your internal state in a position where
     * calling computeNext() will return nextKey or the first one after it.
     */
    protected abstract void performSkipTo(T nextToken);

    // protected because inherited from Guava. We don't want to expose this method.
    protected T recomputeNext()
    {
        return tryToComputeNext() ? peek() : endOfData();
    }

    protected boolean tryToComputeNext()
    {
        boolean hasNext = super.tryToComputeNext();
        current = hasNext ? next : getMaximum();
        return hasNext;
    }

    public static <T extends Comparable<T>> RangeIterator<T> empty()
    {
        return new Builder.EmptyRangeIterator<>();
    }

    public static RangeIterator<PrimaryKey> emptyKeys()
    {
        return EMPTY_KEYS;
    }
    public static RangeIterator<Long> emptyLongs()
    {
        return EMPTY_LONGS;
    }

    public static abstract class Builder<T extends Comparable<T>>
    {
        public enum IteratorType
        {
            CONCAT,
            UNION,
            INTERSECTION;
        }

        @VisibleForTesting
        protected final Statistics<T> statistics;

        @VisibleForTesting
        protected final PriorityQueue<RangeIterator<T>> ranges;

        public Builder(IteratorType type)
        {
            statistics = new Statistics<>(type);
            ranges = new PriorityQueue<>(16, Comparator.comparing(RangeIterator::getCurrent));
        }

        public T getMinimum()
        {
            return statistics.min;
        }

        public T getMaximum()
        {
            return statistics.max;
        }

        public long getTokenCount()
        {
            return statistics.tokenCount;
        }

        public int rangeCount()
        {
            return ranges.size();
        }

        public Collection<RangeIterator<T>> ranges()
        {
            return ranges;
        }

        public Builder<T> add(RangeIterator<T> range)
        {
            if (range == null)
                return this;

            if (range.getCount() > 0)
                ranges.add(range);
            else
                FileUtils.closeQuietly(range);
            statistics.update(range);

            return this;
        }

        public Builder<T> add(List<RangeIterator<T>> ranges)
        {
            if (ranges == null || ranges.isEmpty())
                return this;

            ranges.forEach(this::add);
            return this;
        }

        public final RangeIterator<T> build()
        {
            if (rangeCount() == 0)
                return new Builder.EmptyRangeIterator<>();
            else
                return buildIterator();
        }

        public static class EmptyRangeIterator<T extends Comparable<T>> extends RangeIterator<T>
        {
            EmptyRangeIterator() { super(null, null, 0); }
            public T computeNext() { return endOfData(); }
            protected void performSkipTo(T nextToken) { }
            public void close() { }
        }

        protected abstract RangeIterator<T> buildIterator();

        public static class Statistics<U extends Comparable<U>>
        {
            protected final IteratorType iteratorType;

            protected U min, max;
            protected long tokenCount;

            // iterator with the least number of items
            protected RangeIterator<U> minRange;
            // iterator with the most number of items
            protected RangeIterator<U> maxRange;

            // tracks if all of the added ranges overlap, which is useful in case of intersection,
            // as it gives direct answer as to such iterator is going to produce any results.
            private boolean isOverlapping = true;

            private boolean hasRange = false;

            public Statistics(IteratorType iteratorType)
            {
                this.iteratorType = iteratorType;
            }

            /**
             * Update statistics information with the given range.
             *
             * Updates min/max of the combined range, token count and
             * tracks range with the least/most number of tokens.
             *
             * @param range The range to update statistics with.
             */
            public void update(RangeIterator<U> range)
            {
                switch (iteratorType)
                {
                    case CONCAT:
                        // range iterators should be sorted, but previous max must not be greater than next min.
                        if (range.getCount() > 0)
                        {
                            if (tokenCount == 0)
                            {
                                min = range.getMinimum();
                            }
                            else if (tokenCount > 0 && max.compareTo(range.getMinimum()) > 0)
                            {
                                throw new IllegalArgumentException("RangeIterator must be sorted, previous max: " + max + ", next min: " + range.getMinimum());
                            }

                            max = range.getMaximum();
                        }
                        tokenCount += range.getCount();
                        break;

                    case UNION:
                        min = nullSafeMin(min, range.getMinimum());
                        max = nullSafeMax(max, range.getMaximum());
                        tokenCount += range.getCount();
                        break;

                    case INTERSECTION:
                        // minimum of the intersection is the biggest minimum of individual iterators
                        min = nullSafeMax(min, range.getMinimum());
                        // maximum of the intersection is the smallest maximum of individual iterators
                        max = nullSafeMin(max, range.getMaximum());
                        if (hasRange)
                            tokenCount = Math.min(tokenCount, range.getCount());
                        else
                            tokenCount = range.getCount();
                        break;

                    default:
                        throw new IllegalStateException("Unknown iterator type: " + iteratorType);
                }

                // check if new range is disjoint with already added ranges, which means that this intersection
                // is not going to produce any results, so we can cleanup range storage and never added anything to it.
                isOverlapping &= isOverlapping(min, max, range);

                minRange = minRange == null ? range : min(minRange, range);
                maxRange = maxRange == null ? range : max(maxRange, range);

                hasRange = true;
            }

            private RangeIterator<U> min(RangeIterator<U> a, RangeIterator<U> b)
            {
                return a.getCount() > b.getCount() ? b : a;
            }

            private RangeIterator<U> max(RangeIterator<U> a, RangeIterator<U> b)
            {
                return a.getCount() > b.getCount() ? a : b;
            }

            public boolean isDisjoint()
            {
                return !isOverlapping;
            }

            public double sizeRatio()
            {
                return minRange.getCount() * 1d / maxRange.getCount();
            }
        }
    }

    @VisibleForTesting
    protected static <U extends Comparable<U>> boolean isOverlapping(RangeIterator<U> a, RangeIterator<U> b)
    {
        return isOverlapping(a.getCurrent(), a.getMaximum(), b);
    }

    /**
     * Ranges are overlapping the following cases:
     *
     *   * When they have a common subrange:
     *
     *   min       b.current      max          b.max
     *   +---------|--------------+------------|
     *
     *   b.current      min       max          b.max
     *   |--------------+---------+------------|
     *
     *   min        b.current     b.max        max
     *   +----------|-------------|------------+
     *
     *
     *  If either range is empty, they're disjoint.
     */
    @VisibleForTesting
    protected static <T extends Comparable<T>> boolean isOverlapping(T min, T max, RangeIterator<T> b)
    {
        return (min != null && max != null) &&
               b.getCount() != 0 &&
               (min.compareTo(b.getMaximum()) <= 0 && b.getCurrent().compareTo(max) <= 0);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable> T nullSafeMin(T a, T b)
    {
        if (a == null) return b;
        if (b == null) return a;

        return a.compareTo(b) > 0 ? b : a;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable> T nullSafeMax(T a, T b)
    {
        if (a == null) return b;
        if (b == null) return a;

        return a.compareTo(b) > 0 ? a : b;
    }
}