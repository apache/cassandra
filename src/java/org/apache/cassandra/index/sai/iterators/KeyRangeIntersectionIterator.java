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
package org.apache.cassandra.index.sai.iterators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * A simple intersection iterator that makes no real attempts at optimising the iteration apart from
 * initially sorting the ranges. This implementation also supports an intersection limit which limits
 * the number of ranges that will be included in the intersection. This currently defaults to 2.
 */
public class KeyRangeIntersectionIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(KeyRangeIntersectionIterator.class);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt()));
    }

    private final List<KeyRangeIterator> ranges;

    private KeyRangeIntersectionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
    }

    @Override
    protected PrimaryKey computeNext()
    {
        // Range iterator that has been advanced in the previous cycle of the outer loop.
        // Initially there hasn't been the previous cycle, so set to null.
        int alreadyAvanced = -1;

        // The highest primary key seen on any range iterator so far.
        // It can become null when we reach the end of the iterator.
        PrimaryKey highestKey = getCurrent();

        outer:
        // We need to check if highestKey exceeds the maximum because the maximum is
        // the lowest value maximum of all the ranges. As a result any value could
        // potentially exceed it.
        while (highestKey != null && highestKey.compareTo(getMaximum()) <= 0)
        {
            // Try advance all iterators to the highest key seen so far.
            // Once this inner loop finishes normally, all iterators are guaranteed to be at the same value.
            for (int index = 0; index < ranges.size(); index++)
            {
                if (index != alreadyAvanced)
                {
                    KeyRangeIterator range = ranges.get(index);
                    PrimaryKey nextKey = nextOrNull(range, highestKey);
                    if (nextKey == null || nextKey.compareTo(highestKey) > 0)
                    {
                        // We jumped over the highest key seen so far, so make it the new highest key.
                        highestKey = nextKey;
                        // Remember this iterator to avoid advancing it again, because it is already at the highest key
                        alreadyAvanced = index;
                        // This iterator jumped over, so the other iterators are lagging behind now,
                        // including the ones already advanced in the earlier cycles of the inner loop.
                        // Therefore, restart the inner loop in order to advance
                        // the other iterators except this one to match the new highest key.
                        continue outer;
                    }
                    assert nextKey.compareTo(highestKey) == 0:
                    String.format("skipped to an item smaller than the target; " +
                                  "iterator: %s, target key: %s, returned key: %s", range, highestKey, nextKey);
                }
            }
            // If we reached here, next() has been called at least once on each range iterator and
            // the last call to next() on each iterator returned a value equal to the highestKey.
            return highestKey;
        }
        return endOfData();
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        for (KeyRangeIterator range : ranges)
            if (range.hasNext())
                range.skipTo(nextKey);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(ranges);
    }

    /**
     * Fetches the next available item from the iterator, such that the item is not lower than the given key.
     * If no such items are available, returns null.
     */
    private PrimaryKey nextOrNull(KeyRangeIterator iterator, PrimaryKey minKey)
    {
        iterator.skipTo(minKey);
        return iterator.hasNext() ? iterator.next() : null;
    }

    public static Builder builder(int size)
    {
        return new Builder(size);
    }

    @VisibleForTesting
    public static Builder builder(int size, int limit)
    {
        return new Builder(size, limit);
    }

    @VisibleForTesting
    public static class Builder extends KeyRangeIterator.Builder
    {
        // This controls the maximum number of range iterators that will be used in the final
        // intersection of a query operation. It is set from cassandra.sai.intersection_clause_limit
        // and defaults to 2
        private final int limit;
        // tracks if any of the added ranges are disjoint with the other ranges, which is useful
        // in case of intersection, as it gives a direct answer whether the iterator is going
        // to produce any results.
        private boolean isDisjoint;

        protected final List<KeyRangeIterator> rangeIterators;

        Builder(int size)
        {
            this(size, CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt());
        }

        Builder(int size, int limit)
        {
            super(new IntersectionStatistics());
            rangeIterators = new ArrayList<>(size);
            this.limit = limit;
        }

        @Override
        public KeyRangeIterator.Builder add(KeyRangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getCount() > 0)
                rangeIterators.add(range);
            else
                FileUtils.closeQuietly(range);

            updateStatistics(statistics, range);

            return this;
        }

        @Override
        public int rangeCount()
        {
            return rangeIterators.size();
        }

        @Override
        public void cleanup()
        {
            FileUtils.closeQuietly(rangeIterators);
        }

        @Override
        protected KeyRangeIterator buildIterator()
        {
            rangeIterators.sort(Comparator.comparingLong(KeyRangeIterator::getCount));
            int initialSize = rangeIterators.size();
            // all ranges will be included
            if (limit >= rangeIterators.size() || limit <= 0)
                return buildIterator(statistics, rangeIterators);

            // Apply most selective iterators during intersection, because larger number of iterators will result lots of disk seek.
            Statistics selectiveStatistics = new IntersectionStatistics();
            isDisjoint = false;
            for (int i = rangeIterators.size() - 1; i >= 0 && i >= limit; i--)
                FileUtils.closeQuietly(rangeIterators.remove(i));

            rangeIterators.forEach(range -> updateStatistics(selectiveStatistics, range));

            if (Tracing.isTracing())
                Tracing.trace("Selecting {} {} of {} out of {} indexes",
                              rangeIterators.size(),
                              rangeIterators.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                              rangeIterators.stream().map(KeyRangeIterator::getCount).map(Object::toString).collect(Collectors.joining(", ")),
                              initialSize);

            return buildIterator(selectiveStatistics, rangeIterators);
        }

        public boolean isDisjoint()
        {
            return isDisjoint;
        }

        private KeyRangeIterator buildIterator(Statistics statistics, List<KeyRangeIterator> ranges)
        {
            // if the ranges are disjoint, or we have an intersection with an empty set,
            // we can simply return an empty iterator, because it's not going to produce any results.
            if (isDisjoint)
            {
                FileUtils.closeQuietly(ranges);
                return KeyRangeIterator.empty();
            }

            if (ranges.size() == 1)
                return ranges.get(0);

            return new KeyRangeIntersectionIterator(statistics, ranges);
        }

        private void updateStatistics(Statistics statistics, KeyRangeIterator range)
        {
            statistics.update(range);
            isDisjoint |= isDisjointInternal(statistics.min, statistics.max, range);
        }
    }

    private static class IntersectionStatistics extends KeyRangeIterator.Builder.Statistics
    {
        private boolean empty = true;

        @Override
        public void update(KeyRangeIterator range)
        {
            // minimum of the intersection is the biggest minimum of individual iterators
            min = nullSafeMax(min, range.getMinimum());
            // maximum of the intersection is the smallest maximum of individual iterators
            max = nullSafeMin(max, range.getMaximum());
            if (empty)
            {
                empty = false;
                count = range.getCount();
            }
            else
            {
                count = Math.min(count, range.getCount());
            }
        }
    }

    @VisibleForTesting
    protected static boolean isDisjoint(KeyRangeIterator a, KeyRangeIterator b)
    {
        return isDisjointInternal(a.getCurrent(), a.getMaximum(), b);
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
    private static boolean isDisjointInternal(PrimaryKey min, PrimaryKey max, KeyRangeIterator b)
    {
        return min == null || max == null || b.getCount() == 0 || min.compareTo(b.getMaximum()) > 0 || b.getCurrent().compareTo(max) > 0;
    }
}
