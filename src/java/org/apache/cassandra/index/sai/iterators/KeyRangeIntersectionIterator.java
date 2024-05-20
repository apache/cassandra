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
import org.apache.cassandra.index.sai.utils.PrimaryKey.Kind;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

import javax.annotation.Nullable;

/**
 * A simple intersection iterator that makes no real attempts at optimising the iteration apart from
 * initially sorting the ranges. This implementation also supports an intersection limit via
 * {@code CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT} which limits the number of ranges that will 
 * be included in the intersection. This currently defaults to 2.
 * <p> 
 * Intersection only works for ranges that are compatible according to {@link PrimaryKey.Kind#isIntersectable(Kind)}.
 */
public class KeyRangeIntersectionIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(KeyRangeIntersectionIterator.class);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt()));
    }

    private final List<KeyRangeIterator> ranges;
    private PrimaryKey highestKey;

    private KeyRangeIntersectionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges, Runnable onClose)
    {
        super(statistics, onClose);
        this.ranges = ranges;
        this.highestKey = null;
    }

    @Override
    protected PrimaryKey computeNext()
    {
        if (highestKey == null)
            highestKey = computeHighestKey();

        outer:
        // After advancing one iterator, we must try to advance all the other iterators that got behind,
        // so they catch up to it. Note that we will not advance the iterators for static columns
        // as long as they point to the partition of the highest key. (This is because STATIC primary keys 
        // compare to other keys only by partition.) This loop continues until all iterators point to the same key,
        // or if we run out of keys on any of them, or if we exceed the maximum key.
        // There is no point in iterating after maximum, because no keys will match beyond that point.
        while (highestKey != null && highestKey.compareTo(getMaximum()) <= 0)
        {
            // Try to advance all iterators to the highest key seen so far.
            // Once this inner loop finishes normally, all iterators are guaranteed to be at the same value.
            for (KeyRangeIterator range : ranges)
            {
                if (!range.hasNext())
                    return endOfData();

                if (range.peek().compareTo(highestKey) < 0)
                {
                    // If we advance a STATIC key, then we must advance it to the same partition as the highestKey.
                    // Advancing a STATIC key to a WIDE key directly (without throwing away the clustering) would
                    // go too far, as WIDE keys are stored after STATIC in the posting list.
                    PrimaryKey nextKey = range.peek().kind() == Kind.STATIC
                                         ? skipAndPeek(range, highestKey.toStatic())
                                         : skipAndPeek(range, highestKey);

                    // We use strict comparison here, since it orders WIDE primary keys after STATIC primary keys
                    // in the same partition. When WIDE keys are present, we want to return them rather than STATIC
                    // keys to avoid retrieving and post-filtering entire partitions.
                    if (nextKey == null || nextKey.compareToStrict(highestKey) > 0)
                    {
                        // We jumped over the highest key seen so far, so make it the new highest key.
                        highestKey = nextKey;

                        // This iterator jumped over, so the other iterators might be lagging behind now,
                        // including the ones already advanced in the earlier cycles of the inner loop.
                        // Therefore, restart the inner loop in order to advance the lagging iterators.
                        continue outer;
                    }
                    assert nextKey.compareTo(highestKey) == 0 :
                        String.format("Skipped to a key smaller than the target! " +
                                      "iterator: %s, target key: %s, returned key: %s", range, highestKey, nextKey);
                }
            }

            // If we get here, all iterators have been advanced to the same key. When STATIC and WIDE keys are
            // mixed, this means WIDE keys point to exactly the same row, and STATIC keys the same partition.
            PrimaryKey result = highestKey;

            // Advance one iterator to the next key and remember the key as the highest seen so far.
            // It can become null when we reach the end of the iterator.
            // If there are both static and non-static keys being iterated here, we advance a non-static one,
            // regardless of the order of ranges in the ranges list.
            highestKey = advanceOneRange();

            // If we get here, all iterators have been advanced to the same key. When STATIC and WIDE keys are
            // mixed, this means WIDE keys point to exactly the same row, and STATIC keys the same partition.
            return result;
        }

        return endOfData();
    }

    /**
     * Advances the iterator of one range to the next item, which becomes the highest seen so far.
     * Iterators pointing to STATIC keys are advanced only if no non-STATIC keys have been advanced.
     *
     * @return the next highest key or null if the iterator has reached the end
     */
    private @Nullable PrimaryKey advanceOneRange()
    {
        for (KeyRangeIterator range : ranges)
            if (range.peek().kind() != Kind.STATIC)
            {
                range.next();
                return range.hasNext() ? range.peek() : null;
            }
        
        for (KeyRangeIterator range : ranges)
            if (range.peek().kind() == Kind.STATIC)
            {
                range.next();
                return range.hasNext() ? range.peek() : null;
            }

        throw new IllegalStateException("There should be at least one range to advance!");
    }

    private @Nullable PrimaryKey computeHighestKey()
    {
        PrimaryKey max = getMinimum();
        for (KeyRangeIterator range : ranges)
        {
            if (!range.hasNext())
                return null;
            if (range.peek().compareToStrict(max) > 0)
                max = range.peek();
        }
        return max;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        // Resist the temptation to call range.hasNext before skipTo: this is a pessimisation, hasNext will invoke
        // computeNext under the hood, which is an expensive operation to produce a value that we plan to throw away.
        // Instead, it is the responsibility of the child iterators to make skipTo fast when the iterator is exhausted.
        for (KeyRangeIterator range : ranges)
            range.skipTo(nextKey);

        // Force recomputing the highest key on the next call to computeNext()
        highestKey = null;
    }

    @Override
    public void close()
    {
        super.close();
        FileUtils.closeQuietly(ranges);
    }

    /**
     * Fetches the next available item from the iterator, such that the item is not lower than the given key.
     * If no such items are available, returns null.
     */
    private PrimaryKey skipAndPeek(KeyRangeIterator iterator, PrimaryKey minKey)
    {
        iterator.skipTo(minKey);
        return iterator.hasNext() ? iterator.peek() : null;
    }

    public static Builder builder(int size, int limit)
    {
        return builder(size, limit, () -> {});
    }

    public static Builder builder(int size, Runnable onClose)
    {
        return new Builder(size, onClose);
    }

    @VisibleForTesting
    public static Builder builder(int size, int limit, Runnable onClose)
    {
        return new Builder(size, limit, onClose);
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

        Builder(int size, Runnable onClose)
        {
            this(size, CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt(), onClose);
        }

        Builder(int size, int limit, Runnable onClose)
        {
            super(new IntersectionStatistics(), onClose);
            rangeIterators = new ArrayList<>(size);
            this.limit = limit;
        }

        @Override
        public KeyRangeIterator.Builder add(KeyRangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getMaxKeys() > 0)
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
            super.cleanup();
            FileUtils.closeQuietly(rangeIterators);
        }

        @Override
        protected KeyRangeIterator buildIterator()
        {
            rangeIterators.sort(Comparator.comparingLong(KeyRangeIterator::getMaxKeys));
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
                              rangeIterators.stream().map(KeyRangeIterator::getMaxKeys).map(Object::toString).collect(Collectors.joining(", ")),
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
                onClose.run();
                return KeyRangeIterator.empty();
            }

            if (ranges.size() == 1)
            {
                KeyRangeIterator single = ranges.get(0);
                single.setOnClose(onClose);
                return single;
            }

            // Make sure intersection is supported on the ranges provided:
            PrimaryKey.Kind firstKind = null;
            
            for (KeyRangeIterator range : ranges)
            {
                PrimaryKey key;
                if(range.hasNext())
                    key = range.peek();
                else
                    key = range.getMaximum();

                if (key != null)
                    if (firstKind == null)
                        firstKind = key.kind();
                    else if (!firstKind.isIntersectable(key.kind()))
                        throw new IllegalArgumentException("Cannot intersect " + firstKind + " and " + key.kind() + " ranges!");
            }

            return new KeyRangeIntersectionIterator(statistics, ranges, onClose);
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
                count = range.getMaxKeys();
            }
            else
            {
                count = Math.min(count, range.getMaxKeys());
            }
        }
    }

    @VisibleForTesting
    protected static boolean isDisjoint(KeyRangeIterator a, KeyRangeIterator b)
    {
        return isDisjointInternal(a.peek(), a.getMaximum(), b);
    }

    /**
     * Ranges are overlapping the following cases:
     * <p>
     *   * When they have a common subrange:
     * <p>
     *   min       b.current      max          b.max
     *   +---------|--------------+------------|
     * <p>
     *   b.current      min       max          b.max
     *   |--------------+---------+------------|
     * <p>
     *   min        b.current     b.max        max
     *   +----------|-------------|------------+
     * <p>
     *
     *  If either range is empty, they're disjoint.
     */
    private static boolean isDisjointInternal(PrimaryKey min, PrimaryKey max, KeyRangeIterator b)
    {
        return min == null || max == null || b.getMaxKeys() == 0 || min.compareTo(b.getMaximum()) > 0 || (b.hasNext() && b.peek().compareTo(max) > 0);
    }
}
