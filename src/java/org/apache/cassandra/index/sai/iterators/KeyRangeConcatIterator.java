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
import java.util.PriorityQueue;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.FileUtils;

/**
 * {@link KeyRangeConcatIterator} takes a list of sorted range iterator and concatenates them, leaving duplicates in
 * place, to produce a new stably sorted iterator. Duplicates are eliminated later in
 * {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher}
 * as results from multiple SSTable indexes and their respective segments are consumed.
 * <p>
 * ex. (1, 2, 3) + (3, 3, 4, 5) -> (1, 2, 3, 3, 3, 4, 5)
 * ex. (1, 2, 2, 3) + (3, 4, 4, 6, 6, 7) -> (1, 2, 2, 3, 3, 4, 4, 6, 6, 7)
 * <p>
 * TODO Investigate removing the use of PriorityQueue from this class <a href="https://issues.apache.org/jira/browse/CASSANDRA-18165">CASSANDRA-18165</a>
 */
public class KeyRangeConcatIterator<T extends Comparable<T>> extends KeyRangeIterator<T>
{
    public static final String MUST_BE_SORTED_ERROR = "RangeIterator must be sorted, previous max: %s, next min: %s";
    private final PriorityQueue<KeyRangeIterator<T>> ranges;
    private final List<KeyRangeIterator<T>> toRelease;

    protected KeyRangeConcatIterator(KeyRangeIterator.Builder.Statistics<T> statistics, PriorityQueue<KeyRangeIterator<T>> ranges)
    {
        super(statistics);

        this.ranges = ranges;
        this.toRelease = new ArrayList<>(ranges);
    }

    @Override
    @SuppressWarnings({"resource", "RedundantSuppression"})
    protected void performSkipTo(T nextKey)
    {
        while (!ranges.isEmpty())
        {
            if (ranges.peek().getCurrent().compareTo(nextKey) >= 0)
                break;

            KeyRangeIterator<T> head = ranges.poll();

            if (head.getMaximum().compareTo(nextKey) >= 0)
            {
                head.skipTo(nextKey);
                ranges.add(head);
                break;
            }
        }
    }

    @Override
    public void close()
    {
        // due to lazy key fetching, we cannot close iterator immediately
        FileUtils.closeQuietly(toRelease);
    }

    @Override
    @SuppressWarnings({"resource", "RedundantSuppression"})
    protected T computeNext()
    {
        while (!ranges.isEmpty())
        {
            KeyRangeIterator<T> current = ranges.poll();
            if (current.hasNext())
            {
                T next = current.next();
                // hasNext will update RangeIterator's current which is used to sort in PQ
                if (current.hasNext())
                    ranges.add(current);

                return next;
            }
        }

        return endOfData();
    }

    public static <T extends Comparable<T>> KeyRangeIterator<T> build(List<KeyRangeIterator<T>> iterators)
    {
        return new Builder<T>(iterators.size()).add(iterators).build();
    }

    public static <T extends Comparable<T>> Builder<T> builder(int size)
    {
        return new Builder<>(size);
    }

    @VisibleForTesting
    public static class Builder<T extends Comparable<T>> extends KeyRangeIterator.Builder<T>
    {
        private final PriorityQueue<KeyRangeIterator<T>> ranges;

        Builder(int size)
        {
            super(new ConcatStatistics<>());
            ranges = new PriorityQueue<>(size, Comparator.comparing(KeyRangeIterator::getCurrent));
        }

        @Override
        public KeyRangeIterator.Builder<T> add(KeyRangeIterator<T> range)
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

        @Override
        public int rangeCount()
        {
            return ranges.size();
        }

        @Override
        public void cleanup()
        {
            FileUtils.closeQuietly(ranges);
        }

        @Override
        protected KeyRangeIterator<T> buildIterator()
        {
            if (rangeCount() == 1)
                return ranges.poll();

            return new KeyRangeConcatIterator<>(statistics, ranges);
        }
    }

    private static class ConcatStatistics<U extends Comparable<U>> extends KeyRangeIterator.Builder.Statistics<U>
    {
        @Override
        public void update(KeyRangeIterator<U> range)
        {
            // range iterators should be sorted, but previous max must not be greater than next min.
            if (range.getCount() > 0)
            {
                if (count == 0)
                {
                    min = range.getMinimum();
                }
                else if (count > 0 && max.compareTo(range.getMinimum()) > 0)
                {
                    throw new IllegalArgumentException(String.format(MUST_BE_SORTED_ERROR, max, range.getMinimum()));
                }

                max = range.getMaximum();
                count += range.getCount();
            }
        }
    }
}
