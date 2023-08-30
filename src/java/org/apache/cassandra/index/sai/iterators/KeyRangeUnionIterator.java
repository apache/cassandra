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
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.FileUtils;

/**
 * Range Union Iterator is used to return sorted stream of elements from multiple RangeIterator instances.
 */
@SuppressWarnings({"resource", "RedundantSuppression"})
public class KeyRangeUnionIterator<T extends Comparable<T>> extends KeyRangeIterator<T>
{
    private final List<KeyRangeIterator<T>> ranges;
    private final List<KeyRangeIterator<T>> candidates;

    private KeyRangeUnionIterator(Builder.Statistics<T> statistics, List<KeyRangeIterator<T>> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.candidates = new ArrayList<>(ranges.size());
    }

    @Override
    public T computeNext()
    {
        candidates.clear();
        T candidate = null;
        for (KeyRangeIterator<T> range : ranges)
        {
            if (range.hasNext())
            {
                // Avoid repeated values but only if we have read at least one value
                while (next != null && range.hasNext() && range.peek().compareTo(getCurrent()) == 0)
                    range.next();
                if (!range.hasNext())
                    continue;
                if (candidate == null)
                {
                    candidate = range.peek();
                    candidates.add(range);
                }
                else
                {
                    int cmp = candidate.compareTo(range.peek());
                    if (cmp == 0)
                        candidates.add(range);
                    else if (cmp > 0)
                    {
                        candidates.clear();
                        candidate = range.peek();
                        candidates.add(range);
                    }
                }
            }
        }
        if (candidates.isEmpty())
            return endOfData();
        candidates.forEach(KeyRangeIterator::next);
        return candidate;
    }

    @Override
    protected void performSkipTo(T nextKey)
    {
        for (KeyRangeIterator<T> range : ranges)
        {
            if (range.hasNext())
                range.skipTo(nextKey);
        }
    }

    @Override
    public void close()
    {
        // Due to lazy key fetching, we cannot close iterator immediately
        FileUtils.closeQuietly(ranges);
    }

    public static <T extends Comparable<T>> Builder<T> builder(int size)
    {
        return new Builder<>(size);
    }

    public static <T extends Comparable<T>> KeyRangeIterator<T> build(List<KeyRangeIterator<T>> keys)
    {
        return new Builder<T>(keys.size()).add(keys).build();
    }

    @VisibleForTesting
    public static class Builder<T extends Comparable<T>> extends KeyRangeIterator.Builder<T>
    {
        protected final List<KeyRangeIterator<T>> rangeIterators;

        Builder(int size)
        {
            super(new UnionStatistics<>());
            this.rangeIterators = new ArrayList<>(size);
        }

        @Override
        public KeyRangeIterator.Builder<T> add(KeyRangeIterator<T> range)
        {
            if (range == null)
                return this;

            if (range.getCount() > 0)
            {
                rangeIterators.add(range);
                statistics.update(range);
            }
            else
            {
                FileUtils.closeQuietly(range);
            }

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
        protected KeyRangeIterator<T> buildIterator()
        {
            if (rangeCount() == 1)
                return rangeIterators.get(0);

            return new KeyRangeUnionIterator<>(statistics, rangeIterators);
        }
    }

    private static class UnionStatistics<U extends Comparable<U>> extends KeyRangeIterator.Builder.Statistics<U>
    {
        @Override
        public void update(KeyRangeIterator<U> range)
        {
            min = nullSafeMin(min, range.getMinimum());
            max = nullSafeMax(max, range.getMaximum());
            count += range.getCount();
        }
    }
}
