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

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * {@link KeyRangeConcatIterator} takes a list of sorted range iterators and concatenates them, leaving duplicates in
 * place, to produce a new stably sorted iterator. Duplicates are eliminated later in
 * {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher}
 * as results from multiple SSTable indexes and their respective segments are consumed.
 * <p>
 * ex. (1, 2, 3) + (3, 3, 4, 5) -> (1, 2, 3, 3, 3, 4, 5)
 * ex. (1, 2, 2, 3) + (3, 4, 4, 6, 6, 7) -> (1, 2, 2, 3, 3, 4, 4, 6, 6, 7)
 */
public class KeyRangeConcatIterator extends KeyRangeIterator
{
    public static final String MUST_BE_SORTED_ERROR = "RangeIterator must be sorted, previous max: %s, next min: %s";
    private final List<KeyRangeIterator> ranges;

    private int current;

    protected KeyRangeConcatIterator(KeyRangeIterator.Builder.Statistics statistics, List<KeyRangeIterator> ranges, Runnable onClose)
    {
        super(statistics, onClose);

        if (ranges.isEmpty())
            throw new IllegalArgumentException("Cannot concatenate empty list of ranges");

        this.current = 0;
        this.ranges = ranges;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        while (current < ranges.size())
        {
            KeyRangeIterator currentIterator = ranges.get(current);

            if (currentIterator.hasNext() && currentIterator.peek().compareTo(nextKey) >= 0)
                break;

            if (currentIterator.getMaximum().compareTo(nextKey) >= 0)
            {
                currentIterator.skipTo(nextKey);
                break;
            }

            current++;
        }
    }

    @Override
    protected PrimaryKey computeNext()
    {
        while (current < ranges.size())
        {
            KeyRangeIterator currentIterator = ranges.get(current);

            if (currentIterator.hasNext())
                return currentIterator.next();

            current++;
        }

        return endOfData();
    }

    @Override
    public void close()
    {
        super.close();

        // due to lazy key fetching, we cannot close iterator immediately
        FileUtils.closeQuietly(ranges);
    }

    public static Builder builder(int size)
    {
        return builder(size, () -> {});
    }

    public static Builder builder(int size, Runnable onClose)
    {
        return new Builder(size, onClose);
    }

    @VisibleForTesting
    public static class Builder extends KeyRangeIterator.Builder
    {
        // We can use a list because the iterators are already in order
        private final List<KeyRangeIterator> ranges;

        Builder(int size, Runnable onClose)
        {
            super(new ConcatStatistics(), onClose);
            this.ranges = new ArrayList<>(size);
        }

        @Override
        public KeyRangeIterator.Builder add(KeyRangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getMaxKeys() > 0)
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
            super.cleanup();
            FileUtils.closeQuietly(ranges);
        }

        @Override
        protected KeyRangeIterator buildIterator()
        {
            if (rangeCount() == 0)
            {
                onClose.run();
                return empty();
            }
            if (rangeCount() == 1)
            {
                KeyRangeIterator single = ranges.get(0);
                single.setOnClose(onClose);
                return single;
            }

            return new KeyRangeConcatIterator(statistics, ranges, onClose);
        }
    }

    private static class ConcatStatistics extends KeyRangeIterator.Builder.Statistics
    {
        @Override
        public void update(KeyRangeIterator range)
        {
            // range iterators should be sorted, but previous max must not be greater than next min.
            if (range.getMaxKeys() > 0)
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
                count += range.getMaxKeys();
            }
        }
    }
}
