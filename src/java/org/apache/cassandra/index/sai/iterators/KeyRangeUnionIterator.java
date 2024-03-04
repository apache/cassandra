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
 * Range Union Iterator is used to return sorted stream of elements from multiple RangeIterator instances.
 */
public class KeyRangeUnionIterator extends KeyRangeIterator
{
    private final List<KeyRangeIterator> ranges;
    private final List<KeyRangeIterator> candidates;

    private KeyRangeUnionIterator(Builder.Statistics statistics, List<KeyRangeIterator> ranges, Runnable onClose)
    {
        super(statistics, onClose);
        this.ranges = ranges;
        this.candidates = new ArrayList<>(ranges.size());
    }

    @Override
    public PrimaryKey computeNext()
    {
        // the design is to find the next best value from all the ranges,
        // and then advance all the ranges that have the same value.
        candidates.clear();
        PrimaryKey candidateKey = null;
        for (KeyRangeIterator range : ranges)
        {
            if (!range.hasNext())
                continue;

            if (candidateKey == null)
            {
                candidateKey = range.peek();
                candidates.add(range);
            }
            else
            {
                PrimaryKey peeked = range.peek();
    
                int cmp = candidateKey.compareTo(peeked);

                if (cmp == 0)
                {
                    // Replace any existing candidate key if this one is STATIC:
                    if (peeked.kind() == PrimaryKey.Kind.STATIC)
                        candidateKey = peeked;

                    candidates.add(range);
                }
                else if (cmp > 0)
                {
                    // we found a new best candidate, throw away the old ones
                    candidates.clear();
                    candidateKey = peeked;
                    candidates.add(range);
                }
                // else, existing candidate is less than the next in this range
            }
        }
        if (candidates.isEmpty())
            return endOfData();

        for (KeyRangeIterator candidate : candidates)
        {
            do
            {
                // Consume the remaining values equal to the candidate key:
                candidate.next();
            }
            while (candidate.hasNext() && candidate.peek().compareTo(candidateKey) == 0);
        }

        return candidateKey;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        for (KeyRangeIterator range : ranges)
        {
            if (range.hasNext())
                range.skipTo(nextKey);
        }
    }

    @Override
    public void close()
    {
        super.close();

        // Due to lazy key fetching, we cannot close iterator immediately
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

    public static KeyRangeIterator build(List<KeyRangeIterator> keys, Runnable onClose)
    {
        return new Builder(keys.size(), onClose).add(keys).build();
    }
    public static KeyRangeIterator build(List<KeyRangeIterator> keys)
    {
        return build(keys, () -> {});
    }

    @VisibleForTesting
    public static class Builder extends KeyRangeIterator.Builder
    {
        protected final List<KeyRangeIterator> rangeIterators;

        Builder(int size, Runnable onClose)
        {
            super(new UnionStatistics(), onClose);
            this.rangeIterators = new ArrayList<>(size);
        }

        @Override
        public KeyRangeIterator.Builder add(KeyRangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getMaxKeys() > 0)
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
            super.cleanup();
            FileUtils.closeQuietly(rangeIterators);
        }

        @Override
        protected KeyRangeIterator buildIterator()
        {
            if (rangeCount() == 1)
            {
                KeyRangeIterator single = rangeIterators.get(0);
                single.setOnClose(onClose);
                return single;
            }

            return new KeyRangeUnionIterator(statistics, rangeIterators, onClose);
        }
    }

    private static class UnionStatistics extends KeyRangeIterator.Builder.Statistics
    {
        @Override
        public void update(KeyRangeIterator range)
        {
            min = nullSafeMin(min, range.getMinimum());
            max = nullSafeMax(max, range.getMaximum());
            count += range.getMaxKeys();
        }
    }
}
