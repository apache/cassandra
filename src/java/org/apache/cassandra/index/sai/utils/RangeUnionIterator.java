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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.util.FileUtils;

/**
 * Range Union Iterator is used to return sorted stream of elements from multiple RangeIterator instances.
 */
@SuppressWarnings("resource")
public class RangeUnionIterator extends RangeIterator
{
    private final List<RangeIterator> ranges;

    private final List<RangeIterator> toRelease;
    private final List<RangeIterator> candidates = new ArrayList<>();

    private RangeUnionIterator(Builder.Statistics statistics, List<RangeIterator> ranges)
    {
        super(statistics);
        this.ranges = ranges;
        this.toRelease = new ArrayList<>(ranges);
    }

    public PrimaryKey computeNext()
    {
        candidates.clear();
        PrimaryKey candidate = null;
        for (RangeIterator range : ranges)
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
        candidates.forEach(RangeIterator::next);
        return candidate;
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        for (RangeIterator range : ranges)
        {
            if (range.hasNext())
                range.skipTo(nextKey);
        }
    }

    public void close() throws IOException
    {
        // Due to lazy key fetching, we cannot close iterator immediately
        toRelease.forEach(FileUtils::closeQuietly);
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static RangeIterator build(List<RangeIterator> tokens)
    {
        return new Builder(tokens.size()).add(tokens).build();
    }

    public static class Builder extends RangeIterator.Builder
    {
        protected List<RangeIterator> rangeIterators;

        public Builder()
        {
            super(IteratorType.UNION);
            this.rangeIterators = new ArrayList<>();
        }

        public Builder(int size)
        {
            super(IteratorType.UNION);
            this.rangeIterators = new ArrayList<>(size);
        }

        public RangeIterator.Builder add(RangeIterator range)
        {
            if (range == null)
                return this;

            if (range.getCount() > 0)
            {
                rangeIterators.add(range);
                statistics.update(range);
            }
            else
                FileUtils.closeQuietly(range);

            return this;
        }

        public RangeIterator.Builder add(List<RangeIterator> ranges)
        {
            if (ranges == null || ranges.isEmpty())
                return this;

            ranges.forEach(this::add);
            return this;
        }

        public int rangeCount()
        {
            return rangeIterators.size();
        }

        protected RangeIterator buildIterator()
        {
            switch (rangeCount())
            {
                case 1:
                    return rangeIterators.get(0);

                default:
                    //TODO Need to test whether an initial sort improves things
                    return new RangeUnionIterator(statistics, rangeIterators);
            }
        }
    }
}
