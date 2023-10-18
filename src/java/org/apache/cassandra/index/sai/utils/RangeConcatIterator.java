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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.io.util.FileUtils;

/**
 * {@link RangeConcatIterator} takes a list of sorted range iterator and concatenates them, leaving duplicates in
 * place, to produce a new stably sorted iterator. Duplicates are eliminated later in
 * {@link org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher}
 * as results from multiple SSTable indexes and their respective segments are consumed.
 *
 * ex. (1, 2, 3) + (3, 3, 4, 5) -> (1, 2, 3, 3, 3, 4, 5)
 * ex. (1, 2, 2, 3) + (3, 4, 4, 6, 6, 7) -> (1, 2, 2, 3, 3, 4, 4, 6, 6, 7)
 *
 */
public class RangeConcatIterator extends RangeIterator
{
    private final Iterator<RangeIterator> ranges;
    private RangeIterator currentRange;
    private final List<RangeIterator> toRelease;

    protected RangeConcatIterator(RangeIterator.Builder.Statistics statistics, List<RangeIterator> ranges)
    {
        super(statistics);

        this.ranges = ranges.iterator();
        this.toRelease = ranges;
    }

    @Override
    @SuppressWarnings("resource")
    protected void performSkipTo(PrimaryKey primaryKey)
    {
        if (currentRange != null && currentRange.getMaximum().compareTo(primaryKey) >= 0)
        {
            currentRange.skipTo(primaryKey);
            return;
        }
        while (ranges.hasNext())
        {
            currentRange = ranges.next();
            if (currentRange.getMaximum().compareTo(primaryKey) >= 0)
            {
                currentRange.skipTo(primaryKey);
                return;
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        // due to lazy key fetching, we cannot close iterator immediately
        toRelease.forEach(FileUtils::closeQuietly);
    }

    @Override
    @SuppressWarnings("resource")
    protected PrimaryKey computeNext()
    {
        if (currentRange == null || !currentRange.hasNext())
        {
            do
            {
                if (!ranges.hasNext())
                    return endOfData();
                currentRange = ranges.next();
            } while (!currentRange.hasNext());
        }
        return currentRange.next();
    }

    public static Builder builder()
    {
        return builder(1);
    }

    public static Builder builder(int size)
    {
        return new Builder(size);
    }

    public static RangeIterator build(List<RangeIterator> tokens)
    {
        return new Builder(tokens.size()).add(tokens).build();
    }

    public static class Builder extends RangeIterator.Builder
    {
        // We can use a list because the iterators are already in order
        private final List<RangeIterator> rangeIterators;
        public Builder(int size)
        {
            super(IteratorType.CONCAT);
            this.rangeIterators = new ArrayList<>(size);
        }

        @Override
        public int rangeCount()
        {
            return rangeIterators.size();
        }

        @Override
        public Builder add(RangeIterator range)
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

        @Override
        public RangeIterator.Builder add(List<RangeIterator> ranges)
        {
            if (ranges == null || ranges.isEmpty())
                return this;

            ranges.forEach(this::add);
            return this;
        }

        protected RangeIterator buildIterator()
        {
            if (rangeCount() == 1)
                return rangeIterators.get(0);
            return new RangeConcatIterator(statistics, rangeIterators);
        }
    }
}
