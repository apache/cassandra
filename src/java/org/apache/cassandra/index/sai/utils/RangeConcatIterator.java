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
import java.util.PriorityQueue;

import org.apache.cassandra.index.sai.Token;
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
    private final PriorityQueue<RangeIterator> ranges;
    private final List<RangeIterator> toRelease;

    protected RangeConcatIterator(RangeIterator.Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
    {
        super(statistics);

        this.ranges = ranges;
        this.toRelease = new ArrayList<>(ranges);
    }

    @Override
    @SuppressWarnings("resource")
    protected void performSkipTo(Long nextToken)
    {
        while (!ranges.isEmpty())
        {
            if (ranges.peek().getCurrent().compareTo(nextToken) >= 0)
                break;

            RangeIterator head = ranges.poll();

            if (head.getMaximum().compareTo(nextToken) >= 0)
            {
                head.skipTo(nextToken);
                ranges.add(head);
                break;
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
    protected Token computeNext()
    {
        while (!ranges.isEmpty())
        {
            RangeIterator current = ranges.poll();
            if (current.hasNext())
            {
                Token next = current.next();
                // hasNext will update RangeIterator's current which is used to sort in PQ
                if (current.hasNext())
                    ranges.add(current);

                return next;
            }
        }

        return endOfData();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static RangeIterator build(List<RangeIterator> tokens)
    {
        return new Builder().add(tokens).build();
    }

    public static class Builder extends RangeIterator.Builder
    {
        public Builder()
        {
            super(IteratorType.CONCAT);
        }

        protected RangeIterator buildIterator()
        {
            switch (rangeCount())
            {
                case 1:
                    return ranges.poll();

                default:
                    return new RangeConcatIterator(statistics, ranges);
            }
        }
    }
}
