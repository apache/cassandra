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
package org.apache.cassandra.index.sasi.utils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.io.util.FileUtils;

/**
 * Range Union Iterator is used to return sorted stream of elements from multiple RangeIterator instances.
 *
 * PriorityQueue is used as a sorting mechanism for the ranges, where each computeNext() operation would poll
 * from the queue (and push when done), which returns range that contains the smallest element, because
 * sorting is done on the moving window of range iteration {@link RangeIterator#getCurrent()}. Once retrieved
 * the smallest element (return candidate) is attempted to be merged with other ranges, because there could
 * be equal elements in adjacent ranges, such ranges are poll'ed only if their {@link RangeIterator#getCurrent()}
 * equals to the return candidate.
 *
 * @param <K> The type used to sort ranges.
 * @param <D> The container type which is going to be returned by {@link Iterator#next()}.
 */
public class RangeUnionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator<K, D>
{
    private final PriorityQueue<RangeIterator<K, D>> ranges;

    private RangeUnionIterator(Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges)
    {
        super(statistics);
        this.ranges = ranges;
    }

    public D computeNext()
    {
        RangeIterator<K, D> head = null;

        while (!ranges.isEmpty())
        {
            head = ranges.poll();
            if (head.hasNext())
                break;

            FileUtils.closeQuietly(head);
        }

        if (head == null || !head.hasNext())
            return endOfData();

        D candidate = head.next();

        List<RangeIterator<K, D>> processedRanges = new ArrayList<>();

        if (head.hasNext())
            processedRanges.add(head);
        else
            FileUtils.closeQuietly(head);

        while (!ranges.isEmpty())
        {
            // peek here instead of poll is an optimization
            // so we can re-insert less ranges back if candidate
            // is less than head of the current range.
            RangeIterator<K, D> range = ranges.peek();

            int cmp = candidate.get().compareTo(range.getCurrent());

            assert cmp <= 0;

            if (cmp < 0)
            {
                break; // candidate is smaller than next token, return immediately
            }
            else if (cmp == 0)
            {
                candidate.merge(range.next()); // consume and merge

                range = ranges.poll();
                // re-prioritize changed range

                if (range.hasNext())
                    processedRanges.add(range);
                else
                    FileUtils.closeQuietly(range);
            }
        }

        ranges.addAll(processedRanges);
        return candidate;
    }

    protected void performSkipTo(K nextToken)
    {
        List<RangeIterator<K, D>> changedRanges = new ArrayList<>();

        while (!ranges.isEmpty())
        {
            if (ranges.peek().getCurrent().compareTo(nextToken) >= 0)
                break;

            RangeIterator<K, D> head = ranges.poll();

            if (head.getMaximum().compareTo(nextToken) >= 0)
            {
                head.skipTo(nextToken);
                changedRanges.add(head);
                continue;
            }

            FileUtils.closeQuietly(head);
        }

        ranges.addAll(changedRanges.stream().collect(Collectors.toList()));
    }

    public void close() throws IOException
    {
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static <K extends Comparable<K>, D extends CombinedValue<K>> Builder<K, D> builder()
    {
        return new Builder<>();
    }

    public static <K extends Comparable<K>, D extends CombinedValue<K>> RangeIterator<K, D> build(List<RangeIterator<K, D>> tokens)
    {
        return new Builder<K, D>().add(tokens).build();
    }

    public static class Builder<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator.Builder<K, D>
    {
        public Builder()
        {
            super(IteratorType.UNION);
        }

        protected RangeIterator<K, D> buildIterator()
        {
            switch (rangeCount())
            {
                case 1:
                    return ranges.poll();

                default:
                    return new RangeUnionIterator<>(statistics, ranges);
            }
        }
    }
}
