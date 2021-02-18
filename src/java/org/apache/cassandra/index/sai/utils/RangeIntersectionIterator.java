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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * Modified from {@link org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator} to support:
 * 1. no generic type to reduce allocation
 * 2. support selective intersection to reduce disk io
 * 3. make sure iterators are closed when intersection ends because of lazy key fetching
 */
@SuppressWarnings("resource")
public class RangeIntersectionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // The cassandra.sai.intersection.clause.limit (default: 2) controls the maximum number of range iterator that
    // will be used in the final intersection of a query operation.
    private static final int INTERSECTION_CLAUSE_LIMIT = Integer.getInteger("cassandra.sai.intersection.clause.limit", 2);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", INTERSECTION_CLAUSE_LIMIT));
    }

    public static boolean shouldDefer(int numberOfExpressions)
    {
        return (INTERSECTION_CLAUSE_LIMIT <= 0) || (numberOfExpressions <= INTERSECTION_CLAUSE_LIMIT);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder selectiveBuilder()
    {
        return selectiveBuilder(INTERSECTION_CLAUSE_LIMIT);
    }

    public static Builder selectiveBuilder(int limit)
    {
        return new Builder(limit);
    }

    public static class Builder extends RangeIterator.Builder
    {
        private final int limit;

        public Builder()
        {
            super(IteratorType.INTERSECTION);
            this.limit = Integer.MAX_VALUE;
        }

        public Builder(int limit)
        {
            super(IteratorType.INTERSECTION);
            this.limit = limit;
        }

        protected RangeIterator buildIterator()
        {
            // all ranges will be included
            if (limit >= ranges.size() || limit <= 0)
                return buildIterator(statistics, ranges);

            // Apply most selective iterators during intersection, because larger number of iterators will result lots of disk seek.
            List<RangeIterator> selectiveIterator = new ArrayList<>(ranges);
            selectiveIterator.sort(Comparator.comparingLong(RangeIterator::getCount));

            Statistics selectiveStatistics = new Statistics(IteratorType.INTERSECTION);
            for (int i = selectiveIterator.size() - 1; i >= 0 && i >= limit; i--)
                FileUtils.closeQuietly(selectiveIterator.remove(i));

            for (RangeIterator iterator : selectiveIterator)
                selectiveStatistics.update(iterator);

            if (Tracing.isTracing())
                Tracing.trace("Selecting {} {} of {} out of {} indexes",
                              selectiveIterator.size(),
                              selectiveIterator.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                              selectiveIterator.stream().map(RangeIterator::getCount).map(Object::toString).collect(Collectors.joining(", ")),
                              ranges.size());

            PriorityQueue<RangeIterator> selectiveRanges = new PriorityQueue<>(limit, Comparator.comparing(RangeIterator::getCurrent));
            selectiveRanges.addAll(selectiveIterator);

            return buildIterator(selectiveStatistics, selectiveRanges);
        }

        private static RangeIterator buildIterator(Statistics statistics, PriorityQueue<RangeIterator> ranges)
        {
            // if the range is disjoint or we have an intersection with an empty set,
            // we can simply return an empty iterator, because it's not going to produce any results.
            if (statistics.isDisjoint())
            {
                // release posting lists
                FileUtils.closeQuietly(ranges);
                return RangeIterator.empty();
            }

            if (ranges.size() == 1)
                return ranges.poll();

            return new BounceIntersectionIterator(statistics, ranges);
        }
    }

    /**
     * Iterator which performs intersection of multiple ranges by using bouncing (merge-join) technique to identify
     * common elements in the given ranges. Aforementioned "bounce" works as follows: range queue is poll'ed for the
     * range with the smallest current token (main loop), that token is used to {@link RangeIterator#skipTo(Long)}
     * other ranges, if token produced by {@link RangeIterator#skipTo(Long)} is equal to current "candidate" token,
     * both get merged together and the same operation is repeated for next range from the queue, if returned token
     * is not equal than candidate, candidate's range gets put back into the queue and the main loop gets repeated until
     * next intersection token is found or at least one iterator runs out of tokens.
     *
     * This technique is every efficient to jump over gaps in the ranges.
     */
    private static class BounceIntersectionIterator extends RangeIterator
    {
        private final PriorityQueue<RangeIterator> ranges;
        private final Token.TokenMerger merger;
        private final List<RangeIterator> toRelease;
        private final List<RangeIterator> processedRanges;

        private BounceIntersectionIterator(Builder.Statistics statistics, PriorityQueue<RangeIterator> ranges)
        {
            super(statistics);
            this.ranges = ranges;
            this.toRelease = new ArrayList<>(ranges);
            this.merger = new Token.ReusableTokenMerger(ranges.size());
            this.processedRanges = new ArrayList<>(ranges.size());
        }

        protected Token computeNext()
        {
            RangeIterator head = ranges.poll();

            if (head == null)
                return endOfData();

            // jump right to the beginning of the intersection or return next element
            if (head.getCurrent().compareTo(getMinimum()) < 0)
                head.skipTo(getMinimum());

            Token candidate = head.hasNext() ? head.next() : null;

            if (candidate == null || candidate.get() > getMaximum())
                return endOfData();

            merger.reset();
            merger.add(candidate);

            processedRanges.clear();

            boolean intersectsAll = true;
            while (!ranges.isEmpty())
            {
                RangeIterator range = ranges.poll();

                // found a range which doesn't overlap with one (or possibly more) other range(s)
                // or the range is exhausted
                if (!isOverlapping(head, range) || (range.skipTo(candidate.get()) == null))
                {
                    intersectsAll = false;
                    break;
                }

                int cmp = Long.compare(candidate.get(), range.getCurrent());

                if (cmp == 0)
                {
                    merger.add(range.next());
                    // advance skipped range to the next element if any
                    range.hasNext();
                    processedRanges.add(range);
                }
                else if (cmp < 0)
                {
                    // the candidate is less than the current value in the next range
                    // so make the next range the candidate and start again
                    candidate = range.next();
                    merger.reset();
                    merger.add(candidate);
                    ranges.add(head);
                    ranges.addAll(processedRanges);
                    processedRanges.clear();
                    head = range;
                }
                else
                {
                    intersectsAll = false;
                    break;
                }
            }

            if (intersectsAll)
            {
                ranges.add(head);
                ranges.addAll(processedRanges);
                return merger.merge();
            }

            return endOfData();
        }

        protected void performSkipTo(Long nextToken)
        {
            List<RangeIterator> skipped = new ArrayList<>();

            while (!ranges.isEmpty())
            {
                RangeIterator range = ranges.poll();
                range.skipTo(nextToken);
                skipped.add(range);
            }

            for (RangeIterator range : skipped)
                ranges.add(range);
        }

        public void close() throws IOException
        {
            toRelease.forEach(FileUtils::closeQuietly);
        }
    }
}
