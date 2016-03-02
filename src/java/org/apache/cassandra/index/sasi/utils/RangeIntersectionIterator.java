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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.collect.Iterators;
import org.apache.cassandra.io.util.FileUtils;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings("resource")
public class RangeIntersectionIterator
{
    protected enum Strategy
    {
        BOUNCE, LOOKUP, ADAPTIVE
    }

    public static <K extends Comparable<K>, D extends CombinedValue<K>> Builder<K, D> builder()
    {
        return builder(Strategy.ADAPTIVE);
    }

    @VisibleForTesting
    protected static <K extends Comparable<K>, D extends CombinedValue<K>> Builder<K, D> builder(Strategy strategy)
    {
        return new Builder<>(strategy);
    }

    public static class Builder<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator.Builder<K, D>
    {
        private final Strategy strategy;

        public Builder(Strategy strategy)
        {
            super(IteratorType.INTERSECTION);
            this.strategy = strategy;
        }

        protected RangeIterator<K, D> buildIterator()
        {
            // if the range is disjoint we can simply return empty
            // iterator of any type, because it's not going to produce any results.
            if (statistics.isDisjoint())
                return new BounceIntersectionIterator<>(statistics, new PriorityQueue<RangeIterator<K, D>>(1));

            switch (strategy)
            {
                case LOOKUP:
                    return new LookupIntersectionIterator<>(statistics, ranges);

                case BOUNCE:
                    return new BounceIntersectionIterator<>(statistics, ranges);

                case ADAPTIVE:
                    return statistics.sizeRatio() <= 0.01d
                            ? new LookupIntersectionIterator<>(statistics, ranges)
                            : new BounceIntersectionIterator<>(statistics, ranges);

                default:
                    throw new IllegalStateException("Unknown strategy: " + strategy);
            }
        }
    }

    private static abstract class AbstractIntersectionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator<K, D>
    {
        protected final PriorityQueue<RangeIterator<K, D>> ranges;

        private AbstractIntersectionIterator(Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges)
        {
            super(statistics);
            this.ranges = ranges;
        }

        public void close() throws IOException
        {
            for (RangeIterator<K, D> range : ranges)
                FileUtils.closeQuietly(range);
        }
    }

    /**
     * Iterator which performs intersection of multiple ranges by using bouncing (merge-join) technique to identify
     * common elements in the given ranges. Aforementioned "bounce" works as follows: range queue is poll'ed for the
     * range with the smallest current token (main loop), that token is used to {@link RangeIterator#skipTo(Comparable)}
     * other ranges, if token produced by {@link RangeIterator#skipTo(Comparable)} is equal to current "candidate" token,
     * both get merged together and the same operation is repeated for next range from the queue, if returned token
     * is not equal than candidate, candidate's range gets put back into the queue and the main loop gets repeated until
     * next intersection token is found or at least one iterator runs out of tokens.
     *
     * This technique is every efficient to jump over gaps in the ranges.
     *
     * @param <K> The type used to sort ranges.
     * @param <D> The container type which is going to be returned by {@link Iterator#next()}.
     */
    @VisibleForTesting
    protected static class BounceIntersectionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends AbstractIntersectionIterator<K, D>
    {
        private BounceIntersectionIterator(Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges)
        {
            super(statistics, ranges);
        }

        protected D computeNext()
        {
            while (!ranges.isEmpty())
            {
                RangeIterator<K, D> head = ranges.poll();

                // jump right to the beginning of the intersection or return next element
                if (head.getCurrent().compareTo(getMinimum()) < 0)
                    head.skipTo(getMinimum());

                D candidate = head.hasNext() ? head.next() : null;
                if (candidate == null || candidate.get().compareTo(getMaximum()) > 0)
                {
                    ranges.add(head);
                    return endOfData();
                }

                List<RangeIterator<K, D>> processed = new ArrayList<>();

                boolean intersectsAll = true, exhausted = false;
                while (!ranges.isEmpty())
                {
                    RangeIterator<K, D> range = ranges.poll();

                    // found a range which doesn't overlap with one (or possibly more) other range(s)
                    if (!isOverlapping(head, range))
                    {
                        exhausted = true;
                        intersectsAll = false;
                        break;
                    }

                    D point = range.skipTo(candidate.get());

                    if (point == null) // other range is exhausted
                    {
                        exhausted = true;
                        intersectsAll = false;
                        break;
                    }

                    processed.add(range);

                    if (candidate.get().equals(point.get()))
                    {
                        candidate.merge(point);
                        // advance skipped range to the next element if any
                        Iterators.getNext(range, null);
                    }
                    else
                    {
                        intersectsAll = false;
                        break;
                    }
                }

                ranges.add(head);

                for (RangeIterator<K, D> range : processed)
                    ranges.add(range);

                if (exhausted)
                    return endOfData();

                if (intersectsAll)
                    return candidate;
            }

            return endOfData();
        }

        protected void performSkipTo(K nextToken)
        {
            List<RangeIterator<K, D>> skipped = new ArrayList<>();

            while (!ranges.isEmpty())
            {
                RangeIterator<K, D> range = ranges.poll();
                range.skipTo(nextToken);
                skipped.add(range);
            }

            for (RangeIterator<K, D> range : skipped)
                ranges.add(range);
        }
    }

    /**
     * Iterator which performs a linear scan over a primary range (the smallest of the ranges)
     * and O(log(n)) lookup into secondary ranges using values from the primary iterator.
     * This technique is efficient when one of the intersection ranges is smaller than others
     * e.g. ratio 0.01d (default), in such situation scan + lookup is more efficient comparing
     * to "bounce" merge because "bounce" distance is never going to be big.
     *
     * @param <K> The type used to sort ranges.
     * @param <D> The container type which is going to be returned by {@link Iterator#next()}.
     */
    @VisibleForTesting
    protected static class LookupIntersectionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends AbstractIntersectionIterator<K, D>
    {
        private final RangeIterator<K, D> smallestIterator;

        private LookupIntersectionIterator(Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges)
        {
            super(statistics, ranges);

            smallestIterator = statistics.minRange;

            if (smallestIterator.getCurrent().compareTo(getMinimum()) < 0)
                smallestIterator.skipTo(getMinimum());
        }

        protected D computeNext()
        {
            while (smallestIterator.hasNext())
            {
                D candidate = smallestIterator.next();
                K token = candidate.get();

                boolean intersectsAll = true;
                for (RangeIterator<K, D> range : ranges)
                {
                    // avoid checking against self, much cheaper than changing queue comparator
                    // to compare based on the size and re-populating such queue.
                    if (range.equals(smallestIterator))
                        continue;

                    // found a range which doesn't overlap with one (or possibly more) other range(s)
                    if (!isOverlapping(smallestIterator, range))
                        return endOfData();

                    D point = range.skipTo(token);

                    if (point == null) // one of the iterators is exhausted
                        return endOfData();

                    if (!point.get().equals(token))
                    {
                        intersectsAll = false;
                        break;
                    }

                    candidate.merge(point);
                }

                if (intersectsAll)
                    return candidate;
            }

            return endOfData();
        }

        protected void performSkipTo(K nextToken)
        {
            smallestIterator.skipTo(nextToken);
        }
    }
}
