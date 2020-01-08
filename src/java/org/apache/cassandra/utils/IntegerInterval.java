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

package org.apache.cassandra.utils;

import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

/**
 * Mutable integer interval class, thread-safe.
 * Represents the interval [lower,upper].
 */
public class IntegerInterval
{
    volatile long interval;
    private static AtomicLongFieldUpdater<IntegerInterval> intervalUpdater =
            AtomicLongFieldUpdater.newUpdater(IntegerInterval.class, "interval");

    private IntegerInterval(long interval)
    {
        this.interval = interval;
    }

    public IntegerInterval(int lower, int upper)
    {
        this(make(lower, upper));
    }

    public IntegerInterval(IntegerInterval src)
    {
        this(src.interval);
    }

    public int lower()
    {
        return lower(interval);
    }

    public int upper()
    {
        return upper(interval);
    }

    /**
     * Expands the interval to cover the given value by extending one of its sides if necessary.
     * Mutates this. Thread-safe.
     */
    public void expandToCover(int value)
    {
        long prev;
        int lower;
        int upper;
        do
        {
            prev = interval;
            upper = upper(prev);
            lower = lower(prev);
            if (value > upper) // common case
                upper = value;
            else if (value < lower)
                lower = value;
        }
        while (!intervalUpdater.compareAndSet(this, prev, make(lower, upper)));
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(interval);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (getClass() != obj.getClass())
            return false;
        IntegerInterval other = (IntegerInterval) obj;
        return interval == other.interval;
    }

    public String toString()
    {
        long interval = this.interval;
        return "[" + lower(interval) + "," + upper(interval) + "]";
    }

    private static long make(int lower, int upper)
    {
        assert lower <= upper;
        return ((lower & 0xFFFFFFFFL) << 32) | upper & 0xFFFFFFFFL;
    }

    private static int lower(long interval)
    {
        return (int) (interval >>> 32);
    }

    private static int upper(long interval)
    {
        return (int) interval;
    }


    /**
     * A mutable set of closed integer intervals, stored in normalized form (i.e. where overlapping intervals are
     * converted to a single interval covering both). Thread-safe.
     */
    public static class Set
    {
        static long[] EMPTY = new long[0];

        private volatile long[] ranges = EMPTY;

        /**
         * Adds an interval to the set, performing the necessary normalization.
         */
        public synchronized void add(int start, int end)
        {
            assert start <= end;
            long[] ranges, newRanges;
            {
                ranges = this.ranges; // take local copy to avoid risk of it changing in the midst of operation

                // extend ourselves to cover any ranges we overlap
                // record directly preceding our end may extend past us, so take the max of our end and its
                int rpos = Arrays.binarySearch(ranges, ((end & 0xFFFFFFFFL) << 32) | 0xFFFFFFFFL); // floor (i.e. greatest <=) of the end position
                if (rpos < 0)
                    rpos = (-1 - rpos) - 1;
                if (rpos >= 0)
                {
                    int extend = upper(ranges[rpos]);
                    if (extend > end)
                        end = extend;
                }

                // record directly preceding our start may extend into us; if it does, we take it as our start
                int lpos = Arrays.binarySearch(ranges, ((start & 0xFFFFFFFFL) << 32) | 0); // lower (i.e. greatest <) of the start position
                if (lpos < 0)
                    lpos = -1 - lpos;
                lpos -= 1;
                if (lpos >= 0)
                {
                    if (upper(ranges[lpos]) >= start)
                    {
                        start = lower(ranges[lpos]);
                        --lpos;
                    }
                }

                newRanges = new long[ranges.length - (rpos - lpos) + 1];
                int dest = 0;
                for (int i = 0; i <= lpos; ++i)
                    newRanges[dest++] = ranges[i];
                newRanges[dest++] = make(start, end);
                for (int i = rpos + 1; i < ranges.length; ++i)
                    newRanges[dest++] = ranges[i];
            }
            this.ranges = newRanges;
        }

        /**
         * Returns true if the set completely covers the given interval.
         */
        public boolean covers(IntegerInterval iv)
        {
            long l = iv.interval;
            return covers(lower(l), upper(l));
        }

        /**
         * Returns true if the set completely covers the given interval.
         */
        public boolean covers(int start, int end)
        {
            long[] ranges = this.ranges; // take local copy to avoid risk of it changing in the midst of operation
            int rpos = Arrays.binarySearch(ranges, ((start & 0xFFFFFFFFL) << 32) | 0xFFFFFFFFL);        // floor (i.e. greatest <=) of the end position
            if (rpos < 0)
                rpos = (-1 - rpos) - 1;
            if (rpos == -1)
                return false;
            return upper(ranges[rpos]) >= end;
        }

        /**
         * Returns a lower bound for the whole set. Will throw if set is not empty.
         */
        public int lowerBound()
        {
            return lower(ranges[0]);
        }

        /**
         * Returns an upper bound for the whole set. Will throw if set is not empty.
         */
        public int upperBound()
        {
            long[] ranges = this.ranges; // take local copy to avoid risk of it changing in the midst of operation
            return upper(ranges[ranges.length - 1]);
        }

        public Collection<IntegerInterval> intervals()
        {
            return Lists.transform(Longs.asList(ranges), iv -> new IntegerInterval(iv));
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(ranges);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (getClass() != obj.getClass())
                return false;
            Set other = (Set) obj;
            return Arrays.equals(ranges, other.ranges);
        }

        public String toString()
        {
            return "[" + intervals().stream().map(IntegerInterval::toString).collect(Collectors.joining(", ")) + "]";
        }
    }
}
