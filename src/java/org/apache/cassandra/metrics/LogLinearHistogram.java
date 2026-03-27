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

package org.apache.cassandra.metrics;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.utils.Invariants;
import accord.utils.SortedArrays;

import static accord.utils.SortedArrays.Search.CEIL;

/**
 * A simple single-threaded histogram with log-linear buckets.
 * This has approximately the same accuracy as the lg 1.2 growth of EstimatedHistogram, but is simpler and faster.
 * This histogram also importantly supports decrements and replace (decrement/increment pairs), and auto grows if necessary,
 * so no overflow and can be initialised to a small size to avoid wasting memory
 *
 * TODO (desired): improve performance and memory locality by using a small buffer for collecting updates with e.g. 4 bits per counter,
 */
public class LogLinearHistogram
{
    public static final int MAX_INDEX = 247;
    private long[] buckets;
    long totalCount;

    public LogLinearHistogram(long expectedMaxValue)
    {
        buckets = new long[bucketCount(expectedMaxValue)];
    }

    public void increment(long value)
    {
        int index = index(value);
        buckets(index)[index]++;
        ++totalCount;
    }

    public void decrement(long value)
    {
        int index = index(value);
        buckets(index)[index]--;
        --totalCount;
    }

    public void replace(long decrement, long increment)
    {
        int decrementIndex = index(decrement);
        int incrementIndex = index(increment);
        long[] buckets = buckets(Math.max(decrementIndex, incrementIndex));
        if (decrementIndex != incrementIndex)
        {
            --buckets[decrementIndex];
            ++buckets[incrementIndex];
        }
    }

    public static long[] bucketsWithLength(int length)
    {
        Invariants.require(length <= MAX_INDEX + 1);
        long[] buckets = new long[length];
        for (int i = 0 ; i < length ; ++i)
            buckets[i] = invertIndex(i);
        return buckets;
    }

    public static long[] bucketsWithScale(long maxScale)
    {
        return bucketsWithLength(1 + index(maxScale));
    }

    private long[] buckets(int withIndexAtLeast)
    {
        if (buckets.length <= withIndexAtLeast)
        {
            Invariants.require(withIndexAtLeast <= MAX_INDEX);
            buckets = Arrays.copyOf(buckets, (withIndexAtLeast | 0x3) + 1);
        }
        return buckets;
    }

    static int bucketCount(long maxValue)
    {
        return 1 + (index(maxValue) | 0x3);
    }

    static int index(long value)
    {
        if (value < 4)
            return (int) value;
        int log = 61 - Long.numberOfLeadingZeros(value);
        int linear = (int) (value >>> log) & 0x3;
        return (log + 1) * 4 + linear;
    }

    static long invertIndex(int index)
    {
        if (index < 4)
            return index;

        int log = index / 4;
        int linear = index & 0x3;
        return (4L | linear) << (log - 1);
    }

    static
    {
        for (int i = 0 ; i < MAX_INDEX ; ++i)
        {
            Invariants.require(index(invertIndex(i)) == i);
            Invariants.require(i == 0 || index(invertIndex(i) - 1) == i - 1);
        }
        Invariants.require(index(invertIndex(MAX_INDEX + 1) - 1) == MAX_INDEX);
    }

    public static class LogLinearSnapshot extends Snapshot
    {
        long totalCount;
        long[] raw;
        long[] cumulative;

        public static LogLinearSnapshot emptyForMax(long maxValue)
        {
            return new LogLinearSnapshot(bucketCount(maxValue));
        }

        LogLinearSnapshot(int size)
        {
            this.raw = new long[size];
        }

        @VisibleForTesting
        LogLinearSnapshot(long[] raw, long totalCount)
        {
            this.raw = raw;
            this.totalCount = totalCount;
        }

        private long[] cumulative()
        {
            if (cumulative == null)
            {
                cumulative = new long[raw.length];
                long sum = 0;
                for (int i = 0 ; i < cumulative.length ; ++i)
                    cumulative[i] = sum += raw[i];
            }
            return cumulative;
        }

        private double get(long tot)
        {
            if (totalCount == 0 || tot == 0)
                return 0;

            long[] cumulative = cumulative();
            int i = SortedArrays.binarySearch(cumulative, 0, cumulative.length, tot, CEIL);
            if (i >= 0)
                return invertIndex(i);

            i = Math.max(0, -2 - i);
            long prevCount = cumulative[i];
            long nextCount = cumulative[i + 1];

            long prevValue = invertIndex(i);
            long nextValue = invertIndex(i + 1);

            double granularity = (nextValue - prevValue) / (double)(nextCount - prevCount);
            double targetGap = tot - prevCount;
            return prevValue + Math.round(targetGap * granularity);
        }

        @Override
        public double getValue(double quantile)
        {
            return get(Math.max(1L, (long)Math.ceil(totalCount * quantile)));
        }

        @Override
        public int size()
        {
            return Ints.saturatedCast(totalCount);
        }

        @Override
        public double getMean()
        {
            if (totalCount == 0)
                return 0.0D;

            double sum = 0;
            for (int i = 0; i < raw.length; i++)
            {
                if (raw[i] != 0)
                    sum += raw[i] * (double) invertIndex(i);
            }
            return sum / totalCount;
        }

        @Override
        public long getMin()
        {
            if (totalCount == 0)
                return 0;

            long[] cumulative = cumulative();
            int i = SortedArrays.binarySearch(cumulative, 0, cumulative.length, 1, CEIL);
            if (i < 0)
            {
                i = Math.max(0, -2 - i);
                if (cumulative[i] == 0)
                    ++i;
            }
            return invertIndex(i);
        }

        @Override
        public long getMax()
        {
            if (totalCount == 0)
                return 0;

            long[] cumulative = cumulative();
            int i = SortedArrays.binarySearch(cumulative, 0, cumulative.length, totalCount, CEIL);
            if (i < 0) i = -2 - i;
            return invertIndex(i + 1);
        }

        /**
         * Get the estimated standard deviation of the values added to this reservoir.
         *
         * As values are collected in variable sized buckets, the actual deviation may be more or less than the value
         * returned.
         *
         * @return an estimate of the standard deviation
         */
        @Override
        public double getStdDev()
        {
            if (totalCount <= 1)
                return 0.0D;

            double mean = this.getMean();
            double sum = 0.0D;

            for(int i = 0; i < raw.length; ++i)
            {
                long value = invertIndex(i);
                double diff = value - mean;
                sum += diff * diff * raw[i];
            }

            return Math.sqrt(sum / (totalCount - 1));
        }

        @Override
        public void dump(OutputStream output)
        {
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8)))
            {
                for(int i = 0; i < raw.length; ++i)
                    out.printf("%d%n", raw[i]);
            }
        }

        @Override
        public long[] getValues()
        {
            return raw;
        }
    }

    public LogLinearSnapshot destroyToSnapshot()
    {
        LogLinearSnapshot result = new LogLinearSnapshot(buckets, totalCount);
        buckets = null;
        this.totalCount = 0;
        return result;
    }

    public LogLinearSnapshot copyToSnapshot()
    {
        return new LogLinearSnapshot(buckets.clone(), totalCount);
    }

    public void updateSnapshot(LogLinearSnapshot snapshot)
    {
        if (snapshot.raw.length < buckets.length)
            snapshot.raw = Arrays.copyOf(snapshot.raw, buckets.length);

        long[] raw = snapshot.raw;
        for (int i = 0 ; i < buckets.length ; ++i)
            raw[i] = raw[i] + buckets[i];
        snapshot.totalCount += totalCount;
        snapshot.cumulative = null;
    }

    public void clear()
    {
        totalCount = 0;
        Arrays.fill(buckets, 0);
    }
}
