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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import accord.utils.Invariants;

import org.apache.cassandra.metrics.LogLinearHistogram.LogLinearSnapshot;

import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.MEAN_LIFETIME_IN_S;
import static org.apache.cassandra.metrics.LogLinearHistogram.MAX_INDEX;
import static org.apache.cassandra.metrics.LogLinearHistogram.index;
import static org.apache.cassandra.metrics.LogLinearHistogram.invertIndex;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A simple single-threaded decaying histogram with log-linear bucket distribution.
 * This has approximately the same accuracy as the lg 1.2 growth of EstimatedHistogram, but is simpler and faster.
 *
 * Logically very similar to DecayingEstimatedHistogramReservoir, only due to single-threaded behaviour this can be
 * performed more simply and efficiently. We project forward single updates, and periodically rebase in place.
 * We also share the costly arithmetic across a group of histograms that share mutual exclusivity.
 *
 * TODO (desired): improve performance and memory locality by using a small buffer for collecting updates with e.g. 4 bits per counter,
 */
public class LogLinearDecayingHistograms
{
    public static class Buffer
    {
        static final long LARGE_VALUE = -1L >>> (1 + HISTOGRAM_INDEX_BITS);
        private final LogLinearDecayingHistograms histograms;
        private long[] buffer = new long[2];
        private int bufferCount;

        public Buffer(LogLinearDecayingHistograms histograms)
        {
            this.histograms = histograms;
        }

        private void add(long histogramIndex, long value)
        {
            if (!Invariants.expect(value >= 0, "Negative value reported: %s", value))
                return;

            Invariants.require(histogramIndex <= HISTOGRAM_INDEX_MASK);
            Invariants.require(value >= 0);
            if (value <= LARGE_VALUE) value <<= HISTOGRAM_INDEX_BITS;
            else
            {
                value |= Long.MIN_VALUE;
                value &= ~HISTOGRAM_INDEX_MASK;
            }
            value |= histogramIndex;
            if (bufferCount == buffer.length)
                buffer = Arrays.copyOf(buffer, bufferCount * 2);
            buffer[bufferCount++] = value;
        }

        public void clear()
        {
            bufferCount = 0;
        }

        public void flush(long at)
        {
            for (int i = 0 ; i < bufferCount ; ++i)
            {
                long v = buffer[i];
                int histogramIndex = (int) (v & HISTOGRAM_INDEX_MASK);
                if (v < 0) v &= ~Long.MIN_VALUE & ~HISTOGRAM_INDEX_MASK;
                else v >>>= HISTOGRAM_INDEX_BITS;
                histograms.histograms.get(histogramIndex).increment(v, at);
            }
            bufferCount = 0;
        }

        public boolean isEmpty()
        {
            return bufferCount == 0;
        }
    }

    public class LogLinearDecayingHistogram
    {
        final int histogramIndex;

        private double[] buckets;
        double totalCount;

        private LogLinearDecayingHistogram(int histogramIndex, long expectedMaxValue)
        {
            this.histogramIndex = histogramIndex;
            buckets = new double[LogLinearHistogram.bucketCount(expectedMaxValue)];
        }

        public void increment(long value)
        {
            increment(value, nanoTime());
        }

        public void increment(long value, long at)
        {
            if (!Invariants.expect(value >= 0, "Negative value reported: %s", value))
                return;

            int index = index(value);
            if (bufferCount >= buffer.length)
            {
                Invariants.require(bufferCount == buffer.length);
                flush();
                Invariants.require(bufferCount == 0);
            }
            updateDecay(at);
            long v = Double.doubleToRawLongBits(increment) & VALUE_MASK;
            v |= histogramIndex | ((long)index << HISTOGRAM_INDEX_BITS);
            Invariants.require(Double.longBitsToDouble(v & VALUE_MASK) > 0);
            buffer[bufferCount++] = v;
        }

        private double[] buckets(int withIndexAtLeast)
        {
            if (buckets.length <= withIndexAtLeast)
            {
                Invariants.require(withIndexAtLeast <= MAX_INDEX);
                buckets = Arrays.copyOf(buckets, (withIndexAtLeast | 0x3) + 1);
            }
            return buckets;
        }

        public LogLinearSnapshot copyToSnapshot(long at)
        {
            LogLinearSnapshot snapshot = new LogLinearSnapshot(new long[buckets.length], 0);
            updateSnapshot(snapshot, at);
            return snapshot;
        }

        public void updateSnapshot(LogLinearSnapshot snapshot, long at)
        {
            decay(at);

            if (snapshot.raw.length < buckets.length)
                snapshot.raw = Arrays.copyOf(snapshot.raw, buckets.length);

            long[] raw = snapshot.raw;
            long total = 0;
            for (int i = 0 ; i < buckets.length ; ++i)
            {
                long v = Math.round(buckets[i]);
                raw[i] += v;
                total += v;
            }

            snapshot.totalCount += total;
            snapshot.cumulative = null;
        }

        public void add(Buffer buffer, long value)
        {
            Invariants.require(buffer.histograms == LogLinearDecayingHistograms.this);
            buffer.add(histogramIndex, value);
        }

        public void clear()
        {
            flush();
            totalCount = 0;
            Arrays.fill(buckets, 0);
        }
    }

    private static final long ANTI_DECAY_REFRESH_RATE = TimeUnit.SECONDS.toNanos(1L);
    private static final long DECAY_REFRESH_RATE = TimeUnit.HOURS.toNanos(1L);
    private static final double DECAY_RATE = 1d/(TimeUnit.SECONDS.toNanos(1) * MEAN_LIFETIME_IN_S);
    private static final int BUCKET_INDEX_BITS = 8;
    private static final int HISTOGRAM_INDEX_BITS = 5;
    private static final long HISTOGRAM_INDEX_MASK = (1L << HISTOGRAM_INDEX_BITS) - 1;
    private static final long BUCKET_INDEX_MASK = (1L << BUCKET_INDEX_BITS) - 1;
    private static final long VALUE_MASK = ~(HISTOGRAM_INDEX_MASK | (BUCKET_INDEX_MASK << HISTOGRAM_INDEX_BITS));
    private static final int BUFFER_SIZE = 64;

    static
    {
        Invariants.require((1 << BUCKET_INDEX_BITS) > MAX_INDEX);
    }

    private final long[] buffer = new long[BUFFER_SIZE];
    private int bufferCount;
    private long lastDecayedAt, lastAntiDecayedAt;
    private double increment = 1d;

    List<LogLinearDecayingHistogram> histograms = new ArrayList<>();

    void updateDecay(long now)
    {
        long delta = now - lastAntiDecayedAt;
        if (delta <= ANTI_DECAY_REFRESH_RATE)
            return;

        if (delta < DECAY_REFRESH_RATE) antiDecay(now);
        else decay(now);
    }

    private void antiDecay(long now)
    {
        increment = Math.exp((now - lastDecayedAt) * DECAY_RATE);
    }

    private void flush()
    {
        for (int i = 0 ; i < bufferCount ; ++i)
        {
            long bits = buffer[i];
            double increment = Double.longBitsToDouble(bits & VALUE_MASK);
            int histogramIndex = (int) (bits & HISTOGRAM_INDEX_MASK);
            int bucketIndex = (int)((bits >>> HISTOGRAM_INDEX_BITS) & BUCKET_INDEX_MASK);
            LogLinearDecayingHistogram histogram = histograms.get(histogramIndex);
            histogram.buckets(bucketIndex)[bucketIndex] += increment;
            histogram.totalCount += increment;
        }
        bufferCount = 0;
    }

    private void decay(long now)
    {
        flush();

        if (now <= lastDecayedAt)
            return;

        antiDecay(now);
        double decay = 1d/increment;
        increment = 1d;
        for (LogLinearDecayingHistogram histogram : histograms)
        {
            if (histogram.totalCount <= 0d)
                continue;

            double[] buckets = histogram.buckets;
            double total = 0d;
            for (int i = 0 ; i < buckets.length ; ++i)
            {
                if (buckets[i] <= 0d)
                    continue;

                double in = buckets[i];
                double out = in * decay;
                if (out < 0.5d)
                    out = 0d;
                buckets[i] = out;
                total += out;
            }
            histogram.totalCount = total;
        }
        lastDecayedAt = lastAntiDecayedAt = now;
    }

    public synchronized LogLinearDecayingHistogram allocate(long expectedMaxValue)
    {
        Invariants.require(histograms.size() <= HISTOGRAM_INDEX_MASK);
        LogLinearDecayingHistogram histogram = new LogLinearDecayingHistogram(histograms.size(), expectedMaxValue);
        histograms.add(histogram);
        return histogram;
    }

    public synchronized LogLinearDecayingHistogram get(int histogramIndex)
    {
        return histograms.get(histogramIndex);
    }

    public static double[] bucketsWithScale(long maxScale)
    {
        return bucketsWithLength(1 + index(maxScale));
    }

    public static double[] bucketsWithLength(int length)
    {
        Invariants.require(length <= MAX_INDEX + 1);
        double[] buckets = new double[length];
        for (int i = 0 ; i < length ; ++i)
            buckets[i] = invertIndex(i);
        return buckets;
    }
}
