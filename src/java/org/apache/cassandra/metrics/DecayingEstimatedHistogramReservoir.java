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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * A decaying histogram reservoir where values collected during each minute will be twice as significant as the values
 * collected in the previous minute. Measured values are collected in variable sized buckets, using small buckets in the
 * lower range and larger buckets in the upper range. Use this histogram when you want to know if the distribution of
 * the underlying data stream has changed recently and you want high resolution on values in the lower range.
 *
 * The histogram use forward decay [1] to make recent values more significant. The forward decay factor will be doubled
 * every minute (half-life time set to 60 seconds) [2]. The forward decay landmark is reset every 30 minutes (or at
 * first read/update after 30 minutes). During landmark reset, updates and reads in the reservoir will be blocked in a
 * fashion similar to the one used in the metrics library [3]. The 30 minute rescale interval is used based on the
 * assumption that in an extreme case we would have to collect a metric 1M times for a single bucket each second. By the
 * end of the 30:th minute all collected values will roughly add up to 1.000.000 * 60 * pow(2, 30) which can be
 * represented with 56 bits giving us some head room in a signed 64 bit long.
 *
 * Internally two reservoirs are maintained, one with decay and one without decay. All public getters in a {@Snapshot}
 * will expose the decay functionality with the exception of the {@link Snapshot#getValues()} which will return values
 * from the reservoir without decay. This makes it possible for the caller to maintain precise deltas in an interval of
 * its choise.
 *
 * The bucket size starts at 1 and grows by 1.2 each time (rounding and removing duplicates). It goes from 1 to around
 * 18T by default (creating 164+1 buckets), which will give a timing resolution from microseconds to roughly 210 days,
 * with less precision as the numbers get larger.
 *
 * The series of values to which the counts in `decayingBuckets` correspond:
 * 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, 24, 29, 35, 42, 50, 60, 72 etc.
 * Thus, a `decayingBuckets` of [0, 0, 1, 10] would mean we had seen 1 value of 3 and 10 values of 4.
 *
 * Each bucket represents values from (previous bucket offset, current offset].
 *
 * [1]: http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
 * [2]: https://en.wikipedia.org/wiki/Half-life
 * [3]: https://github.com/dropwizard/metrics/blob/v3.1.2/metrics-core/src/main/java/com/codahale/metrics/ExponentiallyDecayingReservoir.java
 */
public class DecayingEstimatedHistogramReservoir implements Reservoir
{
    /**
     * The default number of decayingBuckets. Use this bucket count to reduce memory allocation for bucket offsets.
     */
    public static final int DEFAULT_BUCKET_COUNT = 164;
    public static final boolean DEFAULT_ZERO_CONSIDERATION = false;

    // The offsets used with a default sized bucket array without a separate bucket for zero values.
    public static final long[] DEFAULT_WITHOUT_ZERO_BUCKET_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, false);

    // The offsets used with a default sized bucket array with a separate bucket for zero values.
    public static final long[] DEFAULT_WITH_ZERO_BUCKET_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, true);

    // Represents the bucket offset as created by {@link EstimatedHistogram#newOffsets()}
    private final long[] bucketOffsets;

    // decayingBuckets and buckets are one element longer than bucketOffsets -- the last element is values greater than the last offset
    private final AtomicLongArray decayingBuckets;
    private final AtomicLongArray buckets;

    public static final long HALF_TIME_IN_S = 60L;
    public static final double MEAN_LIFETIME_IN_S = HALF_TIME_IN_S / Math.log(2.0);
    public static final long LANDMARK_RESET_INTERVAL_IN_MS = 30L * 60L * 1000L;

    private final AtomicBoolean rescaling = new AtomicBoolean(false);
    private volatile long decayLandmark;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Wrapper around System.nanoTime() to simplify unit testing.
    private final Clock clock;


    /**
     * Construct a decaying histogram with default number of buckets and without considering zeroes.
     */
    public DecayingEstimatedHistogramReservoir()
    {
        this(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, Clock.defaultClock());
    }

    /**
     * Construct a decaying histogram with default number of buckets.
     *
     * @param considerZeroes when true, 0-value measurements in a separate bucket, otherwise they will be collected in
     *                       same bucket as 1-value measurements
     */
    public DecayingEstimatedHistogramReservoir(boolean considerZeroes)
    {
        this(considerZeroes, DEFAULT_BUCKET_COUNT, Clock.defaultClock());
    }

    /**
     * Construct a decaying histogram.
     *
     * @param considerZeroes when true, 0-value measurements in a separate bucket, otherwise they will be collected in
     *                       same bucket as 1-value measurements
     * @param bucketCount number of buckets used to collect measured values
     */
    public DecayingEstimatedHistogramReservoir(boolean considerZeroes, int bucketCount)
    {
        this(considerZeroes, bucketCount, Clock.defaultClock());
    }

    @VisibleForTesting
    DecayingEstimatedHistogramReservoir(boolean considerZeroes, int bucketCount, Clock clock)
    {
        if (bucketCount == DEFAULT_BUCKET_COUNT)
        {
            if (considerZeroes == true)
            {
                bucketOffsets = DEFAULT_WITH_ZERO_BUCKET_OFFSETS;
            }
            else
            {
                bucketOffsets = DEFAULT_WITHOUT_ZERO_BUCKET_OFFSETS;
            }
        }
        else
        {
            bucketOffsets = EstimatedHistogram.newOffsets(bucketCount, considerZeroes);
        }
        decayingBuckets = new AtomicLongArray(bucketOffsets.length + 1);
        buckets = new AtomicLongArray(bucketOffsets.length + 1);
        this.clock = clock;
        decayLandmark = clock.getTime();
    }

    /**
     * Increments the count of the bucket closest to n, rounding UP.
     *
     * @param value the data point to add to the histogram
     */
    public void update(long value)
    {
        long now = clock.getTime();
        rescaleIfNeeded(now);

        int index = Arrays.binarySearch(bucketOffsets, value);
        if (index < 0)
        {
            // inexact match, take the first bucket higher than n
            index = -index - 1;
        }
        // else exact match; we're good

        lockForRegularUsage();

        try
        {
            decayingBuckets.getAndAdd(index, Math.round(forwardDecayWeight(now)));
        }
        finally
        {
            unlockForRegularUsage();
        }

        buckets.getAndIncrement(index);
    }

    private double forwardDecayWeight(long now)
    {
        return Math.exp(((now - decayLandmark) / 1000L) / MEAN_LIFETIME_IN_S);
    }

    /**
     * Return the number of buckets where recorded values are stored.
     *
     * This method does not return the number of recorded values as suggested by the {@link Reservoir} interface.
     *
     * @return the number of buckets
     */
    public int size()
    {
        return decayingBuckets.length();
    }

    /**
     * Returns a snapshot of the decaying values in this reservoir.
     *
     * Non-decaying reservoir will not be included in the snapshot.
     *
     * @return the snapshot
     */
    public Snapshot getSnapshot()
    {
        rescaleIfNeeded();

        lockForRegularUsage();

        try
        {
            return new EstimatedHistogramReservoirSnapshot(this);
        }
        finally
        {
            unlockForRegularUsage();
        }
    }

    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    @VisibleForTesting
    boolean isOverflowed()
    {
        return decayingBuckets.get(decayingBuckets.length() - 1) > 0;
    }

    private void rescaleIfNeeded()
    {
        rescaleIfNeeded(clock.getTime());
    }

    private void rescaleIfNeeded(long now)
    {
        if (needRescale(now))
        {
            if (rescaling.compareAndSet(false, true))
            {
                try
                {
                    rescale(now);
                }
                finally
                {
                    rescaling.set(false);
                }
            }
        }
    }

    private void rescale(long now)
    {
        // Check again to make sure that another thread didn't complete rescale already
        if (needRescale(now))
        {
            lockForRescale();

            try
            {
                final double rescaleFactor = forwardDecayWeight(now);
                decayLandmark = now;

                final int bucketCount = decayingBuckets.length();
                for (int i = 0; i < bucketCount; i++)
                {
                    long newValue = Math.round((decayingBuckets.get(i) / rescaleFactor));
                    decayingBuckets.set(i, newValue);
                }
            }
            finally
            {
                unlockForRescale();
            }
        }
    }

    private boolean needRescale(long now)
    {
        return (now - decayLandmark) > LANDMARK_RESET_INTERVAL_IN_MS;
    }

    @VisibleForTesting
    public void clear()
    {
        lockForRescale();

        try
        {
            final int bucketCount = decayingBuckets.length();
            for (int i = 0; i < bucketCount; i++)
            {
                decayingBuckets.set(i, 0L);
                buckets.set(i, 0L);
            }
        }
        finally
        {
            unlockForRescale();
        }
    }

    private void lockForRegularUsage()
    {
        this.lock.readLock().lock();
    }

    private void unlockForRegularUsage()
    {
        this.lock.readLock().unlock();
    }

    private void lockForRescale()
    {
        this.lock.writeLock().lock();
    }

    private void unlockForRescale()
    {
        this.lock.writeLock().unlock();
    }


    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * Represents a snapshot of the decaying histogram.
     *
     * The decaying buckets are copied into a snapshot array to give a consistent view for all getters. However, the
     * copy is made without a write-lock and so other threads may change the buckets while the array is copied,
     * probably causign a slight skew up in the quantiles and mean values.
     *
     * The decaying buckets will be used for quantile calculations and mean values, but the non decaying buckets will be
     * exposed for calls to {@link Snapshot#getValues()}.
     */
    private class EstimatedHistogramReservoirSnapshot extends Snapshot
    {
        private final long[] decayingBuckets;

        public EstimatedHistogramReservoirSnapshot(DecayingEstimatedHistogramReservoir reservoir)
        {
            final int length = reservoir.decayingBuckets.length();
            final double rescaleFactor = forwardDecayWeight(clock.getTime());

            this.decayingBuckets = new long[length];

            for (int i = 0; i < length; i++)
                this.decayingBuckets[i] = Math.round(reservoir.decayingBuckets.get(i) / rescaleFactor);
        }

        /**
         * Get the estimated value at the specified quantile in the distribution.
         *
         * @param quantile the quantile specified as a value between 0.0 (zero) and 1.0 (one)
         * @return estimated value at given quantile
         * @throws IllegalStateException in case the histogram overflowed
         */
        public double getValue(double quantile)
        {
            assert quantile >= 0 && quantile <= 1.0;

            final int lastBucket = decayingBuckets.length - 1;

            if (decayingBuckets[lastBucket] > 0)
                throw new IllegalStateException("Unable to compute when histogram overflowed");

            final long qcount = (long) Math.ceil(count() * quantile);
            if (qcount == 0)
                return 0;

            long elements = 0;
            for (int i = 0; i < lastBucket; i++)
            {
                elements += decayingBuckets[i];
                if (elements >= qcount)
                    return bucketOffsets[i];
            }
            return 0;
        }

        /**
         * Will return a snapshot of the non-decaying buckets.
         *
         * The values returned will not be consistent with the quantile and mean values. The caller must be aware of the
         * offsets created by {@link EstimatedHistogram#getBucketOffsets()} to make use of the values returned.
         *
         * @return a snapshot of the non-decaying buckets.
         */
        public long[] getValues()
        {
            final int length = buckets.length();

            long[] values = new long[length];

            for (int i = 0; i < length; i++)
                values[i] = buckets.get(i);

            return values;
        }

        /**
         * Return the number of buckets where recorded values are stored.
         *
         * This method does not return the number of recorded values as suggested by the {@link Snapshot} interface.
         *
         * @return the number of buckets
         */
        public int size()
        {
            return decayingBuckets.length;
        }

        /**
         * Return the number of registered values taking forward decay into account.
         *
         * @return the sum of all bucket values
         */
        private long count()
        {
            long sum = 0L;
            for (int i = 0; i < decayingBuckets.length; i++)
                sum += decayingBuckets[i];
            return sum;
        }

        /**
         * Get the estimated max-value that could have been added to this reservoir.
         *
         * As values are collected in variable sized buckets, the actual max value recored in the reservoir may be less
         * than the value returned.
         *
         * @return the largest value that could have been added to this reservoir, or Long.MAX_VALUE if the reservoir
         * overflowed
         */
        public long getMax()
        {
            final int lastBucket = decayingBuckets.length - 1;

            if (decayingBuckets[lastBucket] > 0)
                return Long.MAX_VALUE;

            for (int i = lastBucket - 1; i >= 0; i--)
            {
                if (decayingBuckets[i] > 0)
                    return bucketOffsets[i];
            }
            return 0;
        }

        /**
         * Get the estimated mean value in the distribution.
         *
         * @return the mean histogram value (average of bucket offsets, weighted by count)
         * @throws IllegalStateException if any values were greater than the largest bucket threshold
         */
        public double getMean()
        {
            final int lastBucket = decayingBuckets.length - 1;

            if (decayingBuckets[lastBucket] > 0)
                throw new IllegalStateException("Unable to compute when histogram overflowed");

            long elements = 0;
            long sum = 0;
            for (int i = 0; i < lastBucket; i++)
            {
                long bCount = decayingBuckets[i];
                elements += bCount;
                sum += bCount * bucketOffsets[i];
            }

            return (double) sum / elements;
        }

        /**
         * Get the estimated min-value that could have been added to this reservoir.
         *
         * As values are collected in variable sized buckets, the actual min value recored in the reservoir may be
         * higher than the value returned.
         *
         * @return the smallest value that could have been added to this reservoir
         */
        public long getMin()
        {
            for (int i = 0; i < decayingBuckets.length; i++)
            {
                if (decayingBuckets[i] > 0)
                    return i == 0 ? 0 : 1 + bucketOffsets[i - 1];
            }
            return 0;
        }

        /**
         * Get the estimated standard deviation of the values added to this reservoir.
         *
         * As values are collected in variable sized buckets, the actual deviation may be more or less than the value
         * returned.
         *
         * @return an estimate of the standard deviation
         */
        public double getStdDev()
        {
            final int lastBucket = decayingBuckets.length - 1;

            if (decayingBuckets[lastBucket] > 0)
                throw new IllegalStateException("Unable to compute when histogram overflowed");

            final long count = count();

            if(count <= 1)
            {
                return 0.0D;
            }
            else
            {
                double mean = this.getMean();
                double sum = 0.0D;

                for(int i = 0; i < lastBucket; ++i)
                {
                    long value = bucketOffsets[i];
                    double diff = value - mean;
                    sum += diff * diff * decayingBuckets[i];
                }

                return Math.sqrt(sum / (count - 1));
            }
        }

        public void dump(OutputStream output)
        {
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(output, UTF_8)))
            {
                int length = decayingBuckets.length;

                for(int i = 0; i < length; ++i)
                {
                    out.printf("%d%n", decayingBuckets[i]);
                }
            }
        }
    }
}
