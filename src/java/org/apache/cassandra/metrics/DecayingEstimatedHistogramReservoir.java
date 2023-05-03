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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.cassandra.config.CassandraRelevantProperties.DECAYING_ESTIMATED_HISTOGRAM_RESERVOIR_STRIPE_COUNT;

/**
 * A decaying histogram reservoir where values collected during each minute will be twice as significant as the values
 * collected in the previous minute. Measured values are collected in variable sized buckets, using small buckets in the
 * lower range and larger buckets in the upper range. Use this histogram when you want to know if the distribution of
 * the underlying data stream has changed recently and you want high resolution on values in the lower range.
 * <p/>
 * The histogram use forward decay [1] to make recent values more significant. The forward decay factor will be doubled
 * every minute (half-life time set to 60 seconds) [2]. The forward decay landmark is reset every 30 minutes (or at
 * first read/update after 30 minutes). During landmark reset, updates and reads in the reservoir will be blocked in a
 * fashion similar to the one used in the metrics library [3]. The 30 minute rescale interval is used based on the
 * assumption that in an extreme case we would have to collect a metric 1M times for a single bucket each second. By the
 * end of the 30:th minute all collected values will roughly add up to 1.000.000 * 60 * pow(2, 30) which can be
 * represented with 56 bits giving us some head room in a signed 64 bit long.
 * <p/>
 * Internally two reservoirs are maintained, one with decay and one without decay. All public getters in a {@link Snapshot}
 * will expose the decay functionality with the exception of the {@link Snapshot#getValues()} which will return values
 * from the reservoir without decay. This makes it possible for the caller to maintain precise deltas in an interval of
 * its choice.
 * <p/>
 * The bucket size starts at 1 and grows by 1.2 each time (rounding and removing duplicates). It goes from 1 to around
 * 18T by default (creating 164+1 buckets), which will give a timing resolution from microseconds to roughly 210 days,
 * with less precision as the numbers get larger.
 * <p/>
 * The series of values to which the counts in `decayingBuckets` correspond:
 * 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, 24, 29, 35, 42, 50, 60, 72 etc.
 * Thus, a `decayingBuckets` of [0, 0, 1, 10] would mean we had seen 1 value of 3 and 10 values of 4.
 * <p/>
 * Each bucket represents values from (previous bucket offset, current offset].
 * <p/>
 * To reduce contention each logical bucket is striped accross a configurable number of stripes (default: 2). Threads are
 * assigned to specific stripes. In addition, logical buckets are distributed across the physical storage to reduce conention
 * when logically adjacent buckets are updated. See CASSANDRA-15213.
 * <p/>
 * <ul>
 *   <li>[1]: http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf</li>
 *   <li>[2]: https://en.wikipedia.org/wiki/Half-life</li>
 *   <li>[3]: https://github.com/dropwizard/metrics/blob/v3.1.2/metrics-core/src/main/java/com/codahale/metrics/ExponentiallyDecayingReservoir.java</li>
 * </ul>
 */
public class DecayingEstimatedHistogramReservoir implements SnapshottingReservoir
{
    private static final Logger logger = LoggerFactory.getLogger(DecayingEstimatedHistogramReservoir.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);
    /**
     * The default number of decayingBuckets. Use this bucket count to reduce memory allocation for bucket offsets.
     */
    public static final int DEFAULT_BUCKET_COUNT = 164;
    public static final int LOW_BUCKET_COUNT = 127;
    public static final int DEFAULT_STRIPE_COUNT = DECAYING_ESTIMATED_HISTOGRAM_RESERVOIR_STRIPE_COUNT.getInt();
    public static final int MAX_BUCKET_COUNT = 237;
    public static final boolean DEFAULT_ZERO_CONSIDERATION = false;

    private static final int[] DISTRIBUTION_PRIMES = new int[] { 17, 19, 23, 29 };

    // The offsets used with a default sized bucket array without a separate bucket for zero values.
    public static final long[] DEFAULT_WITHOUT_ZERO_BUCKET_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, false);

    // The offsets used with a default sized bucket array with a separate bucket for zero values.
    public static final long[] DEFAULT_WITH_ZERO_BUCKET_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, true);

    private static final int TABLE_BITS = 4;
    private static final int TABLE_MASK = -1 >>> (32 - TABLE_BITS);
    private static final float[] LOG2_TABLE = computeTable(TABLE_BITS);
    private static final float log2_12_recp = (float) (1d / slowLog2(1.2d));

    private static float[] computeTable(int bits)
    {
        float[] table = new float[1 << bits];
        for (int i = 1 ; i < 1<<bits ; ++i)
            table[i] = (float) slowLog2(ratio(i, bits));
        return table;
    }

    public static float fastLog12(long v)
    {
        return fastLog2(v) * log2_12_recp;
    }

    // returns 0 for all inputs <= 1
    private static float fastLog2(long v)
    {
        v = max(v, 1);
        int highestBitPosition = 63 - Long.numberOfLeadingZeros(v);
        v = Long.rotateRight(v, highestBitPosition - TABLE_BITS);
        int index = (int) (v & TABLE_MASK);
        float result = LOG2_TABLE[index];
        result += highestBitPosition;
        return result;
    }

    private static double slowLog2(double v)
    {
        return Math.log(v) / Math.log(2);
    }

    private static double ratio(int i, int bits)
    {
        return Float.intBitsToFloat((127 << 23) | (i << (23 - bits)));
    }

    // Represents the bucket offset as created by {@link EstimatedHistogram#newOffsets()}
    private final int nStripes;
    private final long[] bucketOffsets;
    private final int distributionPrime;

    // decayingBuckets and buckets are one element longer than bucketOffsets -- the last element is values greater than the last offset
    private final AtomicLongArray decayingBuckets;
    private final AtomicLongArray buckets;

    public static final long HALF_TIME_IN_S = 60L;
    public static final double MEAN_LIFETIME_IN_S = HALF_TIME_IN_S / Math.log(2.0);
    public static final long LANDMARK_RESET_INTERVAL_IN_NS = TimeUnit.MINUTES.toNanos(30L);

    private final AtomicBoolean rescaling = new AtomicBoolean(false);
    private volatile long decayLandmark;

    // Wrapper around System.nanoTime() to simplify unit testing.
    private final MonotonicClock clock;

    /**
     * Construct a decaying histogram with default number of buckets and without considering zeroes.
     */
    public DecayingEstimatedHistogramReservoir()
    {
        this(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT, MonotonicClock.Global.approxTime);
    }

    /**
     * Construct a decaying histogram with default number of buckets.
     *
     * @param considerZeroes when true, 0-value measurements in a separate bucket, otherwise they will be collected in
     *                       same bucket as 1-value measurements
     */
    public DecayingEstimatedHistogramReservoir(boolean considerZeroes)
    {
        this(considerZeroes, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT, MonotonicClock.Global.approxTime);
    }

    /**
     * Construct a decaying histogram.
     *
     * @param considerZeroes when true, 0-value measurements in a separate bucket, otherwise they will be collected in
     *                       same bucket as 1-value measurements
     * @param bucketCount number of buckets used to collect measured values
     */
    public DecayingEstimatedHistogramReservoir(boolean considerZeroes, int bucketCount, int stripes)
    {
        this(considerZeroes, bucketCount, stripes, MonotonicClock.Global.approxTime);
    }

    @VisibleForTesting
    public DecayingEstimatedHistogramReservoir(MonotonicClock clock)
    {
        this(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT, clock);
    }

    @VisibleForTesting
    DecayingEstimatedHistogramReservoir(boolean considerZeroes, int bucketCount, int stripes, MonotonicClock clock)
    {
        assert bucketCount <= MAX_BUCKET_COUNT : "bucket count cannot exceed: " + MAX_BUCKET_COUNT;

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

        nStripes = stripes;
        decayingBuckets = new AtomicLongArray((bucketOffsets.length + 1) * nStripes);
        buckets = new AtomicLongArray((bucketOffsets.length + 1) * nStripes);
        this.clock = clock;
        decayLandmark = clock.now();
        int distributionPrime = 1;
        for (int prime : DISTRIBUTION_PRIMES)
        {
            if (buckets.length() % prime != 0)
            {
                distributionPrime = prime;
                break;
            }
        }
        this.distributionPrime = distributionPrime;
    }

    /**
     * Increments the count of the bucket closest to n, rounding UP.
     *
     * @param value the data point to add to the histogram
     */
    public void update(long value)
    {
        long now = clock.now();
        rescaleIfNeeded(now);

        int index = findIndex(bucketOffsets, value);

        updateBucket(decayingBuckets, index, Math.round(forwardDecayWeight(now)));
        updateBucket(buckets, index, 1);
    }

    public void updateBucket(AtomicLongArray buckets, int index, long value)
    {
        int stripe = (int) (Thread.currentThread().getId() & (nStripes - 1));
        buckets.addAndGet(stripedIndex(index, stripe), value);
    }

    public int stripedIndex(int offsetIndex, int stripe)
    {
        return (((offsetIndex * nStripes + stripe) * distributionPrime) % buckets.length());
    }

    @VisibleForTesting
    public static int findIndex(long[] bucketOffsets, long value)
    {
        // values below zero are nonsense, but we have never failed when presented them
        value = max(value, 0);

        // The bucket index can be estimated using the equation Math.floor(Math.log(value) / Math.log(1.2))

        // By using an integer domain we effectively squeeze multiple exponents of 1.2 into the same bucket,
        // so for values > 2, we must "subtract" these exponents from the logarithm to determine which two buckets
        // to consult (as our approximation otherwise produces a value that is within 1 of the true value)
        int offset = (value > 2 ? 3 : 1) + (int)bucketOffsets[0];

        // See DecayingEstimatedHistogramResevoirTest#showEstimationWorks and DecayingEstimatedHistogramResevoirTest#testFindIndex()
        // for a runnable "proof"
        //
        // With this assumption, the estimate is calculated and the furthest offset from the estimation is checked
        // if this bucket does not contain the value then the next one will

        int firstCandidate = max(0, min(bucketOffsets.length - 1, ((int) fastLog12(value)) - offset));
        return value <= bucketOffsets[firstCandidate] ? firstCandidate : firstCandidate + 1;
    }

    private double forwardDecayWeight(long now)
    {
        return Math.exp(TimeUnit.NANOSECONDS.toSeconds(now - decayLandmark) / MEAN_LIFETIME_IN_S);
    }

    /**
     * Returns the logical number of buckets where recorded values are stored. The actual number of physical buckets
     * is size() * stripeCount()
     *
     * This method does not return the number of recorded values as suggested by the {@link Reservoir} interface.
     *
     * @return the number of buckets
     * @see #stripeCount()
     */
    public int size()
    {
        return bucketOffsets.length + 1;
    }


    public int stripeCount()
    {
        return nStripes;
    }
    /**
     * Returns a snapshot of the decaying values in this reservoir.
     *
     * Non-decaying reservoir will not be included in the snapshot.
     *
     * @return the snapshot
     */
    @Override
    public Snapshot getSnapshot()
    {
        rescaleIfNeeded();
        return new EstimatedHistogramReservoirSnapshot(this);
    }

    @Override
    public Snapshot getPercentileSnapshot()
    {
        rescaleIfNeeded();
        return new DecayingBucketsOnlySnapshot(this);
    }

    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    @VisibleForTesting
    boolean isOverflowed()
    {
        return bucketValue(bucketOffsets.length, true) > 0;
    }

    private long bucketValue(int index, boolean withDecay)
    {
        long val = 0;
        AtomicLongArray bs = withDecay ? decayingBuckets : buckets;
        for (int stripe = 0; stripe < nStripes; stripe++)
            val += bs.get(stripedIndex(index, stripe));

        return val;
    }

    @VisibleForTesting
    long stripedBucketValue(int i, boolean withDecay)
    {
        return withDecay ? decayingBuckets.get(i) : buckets.get(i);
    }

    private void rescaleIfNeeded()
    {
        rescaleIfNeeded(clock.now());
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
                    decayLandmark = now;
                    rescaling.set(false);
                }
            }
        }
    }

    private void rescale(long now)
    {
        // despite striping its safe to rescale each bucket individually
        final double rescaleFactor = forwardDecayWeight(now);
        for (int i = 0; i < decayingBuckets.length(); i++)
        {
            long newValue = Math.round(decayingBuckets.get(i) / rescaleFactor);
            decayingBuckets.set(i, newValue);
        }
    }

    private boolean needRescale(long now)
    {
        return (now - decayLandmark) > LANDMARK_RESET_INTERVAL_IN_NS;
    }

    @VisibleForTesting
    public void clear()
    {
        final int bucketCount = decayingBuckets.length();
        for (int i = 0; i < bucketCount; i++)
        {
            decayingBuckets.set(i, 0L);
            buckets.set(i, 0L);
        }
    }

    /**
     * Replaces current internal values with the given one from a Snapshot. This method is NOT thread safe, values
     * added at the same time to this reservoir using methods such as update may lose their data
     */
    public void rebase(EstimatedHistogramReservoirSnapshot snapshot)
    {
        // Check bucket count (a snapshot always has one stripe so the logical bucket count is used
        if (size() != snapshot.decayingBuckets.length)
        {
            throw new IllegalStateException("Unable to merge two DecayingEstimatedHistogramReservoirs with different bucket sizes");
        }

        // Check bucketOffsets
        for (int i = 0; i < bucketOffsets.length; i++)
        {
            if (bucketOffsets[i] != snapshot.bucketOffsets[i])
            {
                throw new IllegalStateException("Merge is only supported with equal bucketOffsets");
            }
        }

        this.decayLandmark = snapshot.snapshotLandmark;
        for (int i = 0; i < size(); i++)
        {
            // set rebased values in the first stripe and clear out all other data
            decayingBuckets.set(stripedIndex(i, 0), snapshot.decayingBuckets[i]);
            buckets.set(stripedIndex(i, 0), snapshot.values[i]);
            for (int stripe = 1; stripe < nStripes; stripe++)
            {
                decayingBuckets.set(stripedIndex(i, stripe), 0);
                buckets.set(stripedIndex(i, stripe), 0);
            }
        }

    }

    private static abstract class AbstractSnapshot extends Snapshot
    {
        protected final long[] decayingBuckets;
        protected final long[] bucketOffsets;

        AbstractSnapshot(DecayingEstimatedHistogramReservoir reservoir)
        {
            int length = reservoir.size();
            this.decayingBuckets = new long[length];
            this.bucketOffsets = reservoir.bucketOffsets; // No need to copy, these are immutable
        }

        /**
         * Get the estimated value at the specified quantile in the distribution.
         *
         * @param quantile the quantile specified as a value between 0.0 (zero) and 1.0 (one)
         * @return estimated value at given quantile
         * @throws IllegalStateException in case the histogram overflowed
         */
        @Override
        public double getValue(double quantile)
        {
            assert quantile >= 0 && quantile <= 1.0;

            final int lastBucket = decayingBuckets.length - 1;

            if (decayingBuckets[lastBucket] > 0)
            {
                try { throw new IllegalStateException("EstimatedHistogram overflow: " + Arrays.toString(decayingBuckets)); }
                catch (IllegalStateException e) { noSpamLogger.warn("", e); }
            }

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
         * Return the number of registered values taking forward decay into account.
         *
         * @return the sum of all bucket values
         */
        protected long count()
        {
            long sum = 0L;
            for (int i = 0; i < decayingBuckets.length; i++)
                sum += decayingBuckets[i];
            return sum;
        }

        /**
         * Get the estimated max-value that could have been added to this reservoir.
         *
         * As values are collected in variable sized buckets, the actual max value recorded in the reservoir may be less
         * than the value returned.
         *
         * @return the largest value that could have been added to this reservoir, or Long.MAX_VALUE if the reservoir
         * overflowed
         */
        @Override
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
        @Override
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
         * As values are collected in variable sized buckets, the actual min value recorded in the reservoir may be
         * higher than the value returned.
         *
         * @return the smallest value that could have been added to this reservoir
         */
        @Override
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
        @Override
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

        @Override
        public void dump(OutputStream output)
        {
            try (PrintWriter out = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8)))
            {
                int length = decayingBuckets.length;

                for(int i = 0; i < length; ++i)
                {
                    out.printf("%d%n", decayingBuckets[i]);
                }
            }
        }
    }

    /**
     * Represents a snapshot of the decaying histogram.
     *
     * The decaying buckets are copied into a snapshot array to give a consistent view for all getters. However, the
     * copy is made without a write-lock and so other threads may change the buckets while the array is copied,
     * probably causing a slight skew up in the quantiles and mean values.
     *
     * The decaying buckets will be used for quantile calculations and mean values, but the non decaying buckets will be
     * exposed for calls to {@link Snapshot#getValues()}.
     */
    static class EstimatedHistogramReservoirSnapshot extends AbstractSnapshot
    {
        private final long[] values;
        private long count;
        private long snapshotLandmark;
        private final DecayingEstimatedHistogramReservoir reservoir;

        public EstimatedHistogramReservoirSnapshot(DecayingEstimatedHistogramReservoir reservoir)
        {
            super(reservoir);
            
            int length = reservoir.size();
            double rescaleFactor = reservoir.forwardDecayWeight(reservoir.clock.now());

            this.values = new long[length];
            this.snapshotLandmark = reservoir.decayLandmark;

            for (int i = 0; i < length; i++)
            {
                this.decayingBuckets[i] = Math.round(reservoir.bucketValue(i, true) / rescaleFactor);
                this.values[i] = reservoir.bucketValue(i, false);
            }
            this.count = count();
            this.reservoir = reservoir;
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
            return values;
        }

        @Override
        public int size()
        {
            return Ints.saturatedCast(count);
        }

        @VisibleForTesting
        public long getSnapshotLandmark()
        {
            return snapshotLandmark;
        }

        @VisibleForTesting
        public Range getBucketingRangeForValue(long value)
        {
            int index = findIndex(bucketOffsets, value);
            long max = bucketOffsets[index];
            long min = index == 0 ? 0 : 1 + bucketOffsets[index - 1];
            return new Range(min, max);
        }

        /**
         * Adds another DecayingEstimatedHistogramReservoir's Snapshot to this one. Both reservoirs must have same bucket definitions. This will rescale both snapshots if needed.
         *
         * @param other EstimatedHistogramReservoirSnapshot with identical bucket definition (offsets and length)
         */
        public void add(Snapshot other)
        {
            if (!(other instanceof EstimatedHistogramReservoirSnapshot))
            {
                throw new IllegalStateException("Unable to add other types of Snapshot than another DecayingEstimatedHistogramReservoir");
            }

            EstimatedHistogramReservoirSnapshot snapshot = (EstimatedHistogramReservoirSnapshot) other;

            if (decayingBuckets.length != snapshot.decayingBuckets.length)
            {
                throw new IllegalStateException("Unable to merge two DecayingEstimatedHistogramReservoirs with different bucket sizes");
            }

            // Check bucketOffsets
            for (int i = 0; i < bucketOffsets.length; i++)
            {
                if (bucketOffsets[i] != snapshot.bucketOffsets[i])
                {
                    throw new IllegalStateException("Merge is only supported with equal bucketOffsets");
                }
            }

            // We need to rescale the reservoirs to the same landmark
            if (snapshot.snapshotLandmark < snapshotLandmark)
            {
                rescaleArray(snapshot.decayingBuckets, (snapshotLandmark - snapshot.snapshotLandmark));
            }
            else if (snapshot.snapshotLandmark > snapshotLandmark)
            {
                rescaleArray(decayingBuckets, (snapshot.snapshotLandmark - snapshotLandmark));
                this.snapshotLandmark = snapshot.snapshotLandmark;
            }

            // Now merge the buckets
            for (int i = 0; i < snapshot.decayingBuckets.length; i++)
            {
                decayingBuckets[i] += snapshot.decayingBuckets[i];
                values[i] += snapshot.values[i];
            }

            this.count += snapshot.count;
        }

        private void rescaleArray(long[] decayingBuckets, long landMarkDifference)
        {
            final double rescaleFactor = Math.exp((landMarkDifference / 1000.0) / MEAN_LIFETIME_IN_S);
            for (int i = 0; i < decayingBuckets.length; i++)
            {
                decayingBuckets[i] = Math.round(decayingBuckets[i] / rescaleFactor);
            }
        }

        public void rebaseReservoir()
        {
            this.reservoir.rebase(this);
        }
    }

    /**
     * Like {@link EstimatedHistogramReservoirSnapshot}, represents a snapshot of a given histogram reservoir.
     * 
     * Unlike {@link EstimatedHistogramReservoirSnapshot}, this only copies and supports operations based on the
     * decaying buckets from the source reservoir. (ex. percentiles, min, max) It also does not support snapshot 
     * merging or rebasing on the source reservoir.
     */
    private static class DecayingBucketsOnlySnapshot extends AbstractSnapshot
    {
        private final long count;

        public DecayingBucketsOnlySnapshot(DecayingEstimatedHistogramReservoir reservoir)
        {
            super(reservoir);

            int length = reservoir.size();
            double rescaleFactor = reservoir.forwardDecayWeight(reservoir.clock.now());

            for (int i = 0; i < length; i++)
            {
                this.decayingBuckets[i] = Math.round(reservoir.bucketValue(i, true) / rescaleFactor);
            }

            this.count = count();
        }

        @Override
        public long[] getValues()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            return Ints.saturatedCast(count);
        }
    }

    static class Range
    {
        public final long min;
        public final long max;

        public Range(long min, long max)
        {
            this.min = min;
            this.max = max;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Range that = (Range) o;
            return min == that.min &&
                   max == that.max;
        }

        public int hashCode()
        {
            return Objects.hash(min, max);
        }

        @Override
        public String toString()
        {
            return "[" + min + ',' + max + ']';
        }
    }
}
