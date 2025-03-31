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
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.LongBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;
import java.util.function.LongBinaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;

import static com.google.common.collect.ImmutableList.of;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.UNSAFE;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_DECAYING_HISTOGRAM_RESET_INTERVAL_MS;

/**
 * A decaying histogram reservoir where values collected during each minute will be twice as significant as the values
 * collected in the previous minute. Measured values are collected in variable sized buckets, using small buckets in the
 * lower range and larger buckets in the upper range. Use this histogram when you want to know if the distribution of
 * the underlying data stream has changed recently and you want high resolution on values in the lower range.
 * <p/>
 * The histogram use forward decay [1] to make recent values more significant. The forward decay factor will be doubled
 * every minute (half-life time set to 60 seconds) [2]. The forward decay landmark is reset every 30 minutes (or at
 * first read/update after 30 minutes). The 30 minute rescale interval is used based on the assumption that in an
 * extreme case we would have to collect a metric 1M times for a single bucket each second. By the
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
 * To reduce contention the buckets are stored in the thread local variables, so each thread has its own copy of the
 * buckets. The buckets are updated in a thread local variable and then merged into the main bucket array when a
 * corresponding thread is dead (phantom references are used to detect this situation and to transfer the values).
 * The reservoir main buckets are represented as a single long array, where the first half of the array contains the decaying
 * buckets and the second half contains the estimated buckets. The decaying buckets are updated with the forward decay
 * applied, while the estimated buckets are updated with the raw values.
 * The readers use the optimistic locking to read the buckets, which means that they will read the buckets without locking them.
 * If the buckets are updated while they are being read, the reader will retry the read operation until it succeeds.
 * <p/>
 * <ul>
 *   <li>[1]: http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf</li>
 *   <li>[2]: https://en.wikipedia.org/wiki/Half-life</li>
 *   <li>[3]: https://github.com/dropwizard/metrics/blob/v3.1.2/metrics-core/src/main/java/com/codahale/metrics/ExponentiallyDecayingReservoir.java</li>
 *   <li>[4]: https://psy-lob-saw.blogspot.com/2013/06/java-concurrent-counters-by-numbers.html</li>
 * </ul>
 *
 * @see ExponentiallyDecayingReservoir
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
    public static final int MAX_BUCKET_COUNT = 237;
    public static final boolean DEFAULT_ZERO_CONSIDERATION = false;

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

    private final long[] bucketOffsets;

    /**
     * This constant defines the half-life of the data in the histogram. This is the exponential decay model, and the half-life is
     * the period over which the weight (or influence) of a data point decreases by 50%. This parameter controls the rate at which
     * older observations lose significance.
     */
    public static final long HALF_TIME_IN_S = 60L;
    /**
     * Given {@code HALF_TIME_IN_S} is {@code 60} seconds, the mean lifetime calculates to approximately 86.6 seconds.
     * This value provides an average duration over which a data point meaningfully affects the histogram before
     * its weight diminishes substantially.
     */
    public static final double MEAN_LIFETIME_IN_S = HALF_TIME_IN_S / Math.log(2.0);
    /**
     * The rescaling of decaying buckets every 30 minutes serves several key purposes:
     * <ul>
     *     <li>Prevent overflow of the decaying buckets</li>
     *     <li>Prevent loss of precision in the decaying buckets</li>
     *     <li>Maintain a consistent forward decay landmark</li>
     *     <li>Efficient decay computation</li>
     * </ul>
     */
    public static final long LANDMARK_RESET_INTERVAL_IN_NS = TimeUnit.MILLISECONDS.toNanos(CASSANDRA_DECAYING_HISTOGRAM_RESET_INTERVAL_MS.getInt());

    private static final ReferenceQueue<Object> retirementPhantomRefsQueue = new ReferenceQueue<>();
    private static final Set<PhantomReference<Object>> phantomReferences = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Interruptible releaseThread = executorFactory().infiniteLoop("DecayingBuckets-Releaser",
                                                                                     DecayingEstimatedHistogramReservoir::release,
                                                                                     UNSAFE);

    // Set of all decaying buckets thread locals, used to release the thread local when the thread is dead or to get the values.
    private static final Set<WeakReference<DecayingEstimatedHistogramReservoir>> allReservoirRefs = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final Object rescaleMutex = new Object();
    private static final ScheduledFuture<?> rescalerThread = ScheduledExecutors.scheduledTasks
                                                             .scheduleWithFixedDelay(DecayingEstimatedHistogramReservoir::rescale,
                                                                                     LANDMARK_RESET_INTERVAL_IN_NS,
                                                                                     LANDMARK_RESET_INTERVAL_IN_NS,
                                                                                     TimeUnit.NANOSECONDS);

    /** Lock is used to synchronize access to the bucket array. Only one thread can update the bucket array at a time. */
    private final StampedLock bucketsStampedLock = new StampedLock();
    private final DecayingEstimatedBuckets decayingEstimatedBuckets;
    private final Set<BucketsThreadLocal> bucketsThreadLocals = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final FastThreadLocal<BucketsThreadLocal> bucketsThreadLocal = new FastThreadLocal<>()
    {
        @Override
        protected BucketsThreadLocal initialValue()
        {
            BucketsThreadLocal holder = new BucketsThreadLocal(DecayingEstimatedHistogramReservoir.this.size());
            bucketsThreadLocals.add(holder);
            phantomReferences.add(new BucketsPhantomReference(retirementPhantomRefsQueue, holder::release));
            return holder;
        }

        @Override
        protected void onRemoval(BucketsThreadLocal hodler)
        {
            hodler.release();
        }
    };

    // Wrapper around System.nanoTime() to simplify unit testing.
    private final MonotonicClock clock;

    /**
     * Construct a decaying histogram with default number of buckets and without considering zeroes.
     */
    public DecayingEstimatedHistogramReservoir()
    {
        this(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, MonotonicClock.Global.approxTime);
    }

    /**
     * Construct a decaying histogram with default number of buckets.
     *
     * @param considerZeroes when true, 0-value measurements in a separate bucket, otherwise they will be collected in
     *                       same bucket as 1-value measurements
     */
    public DecayingEstimatedHistogramReservoir(boolean considerZeroes)
    {
        this(considerZeroes, DEFAULT_BUCKET_COUNT, MonotonicClock.Global.approxTime);
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
        this(considerZeroes, bucketCount, MonotonicClock.Global.approxTime);
    }

    @VisibleForTesting
    public DecayingEstimatedHistogramReservoir(MonotonicClock clock)
    {
        this(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, clock);
    }

    @VisibleForTesting
    public DecayingEstimatedHistogramReservoir(boolean considerZeroes, int bucketCount, MonotonicClock clock)
    {
        assert bucketCount <= MAX_BUCKET_COUNT : "bucket count cannot exceed: " + MAX_BUCKET_COUNT;

        if (bucketCount == DEFAULT_BUCKET_COUNT)
        {
            if (considerZeroes)
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

        this.clock = clock;
        this.decayingEstimatedBuckets = new DecayingEstimatedBuckets(bucketsStampedLock, size(), clock.now());
        allReservoirRefs.add(new WeakReference<>(this));
    }

    /**
     * Increments the count of the bucket closest to n, rounding UP.
     *
     * @param value the data point to add to the histogram
     */
    public void update(long value)
    {
        int index = findIndex(bucketOffsets, value);
        bucketsThreadLocal.get().update(index, clock.now());
    }

    public static void release() throws InterruptedException
    {
        Object ref = retirementPhantomRefsQueue.remove(1000);
        if (ref instanceof MetricCleaner)
        {
            ((MetricCleaner) ref).clean();
            phantomReferences.remove(ref);
        }
        allReservoirRefs.removeIf(o -> o.get() == null);
    }

    public static void rescale()
    {
        synchronized (rescaleMutex)
        {
            Iterator<WeakReference<DecayingEstimatedHistogramReservoir>> iterator = allReservoirRefs.iterator();
            while (iterator.hasNext())
            {
                DecayingEstimatedHistogramReservoir reservoir = iterator.next().get();
                if (reservoir == null)
                    continue;
                reservoir.rescaleReservoir();
            }
            logger.info("Rescaled decaying histogram buckets with configured interval of {} ms",
                        TimeUnit.NANOSECONDS.toMillis(LANDMARK_RESET_INTERVAL_IN_NS));
        }
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

    private static long forwardDecayWeight(long decayLandmark, long now)
    {
        return Math.round(Math.exp(TimeUnit.NANOSECONDS.toSeconds(now - decayLandmark) / MEAN_LIFETIME_IN_S));
    }

    private static void decay(LongBuffer buffer, long decayLandmark, long now)
    {
        for (int i = buffer.position(); i < buffer.limit(); i++)
            buffer.put(i, Math.round((float) buffer.get(i) / forwardDecayWeight(decayLandmark, now)));
    }

    private void rescaleReservoir()
    {
        long now = clock.now();
        if (now - decayingEstimatedBuckets.decayLandmark > LANDMARK_RESET_INTERVAL_IN_NS)
            decayingEstimatedBuckets.rescale(bucketsThreadLocals, now);
    }

    /**
     * @return the decaying buckets with the forward decay applied.
     */
    private DecayingEstimatedArray snapshotBuckets()
    {
        return decayingEstimatedBuckets.snapshot(bucketsThreadLocals);
    }

    @VisibleForTesting
    protected Set<BucketsThreadLocal> getBucketsThreadLocals()
    {
        return bucketsThreadLocals;
    }

    /**
     * Returns the logical number of buckets where recorded values are stored. The actual number of physical buckets
     * is size() * stripeCount()
     *
     * This method does not return the number of recorded values as suggested by the {@link Reservoir} interface.
     *
     * @return the number of buckets
     */
    public int size()
    {
        return bucketOffsets.length + 1;
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
        return new EstimatedHistogramReservoirSnapshot(this);
    }

    @Override
    public Snapshot getPercentileSnapshot()
    {
        return new DecayingBucketsOnlySnapshot(this);
    }

    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    @VisibleForTesting
    boolean isOverflowed()
    {
        return snapshotBuckets().decaying()[bucketOffsets.length] > 0;
    }

    @VisibleForTesting
    public void clear()
    {
        long stamp = bucketsStampedLock.writeLock();
        try
        {
            decayingEstimatedBuckets.updateExclusive((index, value) -> 0, (index, value) -> 0, clock.now());
        }
        finally
        {
            bucketsStampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Replaces current internal values with the given one from a Snapshot. This method is NOT thread safe, values
     * added at the same time to this reservoir using methods such as update may lose their data
     */
    private void rebase(EstimatedHistogramReservoirSnapshot snapshot)
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

        long stamp = bucketsStampedLock.writeLock();
        try
        {
            decayingEstimatedBuckets.updateExclusive((index, value) -> snapshot.decayingBuckets[(int) index],
                                                     (index, value) -> snapshot.estimatedBuckets[(int) index],
                                                     snapshot.snapshotLandmark);
        }
        finally
        {
            bucketsStampedLock.unlockWrite(stamp);
        }
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, of(releaseThread));
        rescalerThread.cancel(false);
    }

    private static abstract class AbstractSnapshot extends Snapshot
    {
        protected final long[] decayingBuckets;
        protected final long[] estimatedBuckets;
        protected long snapshotLandmark;
        protected final long[] bucketOffsets;

        AbstractSnapshot(DecayingEstimatedHistogramReservoir reservoir)
        {
            DecayingEstimatedArray snapshot = reservoir.snapshotBuckets();
            this.decayingBuckets = snapshot.decaying();
            this.estimatedBuckets = snapshot.estimated();
            this.snapshotLandmark = snapshot.landmark();
            decay(LongBuffer.wrap(decayingBuckets), snapshotLandmark, reservoir.clock.now());
            this.bucketOffsets = reservoir.bucketOffsets;
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
        private long count;
        private final DecayingEstimatedHistogramReservoir reservoir;

        public EstimatedHistogramReservoirSnapshot(DecayingEstimatedHistogramReservoir reservoir)
        {
            super(reservoir);
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
            return estimatedBuckets;
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
                estimatedBuckets[i] += snapshot.estimatedBuckets[i];
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
     * <p>
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
            this.count = count();
        }

        @Override
        public long[] getValues()
        {
            return decayingBuckets;
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

    interface MetricCleaner
    {
        void clean();
    }

    private static class BucketsPhantomReference extends PhantomReference<Object> implements MetricCleaner
    {
        private final MetricCleaner cleaner;

        public BucketsPhantomReference(ReferenceQueue<? super Object> q, MetricCleaner cleaner)
        {
            super(Thread.currentThread(), q);
            this.cleaner = cleaner;
        }

        public void clean()
        {
            cleaner.clean();
        }
    }

    /**
     * Writes are exclusive to the thread-local buckets, so we can use a single updater for all threads.
     * Readers will see a consistent view of the buckets and could be blocked for a while.
     * <p>
     * The class is aslso being tracked by a phantom reference queue to release the accumulated buckets when the thread is dead.
     */
    protected class BucketsThreadLocal
    {
        // try to use int[] instead of long[] to reduce memory usage, and move to the sum array when overflow
        private final AtomicReference<DecayingArray> decayingRef;
        private final long[] estimated;
        private volatile boolean writing;

        public BucketsThreadLocal(int size)
        {
            this.decayingRef = new AtomicReference<>(new DecayingArray(size, decayingEstimatedBuckets.decayLandmark));
            this.estimated = new long[size];
        }

        public void update(int index, long now)
        {
            // This is only called by the thread that owns the thread local, so we don't need to worry about contention.
            // Once the rescaling has occurred, we need to flush the values to the decayingBucket and report that the values are no longer in use.
            writing = true;
            try
            {
                DecayingArray decaying = decayingRef.get();
                if (decaying.decayLandmark != decayingEstimatedBuckets.decayLandmark)
                    decayingEstimatedBuckets.flush(this, decaying);

                decayingRef.get().update(index, now);
                estimated[index]++;
            }
            finally
            {
                writing = false;
            }
        }

        public void release()
        {
            // The release method could be called by the FastThreadLocal#onRemoval or by the PhantomReference queue.
            // We need to make sure we transfer the values to the decayingBuckets only once.
            // There is also no need for the BucketsThreadLocal#inUse check since the thread is dead and no one will update the values.
            if (!bucketsThreadLocals.contains(this))
                return;
            long stamp = bucketsStampedLock.writeLock();
            try
            {
                if (!bucketsThreadLocals.remove(this))
                    return;
                DecayingArray locDecaying = decayingRef.get();
                // The same write lock is used to flush the values to the decaying buckets and to rescale the values,
                // so the landmark is safe to use from the decaying array.
                decayingEstimatedBuckets.updateExclusive((index, value) -> locDecaying.data[(int) index] + value,
                                                         (index, value) -> estimated[(int) index] + value,
                                                         locDecaying.decayLandmark);
            }
            finally
            {
                bucketsStampedLock.unlockWrite(stamp);
            }
        }
    }

    private static class DecayingEstimatedBuckets
    {
        private final int size;
        /** Lock to protect the decaying {@code buckets}. Only one thread can update the buckets at a time. */
        private final StampedLock stampedLock;
        private final ConcurrentLinkedQueue<DecayingArray> decayingPending = new ConcurrentLinkedQueue<>();
        /**
         * The buckets array is used to store the decaying and estimated buckets, we can use the same array for both.
         * The actual size of the array is twice the size of the decaying buckets or the estimated buckets.
         * <p>
         * The first half is used for the decaying buckets and the second half is used for the estimated buckets.
         */
        private final long[] buckets;
        private volatile long decayLandmark;

        public DecayingEstimatedBuckets(StampedLock shared, int size, long now)
        {
            this.size = size;
            this.buckets = new long[size * 2];
            this.decayLandmark = now;
            this.stampedLock = shared;
        }

        public void rescale(Set<BucketsThreadLocal> locals, long now)
        {
            if (now - decayLandmark <= LANDMARK_RESET_INTERVAL_IN_NS)
                return;
            long stamp = stampedLock.writeLock();
            try
            {
                long previousDecayLandmark = decayLandmark;
                decayLandmark = now;

                // The list of thread locals should be fetched after the lock is taken and decayLandmark is updated.
                for (BucketsThreadLocal local : locals)
                {
                    while (true)
                    {
                        DecayingArray prev = local.decayingRef.get();
                        // Skip the thread local if it was created after the decayLandmark was updated.
                        if (prev.decayLandmark == now)
                            break;
                        if (local.decayingRef.compareAndSet(prev, new DecayingArray(size, now)))
                        {
                            // We successfully switched the thread local to the new decayLandmark, wait for the thread to finish updating.
                            while (local.writing)
                                LockSupport.parkNanos(50);
                            decayingPending.offer(prev);
                            break;
                        }
                    }
                }
                flushPendingExclusive();
                DecayingEstimatedHistogramReservoir.decay(LongBuffer.wrap(buckets, 0, size), previousDecayLandmark, now);
            }
            finally
            {
                stampedLock.unlockWrite(stamp);
            }
        }

        public void updateExclusive(LongBinaryOperator decayingOp, LongBinaryOperator estimatedOp, long decayLandmark)
        {
            assert stampedLock.isWriteLocked();
            this.decayLandmark = decayLandmark;
            for (int i = 0; i < size; i++)
            {
                buckets[i] = decayingOp.applyAsLong(i, buckets[i]);
                buckets[size + i] = estimatedOp.applyAsLong(i, buckets[size + i]);
            }
        }

        /**
         * Used only by a thread-local writer to flush the values to the buffer.
         */
        public void flush(BucketsThreadLocal local, DecayingArray decaying)
        {
            long stamp = stampedLock.tryWriteLock();
            if (stamp > 0)
            {
                try
                {
                    boolean success = local.decayingRef.compareAndSet(decaying, new DecayingArray(size, decayLandmark));
                    assert success : "The thread local was updated by another thread";
                    decayingPending.offer(decaying);
                    flushPendingExclusive();
                }
                finally
                {
                    stampedLock.unlockWrite(stamp);
                }
            }
            else
            {
                boolean success = local.decayingRef.compareAndSet(decaying, new DecayingArray(size, decayLandmark));
                // If the CAS failed, the thread local was updated by the rescale thread and all the values were already flushed.
                if (success)
                    decayingPending.offer(decaying);
            }
        }

        public DecayingEstimatedArray snapshot(Set<BucketsThreadLocal> locals)
        {
            long[] decaying = new long[size];
            long[] estimated = new long[size];
            long resultLandmark = this.decayLandmark;
            long stamp;
            do
            {
                stamp = stampedLock.tryWriteLock();
                if (stamp > 0)
                {
                    try
                    {
                        flushPendingExclusive();
                    }
                    finally
                    {
                        stampedLock.unlockWrite(stamp);
                    }
                }
                // If the write lock is not available, we need to use the optimistic read lock.
                // This will allow us to read the buckets without blocking other threads.
                // We need to make sure that the buckets and the list of thread locals are consistent,
                // while we are reading them, so we won't miss any updates or overlap with thread locals being released.
                stamp = stampedLock.tryOptimisticRead();
                if (stamp == 0)
                {
                    LockSupport.parkNanos(this, 100);
                    continue;
                }
                Arrays.fill(decaying, 0);
                Arrays.fill(estimated, 0);
                resultLandmark = this.decayLandmark;
                for (int i = 0; i < size; i++)
                {
                    decaying[i] = buckets[i];
                    estimated[i] = buckets[size + i];
                }
                for (BucketsThreadLocal local : locals)
                {
                    DecayingArray decayingLoc = local.decayingRef.get();
                    long[] estimatedLoc = local.estimated;
                    for (int i = 0; i < size; i++)
                    {
                        decaying[i] += decayingLoc.data[i];
                        estimated[i] += estimatedLoc[i];
                    }
                }
            } while (!stampedLock.validate(stamp));

            return new DecayingEstimatedArray(decaying, estimated, resultLandmark);
        }

        private void flushPendingExclusive()
        {
            assert stampedLock.isWriteLocked();
            DecayingArray arr;
            while ((arr = decayingPending.poll()) != null)
            {
                // We need to flush the values to the decaying buckets only, which is a half of the buckets array.
                for (int i = 0; i < arr.data.length; i++)
                    buckets[i] += arr.data[i];
            }
        }
    }

    private static class DecayingEstimatedArray
    {
        private final long[] decaying;
        private final long[] estimated;
        private final long decayLandmark;

        public DecayingEstimatedArray(long[] decaying, long[] estimated, long decayLandmark)
        {
            this.decaying = decaying;
            this.estimated = estimated;
            this.decayLandmark = decayLandmark;
        }

        public long[] estimated()
        {
            return estimated;
        }

        public long[] decaying()
        {
            return decaying;
        }

        public long landmark()
        {
            return decayLandmark;
        }
    }

    /**
     * This class is used to store the decaying buckets in a thread local variable along with the landmark.
     * No concurrency issues are expected here, as the thread local is only used by one thread at a time.
     */
    private static class DecayingArray
    {
        private final long[] data;
        private final long decayLandmark;
        /**
         * As SampledClock is used to register the last time the decay weight was sampled,
         * and the precision of the clock is not guaranteed to be nanoseconds (approximately 2ms),
         * we can avoid calculating the decay weight for every sample and instead use the last calculated weight.
         */
        private long lastSampledClock;
        private long lastDecayedWeight;

        public DecayingArray(int size, long decayLandmark)
        {
            this.data = new long[size];
            this.decayLandmark = decayLandmark;
        }

        public void update(int index, long now)
        {
            if (lastSampledClock != now)
            {
                lastSampledClock = now;
                lastDecayedWeight = forwardDecayWeight(decayLandmark, now);
            }
            data[index] += lastDecayedWeight;
        }
    }
}
