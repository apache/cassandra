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
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * To reduce contention each logical bucket is striped accross a configurable number of stripes (default: 2). Threads are
 * assigned to specific stripes. In addition, logical buckets are distributed across the physical storage to reduce conention
 * when logically adjacent buckets are updated. See CASSANDRA-15213.
 * <p/>
 * <ul>
 *   <li>[1]: http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf</li>
 *   <li>[2]: https://en.wikipedia.org/wiki/Half-life</li>
 *   <li>[3]: https://github.com/dropwizard/metrics/blob/v3.1.2/metrics-core/src/main/java/com/codahale/metrics/ExponentiallyDecayingReservoir.java</li>
 * </ul>
 *
 * @see ExponentiallyDecayingReservoir
 */
public class DecayingEstimatedHistogramReservoir implements CassandraReservoir
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

    // Leading 64-byte padding prepended to the physical bucket arrays so that no live bucket shares
    // a cache line with the array-length field in the object header
    private static final int BUCKET_INDEX_OFFSET = 8;

    // The offsets used with a default sized bucket array without a separate bucket for zero values.
    public static final long[] DEFAULT_WITHOUT_ZERO_BUCKET_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, false);

    // The offsets used with a default sized bucket array with a separate bucket for zero values.
    public static final long[] DEFAULT_WITH_ZERO_BUCKET_OFFSETS = EstimatedHistogram.newOffsets(DEFAULT_BUCKET_COUNT, true);

    private static final int TABLE_BITS = 4;
    private static final int TABLE_MASK = -1 >>> (32 - TABLE_BITS);
    private static final float[] LOG2_TABLE = computeTable(TABLE_BITS);
    private static final float log2_12_recp = (float) (1d / slowLog2(1.2d));
    // Floor of the base 1.2 logarithm, indexed by (highest set bit, top TABLE_BITS bits below it).
    // This table allows to do one short load instead of a float load, a float add, a float multiply and a float to int conversion.
    // Entries reach 243, so a short is enough.
    private static final short[] LOG12_TABLE = computeLog12Table(TABLE_BITS);

    private static final long WEIGHT_NOT_COMPUTED = Long.MIN_VALUE;

    private static float[] computeTable(int bits)
    {
        float[] table = new float[1 << bits];
        for (int i = 1 ; i < 1<<bits ; ++i)
            table[i] = (float) slowLog2(ratio(i, bits));
        return table;
    }

    private static short[] computeLog12Table(int bits)
    {
        short[] table = new short[64 << bits];
        for (int highestBitPosition = 0 ; highestBitPosition < 64 ; ++highestBitPosition)
            for (int i = 0 ; i < 1<<bits ; ++i)
                // preserve the expression findIndex used to evaluate, to keep the original results
                table[(highestBitPosition << bits) | i] = (short) (int) ((LOG2_TABLE[i] + highestBitPosition) * log2_12_recp);
        return table;
    }

    /** Equivalent to {@code (int) fastLog12(v)}, from a table. */
    private static int fastLog12Floor(long v)
    {
        v = max(v, 1);
        int highestBitPosition = 63 - Long.numberOfLeadingZeros(v);
        long rotated = Long.rotateRight(v, highestBitPosition - TABLE_BITS);
        return LOG12_TABLE[(highestBitPosition << TABLE_BITS) | (int) (rotated & TABLE_MASK)];
    }

    /**
     * Not on the update path any more -- {@link #findIndex} reads {@link #LOG12_TABLE} instead. Kept because that
     * table is built from this expression and tested against it, so it is the reference the table must match.
     */
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
    // The unpadded number of physical slots, i.e. (bucketOffsets.length + 1) * nStripes
    private final int stripedBucketCount;
    // id identifying this reservoir in the shared per-thread update buffers
    private final int reservoirId;

    private static final AtomicReferenceFieldUpdater<DecayingEstimatedHistogramReservoir, DecayingBuckets> decayingBucketsUpdater =
        AtomicReferenceFieldUpdater.newUpdater(DecayingEstimatedHistogramReservoir.class, DecayingBuckets.class, "decayingBuckets");

    // decayingBuckets and buckets are one element longer than bucketOffsets -- the last element is values greater than the last offset
    private volatile DecayingBuckets decayingBuckets;
    private final AtomicLongArray buckets;

    public static final long HALF_TIME_IN_S = 60L;
    public static final double MEAN_LIFETIME_IN_S = HALF_TIME_IN_S / Math.log(2.0);
    public static final long LANDMARK_RESET_INTERVAL_IN_S = TimeUnit.MINUTES.toSeconds(30L);
    // Wrapper around System.nanoTime() to simplify unit testing.
    private final MonotonicClock clock;
    /** Interval in seconds to reset the forward decay landmark for the decaying histograms. Default {@code 30 mins}. */
    private final long landmarkResetIntervalInSec;

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
        this(considerZeroes, bucketCount, stripes, clock, LANDMARK_RESET_INTERVAL_IN_S);
    }

    @VisibleForTesting
    public DecayingEstimatedHistogramReservoir(boolean considerZeroes,
                                               int bucketCount,
                                               int stripes,
                                               MonotonicClock clock,
                                               long landmarkResetIntervalInSec)
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
        this.clock = clock;
        stripedBucketCount = (bucketOffsets.length + 1) * nStripes;
        buckets = new AtomicLongArray(stripedBucketCount + BUCKET_INDEX_OFFSET);
        decayingBuckets = new DecayingBuckets(clock.nowInSec());
        this.landmarkResetIntervalInSec = landmarkResetIntervalInSec;
        int distributionPrime = 1;
        for (int prime : DISTRIBUTION_PRIMES)
        {
            if (stripedBucketCount % prime != 0)
            {
                distributionPrime = prime;
                break;
            }
        }
        this.distributionPrime = distributionPrime;

        this.reservoirId = HistogramUpdateBuffers.registerReservoir(this);
        if (reservoirId >= 0)
        {
            int idToRecycle = reservoirId;
            ThreadLocalMetrics.destroyWhenUnreachable(this, () -> HistogramUpdateBuffers.recycleReservoir(idToRecycle));
        }
    }

    /**
     * Increments the count of the bucket closest to n, rounding UP.
     *
     * @param value the data point to add to the histogram
     */
    public void update(long value)
    {
        int index = findIndex(bucketOffsets, value);
        long nowInSec = clock.nowInSec();

        if (reservoirId < 0)
        {
            // no buffer id could be allocated for this reservoir, fall back to updating the shared arrays directly
            rescaleIfNeeded(nowInSec).update(index, nowInSec);
            updateBucket(buckets, index, 1);
            return;
        }

        HistogramUpdateBuffers.append(reservoirId, index, nowInSec);
    }

    /**
     * Applies one drained batch of buffered updates to this reservoir.
     * Called by {@link HistogramUpdateBuffers} from the owning, snapshotting, or dying thread.
     *
     * @param entries         the drained batch, as (packed update, occurrences) pairs
     * @param from            index of this reservoir's first entry, inclusive
     * @param to              index of this reservoir's last entry, exclusive
     * @param ownerThreadId   the thread the buffered updates were recorded on
     * @param baselineSeconds the second the entries' recorded time deltas are relative to
     * @param weightsCache    weight cache, reset and owned by this call for its duration (we re-use it to avoid allocations)
     */
    void applyBufferedUpdates(long[] entries, int from, int to, long ownerThreadId, long baselineSeconds, long[] weightsCache)
    {
        if (from >= to)
            return;

        DecayingBuckets decaying = rescaleIfNeeded(clock.nowInSec());
        long landmarkSeconds = decaying.decayLandmarkInSec;
        int stripe = getStripe(ownerThreadId);
        Arrays.fill(weightsCache, WEIGHT_NOT_COMPUTED);

        int i = from;
        long entryCountPair = entries[i];
        int entry = HistogramUpdateBuffers.entryOf(entryCountPair);
        int bucketId = HistogramUpdateBuffers.bucketOf(entry);
        while (i < to)
        {
            int currentBucketId = bucketId;
            long totalCountPerBucket = 0;
            long weightedCountPerBucket = 0;
            do
            {
                int timeDeltaSec = HistogramUpdateBuffers.timeDeltaOf(entry);
                long entryWeight = weightsCache[timeDeltaSec];
                if (entryWeight == WEIGHT_NOT_COMPUTED)
                {
                    long seconds = baselineSeconds + timeDeltaSec;
                    entryWeight = Math.round(Math.exp((seconds - landmarkSeconds) / MEAN_LIFETIME_IN_S));
                    weightsCache[timeDeltaSec] = entryWeight;
                }
                long countPerEntry = HistogramUpdateBuffers.countOf(entryCountPair);
                totalCountPerBucket += countPerEntry;
                weightedCountPerBucket += countPerEntry * entryWeight;

                if (++i == to)
                    break;
                entryCountPair = entries[i];
                entry = HistogramUpdateBuffers.entryOf(entryCountPair);
                bucketId = HistogramUpdateBuffers.bucketOf(entry);
            }
            while (bucketId == currentBucketId);

            int stripedIndex = stripedIndex(currentBucketId, stripe);
            buckets.addAndGet(stripedIndex, totalCountPerBucket);
            decaying.decayBuckets.addAndGet(stripedIndex, weightedCountPerBucket);
        }
    }

    public void updateBucket(AtomicLongArray buckets, int index, long value)
    {
        int stripe = getStripe(Thread.currentThread().getId());
        buckets.addAndGet(stripedIndex(index, stripe), value);
    }

    private int getStripe(long threadId)
    {
        return (int) (threadId & (nStripes - 1));
    }

    public int stripedIndex(int offsetIndex, int stripe)
    {
        return BUCKET_INDEX_OFFSET + (((offsetIndex * nStripes + stripe) * distributionPrime) % stripedBucketCount);
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

        int firstCandidate = max(0, min(bucketOffsets.length - 1, fastLog12Floor(value) - offset));
        return value <= bucketOffsets[firstCandidate] ? firstCandidate : firstCandidate + 1;
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
        HistogramUpdateBuffers.relaxedFlush();
        return new EstimatedHistogramReservoirSnapshot(this);
    }

    @Override
    public Snapshot getPercentileSnapshot()
    {
        HistogramUpdateBuffers.relaxedFlush();
        return new DecayingBucketsOnlySnapshot(this);
    }

    private DecayingBuckets getDecayingBuckets()
    {
        return rescaleIfNeeded(clock.nowInSec());
    }

    @Override
    public long[] buckets(int length)
    {
        if (length == bucketOffsets.length)
            return bucketOffsets;
        return EstimatedHistogram.newOffsets(length, bucketOffsets[0] == 0);
    }

    @Override
    public BucketStrategy bucketStrategy()
    {
        return bucketOffsets[0] == 0 ? BucketStrategy.exp_12 : BucketStrategy.exp_12_nozero;
    }

    /**
     * @return true if this histogram has overflowed -- that is, a value larger than our largest bucket could bound was added
     */
    @VisibleForTesting
    boolean isOverflowed()
    {
        HistogramUpdateBuffers.flush();
        return bucketValue(bucketOffsets.length, getDecayingBuckets().decayBuckets) > 0;
    }

    private long bucketValue(int index, AtomicLongArray buckets)
    {
        long val = 0;
        for (int stripe = 0; stripe < nStripes; stripe++)
            val += buckets.get(stripedIndex(index, stripe));

        return val;
    }

    @VisibleForTesting
    long stripedBucketValue(int i, boolean withDecay)
    {
        HistogramUpdateBuffers.flush();
        return withDecay ? getDecayingBuckets().decayBuckets.get(i) : buckets.get(i);
    }

    private DecayingBuckets rescaleIfNeeded(long nowInSec)
    {
        DecayingBuckets buckets = decayingBuckets;
        while (nowInSec - buckets.decayLandmarkInSec > landmarkResetIntervalInSec)
        {
            double rescaleFactor = buckets.forwardDecayWeight(nowInSec);
            DecayingBuckets newBuckets = new DecayingBuckets(nowInSec);
            for (int i = 0; i < buckets.decayBuckets.length(); i++)
                newBuckets.decayBuckets.set(i, Math.round(buckets.decayBuckets.get(i) / rescaleFactor));

            boolean success = decayingBucketsUpdater.compareAndSet(this, buckets, newBuckets);
            if (success)
                return newBuckets;
            buckets = decayingBuckets;
        }
        return buckets;
    }

    @VisibleForTesting
    public void clear()
    {
        HistogramUpdateBuffers.flush();
        for (int i = 0; i < buckets.length(); i++)
            buckets.set(i, 0L);

        decayingBucketsUpdater.set(this, new DecayingBuckets(clock.nowInSec()));
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

        DecayingBuckets newDecayingBuckets = new DecayingBuckets(snapshot.snapshotLandmarkInSec);
        for (int i = 0; i < size(); i++)
        {
            // set rebased values in the first stripe and clear out all other data
            newDecayingBuckets.decayBuckets.set(stripedIndex(i, 0), snapshot.decayingBuckets[i]);
            buckets.set(stripedIndex(i, 0), snapshot.values[i]);
            for (int stripe = 1; stripe < nStripes; stripe++)
            {
                newDecayingBuckets.decayBuckets.set(stripedIndex(i, stripe), 0);
                buckets.set(stripedIndex(i, stripe), 0);
            }
        }
        decayingBucketsUpdater.set(this, newDecayingBuckets);
    }

    /**
     * The DecayingBuckets class provides a facility to prevent destruction of the reservoir internal state from
     * occurring. The root cause of CASSANDRA-19365 is lack of synchronization between udpates and rescaling.
     * This class lets us retain lack of synchronization (for performance reasons) while changing the race condition
     * effect from destructive to benign.
     * <p>
     * DecayingBuckets class encapsulates the decaying buckets and the decay landmark together. The decay landmark is
     * immutable and so the values in the buckets are consistent with the landmark at any given time. In particular,
     * every update is always given a weight that's consistent with weights given to other updates of the same buckets.
     * <p>
     * Additionally, the class allows creating snapshots of the reservoir without risking that the snapshot uses data
     * from a half-rescaled reservoir.
     */
    private class DecayingBuckets
    {
        private final long decayLandmarkInSec;
        private final AtomicLongArray decayBuckets;

        public DecayingBuckets(long decayLandmarkInSec)
        {
            this.decayLandmarkInSec = decayLandmarkInSec;
            this.decayBuckets = new AtomicLongArray(stripedBucketCount + BUCKET_INDEX_OFFSET);
        }

        public void update(int index, long nowInSec)
        {
            updateBucket(decayBuckets, index, forwardDecayWeight(nowInSec));
        }

        private long forwardDecayWeight(long nowInSec)
        {
            return Math.round(Math.exp((nowInSec - decayLandmarkInSec) / MEAN_LIFETIME_IN_S));
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
            double sum = 0;
            for (int i = 0; i < lastBucket; i++)
            {
                long bCount = decayingBuckets[i];
                elements += bCount;
                sum += bCount * (double)bucketOffsets[i];
            }

            if (elements == 0)
                return 0d;

            return sum / elements;
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
        private long snapshotLandmarkInSec;
        private final DecayingEstimatedHistogramReservoir reservoir;

        public EstimatedHistogramReservoirSnapshot(DecayingEstimatedHistogramReservoir reservoir)
        {
            super(reservoir);
            
            int length = reservoir.size();
            this.values = new long[length];

            DecayingBuckets decayingBucketsRef = reservoir.getDecayingBuckets();

            this.snapshotLandmarkInSec = decayingBucketsRef.decayLandmarkInSec;
            double rescaleFactor = decayingBucketsRef.forwardDecayWeight(reservoir.clock.nowInSec());

            for (int i = 0; i < length; i++)
            {
                this.decayingBuckets[i] = Math.round(reservoir.bucketValue(i, decayingBucketsRef.decayBuckets) / rescaleFactor);
                this.values[i] = reservoir.bucketValue(i, reservoir.buckets);
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
        public long getSnapshotLandmarkInSec()
        {
            return snapshotLandmarkInSec;
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
            if (snapshot.snapshotLandmarkInSec < snapshotLandmarkInSec)
            {
                rescaleArray(snapshot.decayingBuckets, (snapshotLandmarkInSec - snapshot.snapshotLandmarkInSec));
            }
            else if (snapshot.snapshotLandmarkInSec > snapshotLandmarkInSec)
            {
                rescaleArray(decayingBuckets, (snapshot.snapshotLandmarkInSec - snapshotLandmarkInSec));
                this.snapshotLandmarkInSec = snapshot.snapshotLandmarkInSec;
            }

            // Now merge the buckets
            for (int i = 0; i < snapshot.decayingBuckets.length; i++)
            {
                decayingBuckets[i] += snapshot.decayingBuckets[i];
                values[i] += snapshot.values[i];
            }

            this.count += snapshot.count;
        }

        private void rescaleArray(long[] decayingBuckets, long landMarkDifferenceSec)
        {
            final double rescaleFactor = Math.exp(landMarkDifferenceSec / MEAN_LIFETIME_IN_S);
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
            DecayingBuckets decayingBucketsRef = reservoir.getDecayingBuckets();
            double rescaleFactor = decayingBucketsRef.forwardDecayWeight(reservoir.clock.nowInSec());

            for (int i = 0; i < length; i++)
            {
                this.decayingBuckets[i] = Math.round(reservoir.bucketValue(i, decayingBucketsRef.decayBuckets) / rescaleFactor);
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
