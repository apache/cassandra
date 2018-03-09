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

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.Locale;

import com.google.common.annotations.VisibleForTesting;

/**
 * Groups strategies to coalesce messages.
 */
public class CoalescingStrategies
{
    /*
     * Log debug information at info level about what the average is and when coalescing is enabled/disabled
     */
    private static final String DEBUG_COALESCING_PROPERTY = Config.PROPERTY_PREFIX + "coalescing_debug";
    private static final boolean DEBUG_COALESCING = Boolean.getBoolean(DEBUG_COALESCING_PROPERTY);

    private static final String DEBUG_COALESCING_PATH_PROPERTY = Config.PROPERTY_PREFIX + "coalescing_debug_path";
    private static final String DEBUG_COALESCING_PATH = System.getProperty(DEBUG_COALESCING_PATH_PROPERTY, "/tmp/coleascing_debug");

    public enum Strategy { MOVINGAVERAGE, FIXED, TIMEHORIZON, DISABLED }

    static
    {
        if (DEBUG_COALESCING)
        {
            File directory = new File(DEBUG_COALESCING_PATH);

            if (directory.exists())
                FileUtils.deleteRecursive(directory);

            if (!directory.mkdirs())
                throw new ExceptionInInitializerError("Couldn't create log dir");
        }
    }

    public static interface Coalescable
    {
        long timestampNanos();
    }

    @VisibleForTesting
    static long determineCoalescingTime(long averageGap, long maxCoalesceWindow)
    {
        // Don't bother waiting at all if we're unlikely to get any new message within our max window
        if (averageGap > maxCoalesceWindow)
            return -1;

        // avoid the degenerate case of zero (very unlikely, but let's be safe)
        if (averageGap <= 0)
            return maxCoalesceWindow;

        // assume we receive as many messages as we expect; apply the same logic to the future batch:
        // expect twice as many messages to consider sleeping for "another" interval; this basically translates
        // to doubling our sleep period until we exceed our max sleep window.
        long sleep = averageGap;
        while (sleep * 2 < maxCoalesceWindow)
            sleep *= 2;
        return sleep;
    }

    /**
     * A coalescing strategy, that decides when to coalesce messages.
     * <p>
     * The general principle is that, when asked, the strategy returns the time delay we want to wait for more messages
     * to arrive before sending so message can be coalesced. For that, the strategy must be fed new messages through
     * the {@link #newArrival(Coalescable)} method (the only assumption we make on messages is that they have an associated
     * timestamp). The strategy can then be queried for the time to wait for coalescing through
     * {@link #currentCoalescingTimeNanos()}.
     * <p>
     * Note that it is expected that a call {@link #currentCoalescingTimeNanos()} will come just after a call to
     * {@link #newArrival(Coalescable))}, as the intent of the value returned by the former method is "Given a new message, how much
     * time should I wait for more messages to arrive and be coalesced with that message". But both calls are separated
     * as one may not want to call {@link #currentCoalescingTimeNanos()} after every call to {@link #newArrival(Coalescable)}
     * and we thus save processing. How arrivals influence the coalescing time is however entirely up to the strategy and some
     * strategy may ignore arrivals completely and return a constant coalescing time.
     */
    public interface CoalescingStrategy
    {
        /**
         * Inform the strategy of a new message to consider.
         *
         * @param message the message to consider.
         */
        void newArrival(Coalescable message);

        /**
         * The current time to wait for the purpose of coalescing messages.
         *
         * @return the coalescing time. A negative value can be returned if no coalescing should be done (which can be a
         * transient thing).
         */
        long currentCoalescingTimeNanos();
    }

    public static abstract class AbstractCoalescingStrategy implements CoalescingStrategy
    {
        protected final Logger logger;
        protected volatile boolean shouldLogAverage = false;
        protected final ByteBuffer logBuffer;
        private RandomAccessFile ras;
        private final String displayName;

        protected AbstractCoalescingStrategy(Logger logger, String displayName)
        {
            this.logger = logger;
            this.displayName = displayName;

            RandomAccessFile rasTemp = null;
            ByteBuffer logBufferTemp = null;
            if (DEBUG_COALESCING)
            {
                ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(() -> shouldLogAverage = true, 5, 5, TimeUnit.SECONDS);
                try
                {
                    File outFile = File.createTempFile("coalescing_" + this.displayName + "_", ".log", new File(DEBUG_COALESCING_PATH));
                    rasTemp = new RandomAccessFile(outFile, "rw");
                    logBufferTemp = ras.getChannel().map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
                    logBufferTemp.putLong(0);
                }
                catch (Exception e)
                {
                    logger.error("Unable to create output file for debugging coalescing", e);
                }
            }
            ras = rasTemp;
            logBuffer = logBufferTemp;
        }

        /*
         * If debugging is enabled log to the logger the current average gap calculation result.
         */
        final protected void debugGap(long averageGap)
        {
            if (DEBUG_COALESCING && shouldLogAverage)
            {
                shouldLogAverage = false;
                logger.info("{} gap {}μs", this, TimeUnit.NANOSECONDS.toMicros(averageGap));
            }
        }

        /*
         * If debugging is enabled log the provided nanotime timestamp to a file.
         */
        final protected void debugTimestamp(long timestamp)
        {
            if(DEBUG_COALESCING && logBuffer != null)
            {
                logBuffer.putLong(0, logBuffer.getLong(0) + 1);
                logBuffer.putLong(timestamp);
            }
        }

        /*
         * If debugging is enabled log the timestamps of all the items in the provided collection
         * to a file.
         */
        final protected <C extends Coalescable> void debugTimestamps(Collection<C> coalescables)
        {
            if (DEBUG_COALESCING)
            {
                for (C coalescable : coalescables)
                {
                    debugTimestamp(coalescable.timestampNanos());
                }
            }
        }
    }

    @VisibleForTesting
    static class TimeHorizonMovingAverageCoalescingStrategy extends AbstractCoalescingStrategy
    {
        // for now we'll just use 64ms per bucket; this can be made configurable, but results in ~1s for 16 samples
        private static final int INDEX_SHIFT = 26;
        private static final long BUCKET_INTERVAL = 1L << 26;
        private static final int BUCKET_COUNT = 16;
        private static final long INTERVAL = BUCKET_INTERVAL * BUCKET_COUNT;
        private static final long MEASURED_INTERVAL = BUCKET_INTERVAL * (BUCKET_COUNT - 1);

        // the minimum timestamp we will now accept updates for; only moves forwards, never backwards
        private long epoch;
        // the buckets, each following on from epoch; the measurements run from ix(epoch) to ix(epoch - 1)
        // ix(epoch-1) is a partial result, that is never actually part of the calculation, and most updates
        // are expected to hit this bucket
        private final int samples[] = new int[BUCKET_COUNT];
        private long sum = 0;
        private final long maxCoalesceWindow;

        public TimeHorizonMovingAverageCoalescingStrategy(int maxCoalesceWindow, Logger logger, String displayName, long initialEpoch)
        {
            super(logger, displayName);
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            sum = 0;
            epoch = initialEpoch;
        }

        private long averageGap()
        {
            if (sum == 0)
                return Integer.MAX_VALUE;
            return MEASURED_INTERVAL / sum;
        }

        // this sample extends past the end of the range we cover, so rollover
        private long rollEpoch(long delta, long epoch, long nanos)
        {
            if (delta > 2 * INTERVAL)
            {
                // this sample is more than twice our interval ahead, so just clear our counters completely
                epoch = epoch(nanos);
                sum = 0;
                Arrays.fill(samples, 0);
            }
            else
            {
                // ix(epoch - 1) => last index; this is our partial result bucket, so we add this to the sum
                sum += samples[ix(epoch - 1)];
                // then we roll forwards, clearing buckets, until our interval covers the new sample time
                while (epoch + INTERVAL < nanos)
                {
                    int index = ix(epoch);
                    sum -= samples[index];
                    samples[index] = 0;
                    epoch += BUCKET_INTERVAL;
                }
            }
            // store the new epoch
            this.epoch = epoch;
            return epoch;
        }

        private long epoch(long latestNanos)
        {
            return (latestNanos - MEASURED_INTERVAL) & ~(BUCKET_INTERVAL - 1);
        }

        private int ix(long nanos)
        {
            return (int) ((nanos >>> INDEX_SHIFT) & 15);
        }

        public void newArrival(Coalescable message)
        {
            final long timestamp = message.timestampNanos();
            debugTimestamp(timestamp);
            long epoch = this.epoch;
            long delta = timestamp - epoch;
            if (delta < 0)
                // have to simply ignore, but would be a bit unlucky to get such reordering
                return;

            if (delta > INTERVAL)
                epoch = rollEpoch(delta, epoch, timestamp);

            int ix = ix(timestamp);
            samples[ix]++;

            // if we've updated an old bucket, we need to update the sum to match
            if (ix != ix(epoch - 1))
                sum++;
        }

        public long currentCoalescingTimeNanos()
        {
            long averageGap = averageGap();
            debugGap(averageGap);
            return determineCoalescingTime(averageGap, maxCoalesceWindow);
        }

        @Override
        public String toString()
        {
            return "Time horizon moving average";
        }
    }

    /**
     * Start coalescing by sleeping if the moving average is < the requested window.
     * The actual time spent waiting to coalesce will be the min( window, moving average * 2)
     * The actual amount of time spent waiting can be greater then the window. For instance
     * observed time spent coalescing was 400 microseconds with the window set to 200 in one benchmark.
     */
    @VisibleForTesting
    static class MovingAverageCoalescingStrategy extends AbstractCoalescingStrategy
    {
        static final int SAMPLE_SIZE = 16;
        private final int samples[] = new int[SAMPLE_SIZE];
        private final long maxCoalesceWindow;

        private long lastSample = 0;
        private int index = 0;
        private long sum = 0;
        private long currentGap;

        public MovingAverageCoalescingStrategy(int maxCoalesceWindow, Logger logger, String displayName)
        {
            super(logger, displayName);
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            for (int ii = 0; ii < samples.length; ii++)
                samples[ii] = Integer.MAX_VALUE;
            sum = Integer.MAX_VALUE * (long)samples.length;
        }

        private long logSample(int value)
        {
            sum -= samples[index];
            sum += value;
            samples[index] = value;
            index++;
            index = index & ((1 << 4) - 1);
            return sum / SAMPLE_SIZE;
        }

        public void newArrival(Coalescable message)
        {
            final long timestamp = message.timestampNanos();
            debugTimestamp(timestamp);
            if (timestamp > lastSample)
            {
                final int delta = (int)(Math.min(Integer.MAX_VALUE, timestamp - lastSample));
                lastSample = timestamp;
                currentGap = logSample(delta);
            }
            else
            {
                currentGap = logSample(1);
            }
        }

        public long currentCoalescingTimeNanos()
        {
            debugGap(currentGap);
            return determineCoalescingTime(currentGap, maxCoalesceWindow);
        }

        @Override
        public String toString()
        {
            return "Moving average";
        }
    }

    /**
     * A fixed strategy as a backup in case MovingAverage or TimeHorizongMovingAverage fails in some scenario
     */
    @VisibleForTesting
    static class FixedCoalescingStrategy extends AbstractCoalescingStrategy
    {
        private final long coalesceWindow;

        public FixedCoalescingStrategy(int coalesceWindowMicros, Logger logger, String displayName)
        {
            super(logger, displayName);
            coalesceWindow = TimeUnit.MICROSECONDS.toNanos(coalesceWindowMicros);
        }

        public void newArrival(Coalescable message)
        {
            debugTimestamp(message.timestampNanos());
        }

        public long currentCoalescingTimeNanos()
        {
            return coalesceWindow;
        }

        @Override
        public String toString()
        {
            return "Fixed";
        }
    }

    public static Optional<CoalescingStrategy> newCoalescingStrategy(String strategy, int coalesceWindow, Logger logger, String displayName)
    {
        String strategyCleaned = strategy.trim().toUpperCase(Locale.ENGLISH);

        try
        {
            switch (Enum.valueOf(Strategy.class, strategyCleaned))
            {
                case MOVINGAVERAGE:
                    return Optional.of(new MovingAverageCoalescingStrategy(coalesceWindow, logger, displayName));
                case FIXED:
                    return Optional.of(new FixedCoalescingStrategy(coalesceWindow, logger, displayName));
                case TIMEHORIZON:
                    long initialEpoch = System.nanoTime();
                    return Optional.of(new TimeHorizonMovingAverageCoalescingStrategy(coalesceWindow, logger, displayName, initialEpoch));
                case DISABLED:
                    return Optional.empty();
                default:
                    throw new IllegalArgumentException("supported coalese strategy");
            }
        }
        catch (IllegalArgumentException iae)
        {
            try
            {
                Class<?> clazz = Class.forName(strategy);

                if (!CoalescingStrategy.class.isAssignableFrom(clazz))
                    throw new RuntimeException(strategy + " is not an instance of CoalescingStrategy");

                Constructor<?> constructor = clazz.getConstructor(int.class, Logger.class, String.class);
                return Optional.of((CoalescingStrategy)constructor.newInstance(coalesceWindow, logger, displayName));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
