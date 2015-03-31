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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class CoalescingStrategies
{

    /*
     * Log debug information at info level about what the average is and when coalescing is enabled/disabled
     */
    private static final String DEBUG_COALESCING_PROPERTY = Config.PROPERTY_PREFIX + "coalescing_debug";
    private static final boolean DEBUG_COALESCING = Boolean.getBoolean(DEBUG_COALESCING_PROPERTY);

    private static final String DEBUG_COALESCING_PATH_PROPERTY = Config.PROPERTY_PREFIX + "coalescing_debug_path";
    private static final String DEBUG_COALESCING_PATH = System.getProperty(DEBUG_COALESCING_PATH_PROPERTY, "/tmp/coleascing_debug");

    static {
        if (DEBUG_COALESCING)
        {
            File directory = new File(DEBUG_COALESCING_PATH);

            if (directory.exists())
                FileUtils.deleteRecursive(directory);

            if (!directory.mkdirs())
                throw new ExceptionInInitializerError("Couldn't create log dir");
        }
    }

    @VisibleForTesting
    interface Clock
    {
        long nanoTime();
    }

    @VisibleForTesting
    static Clock CLOCK = new Clock()
    {
        public long nanoTime()
        {
            return System.nanoTime();
        }
    };

    public static interface Coalescable {
        long timestampNanos();
    }

    @VisibleForTesting
    static void parkLoop(long nanos)
    {
        long now = System.nanoTime();
        final long timer = now + nanos;
        do
        {
            LockSupport.parkNanos(timer - now);
        }
        while (timer - (now = System.nanoTime()) > nanos / 16);
    }

    private static boolean maybeSleep(int messages, long averageGap, long maxCoalesceWindow, Parker parker)
    {
        // only sleep if we can expect to double the number of messages we're sending in the time interval
        long sleep = messages * averageGap;
        if (sleep > maxCoalesceWindow)
            return false;

        // assume we receive as many messages as we expect; apply the same logic to the future batch:
        // expect twice as many messages to consider sleeping for "another" interval; this basically translates
        // to doubling our sleep period until we exceed our max sleep window
        while (sleep * 2 < maxCoalesceWindow)
            sleep *= 2;
        parker.park(sleep);
        return true;
    }

    public static abstract class CoalescingStrategy
    {
        protected final Parker parker;
        protected final Logger logger;
        protected volatile boolean shouldLogAverage = false;
        protected final ByteBuffer logBuffer;
        private RandomAccessFile ras;
        private final String displayName;

        protected CoalescingStrategy(Parker parker, Logger logger, String displayName)
        {
            this.parker = parker;
            this.logger = logger;
            this.displayName = displayName;
            if (DEBUG_COALESCING)
            {
                new Thread(displayName + " debug thread") {
                    @Override
                    public void run() {
                        while (true) {
                            try
                            {
                                Thread.sleep(5000);
                            }
                            catch (InterruptedException e)
                            {
                                throw new AssertionError();
                            }
                            shouldLogAverage = true;
                        }
                    }
                }.start();
            }
            RandomAccessFile rasTemp = null;
            ByteBuffer logBufferTemp = null;
            if (DEBUG_COALESCING)
            {
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
        final protected <C extends Coalescable> void debugTimestamps(Collection<C> coalescables) {
            if (DEBUG_COALESCING) {
                for (C coalescable : coalescables) {
                    debugTimestamp(coalescable.timestampNanos());
                }
            }
        }

        /**
         * Drain from the input blocking queue to the output list up to maxItems elements.
         *
         * The coalescing strategy may choose to park the current thread if it thinks it will
         * be able to produce an output list with more elements.
         *
         * @param input Blocking queue to retrieve elements from
         * @param out Output list to place retrieved elements in. Must be empty.
         * @param maxItems Maximum number of elements to place in the output list
         */
        public <C extends Coalescable> void coalesce(BlockingQueue<C> input, List<C> out, int maxItems) throws InterruptedException
        {
            Preconditions.checkArgument(out.isEmpty(), "out list should be empty");
            coalesceInternal(input, out, maxItems);
        }

        protected abstract <C extends Coalescable> void coalesceInternal(BlockingQueue<C> input, List<C> out, int maxItems) throws InterruptedException;

    }

    @VisibleForTesting
    interface Parker
    {
        void park(long nanos);
    }

    private static final Parker PARKER = new Parker()
    {
        @Override
        public void park(long nanos)
        {
            parkLoop(nanos);
        }
    };

    @VisibleForTesting
    static class TimeHorizonMovingAverageCoalescingStrategy extends CoalescingStrategy
    {
        // for now we'll just use 64ms per bucket; this can be made configurable, but results in ~1s for 16 samples
        private static final int INDEX_SHIFT = 26;
        private static final long BUCKET_INTERVAL = 1L << 26;
        private static final int BUCKET_COUNT = 16;
        private static final long INTERVAL = BUCKET_INTERVAL * BUCKET_COUNT;
        private static final long MEASURED_INTERVAL = BUCKET_INTERVAL * (BUCKET_COUNT - 1);

        // the minimum timestamp we will now accept updates for; only moves forwards, never backwards
        private long epoch = CLOCK.nanoTime();
        // the buckets, each following on from epoch; the measurements run from ix(epoch) to ix(epoch - 1)
        // ix(epoch-1) is a partial result, that is never actually part of the calculation, and most updates
        // are expected to hit this bucket
        private final int samples[] = new int[BUCKET_COUNT];
        private long sum = 0;
        private final long maxCoalesceWindow;

        public TimeHorizonMovingAverageCoalescingStrategy(int maxCoalesceWindow, Parker parker, Logger logger, String displayName)
        {
            super(parker, logger, displayName);
            this.maxCoalesceWindow = TimeUnit.MICROSECONDS.toNanos(maxCoalesceWindow);
            sum = 0;
        }

        private void logSample(long nanos)
        {
            debugTimestamp(nanos);
            long epoch = this.epoch;
            long delta = nanos - epoch;
            if (delta < 0)
                // have to simply ignore, but would be a bit crazy to get such reordering
                return;

            if (delta > INTERVAL)
                epoch = rollepoch(delta, epoch, nanos);

            int ix = ix(nanos);
            samples[ix]++;

            // if we've updated an old bucket, we need to update the sum to match
            if (ix != ix(epoch - 1))
                sum++;
        }

        private long averageGap()
        {
            if (sum == 0)
                return Integer.MAX_VALUE;
            return MEASURED_INTERVAL / sum;
        }

        // this sample extends past the end of the range we cover, so rollover
        private long rollepoch(long delta, long epoch, long nanos)
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

        @Override
        protected <C extends Coalescable> void coalesceInternal(BlockingQueue<C> input, List<C> out,  int maxItems) throws InterruptedException
        {
            if (input.drainTo(out, maxItems) == 0)
            {
                out.add(input.take());
                input.drainTo(out, maxItems - 1);
            }

            for (Coalescable qm : out)
                logSample(qm.timestampNanos());

            long averageGap = averageGap();
            debugGap(averageGap);

            int count = out.size();
            if (maybeSleep(count, averageGap, maxCoalesceWindow, parker))
            {
                input.drainTo(out, maxItems - out.size());
                int prevCount = count;
                count = out.size();
                for (int  i = prevCount; i < count; i++)
                    logSample(out.get(i).timestampNanos());
            }
        }

        @Override
        public String toString() {
            return "Time horizon moving average";
        }
    }

    /*
     * Start coalescing by sleeping if the moving average is < the requested window.
     * The actual time spent waiting to coalesce will be the min( window, moving average * 2)
     * The actual amount of time spent waiting can be greater then the window. For instance
     * observed time spent coalescing was 400 microseconds with the window set to 200 in one benchmark.
     */
    @VisibleForTesting
    static class MovingAverageCoalescingStrategy extends CoalescingStrategy
    {
        private final int samples[] = new int[16];
        private long lastSample = 0;
        private int index = 0;
        private long sum = 0;

        private final long maxCoalesceWindow;

        public MovingAverageCoalescingStrategy(int maxCoalesceWindow, Parker parker, Logger logger, String displayName)
        {
            super(parker, logger, displayName);
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
            return sum / 16;
        }

        private long notifyOfSample(long sample)
        {
            debugTimestamp(sample);
            if (sample > lastSample)
            {
                final int delta = (int)(Math.min(Integer.MAX_VALUE, sample - lastSample));
                lastSample = sample;
                return logSample(delta);
            }
            else
            {
                return logSample(1);
            }
        }

        @Override
        protected <C extends Coalescable> void coalesceInternal(BlockingQueue<C> input, List<C> out,  int maxItems) throws InterruptedException
        {
            if (input.drainTo(out, maxItems) == 0)
            {
                out.add(input.take());
            }

            long average = notifyOfSample(out.get(0).timestampNanos());

            debugGap(average);

            maybeSleep(out.size(), average, maxCoalesceWindow, parker);

            input.drainTo(out, maxItems - out.size());
            for (int ii = 1; ii < out.size(); ii++)
                notifyOfSample(out.get(ii).timestampNanos());
        }

        @Override
        public String toString() {
            return "Moving average";
        }
    }

    /*
     * A fixed strategy as a backup in case MovingAverage or TimeHorizongMovingAverage fails in some scenario
     */
    @VisibleForTesting
    static class FixedCoalescingStrategy extends CoalescingStrategy
    {
        private final long coalesceWindow;

        public FixedCoalescingStrategy(int coalesceWindowMicros, Parker parker, Logger logger, String displayName)
        {
            super(parker, logger, displayName);
            coalesceWindow = TimeUnit.MICROSECONDS.toNanos(coalesceWindowMicros);
        }

        @Override
        protected <C extends Coalescable> void coalesceInternal(BlockingQueue<C> input, List<C> out,  int maxItems) throws InterruptedException
        {
            if (input.drainTo(out, maxItems) == 0)
            {
                out.add(input.take());
                parker.park(coalesceWindow);
                input.drainTo(out, maxItems - 1);
            }
            debugTimestamps(out);
        }

        @Override
        public String toString() {
            return "Fixed";
        }
    }

    /*
     * A coalesscing strategy that just returns all currently available elements
     */
    @VisibleForTesting
    static class DisabledCoalescingStrategy extends CoalescingStrategy
    {

        public DisabledCoalescingStrategy(int coalesceWindowMicros, Parker parker, Logger logger, String displayName)
        {
            super(parker, logger, displayName);
        }

        @Override
        protected <C extends Coalescable> void coalesceInternal(BlockingQueue<C> input, List<C> out,  int maxItems) throws InterruptedException
        {
            if (input.drainTo(out, maxItems) == 0)
            {
                out.add(input.take());
                input.drainTo(out, maxItems - 1);
            }
            debugTimestamps(out);
        }

        @Override
        public String toString() {
            return "Disabled";
        }
    }

    @VisibleForTesting
    static CoalescingStrategy newCoalescingStrategy(String strategy,
                                                    int coalesceWindow,
                                                    Parker parker,
                                                    Logger logger,
                                                    String displayName)
    {
        String classname = null;
        String strategyCleaned = strategy.trim().toUpperCase();
        switch(strategyCleaned)
        {
        case "MOVINGAVERAGE":
            classname = MovingAverageCoalescingStrategy.class.getName();
            break;
        case "FIXED":
            classname = FixedCoalescingStrategy.class.getName();
            break;
        case "TIMEHORIZON":
            classname = TimeHorizonMovingAverageCoalescingStrategy.class.getName();
            break;
        case "DISABLED":
            classname = DisabledCoalescingStrategy.class.getName();
            break;
        default:
            classname = strategy;
        }

        try
        {
            Class<?> clazz = Class.forName(classname);

            if (!CoalescingStrategy.class.isAssignableFrom(clazz))
            {
                throw new RuntimeException(classname + " is not an instance of CoalescingStrategy");
            }

            Constructor<?> constructor = clazz.getConstructor(int.class, Parker.class, Logger.class, String.class);

            return (CoalescingStrategy)constructor.newInstance(coalesceWindow, parker, logger, displayName);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CoalescingStrategy newCoalescingStrategy(String strategy, int coalesceWindow, Logger logger, String displayName)
    {
        return newCoalescingStrategy(strategy, coalesceWindow, PARKER, logger, displayName);
    }
}
