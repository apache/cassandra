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

import java.lang.reflect.Constructor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.APPROXIMATE_TIME_PRECISION_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_APPROX;
import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_MONOTONIC_PRECISE;
import static org.apache.cassandra.config.CassandraRelevantProperties.NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Wrapper around time related functions that are either implemented by using the default JVM calls
 * or by using a custom implementation for testing purposes.
 *
 * See {@link Global#preciseTime} for how to use a custom implementation.
 *
 * Please note that {@link java.time.Clock} wasn't used, as it would not be possible to provide an
 * implementation for {@link #now()} with the exact same properties of {@link System#nanoTime()}.
 *
 * TODO better rationalise MonotonicClock/Clock
 */
@Shared(scope = SIMULATION)
public interface MonotonicClock
{

    /**
     * @see System#nanoTime()
     *
     * Provides a monotonic time that can be compared with any other such value produced by the same clock
     * since the application started only; these times cannot be persisted or serialized to other nodes.
     *
     * Nanosecond precision.
     */
    public long now();

    /**
     * @return nanoseconds of potential error
     */
    public long error();

    public MonotonicClockTranslation translate();

    public boolean isAfter(long instant);
    public boolean isAfter(long now, long instant);

    public static class Global
    {
        private static final Logger logger = LoggerFactory.getLogger(MonotonicClock.class);

        /**
         * Static singleton object that will be instantiated by default with a system clock
         * implementation. Set <code>cassandra.clock</code> system property to a FQCN to use a
         * different implementation instead.
         */
        public static final MonotonicClock preciseTime = precise();
        public static final MonotonicClock approxTime = approx(preciseTime);

        private static MonotonicClock precise()
        {
            String sclock = CLOCK_MONOTONIC_PRECISE.getString();

            if (sclock != null)
            {
                try
                {
                    logger.debug("Using custom clock implementation: {}", sclock);
                    return (MonotonicClock) Class.forName(sclock).newInstance();
                }
                catch (Exception e)
                {
                    logger.error(e.getMessage(), e);
                }
            }

            return new SystemClock();
        }

        private static MonotonicClock approx(MonotonicClock precise)
        {
            String sclock = CLOCK_MONOTONIC_APPROX.getString();
            if (sclock != null)
            {
                try
                {
                    logger.debug("Using custom clock implementation: {}", sclock);
                    Class<? extends MonotonicClock> clazz = (Class<? extends MonotonicClock>) Class.forName(sclock);

                    if (SystemClock.class.equals(clazz) && SystemClock.class.equals(precise.getClass()))
                        return precise;

                    try
                    {
                        Constructor<? extends MonotonicClock> withPrecise = clazz.getConstructor(MonotonicClock.class);
                        return withPrecise.newInstance(precise);
                    }
                    catch (NoSuchMethodException nme)
                    {
                    }

                    return clazz.newInstance();
                }
                catch (Exception e)
                {
                    logger.error(e.getMessage(), e);
                }
            }

            return new SampledClock(precise);
        }
    }

    static abstract class AbstractEpochSamplingClock implements MonotonicClock
    {
        private static final Logger logger = LoggerFactory.getLogger(AbstractEpochSamplingClock.class);
        private static final long UPDATE_INTERVAL_MS = NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL.getLong();

        @VisibleForTesting
        public static class AlmostSameTime implements MonotonicClockTranslation
        {
            final long millisSinceEpoch;
            final long monotonicNanos;
            final long error; // maximum error of millis measurement (in nanos)

            @VisibleForTesting
            public AlmostSameTime(long millisSinceEpoch, long monotonicNanos, long errorNanos)
            {
                this.millisSinceEpoch = millisSinceEpoch;
                this.monotonicNanos = monotonicNanos;
                this.error = errorNanos;
            }

            public long fromMillisSinceEpoch(long currentTimeMillis)
            {
                return monotonicNanos + MILLISECONDS.toNanos(currentTimeMillis - millisSinceEpoch);
            }

            public long toMillisSinceEpoch(long nanoTime)
            {
                return millisSinceEpoch + TimeUnit.NANOSECONDS.toMillis(nanoTime - monotonicNanos);
            }

            public long error()
            {
                return error;
            }
        }

        final LongSupplier millisSinceEpoch;

        private volatile AlmostSameTime almostSameTime = new AlmostSameTime(0L, 0L, Long.MAX_VALUE);
        private Future<?> almostSameTimeUpdater;
        private static double failedAlmostSameTimeUpdateModifier = 1.0;

        AbstractEpochSamplingClock(LongSupplier millisSinceEpoch)
        {
            this.millisSinceEpoch = millisSinceEpoch;
            resumeEpochSampling();
        }

        public MonotonicClockTranslation translate()
        {
            return almostSameTime;
        }

        public synchronized void pauseEpochSampling()
        {
            if (almostSameTimeUpdater == null)
                return;

            almostSameTimeUpdater.cancel(true);
            try { almostSameTimeUpdater.get(); } catch (Throwable t) { }
            almostSameTimeUpdater = null;
        }

        public synchronized void resumeEpochSampling()
        {
            if (almostSameTimeUpdater != null)
                throw new IllegalStateException("Already running");
            updateAlmostSameTime();
            logger.info("Scheduling approximate time conversion task with an interval of {} milliseconds", UPDATE_INTERVAL_MS);
            almostSameTimeUpdater = ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(this::updateAlmostSameTime, UPDATE_INTERVAL_MS, UPDATE_INTERVAL_MS, MILLISECONDS);
        }

        private void updateAlmostSameTime()
        {
            final int tries = 3;
            long[] samples = new long[2 * tries + 1];
            samples[0] = nanoTime();
            for (int i = 1 ; i < samples.length ; i += 2)
            {
                samples[i] = millisSinceEpoch.getAsLong();
                samples[i + 1] = now();
            }

            int best = 1;
            // take sample with minimum delta between calls
            for (int i = 3 ; i < samples.length - 1 ; i += 2)
            {
                if ((samples[i+1] - samples[i-1]) < (samples[best+1]-samples[best-1]))
                    best = i;
            }

            long millis = samples[best];
            long nanos = (samples[best+1] / 2) + (samples[best-1] / 2);
            long error = (samples[best+1] / 2) - (samples[best-1] / 2);

            AlmostSameTime prev = almostSameTime;
            AlmostSameTime next = new AlmostSameTime(millis, nanos, error);

            if (next.error > prev.error && next.error > prev.error * failedAlmostSameTimeUpdateModifier)
            {
                failedAlmostSameTimeUpdateModifier *= 1.1;
                return;
            }

            failedAlmostSameTimeUpdateModifier = 1.0;
            almostSameTime = next;
        }
    }

    public static class SystemClock extends AbstractEpochSamplingClock
    {
        private SystemClock()
        {
            super(Clock.Global::currentTimeMillis);
        }

        @Override
        public long now()
        {
            return nanoTime();
        }

        @Override
        public long error()
        {
            return 1;
        }

        @Override
        public boolean isAfter(long instant)
        {
            return now() > instant;
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            return now > instant;
        }
    }

    public static class SampledClock implements MonotonicClock
    {
        private static final Logger logger = LoggerFactory.getLogger(SampledClock.class);
        private static final int UPDATE_INTERVAL_MS = Math.max(1, APPROXIMATE_TIME_PRECISION_MS.getInt());
        private static final long ERROR_NANOS = MILLISECONDS.toNanos(UPDATE_INTERVAL_MS);

        private final MonotonicClock precise;

        private volatile long almostNow;
        private Future<?> almostNowUpdater;

        public SampledClock(MonotonicClock precise)
        {
            this.precise = precise;
            resumeNowSampling();
        }

        @Override
        public long now()
        {
            return almostNow;
        }

        @Override
        public long error()
        {
            return ERROR_NANOS;
        }

        @Override
        public MonotonicClockTranslation translate()
        {
            return precise.translate();
        }

        @Override
        public boolean isAfter(long instant)
        {
            return isAfter(almostNow, instant);
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            return now - ERROR_NANOS > instant;
        }

        public synchronized void pauseNowSampling()
        {
            if (almostNowUpdater == null)
                return;

            almostNowUpdater.cancel(true);
            try { almostNowUpdater.get(); } catch (Throwable t) { }
            almostNowUpdater = null;
        }

        public synchronized void resumeNowSampling()
        {
            if (almostNowUpdater != null)
                throw new IllegalStateException("Already running");

            almostNow = precise.now();
            logger.info("Scheduling approximate time-check task with a precision of {} milliseconds", UPDATE_INTERVAL_MS);
            almostNowUpdater = ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(() -> almostNow = precise.now(), UPDATE_INTERVAL_MS, UPDATE_INTERVAL_MS, MILLISECONDS);
        }

        public synchronized void refreshNow()
        {
            pauseNowSampling();
            resumeNowSampling();
        }
    }

}
