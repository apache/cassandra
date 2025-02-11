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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.utils.MonotonicClock;

import static java.lang.Math.exp;

public class ThreadLocalMeter implements Meter
{
    private final ThreadLocalExponentialMovingAverages movingAverages;
    private final int countMetricId;
    private final long startTime;
    private final MonotonicClock clock;

    public ThreadLocalMeter()
    {
        this(MonotonicClock.Global.approxTime);
    }

    public ThreadLocalMeter(MonotonicClock clock)
    {
        this.movingAverages = new ThreadLocalExponentialMovingAverages(clock);
        this.clock = clock;
        this.startTime = this.clock.now();
        this.countMetricId = PiggybackArrayThreadLocalMetrics.allocateMetricId();
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark()
    {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n)
    {
        PiggybackArrayThreadLocalMetrics context = PiggybackArrayThreadLocalMetrics.get();
        movingAverages.tickIfNecessary();
        context.addNonStatic(countMetricId, n);
        movingAverages.update(context, n);
    }

    @Override
    public long getCount()
    {
        return PiggybackArrayThreadLocalMetrics.getCount(countMetricId);
    }

    @Override
    public double getFifteenMinuteRate()
    {
        movingAverages.tickIfNecessary();
        return movingAverages.getM15Rate();
    }

    @Override
    public double getFiveMinuteRate()
    {
        movingAverages.tickIfNecessary();
        return movingAverages.getM5Rate();
    }

    @Override
    public double getOneMinuteRate()
    {
        movingAverages.tickIfNecessary();
        return movingAverages.getM1Rate();
    }

    @Override
    public double getMeanRate()
    {
        long count = getCount();
        if (count == 0)
        {
            return 0.0;
        }
        else
        {
            final double elapsed = clock.now() - startTime;
            return count / elapsed * TimeUnit.SECONDS.toNanos(1);
        }
    }

    public void destroy()
    {
        PiggybackArrayThreadLocalMetrics.destroyMetric(countMetricId);
        movingAverages.destroy();
    }

    static class ThreadLocalExponentialMovingAverages
    {
        private static final int INTERVAL_SEC = 5;
        private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(INTERVAL_SEC);
        private static final double SECONDS_PER_MINUTE = 60.0;
        private static final int ONE_MINUTE = 1;
        private static final int FIVE_MINUTES = 5;
        private static final int FIFTEEN_MINUTES = 15;
        private static final double M1_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / ONE_MINUTE);
        private static final double M5_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / FIVE_MINUTES);
        private static final double M15_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / FIFTEEN_MINUTES);

        /**
         * CASSANDRA-19332
         * If ticking would reduce even Long.MAX_VALUE in the 15 minute EWMA below this target then don't bother
         * ticking in a loop and instead reset all the EWMAs.
         */
        private static final double maxTickZeroTarget = 0.0001;
        private static final int maxTicks;

        static
        {
            int m3Ticks = 1;
            double m15Rate = 0.0;
            m15Rate = tickFifteenMinuteEWMA(m15Rate, Long.MAX_VALUE);
            do
            {
                m15Rate = tickFifteenMinuteEWMA(m15Rate, 0);
                m3Ticks++;
            }
            while (getRatePerSecond(m15Rate) > maxTickZeroTarget);
            maxTicks = m3Ticks;
        }

        // Double.MIN_VALUE means non-initialized
        private volatile double m1Rate = Double.MIN_VALUE;
        private volatile double m5Rate = Double.MIN_VALUE;
        private volatile double m15Rate = Double.MIN_VALUE;

        private final AtomicLong lastTick;
        private final MonotonicClock clock;

        private final int uncountedMetricId;

        public ThreadLocalExponentialMovingAverages(MonotonicClock clock)
        {
            this.clock = clock;
            this.lastTick = new AtomicLong(this.clock.now());
            this.uncountedMetricId = PiggybackArrayThreadLocalMetrics.allocateMetricId();
        }

        public void update(PiggybackArrayThreadLocalMetrics context, long n)
        {
            context.addNonStatic(uncountedMetricId, n);
        }

        public void update(long n)
        {
            update(PiggybackArrayThreadLocalMetrics.get(), n);
        }

        public void tickIfNecessary()
        {
            long oldTick = this.lastTick.get();
            long newTick = this.clock.now();
            long age = newTick - oldTick;
            if (age > TICK_INTERVAL)
            {
                long newIntervalStartTick = newTick - age % TICK_INTERVAL;
                if (this.lastTick.compareAndSet(oldTick, newIntervalStartTick))
                {
                    long requiredTicks = age / TICK_INTERVAL;
                    if (requiredTicks >= maxTicks)
                        reset();
                    else
                    {
                        // TODO: check how to make count and reset cheaper
                        // we can skip dead threads check for ticks executed as a part of a meter mark
                        // we can try to replace a global rate and ticks with local rates..
                        long count = PiggybackArrayThreadLocalMetrics.getCountAndReset(uncountedMetricId);
                        for (long i = 0; i < requiredTicks; i++)
                        {
                            m1Rate = tickOneMinuteEWMA(m1Rate, count);
                            m5Rate = tickFiveMinuteEWMA(m5Rate, count);
                            m15Rate = tickFifteenMinuteEWMA(m15Rate, count);
                            count = 0;
                        }
                    }
                }
            }
        }

        public static double tickOneMinuteEWMA(double oldRate, long count)
        {
            return tick(M1_ALPHA, oldRate, count);
        }

        public static double tickFiveMinuteEWMA(double oldRate, long count)
        {
            return tick(M5_ALPHA, oldRate, count);
        }

        public static double tickFifteenMinuteEWMA(double oldRate, long count)
        {
            return tick(M15_ALPHA, oldRate, count);
        }

        private static double tick(double alpha, double oldRate, long count)
        {
            double instantRate = (double) count / TICK_INTERVAL;
            if (oldRate != Double.MIN_VALUE)
                return oldRate + alpha * (instantRate - oldRate);
            else // init
                return instantRate;
        }

        public double getM1Rate()
        {
            return getRatePerSecond(m1Rate);
        }

        public double getM5Rate()
        {
            return getRatePerSecond(m5Rate);
        }

        public double getM15Rate()
        {
            return getRatePerSecond(m15Rate);
        }

        private static double getRatePerSecond(double rate)
        {
            if (rate == Double.MIN_VALUE)
                rate = 0.0;
            return rate * (double) TimeUnit.SECONDS.toNanos(1L);
        }

        /**
         * Set the rate to the smallest possible positive value. Used to avoid calling tick a large number of times.
         */
        public void reset()
        {
            PiggybackArrayThreadLocalMetrics.getCountAndReset(uncountedMetricId);
            m1Rate = Double.MIN_NORMAL;
            m5Rate = Double.MIN_NORMAL;
            m15Rate = Double.MIN_NORMAL;
        }

        public void destroy()
        {
            PiggybackArrayThreadLocalMetrics.destroyMetric(uncountedMetricId);
        }
    }
}
