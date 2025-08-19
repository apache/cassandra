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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Clock;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.ReflectionUtils;

import static java.lang.Math.exp;

/**
 * An alternative to Dropwizard Meter which implements the same kind of API.
 * it has more efficent mark operations and consumes less memory.
 * Only exponential decaying moving average is supported for 1/5/15-minutes rate values.
 * Tick logic is moved out from a mark operation and always executed in a background thread.
 * For better cache locality rate values are extracted to a common non thread-local array
 *  updated by a background thread in bulk.
 * Counter logic inside is implemented using @see ThreadLocalMetrics functionality.
 * NOTE: Dropwizard Meter is a class and there is no an interface for Dropwizard Meter logic,
 *   so we have to create an alternative hierarchy.
 */
public class ThreadLocalMeter extends com.codahale.metrics.Meter implements Meter
{
    private static final int INTERVAL_SEC = 5;
    private static final long TICK_INTERVAL_NS = TimeUnit.SECONDS.toNanos(INTERVAL_SEC);
    private static final double SECONDS_PER_MINUTE = 60.0;

    private static final double ONE_NS_IN_SEC = 1d / TimeUnit.SECONDS.toNanos(1);
    private static final int ONE_MINUTE = 1;
    private static final int FIVE_MINUTES = 5;
    private static final int FIFTEEN_MINUTES = 15;
    private static final double M1_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / ONE_MINUTE);
    private static final double M5_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / FIVE_MINUTES);
    private static final double M15_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / FIFTEEN_MINUTES);

    // each meter instance stores rate stats using 3 consecutive cells in rates array
    private static final int M1_RATE_OFFSET = 0;
    private static final int M5_RATE_OFFSET = 1;
    private static final int M15_RATE_OFFSET = 2;
    private static final int RATES_COUNT = 3;
    private static final double NON_INITIALIZED = Double.MIN_VALUE;

    private static final int BACKGROUND_TICK_INTERVAL_SEC = INTERVAL_SEC;
    private static final List<WeakReference<ThreadLocalMeter>> allMeters = new CopyOnWriteArrayList<>();

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
        double emulatedM15Rate = 0.0;
        emulatedM15Rate = tickFifteenMinuteEWMA(emulatedM15Rate, Long.MAX_VALUE);
        do
        {
            emulatedM15Rate = tickFifteenMinuteEWMA(emulatedM15Rate, 0);
            m3Ticks++;
        }
        while (getRatePerSecond(emulatedM15Rate) > maxTickZeroTarget);
        maxTicks = m3Ticks;
    }

    static
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(ThreadLocalMeter::tickAll,
                                                                 BACKGROUND_TICK_INTERVAL_SEC,
                                                                 BACKGROUND_TICK_INTERVAL_SEC,
                                                                 TimeUnit.SECONDS);
    }

    private static final Object ratesArrayGuard = new Object();

    // writes should be synchonized using ratesArrayGuard
    // 1) write during a copy when we need to extend rates array when a new Meter is created
    // 2) write during a tick
    private static volatile double[] rates = new double[RATES_COUNT * 16];
    static final AtomicInteger rateGroupIdGenerator = new AtomicInteger();

    // we recycle and reuse rate group IDs
    // a set bit means the correspondent rate group id is available to use
    private static final BitSet freeRateGroupIdSet = new BitSet();

    private static int allocateRateGroupOffset()
    {
        int rateGroupId;
        synchronized (freeRateGroupIdSet)
        {
            rateGroupId = freeRateGroupIdSet.nextSetBit(0);
            if (rateGroupId >= 0)
                freeRateGroupIdSet.clear(rateGroupId);
        }
        if (rateGroupId < 0)
        {
            rateGroupId = rateGroupIdGenerator.getAndAdd(RATES_COUNT);
        }
        synchronized (ratesArrayGuard)
        {
            if (rates.length < rateGroupId + RATES_COUNT)
            {
                double[] newRates = new double[rateGroupId + RATES_COUNT];
                System.arraycopy(rates, 0, newRates, 0, rates.length);
                rates = newRates;
            }
            rates[rateGroupId +  M1_RATE_OFFSET] = NON_INITIALIZED;
            rates[rateGroupId +  M5_RATE_OFFSET] = NON_INITIALIZED;
            rates[rateGroupId + M15_RATE_OFFSET] = NON_INITIALIZED;
        }
        return rateGroupId;
    }

    static double getRateValue(int offset)
    {
        return rates[offset];
    }

    private static void setRateValue(int offset, double value)
    {
        rates[offset] = value;
    }

    private final int countMetricId;
    private final int uncountedMetricId;
    private final int rateGroupId;
    private final long startTime;
    private final MonotonicClock clock;
    private long lastTick;

    public ThreadLocalMeter()
    {
        this(MonotonicClock.Global.approxTime);
    }

    public ThreadLocalMeter(MonotonicClock clock)
    {
        // movingAverages is set to null to reduce metrics memory footprint
        super(null, Clock.defaultClock());
        this.clock = clock;
        this.startTime = this.clock.now();
        this.lastTick = this.startTime;
        this.countMetricId = ThreadLocalMetrics.allocateMetricId();
        this.uncountedMetricId = ThreadLocalMetrics.allocateMetricId();
        this.rateGroupId = allocateRateGroupOffset();
        allMeters.add(new WeakReference<>(this));
        ThreadLocalMetrics.destroyWhenUnreachable(this, new MeterCleaner(countMetricId, uncountedMetricId, rateGroupId));
        ReflectionUtils.setFieldToNull(this, com.codahale.metrics.Meter.class, "count"); // to reduce metrics memory footprint
    }

    private static class MeterCleaner implements ThreadLocalMetrics.MetricCleaner
    {
        private final int countMetricId;
        private final int uncountedMetricId;
        private final int rateGroupId;

        private MeterCleaner(int countMetricId, int uncountedMetricId, int rateGroupId)
        {
            this.countMetricId = countMetricId;
            this.uncountedMetricId = uncountedMetricId;
            this.rateGroupId = rateGroupId;
        }

        @Override
        public void clean()
        {
            recycleRateGroupId(rateGroupId);
            ThreadLocalMetrics.recycleMetricId(countMetricId);
            ThreadLocalMetrics.recycleMetricId(uncountedMetricId);
        }

        private static void recycleRateGroupId(int rateGroupId)
        {
            synchronized (freeRateGroupIdSet)
            {
                freeRateGroupIdSet.set(rateGroupId);
            }
        }
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
        // we retrieve the context once here to reduce thread local lookup overheads
        ThreadLocalMetrics context = ThreadLocalMetrics.get();
        context.addNonStatic(countMetricId, n);
        context.addNonStatic(uncountedMetricId, n);
    }

    @Override
    public long getCount()
    {
        return ThreadLocalMetrics.getCount(countMetricId);
    }

    @Override
    public double getFifteenMinuteRate()
    {
        return getRatePerSecond(getRateValue(rateGroupId + M15_RATE_OFFSET));
    }

    @Override
    public double getFiveMinuteRate()
    {
        return getRatePerSecond(getRateValue(rateGroupId + M5_RATE_OFFSET));
    }

    @Override
    public double getOneMinuteRate()
    {
        return getRatePerSecond(getRateValue(rateGroupId + M1_RATE_OFFSET));
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
            long elapsed = clock.now() - startTime;
            return count / (elapsed * ONE_NS_IN_SEC);
        }
    }

    @VisibleForTesting
    static void tickAll()
    {
            List<WeakReference<ThreadLocalMeter>> emptyRefsToRemove = null;
            for (WeakReference<ThreadLocalMeter> threadLocalMeterRef : allMeters)
            {
                ThreadLocalMeter meter = threadLocalMeterRef.get();
                if (meter != null)
                {
                    meter.tickIfNessesary();
                }
                else
                {
                    if (emptyRefsToRemove == null)
                        emptyRefsToRemove = new ArrayList<>();
                    emptyRefsToRemove.add(threadLocalMeterRef);
                }
            }
            if (emptyRefsToRemove != null)
                allMeters.removeAll(emptyRefsToRemove);

    }

    private void tickIfNessesary()
    {
        long newTick = clock.now();
        long age = newTick - lastTick;
        if (age > TICK_INTERVAL_NS)
        {
            lastTick = newTick - age % TICK_INTERVAL_NS;
            long requiredTicks = age / TICK_INTERVAL_NS;
            tick(requiredTicks);
        }
    }
    private void tick(long requiredTicks)
    {
        synchronized (ratesArrayGuard)
        {
            if (requiredTicks >= maxTicks)
            {
                reset();
            }
            else if (requiredTicks > 0)
            {
                long count = ThreadLocalMetrics.getCountAndReset(uncountedMetricId);
                for (long i = 0; i < requiredTicks; i++)
                {
                    int m1Offset  = rateGroupId +  M1_RATE_OFFSET;
                    int m5Offset  = rateGroupId +  M5_RATE_OFFSET;
                    int m15Offset = rateGroupId + M15_RATE_OFFSET;
                    double m1Rate  = getRateValue(m1Offset);
                    double m5Rate  = getRateValue(m5Offset);
                    double m15Rate = getRateValue(m15Offset);
                    setRateValue(m1Offset,  tickOneMinuteEWMA(m1Rate, count));
                    setRateValue(m5Offset,  tickFiveMinuteEWMA(m5Rate, count));
                    setRateValue(m15Offset, tickFifteenMinuteEWMA(m15Rate, count));
                    count = 0;
                }
            }
        }
    }

    private static double tickOneMinuteEWMA(double oldRate, long count)
    {
        return tick(M1_ALPHA, oldRate, count);
    }

    private static double tickFiveMinuteEWMA(double oldRate, long count)
    {
        return tick(M5_ALPHA, oldRate, count);
    }

    private static double tickFifteenMinuteEWMA(double oldRate, long count)
    {
        return tick(M15_ALPHA, oldRate, count);
    }

    private static double tick(double alpha, double oldRate, long count)
    {
        double instantRate = (double) count / TICK_INTERVAL_NS;
        if (oldRate != NON_INITIALIZED)
            return oldRate + alpha * (instantRate - oldRate);
        else // init
            return instantRate;
    }
    private static double getRatePerSecond(double rate)
    {
        if (rate == NON_INITIALIZED)
            rate = 0.0;
        return rate * (double) TimeUnit.SECONDS.toNanos(1L);
    }

    /**
     * Set the rate to the smallest possible positive value. Used to avoid calling tick a large number of times.
     */
    private void reset()
    {
        ThreadLocalMetrics.getCountAndReset(uncountedMetricId);
        setRateValue(rateGroupId +  M1_RATE_OFFSET, Double.MIN_NORMAL);
        setRateValue(rateGroupId +  M5_RATE_OFFSET, Double.MIN_NORMAL);
        setRateValue(rateGroupId + M15_RATE_OFFSET, Double.MIN_NORMAL);
    }

    @VisibleForTesting
    static int getTickingMetersCount()
    {
        return allMeters.size();
    }
}
