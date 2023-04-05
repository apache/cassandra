/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.exp;
import com.codahale.metrics.Clock;

/**
 * A meter metric which measures mean throughput as well as fifteen-minute and two-hour
 * exponentially-weighted moving average throughputs.
 *
 * This is based heavily on the Meter and EWMA classes from codahale/yammer metrics.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class RestorableMeter
{
    private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5);
    private static final double NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    private final RestorableEWMA m15Rate;
    private final RestorableEWMA m120Rate;

    private final AtomicLong count = new AtomicLong();
    private final long startTime;
    private final AtomicLong lastTick;
    private final Clock clock = Clock.defaultClock();

    /**
     * Creates a new, uninitialized RestorableMeter.
     */
    public RestorableMeter()
    {
        this.m15Rate = new RestorableEWMA(TimeUnit.MINUTES.toSeconds(15));
        this.m120Rate = new RestorableEWMA(TimeUnit.MINUTES.toSeconds(120));
        this.startTime = this.clock.getTick();
        this.lastTick = new AtomicLong(startTime);
    }

    /**
     * Restores a RestorableMeter from the last seen 15m and 2h rates.
     * @param lastM15Rate the last-seen 15m rate, in terms of events per second
     * @param lastM120Rate the last seen 2h rate, in terms of events per second
     */
    public RestorableMeter(double lastM15Rate, double lastM120Rate)
    {
        this.m15Rate = new RestorableEWMA(lastM15Rate, TimeUnit.MINUTES.toSeconds(15));
        this.m120Rate = new RestorableEWMA(lastM120Rate, TimeUnit.MINUTES.toSeconds(120));
        this.startTime = this.clock.getTick();
        this.lastTick = new AtomicLong(startTime);
    }

    /**
     * Updates the moving averages as needed.
     */
    private void tickIfNecessary()
    {
        final long oldTick = lastTick.get();
        final long newTick = clock.getTick();
        final long age = newTick - oldTick;
        if (age > TICK_INTERVAL)
        {
            final long newIntervalStartTick = newTick - age % TICK_INTERVAL;
            if (lastTick.compareAndSet(oldTick, newIntervalStartTick))
            {
                final long requiredTicks = age / TICK_INTERVAL;
                for (long i = 0; i < requiredTicks; i++)
                {
                    m15Rate.tick();
                    m120Rate.tick();
                }
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
        tickIfNecessary();
        count.addAndGet(n);
        m15Rate.update(n);
        m120Rate.update(n);
    }

    /**
     * Returns the 15-minute rate in terms of events per second.  This carries the previous rate when restored.
     */
    public double fifteenMinuteRate()
    {
        tickIfNecessary();
        return m15Rate.rate();
    }

    /**
     * Returns the two-hour rate in terms of events per second.  This carries the previous rate when restored.
     */
    public double twoHourRate()
    {
        tickIfNecessary();
        return m120Rate.rate();
    }

    /**
     * The total number of events that have occurred since this object was created.  Note that the previous count
     * is *not* carried over when a RestorableMeter is restored.
     */
    public long count()
    {
        return count.get();
    }

    /**
     * Returns the mean rate of events per second since this object was created.  Note that the mean rate
     * does *not* carry over when a RestorableMeter is restored, so the mean rate is only a measure since
     * this object was created.
     */
    public double meanRate()
    {
        if (count() == 0)
        {
            return 0.0;
        } else {
            final long elapsed = (clock.getTick() - startTime);
            return (count() / (double) elapsed) * NANOS_PER_SECOND;
        }
    }

    static class RestorableEWMA
    {
        private volatile boolean initialized = false;
        private volatile double rate = 0.0; // average rate in terms of events per nanosecond

        private final AtomicLong uncounted = new AtomicLong();
        private final double alpha, interval;

        /**
         * Create a new, uninitialized EWMA with a given window.
         *
         * @param windowInSeconds the window of time this EWMA should average over, expressed as a number of seconds
         */
        public RestorableEWMA(long windowInSeconds)
        {
            this.alpha = 1 - exp((-TICK_INTERVAL / NANOS_PER_SECOND) / windowInSeconds);
            this.interval = TICK_INTERVAL;
        }

        /**
         * Restore an EWMA from a last-seen rate and a given window.
         *
         * @param intervalInSeconds the window of time this EWMA should average over, expressed as a number of seconds
         */
        public RestorableEWMA(double lastRate, long intervalInSeconds)
        {
            this(intervalInSeconds);
            this.rate = lastRate / NANOS_PER_SECOND;
            this.initialized = true;
        }

        /**
         * Update the moving average with a new value.
         */
        public void update(long n)
        {
            uncounted.addAndGet(n);
        }

        /**
         * Mark the passage of time and decay the current rate accordingly.
         */
        public void tick()
        {
            final long count = uncounted.getAndSet(0);
            final double instantRate = count / interval;
            if (initialized)
            {
                rate += (alpha * (instantRate - rate));
            }
            else
            {
                rate = instantRate;
                initialized = true;
            }
        }

        /**
         * Returns the rate in terms of events per second.
         */
        public double rate()
        {
            return rate * NANOS_PER_SECOND;
        }
    }
}
