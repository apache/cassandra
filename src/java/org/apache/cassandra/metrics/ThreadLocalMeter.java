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

import com.codahale.metrics.Clock;
import static java.lang.Math.exp;

public class ThreadLocalMeter implements Meter
{
    private final ThreadLocalExponentialMovingAverages movingAverages;
    private final int countMetricId;
    private final long startTime;
    private final Clock clock;

    public ThreadLocalMeter() {
        this(Clock.defaultClock());
    }

    public ThreadLocalMeter(Clock clock) {
        this.movingAverages = new ThreadLocalExponentialMovingAverages(clock);
        this.clock = clock;
        this.startTime = this.clock.getTick();
        this.countMetricId = PiggybackArrayThreadLocalMetrics.getMetricId();
    }

    /**
     * Mark the occurrence of an event.
     */
    public void mark() {
        mark(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n) {
        PiggybackArrayThreadLocalMetrics context = PiggybackArrayThreadLocalMetrics.get();
        movingAverages.tickIfNecessary();
        context.addNonStatic(countMetricId, n);
        movingAverages.update(context, n);
    }

    @Override
    public long getCount() {
        return PiggybackArrayThreadLocalMetrics.getCount(countMetricId);
    }

    @Override
    public double getFifteenMinuteRate() {
        movingAverages.tickIfNecessary();
        return movingAverages.getM15Rate();
    }

    @Override
    public double getFiveMinuteRate() {
        movingAverages.tickIfNecessary();
        return movingAverages.getM5Rate();
    }

    @Override
    public double getOneMinuteRate() {
        movingAverages.tickIfNecessary();
        return movingAverages.getM1Rate();
    }

    @Override
    public double getMeanRate() {
        long count = getCount();
        if (count == 0) {
            return 0.0;
        } else {
            final double elapsed = clock.getTick() - startTime;
            return count / elapsed * TimeUnit.SECONDS.toNanos(1);
        }
    }

    private static class ThreadLocalExponentialMovingAverages
    {
        private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(EWMA.INTERVAL_SEC);
        private final EWMA m1Rate;
        private final EWMA m5Rate;
        private final EWMA m15Rate;
        private final AtomicLong lastTick;
        private final Clock clock;

        public ThreadLocalExponentialMovingAverages(Clock clock) {
            this.m1Rate = EWMA.oneMinuteEWMA();
            this.m5Rate = EWMA.fiveMinuteEWMA();
            this.m15Rate = EWMA.fifteenMinuteEWMA();
            this.clock = clock;
            this.lastTick = new AtomicLong(this.clock.getTick());
        }

        public void update(PiggybackArrayThreadLocalMetrics context, long n) {
            this.m1Rate.update(context, n);
            this.m5Rate.update(context, n);
            this.m15Rate.update(context, n);
        }

        public void tickIfNecessary() {
            long oldTick = this.lastTick.get();
            long newTick = this.clock.getTick();
            long age = newTick - oldTick;
            if (age > TICK_INTERVAL) {
                long newIntervalStartTick = newTick - age % TICK_INTERVAL;
                if (this.lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
                    long requiredTicks = age / TICK_INTERVAL;

                    for(long i = 0L; i < requiredTicks; ++i) {
                        this.m1Rate.tick();
                        this.m5Rate.tick();
                        this.m15Rate.tick();
                    }
                }
            }
        }

        public double getM1Rate() {
            return this.m1Rate.getRatePerSecond();
        }

        public double getM5Rate() {
            return this.m5Rate.getRatePerSecond();
        }

        public double getM15Rate() {
            return this.m15Rate.getRatePerSecond();
        }
    }

    // TODO: check if we can avoid using a separate object for EWMA
    public static class EWMA {
        private static final int INTERVAL_SEC = 5;
        private static final long INTERVAL_NANO = TimeUnit.SECONDS.toNanos(INTERVAL_SEC);
        private static final double SECONDS_PER_MINUTE = 60.0;
        private static final int ONE_MINUTE = 1;
        private static final int FIVE_MINUTES = 5;
        private static final int FIFTEEN_MINUTES = 15;
        private static final double M1_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / ONE_MINUTE);
        private static final double M5_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / FIVE_MINUTES);
        private static final double M15_ALPHA = 1 - exp(-INTERVAL_SEC / SECONDS_PER_MINUTE / FIFTEEN_MINUTES);


        private volatile boolean initialized = false;
        private volatile double rate = 0.0;
        private final int uncountedMetricId;
        private final double alpha;

        public static EWMA oneMinuteEWMA() {
            return new EWMA(M1_ALPHA);
        }

        public static EWMA fiveMinuteEWMA() {
            return new EWMA(M5_ALPHA);
        }

        public static EWMA fifteenMinuteEWMA() {
            return new EWMA(M15_ALPHA);
        }

        public EWMA(double alpha) {
            this.alpha = alpha;
            this.uncountedMetricId = PiggybackArrayThreadLocalMetrics.getMetricId();
        }

        public void update(PiggybackArrayThreadLocalMetrics context, long n) {
            context.addNonStatic(uncountedMetricId, n);
        }

        public void tick() {
            // TODO: check how to make tick cheaper
            // we can skip dead threads check for ticks executed as a part of a meter mark
            // we can try to replace a global rate and ticks with local rates..
            long count = PiggybackArrayThreadLocalMetrics.getCountAndReset(uncountedMetricId);
            double instantRate = (double)count / INTERVAL_NANO;
            if (this.initialized) {
                double oldRate = this.rate;
                this.rate = oldRate + this.alpha * (instantRate - oldRate);
            } else {
                this.rate = instantRate;
                this.initialized = true;
            }
        }

        public double getRatePerSecond() {
            return this.rate * (double)TimeUnit.SECONDS.toNanos(1L);
        }
    }
}
