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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Clock;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.offset;
import static org.junit.Assert.assertEquals;

public class ThreadLocalMeterTest
{

    @Before
    public void before()
    {
        ThreadLocalMeter.disableBackgroundTicking();
    }

    @Test
    public void checkNoMark()
    {
        DeterministicClock clock = new DeterministicClock(0);
        ThreadLocalMeter meter = new ThreadLocalMeter(clock);
        com.codahale.metrics.Meter codahaleMeter = new com.codahale.metrics.Meter(clock);
        clock.setTime(TimeUnit.SECONDS.toNanos(10));
        ThreadLocalMeter.tickAll();
        assertMeter(meter, codahaleMeter);
    }

    @Test
    public void constantRate()
    {
        DeterministicClock clock = new DeterministicClock(0);
        ThreadLocalMeter meter = new ThreadLocalMeter(clock);
        com.codahale.metrics.Meter codahaleMeter = new com.codahale.metrics.Meter(clock);

        long seconds = TimeUnit.MINUTES.toSeconds(15);
        for (long i = 0; i < seconds; i++)
        {
            ThreadLocalMeter.tickAll();
            meter.mark();
            codahaleMeter.mark();
            clock.setTime(TimeUnit.SECONDS.toNanos(i + 1));
        }

        assertMeter(meter, codahaleMeter);
    }

    @Test
    public void marksEventsAndUpdatesRatesAndCount()
    {
        DeterministicClock clock = new DeterministicClock(0);
        ThreadLocalMeter meter = new ThreadLocalMeter(clock);
        com.codahale.metrics.Meter codahaleMeter = new com.codahale.metrics.Meter(clock);

        clock.setTime(TimeUnit.SECONDS.toNanos(10));
        ThreadLocalMeter.tickAll();
        meter.mark();
        meter.mark(2);
        codahaleMeter.mark();
        codahaleMeter.mark(2);

        assertMeter(meter, codahaleMeter);
    }

    @Test
    public void pseudoRandomRateSimulation()
    {
        Random random = new Random(1);
        DeterministicClock clock = new DeterministicClock(0);
        ThreadLocalMeter meter = new ThreadLocalMeter(clock);
        com.codahale.metrics.Meter codahaleMeter = new com.codahale.metrics.Meter(clock);

        int rounds = 10_000;
        for (int i = 0; i < rounds; i++)
        {
            long n = random.nextInt();
            ThreadLocalMeter.tickAll();
            meter.mark(n);
            codahaleMeter.mark(n);
            clock.setTime(clock.now() + random.nextInt());
        }
        assertMeter(meter, codahaleMeter);
    }

    @Test // CASSANDRA-19332
    public void testMaxTicks()
    {
        DeterministicClock clock = new DeterministicClock(0);
        ThreadLocalMeter threadLocalMeter = new ThreadLocalMeter(clock);
        clock.setTime(Long.MAX_VALUE);
        threadLocalMeter.mark(Long.MAX_VALUE);
        ThreadLocalMeter.tickAll();
        final long secondNanos = TimeUnit.SECONDS.toNanos(1);
        assertEquals(threadLocalMeter.getOneMinuteRate(), Double.MIN_NORMAL * secondNanos, 0.0);
        assertEquals(threadLocalMeter.getFiveMinuteRate(), Double.MIN_NORMAL * secondNanos, 0.0);
        assertEquals(threadLocalMeter.getFifteenMinuteRate(), Double.MIN_NORMAL * secondNanos, 0.0);
    }

    @Test
    public void testAllocationAndDestroy()
    {
        DeterministicClock clock = new DeterministicClock(0);
        Random random = new Random(42);
        List<MeterPair> meters = new ArrayList<>();
        for (int i = 0; i < 50_000; i++)
        {
            boolean create = random.nextBoolean();
            if (create)
            {
                MeterPair pair = new MeterPair();
                pair.meter = new ThreadLocalMeter(clock);
                pair.codahaleMeter = new com.codahale.metrics.Meter(clock);
                meters.add(pair);
            }
            else if (!meters.isEmpty())
            {
               int meterToRemove = random.nextInt(meters.size());
               meters.remove(meterToRemove);
            }
            ThreadLocalMeter.tickAll();
            for (MeterPair meterPair : meters)
            {
                meterPair.meter.mark();
                meterPair.codahaleMeter.mark();
                assertMeter(meterPair.meter, meterPair.codahaleMeter);
            }
            // note: Random.nextLong(long) is not available in Java 11
            clock.setTime(clock.now() + random.nextInt((int)TimeUnit.SECONDS.toNanos(10)));
        }

        int NUMBER_OF_COUNTERS_PER_METER = 2;
        Util.spinAssertEquals(NUMBER_OF_COUNTERS_PER_METER * meters.size(), () -> {
            System.gc(); // to trigger PhantomReferences queuing and recycle unused Meter thread local counters
            return ThreadLocalMetrics.getAllocatedMetricsCount();
        }, 20);

        ThreadLocalMeter.tickAll(); // to force recycling of empty weak references
        Assert.assertEquals(meters.size(), ThreadLocalMeter.getTickingMetersCount());
        Assert.assertEquals(1, ThreadLocalMetrics.getThreadLocalMetricsObjectsCount());

    }

    private static class MeterPair
    {
        ThreadLocalMeter meter;
        com.codahale.metrics.Meter codahaleMeter;

    }

    private static void assertMeter(ThreadLocalMeter checkingMeter, com.codahale.metrics.Meter standardMeter)
    {
        assertThat(checkingMeter.getCount()).isEqualTo(standardMeter.getCount());
        assertThat(checkingMeter.getMeanRate()).isEqualTo(standardMeter.getMeanRate(), offset(0.001));
        assertThat(checkingMeter.getOneMinuteRate()).isEqualTo(standardMeter.getOneMinuteRate(), offset(0.001));
        assertThat(checkingMeter.getFiveMinuteRate()).isEqualTo(standardMeter.getFiveMinuteRate(), offset(0.001));
        assertThat(checkingMeter.getFifteenMinuteRate()).isEqualTo(standardMeter.getFifteenMinuteRate(), offset(0.001));
    }

    private static class DeterministicClock extends Clock implements MonotonicClock
    {
        private volatile long tickNs;

        public DeterministicClock(long initialTime)
        {
            tickNs = initialTime;
        }

        public void setTime(long tickNs)
        {
            this.tickNs = tickNs;
        }

        @Override
        public long getTick()
        {
            return tickNs;
        }

        @Override
        public long now()
        {
            return tickNs;
        }

        @Override
        public long error()
        {
            return 0;
        }

        @Override
        public MonotonicClockTranslation translate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAfter(long instant)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            throw new UnsupportedOperationException();
        }
    }
}
