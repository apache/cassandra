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

package org.apache.cassandra.simulator.systems;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;

import static java.lang.Math.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.simulator.ActionListener.*;

// TODO (now): when we encounter an exception and unwind the simulation, we should restore normal time to go with normal waits etc.
// TODO (future): configurable clock skew
public class SimulatedTime
{
    public static class Throwing implements Clock, MonotonicClock
    {
        public long nanoTime() { throw new IllegalStateException("Using time is not allowed during simulation"); }
        public long currentTimeMillis()  { throw new IllegalStateException("Using time is not allowed during simulation"); }
        public long now()  { throw new IllegalStateException("Using time is not allowed during simulation"); }
        public long error()  { throw new IllegalStateException("Using time is not allowed during simulation"); }
        public MonotonicClockTranslation translate()  { throw new IllegalStateException("Using time is not allowed during simulation"); }
        public boolean isAfter(long instant)  { throw new IllegalStateException("Using time is not allowed during simulation"); }
        public boolean isAfter(long now, long instant)  { throw new IllegalStateException("Using time is not allowed during simulation"); }
    }

    public static class Wrapped implements Clock, MonotonicClock
    {
        private static Clock systemClock;
        private static MonotonicClock monotonicClock;

        public long nanoTime()
        {
            return systemClock.nanoTime();
        }

        public long currentTimeMillis()
        {
            return systemClock.currentTimeMillis();
        }

        @Override
        public long now()
        {
            return monotonicClock.now();
        }

        @Override
        public long error()
        {
            return monotonicClock.error();
        }

        @Override
        public MonotonicClockTranslation translate()
        {
            return monotonicClock.translate();
        }

        @Override
        public boolean isAfter(long instant)
        {
            return monotonicClock.isAfter(instant);
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            return monotonicClock.isAfter(now, instant);
        }

        public static void setup(Clock newSystemClock, MonotonicClock newMonotonicClock)
        {
            systemClock = newSystemClock;
            monotonicClock = newMonotonicClock;
        }
    }

    private final RandomSource random;
    private final long millisEpoch, nanosPerMajorTick;
    private long maxNanos;
    private long futureTimestamp;
    private long minorTicks;

    private class InstanceTime implements Clock, MonotonicClock
    {
        long nanos, millis = millisEpoch;
        long ticks;

        void update()
        {
            if (ticks == minorTicks)
                return;

            do nanos += random.uniform(0, max(1, 1L << (3*Long.numberOfLeadingZeros(maxNanos - nanos)/4)));
            while (++ticks < minorTicks && nanos < maxNanos);
            ticks = minorTicks;
            millis = millisEpoch + NANOSECONDS.toMillis(nanos);
        }

        @Override
        public long nanoTime()
        {
            update();
            return ++nanos;
        }

        @Override
        public long currentTimeMillis()
        {
            update();
            return millis;
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
        public MonotonicClockTranslation translate()
        {
            return new MonotonicClockTranslation()
            {
                @Override
                public long fromMillisSinceEpoch(long currentTimeMillis)
                {
                    return MILLISECONDS.toNanos(currentTimeMillis - millisEpoch);
                }

                @Override
                public long toMillisSinceEpoch(long nanoTime)
                {
                    return NANOSECONDS.toMillis(nanoTime) + millisEpoch;
                }

                @Override
                public long error()
                {
                    return MILLISECONDS.toNanos(1L);
                }
            };
        }

        @Override
        public boolean isAfter(long instant)
        {
            return false;
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            return false;
        }
    }

    public SimulatedTime(RandomSource random, long millisEpoch, long nanosPerMajorTick)
    {
        this.random = random;
        this.millisEpoch = millisEpoch;
        this.maxNanos = this.nanosPerMajorTick = nanosPerMajorTick;
        this.futureTimestamp = (millisEpoch + TimeUnit.DAYS.toMillis(1000)) * 1000;
    }

    public void setup(ClassLoader classLoader)
    {
        InstanceTime instanceTime = new InstanceTime();
        IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableBiConsumer<Clock, MonotonicClock>) SimulatedTime.Wrapped::setup, classLoader)
                        .accept(instanceTime, instanceTime);
    }

    public void tick()
    {
        ++minorTicks;
    }

    public void majorTick()
    {
        maxNanos += nanosPerMajorTick;
    }

    class MajorTickListener implements SelfAddingActionListener
    {
        @Override
        public void after(Action finished)
        {
            majorTick();
        }
    }

    public void initialize(ActionList actions)
    {
        actions.forEach(new MajorTickListener());
    }

    // make sure schema changes persist
    public synchronized long futureTimestamp()
    {
        return ++futureTimestamp;
    }
}
