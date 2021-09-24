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

import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.utils.KindOfSequence;
import org.apache.cassandra.simulator.utils.KindOfSequence.Period;
import org.apache.cassandra.simulator.utils.LongRange;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.apache.cassandra.utils.Shared;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.simulator.RandomSource.Choices.uniform;

// TODO (cleanup): when we encounter an exception and unwind the simulation, we should restore normal time to go with normal waits etc.
// TODO (now): configurable clock skew
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

    @Shared(scope = Shared.Scope.SIMULATION)
    public interface LocalTime extends Clock, MonotonicClock
    {
        long relativeNanosToAbsolute(long relativeNanos);
        long absoluteToRelativeNanos(long absoluteNanos);
        long localToGlobal(long absoluteNanos);
        long nextGlobalMonotonicMicros();
    }

    @PerClassLoader
    public static class Global implements Clock, MonotonicClock
    {
        private static LocalTime current;

        public long nanoTime()
        {
            return current.nanoTime();
        }

        public long currentTimeMillis()
        {
            return current.currentTimeMillis();
        }

        @Override
        public long now()
        {
            return current.now();
        }

        @Override
        public long error()
        {
            return current.error();
        }

        @Override
        public MonotonicClockTranslation translate()
        {
            return current.translate();
        }

        @Override
        public boolean isAfter(long instant)
        {
            return current.isAfter(instant);
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            return current.isAfter(now, instant);
        }

        public static long relativeToGlobalAbsoluteNanos(long relativeNanos)
        {
            return current.localToGlobal(current.relativeNanosToAbsolute(relativeNanos));
        }

        public static long relativeToAbsoluteNanos(long relativeNanos)
        {
            return current.relativeNanosToAbsolute(relativeNanos);
        }

        public static long absoluteToRelativeNanos(long absoluteNanos)
        {
            return current.absoluteToRelativeNanos(absoluteNanos);
        }

        public static long localToGlobalNanos(long absoluteNanos)
        {
            return current.localToGlobal(absoluteNanos);
        }

        public static LocalTime current()
        {
            return current;
        }

        @SuppressWarnings("unused") // used by simulator for schema changes
        public static long nextGlobalMonotonicMicros()
        {
            return current.nextGlobalMonotonicMicros();
        }

        public static void setup(LocalTime newLocalTime)
        {
            current = newLocalTime;
        }
    }

    private class InstanceTime implements LocalTime
    {
        final Period nanosDriftSupplier;
        long localNanoTime;
        long nanosDrift;

        private InstanceTime(Period nanosDriftSupplier)
        {
            this.nanosDriftSupplier = nanosDriftSupplier;
        }

        @Override
        public long nanoTime()
        {
            if (globalNanoTime + nanosDrift > localNanoTime)
            {
                localNanoTime = globalNanoTime + nanosDrift;
                nanosDrift = nanosDriftSupplier.get(random);
            }
            return localNanoTime;
        }

        @Override
        public long currentTimeMillis()
        {
            return NANOSECONDS.toMillis(nanoTime()) + millisEpoch;
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

        @Override
        public long nextGlobalMonotonicMicros()
        {
            return SimulatedTime.this.nextGlobalMonotonicMicros();
        }

        @Override
        public long relativeNanosToAbsolute(long relativeNanos)
        {
            return relativeNanos + localNanoTime;
        }

        @Override
        public long absoluteToRelativeNanos(long absoluteNanos)
        {
            return absoluteNanos - localNanoTime;
        }

        @Override
        public long localToGlobal(long absoluteNanos)
        {
            return absoluteNanos + (globalNanoTime - localNanoTime);
        }
    }

    private final KindOfSequence kindOfDrift;
    private final LongRange nanosDriftRange;
    private final RandomSource random;
    private final Period discontinuityTimeSupplier;
    private final long millisEpoch;
    private long globalNanoTime;
    private long futureTimestamp;
    private long discontinuityTime;
    private boolean permitDiscontinuities;

    public SimulatedTime(RandomSource random, long millisEpoch, LongRange nanoDriftRange, KindOfSequence kindOfDrift, Period discontinuityTimeSupplier)
    {
        this.random = random;
        this.millisEpoch = millisEpoch;
        this.nanosDriftRange = nanoDriftRange;
        this.futureTimestamp = (millisEpoch + DAYS.toMillis(1000)) * 1000;
        this.kindOfDrift = kindOfDrift;
        this.discontinuityTime = MILLISECONDS.toNanos(random.uniform(500L, 30000L));
        this.discontinuityTimeSupplier = discontinuityTimeSupplier;
    }

    public void setup(ClassLoader classLoader)
    {
        InstanceTime instanceTime = new InstanceTime(kindOfDrift.period(nanosDriftRange, random));
        IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableConsumer<LocalTime>) Global::setup, classLoader)
                        .accept(instanceTime);
    }

    public void permitDiscontinuities()
    {
        permitDiscontinuities = true;
        maybeApplyDiscontinuity();
    }

    private void maybeApplyDiscontinuity()
    {
        if (permitDiscontinuities && globalNanoTime >= discontinuityTime)
        {
            globalNanoTime += uniform(DAYS, HOURS, MINUTES).choose(random).toNanos(1L);
            discontinuityTime = globalNanoTime + discontinuityTimeSupplier.get(random);
        }
    }

    public void forbidDiscontinuities()
    {
        permitDiscontinuities = false;
    }

    public void tick(long nanos)
    {
        if (nanos > globalNanoTime)
        {
            globalNanoTime = nanos;
            maybeApplyDiscontinuity();
        }
        else
        {
            ++globalNanoTime;
        }
    }

    public long nanoTime()
    {
        return globalNanoTime;
    }

    // make sure schema changes persist
    public synchronized long nextGlobalMonotonicMicros()
    {
        return ++futureTimestamp;
    }
}
