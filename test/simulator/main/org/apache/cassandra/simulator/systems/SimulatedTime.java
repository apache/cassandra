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

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.utils.KindOfSequence;
import org.apache.cassandra.simulator.utils.KindOfSequence.Period;
import org.apache.cassandra.simulator.utils.LongRange;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClock.AbstractEpochSamplingClock.AlmostSameTime;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.apache.cassandra.utils.Shared;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.simulator.RandomSource.Choices.uniform;

// TODO (cleanup): when we encounter an exception and unwind the simulation, we should restore normal time to go with normal waits etc.
public class SimulatedTime
{
    private static final Pattern PERMITTED_TIME_THREADS = Pattern.compile("(logback|SimulationLiveness|Reconcile)[-:][0-9]+");

    @Shared(scope = Shared.Scope.SIMULATION)
    public interface Listener
    {
        void accept(String kind, long value);
    }

    @Shared(scope = Shared.Scope.SIMULATION)
    public interface ClockAndMonotonicClock extends Clock, MonotonicClock
    {
    }

    @Shared(scope = Shared.Scope.SIMULATION)
    public interface LocalTime extends ClockAndMonotonicClock
    {
        long relativeToLocalNanos(long relativeNanos);
        long relativeToGlobalNanos(long relativeNanos);
        long localToRelativeNanos(long absoluteLocalNanos);
        long localToGlobalNanos(long absoluteLocalNanos);
        long nextGlobalMonotonicMicros();
    }

    @PerClassLoader
    private static class Disabled extends Clock.Default implements LocalTime
    {
        @Override
        public long now()
        {
            return nanoTime();
        }

        @Override
        public long error()
        {
            return 0;
        }

        @Override
        public MonotonicClockTranslation translate()
        {
            return new AlmostSameTime(System.currentTimeMillis(), System.nanoTime(), 0L);
        }

        @Override
        public boolean isAfter(long instant)
        {
            return isAfter(System.nanoTime(), instant);
        }

        @Override
        public boolean isAfter(long now, long instant)
        {
            return now > instant;
        }

        @Override
        public long relativeToLocalNanos(long relativeNanos)
        {
            return System.nanoTime() + relativeNanos;
        }

        @Override
        public long relativeToGlobalNanos(long relativeNanos)
        {
            return System.nanoTime() + relativeNanos;
        }

        @Override
        public long localToRelativeNanos(long absoluteLocalNanos)
        {
            return absoluteLocalNanos - System.nanoTime();
        }

        @Override
        public long localToGlobalNanos(long absoluteLocalNanos)
        {
            return absoluteLocalNanos;
        }

        @Override
        public long nextGlobalMonotonicMicros()
        {
            return FBUtilities.timestampMicros();
        }
    }

    public static class Delegating implements ClockAndMonotonicClock
    {
        final Disabled disabled = new Disabled();
        private ClockAndMonotonicClock check()
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                InterceptibleThread interceptibleThread = ((InterceptibleThread) thread);
                if (interceptibleThread.isIntercepting())
                    return interceptibleThread.time();
            }
            if (PERMITTED_TIME_THREADS.matcher(Thread.currentThread().getName()).matches())
                return disabled;
            throw new IllegalStateException("Using time is not allowed during simulation");
        }

        public long nanoTime() { return check().nanoTime(); }
        public long currentTimeMillis()  { return check().currentTimeMillis(); }
        public long now()  { return check().now(); }
        public long error()  { return check().error(); }
        public MonotonicClockTranslation translate()  { return check().translate(); }
        public boolean isAfter(long instant)  { return check().isAfter(instant); }
        public boolean isAfter(long now, long instant)  { return check().isAfter(now, instant); }
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

        public static long relativeToGlobalNanos(long relativeNanos)
        {
            return current.relativeToGlobalNanos(relativeNanos);
        }

        public static long relativeToLocalNanos(long relativeNanos)
        {
            return current.relativeToLocalNanos(relativeNanos);
        }

        public static long localToRelativeNanos(long absoluteNanos)
        {
            return current.localToRelativeNanos(absoluteNanos);
        }

        public static long localToGlobalNanos(long absoluteNanos)
        {
            return current.localToGlobalNanos(absoluteNanos);
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

        public static void disable()
        {
            current = new Disabled();
        }
    }

    public class InstanceTime implements LocalTime
    {
        final Period nanosDriftSupplier;
        long from, to;
        long baseDrift, nextDrift, lastLocalNanoTime, lastDrift, lastGlobal;
        double diffPerGlobal;

        private InstanceTime(Period nanosDriftSupplier)
        {
            this.nanosDriftSupplier = nanosDriftSupplier;
        }

        @Override
        public long nanoTime()
        {
            long global = globalNanoTime;
            if (lastGlobal == global)
                return lastLocalNanoTime;

            if (global >= to)
            {
                baseDrift = nextDrift;
                nextDrift = nanosDriftSupplier.get(random);
                from = global;
                to = global + Math.max(baseDrift, nextDrift);
                diffPerGlobal = (nextDrift - baseDrift) / (double)(to - from);
                listener.accept("SetNextDrift", nextDrift);
            }

            long drift = baseDrift + (long)(diffPerGlobal * (global - from));
            long local = global + drift;
            lastGlobal = global;
            lastDrift = drift;
            lastLocalNanoTime = local;
            listener.accept("ReadLocal", local);
            return local;
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
        public long relativeToLocalNanos(long relativeNanos)
        {
            return relativeNanos + lastLocalNanoTime;
        }

        @Override
        public long relativeToGlobalNanos(long relativeNanos)
        {
            return relativeNanos + globalNanoTime;
        }

        @Override
        public long localToRelativeNanos(long absoluteLocalNanos)
        {
            return absoluteLocalNanos - lastLocalNanoTime;
        }

        @Override
        public long localToGlobalNanos(long absoluteLocalNanos)
        {
            return absoluteLocalNanos - lastDrift;
        }
    }

    private final KindOfSequence kindOfDrift;
    private final LongRange nanosDriftRange;
    private final RandomSource random;
    private final Period discontinuityTimeSupplier;
    private final long millisEpoch;
    private volatile long globalNanoTime;
    private long futureTimestamp;
    private long discontinuityTime;
    private boolean permitDiscontinuities;
    private final List<LongConsumer> onDiscontinuity = new ArrayList<>();
    private final Listener listener;
    private InstanceTime[] instanceTimes;

    public SimulatedTime(int nodeCount, RandomSource random, long millisEpoch, LongRange nanoDriftRange, KindOfSequence kindOfDrift, Period discontinuityTimeSupplier, Listener listener)
    {
        this.random = random;
        this.millisEpoch = millisEpoch;
        this.nanosDriftRange = nanoDriftRange;
        this.futureTimestamp = (millisEpoch + DAYS.toMillis(1000)) * 1000;
        this.kindOfDrift = kindOfDrift;
        this.discontinuityTime = MILLISECONDS.toNanos(random.uniform(500L, 30000L));
        this.discontinuityTimeSupplier = discontinuityTimeSupplier;
        this.listener = listener;
        this.instanceTimes = new InstanceTime[nodeCount];
    }

    public Closeable setup(int nodeNum, ClassLoader classLoader)
    {
        Preconditions.checkState(instanceTimes[nodeNum - 1] == null);
        InstanceTime instanceTime = new InstanceTime(kindOfDrift.period(nanosDriftRange, random));
        IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableConsumer<LocalTime>) Global::setup, classLoader)
                        .accept(instanceTime);
        instanceTimes[nodeNum - 1] = instanceTime;
        return IsolatedExecutor.transferAdhoc((IIsolatedExecutor.SerializableRunnable) Global::disable, classLoader)::run;
    }

    public InstanceTime get(int nodeNum)
    {
        return instanceTimes[nodeNum - 1];
    }

    public void permitDiscontinuities()
    {
        listener.accept("PermitDiscontinuity", 1);
        permitDiscontinuities = true;
        updateAndMaybeApplyDiscontinuity(globalNanoTime);
    }

    public void forbidDiscontinuities()
    {
        listener.accept("PermitDiscontinuity", 0);
        permitDiscontinuities = false;
    }

    private void updateAndMaybeApplyDiscontinuity(long newGlobal)
    {
        if (permitDiscontinuities && newGlobal >= discontinuityTime)
        {
            updateAndApplyDiscontinuity(newGlobal);
        }
        else
        {
            globalNanoTime = newGlobal;
            listener.accept("SetGlobal", newGlobal);
        }
    }

    private void updateAndApplyDiscontinuity(long newGlobal)
    {
        long discontinuity = uniform(DAYS, HOURS, MINUTES).choose(random).toNanos(1L);
        listener.accept("ApplyDiscontinuity", discontinuity);
        discontinuityTime = newGlobal + discontinuity + discontinuityTimeSupplier.get(random);
        globalNanoTime = newGlobal + discontinuity;
        listener.accept("SetGlobal", newGlobal + discontinuity);
        onDiscontinuity.forEach(l -> l.accept(discontinuity));
    }

    public void tick(long nanos)
    {
        listener.accept("Tick", nanos);
        long global = globalNanoTime;
        if (nanos > global)
        {
            updateAndMaybeApplyDiscontinuity(nanos);
        }
        else
        {
            globalNanoTime = global + 1;
            listener.accept("IncrGlobal", global + 1);
        }
    }

    public long nanoTime()
    {
        long global = globalNanoTime;
        listener.accept("ReadGlobal", global);
        return global;
    }

    // make sure schema changes persist
    public synchronized long nextGlobalMonotonicMicros()
    {
        return ++futureTimestamp;
    }

    public void onDiscontinuity(LongConsumer onDiscontinuity)
    {
        this.onDiscontinuity.add(onDiscontinuity);
    }

    public void onTimeEvent(String kind, long value)
    {
        listener.accept(kind, value);
    }
}
