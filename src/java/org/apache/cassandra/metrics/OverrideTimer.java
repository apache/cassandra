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

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;

/**
 * An alternative to Dropwizard Timer which implements the same kind of API.
 * it has more efficent latency histogram implementation and consumes less memory.
 *
 * NOTE: Dropwizard Timer is a concrete class and there is no an interface for Dropwizard Timer logic,
 *   so we have to create an alternative hierarchy.
 */
public class OverrideTimer extends com.codahale.metrics.Timer implements Timer
{
    protected final Meter meter;
    protected final OverrideHistogram histogram;
    // usually we need precise clocks for timing, so we do not replace it with an approximate version
    protected final MetricClock clock;

    /**
     * Creates a new {@link Timer} using an {@link ExponentiallyDecayingReservoir} and the default
     * {@link MetricClock}.
     */
    public OverrideTimer()
    {
        this(new DecayingEstimatedHistogramReservoir());
    }

    /**
     * Creates a new {@link Timer} that uses the given {@link Reservoir}.
     *
     * @param reservoir the {@link Reservoir} implementation the timer should use
     */
    public OverrideTimer(CassandraReservoir reservoir)
    {
        this(reservoir, MetricClock.defaultClock());
    }

    /**
     * Creates a new {@link Timer} that uses the given {@link Reservoir} and {@link MetricClock}.
     *
     * @param reservoir the {@link Reservoir} implementation the timer should use
     * @param clock     the {@link MetricClock} implementation the timer should use
     */
    public OverrideTimer(CassandraReservoir reservoir, MetricClock clock)
    {
        // the precise clock is intentionally not propagated to ThreadLocalMeter
        // we do not need a precise and more expensive time within the meter
        this(new OverrideMeter(clock), new OverrideHistogram(reservoir), clock);
    }

    public OverrideTimer(Meter meter, OverrideHistogram histogram, MetricClock clock)
    {
        // original Codahale meter and histogram are set to null to reduce memory footprint
        super(null, null, clock);
        this.meter = meter;
        this.histogram = histogram;
        this.clock = clock;
    }

    /**
     * Adds a recorded duration.
     *
     * @param duration the length of the duration
     * @param unit     the scale unit of {@code duration}
     */
    @Override
    public void update(long duration, TimeUnit unit)
    {
        update(unit.toNanos(duration));
    }

    /**
     * Adds a recorded duration.
     *
     * @param duration the {@link Duration} to add to the timer. Negative or zero value are ignored.
     */
    @Override
    public void update(Duration duration)
    {
        update(duration.toNanos());
    }

    /**
     * Times and records the duration of event.
     *
     * @param event a {@link Callable} whose {@link Callable#call()} method implements a process
     *              whose duration should be timed
     * @param <T>   the type of the value returned by {@code event}
     * @return the value returned by {@code event}
     * @throws Exception if {@code event} throws an {@link Exception}
     */
    @Override
    public <T> T time(Callable<T> event) throws Exception
    {
        final long startTime = clock.getTick();
        try
        {
            return event.call();
        }
        finally
        {
            update(clock.getTick() - startTime);
        }
    }

    /**
     * Times and records the duration of event. Should not throw exceptions, for that use the
     * {@link #time(Callable)} method.
     *
     * @param event a {@link Supplier} whose {@link Supplier#get()} method implements a process
     *              whose duration should be timed
     * @param <T>   the type of the value returned by {@code event}
     * @return the value returned by {@code event}
     */
    @Override
    public <T> T timeSupplier(Supplier<T> event)
    {
        final long startTime = clock.getTick();
        try
        {
            return event.get();
        }
        finally
        {
            update(clock.getTick() - startTime);
        }
    }

    /**
     * Times and records the duration of event.
     *
     * @param event a {@link Runnable} whose {@link Runnable#run()} method implements a process
     *              whose duration should be timed
     */
    @Override
    public void time(Runnable event)
    {
        final long startTime = clock.getTick();
        try
        {
            event.run();
        }
        finally
        {
            update(clock.getTick() - startTime);
        }
    }

    @Override
    public Timer.Context startTime()
    {
        return new Timer.Context(this, clock);
    }

    @Override
    public long getCount()
    {
        return histogram.getCount();
    }

    @Override
    public double getFifteenMinuteRate()
    {
        return meter.getFifteenMinuteRate();
    }

    @Override
    public double getFiveMinuteRate()
    {
        return meter.getFiveMinuteRate();
    }

    @Override
    public double getMeanRate()
    {
        return meter.getMeanRate();
    }

    @Override
    public double getOneMinuteRate()
    {
        return meter.getOneMinuteRate();
    }

    @Override
    public Snapshot getSnapshot()
    {
        return histogram.getSnapshot();
    }

    private void update(long duration)
    {
        if (duration >= 0)
        {
            histogram.update(duration);
            meter.mark();
        }
    }

    public CassandraReservoir.BucketStrategy bucketStrategy()
    {
        return histogram.bucketStrategy();
    }

    public long[] bucketStarts(int length)
    {
        return histogram.reservoir.buckets(length);
    }

    public boolean isCumulative()
    {
        return true;
    }
}
