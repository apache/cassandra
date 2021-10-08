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

package org.apache.cassandra.utils.concurrent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.toIntExact;

/**
 * A rate limiter implementation that allows callers to reserve permits that may only be available 
 * in the future, delegating to them decisions about how to schedule/delay work and whether or not
 * to block execution to do so.
 */
@SuppressWarnings("UnstableApiUsage")
@ThreadSafe
public class NonBlockingRateLimiter
{
    public static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    static final long DEFAULT_BURST_NANOS = NANOS_PER_SECOND;

    /** a starting time for elapsed time calculations */
    private final long startedNanos;

    /** nanoseconds from start time corresponding to the next available permit */
    private final AtomicLong nextAvailable = new AtomicLong();
    
    private volatile Ticker ticker;
    
    private volatile int permitsPerSecond;
    
    /** time in nanoseconds between permits on the timeline */
    private volatile long intervalNanos;

    /**
     * To allow the limiter to more closely adhere to the configured rate in the face of
     * unevenly distributed permits requests, it will allow a number of permits equal to
     * burstNanos / intervalNanos to be issued in a "burst" before reaching a steady state.
     * 
     * Another way to think about this is that it allows us to bring forward the permits
     * from short periods of inactivity. This is especially useful when the upstream user
     * of the limiter delays request processing mechanism contains overhead that is longer
     * than the intervalNanos in duration.
     */
    private final long burstNanos;

    public NonBlockingRateLimiter(int permitsPerSecond)
    {
        this(permitsPerSecond, DEFAULT_BURST_NANOS, Ticker.systemTicker());
    }

    @VisibleForTesting
    public NonBlockingRateLimiter(int permitsPerSecond, long burstNanos, Ticker ticker)
    {
        this.startedNanos = ticker.read();
        this.burstNanos = burstNanos;
        setRate(permitsPerSecond, ticker);
    }

    public void setRate(int permitsPerSecond)
    {
        setRate(permitsPerSecond, Ticker.systemTicker());
    }

    @VisibleForTesting
    public synchronized void setRate(int permitsPerSecond, Ticker ticker)
    {
        Preconditions.checkArgument(permitsPerSecond > 0, "permits/second must be positive");
        Preconditions.checkArgument(permitsPerSecond <= NANOS_PER_SECOND, "permits/second cannot be greater than " + NANOS_PER_SECOND);

        this.ticker = ticker;
        this.permitsPerSecond = permitsPerSecond;
        intervalNanos = NANOS_PER_SECOND / permitsPerSecond;
        nextAvailable.set(nanosElapsed());
    }

    /**
     * @return the number of available permits per second
     */
    public int getRate()
    {
        return permitsPerSecond;
    }

    /**
     * Reserves a single permit slot on the timeline which may not yet be available.
     *
     * @return time until the reserved permit will be available (or zero if it already is) in the specified units
     */
    public long reserveAndGetDelay(TimeUnit delayUnit)
    {
        long nowNanos = nanosElapsed();

        for (;;)
        {
            long prev = nextAvailable.get();
            long interval = this.intervalNanos;

            // Push the first available permit slot up to the burst window if necessary.
            long firstAvailable = Math.max(prev, nowNanos - burstNanos);

            // Advance the configured interval starting from the bounded previous permit slot.
            if (nextAvailable.compareAndSet(prev, firstAvailable + interval))
                // If the time now is before the first available slot, return the delay.  
                return delayUnit.convert(Math.max(0,  firstAvailable - nowNanos), TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Reserves a single permit slot on the timeline, but only if one is available.
     *
     * @return true if a permit is available, false if one is not
     */
    public boolean tryReserve()
    {
        long nowNanos = nanosElapsed();
    
        for (;;)
        {
            long prev = nextAvailable.get();
            long interval = this.intervalNanos;
    
            // Push the first available permit slot up to the burst window if necessary.
            long firstAvailable = Math.max(prev, nowNanos - burstNanos);
            
            // If we haven't reached the time for the first available permit, we've failed to reserve. 
            if (nowNanos < firstAvailable)
                return false;
    
            // Advance the configured interval starting from the bounded previous permit slot.
            // If another thread has already taken the next slot, retry.
            if (nextAvailable.compareAndSet(prev, firstAvailable + interval))
                return true;
        }
    }

    @VisibleForTesting
    public long getIntervalNanos()
    {
        return intervalNanos;
    }
    
    @VisibleForTesting
    public long getStartedNanos()
    {
        return startedNanos;
    }

    private long nanosElapsed()
    {
        return ticker.read() - startedNanos;
    }

    public static final NonBlockingRateLimiter NO_OP_LIMITER = new NonBlockingRateLimiter(toIntExact(NANOS_PER_SECOND))
    {
        @Override
        public long reserveAndGetDelay(TimeUnit delayUnit) {
            return 0;
        }

        @Override
        public boolean tryReserve()
        {
            return true;
        }
    };
}
