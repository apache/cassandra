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

package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.math.LongMath;

@ThreadSafe
@SuppressWarnings("UnstableApiUsage")
public class NoWaitRateLimiter
{
    @GuardedBy("this")
    private Stopwatch stopwatch;

    @GuardedBy("this")
    private SmoothRateLimiter delegate;

    private NoWaitRateLimiter(SmoothRateLimiter delegate, Ticker ticker)
    {
        this.delegate = delegate;
        this.stopwatch = Stopwatch.createStarted(ticker);
    }
    
    public static NoWaitRateLimiter create(double permitsPerSecond)
    {
        return create(permitsPerSecond, Ticker.systemTicker());
    }

    public static NoWaitRateLimiter create(double permitsPerSecond, Ticker ticker)
    {
        return create(permitsPerSecond, 1.0D, ticker);
    }

    public static NoWaitRateLimiter create(double permitsPerSecond, double burstSeconds)
    {
        return create(permitsPerSecond, burstSeconds, Ticker.systemTicker());
    }

    public static NoWaitRateLimiter create(double permitsPerSecond, double burstSeconds, Ticker ticker)
    {
        SmoothRateLimiter delegate = new SmoothRateLimiter(burstSeconds);
        NoWaitRateLimiter limiter = new NoWaitRateLimiter(delegate, ticker);
        limiter.setRate(permitsPerSecond);
        return limiter;
    }
    
    @VisibleForTesting
    public synchronized void reset(double permitsPerSecond, double burstSeconds, Ticker ticker)
    {
        this.stopwatch = Stopwatch.createStarted(ticker);
        this.delegate = new SmoothRateLimiter(burstSeconds);
        setRate(permitsPerSecond);
    }

    public final void setRate(double permitsPerSecond) 
    {
        Preconditions.checkArgument(permitsPerSecond > 0.0D && !Double.isNaN(permitsPerSecond), "rate must be positive");
        
        synchronized (this) 
        {
            delegate.doSetRate(permitsPerSecond, stopwatch.elapsed(TimeUnit.MICROSECONDS));
        }
    }

    public synchronized double getRate() 
    {
        return delegate.getRate();
    }

    /**
     * Forces the acquisition of a single permit.
     * 
     * @return microseconds until the acquired permit is available, and zero if it already is
     */
    public synchronized long reserveAndGetWaitLength() 
    {
        long nowMicros = this.stopwatch.elapsed(TimeUnit.MICROSECONDS);
        long momentAvailable = delegate.reserveEarliestAvailable(1, nowMicros);
        return Math.max(momentAvailable - nowMicros, 0L);
    }

    public synchronized boolean tryAcquire()
    {
        long nowMicros = this.stopwatch.elapsed(TimeUnit.MICROSECONDS);

        if (!delegate.canAcquire(nowMicros)) 
        {
            return false;
        }

        delegate.reserveEarliestAvailable(1, nowMicros);
            
        return true;
    }

    public synchronized boolean canAcquire() 
    {
        long nowMicros = this.stopwatch.elapsed(TimeUnit.MICROSECONDS);
        return delegate.canAcquire(nowMicros);
    }

    public synchronized long waitTimeMicros()
    {
        long nowMicros = this.stopwatch.elapsed(TimeUnit.MICROSECONDS);
        return delegate.waitTimeMicros(nowMicros);
    }

    @VisibleForTesting
    public synchronized long getNextFreeTicketMicros()
    {
        return delegate.nextFreeTicketMicros;
    }

    public synchronized double getStoredPermits()
    {
        return delegate.getStoredPermits();
    }
    
    @Override
    public String toString()
    {
        return String.format("Maximum requests/second is %.2f. %.2f stored permits available.", 
                             getRate(), getStoredPermits());
    }

    @NotThreadSafe
    private static class SmoothRateLimiter
    {
        private double storedPermits;
        private double maxPermits;
        private double stableIntervalMicros;
        private long nextFreeTicketMicros;
        private final double maxBurstSeconds;

        private SmoothRateLimiter(double maxBurstSeconds)
        {
            this.nextFreeTicketMicros = 0L;
            this.maxBurstSeconds = maxBurstSeconds;
        }
        
        public double getStoredPermits()
        {
            return storedPermits;
        }

        public void doSetRate(double permitsPerSecond, long nowMicros)
        {
            resync(nowMicros);
            this.stableIntervalMicros = (double) TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond;
            doSetRate(permitsPerSecond);
        }

        private void doSetRate(double permitsPerSecond)
        {
            double oldMaxPermits = this.maxPermits;
            this.maxPermits = this.maxBurstSeconds * permitsPerSecond;
            this.storedPermits = oldMaxPermits == 0.0D ? 0.0D : this.storedPermits * this.maxPermits / oldMaxPermits;
        }

        final double getRate()
        {
            return (double) TimeUnit.SECONDS.toMicros(1L) / this.stableIntervalMicros;
        }

        long waitTimeMicros(long nowMicros)
        {
            return Math.max(this.nextFreeTicketMicros - nowMicros, 0L);
        }

        boolean canAcquire(long nowMicros)
        {
            return nowMicros >= this.nextFreeTicketMicros;
        }

        public long reserveEarliestAvailable(int requiredPermits, long nowMicros)
        {
            resync(nowMicros);
            long momentAvailableMicros = this.nextFreeTicketMicros;
            double storedPermitsToSpend = Math.min(requiredPermits, this.storedPermits);
            double freshPermits = (double) requiredPermits - storedPermitsToSpend;
            long waitMicros = (long) (freshPermits * this.stableIntervalMicros);
            this.nextFreeTicketMicros = LongMath.saturatedAdd(this.nextFreeTicketMicros, waitMicros);
            this.storedPermits -= storedPermitsToSpend;
            return momentAvailableMicros;
        }

        private void resync(long nowMicros)
        {
            if (nowMicros > this.nextFreeTicketMicros)
            {
                double newPermits = (double) (nowMicros - this.nextFreeTicketMicros) / this.stableIntervalMicros;
                this.storedPermits = Math.min(this.maxPermits, this.storedPermits + newPermits);
                this.nextFreeTicketMicros = nowMicros;
            }
        }
    }
    
    public static final NoWaitRateLimiter NO_OP_LIMITER = new NoWaitRateLimiter(null, Ticker.systemTicker())
    {
        @Override
        public void reset(double permitsPerSecond, double burstSeconds, Ticker ticker)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long reserveAndGetWaitLength()
        {
            return 0;
        }

        @Override
        public boolean tryAcquire()
        {
            return true;
        }

        @Override
        public boolean canAcquire()
        {
            return true;
        }

        @Override
        public long waitTimeMicros()
        {
            return 0;
        }

        @Override
        public long getNextFreeTicketMicros()
        {
            throw new UnsupportedOperationException();
        }
    };
}
