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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Ticker;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NoWaitRateLimiterTest
{
    @Test
    public void shouldProperlyInitializeRate()
    {
        NoWaitRateLimiter limiter = NoWaitRateLimiter.create(1000.0);
        assertEquals(1000.0, limiter.getRate(), 0.0);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void shouldAdvanceWaitTime()
    {
        Ticker ticker = new Ticker()
        {
            @Override
            public long read()
            {
                return 0;
            }
        };

        double permitsPerSecond = 1000.0;
        NoWaitRateLimiter limiter = NoWaitRateLimiter.create(permitsPerSecond, 0.0, ticker);
        assertEquals(0.0, limiter.waitTimeMicros(), 0.0);
        limiter.reserveAndGetWaitLength();
        assertEquals(permitsPerSecond, limiter.waitTimeMicros(), 0.0);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void shouldAdvanceNextFreeTicketMicrosWithZeroBurst()
    {
        double permitsPerSecond = 1000.0;
        
        Ticker ticker = new Ticker()
        {
            @Override
            public long read()
            {
                return 0;
            }
        };
        
        NoWaitRateLimiter limiter = NoWaitRateLimiter.create(permitsPerSecond, 0.0, ticker);
        long start = limiter.getNextFreeTicketMicros();
        limiter.reserveAndGetWaitLength();
        assertEquals(permitsPerSecond, limiter.getNextFreeTicketMicros() - start, 0.0);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void shouldNotAcquireWhenPermitsExhaustedAndBeforeNextTicket()
    {
        double permitsPerSecond = 1000.0;
        final AtomicLong tick = new AtomicLong(0);

        Ticker ticker = new Ticker()
        {
            @Override
            public long read()
            {
                return tick.get();
            }
        };

        NoWaitRateLimiter limiter = NoWaitRateLimiter.create(permitsPerSecond, 0.0, ticker);
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.canAcquire());
        
        // Advance the clock to the next ticket time, and verify we can acquire again:
        tick.addAndGet(TimeUnit.MICROSECONDS.toNanos((long) permitsPerSecond));
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.canAcquire());
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void shouldNotAcquireAfterConumingAllStoredPermits()
    {
        double permitsPerSecond = 1000.0;
        double burstSeconds = 1.0;
        final AtomicLong tick = new AtomicLong(0);

        Ticker ticker = new Ticker()
        {
            @Override
            public long read()
            {
                return tick.get();
            }
        };

        NoWaitRateLimiter limiter = NoWaitRateLimiter.create(permitsPerSecond, burstSeconds, ticker);
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.canAcquire());
        
        // Advance the clock the number of burst seconds and then consume all the stored permits.
        tick.addAndGet(TimeUnit.SECONDS.toNanos((long) burstSeconds));
        
        for (int i = 0; i < burstSeconds * permitsPerSecond; i++)
        {
            limiter.reserveAndGetWaitLength();
        }

        assertFalse(limiter.canAcquire());

        // Advance the clock to the next ticket time, and verify we can acquire again:
        tick.addAndGet(TimeUnit.MICROSECONDS.toNanos((long) permitsPerSecond));
        assertTrue(limiter.tryAcquire());
        assertFalse(limiter.tryAcquire());
    }
}
