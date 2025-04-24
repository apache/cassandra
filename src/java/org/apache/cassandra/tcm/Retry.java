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

package org.apache.cassandra.tcm;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory;
import org.apache.cassandra.service.WaitStrategy;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

// TODO (expected): unwrap this, use RetryStrategy directly
public class Retry implements WaitStrategy
{
    private static final WaitStrategy DEFAULT_STRATEGY;
    static
    {
        DurationSpec.IntMillisecondsBound defaultBackoff = DatabaseDescriptor.getDefaultRetryBackoff();
        DurationSpec.IntMillisecondsBound defaultMaxBackoff = DatabaseDescriptor.getDefaultMaxRetryBackoff();
        String defaultSpec = DatabaseDescriptor.getCMSRetryDelay();
        if (defaultSpec == null || (defaultBackoff != null || defaultMaxBackoff != null))
        {
            defaultSpec = (defaultBackoff == null ? "50ms" : defaultBackoff.toMilliseconds() + "ms")
                          + "*attempts <=" + (defaultMaxBackoff == null ? "10s" : defaultMaxBackoff.toMilliseconds() + "ms")
                          + ",retries=" + DatabaseDescriptor.getCmsDefaultRetryMaxTries();
        }
        DEFAULT_STRATEGY = RetryStrategy.parse(defaultSpec, LatencySourceFactory.none());
    }

    public final long deadlineNanos;
    protected Meter retryMeter;
    private final WaitStrategy delegate;
    int attempts = 1;

    public Retry(long deadlineNanos, Meter retryMeter, WaitStrategy delegate)
    {
        this.deadlineNanos = deadlineNanos;
        this.retryMeter = retryMeter;
        this.delegate = delegate;
    }

    public Retry(long deadlineNanos, Meter retryMeter)
    {
        this(deadlineNanos, retryMeter, DEFAULT_STRATEGY);
    }

    public int attempts()
    {
        return attempts;
    }

    public boolean hasExpired()
    {
        return nanoTime() >= deadlineNanos;
    }

    public boolean maybeSleep()
    {
        long wait = computeWait(attempts, TimeUnit.MILLISECONDS);
        if (wait < 0)
            return false;
        sleepUninterruptibly(wait, TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public long computeWaitUntil(int attempts)
    {
        long wait = computeWaitInternal(attempts, TimeUnit.NANOSECONDS);
        if (wait < 0)
            return -1;
        long now = nanoTime();
        if (now >= deadlineNanos)
            return -1;
        return Math.min(deadlineNanos, wait + now);
    }

    @Override
    public long computeWait(int attempts, TimeUnit units)
    {
        long wait = computeWaitInternal(attempts, TimeUnit.NANOSECONDS);
        if (wait < 0)
            return -1;

        if (deadlineNanos == Long.MAX_VALUE)
            return wait;

        long now = nanoTime();
        wait = Math.min(deadlineNanos - now, wait);
        if (wait <= 0)
            return -1;
        return units.convert(wait, TimeUnit.NANOSECONDS);
    }

    private long computeWaitInternal(int attempts, TimeUnit units)
    {
        retryMeter.mark();
        attempts = Math.max(attempts, ++this.attempts);
        return delegate.computeWait(attempts, units);
    }

    // imposes attempt limit
    public static Retry withNoTimeLimit(Meter retryMeter)
    {
        return new Retry(Long.MAX_VALUE, retryMeter, DEFAULT_STRATEGY);
    }

    public static Retry withNoTimeLimit(Meter retryMeter, WaitStrategy delegate)
    {
        return new Retry(Long.MAX_VALUE, retryMeter, delegate);
    }

    public static Retry until(long deadlineNanos, Meter retryMeter)
    {
        return new Retry(deadlineNanos, retryMeter, DEFAULT_STRATEGY);
    }

    public static Retry untilElapsed(long timeoutNanos, Meter retryMeter)
    {
        return new Retry(nanoTime() + timeoutNanos, retryMeter, DEFAULT_STRATEGY);
    }

    public static Retry untilElapsed(long timeoutNanos, Meter retryMeter, WaitStrategy waitStrategy)
    {
        return new Retry(nanoTime() + timeoutNanos, retryMeter, waitStrategy);
    }

    public String toString()
    {
        if (deadlineNanos == Long.MAX_VALUE)
            return "RetryIndefinitely{attempts=" + attempts + '}';
        return String.format("Retry{remainingMs=%d, attempts=%d}", TimeUnit.NANOSECONDS.toMillis(remainingNanos()), attempts());
    }

    public long remainingNanos()
    {
        return Math.max(0, deadlineNanos - nanoTime());
    }
}
