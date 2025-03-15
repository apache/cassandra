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

package org.apache.cassandra.config;

import java.util.Objects;
import java.util.Random;

import javax.annotation.Nullable;

import accord.utils.DefaultRandom;
import org.apache.cassandra.config.DurationSpec.LongMillisecondsBound;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.service.TimeoutStrategy.LatencySupplier.Constant;
import org.apache.cassandra.service.TimeoutStrategy.Wait.Modifying;
import org.apache.cassandra.service.WaitStrategy;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.service.RetryStrategy.randomizers;
import static org.apache.cassandra.service.TimeoutStrategy.modifiers;

public class RetrySpec
{
    public static class MaxRetry
    {
        public static final MaxRetry DISABLED = new MaxRetry();

        public final int value;

        public MaxRetry(int value)
        {
            if (value < 1)
                throw new IllegalArgumentException("max attempt must be positive; but given " + value);
            this.value = value;
        }

        private MaxRetry()
        {
            value = 0;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null) return false;
            if (o instanceof Integer) return this.value == ((Integer) o).intValue();
            if (getClass() != o.getClass()) return false;
            MaxRetry that = (MaxRetry) o;
            return value == that.value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value);
        }

        @Override
        public String toString()
        {
            return Integer.toString(value);
        }
    }

    public static class Partial extends RetrySpec
    {
        public Partial()
        {
            this.maxRetries = null;
            this.baseSleepTime = null;
            this.maxSleepTime = null;
        }

        public RetrySpec withDefaults(RetrySpec defaultValues)
        {
            MaxRetry maxAttempts = nonNull(this.maxRetries, defaultValues.getMaxRetries(), DEFAULT_MAX_RETRIES);
            LongMillisecondsBound baseSleepTime = nonNull(this.baseSleepTime, defaultValues.getBaseSleepTime(), DEFAULT_BASE_SLEEP);
            LongMillisecondsBound maxSleepTime = nonNull(this.maxSleepTime, defaultValues.getMaxSleepTime(), DEFAULT_MAX_SLEEP);
            return new RetrySpec(maxAttempts, baseSleepTime, maxSleepTime);
        }

        private static <T> T nonNull(@Nullable T left, @Nullable T right, T defaultValue)
        {
            if (left != null)
                return left;
            if (right != null)
                return right;
            return defaultValue;
        }
    }

    public static final MaxRetry DEFAULT_MAX_RETRIES = MaxRetry.DISABLED;
    public static final LongMillisecondsBound DEFAULT_BASE_SLEEP = new LongMillisecondsBound("200ms");
    public static final LongMillisecondsBound DEFAULT_MAX_SLEEP = new LongMillisecondsBound("1s");

    /**
     * Represents how many retry attempts are allowed.  If the value is 2, this will cause 2 retries + 1 original request, for a total of 3 requests!
     * <p/>
     * To disable, set to 0.
     */
    public MaxRetry maxRetries = DEFAULT_MAX_RETRIES; // 2 retries, 1 original request; so 3 total
    public LongMillisecondsBound baseSleepTime = DEFAULT_BASE_SLEEP;
    public LongMillisecondsBound maxSleepTime = DEFAULT_MAX_SLEEP;

    public RetrySpec()
    {
    }

    public RetrySpec(MaxRetry maxRetries, LongMillisecondsBound baseSleepTime, LongMillisecondsBound maxSleepTime)
    {
        this.maxRetries = maxRetries;
        this.baseSleepTime = baseSleepTime;
        this.maxSleepTime = maxSleepTime;
    }

    public boolean isEnabled()
    {
        return maxRetries != MaxRetry.DISABLED;
    }

    public void setEnabled(boolean enabled)
    {
        if (!enabled)
        {
            maxRetries = MaxRetry.DISABLED;
        }
        else if (maxRetries == MaxRetry.DISABLED)
        {
            maxRetries = new MaxRetry(2);
        }
    }

    @Nullable
    public MaxRetry getMaxRetries()
    {
        return !isEnabled() ? null : maxRetries;
    }

    @Nullable
    public LongMillisecondsBound getBaseSleepTime()
    {
        return !isEnabled() ? null : baseSleepTime;
    }

    public LongMillisecondsBound getMaxSleepTime()
    {
        return !isEnabled() ? null : maxSleepTime;
    }

    public static WaitStrategy toStrategy(SharedContext ctx, RetrySpec spec)
    {
        if (!spec.isEnabled())
            return WaitStrategy.None.INSTANCE;
        return doublingWaitStrategy(spec.maxRetries.value, spec.baseSleepTime.to(MICROSECONDS), spec.maxSleepTime.to(MICROSECONDS), ctx.random().get());
    }

    @Override
    public String toString()
    {
        return "RetrySpec{" +
               "maxAttempts=" + maxRetries +
               ", baseSleepTime=" + baseSleepTime +
               ", maxSleepTime=" + maxSleepTime +
               '}';
    }

    // note: maxAttempts here excludes the initial attempt, so we are permitted this many retries
    private static WaitStrategy doublingWaitStrategy(int maxRetries, long baseSleepTimeMicros, long maxSleepMicros, Random random)
    {
        return new RetryStrategy(randomizers(new DefaultRandom(random)).uniform(),
                                 0,
                                 doublingWait(baseSleepTimeMicros / 2),
                                 doublingWait(baseSleepTimeMicros + (baseSleepTimeMicros / 2)),
                                 maxSleepMicros, maxRetries);
    }

    private static TimeoutStrategy.Wait doublingWait(long baseSleepTimeMicros)
    {
        return new Modifying(new Constant(baseSleepTimeMicros), modifiers.doubleByRetries());
    }
}
