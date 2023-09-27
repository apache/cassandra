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

import javax.annotation.Nullable;

import org.apache.cassandra.config.DurationSpec.LongMillisecondsBound;

public class RetrySpec
{
    public static class MaxAttempt
    {
        public static final MaxAttempt DISABLED = new MaxAttempt();

        public final int value;

        public MaxAttempt(int value)
        {
            if (value < 1)
                throw new IllegalArgumentException("max attempt must be positive; but given " + value);
            this.value = value;
        }

        private MaxAttempt()
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
            MaxAttempt that = (MaxAttempt) o;
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
            this.maxAttempts = null;
            this.baseSleepTime = null;
            this.maxSleepTime = null;
        }

        public RetrySpec withDefaults(RetrySpec defaultValues)
        {
            MaxAttempt maxAttempts = nonNull(this.maxAttempts, defaultValues.getMaxAttempts(), DEFAULT_MAX_ATTEMPTS);
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

    public static final MaxAttempt DEFAULT_MAX_ATTEMPTS = MaxAttempt.DISABLED;
    public static final LongMillisecondsBound DEFAULT_BASE_SLEEP = new LongMillisecondsBound("200ms");
    public static final LongMillisecondsBound DEFAULT_MAX_SLEEP = new LongMillisecondsBound("1s");

    /**
     * Represents how many retry attempts are allowed.  If the value is 2, this will cause 2 retries + 1 original request, for a total of 3 requests!
     * <p/>
     * To disable, set to 0.
     */
    public MaxAttempt maxAttempts = DEFAULT_MAX_ATTEMPTS; // 2 retries, 1 original request; so 3 total
    public LongMillisecondsBound baseSleepTime = DEFAULT_BASE_SLEEP;
    public LongMillisecondsBound maxSleepTime = DEFAULT_MAX_SLEEP;

    public RetrySpec()
    {
    }

    public RetrySpec(MaxAttempt maxAttempts, LongMillisecondsBound baseSleepTime, LongMillisecondsBound maxSleepTime)
    {
        this.maxAttempts = maxAttempts;
        this.baseSleepTime = baseSleepTime;
        this.maxSleepTime = maxSleepTime;
    }

    public boolean isEnabled()
    {
        return maxAttempts != MaxAttempt.DISABLED;
    }

    public void setEnabled(boolean enabled)
    {
        if (!enabled)
        {
            maxAttempts = MaxAttempt.DISABLED;
        }
        else if (maxAttempts == MaxAttempt.DISABLED)
        {
            maxAttempts = new MaxAttempt(2);
        }
    }

    @Nullable
    public MaxAttempt getMaxAttempts()
    {
        return !isEnabled() ? null : maxAttempts;
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

    @Override
    public String toString()
    {
        return "RetrySpec{" +
               "maxAttempts=" + maxAttempts +
               ", baseSleepTime=" + baseSleepTime +
               ", maxSleepTime=" + maxSleepTime +
               '}';
    }
}
