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
import java.util.function.DoubleSupplier;

import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.tcm.Retry;

public interface Backoff
{
    boolean mayRetry(int attempt);
    long computeWaitTime(int attempt);
    TimeUnit unit();

    static Backoff fromRetry(Retry retry)
    {
        return new Backoff()
        {
            @Override
            public boolean mayRetry(int attempt)
            {
                return !retry.reachedMax();
            }

            @Override
            public long computeWaitTime(int retryCount)
            {
                return retry.computeSleepFor();
            }

            @Override
            public TimeUnit unit()
            {
                return TimeUnit.MILLISECONDS;
            }
        };
    }

    static Backoff fromConfig(SharedContext ctx, RetrySpec spec)
    {
        if (!spec.isEnabled())
            return Backoff.None.INSTANCE;
        return new Backoff.ExponentialBackoff(spec.maxAttempts.value, spec.baseSleepTime.toMilliseconds(), spec.maxSleepTime.toMilliseconds(), ctx.random().get()::nextDouble);
    }

    enum None implements Backoff
    {
        INSTANCE;

        @Override
        public boolean mayRetry(int attempt)
        {
            return false;
        }

        @Override
        public long computeWaitTime(int retryCount)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TimeUnit unit()
        {
            throw new UnsupportedOperationException();
        }
    }

    class ExponentialBackoff implements Backoff
    {
        private final int maxAttempts;
        private final long baseSleepTimeMillis;
        private final long maxSleepMillis;
        private final DoubleSupplier randomSource;

        public ExponentialBackoff(int maxAttempts, long baseSleepTimeMillis, long maxSleepMillis, DoubleSupplier randomSource)
        {
            this.maxAttempts = maxAttempts;
            this.baseSleepTimeMillis = baseSleepTimeMillis;
            this.maxSleepMillis = maxSleepMillis;
            this.randomSource = randomSource;
        }

        public int maxAttempts()
        {
            return maxAttempts;
        }

        @Override
        public boolean mayRetry(int attempt)
        {
            return attempt < maxAttempts;
        }

        @Override
        public long computeWaitTime(int retryCount)
        {
            long baseTimeMillis = baseSleepTimeMillis * (1L << retryCount);
            // it's possible that this overflows, so fall back to max;
            if (baseTimeMillis <= 0)
                baseTimeMillis = maxSleepMillis;
            // now make sure this is capped to target max
            baseTimeMillis = Math.min(baseTimeMillis, maxSleepMillis);

            return (long) (baseTimeMillis * (randomSource.getAsDouble() + 0.5));
        }

        @Override
        public TimeUnit unit()
        {
            return TimeUnit.MILLISECONDS;
        }
    }
}
