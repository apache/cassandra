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

public interface Backoff
{
    /**
     * @return max attempts allowed, {@code == 0} implies no retries are allowed
     */
    int maxAttempts();
    long computeWaitTime(int retryCount);
    TimeUnit unit();

    enum None implements Backoff
    {
        INSTANCE;

        @Override
        public int maxAttempts()
        {
            return 0;
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

        @Override
        public int maxAttempts()
        {
            return maxAttempts;
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
