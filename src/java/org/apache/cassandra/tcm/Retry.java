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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public abstract class Retry
{
    protected static final int MAX_TRIES = DatabaseDescriptor.getDefaultRetryMaxTries();
    protected final int maxTries;
    protected int tries;

    public Retry()
    {
        this(MAX_TRIES);
    }

    public Retry(int maxTries)
    {
        this.maxTries = maxTries;
    }

    public int maxTries()
    {
        return maxTries;
    }

    public int currentTries()
    {
        return tries;
    }

    public boolean reachedMax()
    {
        return tries >= maxTries;
    }

    public void maybeSleep()
    {
        tries++;
        sleepUninterruptibly(sleepFor(), TimeUnit.MILLISECONDS);
    }

    protected abstract long sleepFor();

    public static class Jitter extends Retry
    {
        private static final int MAX_JITTER_MS = Math.toIntExact(DatabaseDescriptor.getDefaultRetryBackoff().to(TimeUnit.MILLISECONDS));
        private final Random random;
        private final int maxJitterMs;

        public Jitter()
        {
            this(MAX_TRIES, MAX_JITTER_MS, new Random());
        }

        public Jitter(int maxTries, int maxJitterMs, Random random)
        {
            super(maxTries);
            this.random = random;
            this.maxJitterMs = maxJitterMs;
        }

        public long sleepFor()
        {
            int actualBackoff = ThreadLocalRandom.current().nextInt(maxJitterMs / 2, maxJitterMs);
            return random.nextInt(actualBackoff);
        }
    }

    public static class Backoff extends Retry
    {
        private static final int RETRY_BACKOFF_MS = Math.toIntExact(DatabaseDescriptor.getDefaultRetryBackoff().to(TimeUnit.MILLISECONDS));
        protected final int backoffMs;

        public Backoff()
        {
            this(MAX_TRIES, RETRY_BACKOFF_MS);
        }

        public Backoff(int maxTries, int backoffMs)
        {
            super(maxTries);
            this.backoffMs = backoffMs;
        }

        public long sleepFor()
        {
            return (long) tries * backoffMs;
        }

        @Override
        public String toString()
        {
            return "Backoff{" +
                   "backoffMs=" + backoffMs +
                   ", maxTries=" + maxTries +
                   ", tries=" + tries +
                   '}';
        }
    }
}
