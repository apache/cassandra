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

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Clock;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.tcm.Retry.Jitter.MAX_JITTER_MS;

/**
 * Represents a strategy for retrying operations in the event of failures or issues.
 * It provides mechanisms to manage and control retry attempts, respecting certain conditions and utilizing different
 * backoff strategies to avoid overwhelming systems and to handle transient failures gracefully.
 */
public abstract class Retry
{
    protected static final int MAX_TRIES = DatabaseDescriptor.getCmsDefaultRetryMaxTries();
    protected final int maxTries;

    /**
     * The number of attempts made so far.
     */
    protected int tries;
    protected Meter retryMeter;

    public Retry(Meter retryMeter)
    {
        this(MAX_TRIES, retryMeter);
    }

    public Retry(int maxTries, Meter retryMeter)
    {
        this.maxTries = maxTries;
        this.retryMeter = retryMeter;
    }

    /**
     * Returns the current number of attempts.
     * @return the current number of attempts.
     */
    public int currentTries()
    {
        return tries;
    }

    /**
     * Determines whether the retry strategy has reached the maximum retry attempts or has surpassed the deadline.
     *
     * @return {@code true} if the maximum retry attempts are reached or the deadline has been surpassed; {@code false} otherwise.
     */
    public boolean reachedMax()
    {
        return tries >= maxTries;
    }

    /**
     * Sleep if needed.
     */
    public void maybeSleep()
    {
        tries++;
        retryMeter.mark();
        sleepUninterruptibly(sleepFor(), TimeUnit.MILLISECONDS);
    }

    /**
     * Determines the duration to sleep before the next retry attempt.
     *
     * @return the duration to sleep in milliseconds.
     */
    protected abstract long sleepFor();

    /**
     * Represents a retry strategy that introduces a randomized delay (jitter)
     * between retry attempts.
     */
    public static class Jitter extends Retry
    {
        public static final int MAX_JITTER_MS = Math.toIntExact(DatabaseDescriptor.getDefaultRetryBackoff().to(TimeUnit.MILLISECONDS));
        private final Random random;

        /**
         * The maximum ammount of jitter per milliseconds
         */
        private final int maxJitterMs;

        public Jitter(Meter retryMeter)
        {
            this(MAX_TRIES, MAX_JITTER_MS, new Random(), retryMeter);
        }

        private Jitter(int maxTries, int maxJitterMs, Random random, Meter retryMeter)
        {
            super(maxTries, retryMeter);
            this.random = random;
            this.maxJitterMs = maxJitterMs;
        }

        @Override
        public long sleepFor()
        {
            int actualBackoff = ThreadLocalRandom.current().nextInt(maxJitterMs / 2, maxJitterMs);
            return random.nextInt(actualBackoff);
        }
    }

    /**
     * Retry strategy that introduces a fixed or exponentially increasing delay between retry attempts,
     * allowing for a more conservative retry approach.
     */
    public static class Backoff extends Retry
    {
        private static final int RETRY_BACKOFF_MS = Math.toIntExact(DatabaseDescriptor.getDefaultRetryBackoff().to(TimeUnit.MILLISECONDS));

        /**
         * The initial delay between retries in milliseconds
         */
        protected final int backoffMs;

        /**
         * Constructs a new {@code Backoff} instance using specified retry meter.
         *
         * @param retryMeter a {@code Meter} instance used to keep track of retry attempts.
         */
        public Backoff(Meter retryMeter)
        {
            this(MAX_TRIES, RETRY_BACKOFF_MS, retryMeter);
        }

        public Backoff(int maxTries, int backoffMs, Meter retryMeter)
        {
            super(maxTries, retryMeter);
            this.backoffMs = backoffMs;
        }

        @Override
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

    /**
     * {@code Retry} strategy that enforces a deadline, ensuring that retry attempts are halted after a certain point in time,
     * preventing indefinite retries.
     * <p>{@code Deadline} will retry using provided delegate but will ensure that the attempts stop after the deadline has been reached.</p>
     */
    public static class Deadline extends Retry
    {
        /**
         * The deadline in nanoseconds
         */
        public final long deadlineNanos;
        /**
         * The decorated {@code Retry} used to perform the retry attempts under the hood.
         */
        protected final Retry delegate;

        private Deadline(long deadlineNanos, Retry delegate)
        {
            super(delegate.maxTries, delegate.retryMeter);
            assert deadlineNanos > 0 : String.format("Deadline should be strictly positive but was %d.", deadlineNanos);
            this.deadlineNanos = deadlineNanos;
            this.delegate = delegate;
        }

        /**
         * Creates a {@code Deadline} that will stop retrying at the specified time.
         *
         * @param deadlineNanos the number of nanoseconds at which the {@code Deadline} should stop retrying
         * @param delegate the {@code Retry} to which the {@code Deadline} will delegate the retries.
         * @return a new {@code Deadline} that will stop retrying at the specified time.
         */
        public static Deadline at(long deadlineNanos, Retry delegate)
        {
            return new Deadline(deadlineNanos, delegate);
        }

        /**
         * Creates a {@code Deadline} that will stop retrying after the specified amount of nanoseconds.
         *
         * @param timeoutNanos the number of nanoseconds after which the {@code Deadline} should stop retrying
         * @param delegate the {@code Retry} to which the {@code Deadline} will delegate the retries.
         * @return a new {@code Deadline} that will stop retrying after the specified amount of nanoseconds.
         */
        public static Deadline after(long timeoutNanos, Retry delegate)
        {
            return new Deadline(Clock.Global.nanoTime() + timeoutNanos, delegate);
        }

        /**
         * Since we are using message expiration for communicating timeouts to CMS nodes, we have to be careful not
         * to overflow the long, since messaging is using only 32 bits for deadlines. To achieve that, we are
         * giving `timeoutNanos` every time we retry, but will retry indefinitely.
         */
        public static Deadline retryIndefinitely(long timeoutNanos, Meter retryMeter)
        {
            return new Deadline(Clock.Global.nanoTime() + timeoutNanos,
                                new Retry.Jitter(Integer.MAX_VALUE, MAX_JITTER_MS, new Random(), retryMeter))
            {
                @Override
                public boolean reachedMax()
                {
                    return false;
                }

                @Override
                public long remainingNanos()
                {
                    return timeoutNanos;
                }
            };
        }

        @Override
        public boolean reachedMax()
        {
            return delegate.reachedMax() || Clock.Global.nanoTime() > deadlineNanos;
        }

        /**
         * Calculates the remaining time until the deadline.
         *
         * @return the remaining time in nanoseconds.
         */
        public long remainingNanos()
        {
            return Math.max(0, deadlineNanos - Clock.Global.nanoTime());
        }

        @Override
        public int currentTries()
        {
            return delegate.currentTries();
        }

        @Override
        public long sleepFor()
        {
            return delegate.sleepFor();
        }

        public String toString()
        {
            return String.format("Deadline{remainingMs=%d, tries=%d/%d}", TimeUnit.NANOSECONDS.toMillis(remainingNanos()), currentTries(), delegate.maxTries);
        }
    }

}
