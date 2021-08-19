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
package org.apache.cassandra.net;

import org.apache.cassandra.exceptions.UnrecoverableIllegalStateException;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class ResourceLimits
{
    /**
     * Represents permits to utilise a resource and ways to allocate and release them.
     *
     * Two implementations are currently provided:
     * 1. {@link Concurrent}, for shared limits, which is thread-safe;
     * 2. {@link Basic}, for limits that are not shared between threads, is not thread-safe.
     */
    public interface Limit
    {
        /**
         * @return total amount of permits represented by this {@link Limit} - the capacity
         */
        long limit();

        /**
         * Sets the total amount of permits represented by this {@link Limit} - the capacity
         *
         * If the old limit has been reached and the new limit is large enough to allow for more
         * permits to be acquired, subsequent calls to {@link #allocate(long)} or {@link #tryAllocate(long)}
         * will succeed.
         *
         * If the new limit is lower than the current amount of allocated permits then subsequent calls
         * to {@link #allocate(long)} or {@link #tryAllocate(long)} will block or fail respectively.
         *
         * @return the old limit
         */
        long setLimit(long newLimit);

        /**
         * @return remaining, unallocated permit amount
         */
        long remaining();

        /**
         * @return amount of permits currently in use
         */
        long using();

        /**
         * Attempts to allocate an amount of permits from this limit. If allocated, <em>MUST</em> eventually
         * be released back with {@link #release(long)}.
         *
         * @return {@code true} if the allocation was successful, {@code false} otherwise
         */
        boolean tryAllocate(long amount);

        /**
         * Allocates an amount independent of permits available from this limit. <em>MUST</em> eventually
         * be released back with {@link #release(long)}.
         *
         */
        void allocate(long amount);

        /**
         * @param amount return the amount of permits back to this limit
         * @return {@code ABOVE_LIMIT} if there aren't enough permits available even after the release, or
         *         {@code BELOW_LIMIT} if there are enough permits available after the releaese.
         */
        Outcome release(long amount);
    }

    /**
     * A thread-safe permit container.
     */
    public static class Concurrent implements Limit
    {
        private volatile long limit;
        private static final AtomicLongFieldUpdater<Concurrent> limitUpdater =
            AtomicLongFieldUpdater.newUpdater(Concurrent.class, "limit");

        private volatile long using;
        private static final AtomicLongFieldUpdater<Concurrent> usingUpdater =
            AtomicLongFieldUpdater.newUpdater(Concurrent.class, "using");

        public Concurrent(long limit)
        {
            this.limit = limit;
        }

        public long limit()
        {
            return limit;
        }

        public long setLimit(long newLimit)
        {
            long oldLimit;
            do {
                oldLimit = limit;
            } while (!limitUpdater.compareAndSet(this, oldLimit, newLimit));

            return oldLimit;
        }

        public long remaining()
        {
            return limit - using;
        }

        public long using()
        {
            return using;
        }

        public boolean tryAllocate(long amount)
        {
            long current, next;
            do
            {
                current = using;
                next = current + amount;

                if (next > limit)
                    return false;
            }
            while (!usingUpdater.compareAndSet(this, current, next));

            return true;
        }

        public void allocate(long amount)
        {
            long current, next;
            do
            {
                current = using;
                next = current + amount;
            } while (!usingUpdater.compareAndSet(this, current, next));
        }

        public Outcome release(long amount)
        {
            assert amount >= 0;
            long using = usingUpdater.addAndGet(this, -amount);
            if (using < 0L)
            {
                // Should never be able to release more than was allocated.  While recovery is
                // possible it would require synchronizing the closing of all outbound connections
                // and reinitializing the Concurrent limit before reopening.  For such an unlikely path
                // (previously this was an assert), it is safer to terminate the JVM and have something external
                // restart and get back to a known good state rather than intermittently crashing on any of
                // the connections sharing this limit.
                throw new UnrecoverableIllegalStateException(
                    "Internode messaging byte limits that are shared between connections is invalid (using="+using+")");
            }
            return using >= limit ? Outcome.ABOVE_LIMIT : Outcome.BELOW_LIMIT;
        }
    }

    /**
     * A cheaper, thread-unsafe permit container to be used for unshared limits.
     */
    public static class Basic implements Limit
    {
        private long limit;
        private long using;

        public Basic(long limit)
        {
            this.limit = limit;
        }

        public long limit()
        {
            return limit;
        }

        public long setLimit(long newLimit)
        {
            long oldLimit = limit;
            limit = newLimit;

            return oldLimit;
        }

        public long remaining()
        {
            return limit - using;
        }

        public long using()
        {
            return using;
        }

        public boolean tryAllocate(long amount)
        {
            if (using + amount > limit)
                return false;

            using += amount;
            return true;
        }

        public void allocate(long amount)
        {
            using += amount;
        }

        public Outcome release(long amount)
        {
            assert amount >= 0 && amount <= using;
            using -= amount;
            return using >= limit ? Outcome.ABOVE_LIMIT : Outcome.BELOW_LIMIT;
        }
    }

    /**
     * A convenience class that groups a per-endpoint limit with the global one
     * to allow allocating/releasing permits from/to both limits as one logical operation.
     */
    public static class EndpointAndGlobal
    {
        final Limit endpoint;
        final Limit global;

        public EndpointAndGlobal(Limit endpoint, Limit global)
        {
            this.endpoint = endpoint;
            this.global = global;
        }

        public Limit endpoint()
        {
            return endpoint;
        }

        public Limit global()
        {
            return global;
        }

        /**
         * @return {@code INSUFFICIENT_GLOBAL} if there weren't enough permits in the global limit, or
         *         {@code INSUFFICIENT_ENDPOINT} if there weren't enough permits in the per-endpoint limit, or
         *         {@code SUCCESS} if there were enough permits to take from both.
         */
        public Outcome tryAllocate(long amount)
        {
            if (!global.tryAllocate(amount))
                return Outcome.INSUFFICIENT_GLOBAL;

            if (endpoint.tryAllocate(amount))
                return Outcome.SUCCESS;

            global.release(amount);
            return Outcome.INSUFFICIENT_ENDPOINT;
        }

        public void allocate(long amount)
        {
            global.allocate(amount);
            endpoint.allocate(amount);
        }

        public Outcome release(long amount)
        {
            Outcome endpointReleaseOutcome = endpoint.release(amount);
            Outcome globalReleaseOutcome = global.release(amount);
            return (endpointReleaseOutcome == Outcome.ABOVE_LIMIT || globalReleaseOutcome == Outcome.ABOVE_LIMIT)
                   ? Outcome.ABOVE_LIMIT : Outcome.BELOW_LIMIT;
        }
    }

    public enum Outcome { SUCCESS, INSUFFICIENT_ENDPOINT, INSUFFICIENT_GLOBAL, BELOW_LIMIT, ABOVE_LIMIT }
}
