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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Intercept;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface Semaphore
{
    /**
     * @return the number of permits presently in this semaphore
     */
    int permits();

    /**
     * set the number of permits in this semaphore to zero
     */
    int drain();

    /**
     * Increase the number of available permits and signal any waiters that may be served by the release
     */
    void release(int permits);

    /**
     * Try to take permits, returning immediately
     * @return true iff permits acquired
     */
    boolean tryAcquire(int acquire);

    /**
     * Try to take permits, waiting up to timeout
     * @return true iff permits acquired
     * @throws InterruptedException if interrupted
     */
    boolean tryAcquire(int acquire, long time, TimeUnit unit) throws InterruptedException;

    /**
     * Try to take permits, waiting until the deadline
     * @return true iff permits acquired
     * @throws InterruptedException if interrupted
     */
    boolean tryAcquireUntil(int acquire, long nanoTimeDeadline) throws InterruptedException;

    /**
     * Take permits, waiting indefinitely until available
     * @throws InterruptedException if interrupted
     */
    void acquire(int acquire) throws InterruptedException;

    /**
     * Take permits, waiting indefinitely until available
     * @throws UncheckedInterruptedException if interrupted
     */
    void acquireThrowUncheckedOnInterrupt(int acquire) throws UncheckedInterruptedException;

    /**
     * Factory method used to capture and redirect instantiations for simulation
     *
     * Construct an unfair Semaphore initially holding the specified number of permits
     */
    @Intercept
    public static Semaphore newSemaphore(int permits)
    {
        return new Standard(permits, false);
    }

    /**
     * Factory method used to capture and redirect instantiations for simulation
     *
     * Construct a fair Semaphore initially holding the specified number of permits
     */
    @Intercept
    public static Semaphore newFairSemaphore(int permits)
    {
        return new Standard(permits, true);
    }

    public static class Standard extends java.util.concurrent.Semaphore implements Semaphore
    {
        public Standard(int permits)
        {
            this(permits, false);
        }

        public Standard(int permits, boolean fair)
        {
            super(permits, fair);
        }

        /**
         * {@link Semaphore#drain()}
         */
        public int drain()
        {
            return drainPermits();
        }

        /**
         * Number of permits that are available to be acquired. {@link Semaphore#permits()}
         */
        public int permits()
        {
            return availablePermits();
        }

        /**
         * Number of permits that have been acquired in excess of available. {@link Semaphore#permits()}
         */
        public int waiting()
        {
            return getQueueLength();
        }

        /**
         * {@link Semaphore#tryAcquireUntil(int, long)}
         */
        public boolean tryAcquireUntil(int acquire, long nanoTimeDeadline) throws InterruptedException
        {
            long wait = nanoTimeDeadline - System.nanoTime();
            return tryAcquire(acquire, Math.max(0, wait), TimeUnit.NANOSECONDS);
        }

        @Override
        public void acquireThrowUncheckedOnInterrupt(int acquire) throws UncheckedInterruptedException
        {
            try
            {
                acquire(acquire);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
        }
    }
}
