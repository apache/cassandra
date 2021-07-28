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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import net.openhft.chronicle.core.util.ThrowingConsumer;
import org.apache.cassandra.utils.Intercept;
import org.apache.cassandra.utils.Shared;

import static java.lang.System.nanoTime;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;
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
        return new UnfairAsync(permits);
    }

    /**
     * Factory method used to capture and redirect instantiations for simulation
     *
     * Construct a fair Semaphore initially holding the specified number of permits
     */
    @Intercept
    public static Semaphore newFairSemaphore(int permits)
    {
        return new FairJDK(permits);
    }

    /**
     * An unfair semaphore, making no guarantees about thread starvation.
     *
     * TODO this Semaphore is potentially inefficient if used with release quantities other than 1
     *      (this is unimportant at time of authoring)
     */
    public static class UnfairAsync implements Semaphore
    {
        private static final AtomicReferenceFieldUpdater<UnfairAsync, WaitQueue> waitingUpdater = AtomicReferenceFieldUpdater.newUpdater(UnfairAsync.class, WaitQueue.class, "waiting");
        private static final AtomicIntegerFieldUpdater<UnfairAsync> permitsUpdater = AtomicIntegerFieldUpdater.newUpdater(UnfairAsync.class, "permits");
        private volatile WaitQueue waiting;
        private volatile int permits;

        // WARNING: if extending this class, consider simulator interactions
        public UnfairAsync(int permits)
        {
            this.permits = permits;
        }

        /**
         * {@link Semaphore#drain()}
         */
        public int drain()
        {
            return permitsUpdater.getAndSet(this, 0);
        }

        /**
         * {@link Semaphore#permits()}
         */
        public int permits()
        {
            return permits;
        }

        /**
         * {@link Semaphore#release(int)}
         */
        public void release(int permits)
        {
            if (permits < 0) throw new IllegalArgumentException();
            if (permits > 0 && permitsUpdater.getAndAdd(this, permits) == 0)
            {
                if (waiting != null)
                {
                    if (permits > 1) waiting.signalAll();
                    else waiting.signal();
                }
            }
        }

        /**
         * {@link Semaphore#tryAcquire(int)}
         */
        public boolean tryAcquire(int acquire)
        {
            if (acquire < 0)
                throw new IllegalArgumentException();
            while (true)
            {
                int cur = permits;
                if (cur < acquire)
                    return false;
                if (permitsUpdater.compareAndSet(this, cur, cur - acquire))
                    return true;
            }
        }

        /**
         * {@link Semaphore#tryAcquire(int, long, TimeUnit)}
         */
        public boolean tryAcquire(int acquire, long time, TimeUnit unit) throws InterruptedException
        {
            return tryAcquireUntil(acquire, nanoTime() + unit.toNanos(time));
        }

        /**
         * {@link Semaphore#tryAcquireUntil(int, long)}
         */
        public boolean tryAcquireUntil(int acquire, long nanoTimeDeadline) throws InterruptedException
        {
            boolean wait = true;
            while (true)
            {
                int cur = permits;
                if (cur < acquire)
                {
                    if (!wait) return false;

                    WaitQueue.Signal signal = register();
                    if (permits < acquire) wait = signal.awaitUntil(nanoTimeDeadline);
                    else signal.cancel();
                }
                else if (permitsUpdater.compareAndSet(this, cur, cur - acquire))
                    return true;
            }
        }

        /**
         * {@link Semaphore#acquire(int)}
         */
        public void acquire(int acquire) throws InterruptedException
        {
            acquire(acquire, WaitQueue.Signal::await);
        }

        /**
         * {@link Semaphore#acquireThrowUncheckedOnInterrupt(int)}
         */
        public void acquireThrowUncheckedOnInterrupt(int acquire)
        {
            acquire(acquire, WaitQueue.Signal::awaitThrowUncheckedOnInterrupt);
        }

        private <T extends Throwable> void acquire(int acquire, ThrowingConsumer<WaitQueue.Signal, T> wait) throws T
        {
            while (true)
            {
                int cur = permits;
                if (cur < acquire)
                {
                    WaitQueue.Signal signal = register();
                    if (permits < acquire) wait.accept(signal);
                    else signal.cancel();
                }
                else if (permitsUpdater.compareAndSet(this, cur, cur - acquire))
                    return;
            }
        }

        private WaitQueue.Signal register()
        {
            if (waiting == null)
                waitingUpdater.compareAndSet(this, null, newWaitQueue());
            return waiting.register();
        }
    }

    /**
     * A fair semaphore, guaranteeing threads are signalled in the order they request permits.
     *
     * Unlike {@link UnfairAsync} this class is efficient for arbitrarily-sized increments and decrements,
     * however it has the normal throughput bottleneck of fairness.
     */
    public static class FairJDK implements Semaphore
    {
        final java.util.concurrent.Semaphore wrapped;

        public FairJDK(int permits)
        {
            wrapped = new java.util.concurrent.Semaphore(permits, true);
        }

        /**
         * {@link Semaphore#drain()}
         */
        public int drain()
        {
            return wrapped.drainPermits();
        }

        /**
         * Number of permits that are available to be acquired. {@link Semaphore#permits()}
         */
        public int permits()
        {
            return wrapped.availablePermits();
        }

        /**
         * Number of permits that have been acquired in excess of available. {@link Semaphore#permits()}
         */
        public int waiting()
        {
            return wrapped.getQueueLength();
        }

        /**
         * {@link Semaphore#release(int)}
         */
        public void release(int permits)
        {
            wrapped.release(permits);
        }

        /**
         * {@link Semaphore#tryAcquire(int)}
         */
        public boolean tryAcquire(int permits)
        {
            return wrapped.tryAcquire(permits);
        }

        /**
         * {@link Semaphore#tryAcquire(int, long, TimeUnit)}
         */
        public boolean tryAcquire(int acquire, long time, TimeUnit unit) throws InterruptedException
        {
            return wrapped.tryAcquire(acquire, time, unit);
        }

        /**
         * {@link Semaphore#tryAcquireUntil(int, long)}
         */
        public boolean tryAcquireUntil(int acquire, long nanoTimeDeadline) throws InterruptedException
        {
            long wait = nanoTimeDeadline - System.nanoTime();
            return wrapped.tryAcquire(acquire, Math.max(0, wait), TimeUnit.NANOSECONDS);
        }

        /**
         * {@link Semaphore#acquire(int)}
         */
        public void acquire(int acquire) throws InterruptedException
        {
            wrapped.acquire(acquire);
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
