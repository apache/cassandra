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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.utils.Shared;

import org.apache.cassandra.utils.Intercept;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * A generic signal consumer, supporting all of the typical patterns used in Cassandra.
 * All of the methods defined in {@link Awaitable} may be waited on without a loop,
 * as this interface declares that there are no spurious wake-ups.
 */
@Shared(scope = SIMULATION)
public interface Awaitable
{
    /**
     * Await until the deadline (in nanoTime), throwing any interrupt.
     * No spurious wakeups.
     * @return true if we were signalled, false if the deadline elapsed
     * @throws InterruptedException if interrupted
     */
    boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException;

    /**
     * Await until the deadline (in nanoTime), throwing any interrupt as an unchecked exception.
     * No spurious wakeups.
     * @return true if we were signalled, false if the deadline elapsed
     * @throws UncheckedInterruptedException if interrupted
     */
    boolean awaitUntilThrowUncheckedOnInterrupt(long nanoTimeDeadline) throws UncheckedInterruptedException;

    /**
     * Await until the deadline (in nanoTime), ignoring interrupts (but maintaining the interrupt flag on exit).
     * No spurious wakeups.
     * @return true if we were signalled, false if the deadline elapsed
     */
    boolean awaitUntilUninterruptibly(long nanoTimeDeadline);

    /**
     * Await for the specified period, throwing any interrupt.
     * No spurious wakeups.
     * @return true if we were signalled, false if the timeout elapses
     * @throws InterruptedException if interrupted
     */
    boolean await(long time, TimeUnit units) throws InterruptedException;

    /**
     * Await for the specified period, throwing any interrupt as an unchecked exception.
     * No spurious wakeups.
     * @return true if we were signalled, false if the timeout elapses
     * @throws UncheckedInterruptedException if interrupted
     */
    boolean awaitThrowUncheckedOnInterrupt(long time, TimeUnit units) throws UncheckedInterruptedException;

    /**
     * Await until the deadline (in nanoTime), ignoring interrupts (but maintaining the interrupt flag on exit).
     * No spurious wakeups.
     * @return true if we were signalled, false if the timeout elapses
     */
    boolean awaitUninterruptibly(long time, TimeUnit units);

    /**
     * Await indefinitely, throwing any interrupt.
     * No spurious wakeups.
     * @throws InterruptedException if interrupted
     */
    Awaitable await() throws InterruptedException;

    /**
     * Await indefinitely, throwing any interrupt as an unchecked exception.
     * No spurious wakeups.
     * @throws UncheckedInterruptedException if interrupted
     */
    Awaitable awaitThrowUncheckedOnInterrupt() throws UncheckedInterruptedException;

    /**
     * Await indefinitely, ignoring interrupts (but maintaining the interrupt flag on exit).
     * No spurious wakeups.
     */
    Awaitable awaitUninterruptibly();

    // we must declare the static implementation methods outside of the interface,
    // so that they can be loaded by different classloaders during simulation
    class Defaults
    {
        public static boolean await(Awaitable await, long time, TimeUnit unit) throws InterruptedException
        {
            return await.awaitUntil(nanoTime() + unit.toNanos(time));
        }

        public static boolean awaitThrowUncheckedOnInterrupt(Awaitable await, long time, TimeUnit units) throws UncheckedInterruptedException
        {
            return awaitUntilThrowUncheckedOnInterrupt(await, nanoTime() + units.toNanos(time));
        }

        public static boolean awaitUninterruptibly(Awaitable await, long time, TimeUnit units)
        {
            return awaitUntilUninterruptibly(await, nanoTime() + units.toNanos(time));
        }

        public static <A extends Awaitable> A awaitThrowUncheckedOnInterrupt(A await) throws UncheckedInterruptedException
        {
            try
            {
                await.await();
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException();
            }
            return await;
        }

        public static boolean awaitUntilThrowUncheckedOnInterrupt(Awaitable await, long nanoTimeDeadline) throws UncheckedInterruptedException
        {
            try
            {
                return await.awaitUntil(nanoTimeDeadline);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException();
            }
        }

        /**
         * {@link Awaitable#awaitUntilUninterruptibly(long)}
         */
        public static boolean awaitUntilUninterruptibly(Awaitable await, long nanoTimeDeadline)
        {
            boolean interrupted = false;
            boolean result;
            while (true)
            {
                try
                {
                    result = await.awaitUntil(nanoTimeDeadline);
                    break;
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            return result;
        }

        /**
         * {@link Awaitable#awaitUninterruptibly()}
         */
        public static <A extends Awaitable> A awaitUninterruptibly(A await)
        {
            boolean interrupted = false;
            while (true)
            {
                try
                {
                    await.await();
                    break;
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            return await;
        }
    }

    abstract class AbstractAwaitable implements Awaitable
    {
        protected AbstractAwaitable() {}

        /**
         * {@link Awaitable#await(long, TimeUnit)}
         */
        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException
        {
            return Defaults.await(this, time, unit);
        }

        /**
         * {@link Awaitable#awaitThrowUncheckedOnInterrupt(long, TimeUnit)}
         */
        @Override
        public boolean awaitThrowUncheckedOnInterrupt(long time, TimeUnit units) throws UncheckedInterruptedException
        {
            return Defaults.awaitThrowUncheckedOnInterrupt(this, time, units);
        }

        /**
         * {@link Awaitable#awaitUninterruptibly(long, TimeUnit)}
         */
        public boolean awaitUninterruptibly(long time, TimeUnit units)
        {
            return awaitUntilUninterruptibly(nanoTime() + units.toNanos(time));
        }

        /**
         * {@link Awaitable#awaitThrowUncheckedOnInterrupt()}
         */
        public Awaitable awaitThrowUncheckedOnInterrupt() throws UncheckedInterruptedException
        {
            return Defaults.awaitThrowUncheckedOnInterrupt(this);
        }

        /**
         * {@link Awaitable#awaitUntilThrowUncheckedOnInterrupt(long)}
         */
        public boolean awaitUntilThrowUncheckedOnInterrupt(long nanoTimeDeadline) throws UncheckedInterruptedException
        {
            return Defaults.awaitUntilThrowUncheckedOnInterrupt(this, nanoTimeDeadline);
        }

        /**
         * {@link Awaitable#awaitUntilUninterruptibly(long)}
         */
        public boolean awaitUntilUninterruptibly(long nanoTimeDeadline)
        {
            return Defaults.awaitUntilUninterruptibly(this, nanoTimeDeadline);
        }

        /**
         * {@link Awaitable#awaitUninterruptibly()}
         */
        public Awaitable awaitUninterruptibly()
        {
            return Defaults.awaitUninterruptibly(this);
        }
    }

    /**
     * A barebones asynchronous {@link Awaitable}.
     * If your state is minimal, or can be updated concurrently, extend this class.
     */
    abstract class AsyncAwaitable extends AbstractAwaitable
    {
        /**
         * Maintain an internal variable containing a lazily-initialized wait queue
         * @return null if is done
         */
        @Inline
        private static <A extends Awaitable> WaitQueue.Signal register(AtomicReferenceFieldUpdater<A, WaitQueue> waitingUpdater, Predicate<A> isDone, A awaitable)
        {
            if (isDone.test(awaitable))
                return null;

            WaitQueue waiting = waitingUpdater.get(awaitable);
            if (waiting == null)
            {
                if (!waitingUpdater.compareAndSet(awaitable, null, waiting = newWaitQueue()))
                {
                    waiting = waitingUpdater.get(awaitable);
                    if (waiting == null)
                    {
                        assert isDone.test(awaitable);
                        return null;
                    }
                }
            }

            WaitQueue.Signal s = waiting.register();
            if (!isDone.test(awaitable))
                return s;

            s.cancel();
            return null;
        }

        @Inline
        static <A extends Awaitable> A await(AtomicReferenceFieldUpdater<A, WaitQueue> waitingUpdater, Predicate<A> isDone, A awaitable) throws InterruptedException
        {
            WaitQueue.Signal s = register(waitingUpdater, isDone, awaitable);
            if (s != null)
                s.await();
            return awaitable;
        }

        @Inline
        static <A extends Awaitable> boolean awaitUntil(AtomicReferenceFieldUpdater<A, WaitQueue> waitingUpdater, Predicate<A> isDone, A awaitable, long nanoTimeDeadline) throws InterruptedException
        {
            WaitQueue.Signal s = register(waitingUpdater, isDone, awaitable);
            return s == null || s.awaitUntil(nanoTimeDeadline) || isDone.test(awaitable);
        }

        @Inline
        static <A extends Awaitable> void signalAll(AtomicReferenceFieldUpdater<A, WaitQueue> waitingUpdater, A awaitable)
        {
            WaitQueue waiting = waitingUpdater.get(awaitable);
            if (waiting == null)
                return;

            waiting.signalAll();
            waitingUpdater.lazySet(awaitable, null);
        }

        private static final AtomicReferenceFieldUpdater<AsyncAwaitable, WaitQueue> waitingUpdater = AtomicReferenceFieldUpdater.newUpdater(AsyncAwaitable.class, WaitQueue.class, "waiting");
        private volatile WaitQueue waiting;

        protected AsyncAwaitable() {}

        /**
         * {@link Awaitable#await()}
         */
        public Awaitable await() throws InterruptedException
        {
            return await(waitingUpdater, AsyncAwaitable::isSignalled, this);
        }

        /**
         * {@link Awaitable#awaitUntil(long)}
         */
        public boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
        {
            return awaitUntil(waitingUpdater, AsyncAwaitable::isSignalled, this, nanoTimeDeadline);
        }

        /**
         * Signal any waiting threads; {@link #isSignalled()} must return {@code true} before this method is invoked.
         */
        protected void signal()
        {
            signalAll(waitingUpdater, this);
        }

        /**
         * Return true once signalled. Unidirectional; once true, must never again be false.
         */
        protected abstract boolean isSignalled();
    }

    /**
     * A barebones {@link Awaitable} that uses mutual exclusion.
     * If your state will be updated while holding the object monitor, extend this class.
     */
    abstract class SyncAwaitable extends AbstractAwaitable
    {
        protected SyncAwaitable() {}

        /**
         * {@link Awaitable#await()}
         */
        public synchronized Awaitable await() throws InterruptedException
        {
            while (!isSignalled())
                wait();
            return this;
        }

        /**
         * {@link Awaitable#awaitUntil(long)}
         */
        public synchronized boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
        {
            while (true)
            {
                if (isSignalled()) return true;
                if (!waitUntil(this, nanoTimeDeadline)) return false;
            }
        }

        /**
         * Return true once signalled. Unidirectional; once true, must never again be false.
         */
        protected abstract boolean isSignalled();

        @Intercept
        public static boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException
        {
            long wait = deadlineNanos - nanoTime();
            if (wait <= 0)
                return false;

            monitor.wait((wait + 999999) / 1000000);
            return true;
        }
    }
}
