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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the await/signal contract of {@link SignalLock}, i.e. the states in which
 * {@link SignalLock#awaitAsyncOrLock} must return rather than park:
 * <ul>
 *   <li>an async permit is already available as we begin to wait - the waiter must serve itself, since a permit made
 *       available with {@code incrementAsyncWork(false)} carries no signal, so nothing will wake it;</li>
 *   <li>a permit is signalled to us while we wait - the permit is transferred to us, and must not be consumed twice;</li>
 *   <li>work requiring the lock is signalled - we must acquire the lock;</li>
 * </ul>
 * plus {@link SignalLock#ensureNotOwner}, which must release a reentrant hold in full.
 */
public class SignalLockTest
{
    private static final long TIMEOUT_SECONDS = 10;

    /**
     * A permit made available before we begin waiting comes with no signal, so nothing will ever wake us: the waiter
     * must notice the available permit itself instead of parking.
     */
    @Test
    public void awaitReturnsWhenPermitAlreadyAvailable() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        assertThat(lock.incrementAsyncWork(false)).isTrue();
        assertThat(lock.asyncSignalCount()).isEqualTo(1);

        Waiter waiter = new Waiter(lock, 0);
        waiter.start();

        assertThat(waiter.awaitResult()).describedAs("acquired the lock instead of the available permit").isFalse();
        assertThat(lock.asyncSignalCount()).describedAs("permit was not consumed").isZero();
        assertThat(lock.waitingThreadCount()).describedAs("waiter left itself registered as waiting").isZero();
    }

    /** A permit signalled to a parked waiter is transferred to it, and so must not be consumed a second time. */
    @Test
    public void awaitReturnsWhenPermitSignalled() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        Waiter waiter = new Waiter(lock, 0);
        waiter.start();
        waiter.awaitWaiting();

        assertThat(lock.incrementAsyncWork(true)).isTrue();

        assertThat(waiter.awaitResult()).describedAs("acquired the lock instead of the signalled permit").isFalse();
        assertThat(lock.asyncSignalCount()).describedAs("the signalled permit was consumed twice").isZero();
        assertThat(lock.waitingThreadCount()).describedAs("waiter left itself registered as waiting").isZero();
    }

    /** Every waiter must be woken when enough permits are burst signalled for all of them. */
    @Test
    public void burstSignalWakesEveryWaiter() throws Throwable
    {
        SignalLock lock = new SignalLock(3);
        Waiter[] waiters = { new Waiter(lock, 0), new Waiter(lock, 1), new Waiter(lock, 2) };
        for (Waiter waiter : waiters)
            waiter.start();
        for (Waiter waiter : waiters)
            waiter.awaitWaiting();

        for (int i = 0 ; i < waiters.length ; ++i)
            assertThat(lock.incrementAsyncWork(false)).isTrue();
        lock.propagateAsyncWorkSignals(waiters.length);

        for (Waiter waiter : waiters)
            assertThat(waiter.awaitResult()).describedAs("acquired the lock instead of a permit").isFalse();
        assertThat(lock.asyncSignalCount()).describedAs("permits were not all consumed").isZero();
        assertThat(lock.waitingThreadCount()).describedAs("a waiter left itself registered as waiting").isZero();
    }

    /** A waiter signalled that there is work for the lock must acquire the lock, and be able to release it. */
    @Test
    public void awaitTakesLockWhenLockWorkSignalled() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        Waiter waiter = new Waiter(lock, 0);
        waiter.start();
        waiter.awaitWaiting();

        lock.signalLockWork();

        assertThat(waiter.awaitResult()).describedAs("did not acquire the lock").isTrue();
        assertThat(lock.hasOwner()).describedAs("waiter did not release the lock").isFalse();
        assertThat(lock.waitingThreadCount()).describedAs("waiter left itself registered as waiting").isZero();
    }

    /** unlockAll must release however many times we have acquired the lock, so that another thread may take it. */
    @Test
    public void ensureNotOwnerReleasesReentrantHold() throws Throwable
    {
        SignalLock lock = new SignalLock(2);
        lock.lock();
        lock.lock();
        assertThat(lock.ensureNotOwner()).describedAs("wrong number of acquisitions released").isEqualTo(2);
        assertThat(lock.hasOwner()).describedAs("lock was not released").isFalse();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread other = new Thread(() -> {
            try { assertThat(lock.tryLock()).describedAs("another thread could not take the lock").isTrue(); }
            catch (Throwable t) { failure.set(t); }
        }, "other");
        other.start();
        other.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(other.isAlive()).describedAs("another thread blocked on the released lock").isFalse();
        if (failure.get() != null)
            throw failure.get();
    }

    /** Awaits on one registered thread, recording whether it acquired the lock (and releasing it if so). */
    private static class Waiter
    {
        final SignalLock lock;
        final int index;
        final AtomicInteger result = new AtomicInteger(Integer.MIN_VALUE);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread thread;

        Waiter(SignalLock lock, int index)
        {
            this.lock = lock;
            this.index = index;
            this.thread = new Thread(this::run, "waiter" + index);
        }

        void start()
        {
            lock.register(index, thread);
            thread.start();
        }

        private void run()
        {
            try
            {
                boolean tookLock = lock.awaitAsyncOrLock(index);
                if (tookLock)
                    lock.unlock();
                result.set(tookLock ? 1 : 0);
            }
            catch (Throwable t)
            {
                failure.set(t);
                result.set(-1);
            }
        }

        /** wait until we have registered ourselves as waiting, so that a signal has someone to wake */
        void awaitWaiting()
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (0 == (lock.state() & (1L << index)) && System.nanoTime() < deadline)
                Thread.yield();
            assertThat(lock.state() & (1L << index)).describedAs("waiter never registered itself as waiting").isNotZero();
        }

        /** @return true if the waiter acquired the lock; fails if it is still parked */
        boolean awaitResult() throws Throwable
        {
            thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            if (thread.isAlive())
            {
                StringBuilder out = new StringBuilder("waiter did not return; state=" + Long.toHexString(lock.state()));
                for (StackTraceElement element : thread.getStackTrace())
                    out.append("\n\tat ").append(element);
                assertThat(false).describedAs(out.toString()).isTrue();
            }
            if (failure.get() != null)
                throw failure.get();
            return result.get() == 1;
        }
    }
}
