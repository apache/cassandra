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

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

import accord.utils.Invariants;

// WARNING: experimental - needs more testing
public class LockWithAsyncSignal implements Lock
{
    interface AwaitFunction<T extends Throwable>
    {
        T await(LockWithAsyncSignal lock, Waiter waiter) throws T;
    }

    private static final Waiter AWAITING_LOCK = new Waiter(0, null);

    private volatile Thread owner;
    private static final AtomicReferenceFieldUpdater<LockWithAsyncSignal, Thread> ownerUpdater = AtomicReferenceFieldUpdater.newUpdater(LockWithAsyncSignal.class, Thread.class, "owner");
    private int depth;

    // TODO (desired): better combined queue
    final ConcurrentSkipListSet<Waiter> waiters = new ConcurrentSkipListSet<>();

    static class Waiter implements Comparable<Waiter>
    {
        final long ticket;
        final Thread thread;

        Waiter(long ticket, Thread thread)
        {
            this.ticket = ticket;
            this.thread = thread;
        }

        @Override
        public int compareTo(Waiter that)
        {
            return Long.compare(this.ticket, that.ticket);
        }
    }

    volatile int signal;
    private static final AtomicIntegerFieldUpdater<LockWithAsyncSignal> signalUpdater = AtomicIntegerFieldUpdater.newUpdater(LockWithAsyncSignal.class, "signal");

    volatile long ticket;
    private static final AtomicLongFieldUpdater<LockWithAsyncSignal> ticketUpdater = AtomicLongFieldUpdater.newUpdater(LockWithAsyncSignal.class, "ticket");

    public void lock()
    {
        lockInternal(LockWithAsyncSignal::awaitUninterruptibly);
    }

    public void lockInterruptibly() throws InterruptedException
    {
        lockInternal(LockWithAsyncSignal::awaitThrows);
    }

    private <T extends Throwable> void lockInternal(AwaitFunction<T> await) throws T
    {
        Thread thread = Thread.currentThread();
        if (ownerUpdater.compareAndSet(this, null, thread) || owner == thread)
        {
            ++depth;
        }
        else
        {
            awaitLock(false, thread, 1, await);
        }
    }

    public boolean tryLock()
    {
        Thread thread = Thread.currentThread();
        if (!ownerUpdater.compareAndSet(this, null, thread) && owner != thread)
            return false;

        ++depth;
        return true;
    }

    public void await() throws InterruptedException
    {
        Thread thread = Thread.currentThread();
        int restoreDepth = depth;
        Invariants.checkState(owner == thread);

        depth = 0;
        owner = null;

        awaitLock(true, thread, restoreDepth, LockWithAsyncSignal::awaitDeferThrow);
    }

    public void unlock()
    {
        Invariants.checkState(owner == Thread.currentThread());
        if (--depth > 0)
            return;

        owner = null;
        wakeOne();
    }

    private <T extends Throwable> void awaitLock(boolean awaitingSignal, Thread thread, int restoreDepth, AwaitFunction<T> await) throws T
    {
        T pending = null;
        while (true)
        {
            Waiter waiter = register(awaitingSignal, thread);
            if (awaitingSignal && signal == 0)
            {
                if (owner == null)
                    wakeOne(false); // will not wake ourselves as we only signal pure lock waiters
            }
            else if (ownerUpdater.compareAndSet(this, null, thread))
            {
                depth = restoreDepth;
                waiters.remove(waiter);
                if (pending != null)
                    throw pending;
                return;
            }
            pending = firstNonNull(pending, await.await(this, waiter));
            awaitingSignal &= pending == null;
        }
    }

    private static <T> T firstNonNull(T cur, T next)
    {
        return cur != null ? cur : next;
    }

    public void signal()
    {
        if (signalUpdater.compareAndSet(this, 0, 1) && owner == null)
            wakeOne(true);
    }

    public void clearSignal()
    {
        signal = 0;
    }

    public boolean isOwner(Thread thread)
    {
        return thread == owner;
    }

    private Waiter register(boolean awaitingSignal, Thread thread)
    {
        long ticket = ticketUpdater.updateAndGet(this, v -> v == Long.MAX_VALUE ? 1 : v + 1);
        if (awaitingSignal)
            ticket = -ticket;
        Waiter waiter = new Waiter(ticket, thread);
        waiters.add(waiter);
        return waiter;
    }

    private InterruptedException awaitDeferThrow(Waiter waiter)
    {
        while (waiters.contains(waiter))
        {
            if (Thread.interrupted())
            {
                waiters.remove(waiter);
                return new InterruptedException();
            }
            LockSupport.park();
        }
        return null;
    }

    private InterruptedException awaitThrows(Waiter waiter) throws InterruptedException
    {
        while (waiters.contains(waiter))
        {
            if (Thread.interrupted())
            {
                if (!waiters.remove(waiter))
                    wakeOne(waiter.ticket < 0 || signal > 0);

                throw new InterruptedException();
            }
            LockSupport.park();
        }
        return null;
    }

    private RuntimeException awaitUninterruptibly(Waiter waiter)
    {
        while (waiters.contains(waiter))
            LockSupport.park();
        return null;
    }

    private void wakeOne()
    {
        wakeOne(signal > 0);
    }

    private void wakeOne(boolean awaitingSignal)
    {
        Waiter wake;
        if (awaitingSignal)
        {
            wake = waiters.pollFirst();
            if (wake == null)
                return;
        }
        else
        {
            do
            {
                wake = waiters.ceiling(AWAITING_LOCK);
                if (wake == null)
                    return;
            } while (!waiters.remove(wake));
        }

        LockSupport.unpark(wake.thread);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition()
    {
        throw new UnsupportedOperationException();
    }
}
