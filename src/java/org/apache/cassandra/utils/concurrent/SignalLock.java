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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

import accord.utils.Invariants;

import org.apache.cassandra.utils.Nemesis;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A lock that supports semi-asynchronous operation; a thread may take the lock synchronously,
 * or it may await a signal that it should acquire the lock (to perform synchronous work),
 * or a signal that work has been made available for it outside the lock.
 *
 * In some systems it may be that LockSupport.unpark() often results in the calling thread becoming the callee,
 * which can be harmful if the caller owns the lock. To avoid this problem, we support making available permits
 * that can be acquired asynchronously, and only on releasing the lock do we attempt to actually directly wake
 * any threads. In this mode of operation it is up to the user to unlock when there is sufficient work to signal.
 *
 * TODO (expected): once we support a high enough JDK, port to compareAndExchange/weakCompareAndSet as appropriate
 */
public final class SignalLock implements Lock
{
    private static final AtomicLongFieldUpdater<SignalLock> stateUpdater = AtomicLongFieldUpdater.newUpdater(SignalLock.class, "state");

    private static final long HAS_LOCK_WORK = 0x8000000000000000L;
    // if there is a LOCK_SIGNAL, to avoid unnecessary wakeups we pick a waiting thread to unpark and store it in LOCK_SIGNALLED_MASK bits
    private static final int LOCK_THREAD_SHIFT = 64 - 7;
    private static final int LOCK_OWNED_SHIFT = LOCK_THREAD_SHIFT - 1;
    private static final int ASYNC_SIGNAL_COUNT_SHIFT = LOCK_OWNED_SHIFT - 6;
    private static final int ENABLED_THREAD_COUNT_SHIFT = ASYNC_SIGNAL_COUNT_SHIFT - 6;
    public static final int MAX_THREADS = ENABLED_THREAD_COUNT_SHIFT;
    public static final int THREAD_ID_MASK = 0x3f;
    public static final int MAX_SIGNAL_COUNT = THREAD_ID_MASK;
    private static final long LOCK_THREAD_MASK = (long) THREAD_ID_MASK << LOCK_THREAD_SHIFT;
    private static final long LOCK_OWNED = 1L << LOCK_OWNED_SHIFT;
    private static final long LOCK_SIGNALLED = 0L;
    private static final long ASYNC_SIGNAL_INCREMENT = 1L << ASYNC_SIGNAL_COUNT_SHIFT;
    private static final long ENABLED_THREAD_COUNT_INCREMENT = 1L << ENABLED_THREAD_COUNT_SHIFT;
    private static final long ENABLED_THREAD_COUNT_MASK = (long)THREAD_ID_MASK << ENABLED_THREAD_COUNT_SHIFT;
    private static final long WAITING_THREADS_MASK = -1L >>> (64 - MAX_THREADS);

    private final boolean allowStealing;
    private final Thread[] registered;
    @Nemesis private volatile long state;
    private Thread owner;
    private final ConcurrentLinkedQueue<Thread> locking = new ConcurrentLinkedQueue<>();
    private final AtomicLongArray stopLog;
    private final AtomicLong stopCheck;
    private final long stopCheckIntervalNanos;
    private final long spinIntervalNanos;
    private boolean preferRegistered;
    private boolean autoPreferRegistered = true;

    private int depth;

    public SignalLock(int threadCount)
    {
        this(true, threadCount, 0, 0, null);
    }

    public SignalLock(int threadCount, long spinInterval, long stopCheckInterval, TimeUnit units)
    {
        this(true, threadCount, spinInterval, stopCheckInterval, units);
    }

    public SignalLock(boolean allowStealing, int threadCount, long spinInterval, long stopCheckInterval, TimeUnit units)
    {
        Invariants.requireArgument(threadCount <= MAX_THREADS, "Supports at most " + MAX_THREADS + " registered threads");
        this.allowStealing = allowStealing;
        this.registered = new Thread[threadCount];
        this.state = (long)threadCount << ENABLED_THREAD_COUNT_SHIFT;
        this.spinIntervalNanos = spinInterval <= 0 ? 0 : units.toNanos(spinInterval);
        if (stopCheckInterval > 0)
        {
            this.stopLog = new AtomicLongArray(threadCount);
            this.stopCheck = new AtomicLong();
            this.stopCheckIntervalNanos = units.toNanos(stopCheckInterval);
        }
        else
        {
            this.stopLog = null;
            this.stopCheck = null;
            this.stopCheckIntervalNanos = -1;
        }
    }

    public void register(int index, Thread thread)
    {
        registered[index] = thread;
    }

    public Thread registeredThread(int index)
    {
        return registered[index];
    }

    public void lock()
    {
        Thread self = Thread.currentThread();
        if (tryLock(self))
            return;

        locking.add(self);
        while (true)
        {
            long cur = state;
            if (isLockAvailable(cur, ANONYMOUS_OWNER, self))
            {
                long upd = setLockThread(clearLockThread(cur), ANONYMOUS_OWNER, LOCK_OWNED);
                if (!stateUpdater.compareAndSet(this, cur, upd))
                    continue;

                Invariants.require(depth == 0);
                Invariants.require(owner == null);
                depth = 1;
                owner = self;

                Invariants.require(locking.peek() == self);
                locking.poll();
                return;
            }

            LockSupport.park();
        }
    }

    @Override
    public boolean tryLock()
    {
        return tryLock(Thread.currentThread());
    }

    private boolean tryLock(Thread self)
    {
        return tryLock(ANONYMOUS_OWNER, self);
    }

    public boolean tryLock(int thread)
    {
        Invariants.require(thread >= 0 && thread < registered.length);
        Thread self = registeredThread(thread);
        return tryLock(thread, self);
    }

    private boolean tryLock(int threadOrAnonymous, Thread self)
    {
        if (owner != self)
        {
            while (true)
            {
                long cur = state;
                if (!isLockAvailable(cur, threadOrAnonymous, null))
                    return false;

                long upd = setLockThread(clearLockThread(cur), threadOrAnonymous, LOCK_OWNED);
                if (!stateUpdater.compareAndSet(this, cur, upd))
                    continue;

                Invariants.require(depth == 0);
                Invariants.require(owner == null);
                owner = self;
                break;
            }
        }

        ++depth;
        return true;
    }

    /**
     * Wait for the lock to be signalled and acquire it, or wait for additional work to be signalled.
     * Return true if we acquired the lock.
     */
    public boolean awaitAsyncOrLock(int thread)
    {
        return awaitAsyncOrLock(thread, 1);
    }

    public boolean awaitAsyncOrLock(int thread, int burstSignalOnAcquire)
    {
        long threadBit = threadBit(thread);
        Thread self = registered[thread];
        Invariants.require(self == Thread.currentThread());
        Invariants.require(owner != self);
        Invariants.require((state & threadBit) == 0);

        boolean hasPaused = false;
        if (spinIntervalNanos > 0)
        {
            while (true)
            {
                long cur = state;
                if (hasLockWork(cur) && isLockAvailable(cur, thread, null))
                {
                    if (tryTakeLockInAwaitLoop(cur, thread, self, hasPaused)) return true;
                    else continue;
                }
                else if (tryAcquireAsyncInAwaitLoop(cur, thread, 0L, hasPaused, burstSignalOnAcquire))
                {
                    return false;
                }

                int enabledCount = enabledThreadCount(cur);
                if (thread >= enabledCount || enabledCount <= 1)
                    break; // if we're a disabled thread, or the last thread, move to blocking loop

                hasPaused = pause(thread, hasPaused);
                int waitingEnabledThreadCount = waitingEnabledThreadCount(cur);
                int multiplier = 1 + waitingEnabledThreadCount == 0 ? 0 : ThreadLocalRandom.current().nextInt(2 * waitingEnabledThreadCount(cur));
                LockSupport.parkNanos(spinIntervalNanos * multiplier);
            }
        }

        long cur = stateUpdater.addAndGet(this, threadBit);
        while (true)
        {
            do   // dummy do-while-false loop, to ensure we refresh cur
            {
                boolean isSignalled = (cur & threadBit) == 0;
                if (isSignalled)
                {
                    Invariants.require(lockThread(cur) != thread);
                    unpause(thread, hasPaused);
                    propagateAsyncWorkSignals(cur, burstSignalOnAcquire);
                    return false;
                }
                else if (hasLockWork(cur) && isLockAvailable(cur, thread, null))
                {
                    if (tryTakeLockInAwaitLoop(cur, thread, self, hasPaused)) return true;
                    else continue;
                }
                else if (tryAcquireAsyncInAwaitLoop(cur, thread, threadBit, hasPaused, burstSignalOnAcquire))
                {
                    return false;
                }

                hasPaused = pause(thread, hasPaused);
                LockSupport.park();
            }
            while (false);

            cur = state;
        }
    }

    private boolean tryTakeLockInAwaitLoop(long cur, int thread, Thread self, boolean hasPaused)
    {
        long upd = setLockThread(clearLockThread(cur), thread, LOCK_OWNED) ^ threadBit(thread);
        if (!stateUpdater.compareAndSet(this, cur, upd))
            return false;

        Invariants.require(depth == 0);
        Invariants.require(owner == null);
        owner = self;
        depth = 1;
        unpause(thread, hasPaused);
        return true;
    }

    private boolean pause(int thread, boolean hasPaused)
    {
        if (!hasPaused && stopLog != null)
            stopLog.set(thread, nanoTime());
        return true;
    }

    private void unpause(int thread, boolean hasPaused)
    {
        if (hasPaused && stopLog != null)
        {
            long start = stopLog.getAndSet(thread, 0);
            long stop = nanoTime();
            updateStopCheck(stop - start, stop);
        }
    }

    private void updateStopCheck(long stoppedFor, long now)
    {
        long current = stopCheck.addAndGet(stoppedFor);
        if (current > now && stopCheck.compareAndSet(current, now - stopCheckIntervalNanos))
            decrementEnabledThreadCount();
        else if (current < now - 2 * stopCheckIntervalNanos)
            stopCheck.compareAndSet(current, now - stopCheckIntervalNanos);
    }

    @Override
    public void unlock()
    {
        unlock(1, false);
    }

    public void unlock(int burstSignal)
    {
        unlock(burstSignal, false);
    }

    public boolean unlockAndAcquireAsyncWork()
    {
        return unlockAndAcquireAsyncWork(1);
    }

    public boolean unlockAndAcquireAsyncWork(int burstSignal)
    {
        return unlock(burstSignal, true);
    }

    private boolean unlock(int burstSignal, boolean acquire)
    {
        Thread self = owner;
        Invariants.require(self == Thread.currentThread());
        Invariants.require(!acquire || depth == 1, "Cannot acquire async work with reentrancy (depth " + depth + ')');
        return --depth == 0 && releaseAndSignalExclusive(burstSignal, acquire);
    }

    private boolean releaseAndSignalExclusive(int burstSignal, boolean acquire)
    {
        boolean acquired = acquire && tryAcquireAsyncWork();
        owner = null;
        Thread next = locking.peek();
        boolean preferRegistered = this.preferRegistered;
        while (true)
        {
            long cur = state;
            boolean wakeupLockWork = hasLockWork(cur) && hasMoreWaitersThanSignals(cur);
            if (next == null && !wakeupLockWork)
            {
                if (stateUpdater.compareAndSet(this, cur, clearLockThread(cur)))
                {
                    next = locking.peek();
                    if (next != null)
                        LockSupport.unpark(next);
                    break;
                }
                continue;
            }

            int thread = ANONYMOUS_OWNER;
            if (wakeupLockWork)
            {
                if (preferRegistered || next == null)
                    thread = pickThreadToSignalForLock(cur);
                if (next != null && autoPreferRegistered)
                    this.preferRegistered = !preferRegistered;
            }

            long upd = setLockThread(clearLockThread(cur), thread, LOCK_SIGNALLED);
            if (stateUpdater.compareAndSet(this, cur, upd))
            {
                if (thread >= 0) unpark(thread);
                else LockSupport.unpark(next);
                break;
            }

            // cancel the flip of prefer registered (if any)
            this.preferRegistered = preferRegistered;
        }

        propagateAsyncWorkSignals(burstSignal);
        return acquired;
    }

    private boolean isLockAvailable(long cur, int thread, Thread waiting)
    {
        if (isLockOwned(cur))
            return thread >= 0 && lockThread(cur) == thread;

        if (thread >= 0 || waiting == null)
            return allowStealing || !isLockOwnedOrSignalled(cur);

        return locking.peek() == waiting && lockThread(cur) < 0;
    }

    private boolean tryAcquireAsyncInAwaitLoop(long cur, int thread, long threadBitIfWaiting, boolean hasPaused, int burstSignalOnAcquire)
    {
        while (true)
        {
            int signals = asyncSignalCount(cur);
            if (signals == 0)
                return false;

            if ((cur & threadBitIfWaiting) != 0)
                return false;

            long upd = (cur - ASYNC_SIGNAL_INCREMENT) ^ threadBitIfWaiting;
            if (!stateUpdater.compareAndSet(this, cur, upd))
            {
                cur = state;
                continue;
            }

            propagateAsyncWorkSignals(upd, burstSignalOnAcquire);
            unpause(thread, hasPaused);
            return true;
        }
    }

    public boolean incrementAsyncWork(boolean signalIfWaiting)
    {
        while (true)
        {
            long cur = state;
            int count = asyncSignalCount(cur);
            if (count == MAX_THREADS)
                return false;

            if (signalIfWaiting)
            {
                int thread = pickThreadToSignalAsync(cur);
                if (thread != NO_THREAD)
                {
                    long upd = cur ^ threadBit(thread);
                    if (stateUpdater.compareAndSet(this, cur, upd))
                        return unpark(thread);
                    continue;
                }
            }

            long upd = cur + ASYNC_SIGNAL_INCREMENT;
            if (stateUpdater.compareAndSet(this, cur, upd))
                return true;
        }
    }

    public boolean tryAcquireAsyncWork()
    {
        while (true)
        {
            long cur = state;
            int count = asyncSignalCount(cur);
            if (count == 0)
                return false;

            long upd = cur - ASYNC_SIGNAL_INCREMENT;
            if (stateUpdater.compareAndSet(this, cur, upd))
                return true;
        }
    }

    public void propagateAsyncWorkSignals()
    {
        propagateAsyncWorkSignals(Integer.MAX_VALUE);
    }

    public void propagateAsyncWorkSignals(int burstSignal)
    {
        propagateAsyncWorkSignals(state, burstSignal);
    }

    private void propagateAsyncWorkSignals(long cur, int burstSignal)
    {
        while (burstSignal > 0)
        {
            int signals = asyncSignalCount(cur);
            int thread = pickThreadToSignalAsync(cur);
            if (signals == 0 || thread == NO_THREAD)
                return;

            long threadBit = threadBit(thread);
            Invariants.require((threadBit & cur) != 0);
            long upd = (cur - ASYNC_SIGNAL_INCREMENT) ^ threadBit;
            if (!stateUpdater.compareAndSet(this, cur, upd)) cur = state;
            else
            {
                unpark(thread);
                --burstSignal;
                cur = upd;
            }
        }
    }

    /**
     * Signal a waiting registered thread to try and acquire the lock.
     * Return true if the lock is held, or the signal has been propagated
     */
    private void wakeupForLockWork()
    {
        while (true)
        {
            long cur = state;

            if (!hasLockWork(cur) || isLockOwnedOrSignalled(cur))
                return;

            // signal lock owners from end backwards, and async work from front forwards
            int thread = pickThreadToSignalForLock(cur);
            if (thread < 0)
                return;

            Invariants.require(!isLockOwned(cur));
            long upd = setLockThread(cur, thread, LOCK_SIGNALLED);
            if (stateUpdater.compareAndSet(this, cur, upd))
            {
                unpark(thread);
                return;
            }
        }
    }

    private boolean unpark(int thread)
    {
        LockSupport.unpark(registered[thread]);
        return true;
    }

    /**
     * Signal that there is work to do on the lock
     * If the lock is free and there are any waiting threads, wake one
     */
    public void signalLockWork()
    {
        if (setHasLockWork())
            wakeupForLockWork();
    }

    /**
     * Signal that there is work to do on the lock
     * If the lock is free and there are any waiting threads, wake one
     */
    public void signalLockWorkExclusive()
    {
        setHasLockWork();
    }

    private boolean setHasLockWork()
    {
        return 0 == (stateUpdater.getAndUpdate(this, v -> v | HAS_LOCK_WORK) & HAS_LOCK_WORK);
    }

    public void signalAllRegistered()
    {
        stateUpdater.updateAndGet(this, v -> v & ~WAITING_THREADS_MASK);
        for (Thread thread : registered)
        {
            if (thread != null)
                LockSupport.unpark(thread);
        }
    }

    /**
     * Work that requires the lock has been finished.
     * This must be called prior to draining all work that has been asynchronously submitted for processing with the lock.
     * Otherwise, the caller must check whether new work has been submitted prior to calling unlock,
     * and if so first call signalLockExclusive().
     */
    public void clearLockWork()
    {
        stateUpdater.updateAndGet(this, v -> v & ~HAS_LOCK_WORK);
    }

    public long state()
    {
        return state;
    }

    public int asyncSignalCount()
    {
        return asyncSignalCount(state);
    }

    public int waitingThreadCount()
    {
        return waitingThreadCount(state);
    }

    public int enabledThreadCount()
    {
        return enabledThreadCount(state);
    }

    // if enabledThreadCount() == 1 return false; otherwise reduce by one
    private boolean decrementEnabledThreadCount()
    {
        while (true)
        {
            long cur = state;
            if (enabledThreadCount(cur) == 1)
                return false;

            if (stateUpdater.compareAndSet(this, cur, cur - ENABLED_THREAD_COUNT_INCREMENT))
                return true;
        }
    }

    public int addAndGetEnabledThreadCount(int delta)
    {
        Invariants.require(delta > 0 && delta < threadCount());
        while (true)
        {
            long cur = state;
            int count = enabledThreadCount(cur);
            int newCount = Math.min(threadCount(), count + delta);
            if (count == newCount)
                return 0;

            if (stateUpdater.compareAndSet(this, cur, setEnabledThreadCount(cur, newCount)))
            {
                propagateAsyncWorkSignals(1);
                return newCount - count;
            }
        }
    }

    public void setEnabledThreadCount(int count)
    {
        Invariants.require(count > 0 && count <= threadCount());
        stateUpdater.accumulateAndGet(this, count, SignalLock::setEnabledThreadCount);
        propagateAsyncWorkSignals(1);
    }

    public int threadCount()
    {
        return registered.length;
    }

    public int waitingEnabledThreadCount()
    {
        return waitingEnabledThreadCount(state);
    }

    public int activeThreadCount()
    {
        long cur = state;
        int active = registered.length - waitingThreadCount(cur);
        int lockThread = lockThread(cur);
        if (lockThread >= 0 && (cur & threadBit(lockThread)) != 0)
            ++active;
        return active;
    }

    public boolean hasLockWork()
    {
        return hasLockWork(state);
    }

    public boolean hasOwner()
    {
        return isLockOwned(state);
    }

    public boolean isOwner()
    {
        return Thread.currentThread() == owner;
    }

    public void setAutoPreferRegistered(boolean autoPreferRegistered)
    {
        this.autoPreferRegistered = autoPreferRegistered;
    }

    public void setPreferRegistered(boolean preferRegistered)
    {
        this.preferRegistered = preferRegistered;
    }

    private static final int NO_THREAD = 64;
    private static final int ANONYMOUS_OWNER = -1;
    private static int pickThreadToSignalAsync(long state)
    {
        int lockThread = lockThread(state);
        if (lockThread >= 0)
            state &= ~threadBit(lockThread);
        return Long.numberOfTrailingZeros(state & threadMask(enabledThreadCount(state)));
    }

    private static int pickThreadToSignalForLock(long state)
    {
        return 63 - Long.numberOfLeadingZeros(state & threadMask(enabledThreadCount(state)));
    }

    private static int lockThread(long state)
    {
        return (int) ((state & LOCK_THREAD_MASK) >>> LOCK_THREAD_SHIFT) - 2;
    }

    private static boolean isLockOwnedOrSignalled(long state)
    {
        return 0 != (state & LOCK_THREAD_MASK);
    }

    private static boolean isLockOwned(long state)
    {
        return 0 != (state & LOCK_OWNED);
    }

    private static long setLockThread(long state, int thread, long owned)
    {
        Invariants.require(!isLockOwnedOrSignalled(state));
        state |= (long) (thread + 2) << LOCK_THREAD_SHIFT;
        return state | owned;
    }

    private static long clearLockThread(long state)
    {
        state &= ~(LOCK_THREAD_MASK | LOCK_OWNED);
        return state;
    }

    public static int asyncSignalCount(long state)
    {
        return (int) ((state >>> ASYNC_SIGNAL_COUNT_SHIFT) & THREAD_ID_MASK);
    }

    public static int waitingThreadCount(long state)
    {
        return Long.bitCount(state & WAITING_THREADS_MASK);
    }

    public static int enabledThreadCount(long state)
    {
        return (int) ((state >>> ENABLED_THREAD_COUNT_SHIFT) & THREAD_ID_MASK);
    }

    private static long setEnabledThreadCount(long state, long count)
    {
        Invariants.require(count <= MAX_THREADS);
        return (state & ~ENABLED_THREAD_COUNT_MASK) | ((long)count << ENABLED_THREAD_COUNT_SHIFT);
    }

    public static int activeEnabledThreadCount(long state)
    {
        return activeThreadCount(enabledThreadCount(state), state);
    }

    public static int activeThreadCount(int threadCount, long state)
    {
        return threadCount - Long.bitCount(state & threadMask(threadCount));
    }

    private static long threadMask(int threadCount)
    {
        return (1L << threadCount) - 1;
    }

    public static int waitingEnabledThreadCount(long state)
    {
        return Long.bitCount(state & threadMask(enabledThreadCount(state)));
    }

    public static boolean hasMoreWaitersThanSignals(long state)
    {
        return waitingEnabledThreadCount(state) > asyncSignalCount(state);
    }

    private static long threadBit(int threadIndex)
    {
        return 1L << threadIndex;
    }

    private static boolean hasLockWork(long state)
    {
        return (state & HAS_LOCK_WORK) != 0;
    }

    public void lockInterruptibly()
    {
        throw new UnsupportedOperationException();
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
