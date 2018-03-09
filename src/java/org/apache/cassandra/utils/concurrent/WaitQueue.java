/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.concurrent;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import com.codahale.metrics.Timer;

/**
 * <p>A relatively easy to use utility for general purpose thread signalling.</p>
 * <p>Usage on a thread awaiting a state change using a WaitQueue q is:</p>
 * <pre>
 * {@code
 *      while (!conditionMet())
 *          Signal s = q.register();
 *              if (!conditionMet())    // or, perhaps more correctly, !conditionChanged()
 *                  s.await();
 *              else
 *                  s.cancel();
 * }
 * </pre>
 * A signalling thread, AFTER changing the state, then calls q.signal() to wake up one, or q.signalAll()
 * to wake up all, waiting threads.
 * <p>To understand intuitively how this class works, the idea is simply that a thread, once it considers itself
 * incapable of making progress, registers to be awoken once that changes. Since this could have changed between
 * checking and registering (in which case the thread that made this change would have been unable to signal it),
 * it checks the condition again, sleeping only if it hasn't changed/still is not met.</p>
 * <p>This thread synchronisation scheme has some advantages over Condition objects and Object.wait/notify in that no monitor
 * acquisition is necessary and, in fact, besides the actual waiting on a signal, all operations are non-blocking.
 * As a result consumers can never block producers, nor each other, or vice versa, from making progress.
 * Threads that are signalled are also put into a RUNNABLE state almost simultaneously, so they can all immediately make
 * progress without having to serially acquire the monitor/lock, reducing scheduler delay incurred.</p>
 *
 * <p>A few notes on utilisation:</p>
 * <p>1. A thread will only exit await() when it has been signalled, but this does not guarantee the condition has not
 * been altered since it was signalled, and depending on your design it is likely the outer condition will need to be
 * checked in a loop, though this is not always the case.</p>
 * <p>2. Each signal is single use, so must be re-registered after each await(). This is true even if it times out.</p>
 * <p>3. If you choose not to wait on the signal (because the condition has been met before you waited on it)
 * you must cancel() the signal if the signalling thread uses signal() to awake waiters; otherwise signals will be
 * lost. If signalAll() is used but infrequent, and register() is frequent, cancel() should still be used to prevent the
 * queue growing unboundedly. Similarly, if you provide a TimerContext, cancel should be used to ensure it is not erroneously
 * counted towards wait time.</p>
 * <p>4. Care must be taken when selecting conditionMet() to ensure we are waiting on the condition that actually
 * indicates progress is possible. In some complex cases it may be tempting to wait on a condition that is only indicative
 * of local progress, not progress on the task we are aiming to complete, and a race may leave us waiting for a condition
 * to be met that we no longer need.
 * <p>5. This scheme is not fair</p>
 * <p>6. Only the thread that calls register() may call await()</p>
 */
public final class WaitQueue
{

    private static final int CANCELLED = -1;
    private static final int SIGNALLED = 1;
    private static final int NOT_SET = 0;

    private static final AtomicIntegerFieldUpdater signalledUpdater = AtomicIntegerFieldUpdater.newUpdater(RegisteredSignal.class, "state");

    // the waiting signals
    private final ConcurrentLinkedQueue<RegisteredSignal> queue = new ConcurrentLinkedQueue<>();

    /**
     * The calling thread MUST be the thread that uses the signal
     * @return                                x
     */
    public Signal register()
    {
        RegisteredSignal signal = new RegisteredSignal();
        queue.add(signal);
        return signal;
    }

    /**
     * The calling thread MUST be the thread that uses the signal.
     * If the Signal is waited on, context.stop() will be called when the wait times out, the Signal is signalled,
     * or the waiting thread is interrupted.
     * @return
     */
    public Signal register(Timer.Context context)
    {
        assert context != null;
        RegisteredSignal signal = new TimedSignal(context);
        queue.add(signal);
        return signal;
    }

    /**
     * Signal one waiting thread
     */
    public boolean signal()
    {
        if (!hasWaiters())
            return false;
        while (true)
        {
            RegisteredSignal s = queue.poll();
            if (s == null || s.signal() != null)
                return s != null;
        }
    }

    /**
     * Signal all waiting threads
     */
    public void signalAll()
    {
        if (!hasWaiters())
            return;

        // to avoid a race where the condition is not met and the woken thread managed to wait on the queue before
        // we finish signalling it all, we pick a random thread we have woken-up and hold onto it, so that if we encounter
        // it again we know we're looping. We reselect a random thread periodically, progressively less often.
        // the "correct" solution to this problem is to use a queue that permits snapshot iteration, but this solution is sufficient
        int i = 0, s = 5;
        Thread randomThread = null;
        Iterator<RegisteredSignal> iter = queue.iterator();
        while (iter.hasNext())
        {
            RegisteredSignal signal = iter.next();
            Thread signalled = signal.signal();

            if (signalled != null)
            {
                if (signalled == randomThread)
                    break;

                if (++i == s)
                {
                    randomThread = signalled;
                    s <<= 1;
                }
            }

            iter.remove();
        }
    }

    private void cleanUpCancelled()
    {
        // TODO: attempt to remove the cancelled from the beginning only (need atomic cas of head)
        Iterator<RegisteredSignal> iter = queue.iterator();
        while (iter.hasNext())
        {
            RegisteredSignal s = iter.next();
            if (s.isCancelled())
                iter.remove();
        }
    }

    public boolean hasWaiters()
    {
        return !queue.isEmpty();
    }

    /**
     * Return how many threads are waiting
     * @return
     */
    public int getWaiting()
    {
        if (!hasWaiters())
            return 0;
        Iterator<RegisteredSignal> iter = queue.iterator();
        int count = 0;
        while (iter.hasNext())
        {
            Signal next = iter.next();
            if (!next.isCancelled())
                count++;
        }
        return count;
    }

    /**
     * A Signal is a one-time-use mechanism for a thread to wait for notification that some condition
     * state has transitioned that it may be interested in (and hence should check if it is).
     * It is potentially transient, i.e. the state can change in the meantime, it only indicates
     * that it should be checked, not necessarily anything about what the expected state should be.
     *
     * Signal implementations should never wake up spuriously, they are always woken up by a
     * signal() or signalAll().
     *
     * This abstract definition of Signal does not need to be tied to a WaitQueue.
     * Whilst RegisteredSignal is the main building block of Signals, this abstract
     * definition allows us to compose Signals in useful ways. The Signal is 'owned' by the
     * thread that registered itself with WaitQueue(s) to obtain the underlying RegisteredSignal(s);
     * only the owning thread should use a Signal.
     */
    public static interface Signal
    {

        /**
         * @return true if signalled; once true, must be discarded by the owning thread.
         */
        public boolean isSignalled();

        /**
         * @return true if cancelled; once cancelled, must be discarded by the owning thread.
         */
        public boolean isCancelled();

        /**
         * @return isSignalled() || isCancelled(). Once true, the state is fixed and the Signal should be discarded
         * by the owning thread.
         */
        public boolean isSet();

        /**
         * atomically: cancels the Signal if !isSet(), or returns true if isSignalled()
         *
         * @return true if isSignalled()
         */
        public boolean checkAndClear();

        /**
         * Should only be called by the owning thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        public abstract void cancel();

        /**
         * Wait, without throwing InterruptedException, until signalled. On exit isSignalled() must be true.
         * If the thread is interrupted in the meantime, the interrupted flag will be set.
         */
        public void awaitUninterruptibly();

        /**
         * Wait until signalled, or throw an InterruptedException if interrupted before this happens.
         * On normal exit isSignalled() must be true; however if InterruptedException is thrown isCancelled()
         * will be true.
         * @throws InterruptedException
         */
        public void await() throws InterruptedException;

        /**
         * Wait until signalled, or the provided time is reached, or the thread is interrupted. If signalled,
         * isSignalled() will be true on exit, and the method will return true; if timedout, the method will return
         * false and isCancelled() will be true; if interrupted an InterruptedException will be thrown and isCancelled()
         * will be true.
         * @param nanos System.nanoTime() to wait until
         * @return true if signalled, false if timed out
         * @throws InterruptedException
         */
        public boolean awaitUntil(long nanos) throws InterruptedException;
    }

    /**
     * An abstract signal implementation
     */
    public static abstract class AbstractSignal implements Signal
    {
        public void awaitUninterruptibly()
        {
            boolean interrupted = false;
            while (!isSignalled())
            {
                if (Thread.interrupted())
                    interrupted = true;
                LockSupport.park();
            }
            if (interrupted)
                Thread.currentThread().interrupt();
            checkAndClear();
        }

        public void await() throws InterruptedException
        {
            while (!isSignalled())
            {
                checkInterrupted();
                LockSupport.park();
            }
            checkAndClear();
        }

        public boolean awaitUntil(long until) throws InterruptedException
        {
            long now;
            while (until > (now = System.nanoTime()) && !isSignalled())
            {
                checkInterrupted();
                long delta = until - now;
                LockSupport.parkNanos(delta);
            }
            return checkAndClear();
        }

        private void checkInterrupted() throws InterruptedException
        {
            if (Thread.interrupted())
            {
                cancel();
                throw new InterruptedException();
            }
        }
    }

    /**
     * A signal registered with this WaitQueue
     */
    private class RegisteredSignal extends AbstractSignal
    {
        private volatile Thread thread = Thread.currentThread();
        volatile int state;

        public boolean isSignalled()
        {
            return state == SIGNALLED;
        }

        public boolean isCancelled()
        {
            return state == CANCELLED;
        }

        public boolean isSet()
        {
            return state != NOT_SET;
        }

        private Thread signal()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, SIGNALLED))
            {
                Thread thread = this.thread;
                LockSupport.unpark(thread);
                this.thread = null;
                return thread;
            }
            return null;
        }

        public boolean checkAndClear()
        {
            if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                thread = null;
                cleanUpCancelled();
                return false;
            }
            // must now be signalled assuming correct API usage
            return true;
        }

        /**
         * Should only be called by the registered thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        public void cancel()
        {
            if (isCancelled())
                return;
            if (!signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
            {
                // must already be signalled - switch to cancelled and
                state = CANCELLED;
                // propagate the signal
                WaitQueue.this.signal();
            }
            thread = null;
            cleanUpCancelled();
        }
    }

    /**
     * A RegisteredSignal that stores a TimerContext, and stops the timer when either cancelled or
     * finished waiting. i.e. if the timer is started when the signal is registered it tracks the
     * time in between registering and invalidating the signal.
     */
    private final class TimedSignal extends RegisteredSignal
    {
        private final Timer.Context context;

        private TimedSignal(Timer.Context context)
        {
            this.context = context;
        }

        @Override
        public boolean checkAndClear()
        {
            context.stop();
            return super.checkAndClear();
        }

        @Override
        public void cancel()
        {
            if (!isCancelled())
            {
                context.stop();
                super.cancel();
            }
        }
    }

    /**
     * An abstract signal wrapping multiple delegate signals
     */
    private abstract static class MultiSignal extends AbstractSignal
    {
        final Signal[] signals;
        protected MultiSignal(Signal[] signals)
        {
            this.signals = signals;
        }

        public boolean isCancelled()
        {
            for (Signal signal : signals)
                if (!signal.isCancelled())
                    return false;
            return true;
        }

        public boolean checkAndClear()
        {
            for (Signal signal : signals)
                signal.checkAndClear();
            return isSignalled();
        }

        public void cancel()
        {
            for (Signal signal : signals)
                signal.cancel();
        }
    }

    /**
     * A Signal that wraps multiple Signals and returns when any single one of them would have returned
     */
    private static class AnySignal extends MultiSignal
    {
        protected AnySignal(Signal ... signals)
        {
            super(signals);
        }

        public boolean isSignalled()
        {
            for (Signal signal : signals)
                if (signal.isSignalled())
                    return true;
            return false;
        }

        public boolean isSet()
        {
            for (Signal signal : signals)
                if (signal.isSet())
                    return true;
            return false;
        }
    }

    /**
     * A Signal that wraps multiple Signals and returns when all of them would have finished returning
     */
    private static class AllSignal extends MultiSignal
    {
        protected AllSignal(Signal ... signals)
        {
            super(signals);
        }

        public boolean isSignalled()
        {
            for (Signal signal : signals)
                if (!signal.isSignalled())
                    return false;
            return true;
        }

        public boolean isSet()
        {
            for (Signal signal : signals)
                if (!signal.isSet())
                    return false;
            return true;
        }
    }

    /**
     * @param signals
     * @return a signal that returns only when any of the provided signals would have returned
     */
    public static Signal any(Signal ... signals)
    {
        return new AnySignal(signals);
    }

    /**
     * @param signals
     * @return a signal that returns only when all provided signals would have returned
     */
    public static Signal all(Signal ... signals)
    {
        return new AllSignal(signals);
    }

    /**
     * Loops waiting on the supplied condition and WaitQueue and will not return until the condition is true
     */
    public static void waitOnCondition(BooleanSupplier condition, WaitQueue queue)
    {
        while (!condition.getAsBoolean())
        {
            Signal s = queue.register();
            if (!condition.getAsBoolean())
            {
                s.awaitUninterruptibly();
            }
            else
            {
                s.cancel();
            }
        }
    }
}
