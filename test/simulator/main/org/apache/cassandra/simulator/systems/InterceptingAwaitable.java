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

package org.apache.cassandra.simulator.systems;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.cassandra.simulator.systems.InterceptedWait.InterceptedConditionWait;
import org.apache.cassandra.simulator.systems.InterceptedWait.TriggerListener;
import org.apache.cassandra.utils.concurrent.Awaitable;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.TIMED_WAIT;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.UNBOUNDED_WAIT;

abstract class InterceptingAwaitable implements Awaitable
{
    abstract boolean isSignalled();
    abstract Condition maybeIntercept(InterceptedWait.Kind kind);

    public boolean awaitUntil(long deadline) throws InterruptedException
    {
        maybeIntercept(TIMED_WAIT).awaitUntil(deadline);
        return isSignalled();
    }

    public boolean awaitUntilThrowUncheckedOnInterrupt(long deadline)
    {
        maybeIntercept(TIMED_WAIT).awaitUntilThrowUncheckedOnInterrupt(deadline);
        return isSignalled();
    }

    public boolean awaitUntilUninterruptibly(long deadline)
    {
        maybeIntercept(TIMED_WAIT).awaitUntilUninterruptibly(deadline);
        return isSignalled();
    }

    public Awaitable await() throws InterruptedException
    {
        maybeIntercept(UNBOUNDED_WAIT).await();
        return this;
    }

    public Awaitable awaitThrowUncheckedOnInterrupt()
    {
        maybeIntercept(UNBOUNDED_WAIT).awaitThrowUncheckedOnInterrupt();
        return this;
    }

    public Awaitable awaitUninterruptibly()
    {
        maybeIntercept(UNBOUNDED_WAIT).awaitUninterruptibly();
        return this;
    }

    public boolean await(long time, TimeUnit units) throws InterruptedException
    {
        maybeIntercept(TIMED_WAIT).await(time, units);
        return isSignalled();
    }

    public boolean awaitThrowUncheckedOnInterrupt(long time, TimeUnit units)
    {
        maybeIntercept(TIMED_WAIT).awaitThrowUncheckedOnInterrupt(time, units);
        return isSignalled();
    }

    public boolean awaitUninterruptibly(long time, TimeUnit units)
    {
        maybeIntercept(TIMED_WAIT).awaitUninterruptibly(time, units);
        return isSignalled();
    }

    static class InterceptingCondition extends InterceptingAwaitable implements Condition, TriggerListener
    {
        private final InterceptorOfWaits interceptorOfWaits;
        final Condition inner = new NotInterceptedSyncCondition();
        private List<InterceptedConditionWait> intercepted;

        public InterceptingCondition(InterceptorOfWaits interceptorOfWaits)
        {
            this.interceptorOfWaits = interceptorOfWaits;
        }

        Condition maybeIntercept(InterceptedWait.Kind kind)
        {
            if (inner.isSignalled())
                return inner;

            InterceptibleThread thread = interceptorOfWaits.ifIntercepted();
            if (thread == null)
                return inner;

            InterceptedConditionWait signal = new InterceptedConditionWait(kind, thread, interceptorOfWaits.captureWaitSite(thread), inner);
            synchronized (this)
            {
                if (intercepted == null)
                    intercepted = new ArrayList<>(2);
                intercepted.add(signal);
            }
            signal.addListener(this);
            thread.interceptWait(signal);
            return signal;
        }

        public boolean isSignalled()
        {
            return inner.isSignalled();
        }

        public void signal()
        {
            if (isSignalled())
                return;

            inner.signal();
            synchronized (this)
            {
                if (intercepted != null)
                {
                    Thread signalledBy = Thread.currentThread();
                    intercepted.forEach(signal -> {
                        // TODO (now): make captureSites and interceptedBy methods of InterceptedWait?
                        interceptorOfWaits.interceptSignal(signalledBy, signal, signal.captureSites, signal.interceptedBy);
                    });
                }
            }
        }

        @Override
        public synchronized void onTrigger(InterceptedWait triggered)
        {
            intercepted.remove(triggered);
        }
    }

    static class InterceptingCountDownLatch extends InterceptingCondition implements CountDownLatch
    {
        private final AtomicInteger count;

        public InterceptingCountDownLatch(InterceptorOfWaits interceptor, int count)
        {
            super(interceptor);
            this.count = new AtomicInteger(count);
        }

        public void decrement()
        {
            if (count.decrementAndGet() == 0)
                signal();
        }

        public int count()
        {
            return count.get();
        }
    }

    static class InterceptingSignal<V> extends InterceptingAwaitable implements WaitQueue.Signal
    {
        final InterceptorOfWaits interceptorOfWaits;
        final Condition inner = new NotInterceptedSyncCondition();
        final V supplyOnDone;
        final Consumer<V> receiveOnDone;

        InterceptedConditionWait intercepted;

        boolean isSignalled;
        boolean isCancelled;

        InterceptingSignal(InterceptorOfWaits interceptorOfWaits)
        {
            this(interceptorOfWaits, null, ignore -> {});
        }

        InterceptingSignal(InterceptorOfWaits interceptorOfWaits, V supplyOnDone, Consumer<V> receiveOnDone)
        {
            this.interceptorOfWaits = interceptorOfWaits;
            this.supplyOnDone = supplyOnDone;
            this.receiveOnDone = receiveOnDone;
        }

        public boolean isSignalled()
        {
            return isSignalled;
        }

        public synchronized boolean isCancelled()
        {
            return isCancelled;
        }

        public synchronized boolean isSet()
        {
            return isCancelled | isSignalled;
        }

        public void signal()
        {
            doSignal();
        }

        synchronized boolean doSignal()
        {
            if (isSet())
                return false;

            isSignalled = true;
            receiveOnDone.accept(supplyOnDone);
            inner.signal();
            if (intercepted != null && !intercepted.isTriggered())
            {
                Thread thread = Thread.currentThread();
                interceptorOfWaits.interceptSignal(thread, intercepted, intercepted.captureSites, intercepted.interceptedBy);
            }
            return true;
        }

        public synchronized boolean checkAndClear()
        {
            if (isSet())
                return isSignalled;
            isCancelled = true;
            receiveOnDone.accept(supplyOnDone);
            inner.signal();
            return false;
        }

        public synchronized void cancel()
        {
            checkAndClear();
        }

        Condition maybeIntercept(InterceptedWait.Kind kind)
        {
            assert intercepted == null;
            assert !inner.isSignalled();

            InterceptibleThread thread = interceptorOfWaits.ifIntercepted();
            if (thread == null)
                return inner;

            intercepted = new InterceptedConditionWait(kind, thread, interceptorOfWaits.captureWaitSite(thread), inner);
            thread.interceptWait(intercepted);
            return intercepted;
        }
    }
}
