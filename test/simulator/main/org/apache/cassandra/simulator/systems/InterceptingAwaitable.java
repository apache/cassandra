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
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.WAIT_UNTIL;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.UNBOUNDED_WAIT;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Trigger.SIGNAL;
import static org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods.Global.captureWaitSite;
import static org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods.Global.ifIntercepted;
import static org.apache.cassandra.simulator.systems.SimulatedTime.Global.localToGlobalNanos;
import static org.apache.cassandra.simulator.systems.SimulatedTime.Global.relativeToLocalNanos;

@PerClassLoader
abstract class InterceptingAwaitable implements Awaitable
{
    abstract boolean isSignalled();
    abstract Condition maybeIntercept(InterceptedWait.Kind kind, long waitNanos);

    Condition maybeInterceptThrowChecked(InterceptedWait.Kind kind, long waitNanos) throws InterruptedException
    {
        if (Thread.interrupted())
            throw new InterruptedException();

        return maybeIntercept(kind, waitNanos);
    }

    Condition maybeInterceptThrowUnchecked(InterceptedWait.Kind kind, long waitNanos)
    {
        if (Thread.interrupted())
            throw new UncheckedInterruptedException();

        return maybeIntercept(kind, waitNanos);
    }

    public boolean awaitUntil(long deadline) throws InterruptedException
    {
        maybeInterceptThrowChecked(WAIT_UNTIL, deadline).awaitUntil(deadline);
        return isSignalled();
    }

    public boolean awaitUntilThrowUncheckedOnInterrupt(long deadline)
    {
        maybeInterceptThrowUnchecked(WAIT_UNTIL, deadline).awaitUntilThrowUncheckedOnInterrupt(deadline);
        return isSignalled();
    }

    public boolean awaitUntilUninterruptibly(long deadline)
    {
        maybeIntercept(WAIT_UNTIL, deadline).awaitUntilUninterruptibly(deadline);
        return isSignalled();
    }

    public Awaitable await() throws InterruptedException
    {
        maybeInterceptThrowChecked(UNBOUNDED_WAIT, 0).await();
        return this;
    }

    public Awaitable awaitThrowUncheckedOnInterrupt()
    {
        maybeInterceptThrowUnchecked(UNBOUNDED_WAIT, 0).awaitThrowUncheckedOnInterrupt();
        return this;
    }

    public Awaitable awaitUninterruptibly()
    {
        maybeIntercept(UNBOUNDED_WAIT, 0).awaitUninterruptibly();
        return this;
    }

    public boolean await(long time, TimeUnit units) throws InterruptedException
    {
        long deadline = relativeToLocalNanos(units.toNanos(time));
        maybeInterceptThrowChecked(WAIT_UNTIL, localToGlobalNanos(deadline)).awaitUntil(deadline);
        return isSignalled();
    }

    public boolean awaitThrowUncheckedOnInterrupt(long time, TimeUnit units)
    {
        long deadline = relativeToLocalNanos(units.toNanos(time));
        maybeInterceptThrowUnchecked(WAIT_UNTIL, localToGlobalNanos(deadline)).awaitUntilThrowUncheckedOnInterrupt(deadline);
        return isSignalled();
    }

    public boolean awaitUninterruptibly(long time, TimeUnit units)
    {
        long deadline = relativeToLocalNanos(units.toNanos(time));
        maybeIntercept(WAIT_UNTIL, localToGlobalNanos(deadline)).awaitUntilUninterruptibly(deadline);
        return isSignalled();
    }

    @PerClassLoader
    static class InterceptingCondition extends InterceptingAwaitable implements Condition, TriggerListener
    {
        final Condition inner = new NotInterceptedSyncCondition();
        private List<InterceptedConditionWait> intercepted;

        public InterceptingCondition()
        {
        }

        Condition maybeIntercept(InterceptedWait.Kind kind, long waitNanos)
        {
            if (inner.isSignalled())
                return inner;

            InterceptibleThread thread = ifIntercepted();
            if (thread == null)
                return inner;

            InterceptedConditionWait signal = new InterceptedConditionWait(kind, waitNanos, thread, captureWaitSite(thread), inner);
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
                    intercepted.forEach(signal -> signal.interceptWakeup(SIGNAL, signalledBy));
                }
            }
        }

        @Override
        public synchronized void onTrigger(InterceptedWait triggered)
        {
            intercepted.remove(triggered);
        }
    }

    @PerClassLoader
    static class InterceptingCountDownLatch extends InterceptingCondition implements CountDownLatch
    {
        private final AtomicInteger count;

        public InterceptingCountDownLatch(int count)
        {
            super();
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

    @PerClassLoader
    static class InterceptingSignal<V> extends InterceptingAwaitable implements WaitQueue.Signal
    {
        final Condition inner = new NotInterceptedSyncCondition();
        final V supplyOnDone;
        final Consumer<V> receiveOnDone;

        InterceptedConditionWait intercepted;

        boolean isSignalled;
        boolean isCancelled;

        InterceptingSignal()
        {
            this(null, ignore -> {});
        }

        InterceptingSignal(V supplyOnDone, Consumer<V> receiveOnDone)
        {
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
                intercepted.interceptWakeup(SIGNAL, Thread.currentThread());
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

        Condition maybeIntercept(InterceptedWait.Kind kind, long waitNanos)
        {
            assert intercepted == null;
            assert !inner.isSignalled();

            InterceptibleThread thread = ifIntercepted();
            if (thread == null)
                return inner;

            intercepted = new InterceptedConditionWait(kind, waitNanos, thread, captureWaitSite(thread), inner);
            thread.interceptWait(intercepted);
            return intercepted;
        }
    }
}
