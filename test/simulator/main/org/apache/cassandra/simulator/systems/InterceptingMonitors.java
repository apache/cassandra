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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.concurrent.Awaitable.SyncAwaitable;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.NEMESIS;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.SLEEP;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.TIMED_WAIT;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.UNBOUNDED_WAIT;
import static org.apache.cassandra.simulator.systems.InterceptingMonitors.WaitListAccessor.LOCK;
import static org.apache.cassandra.simulator.systems.InterceptingMonitors.WaitListAccessor.NOTIFY;

@PerClassLoader
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public abstract class InterceptingMonitors implements InterceptorOfGlobalMethods, Closeable
{
    // TODO (future): embed inside the objects we're weaving where possible
    static class MonitorState
    {
        InterceptedMonitorWait waitingOnNotify;
        InterceptedMonitorWait waitingOnLock;
        /**
         * The thread we have assigned lock ownership to.
         * This may not be actively holding the lock, if
         * we found it waiting for the monitor and assigned
         * it to receive
         */
        InterceptibleThread heldBy;
        int depth;
        int suspended;

        boolean isEmpty()
        {
            return depth == 0 && waitingOnLock == null && waitingOnNotify == null && suspended == 0;
        }

        InterceptedMonitorWait removeAllWaitingOn(WaitListAccessor list)
        {
            InterceptedMonitorWait result = list.head(this);
            list.setHead(this, null);

            InterceptedMonitorWait cur = result;
            while (cur != null)
            {
                InterceptedMonitorWait next = cur.next;
                cur.waitingOn = null;
                cur.next = null;
                cur = next;
            }
            return result;
        }

        void removeWaitingOn(InterceptedMonitorWait remove)
        {
            if (remove.waitingOn != null)
            {
                InterceptedMonitorWait head = remove.waitingOn.head(this);
                remove.waitingOn.setHead(this, head.remove(remove));
            }
        }

        @Inline
        InterceptedMonitorWait removeOneWaitingOn(WaitListAccessor list, RandomSource random)
        {
            InterceptedMonitorWait head = list.head(this);
            if (head == null)
                return null;

            if (head.next == null)
            {
                list.setHead(this, null);
                head.waitingOn = null;
                return head;
            }

            int i = random.uniform(0, 1 + head.nextLength);
            if (i == 0)
            {
                list.setHead(this, head.next);
                head.next.nextLength = head.nextLength - 1;
                head.next = null;
                head.waitingOn = null;
                return head;
            }

            InterceptedMonitorWait pred = head;
            while (--i > 0)
                pred = pred.next;

            InterceptedMonitorWait result = pred.next;
            pred.next = result.next;
            --head.nextLength;
            result.next = null;
            result.waitingOn = null;
            return result;
        }

        void waitOn(WaitListAccessor list, InterceptedMonitorWait wait)
        {
            assert wait.waitingOn == null;
            wait.waitingOn = list;

            assert wait.next == null;
            InterceptedMonitorWait head = list.head(this);
            if (head != null)
            {
                wait.next = head.next;
                head.next = wait;
                ++head.nextLength;
            }
            else
            {
                list.setHead(this, wait);
            }
        }

        void suspend(InterceptedMonitorWait wait)
        {
            assert heldBy == wait.waiting;
            wait.suspendMonitor(depth);
            ++suspended;
            heldBy = null;
            depth = 0;
        }

        void restore(InterceptedMonitorWait wait)
        {
            assert heldBy == null || heldBy == wait.waiting;
            assert depth == 0;
            heldBy = wait.waiting;
            depth = wait.unsuspendMonitor();
            --suspended;
        }
    }

    interface WaitListAccessor
    {
        static final WaitListAccessor NOTIFY = new WaitListAccessor()
        {
            @Override public InterceptedMonitorWait head(MonitorState state) { return state.waitingOnNotify; }
            @Override public void setHead(MonitorState state, InterceptedMonitorWait newHead) { state.waitingOnNotify = newHead; }
        };

        static final WaitListAccessor LOCK = new WaitListAccessor()
        {
            @Override public InterceptedMonitorWait head(MonitorState state) { return state.waitingOnLock; }
            @Override public void setHead(MonitorState state, InterceptedMonitorWait newHead) { state.waitingOnLock = newHead; }
        };

        InterceptedMonitorWait head(MonitorState state);
        void setHead(MonitorState state, InterceptedMonitorWait newHead);
    }

    static class InterceptedMonitorWait implements InterceptedWait
    {
        final Kind kind;
        final InterceptibleThread waiting;
        final CaptureSites captureSites;
        final InterceptorOfConsequences interceptedBy;
        final MonitorState state;
        final Object monitor;
        int suspendedMonitorDepth;

        boolean isTriggeredByTimeout;
        boolean isTriggered;
        final List<TriggerListener> onTrigger = new ArrayList<>(3);

        boolean isNotifiedOfThreadPause;

        WaitListAccessor waitingOn;
        volatile InterceptedMonitorWait next;
        int nextLength;

        InterceptedMonitorWait(Kind kind, MonitorState state, InterceptibleThread waiting, CaptureSites captureSites)
        {
            this.kind = kind;
            this.waiting = waiting;
            this.captureSites = captureSites;
            this.interceptedBy = waiting.interceptedBy();
            this.state = state;
            this.monitor = this;
        }

        InterceptedMonitorWait(Kind kind, MonitorState state, InterceptibleThread waiting, CaptureSites captureSites, Object object)
        {
            this.kind = kind;
            this.waiting = waiting;
            this.captureSites = captureSites;
            this.interceptedBy = waiting.interceptedBy();
            this.state = state;
            this.monitor = object;
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        void suspendMonitor(int depth)
        {
            assert suspendedMonitorDepth == 0;
            suspendedMonitorDepth = depth;
        }

        int unsuspendMonitor()
        {
            assert suspendedMonitorDepth > 0;
            int result = suspendedMonitorDepth;
            suspendedMonitorDepth = 0;
            return result;
        }

        public boolean isTriggered()
        {
            return isTriggered;
        }

        public void triggerAndAwaitDone(InterceptorOfConsequences interceptor, boolean isTimeout)
        {
            if (isTriggered)
                return;

            // we may have been assigned ownership of the lock if we attempted to trigger but found the lock held
            if (state.heldBy != null && state.heldBy != waiting)
            {
                state.waitOn(LOCK, this);
                interceptor.beforeInvocation(waiting);
                interceptor.interceptWait(null);
                return;
            }

            synchronized (monitor)
            {
                waiting.beforeInvocation(interceptor, this);

                this.isTriggeredByTimeout = isTimeout;
                isTriggered = true;
                onTrigger.forEach(listener -> listener.onTrigger(this));

                state.removeWaitingOn(this); // if still present, remove
                monitor.notifyAll();

                try
                {
                    while (!isNotifiedOfThreadPause)
                        monitor.wait();
                }
                catch (InterruptedException ie)
                {
                    if (!isTriggered)
                        throw new UncheckedInterruptedException(ie);
                }
            }
        }

        @Override
        public void triggerBypass()
        {
            if (isTriggered)
                return;

            synchronized (monitor)
            {
                isTriggered = true;
                monitor.notifyAll();
                state.removeWaitingOn(this);
            }
        }

        @Override
        public void addListener(TriggerListener onTrigger)
        {
            this.onTrigger.add(onTrigger);
        }

        @Override
        public Thread waiting()
        {
            return waiting;
        }

        @Override
        public void notifyThreadPaused()
        {
            synchronized (monitor)
            {
                isNotifiedOfThreadPause = true;
                monitor.notifyAll();
            }
        }

        void await() throws InterruptedException
        {
            try
            {
                while (!isTriggered())
                    monitor.wait();
            }
            catch (InterruptedException e)
            {
                if (!isTriggered)
                    throw e;
            }
        }

        InterceptedMonitorWait remove(InterceptedMonitorWait remove)
        {
            remove.waitingOn = null;

            if (remove == this)
            {
                InterceptedMonitorWait next = this.next;
                if (next != null)
                {
                    next.nextLength = nextLength - 1;
                    remove.next = null;
                }

                return next;
            }

            InterceptedMonitorWait cur = this;
            while (cur != null && cur.next != remove)
                cur = cur.next;

            if (cur != null)
            {
                cur.next = remove.next;
                --nextLength;
            }
            return this;
        }

        public String toString()
        {
            return captureSites == null ? "" : captureSites.toString();
        }
    }

    final InterceptorOfWaits interceptorOfWaits;
    final RandomSource random;
    private final Map<Object, MonitorState> monitors = new IdentityHashMap<>();
    private boolean disabled;

    public InterceptingMonitors(InterceptorOfWaits interceptorOfWaits, RandomSource random)
    {
        this.interceptorOfWaits = interceptorOfWaits;
        this.random = random;
    }

    private MonitorState state(Object monitor)
    {
        return monitors.computeIfAbsent(monitor, ignore -> new MonitorState());
    }

    private MonitorState maybeState(Object monitor)
    {
        return monitors.get(monitor);
    }

    private void maybeClear(Object monitor, MonitorState state)
    {
        if (state.isEmpty())
            monitors.remove(monitor, state);
    }

    @Override
    public void waitUntil(long deadline) throws InterruptedException
    {
        InterceptibleThread thread = interceptorOfWaits.ifIntercepted();
        if (thread == null)
        {
            Clock.waitUntil(deadline);
            return;
        }

        InterceptedMonitorWait trigger = new InterceptedMonitorWait(SLEEP, new MonitorState(), thread, interceptorOfWaits.captureWaitSite(thread));
        thread.interceptWait(trigger);
        synchronized (trigger)
        {
            try
            {
                trigger.await();
            }
            catch (InterruptedException e)
            {
                if (!trigger.isTriggered)
                    throw e;
            }
        }
    }

    public boolean waitUntil(Object monitor, long deadline) throws InterruptedException
    {
        InterceptibleThread thread = interceptorOfWaits.ifIntercepted();
        if (thread == null) return SyncAwaitable.waitUntil(monitor, deadline);
        else return wait(monitor, thread, TIMED_WAIT);
    }

    @Override
    public void wait(Object monitor) throws InterruptedException
    {
        InterceptibleThread thread = interceptorOfWaits.ifIntercepted();
        if (thread == null) monitor.wait();
        else wait(monitor, thread, UNBOUNDED_WAIT);
    }

    @Override
    public void wait(Object monitor, long millis) throws InterruptedException
    {
        InterceptibleThread thread = interceptorOfWaits.ifIntercepted();
        if (thread == null) monitor.wait(millis);
        else wait(monitor, thread, TIMED_WAIT);
    }

    @Override
    public void wait(Object monitor, long millis, int nanos) throws InterruptedException
    {
        InterceptibleThread thread = interceptorOfWaits.ifIntercepted();
        if (thread == null) monitor.wait(millis, nanos);
        else wait(monitor, thread, TIMED_WAIT);
    }

    private boolean wait(Object monitor, InterceptibleThread thread, InterceptedWait.Kind kind) throws InterruptedException
    {
        MonitorState state = state(monitor);
        InterceptedMonitorWait trigger = new InterceptedMonitorWait(kind, state, thread, interceptorOfWaits.captureWaitSite(thread), monitor);
        state.suspend(trigger);
        state.waitOn(NOTIFY, trigger);
        thread.interceptWait(trigger);
        trigger.await();
        state.restore(trigger);
        return !trigger.isTriggeredByTimeout;
    }

    public void notify(Object monitor)
    {
        MonitorState state = state(monitor);
        if (state != null)
        {
            InterceptedMonitorWait wait = state.removeOneWaitingOn(NOTIFY, random);
            if (wait != null)
            {
                assert wait.waitingOn == null;
                Thread waker = Thread.currentThread();
                interceptorOfWaits.interceptSignal(waker, wait, wait.captureSites, wait.interceptedBy);
                return;
            }
        }
        monitor.notify();
    }

    @Override
    public void notifyAll(Object monitor)
    {
        MonitorState state = state(monitor);
        if (state != null)
        {
            InterceptedMonitorWait wait = state.removeAllWaitingOn(NOTIFY);
            if (wait != null)
            {
                Thread waker = Thread.currentThread();
                interceptorOfWaits.interceptSignal(waker, wait, wait.captureSites, wait.interceptedBy);

                wait = wait.next;
                while (wait != null)
                {
                    InterceptedMonitorWait next = wait.next;
                    state.waitOn(LOCK, wait);
                    wait = next;
                }
                return;
            }
        }
        monitor.notifyAll();
    }

    @Override
    public void preMonitorEnter(Object monitor, float preMonitorDelayChance)
    {
        if (disabled)
            return;

        Thread anyThread = Thread.currentThread();
        if (!(anyThread instanceof InterceptibleThread))
            return;

        InterceptibleThread thread = (InterceptibleThread) anyThread;

        // TODO(now): check for reentrance, and avoid switching threads if this thread is already
        // holding a monitor.
        // TODO(now): hold a stack of threads already paused by the nemesis, and, if one of the threads
        // is entering the monitor, put the contents of this stack into `waitingOn` for this monitor.
        if (random.decide(preMonitorDelayChance))
        {
            InterceptedWait.InterceptedConditionWait signal = new InterceptedWait.InterceptedConditionWait(NEMESIS,
                                                                                                           thread,
                                                                                                           interceptorOfWaits.captureWaitSite(thread),
                                                                                                           null);
            thread.interceptWait(signal);
            signal.awaitThrowUncheckedOnInterrupt();
        }

        MonitorState state = state(monitor);
        if (state.heldBy != thread)
        {
            if (state.heldBy != null)
            {
                if (!thread.isIntercepting() && disabled) return;
                else if (!thread.isIntercepting())
                    throw new AssertionError();

                InterceptedMonitorWait wait = new InterceptedMonitorWait(UNBOUNDED_WAIT, state, thread, interceptorOfWaits.captureWaitSite(thread));
                wait.suspendedMonitorDepth = 1;
                state.waitOn(LOCK, wait);
                thread.interceptWait(wait);
                synchronized (wait)
                {
                    try
                    {
                        wait.await();
                    }
                    catch (InterruptedException e)
                    {
                        throw new UncheckedInterruptedException(e);
                    }
                }
                state.restore(wait);
            }
            else
            {
                state.heldBy = thread;
                state.depth = 1;
            }
        }
        else
        {
            state.depth++;
        }
    }

    @Override
    public void preMonitorExit(Object monitor)
    {
        if (disabled)
            return;

        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread))
            return;

        MonitorState state = maybeState(monitor);
        if (state == null)
            return;

        if (state.heldBy != thread)
            throw new AssertionError();

        if (--state.depth > 0)
            return;

        state.heldBy = null;

        InterceptedMonitorWait wait = state.removeOneWaitingOn(LOCK, random);
        if (wait != null)
        {
            assert wait.waitingOn == null;
            assert !wait.isTriggered();

            interceptorOfWaits.interceptSignal(thread, wait, wait.captureSites, wait.interceptedBy);

            // assign them the lock, so they'll definitely get it when they wake
            state.heldBy = wait.waiting;
        }
        else
        {
            maybeClear(monitor, state);
        }
    }

    public void close()
    {
        disabled = true;
    }
}
