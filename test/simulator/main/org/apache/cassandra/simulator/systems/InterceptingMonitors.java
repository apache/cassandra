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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.systems.InterceptedWait.InterceptedConditionWait;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.concurrent.Awaitable.SyncAwaitable;
import org.apache.cassandra.utils.concurrent.Threads;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SIMULATOR_DEBUG;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.NEMESIS;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.SLEEP_UNTIL;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.UNBOUNDED_WAIT;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.WAIT_UNTIL;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Trigger.SIGNAL;
import static org.apache.cassandra.simulator.systems.InterceptibleThread.interceptorOrDefault;
import static org.apache.cassandra.simulator.systems.InterceptingMonitors.WaitListAccessor.LOCK;
import static org.apache.cassandra.simulator.systems.InterceptingMonitors.WaitListAccessor.NOTIFY;
import static org.apache.cassandra.simulator.systems.SimulatedTime.Global.relativeToGlobalNanos;

@PerClassLoader
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public abstract class InterceptingMonitors implements InterceptorOfGlobalMethods, Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(InterceptingMonitors.class);
    private static final boolean DEBUG_MONITOR_STATE = TEST_SIMULATOR_DEBUG.getBoolean();

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
        Deque<Object> recentActions = DEBUG_MONITOR_STATE ? new ArrayDeque<>() : null;

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
                assert remove.next == null;
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
                wait.nextLength = 0;
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
            assert suspended > 0;
            heldBy = wait.waiting;
            depth = wait.unsuspendMonitor();
            --suspended;
        }

        void claim(InterceptedMonitorWait wait)
        {
            assert heldBy == null || heldBy == wait.waiting;
            assert depth == 0;
            heldBy = wait.waiting;
            depth = wait.unsuspendMonitor();
        }

        void log(Object event, Thread toThread, Thread byThread)
        {
            if (recentActions != null)
                log(event + " " + toThread + " by " + byThread);
        }

        void log(Object event, Thread toThread)
        {
            if (recentActions != null)
                log(event + " " + toThread);
        }

        void log(Object event)
        {
            if (recentActions == null)
                return;

            if (recentActions.size() > 20)
                recentActions.poll();
            recentActions.add(event + " " + depth);
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
        Kind kind;
        final long waitTime;
        final InterceptibleThread waiting;
        final CaptureSites captureSites;
        final InterceptorOfConsequences interceptedBy;
        final MonitorState state;
        final Object monitor;
        int suspendedMonitorDepth;

        Trigger trigger;
        boolean isTriggered;
        final List<TriggerListener> onTrigger = new ArrayList<>(3);

        boolean notifiedOfPause;
        boolean waitingOnRelinquish;

        WaitListAccessor waitingOn;
        volatile InterceptedMonitorWait next;
        int nextLength;
        boolean hasExited;

        InterceptedMonitorWait(Kind kind, long waitTime, MonitorState state, InterceptibleThread waiting, CaptureSites captureSites)
        {
            this.kind = kind;
            this.waitTime = waitTime;
            this.waiting = waiting;
            this.captureSites = captureSites;
            this.interceptedBy = waiting.interceptedBy();
            this.state = state;
            this.monitor = this;
        }

        InterceptedMonitorWait(Kind kind, long waitTime, MonitorState state, InterceptibleThread waiting, CaptureSites captureSites, Object object)
        {
            this.kind = kind;
            this.waitTime = waitTime;
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

        public boolean isInterruptible()
        {
            return true;
        }

        @Override
        public long waitTime()
        {
            return waitTime;
        }

        @Override
        public void interceptWakeup(Trigger trigger, Thread by)
        {
            if (this.trigger != null && this.trigger.compareTo(trigger) >= 0)
                return;

            this.trigger = trigger;
            if (captureSites != null)
                captureSites.registerWakeup(by);
            interceptorOrDefault(by).interceptWakeup(this, trigger, interceptedBy);
        }

        public void triggerAndAwaitDone(InterceptorOfConsequences interceptor, Trigger trigger)
        {
            if (isTriggered)
                return;

            if (hasExited)
                throw failWithOOM();

            state.removeWaitingOn(this); // if still present, remove

            // we may have been assigned ownership of the lock if we attempted to trigger but found the lock held
            if (state.heldBy != null && state.heldBy != waiting)
            {   // reset this condition to wait on lock release
                state.waitOn(LOCK, this);
                this.kind = UNBOUNDED_WAIT;
                this.trigger = null;
                interceptor.beforeInvocation(waiting);
                interceptor.interceptWait(this);
                return;
            }

            try
            {
                synchronized (monitor)
                {
                    waiting.beforeInvocation(interceptor, this);

                    isTriggered = true;
                    onTrigger.forEach(listener -> listener.onTrigger(this));

                    if (!waiting.preWakeup(this))
                        monitor.notifyAll(); // TODO: could use interrupts to target waiting anyway, avoiding notifyAll()

                    while (!notifiedOfPause)
                        monitor.wait();

                    if (waitingOnRelinquish)
                    {
                        waitingOnRelinquish = false;
                        monitor.notifyAll(); // TODO: could use interrupts to target waiting anyway, avoiding notifyAll()
                    }
                }
            }
            catch (InterruptedException ie)
            {
                throw new UncheckedInterruptedException(ie);
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
            notifiedOfPause = true;
            if (Thread.holdsLock(monitor))
            {
                monitor.notifyAll();
                waitingOnRelinquish = true;
                try { while (waitingOnRelinquish) monitor.wait(); }
                catch (InterruptedException e) { throw new UncheckedInterruptedException(e); }
            }
            else
            {
                synchronized (monitor)
                {
                    monitor.notifyAll();
                }
            }
        }

        void await() throws InterruptedException
        {
            try
            {
                while (!isTriggered())
                    monitor.wait();
            }
            finally
            {
                hasExited = true;
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
                remove.next = null;
                --nextLength;
            }
            return this;
        }

        public String toString()
        {
            return captureSites == null ? "" : "[" + captureSites + ']';
        }
    }

    final RandomSource random;
    private final Map<Object, MonitorState> monitors = new IdentityHashMap<>();
    private final Map<Thread, Object> waitingOn = new IdentityHashMap<>();
    protected boolean disabled;

    public InterceptingMonitors(RandomSource random)
    {
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
        InterceptibleThread thread = ifIntercepted();
        if (thread == null)
        {
            Clock.waitUntil(deadline);
            return;
        }

        if (Thread.interrupted())
            throw new InterruptedException();

        InterceptedMonitorWait trigger = new InterceptedMonitorWait(SLEEP_UNTIL, deadline, new MonitorState(), thread, captureWaitSite(thread));
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

    @Override
    public void sleep(long period, TimeUnit units) throws InterruptedException
    {
        waitUntil(nanoTime() + units.toNanos(period));
    }

    @Override
    public void sleepUninterriptibly(long period, TimeUnit units)
    {
        try
        {
            sleep(period, units);
        }
        catch (InterruptedException e)
        {
            // instead of looping uninterruptibly
            throw new UncheckedInterruptedException(e);
        }
    }

    public boolean waitUntil(Object monitor, long deadline) throws InterruptedException
    {
        InterceptibleThread thread = ifIntercepted();
        if (thread == null) return SyncAwaitable.waitUntil(monitor, deadline);
        else return wait(monitor, thread, WAIT_UNTIL, deadline);
    }

    @Override
    public void wait(Object monitor) throws InterruptedException
    {
        InterceptibleThread thread = ifIntercepted();
        if (thread == null) monitor.wait();
        else wait(monitor, thread, UNBOUNDED_WAIT, -1L);
    }

    @Override
    public void wait(Object monitor, long millis) throws InterruptedException
    {
        InterceptibleThread thread = ifIntercepted();
        if (thread == null) monitor.wait(millis);
        else wait(monitor, thread, WAIT_UNTIL, relativeToGlobalNanos(MILLISECONDS.toNanos(millis)));
    }

    @Override
    public void wait(Object monitor, long millis, int nanos) throws InterruptedException
    {
        InterceptibleThread thread = ifIntercepted();
        if (thread == null) monitor.wait(millis, nanos);
        else wait(monitor, thread, WAIT_UNTIL, relativeToGlobalNanos(MILLISECONDS.toNanos(millis) + nanos));
    }

    private boolean wait(Object monitor, InterceptibleThread thread, InterceptedWait.Kind kind, long waitNanos) throws InterruptedException
    {
        if (Thread.interrupted())
            throw new InterruptedException();

        MonitorState state = state(monitor);
        InterceptedMonitorWait trigger = new InterceptedMonitorWait(kind, waitNanos, state, thread, captureWaitSite(thread), monitor);
        state.log("enterwait", thread);
        state.suspend(trigger);
        state.waitOn(NOTIFY, trigger);
        wakeOneWaitingOnLock(thread, state);
        thread.interceptWait(trigger);
        try
        {
            trigger.await();
        }
        finally
        {
            state.restore(trigger);
            state.log("exitwait", thread);
        }
        return trigger.trigger == SIGNAL;
    }

    public void notify(Object monitor)
    {
        MonitorState state = state(monitor);
        if (state != null)
        {
            InterceptedMonitorWait wake = state.removeOneWaitingOn(NOTIFY, random);
            if (wake != null)
            {
                // TODO: assign ownership on monitorExit
                assert wake.waitingOn == null;
                Thread waker = Thread.currentThread();
                wake.interceptWakeup(SIGNAL, waker);
                state.log("notify", wake.waiting, waker);
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
            InterceptedMonitorWait wake = state.removeAllWaitingOn(NOTIFY);
            if (wake != null)
            {
                Thread waker = Thread.currentThread();
                wake.interceptWakeup(SIGNAL, waker);
                state.log("notify", wake.waiting, waker);

                wake = wake.next;
                while (wake != null)
                {
                    InterceptedMonitorWait next = wake.next;
                    state.waitOn(LOCK, wake);
                    state.log("movetowaitonlock ", wake.waiting, waker);
                    wake = next;
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

        boolean restoreInterrupt = false;
        InterceptibleThread thread = (InterceptibleThread) anyThread;
        try
        {
            if (   !thread.isEvaluationDeterministic()
                && random.decide(preMonitorDelayChance))
            {
                // TODO (feature): hold a stack of threads already paused by the nemesis, and, if one of the threads
                //        is entering the monitor, put the contents of this stack into `waitingOn` for this monitor.
                InterceptedConditionWait signal = new InterceptedConditionWait(NEMESIS, 0L, thread, captureWaitSite(thread), null);
                thread.interceptWait(signal);

                // save interrupt state to restore afterwards - new ones only arrive if terminating simulation
                restoreInterrupt = Thread.interrupted();
                while (true)
                {
                    try
                    {
                        signal.awaitDeclaredUninterruptible();
                        break;
                    }
                    catch (InterruptedException e)
                    {
                        if (disabled)
                            throw new UncheckedInterruptedException(e);
                        restoreInterrupt = true;
                    }
                }
            }

            MonitorState state = state(monitor);
            if (state.heldBy != thread)
            {
                if (state.heldBy != null)
                {
                    if (!thread.isIntercepting() && disabled) return;
                    else if (!thread.isIntercepting())
                        throw new AssertionError();

                    checkForDeadlock(thread, state.heldBy);
                    InterceptedMonitorWait wait = new InterceptedMonitorWait(UNBOUNDED_WAIT, 0L, state, thread, captureWaitSite(thread));
                    wait.suspendedMonitorDepth = 1;
                    state.log("monitorenter_wait", thread);
                    state.waitOn(LOCK, wait);
                    thread.interceptWait(wait);
                    synchronized (wait)
                    {
                        waitingOn.put(thread, monitor);
                        restoreInterrupt |= Thread.interrupted();
                        try
                        {
                            while (true)
                            {
                                try
                                {
                                    wait.await();
                                    break;
                                }
                                catch (InterruptedException e)
                                {
                                    if (disabled)
                                    {
                                        if (state.heldBy == thread)
                                        {
                                            state.heldBy = null;
                                            state.depth = 0;
                                        }
                                        throw new UncheckedInterruptedException(e);
                                    }

                                    restoreInterrupt = true;
                                    if (wait.isTriggered)
                                        break;
                                }
                            }
                        }
                        finally
                        {
                            waitingOn.remove(thread);
                        }
                    }
                    state.claim(wait);
                    state.log("monitorenter_claim", thread);
                }
                else
                {
                    state.log("monitorenter_free", thread);
                    state.heldBy = thread;
                    state.depth = 1;
                }
            }
            else
            {
                state.log("monitorreenter", thread);
                state.depth++;
            }
        }
        finally
        {
            if (restoreInterrupt)
                thread.interrupt();
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
        {
            state.log("monitorreexit", thread);
            return;
        }

        state.log("monitorexit", thread);
        state.heldBy = null;

        if (!wakeOneWaitingOnLock(thread, state))
        {
            maybeClear(monitor, state);
        }
    }

    private boolean wakeOneWaitingOnLock(Thread waker, MonitorState state)
    {
        InterceptedMonitorWait wake = state.removeOneWaitingOn(LOCK, random);
        if (wake != null)
        {
            assert wake.waitingOn == null;
            assert !wake.isTriggered();

            wake.interceptWakeup(SIGNAL, waker);

            // assign them the lock, so they'll definitely get it when they wake
            assert state.heldBy == null;
            state.heldBy = wake.waiting;
            state.log("wake", wake.waiting);
            return true;
        }
        return false;
    }

    // TODO (feature): integrate LockSupport waits into this deadlock check
    private void checkForDeadlock(Thread waiting, Thread blockedBy)
    {
        Thread cur = blockedBy;
        while (true)
        {
            Object monitor = waitingOn.get(cur);
            if (monitor == null)
                return;
            MonitorState state = monitors.get(monitor);
            if (state == null)
                return;
            Thread next = state.heldBy;
            if (next == cur)
                return; // not really waiting, just hasn't woken up yet
            if (next == waiting)
            {
                logger.error("Deadlock between {}{} and {}{}", waiting, Threads.prettyPrintStackTrace(waiting, true, ";"), cur, Threads.prettyPrintStackTrace(cur, true, ";"));
                throw failWithOOM();
            }
            cur = next;
        }
    }

    @Override
    public void park()
    {
        InterceptibleThread.park();
    }

    @Override
    public void parkNanos(long nanos)
    {
        InterceptibleThread.parkNanos(nanos);
    }

    @Override
    public void parkUntil(long millis)
    {
        InterceptibleThread.parkUntil(millis);
    }

    @Override
    public void park(Object blocker)
    {
        InterceptibleThread.park(blocker);
    }

    @Override
    public void parkNanos(Object blocker, long nanos)
    {
        InterceptibleThread.parkNanos(blocker, nanos);
    }

    @Override
    public void parkUntil(Object blocker, long millis)
    {
        InterceptibleThread.parkUntil(blocker, millis);
    }

    @Override
    public void unpark(Thread thread)
    {
        InterceptibleThread.unpark(thread);
    }

    public void close()
    {
        disabled = true;
    }
}
