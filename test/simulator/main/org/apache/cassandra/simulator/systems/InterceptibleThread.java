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
import java.util.concurrent.locks.LockSupport;

import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.UNBOUNDED_WAIT;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.WAIT_UNTIL;
import static org.apache.cassandra.simulator.systems.InterceptibleThread.WaitTimeKind.ABSOLUTE_MILLIS;
import static org.apache.cassandra.simulator.systems.InterceptibleThread.WaitTimeKind.NONE;
import static org.apache.cassandra.simulator.systems.InterceptibleThread.WaitTimeKind.RELATIVE_NANOS;
import static org.apache.cassandra.utils.Shared.Recursive.ALL;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION, ancestors = ALL, members = ALL)
public class InterceptibleThread extends FastThreadLocalThread implements InterceptorOfConsequences
{
    enum WaitTimeKind
    {
        NONE, RELATIVE_NANOS, ABSOLUTE_MILLIS
    }

    // used to implement LockSupport.park/unpark
    @Shared(scope = SIMULATION)
    private class Parked implements InterceptedWait
    {
        final Kind kind;
        final InterceptorOfConsequences waitInterceptedBy;
        final List<TriggerListener> onTrigger = new ArrayList<>(3);
        final long waitTime;
        boolean isWakeIntercepted; // we have intercepted a pending wakeup
        boolean isDone; // we have been signalled (by the simulation or otherwise)

        Parked(Kind kind, long waitTime, InterceptorOfConsequences waitInterceptedBy)
        {
            this.kind = kind;
            this.waitTime = waitTime;
            this.waitInterceptedBy = waitInterceptedBy;
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        @Override
        public long waitTime()
        {
            return waitTime;
        }

        @Override
        public boolean isTriggered()
        {
            return parked != this;
        }

        @Override
        public synchronized void triggerAndAwaitDone(InterceptorOfConsequences interceptor, boolean isTimeout)
        {
            if (parked == null)
                return;

            beforeInvocation(interceptor, this);

            parked = null;
            onTrigger.forEach(listener -> listener.onTrigger(this));

            notify();

            try
            {
                while (!isDone)
                    wait();
            }
            catch (InterruptedException ie)
            {
                throw new UncheckedInterruptedException(ie);
            }
        }

        @Override
        public synchronized void triggerBypass()
        {
            parked = null;
            notifyAll();
            LockSupport.unpark(InterceptibleThread.this);
        }

        @Override
        public void addListener(TriggerListener onTrigger)
        {
            this.onTrigger.add(onTrigger);
        }

        @Override
        public Thread waiting()
        {
            return InterceptibleThread.this;
        }

        @Override
        public synchronized void notifyThreadPaused()
        {
            isDone = true;
            notifyAll();
        }

        synchronized void await()
        {
            try
            {
                while (!isTriggered())
                    wait();

                if (interruptOnWakeup)
                    doInterrupt();
                interruptOnWakeup = false;
            }
            catch (InterruptedException e)
            {
                if (!isTriggered()) throw new UncheckedInterruptedException(e);
                else doInterrupt();
            }
        }

        void interceptWakeIfNotAlready(InterceptibleThread by)
        {
            if (!isWakeIntercepted) by.interceptor.interceptWakeup(this, waitInterceptedBy);
            isWakeIntercepted = true;
        }

        @Override
        public String toString()
        {
            return "";
        }
    }

    private static InterceptorOfConsequences debug;

    final Object extraToStringInfo; // optional dynamic extra information for reporting with toString
    final String toString;
    final Runnable onTermination;
    private final InterceptorOfGlobalMethods interceptorOfGlobalMethods;
    private final SimulatedTime.LocalTime time;

    // this is set before the thread's execution begins/continues; events and cessation are reported back to this
    private InterceptorOfConsequences interceptor;
    private NotifyThreadPaused notifyOnPause;

    private boolean hasPendingUnpark;
    private boolean interruptOnWakeup;
    private Parked parked;

    volatile boolean trapInterrupts = true;
    // we need to avoid non-determinism when evaluating things in the debugger and toString() is the main culprit
    // so we re-write toString methods to update this counter, so that while we are evaluating a toString we do not
    // perform any non-deterministic actions
    private int determinismDepth;

    public InterceptibleThread(ThreadGroup group, Runnable target, String name, Object extraToStringInfo, Runnable onTermination, InterceptorOfGlobalMethods interceptorOfGlobalMethods, SimulatedTime.LocalTime time)
    {
        super(group, target, name);
        this.onTermination = onTermination;
        this.interceptorOfGlobalMethods = interceptorOfGlobalMethods;
        this.time = time;
        // group is nulled on termination, and we need it for reporting purposes, so save the toString
        this.toString = "Thread[" + name + ',' + getPriority() + ',' + group.getName() + ']';
        this.extraToStringInfo = extraToStringInfo;
    }

    public boolean park(long waitTime, WaitTimeKind waitTimeKind)
    {
        if (interceptor == null) return false;
        if (hasPendingUnpark) hasPendingUnpark = false;
        else if (!isInterrupted())
        {
            InterceptedWait.Kind kind;
            switch (waitTimeKind)
            {
                default:
                    throw new AssertionError();
                case NONE:
                    kind = UNBOUNDED_WAIT;
                    break;
                case RELATIVE_NANOS:
                    kind = WAIT_UNTIL;
                    waitTime = time.localToGlobal(time.relativeNanosToAbsolute(waitTime));
                    break;
                case ABSOLUTE_MILLIS:
                    kind = WAIT_UNTIL;
                    waitTime = time.translate().fromMillisSinceEpoch(waitTime);
            }
            Parked parked = this.parked = new Parked(kind, waitTime, interceptor);
            interceptWait(parked);
            parked.await();
        }
        return true;
    }

    public boolean unpark(InterceptibleThread by)
    {
        if (by.interceptor == null) return false;
        if (parked == null) hasPendingUnpark = true;
        else parked.interceptWakeIfNotAlready(by);
        return true;
    }

    public void trapInterrupts(boolean trapInterrupts)
    {
        this.trapInterrupts = trapInterrupts;
        if (interruptOnWakeup)
            doInterrupt();
    }

    public void doInterrupt()
    {
        super.interrupt();
    }

    @Override
    public void interrupt()
    {
        Thread by = Thread.currentThread();
        if (by == this || !(by instanceof InterceptibleThread) || !trapInterrupts) doInterrupt();
        else
        {
            interruptOnWakeup = true;
            if (parked == null) hasPendingUnpark = true;
            else parked.interceptWakeIfNotAlready((InterceptibleThread)by);
        }
    }

    @Override
    public void beforeInvocation(InterceptibleThread realThread)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void interceptMessage(IInvokableInstance from, IInvokableInstance to, IMessage message)
    {
        ++determinismDepth;
        try
        {
            if (interceptor == null) to.receiveMessage(message);
            else interceptor.interceptMessage(from, to, message);
            if (debug != null) debug.interceptMessage(from, to, message);
        }
        finally
        {
            --determinismDepth;
        }
    }

    @Override
    public void interceptWakeup(InterceptedWait wakeup, InterceptorOfConsequences waitWasInterceptedBy)
    {
        ++determinismDepth;
        try
        {
            interceptor.interceptWakeup(wakeup, waitWasInterceptedBy);
            if (debug != null) debug.interceptWakeup(wakeup, waitWasInterceptedBy);
        }
        finally
        {
            --determinismDepth;
        }
    }

    @Override
    public void interceptExecution(InterceptedExecution invoke, OrderOn orderOn)
    {
        ++determinismDepth;
        try
        {
            interceptor.interceptExecution(invoke, orderOn);
            if (debug != null) debug.interceptExecution(invoke, orderOn);
        }
        finally
        {
            --determinismDepth;
        }
    }

    @Override
    public void interceptWait(InterceptedWait wakeupWith)
    {
        ++determinismDepth;
        try
        {
            if (interceptor == null)
                return;

            InterceptorOfConsequences interceptor = this.interceptor;
            NotifyThreadPaused notifyOnPause = this.notifyOnPause;
            this.interceptor = null;
            this.notifyOnPause = null;

            interceptor.interceptWait(wakeupWith);
            if (debug != null) debug.interceptWait(wakeupWith);
            notifyOnPause.notifyThreadPaused();
        }
        finally
        {
            --determinismDepth;
        }
    }

    public void onTermination()
    {
        onTermination.run();
    }

    @Override
    public void interceptTermination(boolean isThreadTermination)
    {
        if (isThreadTermination)
            onTermination();

        ++determinismDepth;
        try
        {
            if (interceptor == null)
                return;

            InterceptorOfConsequences interceptor = this.interceptor;
            NotifyThreadPaused notifyOnPause = this.notifyOnPause;
            this.interceptor = null;
            this.notifyOnPause = null;

            interceptor.interceptTermination(isThreadTermination);
            if (debug != null) debug.interceptTermination(isThreadTermination);
            notifyOnPause.notifyThreadPaused();
        }
        finally
        {
            --determinismDepth;
        }
    }

    public void beforeInvocation(InterceptorOfConsequences interceptor, NotifyThreadPaused notifyOnPause)
    {
        assert this.interceptor == null;
        assert this.notifyOnPause == null;

        this.interceptor = interceptor;
        this.notifyOnPause = notifyOnPause;
        this.interceptor.beforeInvocation(this);
    }

    public boolean isEvaluationDeterministic()
    {
        return determinismDepth > 0;
    }

    public boolean isIntercepting()
    {
        return interceptor != null;
    }

    public InterceptorOfConsequences interceptedBy()
    {
        return interceptor;
    }

    public InterceptorOfGlobalMethods interceptorOfGlobalMethods()
    {
        return interceptorOfGlobalMethods;
    }

    public int hashCode()
    {
        return toString.hashCode();
    }

    public String toString()
    {
        return extraToStringInfo == null ? toString : toString + ' ' + extraToStringInfo;
    }

    public static boolean isDeterministic()
    {
        Thread thread = Thread.currentThread();
        return thread instanceof InterceptibleThread && ((InterceptibleThread) thread).determinismDepth > 0;
    }

    public static void runDeterministic(Runnable runnable)
    {
        enterDeterministicMethod();
        try
        {
            runnable.run();
        }
        finally
        {
            exitDeterministicMethod();
        }
    }
    
    public static void enterDeterministicMethod()
    {
        Thread anyThread = Thread.currentThread();
        if (!(anyThread instanceof InterceptibleThread))
            return;

        InterceptibleThread thread = (InterceptibleThread) anyThread;
        ++thread.determinismDepth;
    }

    public static void exitDeterministicMethod()
    {
        Thread anyThread = Thread.currentThread();
        if (!(anyThread instanceof InterceptibleThread))
            return;

        InterceptibleThread thread = (InterceptibleThread) anyThread;
        --thread.determinismDepth;
    }

    public static void park()
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(-1, NONE))
            LockSupport.park();
    }

    public static void parkNanos(long nanos)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(nanos, RELATIVE_NANOS))
            LockSupport.parkNanos(nanos);
    }

    public static void parkUntil(long millis)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(millis, ABSOLUTE_MILLIS))
            LockSupport.parkUntil(millis);
    }

    public static void park(Object blocker)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(-1, NONE))
            LockSupport.park(blocker);
    }

    public static void parkNanos(Object blocker, long relative)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(relative, RELATIVE_NANOS))
            LockSupport.parkNanos(blocker, relative);
    }

    public static void parkUntil(Object blocker, long millis)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(millis, ABSOLUTE_MILLIS))
            LockSupport.parkUntil(blocker, millis);
    }

    public static void unpark(Thread thread)
    {
        Thread currentThread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !(currentThread instanceof InterceptibleThread)
            || !((InterceptibleThread) thread).unpark((InterceptibleThread) currentThread))
            LockSupport.unpark(thread);
    }

    public static void setDebugInterceptor(InterceptorOfConsequences interceptor)
    {
        debug = interceptor;
    }
}
