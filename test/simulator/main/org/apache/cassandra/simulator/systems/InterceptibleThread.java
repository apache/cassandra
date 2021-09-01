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
import org.apache.cassandra.simulator.systems.InterceptedWait.Kind;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.utils.Shared.Recursive.ALL;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION, ancestors = ALL, members = ALL)
public class InterceptibleThread extends FastThreadLocalThread implements InterceptorOfConsequences
{
    // used to implement LockSupport.park/unpark
    @Shared(scope = SIMULATION)
    private class Parked implements InterceptedWait
    {
        final Kind kind;
        final InterceptorOfConsequences waitInterceptedBy;
        final List<TriggerListener> onTrigger = new ArrayList<>(3);
        boolean isWakeIntercepted; // we have intercepted a pending wakeup
        boolean isDone; // we have been signalled (by the simulation or otherwise)

        Parked(Kind kind, InterceptorOfConsequences waitInterceptedBy)
        {
            this.kind = kind;
            this.waitInterceptedBy = waitInterceptedBy;
        }

        @Override
        public Kind kind()
        {
            return kind;
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
    }

    private static InterceptorOfConsequences debug;

    final Object extraToStringInfo; // optional dynamic extra information for reporting with toString
    final String toString;

    // this is set before the thread's execution begins/continues; events and cessation are reported back to this
    private InterceptorOfConsequences interceptor;
    private NotifyThreadPaused notifyOnPause;

    boolean hasPendingUnpark;
    boolean interruptOnWakeup;
    Parked parked;

    volatile boolean trapInterrupts = true;

    public InterceptibleThread(ThreadGroup group, Runnable target, String name, Object extraToStringInfo)
    {
        super(group, target, name);
        // group is nulled on termination, and we need it for reporting purposes, so save the toString
        this.toString = "Thread[" + name + ',' + getPriority() + ',' + group.getName() + ']';
        this.extraToStringInfo = extraToStringInfo;
    }

    boolean park(Kind kind)
    {
        if (interceptor == null) return false;
        if (hasPendingUnpark) hasPendingUnpark = false;
        else if (!isInterrupted())
        {
            parked = new Parked(kind, interceptor);
            interceptor.interceptWait(parked);
            parked.await();
        }
        return true;
    }

    boolean unpark(InterceptibleThread by)
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
        if (interceptor == null) to.receiveMessage(message);
        else interceptor.interceptMessage(from, to, message);
        if (debug != null) debug.interceptMessage(from, to, message);
    }

    @Override
    public void interceptWakeup(InterceptedWait wakeup, InterceptorOfConsequences waitWasInterceptedBy)
    {
        interceptor.interceptWakeup(wakeup, waitWasInterceptedBy);
        if (debug != null) debug.interceptWakeup(wakeup, waitWasInterceptedBy);
    }

    @Override
    public void interceptExecution(InterceptedExecution invoke, InterceptingExecutor executor)
    {
        interceptor.interceptExecution(invoke, executor);
        if (debug != null) debug.interceptExecution(invoke, executor);
    }

    @Override
    public void interceptWait(InterceptedWait wakeupWith)
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

    @Override
    public void interceptTermination()
    {
        if (interceptor == null)
            return;

        InterceptorOfConsequences interceptor = this.interceptor;
        NotifyThreadPaused notifyOnPause = this.notifyOnPause;
        this.interceptor = null;
        this.notifyOnPause = null;

        interceptor.interceptTermination();
        if (debug != null) debug.interceptTermination();
        notifyOnPause.notifyThreadPaused();
    }

    public void beforeInvocation(InterceptorOfConsequences interceptor, NotifyThreadPaused notifyOnPause)
    {
        assert this.interceptor == null;
        assert this.notifyOnPause == null;

        this.interceptor = interceptor;
        this.notifyOnPause = notifyOnPause;
        this.interceptor.beforeInvocation(this);
    }

    public boolean isIntercepting()
    {
        return interceptor != null;
    }

    public InterceptorOfConsequences interceptedBy()
    {
        return interceptor;
    }

    public String toString()
    {
        return extraToStringInfo == null ? toString : toString + ' ' + extraToStringInfo;
    }

    public static void setDebugInterceptor(InterceptorOfConsequences interceptor)
    {
        debug = interceptor;
    }
}
