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

package org.apache.cassandra.service.accord;

import java.util.concurrent.locks.Lock;

import accord.api.Agent;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;

import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.service.accord.AccordExecutorLoops.LoopTask;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExecutorLoop;

import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;

abstract class AccordExecutorAbstractLockLoop extends AccordExecutorAbstractLoop
{
    int runningThreads;
    boolean shutdown;

    AccordExecutorAbstractLockLoop(Lock lock, int executorId, Agent agent)
    {
        super(lock, executorId, agent);
    }

    abstract void notifyWork();
    abstract void notifyWorkExclusive();
    abstract void awaitExclusive() throws InterruptedException;
    abstract <P1s, P1a, P2, P3, P4> void submitExternal(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Task> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4);

    final <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Task> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        // if we're a loop thread, we will poll the waitingToRun queue when we come around
        // NOTE: this assumes no synchronous blocking tasks are submitted to this executor
        if (isInLoop() || isOwningThread()) push(async.apply(p1a, p2, p3, p4));
        else submitExternal(sync, async, p1s, p1a, p2, p3, p4);
    }

    final <P1s, P2, P3, P4> void submitExternalExclusive(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, P1s p1s, P2 p2, P3 p3, P4 p4)
    {
        try
        {
            try
            {
                drainUnqueuedExclusive();
            }
            catch (Throwable t)
            {
                try { sync.accept(this, p1s, p2, p3, p4); }
                catch (Throwable t2) { t.addSuppressed(t2); }
                throw t;
            }
            sync.accept(this, p1s, p2, p3, p4);
        }
        finally
        {
            notifyIfMoreWorkExclusive();
        }
    }

    final void notifyIfMoreWorkExclusive()
    {
        if (hasWaitingToRun())
            notifyWorkExclusive();
    }

    final boolean hasWaitingToRun()
    {
        updateWaitingToRunExclusive();
        return hasAlreadyWaitingToRun();
    }

    final Task pollWaitingToRunExclusive()
    {
        updateWaitingToRunExclusive();
        return pollAlreadyWaitingToRunExclusive();
    }

    final void updateWaitingToRunExclusive()
    {
        drainUnqueuedExclusive();
        super.updateWaitingToRunExclusive();
    }

    final void drainUnqueuedExclusive()
    {
        Task cur = Task.reverse(acquireUnqueuedExclusive());
        while (cur != null)
            cur = enqueueOneExclusive(cur);
    }

    final void drainUnqueuedNewWorkExclusive()
    {
        Task cur = acquireUnqueuedExclusive();
        Task prev = null, requeue = null, requeueLast = null;
        while (cur != null)
        {
            Task next = cur.next;
            if (cur.isNewWork())
            {
                cur.next = prev;
                prev = cur;
            }
            else
            {
                if (requeue == null) requeue = cur;
                else requeueLast.next = cur;
                requeueLast = cur;
            }
            cur = next;
        }

        if (requeue != null)
        {
            while (true)
            {
                Task next = unqueued;
                requeueLast.next = next;
                if (unqueuedUpdater.compareAndSet(this, next, requeue))
                    break;
            }
        }

        cur = prev;
        while (cur != null)
        {
            Task next = cur.next;
            cur.submitExclusive(this);
            cur.next = null;
            cur = next;
        }
    }

    @Override
    final void beforeUnlockExternal()
    {
        beforeUnlockLoop();
    }

    final void beforeUnlockLoop()
    {
        notifyIfMoreWorkExclusive();
    }

    private void enterLockLoop()
    {
        resumeLoop();
    }

    private void exitLockLoop()
    {
        pauseLoop();
        notifyIfMoreWorkExclusive();
    }

    final void pauseLoop()
    {
        if (--runningThreads == 0 && tasks == 0)
            notifyQuiescentExclusive();
    }

    final void resumeLoop()
    {
        if (DEBUG_EXECUTION) debug.onEnterLock();
        ++runningThreads;
    }

    LoopTask task(int index, String name, Mode mode)
    {
        return mode == RUN_WITH_LOCK ? runWithLock(name) : runWithoutLock(name);
    }

    protected LoopTask runWithLock(String name)
    {
        return new LoopTask(name)
        {
            @Override
            public void run()
            {
                Thread thread = Thread.currentThread();
                AccordTaskRunner self = AccordTaskRunner.get(thread);
                self.setAccordActiveExecutor(AccordExecutorAbstractLockLoop.this);
                setWrapped(self);

                Task task;
                while (true)
                {
                    lock(self);
                    try
                    {
                        enterLockLoop();
                        while (true)
                        {
                            task = pollWaitingToRunExclusive();

                            if (task != null)
                            {
                                self.setAccordActiveTask(task);
                                try
                                {
                                    task.preRunExclusive();
                                    task.run();
                                }
                                catch (Throwable t)
                                {
                                    task.fail(t);
                                }
                                finally
                                {
                                    completeTaskExclusive(task);
                                    self.setAccordActiveTask(null);
                                }
                            }
                            else
                            {
                                if (shutdown)
                                {
                                    pauseLoop();
                                    exitLockLoop();
                                    notifyWorkExclusive(); // always notify on shutdown
                                    return;
                                }

                                pauseLoop();
                                awaitExclusive();
                                resumeLoop();
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        exitLockLoop();

                        try { agent.onException(t); }
                        catch (Throwable t2) { }
                    }
                    finally
                    {
                        unlock(self);
                    }
                }
            }
        };
    }

    protected LoopTask runWithoutLock(String name)
    {
        return new LoopTask(name)
        {
            final DebugExecutorLoop debug = DEBUG_EXECUTION ? new DebugExecutorLoop(AccordExecutorAbstractLockLoop.this.debug) : null;
            @Override
            public void run()
            {
                CassandraThread self = (CassandraThread) Thread.currentThread();
                self.setAccordActiveExecutor(AccordExecutorAbstractLockLoop.this);
                setWrapped(self);

                Task task = null;
                while (true)
                {
                    if (DEBUG_EXECUTION) debug.onLock();
                    lock(self);
                    try
                    {
                        if (DEBUG_EXECUTION) debug.onEnterLock();
                        enterLockLoop();
                        if (task != null)
                        {
                            Task tmp = task;
                            task = null;
                            completeTaskExclusive(tmp);
                            self.setAccordActiveTask(null);
                        }

                        while (true)
                        {
                            task = pollWaitingToRunExclusive();

                            if (task != null)
                            {
                                self.setAccordActiveTask(task);
                                task.preRunExclusive();
                                if (DEBUG_EXECUTION) debug.onExitLock();
                                exitLockLoop();
                                break;
                            }

                            if (shutdown)
                            {
                                if (DEBUG_EXECUTION) debug.onExitLock();
                                exitLockLoop();
                                notifyWorkExclusive();
                                return;
                            }

                            pauseLoop();
                            if (DEBUG_EXECUTION) debug.onExitLock();
                            awaitExclusive();
                            if (DEBUG_EXECUTION) debug.onEnterLock();
                            resumeLoop();
                        }
                    }
                    catch (Throwable t)
                    {
                        if (task != null)
                        {
                            try { task.fail(t); }
                            catch (Throwable t2) { t.addSuppressed(t2); }
                            try { completeTaskExclusive(task); }
                            catch (Throwable t2) { t.addSuppressed(t2); }
                            try { agent.onException(t); }
                            catch (Throwable t2) { /* nothing we can sensibly do after already reporting */ }
                            task = null;
                        }
                        else
                        {
                            try { agent.onException(t); }
                            catch (Throwable t2) { /* nothing we can sensibly do after already reporting */ }
                        }
                        exitLockLoop();
                        continue;
                    }
                    finally
                    {
                        if (DEBUG_EXECUTION) debug.onExitLock();
                        unlock(self);
                    }

                    try
                    {
                        task.run();
                    }
                    catch (Throwable t)
                    {
                        try { task.fail(t); }
                        catch (Throwable t2)
                        {
                            try
                            {
                                t2.addSuppressed(t);
                                agent.onException(t2);
                            }
                            catch (Throwable t3)
                            {
                                // empty to ensure we definitely loop so we cleanup the task
                            }
                        }
                    }
                }
            }
        };
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
        notifyWork();
    }
}
