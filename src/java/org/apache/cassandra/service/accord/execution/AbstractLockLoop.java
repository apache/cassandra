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

package org.apache.cassandra.service.accord.execution;

import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;

import accord.api.Agent;

import org.apache.cassandra.service.accord.execution.Loops.LoopTask;

import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITH_LOCK;

abstract class AbstractLockLoop extends AbstractLoop
{
    int runningThreads;
    boolean shutdown;

    AbstractLockLoop(Lock lock, int executorId, Agent agent)
    {
        super(lock, executorId, agent);
    }

    abstract void notifyWork();
    abstract void notifyWorkExclusive();
    abstract void awaitExclusive() throws InterruptedException;
    abstract <P1> void submitExternal(Consumer<P1> sync, Function<P1, Task> async, P1 p1);

    final <P1> void submit(Consumer<P1> sync, Function<P1, Task> async, P1 p1)
    {
        // if we're a loop thread, we will poll the waitingToRun queue when we come around
        // NOTE: this assumes no synchronous blocking tasks are submitted to this executor
        if (isInLoop() || isOwningThread()) push(async.apply(p1));
        else submitExternal(sync, async, p1);
    }

    final <P1> void submitExternalExclusive(Consumer<P1> sync, Function<P1, Task> async, P1 p1)
    {
        try
        {
            try
            {
                drainUnqueuedExclusive();
            }
            catch (Throwable t)
            {
                try { sync.accept(p1); }
                catch (Throwable t2) { t.addSuppressed(t2); }
                throw t;
            }
            sync.accept(p1);
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
            cur.next = null;
            cur.submitExclusiveNoExcept();
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
        // paired with resumeLoop's onEnterLock: the loop releases the lock while it waits, so the hold periods either
        // side of the wait must be reported separately, and the depth must return to where it started
        if (DEBUG_EXECUTION) debug.onExitLock();
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
                TaskRunner self = TaskRunner.get(thread);
                self.setAccordActiveExecutor(AbstractLockLoop.this);
                setWrapped(self);

                Task task;
                while (true)
                {
                    lock(self);
                    boolean running = false;
                    try
                    {
                        enterLockLoop();
                        running = true;
                        while (true)
                        {
                            task = pollWaitingToRunExclusive();
                            if (task != null) task.prepareRunAndCompleteExclusive(self);
                            else
                            {
                                if (shutdown)
                                {
                                    running = false;
                                    exitLockLoop();
                                    notifyWorkExclusive(); // always notify on shutdown
                                    return;
                                }

                                running = false;
                                pauseLoop();
                                awaitExclusive();
                                resumeLoop();
                                running = true;
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        if (running)
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
            @Override
            public void run()
            {
                Thread thread = Thread.currentThread();
                TaskRunner self = TaskRunner.get(thread);
                self.setAccordActiveExecutor(AbstractLockLoop.this);
                setWrapped(self);

                Task task = null;
                while (true)
                {
                    lock(self);
                    boolean running = false;
                    try
                    {
                        enterLockLoop();
                        running = true;
                        if (task != null)
                        {
                            Task tmp = task;
                            task = null;
                            tmp.completeExclusiveNoExcept();
                        }

                        while (true)
                        {
                            task = pollWaitingToRunExclusive();
                            if (task != null)
                            {
                                if (!task.prepareExclusiveNoExcept())
                                {
                                    task = null;
                                    continue;
                                }

                                running = false;
                                exitLockLoop();
                                break;
                            }

                            if (shutdown)
                            {
                                running = false;
                                exitLockLoop();
                                notifyWorkExclusive();
                                return;
                            }

                            running = false;
                            pauseLoop();
                            awaitExclusive();
                            resumeLoop();
                            running = true;
                        }
                    }
                    catch (Throwable t)
                    {
                        try { agent.onException(t); }
                        catch (Throwable t2) { }
                        if (running)
                            exitLockLoop();
                        continue;
                    }
                    finally
                    {
                        unlock(self);
                    }

                    task.runNoExcept(self);
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
