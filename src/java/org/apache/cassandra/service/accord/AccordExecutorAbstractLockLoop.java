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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.AccordExecutorLoops.LoopTask;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExecutorLoop;

import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;

abstract class AccordExecutorAbstractLockLoop extends AccordExecutorAbstractLoop
{
    private static final int YIELD_INTERVAL = DatabaseDescriptor.getAccord().queue_yield_interval;
    int runningThreads;
    boolean shutdown;

    AccordExecutorAbstractLockLoop(Lock lock, int executorId, Agent agent)
    {
        super(lock, executorId, agent);
    }

    abstract void notifyWork();
    abstract void notifyWorkExclusive();
    void loopYieldExclusive() throws InterruptedException {}
    abstract void awaitExclusive() throws InterruptedException;
    abstract <P1s, P1a, P2, P3, P4> void submitExternal(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Task> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4);

    <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Task> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        // if we're a loop thread, we will poll the waitingToRun queue when we come around
        // NOTE: this assumes no synchronous blocking tasks are submitted to this executor
        if (isInLoop() || isOwningThread()) push(async.apply(p1a, p2, p3, p4));
        else submitExternal(sync, async, p1s, p1a, p2, p3, p4);
    }

    <P1s, P2, P3, P4> void submitExternalExclusive(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, P1s p1s, P2 p2, P3 p3, P4 p4)
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
                Thread self = Thread.currentThread();
                Task task;
                while (true)
                {
                    lock();
                    try
                    {
                        enterLockLoop();
                        while (true)
                        {
                            task = pollWaitingToRunExclusive();

                            if (task != null)
                            {
                                setRunning(task);
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
                                    clearRunning();
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
                        unlock();
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
                int count = 0;
                Task task = null;
                while (true)
                {
                    if (DEBUG_EXECUTION) debug.onLock();
                    lock();
                    try
                    {
                        if (DEBUG_EXECUTION) debug.onEnterLock();
                        enterLockLoop();
                        if (task != null)
                        {
                            Task tmp = task;
                            task = null;
                            completeTaskExclusive(tmp);
                            clearRunning();
                        }

                        if (count >= YIELD_INTERVAL)
                        {
                            loopYieldExclusive();
                            count = 0;
                        }

                        while (true)
                        {
                            task = pollWaitingToRunExclusive();

                            if (task != null)
                            {
                                setRunning(task);
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
                            count = 0;
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
                        unlock();
                    }

                    try
                    {
                        ++count;
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
