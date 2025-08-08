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
import java.util.stream.Stream;

import accord.api.Agent;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.service.accord.AccordExecutorLoops.LoopTask;
import org.apache.cassandra.utils.concurrent.ConcurrentLinkedStack;

import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;

abstract class AccordExecutorAbstractLockLoop extends AccordExecutor
{
    final ConcurrentLinkedStack<Submittable> submitted = new ConcurrentLinkedStack<>();
    boolean isHeldByExecutor;
    boolean shutdown;

    AccordExecutorAbstractLockLoop(Lock lock, int executorId, AccordCacheMetrics metrics, Agent agent)
    {
        super(lock, executorId, metrics, agent);
    }

    abstract void notifyWork();
    abstract void notifyWorkExclusive();
    abstract void awaitExclusive() throws InterruptedException;
    abstract AccordExecutorLoops loops();
    abstract boolean isInLoop();
    abstract <P1s, P1a, P2, P3, P4> void submitExternal(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Submittable> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4);

    <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Submittable> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        // if we're a loop thread, we will poll the waitingToRun queue when we come around
        // NOTE: this assumes no synchronous blocking tasks are submitted to this executor
        if (isInLoop() || isOwningThread()) submitted.push(async.apply(p1a, p2, p3, p4));
        else submitExternal(sync, async, p1s, p1a, p2, p3, p4);
    }

    <P1s, P1a, P2, P3, P4> void submitExternalExclusive(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Submittable> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        try
        {
            try
            {
                drainSubmittedExclusive();
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

    public boolean hasTasks()
    {
        if (tasks > 0 || !submitted.isEmpty() || runningThreads > 0)
            return true;

        lock.lock();
        try
        {
            return tasks > 0 || !submitted.isEmpty() || runningThreads > 0;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    void beforeUnlock()
    {
        if (!isInLoop())
            notifyIfMoreWorkExclusive();
    }

    void updateWaitingToRunExclusive()
    {
        drainSubmittedExclusive();
        super.updateWaitingToRunExclusive();
    }

    void drainSubmittedExclusive()
    {
        submitted.drain(AccordExecutor::consumeExclusive, this, true);
    }

    void notifyIfMoreWorkExclusive()
    {
        if (hasWaitingToRun())
            notifyWorkExclusive();
    }

    private void enterLockExclusive()
    {
        isHeldByExecutor = true;
    }

    private void exitLockExclusive()
    {
        notifyIfMoreWorkExclusive();
    }

    private void pauseExclusive()
    {
        isHeldByExecutor = false;
        if (--runningThreads == 0 && tasks == 0)
            notifyQuiescentExclusive();
    }

    private void resumeExclusive()
    {
        isHeldByExecutor = true;
        ++runningThreads;
    }

    LoopTask task(String name, Mode mode)
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
                Task task;
                while (true)
                {
                    lock.lock();
                    try
                    {
                        resumeExclusive();
                        enterLockExclusive();
                        while (true)
                        {
                            task = pollWaitingToRunExclusive();

                            if (task != null)
                            {
                                setRunning(task);
                                try
                                {
                                    task.preRunExclusive();
                                    task.runInternal();
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
                                    pauseExclusive();
                                    exitLockExclusive();
                                    notifyWorkExclusive(); // always notify on shutdown
                                    return;
                                }

                                pauseExclusive();
                                awaitExclusive();
                                resumeExclusive();
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        pauseExclusive();
                        exitLockExclusive();

                        try { agent.onUncaughtException(t); }
                        catch (Throwable t2) { }
                    }
                    finally
                    {
                        lock.unlock();
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
                Task task = null;
                while (true)
                {
                    lock.lock();
                    try
                    {
                        if (task != null)
                        {
                            Task tmp = task;
                            task = null;
                            completeTaskExclusive(tmp);
                            clearRunning();
                        }
                        else resumeExclusive();
                        enterLockExclusive();

                        while (true)
                        {
                            task = pollWaitingToRunExclusive();
                            if (task != null)
                            {
                                setRunning(task);
                                task.preRunExclusive();
                                exitLockExclusive();
                                break;
                            }

                            pauseExclusive();

                            if (shutdown)
                            {
                                exitLockExclusive();
                                notifyWorkExclusive();
                                return;
                            }

                            awaitExclusive();
                            resumeExclusive();
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
                            try { agent.onUncaughtException(t); }
                            catch (Throwable t2) { /* nothing we can sensibly do after already reporting */ }
                            task = null;
                        }
                        else
                        {
                            try { agent.onUncaughtException(t); }
                            catch (Throwable t2) { /* nothing we can sensibly do after already reporting */ }
                        }
                        if (isHeldByExecutor)
                            pauseExclusive();
                        exitLockExclusive();
                        continue;
                    }
                    finally
                    {
                        lock.unlock();
                    }

                    try
                    {
                        task.runInternal();
                    }
                    catch (Throwable t)
                    {
                        try { task.fail(t); }
                        catch (Throwable t2)
                        {
                            try
                            {
                                t2.addSuppressed(t);
                                agent.onUncaughtException(t2);
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
    public Stream<? extends DebuggableTaskRunner> active()
    {
        return loops().active();
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
        notifyWork();
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }
}
