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
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.utils.concurrent.ConcurrentLinkedStack;

import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;

abstract class AccordExecutorAbstractLockLoop extends AccordExecutor
{
    final ConcurrentLinkedStack<Object> submitted = new ConcurrentLinkedStack<>();
    boolean isHeldByExecutor;

    AccordExecutorAbstractLockLoop(Lock lock, int executorId, AccordCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        super(lock, executorId, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    abstract void notifyWorkExclusive();
    abstract void awaitExclusive() throws InterruptedException;
    abstract boolean isInLoop();
    abstract <P1s, P1a, P2, P3, P4> void submitExternal(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4);

    <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        // if we're a loop thread, we will poll the waitingToRun queue when we come around
        if (isInLoop()) submitted.push(async.apply(p1a, p2, p3, p4));
        else submitExternal(sync, async, p1s, p1a, p2, p3, p4);
    }

    <P1s, P1a, P2, P3, P4> void submitExternalExclusive(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
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
        if (tasks > 0 || !submitted.isEmpty() || running > 0)
            return true;

        lock.lock();
        try
        {
            return tasks > 0 || !submitted.isEmpty() || running > 0;
        }
        finally
        {
            lock.unlock();
        }
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
        isHeldByExecutor = false;
        notifyIfMoreWorkExclusive();
    }

    private void pauseExclusive()
    {
        --running;
    }

    private void resumeExclusive()
    {
        ++running;
    }

    Interruptible.Task task(Mode mode)
    {
        return mode == RUN_WITH_LOCK ? this::runWithLock : this::runWithoutLock;
    }

    protected void runWithLock(Interruptible.State state) throws InterruptedException
    {
        lock.lockInterruptibly();
        try
        {
            resumeExclusive();
            enterLockExclusive();
            while (true)
            {
                Task task = pollWaitingToRunExclusive();

                if (task != null)
                {
                    --tasks;
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
                        task.cleanupExclusive();
                    }
                }
                else
                {
                    if (state != NORMAL)
                    {
                        pauseExclusive();
                        exitLockExclusive();
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
            throw t;
        }
        finally
        {
            lock.unlock();
        }
    }

    protected void runWithoutLock(Interruptible.State state) throws InterruptedException
    {
        Task task = null;
        while (true)
        {
            lock.lock();
            try
            {
                if (task != null) task.cleanupExclusive();
                else resumeExclusive();
                enterLockExclusive();

                while (true)
                {
                    task = pollWaitingToRunExclusive();
                    if (task != null)
                    {
                        exitLockExclusive();
                        break;
                    }

                    if (state != NORMAL)
                    {
                        exitLockExclusive();
                        return;
                    }

                    pauseExclusive();
                    awaitExclusive();
                    resumeExclusive();
                }
                --tasks;
                task.preRunExclusive();
            }
            catch (Throwable t)
            {
                if (task != null)
                {
                    try { task.fail(t); }
                    catch (Throwable t2) { t.addSuppressed(t2); }
                    try { task.cleanupExclusive(); }
                    catch (Throwable t2) { t.addSuppressed(t2); }
                    try { agent.onUncaughtException(t); }
                    catch (Throwable t2) { /* nothing we can sensibly do after already reporting */ }
                }
                pauseExclusive();
                exitLockExclusive();
                throw t;
            }
            finally
            {
                lock.unlock();
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
}
