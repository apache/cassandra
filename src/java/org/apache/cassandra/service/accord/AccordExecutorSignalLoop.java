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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.utils.Invariants;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;

import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.utils.concurrent.SignalLock;

import static accord.utils.Invariants.nonNull;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.UNINITIALIZED;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;

public class AccordExecutorSignalLoop extends AccordExecutorAbstractLoop
{
    private static class ShutdownException extends RuntimeException {}

    private static final int MAX_LOOPS = 1000; // limit the amount of time we hold the lock for
    private final SignalLock lock;
    private final AccordExecutorLoops loops;
    private int readyToRunTarget = 1;
    private final int readyToRunLimit;
    // TODO (desired): intrusive queue using Task.next, but a little challenging because we reuse SequentialQueueTask so have ABA problem
    private final ConcurrentLinkedQueue<Task> readyToRun = new ConcurrentLinkedQueue<>();

    private Task pendingExecutedHead, pendingExecutedTail;
    private Task pendingNewHead, pendingNewTail;
    private int pendingCount;

    private boolean shutdown;

    public AccordExecutorSignalLoop(int executorId, Mode mode, int threads, long spinInterval, long stopCheckInterval, TimeUnit units, IntFunction<String> name, Agent agent)
    {
        this(new SignalLock(threads, spinInterval, stopCheckInterval, units), executorId, mode, threads, name, agent);
    }

    public AccordExecutorSignalLoop(SignalLock lock, int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent)
    {
        super(lock, executorId, agent);
        Invariants.require(threads <= SignalLock.MAX_THREADS);
        this.lock = lock;
        this.loops = new AccordExecutorLoops(mode, threads, name, this::task);
        this.readyToRunLimit = Math.min(threads * 4, SignalLock.MAX_SIGNAL_COUNT);
    }

    <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Task> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        Task next = async.apply(p1a, p2, p3, p4);
        Task prev = push(next);
        if (prev == null)
            lock.signalLockWork();
    }

    @Override
    final void beforeUnlockExternal()
    {
    }

    private Task pollReadyToRun()
    {
        return readyToRun.poll();
    }

    private void addReadyToRun(Task task)
    {
        readyToRun.add(task);
    }

    private boolean hasReadyToRun()
    {
        return !readyToRun.isEmpty();
    }

    private LoopTask task(int index, String id, Mode mode)
    {
        return new LoopTask(index, id);
    }

    @Override
    final void drainUnqueuedNewWorkExclusive()
    {
        updatePendingUnqueued();
        Task requeue = null, requeueTail = null;
        Task submit = null, submitTail = null;
        Task cur = pendingNewHead;
        while (cur != null)
        {
            Task next = cur.next;
            cur.next = null;
            if (cur.isNewWork())
            {
                if (submit == null) submit = cur;
                else submitTail.next = cur;
                submitTail = cur;
                --pendingCount;
            }
            else
            {
                if (requeue == null) requeue = cur;
                else requeueTail.next = cur;
                requeueTail = cur;
            }
            cur = next;
        }

        pendingNewHead = requeue;
        pendingNewTail = requeueTail;
        while (submit != null)
        {
            Task next = submit.next;
            submit.next = null;
            submit.submitExclusive(this);
            submit = next;
        }
    }

    private boolean enqueueOnePending()
    {
        if (pendingCount == 0)
            return false;

        --pendingCount;
        if (pendingExecutedHead != null)
        {
            Task executed = pendingExecutedHead;
            pendingExecutedHead = destructiveNext(executed);
            if (pendingExecutedHead == null)
                pendingExecutedTail = null;
            cleanupTaskExclusive(executed, true);
        }
        else
        {
            Task submit = pendingNewHead;
            pendingNewHead = destructiveNext(submit);
            if (pendingNewHead == null)
                pendingNewTail = null;
            submit.submitExclusive(this);
        }
        return true;
    }

    private void fetchWorkExclusive()
    {
        boolean hasReadyToRun = hasReadyToRun();
        boolean hadWaitingToRun = hasAlreadyWaitingToRun();
        {
            int prevPendingUnqueued = pendingCount;
            updateAndEnqueuePendingUntilHasWaitingToRun(prevPendingUnqueued / 2);
        }

        if (hadWaitingToRun && hasReadyToRun)
        {
            lock.addAndGetEnabledThreadCount(1);
        }
        else if (hadWaitingToRun)
        {
            readyToRunTarget = Math.min(readyToRunLimit, readyToRunTarget + (1+readyToRunTarget)/2);
        }
        else if (hasReadyToRun && readyToRunTarget > 1)
        {
            --readyToRunTarget;
        }

        boolean hasDrainedSignal = false;
        int loops = 0;
        while (true)
        {
            long state = lock.state();
            int signals = SignalLock.asyncSignalCount(state);
            int waiters = SignalLock.waitingEnabledThreadCount(state);
            if (signals > 0)
            {
                if (++loops > MAX_LOOPS)
                    return;

                if (signals >= readyToRunTarget)
                {
                    if (enqueueOnePending() || (updatePendingUnqueued() && enqueueOnePending())) continue;
                    else if (hasDrainedSignal)
                        lock.signalLockWorkExclusive();
                    return;
                }
                else if (waiters > 0 && signals > 1 && SignalLock.activeEnabledThreadCount(state) == 1)
                {
                    // ensure at least one other thread is running if there's enough work for it; it will spin up other threads if necessary
                    lock.propagateAsyncWorkSignals(1);
                }
            }

            Task task = pollAlreadyWaitingToRunExclusive();
            if (task == null)
            {
                if (updateAndEnqueuePendingUntilHasWaitingToRun(0))
                    continue;

                lock.clearLockWork();
                hasDrainedSignal = true;
                if (!updateAndEnqueuePendingUntilHasWaitingToRun(0))
                {
                    if (tasks == 0)
                        notifyQuiescentExclusive();
                    return;
                }
            }
            else
            {
                try { task.preRunExclusive(); }
                catch (Throwable t)
                {
                    try { task.failExclusive(t, Task.State.FAILED_OTHER); }
                    catch (Throwable t2) { try { t.addSuppressed(t2); } catch (Throwable t3) {} }
                    try { cleanupTaskExclusive(task, false); }
                    catch (Throwable t2) { try { t.addSuppressed(t2); } catch (Throwable t3) {} }
                    continue;
                }
                if (DEBUG_EXECUTION) DebugTask.get(task).onPreRun();

                addReadyToRun(task);
                boolean incremented = lock.incrementAsyncWork(false);
                Invariants.require(incremented);
            }
        }
    }

    private boolean updatePendingUnqueued()
    {
        if (!hasUnqueued())
            return false;

        int count = 0;
        Task addExecutedHead = null, addExecutedTail = null;
        Task addNewHead = null, addNewTail = null;
        {
            Task cur = Task.reverse(acquireUnqueuedExclusive());
            while (cur != null)
            {
                Task next = cur.next;
                if (cur.is(UNINITIALIZED))
                {
                    if (addNewHead == null) addNewHead = addNewTail = setNextNull(cur);
                    else addNewHead = reverseOne(addNewHead, cur);
                }
                else
                {
                    if (addExecutedHead == null) addExecutedHead = addExecutedTail = setNextNull(cur);
                    else addExecutedHead = reverseOne(addExecutedHead, cur);
                }
                ++count;
                cur = next;
            }
        }

        pendingCount += count;
        if (addExecutedHead != null)
        {
            if (pendingExecutedHead == null) pendingExecutedHead = addExecutedHead;
            else pendingExecutedTail.next = addExecutedHead;
            pendingExecutedTail = addExecutedTail;
        }
        if (addNewHead != null)
        {
            if (pendingNewHead == null) pendingNewHead = addNewHead;
            else pendingNewTail.next = addNewHead;
            pendingNewTail = addNewTail;
        }
        return true;
    }

    private Task reverseOne(Task prev, Task cur)
    {
        cur.next = prev;
        return cur;
    }

    private Task setNextNull(Task cur)
    {
        cur.next = null;
        return cur;
    }

    private boolean updateAndEnqueuePendingUntilHasWaitingToRun(int processAtLeast)
    {
        updatePendingUnqueued();
        int count = 0;
        while (enqueueOnePending())
        {
            if (++count >= processAtLeast && hasAlreadyWaitingToRun())
                return true;
        }
        return hasAlreadyWaitingToRun();
    }

    class LoopTask extends AccordExecutorLoops.LoopTask
    {
        final int index;

        LoopTask(int index, String id)
        {
            super(id);
            this.index = index;
        }

        private Task awaitWork(AccordTaskRunner self)
        {
            while (true)
            {
                if (lock.awaitAsyncOrLock(index))
                {
                    try
                    {
                        if (DEBUG_EXECUTION) debug.onEnterLock();
                        fetchWorkExclusive();
                    }
                    catch (Throwable t)
                    {
                        unlock(self);
                        throw t;
                    }

                    if (!unlockAndAcquire(self))
                        continue;
                }

                if (shutdown)
                    throw new ShutdownException();

                return nonNull(pollReadyToRun());
            }
        }

        private Task executedAndMaybeGetWork(AccordTaskRunner self, @Nullable Task executed)
        {
            if (lock.tryAcquireAsyncWork())
            {
                if (shutdown)
                    throw new ShutdownException();

                return pushExecutedAndReturn(executed, nonNull(pollReadyToRun()));
            }

            if (!tryLock(self))
                return pushExecutedAndReturn(executed, null);

            try
            {
                cleanupTaskExclusive(executed, true);
                fetchWorkExclusive();
            }
            catch (Throwable t)
            {
                unlock(self);
                throw t;
            }

            if (unlockAndAcquire(self))
            {
                if (shutdown)
                    throw new ShutdownException();

                return nonNull(pollReadyToRun());
            }
            return null;
        }

        private Task pushExecutedAndReturn(Task complete, Task result)
        {
            if (push(complete) == null)
                lock.signalLockWork();
            return result;
        }

        final boolean tryLock(AccordTaskRunner self)
        {
            if (self.accordLockedExecutor() != null)
                return false;

            return onTryLock(self, lock.tryLock(index));
        }

        final boolean unlockAndAcquire(AccordTaskRunner self)
        {
            self.exitAccordLockedExecutor();
            if (DEBUG_EXECUTION) debug.onExitLock();
            return lock.unlockAndAcquireAsyncWork();
        }

        @Override
        public void run()
        {
            Thread thread = Thread.currentThread();
            AccordTaskRunner self = AccordTaskRunner.get(thread);
            self.setAccordActiveExecutor(AccordExecutorSignalLoop.this);
            setWrapped(self);

            lock.register(index, thread);
            Task task = null;
            while (true)
            {
                try
                {
                    if (task != null)
                    {
                        try { task = executedAndMaybeGetWork(self, task); }
                        catch (Throwable t) { task = null; throw t; }
                    }
                    if (task == null)
                        task = awaitWork(self);

                    try
                    {
                        self.setAccordActiveTask(task);
                        task.run();
                    }
                    catch (Throwable t)
                    {
                        try { task.failExecution(t); }
                        catch (Throwable t2)
                        {
                            try
                            {
                                t2.addSuppressed(t);
                                agent.onException(t2);
                            }
                            catch (Throwable t3) { /* empty to ensure we definitely loop so we cleanup the task */ }
                        }
                    }
                }
                catch (ShutdownException ignore)
                {
                    break;
                }
                catch (Throwable t)
                {
                    agent.onException(t);
                }
                finally
                {
                    self.setAccordActiveTask(null);
                }
            }
        }
    }

    @Override
    public void shutdown()
    {
        lock.lock();
        try
        {
            shutdown = true;
            lock.signalAllRegistered();
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    AccordExecutorLoops loops()
    {
        return loops;
    }

    @Override
    boolean isInLoop()
    {
        return loops.isInLoop();
    }

    @Override
    boolean isOwningThread()
    {
        return lock.isOwner();
    }

    @Override
    public boolean isTerminated()
    {
        return loops.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return loops.awaitTermination(timeout, unit);
    }
}
