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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import javax.annotation.Nonnull;

import accord.api.Agent;
import accord.utils.Invariants;

import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.utils.concurrent.SignalLock;

import static accord.utils.Invariants.Paranoia.CONSTANT;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
import static accord.utils.Invariants.createIllegalState;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.nonNull;
import static accord.utils.Invariants.testParanoia;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.Task.State.REGISTERED;

public class AccordExecutorSignalLoop extends AbstractLoop
{
    private static class ShutdownException extends RuntimeException {}

    private static final int MAX_LOOPS = 1000; // limit the amount of time we hold the lock for
    private final SignalLock lock;
    private final Loops loops;
    private int readyToRunTarget = 1;
    private final int readyToRunLimit;
    // TODO (desired): intrusive queue using Task.next, but a little challenging because we reuse SequentialQueueTask so have ABA problem
    private final ConcurrentLinkedQueue<Task> readyToRun = new ConcurrentLinkedQueue<>();

    private Task pendingHead, pendingTail;
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
        this.loops = new Loops(mode, threads, name, this::task);
        this.readyToRunLimit = Math.min(threads * 4, SignalLock.MAX_SIGNAL_COUNT);
    }

    <P1> void submit(Consumer<P1> sync, Function<P1, Task> async, P1 p1)
    {
        Task next = async.apply(p1);
        Task prev = push(next);
        if (prev == null)
            lock.signalLockWork();
    }

    @Override
    final void beforeUnlockExternal()
    {
        // work may arrive at the runnable queue directly;
        // we must ensure the lock work bit is set, to ensure this is processed into readyToRun
        if (hasAlreadyWaitingToRun())
            lock.signalLockWorkExclusive();
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
        Invariants.require(mode == Mode.RUN_WITHOUT_LOCK, "%s does not support %s", AccordExecutorSignalLoop.this, mode);
        return new LoopTask(index, id);
    }

    // NOTE: some consequences MUST be processed before their parent task completes. So draining NEW work first is safe
    //  but draining completions first is NOT
    @Override
    final void drainUnqueuedNewWorkExclusive()
    {
        updatePendingUnqueued();
        Task requeue = null, requeueTail = null;
        Task submit = null, submitTail = null;
        Task cur = pendingHead;
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

        pendingHead = requeue;
        pendingTail = requeueTail;
        while (submit != null)
        {
            Task next = submit.next;
            submit.next = null;
            submit.submitExclusiveNoExcept();
            submit = next;
        }

        if (Invariants.isParanoid() && testParanoia(LINEAR, CONSTANT, LOW))
        {
            for (Task check = pendingHead ; check != null ; check = check.next)
                Invariants.require(!check.isNewWork(), "%s is new work left pending by drainUnqueuedNewWorkExclusive", check);
        }
    }

    private boolean enqueueOnePending()
    {
        if (pendingCount == 0)
            return false;

        --pendingCount;
        Task next = pendingHead;
        pendingHead = destructiveNext(next);
        if (pendingHead == null)
            pendingTail = null;

        if (next.compareTo(REGISTERED) < 0) next.submitExclusiveNoExcept();
        else next.completeExclusiveNoExcept();
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
            else if (task.prepareExclusiveNoExcept())
            {
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
        Task addHead = null, addTail = null;
        {
            Task cur = acquireUnqueuedExclusive();
            while (cur != null)
            {
                Task next = cur.next;
                if (addHead == null) addHead = addTail = setNextNull(cur);
                else addHead = reverseOne(addHead, cur);
                ++count;
                cur = next;
            }
        }

        pendingCount += count;
        if (addHead != null)
        {
            if (pendingHead == null) pendingHead = addHead;
            else pendingTail.next = addHead;
            pendingTail = addTail;
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

    class LoopTask extends Loops.LoopTask
    {
        final int index;

        LoopTask(int index, String id)
        {
            super(id);
            this.index = index;
        }

        private Task awaitWork(TaskRunner self)
        {
            while (true)
            {
                if (lock.awaitAsyncOrLock(index))
                {
                    if (!enterLock(self))
                        continue;

                    if (!fetchWorkExclusiveAndUnlock(self))
                        continue;
                }

                // we must not discard a task we have already been granted: it is PREPARED, so it holds cache locks
                // and queue positions that only its completion releases. Run it, and exit on shutdown once we hold
                // nothing (shutdown()'s signalAllRegistered wakes us without granting a permit, so a null poll here
                // is how a shutdown wakeup is normally observed)
                Task next = pollReadyToRun();
                if (next == null && shutdown)
                    throw new ShutdownException();

                return nonNull(next);
            }
        }

        /**
         * Register the lock we have just acquired with our {@link TaskRunner}, so that the reentrancy guards see that
         * we hold it, and so that the {@code exitAccordLockedExecutor} performed when we release it is balanced.
         */
        private boolean enterLock(TaskRunner self)
        {
            if (self.tryEnterAccordLockedExecutor(AccordExecutorSignalLoop.this))
            {
                if (DEBUG_EXECUTION) debug.onEnterLock();
                return true;
            }

            lock.unlock();
            AccordExecutor locked = self.accordLockedExecutor();
            int depth = self.resetAccordLockedExecutor();
            throw illegalState("%s TaskRunner holding %s lock to depth %s", AccordExecutorSignalLoop.this, locked, depth);
        }

        /**
         * Complete {@code executed} (if any), fetch more work, and release the lock, returning true if we acquired
         * async work to run as we did so.
         *
         * The lock is released however we exit: any failure - including one that leaves a nested acquisition
         * unreleased - must not leave the executor locked, as no other thread could then make progress and this thread
         * would fail its next {@link SignalLock#awaitAsyncOrLock} on discovering it is still the owner.
         */
        private boolean completeAndFetchWorkExclusiveAndUnlock(TaskRunner self, Task complete)
        {
            try
            {
                complete.completeExclusiveNoExcept();
                fetchWorkExclusive();
            }
            catch (Throwable t)
            {
                unlockOnFailure(self, t);
                throw t;
            }
            return exitLockAndAcquireAsyncWork(self);
        }

        private boolean fetchWorkExclusiveAndUnlock(TaskRunner self)
        {
            try
            {
                fetchWorkExclusive();
            }
            catch (Throwable t)
            {
                unlockOnFailure(self, t);
                throw t;
            }
            return exitLockAndAcquireAsyncWork(self);
        }

        /**
         * Release the lock while unwinding a failure. We deliberately do not acquire async work as we do so: the
         * permit is paired 1:1 with a {@code readyToRun} entry we are in no position to run.
         */
        private void unlockOnFailure(TaskRunner self, Throwable t)
        {
            try
            {
                // release the lock first: an assertion in exitLock must not be able to leave the executor locked
                lock.unlock();
                exitLock(self);
            }
            catch (Throwable t2)
            {
                try { t.addSuppressed(t2); }
                catch (Throwable t3) { }
            }
        }

        private boolean exitLockAndAcquireAsyncWork(TaskRunner self)
        {
            exitLock(self);
            return lock.unlockAndAcquireAsyncWork();
        }

        private void completeExclusiveAndUnlock(TaskRunner self, Task complete)
        {
            try
            {
                complete.completeExclusiveNoExcept();
            }
            catch (Throwable t)
            {
                unlockOnFailure(self, t);
                throw t;
            }
            exitLock(self);
            lock.unlock();
        }

        /**
         * Balance the {@code tryEnterAccordLockedExecutor} performed by {@link #enterLock}. This must happen on every
         * release: {@link #tryLock}, {@link #ensureLockNotHeld} and {@link AccordExecutor#lock(TaskRunner)} on any
         * other executor all read this state, and a leaked acquisition would silently disable inline completion,
         * report a bogus illegal state on every task exception, and forbid this thread from ever locking another
         * executor.
         */
        private void exitLock(TaskRunner self)
        {
            if (DEBUG_EXECUTION) debug.onExitLock();
            self.exitAccordLockedExecutor();
            Invariants.require(self.accordLockedExecutor() == null, "%s still holds %s as it releases the lock", id, self.accordLockedExecutor());
        }

        private void ensureLockNotHeld(TaskRunner self)
        {
            if (self.accordLockedExecutor() != null || lock.isOwner())
            {
                AccordExecutor locked = self.accordLockedExecutor();
                int lockDepth = lock.unlockAll(1);
                int runnerDepth = self.resetAccordLockedExecutor();
                try
                {
                    if (locked != null && locked != AccordExecutorSignalLoop.this)
                        agent.onException(createIllegalState("Invalid lock state encountered on %s: TaskRunner reports another executor locked (%s) and %s acquisitions; lock reports %s", id, locked, runnerDepth, lockDepth));
                    else
                        agent.onException(createIllegalState("Invalid lock state encountered on %s: TaskRunner reports %s acquisition(s), lock reports %s", id, runnerDepth, lockDepth));
                }
                catch (Throwable t2) {}
            }
        }


        private Task completeAndMaybeGetWork(TaskRunner self, @Nonnull Task complete)
        {
            if (lock.tryAcquireAsyncWork())
                pushCompleted(complete); // we have a permit; fall-through
            else if (!tryLock(self))
                return pushCompletedAndReturn(complete, null);
            else if (!completeAndFetchWorkExclusiveAndUnlock(self, complete))
                return null;

            Task next = pollReadyToRun();
            if (next == null)
            {
                Invariants.expect(shutdown);
                // make sure any pending work is drained
                lock(self);
                try { drainUnqueuedNewWorkExclusive(); }
                finally { unlock(self); }
                next = pollReadyToRun();
                if (next == null)
                    throw new ShutdownException();
            }

            return nonNull(next);
        }

        private Task pushCompletedAndReturn(@Nonnull Task complete, Task result)
        {
            pushCompleted(complete);
            return result;
        }

        private void pushCompleted(@Nonnull Task complete)
        {
            Task wrapped = complete.unwrap();
            Task head, tail;
            if (wrapped.next == null) head = tail = complete;
            else
            {
                Task cur = wrapped.next;
                wrapped.next = null;
                Task.prepareConsequences(wrapped, cur);

                // every consequence is submitted before the parent completes (see Task.completeExclusiveNoExcept), so
                // they all belong in the tail, as the list is reversed when consumed
                head = complete;
                complete.next = cur;
                while (cur.next != null)
                    cur = cur.next;
                tail = cur;
            }

            if (push(head, tail) == null)
                lock.signalLockWork();
        }

        final boolean tryLock(TaskRunner self)
        {
            if (self.accordLockedExecutor() != null)
                return false;

            return lock.tryLock(index) && enterLock(self);
        }

        @Override
        public void run()
        {
            Thread thread = Thread.currentThread();
            TaskRunner self = TaskRunner.get(thread);
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
                        try { task = completeAndMaybeGetWork(self, task); }
                        catch (Throwable t) { task = null; throw t; }
                    }
                    if (task == null)
                        task = awaitWork(self);

                    task.runNoExcept(self);
                }
                catch (ShutdownException ignore)
                {
                    break;
                }
                catch (Throwable t)
                {
                    // an agent that throws (e.g. an in-JVM dtest instance kill) must not silently kill the loop,
                    // as the executor would then stall for everyone else with nothing reported
                    try { agent.onException(t); }
                    catch (Throwable t2) { }
                    finally { ensureLockNotHeld(self); }
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
    Loops loops()
    {
        return loops;
    }

    @Override
    boolean isInLoop()
    {
        return loops.isInLoop();
    }

    @Override
    public boolean isOwningThread()
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
