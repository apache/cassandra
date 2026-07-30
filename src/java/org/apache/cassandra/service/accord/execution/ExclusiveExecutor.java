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

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import accord.api.ExclusiveAsyncExecutor;
import accord.local.ExecutionContext;
import accord.utils.IntrusiveHeapNode;
import accord.utils.Invariants;
import accord.utils.async.AsyncCallbacks;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.service.accord.debug.DebugExecution;
import org.apache.cassandra.service.accord.execution.Task.GroupKind;

import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.EXCLUSIVE_QUEUE_LIMITS;
import static org.apache.cassandra.service.accord.execution.Task.ExecutorQueue.RUNNABLE;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.COMMAND_STORE;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;

public final class ExclusiveExecutor extends TaskQueueMulti<Task> implements ExclusiveAsyncExecutor
{
    private static final AtomicReferenceFieldUpdater<ExclusiveExecutor, Thread> ownerUpdater = AtomicReferenceFieldUpdater.newUpdater(ExclusiveExecutor.class, Thread.class, "owner");

    static final class ExclusiveExecutorTask extends Task
    {
        final ExclusiveExecutor queue;

        ExclusiveExecutorTask(ExclusiveExecutor queue)
        {
            super(COMMAND_STORE);
            this.queue = queue;
        }

        @Override AccordExecutor executor() { return queue.executor; }
        @Override void submitExclusiveMayThrow() { throw new UnsupportedOperationException(); }
        @Override boolean isNewWork() { return false; }
        @Override public void cancel() { throw new UnsupportedOperationException(); }
        @Override boolean runMayThrow() { throw new UnsupportedOperationException(); }
        @Override void unqueueIfQueued() {}
        @Override void reportFailureMayThrow(Throwable t) { throw new UnsupportedOperationException(); }
        @Override void tryCancelExclusive() { throw new UnsupportedOperationException(); }

        boolean prepareTask()
        {
            Task task = queue.task;
            try
            {
                task.prepareExclusiveMayThrow();
                task.setStateExclusive(State.PREPARED);
                setStateExclusive(State.PREPARED);
                return true;
            }
            catch (Throwable t)
            {
                task.setStateExclusive(State.FAILED);
                task.reportFailureNoExcept(t);
                completeExclusiveMayThrow();
                return false;
            }
        }

        @Override
        void completeExclusiveMayThrow()
        {
            try
            {
                unsafeSetStateExclusive(WAITING_TO_RUN);
                executor().runnable.cleanup(this);
                queue.completeTask();
            }
            catch (Throwable t)
            {
                unhandledException(t);
            }
        }

        @Override
        public String description()
        {
            return queue.task.description();
        }

        @Override
        public String briefDescription()
        {
            return queue.task.briefDescription();
        }

        protected boolean isInHeap()
        {
            return super.isInHeap();
        }
    }

    private final AccordExecutor executor;
    final int commandStoreId;
    final ExclusiveExecutorTask selfTask;
    Task task;
    volatile Thread owner, waiting;
    private boolean isStopped;
    private volatile boolean visibleIsStopped;
    private boolean isTerminated;

    final DebugExecution.DebugExclusiveExecutor debug;

    ExclusiveExecutor(AccordExecutor executor)
    {
        this(executor, -1);
    }

    ExclusiveExecutor(AccordExecutor executor, int commandStoreId)
    {
        super(RUNNABLE, commandStoreId < 0 ? GroupKind.NONE : GroupKind.EXCLUSIVE, EXCLUSIVE_QUEUE_LIMITS);
        this.executor = executor;
        this.commandStoreId = commandStoreId;
        this.selfTask = new ExclusiveExecutorTask(this);
        this.debug = DebugExecution.DebugExclusiveExecutor.maybeDebug(executor.debug, commandStoreId);
    }

    void runTask(TaskRunner self)
    {
        Thread thread = Thread.currentThread();
        if (!ownerUpdater.compareAndSet(this, null, thread))
        {
            if (DEBUG_EXECUTION) debug.onWaiting();
            Invariants.require(waiting == null);
            waiting = thread;
            outer:
            do
            {
                while (true)
                {
                    Thread owner = this.owner;
                    if (owner == thread) break outer;
                    if (owner == null) continue outer;
                    LockSupport.park();
                }
            }
            while (!ownerUpdater.compareAndSet(this, null, thread));
            Invariants.require(waiting == thread);
            waiting = null;
        }

        try
        {
            if (isStopped && reject(task))
                task.rejectAtRuntime(new RejectedExecutionException(commandStoreId + " is terminated. Cannot execute " + task.description()));
            else
                task.runNoExcept(self);
        }
        finally
        {
            // NOTE: we can ONLY safely release owner here due to AccordCacheEntry locking, which remains in place until SafeTask.releaseResourcesExclusive
            //       this also relies on SaferCommandStore$ExclusiveCaches.acquireIfLoaded returning false when the entry is locked
            owner = null;
        }
    }

    private boolean reject(Task task)
    {
        if (!(task instanceof SafeTask<?>))
            return true;

        ExecutionContext context = ((SafeTask<?>) task).executionContext();
        return !(isTerminated ? (context instanceof Unterminatable) : (context instanceof Unstoppable));
    }

    void completeTask()
    {
        try
        {
            task.unsetQueue(kind);
            task.completeExclusiveNoExcept();
        }
        catch (Throwable t)
        {
            task.releaseResourcesExclusiveNoExcept();
            throw t;
        }
        finally
        {
            active = 0;
            task = super.pollMulti();
            if (DEBUG_EXECUTION) debug.onSetTask(task);
            if (task != null)
            {
                selfTask.position = task.position;
                selfTask.unsafeSetStateExclusive(WAITING_TO_RUN);
                executor.runnable.enqueue(selfTask, false);
            }
        }
    }

    void enqueue(Task newTask, boolean incrementArrivals)
    {
        if (task != null)
        {
            if (incrementArrivals)
                executor.runnable.incrementArrivals(selfTask);
            // TODO (expected): restore some invariant here
//                Invariants.require(selfTask.isInHeap() || selfTask.is(RUNNING));
            super.enqueueMulti(newTask, incrementArrivals);
        }
        else
        {
            Invariants.require(waitingCount == 0);
            if (incrementArrivals)
                incrementArrivals(newTask);
            incrementDispatches(newTask);
            task = newTask;
            task.setQueue(kind);
            selfTask.position = newTask.position;
            selfTask.unsafeSetStateExclusive(WAITING_TO_RUN);
            executor.runnable.enqueue(selfTask, incrementArrivals);
            if (DEBUG_EXECUTION) debug.onSetTask(newTask);
        }
    }

    @Override
    void unqueue(Task remove)
    {
        if (remove == task) removeCurrentTask(remove);
        else super.unqueueMulti(remove);
    }

    boolean tryUnqueueWaiting(Task remove)
    {
        if (remove == task) return tryRemoveCurrentTask(remove);
        else return super.tryUnqueueWaiting(remove);
    }

    private boolean tryRemoveCurrentTask(IntrusiveHeapNode remove)
    {
        if (executor.runnable.isAssigned(selfTask))
            return false;

        removeCurrentTask(remove);
        return true;
    }

    private void removeCurrentTask(IntrusiveHeapNode remove)
    {
        Invariants.require(executor.runnable.isWaiting(selfTask));
        Invariants.require(remove == task);
        // cannot overwrite task while it is being executed - this cannot happen for SafeTask
        // but can for other tasks that don't track their own state

        decrementDispatches(task);
        task.unsetQueue(kind);
        active = 0;
        task = pollMulti();
        if (DEBUG_EXECUTION) debug.onSetTask(task);
        if (task == null) executor.runnable.unqueue(selfTask);
        else
        {
            selfTask.position = task.position;
            executor.runnable.requeue(selfTask);
        }
        Invariants.require(task == null || executor.runnable.isWaiting(selfTask));
    }

    public boolean inExecutor()
    {
        return owner == Thread.currentThread();
    }

    public boolean stopped()
    {
        return visibleIsStopped;
    }

    public void stop()
    {
        Invariants.require(inExecutor());
        this.isStopped = true;
        this.visibleIsStopped = true;
    }

    public void fullStop()
    {
        Invariants.require(inExecutor());
        this.visibleIsStopped = this.isTerminated = this.isStopped = true;
    }

    @Override
    public AsyncChain<Void> chain(Runnable run)
    {
        return AsyncChains.chain(this, run);
    }

    @Override
    public AsyncChain<Void> continuationChain(Runnable run)
    {
        return AsyncChains.continuationChain(this, run);
    }

    @Override
    public <V> AsyncChain<V> chain(Callable<V> call)
    {
        return AsyncChains.chain(this, call);
    }

    @Override
    public <V> AsyncChain<V> flatChain(Callable<? extends AsyncChain<V>> call)
    {
        return AsyncChains.flatChain(this, call);
    }

    @Override
    public void execute(Runnable run)
    {
        Task inherit = executor.inherit();
        PlainRunnable task = new PlainRunnable(executor, null, run, this, Task.ExclusiveGroup.OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else executor.submitTask(task);
    }

    @Override
    public Cancellable execute(AsyncCallbacks.RunOrFail runOrFail)
    {
        Task inherit = executor.inherit();
        PlainChain task = new PlainChain(executor, runOrFail, ExclusiveExecutor.this, Task.ExclusiveGroup.OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else executor.submitTask(task);
        return task;
    }

    @Override
    public Cancellable executeContinuation(AsyncCallbacks.RunOrFail runOrFail)
    {
        Task inherit = executor.inherit();
        PlainChain task = new PlainChain(executor, runOrFail, ExclusiveExecutor.this, Task.ExclusiveGroup.OTHER);
        if (inherit == null) executor.submitTask(task);
        else
        {
            task.setIsContinuation();
            inherit.addConsequence(task);
        }
        return task;
    }

    @Override
    public boolean tryExecuteImmediately(Runnable run)
    {
        Thread thread = Thread.currentThread();
        Thread owner = this.owner;
        if (owner != null && owner != thread)
            return false;

        TaskRunner self = TaskRunner.get(thread);
        AccordExecutor active = self.accordActiveExecutor();
        if (active != null && active != executor)
            return false; // prevent cross-executor locking/execution

        if (owner == null && !ownerUpdater.compareAndSet(this, null, thread))
            return false;

        try
        {
            if (active == null)
                self.setAccordActiveExecutor(executor);

            run.run();
        }
        catch (Throwable t)
        {
            selfTask.unhandledException(t);
        }
        finally
        {
            if (owner == null)
            {
                Thread waiting = this.waiting;
                Invariants.require(waiting != thread);
                this.owner = waiting;
                if (waiting == null) // recheck, to ensure happens-before relation with a new waiter that expects any non-null owner to notify it
                    waiting = this.waiting;
                if (waiting != null)
                    LockSupport.unpark(waiting);
            }

            if (active == null)
                self.setAccordActiveExecutor(null);
        }
        return true;
    }
}
