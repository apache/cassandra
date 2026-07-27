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

import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.COMMAND_STORE;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;
import static org.apache.cassandra.service.accord.execution.TaskQueueRunnable.RUNNABLE;

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

        @Override void submitExclusive() { throw new UnsupportedOperationException(); }
        @Override boolean isNewWork() { throw new UnsupportedOperationException(); }
        @Override public void cancel() { throw new UnsupportedOperationException(); }

        @Override
        void preRunExclusive()
        {
            super.preRunExclusive();
            queue.preRunTask();
        }

        @Override
        void run()
        {
            queue.runTask();
        }

        @Override
        void cleanupExclusive(AccordExecutor executor, boolean executed)
        {
            unsafeSetStateExclusive(WAITING_TO_RUN);
            queue.cleanupTask(executed);
        }

        @Override
        void reportFailure(Throwable t)
        {
            queue.task.reportFailure(t);
        }

        @Override
        String toDescription()
        {
            return queue.task.toDescription();
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
    private boolean stopped;
    private volatile boolean visibleStopped;
    private boolean terminated;

    final DebugExecution.DebugExclusiveExecutor debug;

    ExclusiveExecutor(AccordExecutor executor)
    {
        this(executor, -1);
    }

    ExclusiveExecutor(AccordExecutor executor, int commandStoreId)
    {
        super(RUNNABLE, commandStoreId < 0 ? Task.GroupKind.NONE : Task.GroupKind.EXCLUSIVE, AccordExecutor.EXCLUSIVE_QUEUE_LIMITS);
        this.executor = executor;
        this.commandStoreId = commandStoreId;
        this.selfTask = new ExclusiveExecutorTask(this);
        this.debug = DebugExecution.DebugExclusiveExecutor.maybeDebug(executor.debug, commandStoreId);
    }

    void preRunTask()
    {
        task.preRunExclusive();
    }

    void runTask()
    {
        Thread self = Thread.currentThread();
        if (!ownerUpdater.compareAndSet(this, null, self))
        {
            if (DEBUG_EXECUTION) debug.onWaiting();
            Invariants.require(waiting == null);
            waiting = self;
            outer:
            do
            {
                while (true)
                {
                    Thread owner = this.owner;
                    if (owner == self) break outer;
                    if (owner == null) continue outer;
                    LockSupport.park();
                }
            }
            while (!ownerUpdater.compareAndSet(this, null, self));
            Invariants.require(waiting == self);
            waiting = null;
        }

        try
        {
            if (stopped && reject(task))
                task.reportFailure(new RejectedExecutionException(commandStoreId + " is terminated. Cannot execute " + ((SafeTask<?>) task).executionContext()));
            else
                task.run();
        }
        finally
        {
            // NOTE: we can ONLY safely release owner here due to AccordCacheEntry locking, which remains in place until AccordTask.releaseResourcesExclusive
            //       this also relies on AccordSafeCommandStore$ExclusiveCaches.acquireIfLoaded returning false when the entry is locked
            owner = null;
        }
    }

    private boolean reject(Task task)
    {
        if (!(task instanceof SafeTask<?>))
            return true;

        ExecutionContext context = ((SafeTask<?>) task).executionContext();
        return !(terminated ? (context instanceof Unterminatable) : (context instanceof Unstoppable));
    }

    void cleanupTask(boolean executed)
    {
        try
        {
            task.unsetQueue(this);
            task.cleanupExclusive(executor, executed);
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
            Invariants.require(isEmptySingle());
            if (incrementArrivals)
                incrementArrivals(newTask);
            incrementDispatches(newTask);
            task = newTask;
            task.setQueue(this);
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
        Invariants.require(remove == task);
        // cannot overwrite task while it is being executed - this cannot happen for AccordTask
        // but can for other tasks that don't track their own state

        decrementDispatches(task);
        task.unsetQueue(this);
        task = pollMulti();
        if (DEBUG_EXECUTION) debug.onSetTask(task);
        if (executor.runnable.isWaiting(selfTask))
        {
            if (task == null) executor.runnable.unqueue(selfTask);
            else
            {
                selfTask.position = task.position;
                executor.runnable.requeue(selfTask);
            }
        }
        else
        {
            Invariants.expect(false, "%s should have been queued to run as it had the task %s pending, that has now been cancelled", this, remove);
            if (task != null)
            {
                selfTask.position = task.position;
                selfTask.unsafeSetStateExclusive(WAITING_TO_RUN);
                executor.runnable.enqueue(selfTask, false);
            }
        }
        Invariants.require(task == null || executor.runnable.isWaiting(selfTask));
    }

    public boolean inExecutor()
    {
        return owner == Thread.currentThread();
    }

    public boolean stopped()
    {
        return visibleStopped;
    }

    public void stop()
    {
        Invariants.require(inExecutor());
        this.stopped = true;
        this.visibleStopped = true;
    }

    public void terminate()
    {
        Invariants.require(inExecutor());
        this.visibleStopped = this.terminated = this.stopped = true;
    }

    @Override
    public AsyncChain<Void> chain(Runnable run)
    {
        return AsyncChains.chain(this, run);
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

    Task inherit()
    {
        Thread thread = Thread.currentThread();
        if (thread == owner)
            return Task.unwrap(task);

        return executor.inherit(thread);
    }

    @Override
    public void execute(Runnable run)
    {
        Task inherit = inherit();
        PlainRunnable submit = inherit == null ? new PlainRunnable(executor, null, run, this, Task.ExclusiveGroup.OTHER)
                                               : new PlainRunnable(executor, null, run, this, Task.ExclusiveGroup.OTHER, inherit.position, inherit.tranche());
        executor.submit(submit);
    }

    @Override
    public Cancellable execute(AsyncCallbacks.RunOrFail runOrFail)
    {
        Task inherit = inherit();
        PlainChain submit = inherit == null ? new PlainChain(executor, runOrFail, ExclusiveExecutor.this, Task.ExclusiveGroup.OTHER)
                                            : new PlainChain(executor, runOrFail, ExclusiveExecutor.this, Task.ExclusiveGroup.OTHER, inherit.position, inherit.tranche());
        return executor.submit(submit);
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
            executor.agent.onException(t);
        }
        finally
        {
            if (owner == null)
            {
                Thread waiting = this.waiting;
                Invariants.require(waiting != self);
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
