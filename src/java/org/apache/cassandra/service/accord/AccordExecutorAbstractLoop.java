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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import accord.api.Agent;
import accord.utils.Invariants;

import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;

abstract class AccordExecutorAbstractLoop extends AccordExecutor
{
    private volatile Task unqueued;
    private static final AtomicReferenceFieldUpdater<AccordExecutorAbstractLoop, Task> unqueuedUpdater = AtomicReferenceFieldUpdater.newUpdater(AccordExecutorAbstractLoop.class, Task.class, "unqueued");

    AccordExecutorAbstractLoop(Lock lock, int executorId, Agent agent)
    {
        super(lock, executorId, agent);
    }

    abstract AccordExecutorLoops loops();

    boolean hasUnqueued()
    {
        return unqueued != null;
    }

    Task unqueued()
    {
        return unqueued;
    }

    final Task push(Task submit)
    {
        Invariants.require(submit.next == null);
        while (true)
        {
            Task next = unqueued;
            submit.next = next;
            if (unqueuedUpdater.compareAndSet(this, next, submit))
                return next;
        }
    }

    @Override
    public boolean hasTasks()
    {
        if (tasks > 0 || unqueued != null)
            return true;

        lock();
        try
        {
            return tasks > 0 || unqueued != null;
        }
        finally
        {
            unlock();
        }
    }

    final void updateWaitingToRunExclusive()
    {
        drainUnqueuedExclusive();
        super.updateWaitingToRunExclusive();
    }

    final void drainUnqueuedExclusive()
    {
        Task cur = acquireUnqueuedExclusive();
        while (cur != null)
            cur = enqueueOneExclusive(cur);
    }

    final Task acquireUnqueuedExclusive()
    {
        return unqueuedUpdater.getAndSet(this, null);
    }

    final Task enqueueOneExclusive(Task cur)
    {
        Invariants.require(cur != null);
        Task next = cur.next;
        cur.next = null;
        if (cur.isReadyToCleanup()) completeTaskExclusive(cur);
        else cur.submitExclusive(this);
        return next;
    }

    final Task enqueueOneCleanup(Task cur)
    {
        Invariants.require(cur != null);
        Task next = cur.next;
        cur.next = null;
        completeTaskExclusive(cur);
        return next;
    }

    final Task enqueueOneSubmit(Task cur)
    {
        Invariants.require(cur != null);
        Task next = cur.next;
        cur.next = null;
        cur.submitExclusive(this);
        return next;
    }

    @Override
    public Stream<? extends DebuggableTaskRunner> active()
    {
        return loops().active();
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }
}
