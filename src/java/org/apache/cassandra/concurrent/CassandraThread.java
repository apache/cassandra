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

package org.apache.cassandra.concurrent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.metrics.ThreadLocalMetrics;
import org.apache.cassandra.service.accord.AccordExecutor;

import io.netty.util.concurrent.FastThreadLocalThread;

public class CassandraThread extends FastThreadLocalThread implements AccordExecutor.AccordTaskRunner
{
    private ThreadLocalMetrics threadLocalMetrics;
    private ExecutorLocals executorLocals;
    private AccordExecutor accordActiveExecutor;
    private AccordExecutor accordLockedExecutor;
    private int accordLockedExecutorDepth;
    private volatile AccordExecutor.Task accordActiveTask;
    private static final AtomicReferenceFieldUpdater<CassandraThread, AccordExecutor.Task> accordActiveTaskUpdater = AtomicReferenceFieldUpdater.newUpdater(CassandraThread.class, AccordExecutor.Task.class, "accordActiveTask");

    private final ImmediateTaskHolder immediateTaskHolder;

    public CassandraThread(ThreadGroup group, Runnable target, String name, ImmediateTaskHolder immediateTaskHolder)
    {
        super(group, target, name);
        assert immediateTaskHolder != null;
        this.immediateTaskHolder = immediateTaskHolder;
    }
    public CassandraThread(ThreadGroup group, Runnable target, String name)
    {
        this(group, target, name, ImmediateTaskHolder.NO_OP);
    }

    public CassandraThread()
    {
        super();
        this.immediateTaskHolder = ImmediateTaskHolder.NO_OP;
    }

    public CassandraThread(Runnable target)
    {
        super(target);
        this.immediateTaskHolder = ImmediateTaskHolder.NO_OP;
    }

    public ImmediateTaskHolder getImmediateTaskHolder()
    {
        return immediateTaskHolder;
    }

    public ThreadLocalMetrics getThreadLocalMetrics()
    {
        ThreadLocalMetrics current = threadLocalMetrics;
        if (current != null)
            return current;

        threadLocalMetrics = ThreadLocalMetrics.create();
        return threadLocalMetrics;
    }

    public ExecutorLocals getExecutorLocals()
    {
        ExecutorLocals current = executorLocals;
        if (current != null)
            return current;

        executorLocals = ExecutorLocals.none();
        return executorLocals;
    }

    public void setExecutorLocals(ExecutorLocals executorLocals)
    {
        this.executorLocals = executorLocals;
    }

    public ExecutorLocals replaceExecutorLocals(ExecutorLocals newExecutorLocals)
    {
        ExecutorLocals current = executorLocals;
        executorLocals = newExecutorLocals;
        return current != null ? current : ExecutorLocals.none();
    }

    public final AccordExecutor accordActiveExecutor()
    {
        return accordActiveExecutor;
    }

    public final void setAccordActiveExecutor(AccordExecutor newExecutor)
    {
        accordActiveExecutor = newExecutor;
    }

    @Override
    public final AccordExecutor accordLockedExecutor()
    {
        return accordLockedExecutor;
    }

    @Override
    public final boolean tryEnterAccordLockedExecutor(AccordExecutor newLockedExecutor)
    {
        if (accordLockedExecutor == null) accordLockedExecutor = newLockedExecutor;
        else if (accordLockedExecutor != newLockedExecutor) return false;
        ++accordLockedExecutorDepth;
        return true;
    }

    @Override
    public final void exitAccordLockedExecutor()
    {
        if (--accordLockedExecutorDepth == 0)
            accordLockedExecutor = null;
    }

    public final AccordExecutor.Task accordActiveTask()
    {
        return accordActiveTask;
    }

    // to be called only by the thread itself, so can (eventually) avoid any memory barriers
    public final AccordExecutor.Task accordActiveSelfTask()
    {
        // TODO (expected): with newer JDK use accordActiveTaskUpdater.getPlain
        return accordActiveTask;
    }

    public final void setAccordActiveTask(AccordExecutor.Task newActiveTask)
    {
        accordActiveTaskUpdater.lazySet(this, newActiveTask);
    }

    // final to avoid skipping of the cleanup logic in child classes
    final public void run()
    {
        try
        {
            super.run();
        }
        finally
        {
            if (threadLocalMetrics != null)
                threadLocalMetrics.release();
        }
    }
}
