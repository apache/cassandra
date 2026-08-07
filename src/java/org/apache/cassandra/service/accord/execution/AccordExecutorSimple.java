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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import accord.api.Agent;
import accord.utils.Invariants;

import org.apache.cassandra.concurrent.ExecutorPlus;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class AccordExecutorSimple extends AccordExecutor
{
    final ExecutorPlus executor;
    final ReentrantLock lock;

    public AccordExecutorSimple(int executorId, String name, Agent agent)
    {
        this(new ReentrantLock(), executorId, name, agent);
    }

    public AccordExecutorSimple(int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent)
    {
        this(new ReentrantLock(), executorId, mode, threads, name, agent);
    }

    private AccordExecutorSimple(ReentrantLock lock, int executorId, String name, Agent agent)
    {
        super(lock, executorId, agent);
        this.lock = lock;
        this.executor = executorFactory().sequential(name);
    }

    public AccordExecutorSimple(ReentrantLock lock, int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent)
    {
        super(lock, executorId, agent);
        Invariants.requireArgument(threads == 1);
        this.lock = lock;
        this.executor = executorFactory().sequential(name.apply(0));
    }

    @Override
    boolean isInLoop()
    {
        return executor.inExecutor();
    }

    @Override
    public boolean hasTasks()
    {
        return tasks + executor.getActiveTaskCount() + executor.getPendingTaskCount() > 0;
    }

    @Override
    void beforeUnlockExternal()
    {
        if (hasWaitingToRun())
            executor.execute(this::run);
    }

    protected void run()
    {
        TaskRunner self = TaskRunner.get();
        self.setAccordActiveExecutor(AccordExecutorSimple.this);
        lock.lock();
        try
        {
            while (true)
            {
                Task task = pollWaitingToRunExclusive();
                if (task == null)
                {
                    notifyQuiescentExclusive();
                    return;
                }

                prepareRunComplete(self, task);
            }
        }
        finally
        {
            if (hasWaitingToRun())
                executor.execute(this::run);
            lock.unlock();
        }
    }

    @Override
    <P1> void submit(Consumer<P1> sync, Function<P1, Task> async, P1 p1)
    {
        lock.lock();
        try
        {
            sync.accept(p1);
        }
        finally
        {
            if (hasWaitingToRun())
                executor.execute(this::run);

            lock.unlock();
        }
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

    @Override
    public boolean isOwningThread()
    {
        return lock.isHeldByCurrentThread();
    }

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return executor.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }

}
