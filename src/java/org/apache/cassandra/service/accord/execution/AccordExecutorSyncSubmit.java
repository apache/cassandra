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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

import accord.api.Agent;

public class AccordExecutorSyncSubmit extends AbstractLockLoop
{
    private final Loops loops;
    private final ReentrantLock lock;
    private final Condition hasWork;

    public AccordExecutorSyncSubmit(int executorId, Mode mode, String name, Agent agent)
    {
        this(executorId, mode, 1, constant(name), agent);
    }

    public AccordExecutorSyncSubmit(int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent)
    {
        this(new ReentrantLock(), executorId, mode, threads, name, agent);
    }

    private AccordExecutorSyncSubmit(ReentrantLock lock, int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent)
    {
        super(lock, executorId, agent);
        this.lock = lock;
        this.hasWork = lock.newCondition();
        this.loops = new Loops(mode, threads, name, this::task);
    }

    @Override
    void awaitExclusive() throws InterruptedException
    {
        hasWork.await();
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
        return lock.isHeldByCurrentThread();
    }

    @Override
    void notifyWork()
    {
        lock.lock();
        try { hasWork.signal(); }
        finally { lock.unlock(); }
    }

    @Override
    void notifyWorkExclusive()
    {
        hasWork.signal();
    }

    <P1> void submitExternal(Consumer<P1> sync, Function<P1, Task> async, P1 p1)
    {
        TaskRunner self = TaskRunner.get();
        lock(self);
        try
        {
            submitExternalExclusive(sync, async, p1);
        }
        finally
        {
            unlock(self);
        }
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
