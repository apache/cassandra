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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;

import accord.api.Agent;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import org.apache.cassandra.metrics.AccordCacheMetrics;

class AccordExecutorSyncSubmit extends AccordExecutorAbstractLockLoop
{
    private final AccordExecutorLoops loops;
    private final ReentrantLock lock;
    private final Condition hasWork;

    public AccordExecutorSyncSubmit(int executorId, Mode mode, String name, AccordCacheMetrics metrics, Agent agent)
    {
        this(executorId, mode, 1, constant(name), metrics, agent);
    }

    public AccordExecutorSyncSubmit(int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, Agent agent)
    {
        this(new ReentrantLock(), executorId, mode, threads, name, metrics, agent);
    }

    private AccordExecutorSyncSubmit(ReentrantLock lock, int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, Agent agent)
    {
        super(lock, executorId, metrics, agent);
        this.lock = lock;
        this.hasWork = lock.newCondition();
        this.loops = new AccordExecutorLoops(mode, threads, name, this::task);
    }

    @Override
    void awaitExclusive() throws InterruptedException
    {
        hasWork.await();
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
        return lock.isHeldByCurrentThread();
    }

    @Override
    void notifyWork()
    {
        lock.lock();
        try
        {
            hasWork.signal();
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    void notifyWorkExclusive()
    {
        hasWork.signal();
    }

    <P1s, P1a, P2, P3, P4> void submitExternal(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Submittable> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        lock.lock();
        try
        {
            submitExternalExclusive(sync, async, p1s, p1a, p2, p3, p4);
        }
        finally
        {
            lock.unlock();
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
