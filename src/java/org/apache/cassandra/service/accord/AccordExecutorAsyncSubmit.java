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
import java.util.function.IntFunction;

import accord.api.Agent;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.utils.concurrent.LockWithAsyncSignal;

// WARNING: experimental - needs more testing
class AccordExecutorAsyncSubmit extends AccordExecutorAbstractSemiSyncSubmit
{
    private final AccordExecutorLoops loops;
    private final LockWithAsyncSignal lock;

    public AccordExecutorAsyncSubmit(int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, Agent agent)
    {
        this(new LockWithAsyncSignal(), executorId, mode, threads, name, metrics, agent);
    }

    private AccordExecutorAsyncSubmit(LockWithAsyncSignal lock, int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, Agent agent)
    {
        super(lock, executorId, metrics, agent);
        this.lock = lock;
        this.loops = new AccordExecutorLoops(mode, threads, name, this::task);
    }

    @Override
    void awaitExclusive() throws InterruptedException
    {
        lock.clearSignal();
        if (submitted.isEmpty())
            lock.await();
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
    void notifyWork()
    {
        lock.signal();
    }

    @Override
    void notifyWorkExclusive()
    {
        lock.signal();
    }

    @Override
    boolean isOwningThread()
    {
        return lock.isOwner(Thread.currentThread());
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
