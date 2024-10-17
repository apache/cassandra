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
import org.apache.cassandra.metrics.AccordCacheMetrics;

// WARNING: experimental - needs more testing
class AccordExecutorSemiSyncSubmit extends AccordExecutorAbstractSemiSyncSubmit
{
    private final AccordExecutorInfiniteLoops loops;
    private final ReentrantLock lock;
    private final Condition hasWork;

    public AccordExecutorSemiSyncSubmit(int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        this(new ReentrantLock(), executorId, mode, threads, name, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    private AccordExecutorSemiSyncSubmit(ReentrantLock lock, int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        super(lock, executorId, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
        this.lock = lock;
        this.hasWork = lock.newCondition();
        this.loops = new AccordExecutorInfiniteLoops(mode, threads, name, this::task);
    }

    @Override
    void awaitExclusive() throws InterruptedException
    {
        if (submitted.isEmpty())
            hasWork.await();
    }

    @Override
    boolean isInLoop()
    {
        return loops.isInLoop();
    }

    @Override
    void notifyWorkAsync()
    {
        // we check running both sides of tryLock for ordering guarantees
        boolean hadRunning = isHeldByExecutor;
        if (lock.tryLock())
        {
            try { hasWork.signal(); }
            finally { lock.unlock(); }
        }
        else if (!hadRunning || !isHeldByExecutor)
        {
            lock.lock();
            try { hasWork.signal(); }
            finally { lock.unlock(); }
        }
    }
    
    @Override
    void notifyWorkExclusive()
    {
        hasWork.signal();
    }

    @Override
    boolean isOwningThread()
    {
        return lock.isHeldByCurrentThread();
    }

    @Override
    public void shutdown()
    {
        loops.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return loops.shutdownNow();
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
