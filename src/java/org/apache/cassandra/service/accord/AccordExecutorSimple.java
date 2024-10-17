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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;

import accord.api.Agent;
import accord.utils.Invariants;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.metrics.AccordCacheMetrics;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

class AccordExecutorSimple extends AccordExecutor
{
    final ExecutorPlus executor;
    final ReentrantLock lock;

    public AccordExecutorSimple(int executorId, String name, AccordCacheMetrics metrics, Agent agent)
    {
        this(executorId, name, metrics, Stage.READ.executor(), Stage.MUTATION.executor(), Stage.READ.executor(), agent);
    }

    public AccordExecutorSimple(int executorId, String name, AccordCacheMetrics metrics, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor, ExecutorPlus rangeLoadExecutor, Agent agent)
    {
        this(executorId, name, metrics, wrap(loadExecutor), wrap(saveExecutor), wrap(rangeLoadExecutor), agent);
    }

    public AccordExecutorSimple(int executorId, String name, AccordCacheMetrics metrics, ExecutorFunction loadExecutor, ExecutorFunction saveExecutor, ExecutorFunction rangeLoadExecutor, Agent agent)
    {
        this(new ReentrantLock(), executorId, name, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    private AccordExecutorSimple(ReentrantLock lock, int executorId, String name, AccordCacheMetrics metrics, ExecutorFunction loadExecutor, ExecutorFunction saveExecutor, ExecutorFunction rangeLoadExecutor, Agent agent)
    {
        super(lock, executorId, metrics, constantFactory(loadExecutor), constantFactory(saveExecutor), constantFactory(rangeLoadExecutor), agent);
        this.lock = lock;
        this.executor = executorFactory().sequential(name);
    }

    public AccordExecutorSimple(int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        this(new ReentrantLock(), executorId, mode, threads, name, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    public AccordExecutorSimple(ReentrantLock lock, int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        super(lock, executorId, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
        Invariants.checkArgument(threads == 1);
        this.lock = lock;
        this.executor = executorFactory().sequential(name.apply(0));

    }

    @Override
    public boolean hasTasks()
    {
        return tasks + executor.getActiveTaskCount() + executor.getPendingTaskCount() > 0;
    }

    protected void run()
    {
        lock.lock();
        try
        {
            running = 1;
            while (true)
            {
                Task task = pollWaitingToRunExclusive();
                if (task == null)
                    return;

                --tasks;
                try { task.preRunExclusive(); task.run(); }
                catch (Throwable t) { task.fail(t); }
                finally { task.cleanupExclusive(); }
            }
        }
        catch (Throwable t)
        {
            throw t;
        }
        finally
        {
            running = 0;
            if (hasWaitingToRun())
                executor.execute(this::run);
            lock.unlock();
        }
    }

    @Override
    <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        lock.lock();
        try
        {
            sync.accept(this, p1s, p2, p3, p4);
        }
        finally
        {
            if (hasWaitingToRun())
                executor.execute(this::run);

            lock.unlock();
        }
    }

    @Override
    boolean isOwningThread()
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
