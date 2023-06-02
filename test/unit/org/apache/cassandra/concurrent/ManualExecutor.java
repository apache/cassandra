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

import accord.utils.Invariants;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ManualExecutor implements ExecutorPlus
{
    private final Queue<Task> tasks = new ArrayDeque<>();
    private int completedCount = 0;

    public void runOne()
    {
        Task task = tasks.remove();
        task.run();
        completedCount++;
    }

    public void runAll()
    {
        while (!tasks.isEmpty())
            runOne();
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable)
    {
        return submit((WithResources) null, callable);
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Callable<T> callable)
    {
        FutureImpl<T> future = new FutureImpl<>();
        tasks.add(new Task(null, callable, withResources, null, future));
        return future;
    }

    @Override
    public Future<?> submit(Runnable runnable)
    {
        return submit(null, runnable, null);
    }

    @Override
    public Future<?> submit(WithResources withResources, Runnable runnable)
    {
        return submit(withResources, runnable, null);
    }

    @Override
    public <T> Future<T> submit(Runnable runnable, T result)
    {
        return submit(null, runnable, result);
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Runnable runnable, T result)
    {
        FutureImpl<T> future = new FutureImpl<>();
        tasks.add(new Task(runnable, null, withResources, result, future));
        return future;
    }

    @Override
    public void execute(Runnable runnable)
    {
        execute(null, runnable);
    }

    @Override
    public void execute(WithResources withResources, Runnable runnable)
    {
        tasks.add(new Task(runnable, null, withResources, null, null));
    }

    private static class Task
    {
        private final Runnable runnable;
        private final Callable<?> callable;
        private final WithResources withResources;
        private final FutureImpl<?> future;

        private Object result;

        Task(Runnable runnable, Callable<?> callable, WithResources withResources, Object result, FutureImpl<?> future)
        {
            Invariants.checkArgument(runnable != null ^ callable != null);

            this.runnable = runnable;
            this.callable = callable;
            this.withResources = withResources;
            this.result = result;
            this.future = future;
        }

        void run()
        {
            try (Closeable ignored = withResources == null ? null : withResources.get())
            {
                if (null != runnable)
                    runnable.run();
                else
                    result = callable.call();

                if (null != future)
                    future.succeed(result);
            }
            catch (Throwable t)
            {
                ExecutionFailure.handle(t);
                if (null != future)
                    future.fail(t);
            }
        }
    }

    private static class FutureImpl<V> extends org.apache.cassandra.utils.concurrent.AsyncFuture<V>
    {
        @SuppressWarnings("unchecked")
        void succeed(Object v)
        {
            trySuccess((V) v);
        }

        void fail(Throwable throwable)
        {
            tryFailure(throwable);
        }
    }

    @Override
    public boolean inExecutor()
    {
        return true;
    }

    @Override public int  getActiveTaskCount()    { return 0; }
    @Override public long getCompletedTaskCount() { return completedCount; }
    @Override public int  getPendingTaskCount()   { return tasks.size(); }

    @Override public int  getCorePoolSize()       { return 0; }
    @Override public int  getMaximumPoolSize()    { return 0; }
    @Override public void setCorePoolSize(int newCorePoolSize)       { throw new IllegalArgumentException("Cannot resize ManualExecutor"); }
    @Override public void setMaximumPoolSize(int newMaximumPoolSize) { throw new IllegalArgumentException("Cannot resize ManualExecutor"); }

    @Override public void shutdown() {}
    @Override public List<Runnable> shutdownNow() { return Collections.emptyList(); }
    @Override public boolean isShutdown()   { return false; }
    @Override public boolean isTerminated() { return false; }
    @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
}
