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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;

public class WrappedExecutorPlus implements ExecutorPlus
{
    protected final ExecutorPlus executor;

    public WrappedExecutorPlus(ExecutorPlus executor)
    {
        this.executor = executor;
    }

    public void maybeExecuteImmediately(Runnable task)
    {
        executor.maybeExecuteImmediately(task);
    }

    public void execute(WithResources withResources, Runnable task)
    {
        executor.execute(withResources, task);
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Callable<T> task)
    {
        return executor.submit(withResources, task);
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Runnable task, T result)
    {
        return executor.submit(withResources, task, result);
    }

    @Override
    public Future<?> submit(WithResources withResources, Runnable task)
    {
        return executor.submit(withResources, task);
    }

    @Override
    public boolean inExecutor()
    {
        return executor.inExecutor();
    }

    public <T> Future<T> submit(Callable<T> task)
    {
        return executor.submit(task);
    }

    public <T> Future<T> submit(Runnable task, T result)
    {
        return executor.submit(task, result);
    }

    public Future<?> submit(Runnable task)
    {
        return executor.submit(task);
    }

    public int getActiveTaskCount()
    {
        return executor.getActiveTaskCount();
    }

    public long getCompletedTaskCount()
    {
        return executor.getCompletedTaskCount();
    }

    public int getPendingTaskCount()
    {
        return executor.getPendingTaskCount();
    }

    public int getMaxTasksQueued()
    {
        return executor.getMaxTasksQueued();
    }

    public int getCorePoolSize()
    {
        return executor.getCorePoolSize();
    }

    public void setCorePoolSize(int newCorePoolSize)
    {
        executor.setCorePoolSize(newCorePoolSize);
    }

    public int getMaximumPoolSize()
    {
        return executor.getMaximumPoolSize();
    }

    public void setMaximumPoolSize(int newMaximumPoolSize)
    {
        executor.setMaximumPoolSize(newMaximumPoolSize);
    }

    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
    {
        return executor.invokeAll(tasks);
    }

    public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
    {
        return executor.invokeAll(tasks, timeout, unit);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
        return executor.invokeAny(tasks);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        return executor.invokeAny(tasks, timeout, unit);
    }

    public void shutdown()
    {
        executor.shutdown();
    }

    public List<Runnable> shutdownNow()
    {
        return executor.shutdownNow();
    }

    public boolean isShutdown()
    {
        return executor.isShutdown();
    }

    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return executor.awaitTermination(timeout, unit);
    }

    public void execute(Runnable task)
    {
        executor.execute(task);
    }
}
