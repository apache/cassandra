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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;

public class ForwardingLocalAwareExecutorPlus implements LocalAwareExecutorPlus
{
    private final ExecutorPlus delegate;

    public ForwardingLocalAwareExecutorPlus(ExecutorPlus delegate)
    {
        this.delegate = delegate;
    }

    protected ExecutorPlus delegate()
    {
        return delegate;
    }

    @Override
    public void shutdown()
    {
        delegate().shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        return delegate().shutdownNow();
    }

    @Override
    public boolean isShutdown()
    {
        return delegate().isShutdown();
    }

    @Override
    public boolean isTerminated()
    {
        return delegate().isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return delegate().awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
        return delegate().submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
        return delegate().submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task)
    {
        return delegate().submit(task);
    }

    @Override
    public void execute(WithResources withResources, Runnable task)
    {
        delegate().execute(withResources, task);
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Callable<T> task)
    {
        return delegate().submit(withResources, task);
    }

    @Override
    public Future<?> submit(WithResources withResources, Runnable task)
    {
        return delegate().submit(withResources, task);
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Runnable task, T result)
    {
        return delegate().submit(withResources, task, result);
    }

    @Override
    public boolean inExecutor()
    {
        return delegate().inExecutor();
    }

    @Override
    public void execute(Runnable command)
    {
        delegate().execute(command);
    }

    @Override
    public int getCorePoolSize()
    {
        return delegate().getCorePoolSize();
    }

    @Override
    public void setCorePoolSize(int newCorePoolSize)
    {
        delegate().setCorePoolSize(newCorePoolSize);
    }

    @Override
    public int getMaximumPoolSize()
    {
        return delegate().getMaximumPoolSize();
    }

    @Override
    public void setMaximumPoolSize(int newMaximumPoolSize)
    {
        delegate().setMaximumPoolSize(newMaximumPoolSize);
    }

    @Override
    public int getActiveTaskCount()
    {
        return delegate().getActiveTaskCount();
    }

    @Override
    public long getCompletedTaskCount()
    {
        return delegate().getCompletedTaskCount();
    }

    @Override
    public int getPendingTaskCount()
    {
        return delegate().getPendingTaskCount();
    }
}
