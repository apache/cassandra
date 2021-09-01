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

import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION) // shared to support instanceof check in SimulatedAction
public class ImmediateExecutor implements LocalAwareExecutorPlus
{
    public static final ImmediateExecutor INSTANCE = new ImmediateExecutor();

    private ImmediateExecutor() {}

    public <T> Future<T> submit(Callable<T> task)
    {
        try
        {
            return ImmediateFuture.success(task.call());
        }
        catch (Throwable t)
        {
            ExecutionFailure.handle(t);
            return ImmediateFuture.failure(t);
        }
    }

    public <T> Future<T> submit(Runnable task, T result)
    {
        try
        {
            task.run();
            return ImmediateFuture.success(result);
        }
        catch (Throwable t)
        {
            ExecutionFailure.handle(t);
            return ImmediateFuture.failure(t);
        }
    }

    public Future<?> submit(Runnable task)
    {
        try
        {
            task.run();
            return ImmediateFuture.success(null);
        }
        catch (Throwable t)
        {
            ExecutionFailure.handle(t);
            return ImmediateFuture.failure(t);
        }
    }

    @Override
    public void execute(WithResources withResources, Runnable task)
    {
        try (Closeable ignored = withResources.get())
        {
            task.run();
        }
        catch (Throwable t)
        {
            ExecutionFailure.handle(t);
        }
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Callable<T> task)
    {
        try (Closeable ignored = withResources.get())
        {
            return ImmediateFuture.success(task.call());
        }
        catch (Throwable t)
        {
            ExecutionFailure.handle(t);
            return ImmediateFuture.failure(t);
        }
    }

    @Override
    public Future<?> submit(WithResources withResources, Runnable task)
    {
        return submit(withResources, task, null);
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Runnable task, T result)
    {
        try (Closeable ignored = withResources.get())
        {
            task.run();
            return ImmediateFuture.success(result);
        }
        catch (Throwable t)
        {
            ExecutionFailure.handle(t);
            return ImmediateFuture.failure(t);
        }
    }

    @Override
    public boolean inExecutor()
    {
        return true;
    }

    public void execute(Runnable task)
    {
        try
        {
            task.run();
        }
        catch (Throwable t)
        {
            ExecutionFailure.handle(t);
        }
    }

    public int  getActiveTaskCount()    { return 0; }
    public long getCompletedTaskCount() { return 0; }
    public int  getPendingTaskCount()   { return 0; }
    public int  getCorePoolSize()       { return 0; }
    public int  getMaximumPoolSize()    { return 0; }
    public void setCorePoolSize(int newCorePoolSize) { throw new IllegalArgumentException("Cannot resize ImmediateExecutor"); }
    public void setMaximumPoolSize(int newMaximumPoolSize) { throw new IllegalArgumentException("Cannot resize ImmediateExecutor"); }

    public void shutdown() { }
    public List<Runnable> shutdownNow() { return Collections.emptyList(); }
    public boolean isShutdown() { return false; }
    public boolean isTerminated() { return false; }
    public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
}
