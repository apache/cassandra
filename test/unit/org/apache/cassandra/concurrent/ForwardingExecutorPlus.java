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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

public class ForwardingExecutorPlus implements ExecutorPlus
{
    private final ExecutorService delegate;

    public ForwardingExecutorPlus(ExecutorService delegate)
    {
        this.delegate = delegate;
    }

    protected ExecutorService delegate()
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
        return wrap(delegate().submit(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
        return wrap(delegate().submit(task, result));
    }

    @Override
    public Future<?> submit(Runnable task)
    {
        return wrap(delegate().submit(task));
    }

    @Override
    public void execute(WithResources withResources, Runnable task)
    {
        execute(TaskFactory.standard().toExecute(withResources, task));
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Callable<T> task)
    {
        class Catch
        {
            T value;
        }
        Catch c = new Catch();
        Runnable exec = TaskFactory.standard().toExecute(withResources, () -> {
            try
            {
                c.value = task.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
        return submit(() -> {
            exec.run();
            return c.value;
        });
    }

    @Override
    public Future<?> submit(WithResources withResources, Runnable task)
    {
        return submit(TaskFactory.standard().toExecute(withResources, task));
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Runnable task, T result)
    {
        return submit(Executors.callable(TaskFactory.standard().toSubmit(withResources, task), result));
    }

    @Override
    public boolean inExecutor()
    {
        return false;
    }

    @Override
    public void execute(Runnable command)
    {
        delegate().execute(command);
    }

    @Override
    public int getCorePoolSize()
    {
        return 0;
    }

    @Override
    public void setCorePoolSize(int newCorePoolSize)
    {

    }

    @Override
    public int getMaximumPoolSize()
    {
        return 0;
    }

    @Override
    public void setMaximumPoolSize(int newMaximumPoolSize)
    {

    }

    @Override
    public int getActiveTaskCount()
    {
        return 0;
    }

    @Override
    public long getCompletedTaskCount()
    {
        return 0;
    }

    @Override
    public int getPendingTaskCount()
    {
        return 0;
    }

    private <T> Future<T> wrap(java.util.concurrent.Future<T> submit)
    {
        if (submit instanceof Future)
            return (Future<T>) submit;
        if (submit instanceof ListenableFuture)
        {
            AsyncPromise<T> promise = new AsyncPromise<>();
            Futures.addCallback((ListenableFuture<T>) submit, new FutureCallback<T>()
            {
                @Override
                public void onSuccess(T result)
                {
                    promise.setSuccess(result);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    promise.setFailure(t);
                }
            }, MoreExecutors.directExecutor());
            return promise;
        }
        throw new IllegalStateException("Unexpected future type: " + submit.getClass());
    }
}
