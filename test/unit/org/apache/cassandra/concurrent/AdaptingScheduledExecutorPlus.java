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
import java.util.concurrent.Executors; // checkstyle: permit this import
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import accord.utils.async.AsyncChain;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.primitives.Longs.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AdaptingScheduledExecutorPlus implements ScheduledExecutorPlus
{
    private final ScheduledExecutorService delegate;

    public AdaptingScheduledExecutorPlus(ScheduledExecutorService delegate)
    {
        this.delegate = delegate;
    }

    protected ScheduledExecutorService delegate()
    {
        return delegate;
    }

    @Override
    public ScheduledFuture<?> scheduleSelfRecurring(Runnable run, long delay, TimeUnit units)
    {
        return schedule(run, delay, units);
    }

    @Override
    public ScheduledFuture<?> scheduleAt(Runnable run, long deadline)
    {
        return schedule(run, max(0, deadline - nanoTime()), NANOSECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleTimeoutAt(Runnable run, long deadline)
    {
        return scheduleTimeoutWithDelay(run, max(0, deadline - nanoTime()), NANOSECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleTimeoutWithDelay(Runnable run, long delay, TimeUnit units)
    {
        return schedule(run, delay, units);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        return delegate().schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
    {
        return delegate().schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
        return delegate().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
        return delegate().scheduleWithFixedDelay(command, initialDelay, delay, unit);
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
        class Catch { T value;}
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

    private static <T> org.apache.cassandra.utils.concurrent.Future<T> wrap(java.util.concurrent.Future<T> future)
    {
        if (future instanceof org.apache.cassandra.utils.concurrent.Future)
            return (Future<T>) future;
        if (future instanceof AsyncChain)
        {
            AsyncChain<T> chain = (AsyncChain<T>) future;
            AsyncPromise<T> promise = new AsyncPromise<>();
            chain.begin((s, f) -> {
                if (f != null) promise.setFailure(f);
                else           promise.setSuccess(s);
            });

            return promise;
        }
        throw new IllegalStateException("Unexpected future type: " + future.getClass());
    }
}
