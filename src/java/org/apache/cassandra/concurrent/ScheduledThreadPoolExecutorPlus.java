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
import java.util.concurrent.*;

import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.RunnableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageService;

import static com.google.common.primitives.Longs.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.ExecutionFailure.propagating;
import static org.apache.cassandra.concurrent.ExecutionFailure.suppressing;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Like ExecutorPlus, ScheduledThreadPoolExecutorPlus always
 * logs exceptions from the tasks it is given, even if Future.get is never called elsewhere.
 *
 * Catches exceptions during Task execution so that they don't suppress subsequent invocations of the task.
 *
 * Finally, there is a special rejected execution handler for tasks rejected during the shutdown hook.
 *  - For fire and forget tasks (like ref tidy) we can safely ignore the exceptions.
 *  - For any callers that care to know their task was rejected we cancel passed task.
 */
public class ScheduledThreadPoolExecutorPlus extends ScheduledThreadPoolExecutor implements ScheduledExecutorPlus
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduledThreadPoolExecutorPlus.class);
    private static final TaskFactory taskFactory = TaskFactory.standard();

    public static final RejectedExecutionHandler rejectedExecutionHandler = (task, executor) ->
    {
        if (executor.isShutdown())
        {
            // TODO: this sequence of events seems poorly thought out
            if (!StorageService.instance.isShutdown())
                throw new RejectedExecutionException("ScheduledThreadPoolExecutor has shut down.");

            //Give some notification to the caller the task isn't going to run
            if (task instanceof java.util.concurrent.Future)
                ((java.util.concurrent.Future<?>) task).cancel(false);

            logger.debug("ScheduledThreadPoolExecutor has shut down as part of C* shutdown");
        }
        else
        {
            throw new AssertionError("Unknown rejection of ScheduledThreadPoolExecutor task");
        }
    };

    ScheduledThreadPoolExecutorPlus(NamedThreadFactory threadFactory)
    {
        super(1, threadFactory);
        setRejectedExecutionHandler(rejectedExecutionHandler);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit)
    {
        return super.schedule(propagating(task), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> task, long delay, TimeUnit unit)
    {
        return super.schedule(propagating(task), delay, unit);
    }

    // override scheduling to suppress exceptions that would cancel future executions
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long initialDelay, long period, TimeUnit unit)
    {
        return super.scheduleAtFixedRate(suppressing(task), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long initialDelay, long delay, TimeUnit unit)
    {
        return super.scheduleWithFixedDelay(suppressing(task), initialDelay, delay, unit);
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

    /*======== BEGIN DIRECT COPY OF ThreadPoolExecutorPlus ===============*/

    private <T extends Runnable> T addTask(T task)
    {
        super.execute(task);
        return task;
    }

    @Override
    public void execute(Runnable run)
    {
        addTask(taskFactory.toExecute(run));
    }

    @Override
    public void execute(WithResources withResources, Runnable run)
    {
        addTask(taskFactory.toExecute(withResources, run));
    }

    @Override
    public Future<?> submit(Runnable run)
    {
        return addTask(taskFactory.toSubmit(run));
    }

    @Override
    public <T> Future<T> submit(Runnable run, T result)
    {
        return addTask(taskFactory.toSubmit(run, result));
    }

    @Override
    public <T> Future<T> submit(Callable<T> call)
    {
        return addTask(taskFactory.toSubmit(call));
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Runnable run, T result)
    {
        return addTask(taskFactory.toSubmit(withResources, run, result));
    }

    @Override
    public Future<?> submit(WithResources withResources, Runnable run)
    {
        return addTask(taskFactory.toSubmit(withResources, run));
    }

    @Override
    public <T> Future<T> submit(WithResources withResources, Callable<T> call)
    {
        return addTask(taskFactory.toSubmit(withResources, call));
    }

    @Override
    public boolean inExecutor()
    {
        return Thread.currentThread().getThreadGroup() == getThreadFactory().threadGroup;
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value)
    {
        return taskFactory.toSubmit(runnable, value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable)
    {
        return taskFactory.toSubmit(callable);
    }

    @Override
    public NamedThreadFactory getThreadFactory()
    {
        return (NamedThreadFactory) super.getThreadFactory();
    }

    /*======== DIRECT COPY OF ThreadPoolExecutorBase ===============*/

    @Override
    public List<Runnable> shutdownNow()
    {
        List<Runnable> cancelled = super.shutdownNow();
        for (Runnable c : cancelled)
        {
            if (c instanceof java.util.concurrent.Future<?>)
                ((java.util.concurrent.Future<?>) c).cancel(true);
        }
        return cancelled;
    }

    @Override
    protected void terminated()
    {
        getThreadFactory().close();
    }

    @Override
    public int getActiveTaskCount()
    {
        return getActiveCount();
    }

    @Override
    public int getPendingTaskCount()
    {
        return getQueue().size();
    }

    /*======== DIRECT COPY OF SingleThreadExecutorPlus ===============*/

    @Override
    public int getCorePoolSize()
    {
        return 1;
    }
    @Override
    public void setCorePoolSize(int number)
    {
        throw new UnsupportedOperationException();
    }
    @Override
    public int getMaximumPoolSize()
    {
        return 1;
    }
    @Override
    public void setMaximumPoolSize(int number)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxTasksQueued()
    {
        return Integer.MAX_VALUE;
    }
}
