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

import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

/**
 * This class incorporates some Executor best practices for Cassandra.  Most of the executors in the system
 * should use or extend {@link ThreadPoolExecutorPlus}, or in rare exceptions this class.
 *
 * This class provides some very basic improvements:
 * <li>We are configured by {@link ThreadPoolExecutorBuilder}
 * <li>Tasks rejected due to overflow of the queue block the submitting thread rather than throwing {@link RejectedExecutionException}
 * <li>{@link RunnableFuture} rejected due to executor shutdown will be cancelled
 * <li>{@link RunnableFuture} removed by {@link #shutdownNow()} will be cancelled
 *
 * We also provide a shutdown hook for JMX registration cleanup.
 */
public class ThreadPoolExecutorBase extends ThreadPoolExecutor implements ResizableThreadPool
{
    public static final RejectedExecutionHandler blockingExecutionHandler = (task, executor) ->
    {
        BlockingQueue<Runnable> queue = executor.getQueue();
        try
        {
            while (true)
            {
                try
                {
                    if (executor.isShutdown())
                        throw new RejectedExecutionException(executor + " has shut down");

                    if (queue.offer(task, 1, TimeUnit.SECONDS))
                        break;
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
            }
        }
        catch (Throwable t)
        {
            //Give some notification to the caller the task isn't going to run
            if (task instanceof java.util.concurrent.Future)
                ((java.util.concurrent.Future<?>) task).cancel(false);
            throw t;
        }
    };

    private Runnable onShutdown;

    // maximumPoolSize is only used when corePoolSize == 0
    // if keepAliveTime < 0 and unit == null, we forbid core thread timeouts (e.g. single threaded executors by default)
    public ThreadPoolExecutorBase(ThreadPoolExecutorBuilder<?> builder)
    {
        super(builder.coreThreads(), builder.maxThreads(), builder.keepAlive(), builder.keepAliveUnits(), builder.newQueue(), builder.newThreadFactory());
        allowCoreThreadTimeOut(builder.allowCoreThreadTimeouts());

        // block task submissions until queue has room.
        // this is fighting TPE's design a bit because TPE rejects if queue.offer reports a full queue.
        // we'll just override this with a handler that retries until it gets in.  ugly, but effective.
        // (there is an extensive analysis of the options here at
        //  http://today.java.net/pub/a/today/2008/10/23/creating-a-notifying-blocking-thread-pool-executor.html)
        setRejectedExecutionHandler(builder.rejectedExecutionHandler(blockingExecutionHandler));
    }

    // no RejectedExecutionHandler
    public ThreadPoolExecutorBase(int threads, int keepAlive, TimeUnit keepAliveUnits, BlockingQueue<Runnable> queue, NamedThreadFactory threadFactory)
    {
        super(threads, threads, keepAlive, keepAliveUnits, queue, threadFactory);
        assert queue.isEmpty() : "Executor initialized with non-empty task queue";
        allowCoreThreadTimeOut(true);
    }

    public void onShutdown(Runnable onShutdown)
    {
        this.onShutdown = onShutdown;
    }

    public Runnable onShutdown()
    {
        return onShutdown;
    }

    @Override
    protected void terminated()
    {
        getThreadFactory().close();
    }

    @Override
    public void shutdown()
    {
        try
        {
            super.shutdown();
        }
        finally
        {
            if (onShutdown != null)
                onShutdown.run();
        }
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        try
        {
            List<Runnable> cancelled = super.shutdownNow();
            for (Runnable c : cancelled)
            {
                if (c instanceof java.util.concurrent.Future<?>)
                    ((java.util.concurrent.Future<?>) c).cancel(true);
            }
            return cancelled;
        }
        finally
        {
            if (onShutdown != null)
                onShutdown.run();
        }
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

    public int getCoreThreads()
    {
        return getCorePoolSize();
    }

    public void setCoreThreads(int number)
    {
        setCorePoolSize(number);
    }

    public int getMaximumThreads()
    {
        return getMaximumPoolSize();
    }

    public void setMaximumThreads(int number)
    {
        setMaximumPoolSize(number);
    }

    @Override
    public NamedThreadFactory getThreadFactory()
    {
        return (NamedThreadFactory) super.getThreadFactory();
    }

    public String toString()
    {
        return getThreadFactory().id;
    }
}
