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

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * A {@link ThreadPoolExecutorBase} adapter to expose it via JMX.
 * The executor is not itself modified to maximise code re-use.
 * Only its rejected execution handler is updated, and a shutdown listener is registered.
 */
@VisibleForTesting
public class ThreadPoolExecutorJMXAdapter implements Runnable, ResizableThreadPoolMXBean
{
    /**
     * A builder wrapper that delegates all methods except {@link Builder#build()}
     * @param <E>
     */
    static class Builder<E extends ThreadPoolExecutorBase> implements ExecutorBuilder<E>
    {
        final ExecutorBuilder<E> wrapped;
        final String jmxPath;
        Builder(ExecutorBuilder<E> wrapped, String jmxPath)
        {
            this.wrapped = wrapped;
            this.jmxPath = jmxPath;
        }

        @Override
        public ExecutorBuilder<E> withKeepAlive(long keepAlive, TimeUnit keepAliveUnits)
        {
            wrapped.withKeepAlive(keepAlive, keepAliveUnits);
            return this;
        }

        @Override
        public ExecutorBuilder<E> withKeepAlive()
        {
            wrapped.withKeepAlive();
            return this;
        }

        @Override
        public ExecutorBuilder<E> withThreadPriority(int threadPriority)
        {
            wrapped.withThreadPriority(threadPriority);
            return this;
        }

        @Override
        public ExecutorBuilder<E> withQueueLimit(int queueLimit)
        {
            wrapped.withQueueLimit(queueLimit);
            return this;
        }

        @Override
        public ExecutorBuilder<E> withThreadGroup(ThreadGroup threadGroup)
        {
            wrapped.withThreadGroup(threadGroup);
            return this;
        }

        @Override
        public ExecutorBuilder<E> withDefaultThreadGroup()
        {
            wrapped.withDefaultThreadGroup();
            return this;
        }

        @Override
        public ExecutorBuilder<E> withRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler)
        {
            wrapped.withRejectedExecutionHandler(rejectedExecutionHandler);
            return this;
        }

        @Override
        public ExecutorBuilder<E> withUncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
        {
            wrapped.withUncaughtExceptionHandler(uncaughtExceptionHandler);
            return this;
        }

        /**
         * Invoke {@link ExecutorBuilder#build()} on {@link #wrapped}, and register the resultant
         * {@link ThreadPoolExecutorBase} with a new {@link ThreadPoolExecutorJMXAdapter}.
         *
         * The executor constructed by {@link #wrapped} is returned.
         */
        @Override
        public E build()
        {
            E result = wrapped.build();
            register(jmxPath, result);
            return result;
        }
    }

    public static void register(String jmxPath, ThreadPoolExecutorBase executor)
    {
        new ThreadPoolExecutorJMXAdapter(jmxPath, executor);
    }

    final String mbeanName;
    final ThreadPoolExecutorBase executor;
    final ThreadPoolMetrics metrics;
    boolean released;

    private ThreadPoolExecutorJMXAdapter(String jmxPath, ThreadPoolExecutorBase executor)
    {
        this.executor = executor;
        this.mbeanName = "org.apache.cassandra." + jmxPath + ":type=" + executor.getThreadFactory().id;
        this.metrics = new ThreadPoolMetrics(executor, jmxPath, executor.getThreadFactory().id).register();
        executor.setRejectedExecutionHandler(rejectedExecutionHandler(metrics, executor.getRejectedExecutionHandler()));
        MBeanWrapper.instance.registerMBean(this, mbeanName);
        executor.onShutdown(this);
    }

    @Override
    public synchronized void run()
    {
        if (released)
            return;

        MBeanWrapper.instance.unregisterMBean(mbeanName);
        metrics.release();
        released = true;
    }

    public ThreadPoolMetrics metrics()
    {
        return metrics;
    }

    @Override
    public int getActiveTaskCount()
    {
        return executor.getActiveTaskCount();
    }

    @Override
    public int getPendingTaskCount()
    {
        return executor.getPendingTaskCount();
    }

    @Override
    public int getCoreThreads()
    {
        return executor.getCoreThreads();
    }

    @Override
    public void setCoreThreads(int number)
    {
        executor.setCoreThreads(number);
    }

    @Override
    public int getMaximumThreads()
    {
        return executor.getMaximumThreads();
    }

    @Override
    public void setMaximumThreads(int number)
    {
        executor.setMaximumThreads(number);
    }

    @Override
    public void setCorePoolSize(int corePoolSize)
    {
        executor.setCorePoolSize(corePoolSize);
    }

    @Override
    public int getCorePoolSize()
    {
        return executor.getCorePoolSize();
    }

    @Override
    public void setMaximumPoolSize(int maximumPoolSize)
    {
        executor.setMaximumPoolSize(maximumPoolSize);
    }

    @Override
    public int getMaximumPoolSize()
    {
        return executor.getMaximumPoolSize();
    }

    @Override
    public long getCompletedTaskCount()
    {
        return executor.getCompletedTaskCount();
    }

    @Override
    public int getMaxTasksQueued()
    {
        return executor.getMaxTasksQueued();
    }

    static RejectedExecutionHandler rejectedExecutionHandler(ThreadPoolMetrics metrics, RejectedExecutionHandler wrap)
    {
        return (task, executor) ->
        {
            metrics.totalBlocked.inc();
            metrics.currentBlocked.inc();
            try
            {
                wrap.rejectedExecution(task, executor);
            }
            finally
            {
                metrics.currentBlocked.dec();
            }
        };
    }
}
