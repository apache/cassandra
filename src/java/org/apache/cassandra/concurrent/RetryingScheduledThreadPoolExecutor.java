package org.apache.cassandra.concurrent;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.concurrent.*;

public class RetryingScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
{
    public RetryingScheduledThreadPoolExecutor(String threadPoolName, int priority)
    {
        this(1, threadPoolName, priority);
    }

    public RetryingScheduledThreadPoolExecutor(int corePoolSize, String threadPoolName, int priority)
    {
        super(corePoolSize, new NamedThreadFactory(threadPoolName, priority));
    }

    public RetryingScheduledThreadPoolExecutor(String threadPoolName)
    {
        this(1, threadPoolName, Thread.NORM_PRIORITY);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task)
    {
        return new LoggingScheduledFuture<V>(task);
    }

    /**
     * Wraps RunnableScheduledFuture.run to log an error on exception rather than kill the executor thread.
     * All the other methods just wrap the RSF counterpart.
     * @param <V>
     */
    private static class LoggingScheduledFuture<V> implements RunnableScheduledFuture<V>
    {
        private final RunnableScheduledFuture<V> task;

        public LoggingScheduledFuture(RunnableScheduledFuture<V> task)
        {
            this.task = task;
        }

        public boolean isPeriodic()
        {
            return task.isPeriodic();
        }

        public long getDelay(TimeUnit unit)
        {
            return task.getDelay(unit);
        }

        public int compareTo(Delayed o)
        {
            return task.compareTo(o);
        }

        public void run()
        {
            try
            {
                task.run();
            }
            catch (Exception e)
            {
                if (Thread.getDefaultUncaughtExceptionHandler() != null)
                    Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e.getCause());
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            return task.cancel(mayInterruptIfRunning);
        }

        public boolean isCancelled()
        {
            return task.isCancelled();
        }

        public boolean isDone()
        {
            return task.isDone();
        }

        public V get() throws InterruptedException, ExecutionException
        {
            return task.get();
        }

        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return task.get(timeout, unit);
        }
    }
}
