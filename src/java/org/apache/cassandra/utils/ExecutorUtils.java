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

package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.concurrent.InfiniteLoopExecutor;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExecutorUtils
{

    public static Runnable runWithThreadName(Runnable runnable, String threadName)
    {
        return () -> {
            String oldThreadName = Thread.currentThread().getName();
            try
            {
                Thread.currentThread().setName(threadName);
                runnable.run();
            }
            finally
            {
                Thread.currentThread().setName(oldThreadName);
            }
        };
    }

    public static void shutdownNow(Iterable<?> executors)
    {
        shutdown(true, executors);
    }

    public static void shutdown(Iterable<?> executors)
    {
        shutdown(false, executors);
    }

    public static void shutdown(boolean interrupt, Iterable<?> executors)
    {
        for (Object executor : executors)
        {
            if (executor instanceof ExecutorService)
            {
                if (interrupt) ((ExecutorService) executor).shutdownNow();
                else ((ExecutorService) executor).shutdown();
            }
            else if (executor instanceof InfiniteLoopExecutor)
                ((InfiniteLoopExecutor) executor).shutdownNow();
            else if (executor instanceof Thread)
                ((Thread) executor).interrupt();
            else if (executor != null)
                throw new IllegalArgumentException(executor.toString());
        }
    }

    public static void shutdown(ExecutorService ... executors)
    {
        shutdown(Arrays.asList(executors));
    }

    public static void shutdownNow(ExecutorService ... executors)
    {
        shutdownNow(Arrays.asList(executors));
    }

    public static void awaitTermination(long timeout, TimeUnit unit, ExecutorService ... executors) throws InterruptedException, TimeoutException
    {
        awaitTermination(timeout, unit, Arrays.asList(executors));
    }

    public static void awaitTermination(long timeout, TimeUnit unit, Collection<?> executors) throws InterruptedException, TimeoutException
    {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        awaitTerminationUntil(deadline, executors);
    }

    public static void awaitTerminationUntil(long deadline, Collection<?> executors) throws InterruptedException, TimeoutException
    {
        for (Object executor : executors)
        {
            long wait = deadline - System.nanoTime();
            if (executor instanceof ExecutorService)
            {
                if (wait <= 0 || !((ExecutorService)executor).awaitTermination(wait, NANOSECONDS))
                    throw new TimeoutException(executor + " did not terminate on time");
            }
            else if (executor instanceof InfiniteLoopExecutor)
            {
                if (wait <= 0 || !((InfiniteLoopExecutor)executor).awaitTermination(wait, NANOSECONDS))
                    throw new TimeoutException(executor + " did not terminate on time");
            }
            else if (executor instanceof Thread)
            {
                Thread t = (Thread) executor;
                if (wait <= 0)
                    throw new TimeoutException(executor + " did not terminate on time");
                t.join((wait + 999999) / 1000000L, (int) (wait % 1000000L));
                if (t.isAlive())
                    throw new TimeoutException(executor + " did not terminate on time");
            }
            else if (executor != null)
            {
                throw new IllegalArgumentException(executor.toString());
            }
        }
    }

    public static void shutdownAndWait(long timeout, TimeUnit unit, Collection<?> executors) throws TimeoutException, InterruptedException
    {
        shutdown(executors);
        awaitTermination(timeout, unit, executors);
    }

    public static void shutdownNowAndWait(long timeout, TimeUnit unit, Collection<?> executors) throws TimeoutException, InterruptedException
    {
        shutdownNow(executors);
        awaitTermination(timeout, unit, executors);
    }

    public static void shutdownAndWait(long timeout, TimeUnit unit, Object ... executors) throws TimeoutException, InterruptedException
    {
        shutdownAndWait(timeout, unit, Arrays.asList(executors));
    }

    public static void shutdownNowAndWait(long timeout, TimeUnit unit, Object ... executors) throws TimeoutException, InterruptedException
    {
        shutdownNowAndWait(timeout, unit, Arrays.asList(executors));
    }
}