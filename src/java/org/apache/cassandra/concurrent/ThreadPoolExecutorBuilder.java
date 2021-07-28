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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.cassandra.concurrent.NamedThreadFactory.MetaFactory;

import static java.lang.Thread.NORM_PRIORITY;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;

/**
 * Configure a {@link ThreadPoolExecutorPlus}, applying Cassandra's best practices by default
 * <li>Core threads may timeout, and use a default {@link #keepAlive} time in {@link #keepAliveUnits}
 * <li>Threads share the same {@link ThreadGroup}, which may be configurably a child of a specified {@link ThreadGroup}
 *     descended from the same parent of the {@link MetaFactory}
 * <li>By default queues are unbounded in length
 * <li>The default {@link RejectedExecutionHandler} is implementation dependent, but may be overridden
 * <li>The default {@link UncaughtExceptionHandler} is inherited from {@link MetaFactory}, which in turn receives it
 *     from the {@link ExecutorBuilderFactory}
 */
public class ThreadPoolExecutorBuilder<E extends ExecutorPlus> extends MetaFactory implements ExecutorBuilder<E>
{
    static <E extends SequentialExecutorPlus> ExecutorBuilder<E> sequential(Function<ThreadPoolExecutorBuilder<E>, E> constructor, ClassLoader contextClassLoader, ThreadGroup threadGroup, UncaughtExceptionHandler uncaughtExceptionHandler, String name)
    {
        ThreadPoolExecutorBuilder<E> result = new ThreadPoolExecutorBuilder<>(constructor, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, 1);
        result.withKeepAlive();
        return result;
    }

    static <E extends SingleThreadExecutorPlus> ExecutorBuilder<E> sequentialJmx(Function<ThreadPoolExecutorBuilder<E>, E> constructor, ClassLoader contextClassLoader, ThreadGroup threadGroup, UncaughtExceptionHandler uncaughtExceptionHandler, String name, String jmxPath)
    {
        return new ThreadPoolExecutorJMXAdapter.Builder<>(sequential(constructor, contextClassLoader, threadGroup, uncaughtExceptionHandler, name), jmxPath);
    }

    static <E extends ExecutorPlus> ExecutorBuilder<E> pooled(Function<ThreadPoolExecutorBuilder<E>, E> constructor, ClassLoader contextClassLoader, ThreadGroup threadGroup, UncaughtExceptionHandler uncaughtExceptionHandler, String name, int threads)
    {
        return new ThreadPoolExecutorBuilder<>(constructor, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, threads);
    }

    static <E extends ThreadPoolExecutorPlus> ExecutorBuilder<E> pooledJmx(Function<ThreadPoolExecutorBuilder<E>, E> constructor, ClassLoader contextClassLoader, ThreadGroup threadGroup, UncaughtExceptionHandler uncaughtExceptionHandler, String name, int threads, String jmxPath)
    {
        return new ThreadPoolExecutorJMXAdapter.Builder<>(pooled(constructor, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, threads), jmxPath);
    }

    private final Function<ThreadPoolExecutorBuilder<E>, E> constructor;
    private final String name;
    private final int threads;
    private int threadPriority = NORM_PRIORITY;
    private Integer queueLimit;

    private long keepAlive = 1;
    private TimeUnit keepAliveUnits = MINUTES;
    private boolean allowCoreThreadTimeouts = true;

    private RejectedExecutionHandler rejectedExecutionHandler = null;

    protected ThreadPoolExecutorBuilder(Function<ThreadPoolExecutorBuilder<E>, E> constructor, ClassLoader contextClassLoader, ThreadGroup overrideThreadGroup, UncaughtExceptionHandler uncaughtExceptionHandler, String name, int threads)
    {
        super(contextClassLoader, overrideThreadGroup, uncaughtExceptionHandler);
        this.constructor = constructor;
        this.name = name;
        this.threads = threads;
    }

    // core and non-core threads will die after this period of inactivity
    public ThreadPoolExecutorBuilder<E> withKeepAlive(long keepAlive, TimeUnit keepAliveUnits)
    {
        this.allowCoreThreadTimeouts = true;
        this.keepAlive = keepAlive;
        this.keepAliveUnits = keepAliveUnits;
        return this;
    }

    // once started, core threads will never die
    public ThreadPoolExecutorBuilder<E> withKeepAlive()
    {
        this.allowCoreThreadTimeouts = false;
        return this;
    }

    public ThreadPoolExecutorBuilder<E> withThreadPriority(int threadPriority)
    {
        this.threadPriority = threadPriority;
        return this;
    }

    @Override
    public ExecutorBuilder<E> withThreadGroup(ThreadGroup threadGroup)
    {
        ThreadGroup current = this.threadGroup;

        ThreadGroup parent = threadGroup;
        while (parent != null && parent != current)
            parent = parent.getParent();
        if (parent != current)
            throw new IllegalArgumentException("threadGroup may only be overridden with a child of the default threadGroup");

        this.threadGroup = threadGroup;
        return this;
    }

    @Override
    public ExecutorBuilder<E> withDefaultThreadGroup()
    {
        this.threadGroup = null;
        return this;
    }

    public ThreadPoolExecutorBuilder<E> withQueueLimit(int queueLimit)
    {
        this.queueLimit = queueLimit;
        return this;
    }

    public ThreadPoolExecutorBuilder<E> withRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler)
    {
        this.rejectedExecutionHandler = rejectedExecutionHandler;
        return this;
    }

    public ThreadPoolExecutorBuilder<E> withUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler)
    {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        return this;
    }

    @Override
    public E build()
    {
        return constructor.apply(this);
    }

    NamedThreadFactory newThreadFactory()
    {
        return newThreadFactory(name, threadPriority);
    }

    BlockingQueue<Runnable> newQueue()
    {
        // if our pool can have an infinite number of threads, there is no point having an infinite queue length
        int size = queueLimit != null
                ? queueLimit
                : threads == Integer.MAX_VALUE
                    ? 0 : Integer.MAX_VALUE;
        return newBlockingQueue(size);
    }

    /**
     * If our queue blocks on/rejects all submissions, we can configure our core pool size to 0,
     * as new threads will always be created for new work, and core threads timeout at the same
     * rate as non-core threads.
     */
    int coreThreads()
    {
        return (queueLimit != null && queueLimit == 0) || threads == Integer.MAX_VALUE ? 0 : threads;
    }

    int maxThreads()
    {
        return threads;
    }

    RejectedExecutionHandler rejectedExecutionHandler(RejectedExecutionHandler ifNotSet)
    {
        return rejectedExecutionHandler == null ? ifNotSet : rejectedExecutionHandler;
    }

    long keepAlive()
    {
        return keepAlive;
    }

    TimeUnit keepAliveUnits()
    {
        return keepAliveUnits;
    }

    boolean allowCoreThreadTimeouts()
    {
        return allowCoreThreadTimeouts;
    }
}