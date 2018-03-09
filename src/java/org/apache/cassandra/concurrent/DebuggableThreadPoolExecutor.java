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

import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.tracing.Tracing.isTracing;

/**
 * This class encorporates some Executor best practices for Cassandra.  Most of the executors in the system
 * should use or extend this.  There are two main improvements over a vanilla TPE:
 *
 * - If a task throws an exception, the default uncaught exception handler will be invoked; if there is
 *   no such handler, the exception will be logged.
 * - MaximumPoolSize is not supported.  Here is what that means (quoting TPE javadoc):
 *
 *     If fewer than corePoolSize threads are running, the Executor always prefers adding a new thread rather than queuing.
 *     If corePoolSize or more threads are running, the Executor always prefers queuing a request rather than adding a new thread.
 *     If a request cannot be queued, a new thread is created unless this would exceed maximumPoolSize, in which case, the task will be rejected.
 *
 *   We don't want this last stage of creating new threads if the queue is full; it makes it needlessly difficult to
 *   reason about the system's behavior.  In other words, if DebuggableTPE has allocated our maximum number of (core)
 *   threads and the queue is full, we want the enqueuer to block.  But to allow the number of threads to drop if a
 *   stage is less busy, core thread timeout is enabled.
 */
public class DebuggableThreadPoolExecutor extends ThreadPoolExecutor implements LocalAwareExecutorService
{
    protected static final Logger logger = LoggerFactory.getLogger(DebuggableThreadPoolExecutor.class);
    public static final RejectedExecutionHandler blockingExecutionHandler = new RejectedExecutionHandler()
    {
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor)
        {
            ((DebuggableThreadPoolExecutor) executor).onInitialRejection(task);
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (true)
            {
                if (executor.isShutdown())
                {
                    ((DebuggableThreadPoolExecutor) executor).onFinalRejection(task);
                    throw new RejectedExecutionException("ThreadPoolExecutor has shut down");
                }
                try
                {
                    if (queue.offer(task, 1000, TimeUnit.MILLISECONDS))
                    {
                        ((DebuggableThreadPoolExecutor) executor).onFinalAccept(task);
                        break;
                    }
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
        }
    };

    public DebuggableThreadPoolExecutor(String threadPoolName, int priority)
    {
        this(1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName, priority));
    }

    public DebuggableThreadPoolExecutor(int corePoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> queue, ThreadFactory factory)
    {
        this(corePoolSize, corePoolSize, keepAliveTime, unit, queue, factory);
    }

    public DebuggableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        allowCoreThreadTimeOut(true);

        // block task submissions until queue has room.
        // this is fighting TPE's design a bit because TPE rejects if queue.offer reports a full queue.
        // we'll just override this with a handler that retries until it gets in.  ugly, but effective.
        // (there is an extensive analysis of the options here at
        //  http://today.java.net/pub/a/today/2008/10/23/creating-a-notifying-blocking-thread-pool-executor.html)
        this.setRejectedExecutionHandler(blockingExecutionHandler);
    }

    /**
     * Creates a thread pool that creates new threads as needed, but
     * will reuse previously constructed threads when they are
     * available.
     * @param threadPoolName the name of the threads created by this executor
     * @return The new DebuggableThreadPoolExecutor
     */
    public static DebuggableThreadPoolExecutor createCachedThreadpoolWithMaxSize(String threadPoolName)
    {
        return new DebuggableThreadPoolExecutor(0, Integer.MAX_VALUE,
                                                60L, TimeUnit.SECONDS,
                                                new SynchronousQueue<Runnable>(),
                                                new NamedThreadFactory(threadPoolName));
    }

    /**
     * Returns a ThreadPoolExecutor with a fixed number of threads.
     * When all threads are actively executing tasks, new tasks are queued.
     * If (most) threads are expected to be idle most of the time, prefer createWithMaxSize() instead.
     * @param threadPoolName the name of the threads created by this executor
     * @param size the fixed number of threads for this executor
     * @return the new DebuggableThreadPoolExecutor
     */
    public static DebuggableThreadPoolExecutor createWithFixedPoolSize(String threadPoolName, int size)
    {
        return createWithMaximumPoolSize(threadPoolName, size, Integer.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Returns a ThreadPoolExecutor with a fixed maximum number of threads, but whose
     * threads are terminated when idle for too long.
     * When all threads are actively executing tasks, new tasks are queued.
     * @param threadPoolName the name of the threads created by this executor
     * @param size the maximum number of threads for this executor
     * @param keepAliveTime the time an idle thread is kept alive before being terminated
     * @param unit tht time unit for {@code keepAliveTime}
     * @return the new DebuggableThreadPoolExecutor
     */
    public static DebuggableThreadPoolExecutor createWithMaximumPoolSize(String threadPoolName, int size, int keepAliveTime, TimeUnit unit)
    {
        return new DebuggableThreadPoolExecutor(size, Integer.MAX_VALUE, keepAliveTime, unit, new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName));
    }

    protected void onInitialRejection(Runnable task) {}
    protected void onFinalAccept(Runnable task) {}
    protected void onFinalRejection(Runnable task) {}

    public void execute(Runnable command, ExecutorLocals locals)
    {
        super.execute(locals == null || command instanceof LocalSessionWrapper
                      ? command
                      : new LocalSessionWrapper<Object>(command, locals));
    }

    public void maybeExecuteImmediately(Runnable command)
    {
        execute(command);
    }

    // execute does not call newTaskFor
    @Override
    public void execute(Runnable command)
    {
        super.execute(isTracing() && !(command instanceof LocalSessionWrapper)
                      ? new LocalSessionWrapper<Object>(Executors.callable(command, null))
                      : command);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T result)
    {
        if (isTracing() && !(runnable instanceof LocalSessionWrapper))
        {
            return new LocalSessionWrapper<T>(Executors.callable(runnable, result));
        }
        return super.newTaskFor(runnable, result);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable)
    {
        if (isTracing() && !(callable instanceof LocalSessionWrapper))
        {
            return new LocalSessionWrapper<T>(callable);
        }
        return super.newTaskFor(callable);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r, t);

        maybeResetTraceSessionWrapper(r);
        logExceptionsAfterExecute(r, t);
    }

    protected static void maybeResetTraceSessionWrapper(Runnable r)
    {
        if (r instanceof LocalSessionWrapper)
        {
            LocalSessionWrapper tsw = (LocalSessionWrapper) r;
            // we have to reset trace state as its presence is what denotes the current thread is tracing
            // and if left this thread might start tracing unrelated tasks
            tsw.reset();
        }
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r)
    {
        if (r instanceof LocalSessionWrapper)
            ((LocalSessionWrapper) r).setupContext();

        super.beforeExecute(t, r);
    }

    /**
     * Send @param t and any exception wrapped by @param r to the default uncaught exception handler,
     * or log them if none such is set up
     */
    public static void logExceptionsAfterExecute(Runnable r, Throwable t)
    {
        Throwable hiddenThrowable = extractThrowable(r);
        if (hiddenThrowable != null)
            handleOrLog(hiddenThrowable);

        // ThreadPoolExecutor will re-throw exceptions thrown by its Task (which will be seen by
        // the default uncaught exception handler) so we only need to do anything if that handler
        // isn't set up yet.
        if (t != null && Thread.getDefaultUncaughtExceptionHandler() == null)
            handleOrLog(t);
    }

    /**
     * Send @param t to the default uncaught exception handler, or log it if none such is set up
     */
    public static void handleOrLog(Throwable t)
    {
        if (Thread.getDefaultUncaughtExceptionHandler() == null)
            logger.error("Error in ThreadPoolExecutor", t);
        else
            Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
    }

    /**
     * @return any exception wrapped by @param runnable, i.e., if it is a FutureTask
     */
    public static Throwable extractThrowable(Runnable runnable)
    {
        // Check for exceptions wrapped by FutureTask.  We do this by calling get(), which will
        // cause it to throw any saved exception.
        //
        // Complicating things, calling get() on a ScheduledFutureTask will block until the task
        // is cancelled.  Hence, the extra isDone check beforehand.
        if ((runnable instanceof Future<?>) && ((Future<?>) runnable).isDone())
        {
            try
            {
                ((Future<?>) runnable).get();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (CancellationException e)
            {
                logger.trace("Task cancelled", e);
            }
            catch (ExecutionException e)
            {
                return e.getCause();
            }
        }

        return null;
    }

    /**
     * Used to wrap a Runnable or Callable passed to submit or execute so we can clone the ExecutorLocals and move
     * them into the worker thread.
     *
     * @param <T>
     */
    private static class LocalSessionWrapper<T> extends FutureTask<T>
    {
        private final ExecutorLocals locals;

        public LocalSessionWrapper(Callable<T> callable)
        {
            super(callable);
            locals = ExecutorLocals.create();
        }

        public LocalSessionWrapper(Runnable command, ExecutorLocals locals)
        {
            super(command, null);
            this.locals = locals;
        }

        private void setupContext()
        {
            ExecutorLocals.set(locals);
        }

        private void reset()
        {
            ExecutorLocals.set(null);
        }
    }
}
