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

package org.apache.cassandra.simulator.systems;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.concurrent.LocalAwareSequentialExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.SingleThreadExecutorPlus;
import org.apache.cassandra.concurrent.SyncFutureTask;
import org.apache.cassandra.concurrent.TaskFactory;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.NotifyThreadPaused.AwaitPaused;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Condition.Sync;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.RunnableFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.DAEMON;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_TASK;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_TIMEOUT;
import static org.apache.cassandra.simulator.systems.SimulatedExecution.callable;
import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

// An executor whose tasks we can intercept the execution of
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface InterceptingExecutor extends OrderOn
{
    class ForbiddenExecutionException extends RejectedExecutionException
    {
    }

    interface InterceptingTaskFactory extends TaskFactory
    {
    }

    void submitUnmanaged(Runnable task);
    void submitAndAwaitPause(Runnable task, InterceptorOfConsequences interceptor);

    // TODO: implement cancellation to integrate with ActionSchedule
    static class InterceptedScheduledFutureTask<T> extends SyncFutureTask<T> implements ScheduledFuture<T>
    {
        public InterceptedScheduledFutureTask(Callable<T> call)
        {
            super(call);
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return 0;
        }

        @Override
        public int compareTo(Delayed o)
        {
            return 0;
        }
    }

    @PerClassLoader
    abstract class AbstractInterceptingExecutor implements InterceptingExecutor, ExecutorPlus
    {
        final InterceptorOfExecution interceptSupplier;
        final InterceptingTaskFactory taskFactory;

        final Condition isTerminated = new Sync();
        volatile boolean isShutdown;

        protected AbstractInterceptingExecutor(InterceptorOfExecution interceptSupplier, InterceptingTaskFactory taskFactory)
        {
            this.interceptSupplier = interceptSupplier;
            this.taskFactory = taskFactory;
        }

        <V, T extends RunnableFuture<V>> T addTask(T task)
        {
            return interceptSupplier.intercept().addTask(task, this);
        }

        public void maybeExecuteImmediately(Runnable command)
        {
            execute(command);
        }

        @Override
        public void execute(Runnable run)
        {
            addTask(taskFactory.toSubmit(run));
        }

        @Override
        public void execute(WithResources withResources, Runnable run)
        {
            addTask(taskFactory.toSubmit(withResources, run));
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

        public boolean isShutdown()
        {
            return isShutdown;
        }

        public boolean isTerminated()
        {
            return isTerminated.isSignalled();
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return isTerminated.await(timeout, unit);
        }

        @Override
        public void setCorePoolSize(int newCorePoolSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMaximumPoolSize(int newMaximumPoolSize)
        {
            throw new UnsupportedOperationException();
        }
    }

    @PerClassLoader
    class InterceptingPooledExecutor extends AbstractInterceptingExecutor implements InterceptingExecutor
    {
        private class WaitingThread
        {
            final InterceptibleThread thread;
            Runnable task;

            WaitingThread(ThreadFactory factory)
            {
                this.thread = (InterceptibleThread) factory.newThread(() -> {
                    InterceptibleThread thread = (InterceptibleThread) Thread.currentThread();
                    try
                    {
                        while (true)
                        {
                            try { task.run(); }
                            catch (Throwable t) { thread.getUncaughtExceptionHandler().uncaughtException(thread, t); }
                            finally
                            {
                                synchronized (this)
                                {
                                    task = null;
                                    waiting.add(this);
                                    thread.interceptTermination();

                                    while (task == null)
                                    {
                                        if (isShutdown)
                                            return;

                                        try { wait(); }
                                        catch (InterruptedException | UncheckedInterruptedException ignore) { }
                                    }

                                    if (isShutdownNow)
                                        return;
                                }
                            }
                        }
                    }
                    finally
                    {
                        task = null;
                        waiting.remove(this);
                        threads.remove(thread);
                        //noinspection ConstantConditions - false positive
                        if (threads.isEmpty() && isShutdown && threads.isEmpty())
                            isTerminated.signal();
                    }
                });

                threads.add(thread);
            }

            synchronized void submit(Runnable task)
            {
                this.task = task;
                if (thread.isAlive()) notify();
                else thread.start();
            }
        }

        volatile boolean isShutdownNow;
        final Set<InterceptibleThread> threads = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
        final ThreadFactory threadFactory;
        final Queue<WaitingThread> waiting = new ConcurrentLinkedQueue<>();
        final int concurrency;

        InterceptingPooledExecutor(InterceptorOfExecution interceptSupplier, int concurrency, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptSupplier, taskFactory);
            this.threadFactory = threadFactory;
            this.concurrency = concurrency;
        }

        public void submitAndAwaitPause(Runnable task, InterceptorOfConsequences interceptor)
        {
            if (isShutdown)
                throw new RejectedExecutionException();
            WaitingThread waiting = getWaiting();
            AwaitPaused done = new AwaitPaused(waiting);
            waiting.thread.beforeInvocation(interceptor, done);
            synchronized (waiting)
            {
                waiting.submit(task);
                done.awaitPause();
            }
        }

        public void submitUnmanaged(Runnable task)
        {
            if (isShutdown)
                throw new RejectedExecutionException();
            WaitingThread waiting = getWaiting();
            waiting.submit(task);
        }

        private WaitingThread getWaiting()
        {
            WaitingThread next = waiting.poll();
            if (next != null)
                return next;

            return new WaitingThread(threadFactory);
        }

        public void shutdown()
        {
            isShutdown = true;
            if (threads.isEmpty() && waiting.isEmpty())
                isTerminated.signal();
            waiting.forEach(waiting -> waiting.thread.interrupt());
        }

        public List<Runnable> shutdownNow()
        {
            isShutdownNow = true;
            shutdown();
            threads.forEach(InterceptibleThread::interrupt);
            return Collections.emptyList();
        }

        @Override
        public boolean inExecutor()
        {
            return threads.contains(Thread.currentThread());
        }

        @Override
        public int getActiveTaskCount()
        {
            return threads.size() - waiting.size();
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

        @Override
        public int getCorePoolSize()
        {
            return concurrency;
        }

        @Override
        public int getMaximumPoolSize()
        {
            return concurrency;
        }

        public String toString()
        {
            return threadFactory.toString();
        }

        @Override
        public int concurrency()
        {
            return concurrency;
        }
    }

    // we might want different variants
    // (did consider a non-intercepting variant, or immediate executor, but we need to intercept the thread events)
    @PerClassLoader
    abstract class AbstractSingleThreadedExecutorPlus extends AbstractInterceptingExecutor implements SequentialExecutorPlus
    {
        private static final Runnable DONE = () -> {};

        volatile boolean executing;

        static class AtLeastOnce extends SingleThreadExecutorPlus.AtLeastOnce
        {
            AtLeastOnce(SequentialExecutorPlus executor, Runnable run)
            {
                super(executor, run);
            }

            public boolean trigger()
            {
                boolean success;
                if (success = compareAndSet(false, true))
                    executor.execute(this);
                else
                    executor.execute(() -> {}); // submit a no-op, so we can still impose our causality orderings
                return success;
            }
        }

        final InterceptibleThread thread;
        final ArrayDeque<Runnable> queue = new ArrayDeque<>();

        AbstractSingleThreadedExecutorPlus(InterceptorOfExecution interceptSupplier, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptSupplier, taskFactory);
            this.thread = (InterceptibleThread) threadFactory.newThread(() -> {
                InterceptibleThread thread = (InterceptibleThread) Thread.currentThread();
                try
                {
                    while (true)
                    {
                        try
                        {
                            Runnable run = dequeue();
                            if (DONE == run)
                                return;

                            run.run();
                        }
                        catch (InterruptedException | UncheckedInterruptedException ignore)
                        {
                        }
                        catch (Throwable t)
                        {
                            thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                        }
                        finally
                        {
                            executing = false;
                            thread.interceptTermination();
                        }
                    }
                }
                finally
                {
                    isTerminated.signal();
                    queue.clear();
                }
            });
            thread.start();
        }

        public void shutdown()
        {
            isShutdown = true;
            enqueue(DONE);
        }

        public List<Runnable> shutdownNow()
        {
            isShutdown = true;
            List<Runnable> cancelled = new ArrayList<>(queue);
            queue.clear();
            enqueue(DONE);
            return cancelled;
        }

        synchronized void enqueue(Runnable runnable)
        {
            queue.add(runnable);
            notify();
        }

        synchronized Runnable dequeue() throws InterruptedException
        {
            while (queue.isEmpty())
                wait();
            return queue.poll();
        }

        public AtLeastOnce atLeastOnceTrigger(Runnable run)
        {
            return new AtLeastOnce(this, run);
        }

        @Override
        public boolean inExecutor()
        {
            return thread == Thread.currentThread();
        }

        @Override
        public int getCorePoolSize()
        {
            return 1;
        }

        @Override
        public int getMaximumPoolSize()
        {
            return 1;
        }

        public String toString()
        {
            return thread.toString();
        }
    }

    @PerClassLoader
    class InterceptingSequentialExecutor extends AbstractSingleThreadedExecutorPlus implements InterceptingExecutor, ScheduledExecutorPlus, OrderOn
    {
        InterceptingSequentialExecutor(InterceptorOfExecution interceptSupplier, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptSupplier, threadFactory, taskFactory);
        }

        public void submitAndAwaitPause(Runnable task, InterceptorOfConsequences interceptor)
        {
            synchronized (this)
            {
                if (isShutdown)
                    throw new RejectedExecutionException();

                if (executing)
                    throw new AssertionError();
                executing = true;

                AwaitPaused done = new AwaitPaused(this);
                thread.beforeInvocation(interceptor, done);
                enqueue(task);
                done.awaitPause();
            }
        }

        public synchronized void submitUnmanaged(Runnable task)
        {
            if (isShutdown)
                throw new RejectedExecutionException();
            enqueue(task);
        }

        @Override public int getActiveTaskCount()
        {
            return !queue.isEmpty() || executing ? 1 : 0;
        }

        @Override public long getCompletedTaskCount()
        {
            return 0;
        }

        @Override
        public int getPendingTaskCount()
        {
            return 0;
        }

        public ScheduledFuture<?> schedule(Runnable run, long delay, TimeUnit unit)
        {
            return interceptSupplier.intercept().schedule(SCHEDULED_TASK, callable(run, null), this);
        }

        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            return interceptSupplier.intercept().schedule(SCHEDULED_TASK, callable, this);
        }

        public ScheduledFuture<?> scheduleTimeoutWithDelay(Runnable run, long delay, TimeUnit unit)
        {
            return scheduleTimeoutAt(run, 0);
        }

        public ScheduledFuture<?> scheduleTimeoutAt(Runnable run, long deadlineNanos)
        {
            return interceptSupplier.intercept().schedule(SCHEDULED_TIMEOUT, callable(run, null), this);
        }

        public ScheduledFuture<?> scheduleAtFixedRate(Runnable run, long initialDelay, long period, TimeUnit unit)
        {
            return interceptSupplier.intercept().schedule(DAEMON, () -> {
                run.run();
                scheduleAtFixedRate(run, initialDelay, period, unit);
                return null;
            }, this);
        }

        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable run, long initialDelay, long delay, TimeUnit unit)
        {
            return scheduleAtFixedRate(run, initialDelay, delay, unit);
        }

        public int concurrency()
        {
            return 1;
        }
    }

    @PerClassLoader
    class InterceptingPooledLocalAwareExecutor extends InterceptingPooledExecutor implements LocalAwareExecutorPlus
    {
        InterceptingPooledLocalAwareExecutor(InterceptorOfExecution interceptors, int concurrency, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptors, concurrency, threadFactory, taskFactory);
        }
    }

    @PerClassLoader
    class InterceptingLocalAwareSequentialExecutor extends InterceptingSequentialExecutor implements LocalAwareSequentialExecutorPlus
    {
        InterceptingLocalAwareSequentialExecutor(InterceptorOfExecution interceptSupplier, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptSupplier, threadFactory, taskFactory);
        }
    }
}
