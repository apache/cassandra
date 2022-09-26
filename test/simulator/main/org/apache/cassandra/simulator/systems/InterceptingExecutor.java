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
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.concurrent.LocalAwareSequentialExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.SingleThreadExecutorPlus;
import org.apache.cassandra.concurrent.SyncFutureTask;
import org.apache.cassandra.concurrent.TaskFactory;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.InterceptingAwaitable.InterceptingCondition;
import org.apache.cassandra.simulator.systems.NotifyThreadPaused.AwaitPaused;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.NotScheduledFuture;
import org.apache.cassandra.utils.concurrent.RunnableFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedMap;
import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SIMULATOR_DEBUG;
import static org.apache.cassandra.simulator.systems.InterceptibleThread.runDeterministic;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_DAEMON;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_TASK;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_TIMEOUT;
import static org.apache.cassandra.simulator.systems.SimulatedExecution.callable;
import static org.apache.cassandra.simulator.systems.SimulatedTime.Global.localToRelativeNanos;
import static org.apache.cassandra.simulator.systems.SimulatedTime.Global.localToGlobalNanos;
import static org.apache.cassandra.simulator.systems.SimulatedTime.Global.relativeToGlobalNanos;
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

    void addPending(Object task);
    void cancelPending(Object task);
    void submitUnmanaged(Runnable task);
    void submitAndAwaitPause(Runnable task, InterceptorOfConsequences interceptor);

    OrderOn orderAppliesAfterScheduling();

    static class InterceptedScheduledFutureTask<T> extends SyncFutureTask<T> implements ScheduledFuture<T>
    {
        final long delayNanos;
        Runnable onCancel;
        public InterceptedScheduledFutureTask(long delayNanos, Callable<T> call)
        {
            super(call);
            this.delayNanos = delayNanos;
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(delayNanos, NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed that)
        {
            return Long.compare(delayNanos, that.getDelay(NANOSECONDS));
        }

        void onCancel(Runnable onCancel)
        {
            this.onCancel = onCancel;
        }

        @Override
        public boolean cancel(boolean b)
        {
            if (onCancel != null)
            {
                onCancel.run();
                onCancel = null;
            }
            return super.cancel(b);
        }
    }

    @PerClassLoader
    abstract class AbstractInterceptingExecutor implements InterceptingExecutor, ExecutorPlus
    {
        private static final AtomicIntegerFieldUpdater<AbstractInterceptingExecutor> pendingUpdater = AtomicIntegerFieldUpdater.newUpdater(AbstractInterceptingExecutor.class, "pending");

        final OrderAppliesAfterScheduling orderAppliesAfterScheduling = new OrderAppliesAfterScheduling(this);

        final InterceptorOfExecution interceptorOfExecution;
        final InterceptingTaskFactory taskFactory;

        final Set<Object> debugPending = TEST_SIMULATOR_DEBUG.getBoolean() ? synchronizedSet(newSetFromMap(new IdentityHashMap<>())) : null;
        final Condition isTerminated;
        volatile boolean isShutdown;
        volatile int pending;

        protected AbstractInterceptingExecutor(InterceptorOfExecution interceptorOfExecution, InterceptingTaskFactory taskFactory)
        {
            this.interceptorOfExecution = interceptorOfExecution;
            this.isTerminated = new InterceptingCondition();
            this.taskFactory = taskFactory;
        }

        @Override
        public void addPending(Object task)
        {
            if (isShutdown)
                throw new RejectedExecutionException();

            pendingUpdater.incrementAndGet(this);
            if (isShutdown)
            {
                if (0 == pendingUpdater.decrementAndGet(this))
                    terminate();
                throw new RejectedExecutionException();
            }

            if (debugPending != null && !debugPending.add(task))
                throw new AssertionError();
        }

        @Override
        public void cancelPending(Object task)
        {
            boolean shutdown = isShutdown;
            if (completePending(task) == 0 && shutdown)
                terminate();
        }

        @Override
        public OrderOn orderAppliesAfterScheduling()
        {
            return orderAppliesAfterScheduling;
        }

        public int completePending(Object task)
        {
            int remaining = pendingUpdater.decrementAndGet(this);
            if (debugPending != null && !debugPending.remove(task))
                throw new AssertionError();
            return remaining;
        }

        <V, T extends RunnableFuture<V>> T addTask(T task)
        {
            if (isShutdown)
                throw new RejectedExecutionException();

            return interceptorOfExecution.intercept().addTask(task, this);
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

        abstract void terminate();

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
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                InterceptibleThread interceptibleThread = (InterceptibleThread) thread;
                if (interceptibleThread.isIntercepting())
                {
                    // simpler to use no timeout than to ensure pending tasks all run first in simulation
                    isTerminated.await();
                    return true;
                }
            }
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
        enum State { RUNNING, TERMINATING, TERMINATED }

        private class WaitingThread
        {
            final InterceptibleThread thread;
            Runnable task;
            State state = State.RUNNING;

            WaitingThread(ThreadFactory factory)
            {
                this.thread = (InterceptibleThread) factory.newThread(() -> {
                    InterceptibleThread thread = (InterceptibleThread) Thread.currentThread();
                    try
                    {
                        while (true)
                        {
                            try
                            {
                                task.run();
                            }
                            catch (Throwable t)
                            {
                                try { thread.getUncaughtExceptionHandler().uncaughtException(thread, t); }
                                catch (Throwable ignore) {}
                            }

                            boolean shutdown = isShutdown;
                            int remaining = completePending(task);
                            if (shutdown && remaining < threads.size())
                            {
                                threads.remove(thread);
                                thread.onTermination();
                                if (threads.isEmpty())
                                    isTerminated.signal(); // this has simulator side-effects, so try to perform before we interceptTermination
                                thread.interceptTermination(true);
                                return;
                            }

                            task = null;
                            waiting.add(this); // inverse order of waiting.add/isShutdown to ensure visibility vs shutdown()
                            thread.interceptTermination(false);
                            synchronized (this)
                            {
                                while (task == null)
                                {
                                    if (state == State.TERMINATING)
                                        return;

                                    try { wait(); }
                                    catch (InterruptedException | UncheckedInterruptedException ignore) { }
                                }
                            }
                        }
                    }
                    finally
                    {
                        try
                        {
                            runDeterministic(() -> {
                                if (null != threads.remove(thread))
                                {
                                    task = null;
                                    waiting.remove(this);
                                    thread.onTermination();
                                    if (isShutdown && threads.isEmpty() && waiting.isEmpty() && !isTerminated())
                                        isTerminated.signal();
                                }
                            });
                        }
                        finally
                        {
                            synchronized (this)
                            {
                                state = State.TERMINATED;
                                notify();
                            }
                        }
                    }
                });

                threads.put(thread, this);
            }

            synchronized void submit(Runnable task)
            {
                if (state != State.RUNNING)
                    throw new IllegalStateException();
                this.task = task;
                if (thread.isAlive()) notify();
                else thread.start();
            }

            synchronized void terminate()
            {
                if (state != State.TERMINATED)
                    state = State.TERMINATING;

                if (thread.isAlive()) notify();
                else thread.start();
                try
                {
                    while (state != State.TERMINATED)
                        wait();
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
            }
        }

        final Map<InterceptibleThread, WaitingThread> threads = synchronizedMap(new IdentityHashMap<>());
        final ThreadFactory threadFactory;
        final Queue<WaitingThread> waiting = new ConcurrentLinkedQueue<>();
        final int concurrency;

        InterceptingPooledExecutor(InterceptorOfExecution interceptorOfExecution, int concurrency, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptorOfExecution, taskFactory);
            this.threadFactory = threadFactory;
            this.concurrency = concurrency;
        }

        public void submitAndAwaitPause(Runnable task, InterceptorOfConsequences interceptor)
        {
            // we don't check isShutdown as we could have a task queued by simulation from prior to shutdown
            if (isTerminated()) throw new AssertionError();
            if (debugPending != null && !debugPending.contains(task)) throw new AssertionError();

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

            addPending(task);
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

        public synchronized void shutdown()
        {
            isShutdown = true;
            WaitingThread next;
            while (null != (next = waiting.poll()))
                next.terminate();

            if (pending == 0)
                terminate();
        }

        synchronized void terminate()
        {
            List<InterceptibleThread> snapshot = new ArrayList<>(threads.keySet());
            for (InterceptibleThread thread : snapshot)
            {
                WaitingThread terminate = threads.get(thread);
                if (terminate != null)
                    terminate.terminate();
            }
            runDeterministic(isTerminated::signal);
        }

        public synchronized List<Runnable> shutdownNow()
        {
            shutdown();
            threads.keySet().forEach(InterceptibleThread::interrupt);
            return Collections.emptyList();
        }

        @Override
        public boolean inExecutor()
        {
            return threads.containsKey(Thread.currentThread());
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
        volatile boolean executing, terminating, terminated;

        AbstractSingleThreadedExecutorPlus(InterceptorOfExecution interceptorOfExecution, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptorOfExecution, taskFactory);
            this.thread = (InterceptibleThread) threadFactory.newThread(() -> {
                InterceptibleThread thread = (InterceptibleThread) Thread.currentThread();
                try
                {
                    while (true)
                    {
                        Runnable task;
                        try
                        {
                            task = dequeue();
                        }
                        catch (InterruptedException | UncheckedInterruptedException ignore)
                        {
                            if (terminating) return;
                            else continue;
                        }

                        try
                        {
                            task.run();
                        }
                        catch (Throwable t)
                        {
                            try { thread.getUncaughtExceptionHandler().uncaughtException(thread, t); }
                            catch (Throwable ignore) {}
                        }

                        executing = false;
                        boolean shutdown = isShutdown;
                        if ((0 == completePending(task) && shutdown))
                            return;

                        thread.interceptTermination(false);
                    }
                }
                finally
                {
                    runDeterministic(thread::onTermination);
                    if (terminating)
                    {
                        synchronized (this)
                        {
                            terminated = true;
                            notifyAll();
                        }
                    }
                    else
                    {
                        runDeterministic(this::terminate);
                    }
                }
            });
            thread.start();
        }

        void terminate()
        {
            synchronized (this)
            {
                assert pending == 0;
                if (terminating)
                    return;

                terminating = true;
                if (Thread.currentThread() != thread)
                {
                    notifyAll();
                    try { while (!terminated) wait(); }
                    catch (InterruptedException e) { throw new UncheckedInterruptedException(e); }
                }
                terminated = true;
            }

            isTerminated.signal(); // this has simulator side-effects, so try to perform before we interceptTermination
            if (Thread.currentThread() == thread && thread.isIntercepting())
                thread.interceptTermination(true);
        }

        public synchronized void shutdown()
        {
            if (isShutdown)
                return;

            isShutdown = true;
            if (pending == 0)
                terminate();
        }

        public synchronized List<Runnable> shutdownNow()
        {
            if (isShutdown)
                return Collections.emptyList();

            isShutdown = true;
            List<Runnable> cancelled = new ArrayList<>(queue);
            queue.clear();
            cancelled.forEach(super::cancelPending);
            if (pending == 0) terminate();
            else thread.interrupt();
            return cancelled;
        }

        synchronized void enqueue(Runnable runnable)
        {
            queue.add(runnable);
            notify();
        }

        synchronized Runnable dequeue() throws InterruptedException
        {
            Runnable next;
            while (null == (next = queue.poll()) && !terminating)
                wait();

            if (next == null)
                throw new InterruptedException();

            return next;
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
        InterceptingSequentialExecutor(InterceptorOfExecution interceptorOfExecution, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptorOfExecution, threadFactory, taskFactory);
        }

        public void submitAndAwaitPause(Runnable task, InterceptorOfConsequences interceptor)
        {
            synchronized (this)
            {
                // we don't check isShutdown as we could have a task queued by simulation from prior to shutdown
                if (terminated) throw new AssertionError();
                if (executing) throw new AssertionError();
                if (debugPending != null && !debugPending.contains(task)) throw new AssertionError();
                executing = true;

                AwaitPaused done = new AwaitPaused(this);
                thread.beforeInvocation(interceptor, done);
                enqueue(task);
                done.awaitPause();
            }
        }

        public synchronized void submitUnmanaged(Runnable task)
        {
            addPending(task);
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
            if (isShutdown)
                throw new RejectedExecutionException();

            long delayNanos = unit.toNanos(delay);
            return interceptorOfExecution.intercept().schedule(SCHEDULED_TASK, delayNanos, relativeToGlobalNanos(delayNanos), callable(run, null), this);
        }

        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            if (isShutdown)
                throw new RejectedExecutionException();

            long delayNanos = unit.toNanos(delay);
            return interceptorOfExecution.intercept().schedule(SCHEDULED_TASK, delayNanos, relativeToGlobalNanos(delayNanos), callable, this);
        }

        public ScheduledFuture<?> scheduleTimeoutWithDelay(Runnable run, long delay, TimeUnit unit)
        {
            return scheduleTimeoutAt(run, relativeToGlobalNanos(unit.toNanos(delay)));
        }

        public ScheduledFuture<?> scheduleAt(Runnable run, long deadlineNanos)
        {
            if (isShutdown)
                throw new RejectedExecutionException();

            return interceptorOfExecution.intercept().schedule(SCHEDULED_TASK, localToRelativeNanos(deadlineNanos), localToGlobalNanos(deadlineNanos), callable(run, null), this);
        }

        public ScheduledFuture<?> scheduleTimeoutAt(Runnable run, long deadlineNanos)
        {
            if (isShutdown)
                throw new RejectedExecutionException();

            return interceptorOfExecution.intercept().schedule(SCHEDULED_TIMEOUT, localToRelativeNanos(deadlineNanos), localToGlobalNanos(deadlineNanos), callable(run, null), this);
        }

        public ScheduledFuture<?> scheduleSelfRecurring(Runnable run, long delay, TimeUnit unit)
        {
            if (isShutdown)
                throw new RejectedExecutionException();

            long delayNanos = unit.toNanos(delay);
            return interceptorOfExecution.intercept().schedule(SCHEDULED_DAEMON, delayNanos, relativeToGlobalNanos(delayNanos), callable(run, null), this);
        }

        public ScheduledFuture<?> scheduleAtFixedRate(Runnable run, long initialDelay, long period, TimeUnit unit)
        {
            if (isShutdown)
                throw new RejectedExecutionException();

            long delayNanos = unit.toNanos(initialDelay);
            return interceptorOfExecution.intercept().schedule(SCHEDULED_DAEMON, delayNanos, relativeToGlobalNanos(delayNanos), new Callable<Object>()
            {
                @Override
                public Object call()
                {
                    run.run();
                    if (!isShutdown)
                        scheduleAtFixedRate(run, period, period, unit);
                    return null;
                }

                @Override
                public String toString()
                {
                    return run.toString();
                }
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
        InterceptingLocalAwareSequentialExecutor(InterceptorOfExecution interceptorOfExecution, ThreadFactory threadFactory, InterceptingTaskFactory taskFactory)
        {
            super(interceptorOfExecution, threadFactory, taskFactory);
        }
    }

    @PerClassLoader
    static class DiscardingSequentialExecutor implements LocalAwareSequentialExecutorPlus, ScheduledExecutorPlus
    {
        @Override
        public void shutdown()
        {
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown()
        {
            return false;
        }

        @Override
        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return false;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            return ImmediateFuture.cancelled();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            return ImmediateFuture.cancelled();
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            return ImmediateFuture.cancelled();
        }

        @Override
        public void execute(WithResources withResources, Runnable task)
        {
        }

        @Override
        public <T> Future<T> submit(WithResources withResources, Callable<T> task)
        {
            return ImmediateFuture.cancelled();
        }

        @Override
        public Future<?> submit(WithResources withResources, Runnable task)
        {
            return ImmediateFuture.cancelled();
        }

        @Override
        public <T> Future<T> submit(WithResources withResources, Runnable task, T result)
        {
            return ImmediateFuture.cancelled();
        }

        @Override
        public boolean inExecutor()
        {
            return false;
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

        @Override
        public AtLeastOnceTrigger atLeastOnceTrigger(Runnable runnable)
        {
            return new AtLeastOnceTrigger()
            {
                @Override
                public boolean trigger()
                {
                    return false;
                }

                @Override
                public void runAfter(Runnable run)
                {
                }

                @Override
                public void sync()
                {
                }
            };
        }

        @Override
        public void execute(Runnable command)
        {
        }

        @Override
        public ScheduledFuture<?> scheduleSelfRecurring(Runnable run, long delay, TimeUnit units)
        {
            return new NotScheduledFuture<>();
        }

        @Override
        public ScheduledFuture<?> scheduleAt(Runnable run, long deadline)
        {
            return new NotScheduledFuture<>();
        }

        @Override
        public ScheduledFuture<?> scheduleTimeoutAt(Runnable run, long deadline)
        {
            return new NotScheduledFuture<>();
        }

        @Override
        public ScheduledFuture<?> scheduleTimeoutWithDelay(Runnable run, long delay, TimeUnit units)
        {
            return new NotScheduledFuture<>();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            return new NotScheduledFuture<>();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            return new NotScheduledFuture<>();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            return new NotScheduledFuture<>();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            return new NotScheduledFuture<>();
        }
    }
}
