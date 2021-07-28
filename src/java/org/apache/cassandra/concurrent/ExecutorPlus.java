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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Cassandra's extension of {@link ExecutorService}, using our own {@link Future}, supporting
 * {@link #inExecutor()}, and execution with associated resources {@link #execute(WithResources, Runnable)}
 * (which is primarily used for encapsulating {@link ExecutorLocals} without leaking implementing classes).
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface ExecutorPlus extends ExecutorService, ResizableThreadPool
{
    interface MaximumPoolSizeListener
    {
        /**
         * Listener to follow changes to the maximum pool size
         */
        void onUpdateMaximumPoolSize(int maximumPoolSize);
    }

    /**
     * MAY execute {@code task} immediately, if the calling thread is permitted to do so.
     */
    default void maybeExecuteImmediately(Runnable task)
    {
        execute(task);
    }

    /**
     * Overrides {@link ExecutorService#submit(Callable)} to return a Cassandra {@link Future}
     */
    @Override
    <T> Future<T> submit(Callable<T> task);

    /**
     * Overrides {@link ExecutorService#submit(Runnable, Object)} to return a Cassandra {@link Future}
     */
    @Override
    <T> Future<T> submit(Runnable task, T result);

    /**
     * Overrides {@link ExecutorService#submit(Runnable)} to return a Cassandra {@link Future}
     */
    @Override
    Future<?> submit(Runnable task);

    /*
     * ==============================================
     * WithResources variants of submit and execute.
     *
     * (We need a way to inject a TraceState directly into the Executor context without going through
     * the global Tracing sessions; see CASSANDRA-5668)
     * ==============================================
     */

    /**
     * Invoke {@code task}. The invoking thread will first instantiate the resources provided before
     * invoking {@code task}, so that thread state may be modified and cleaned up.
     *
     * The invoking thread will execute something semantically equivlent to:
     *
     * <code>
     *     try (Closeable close = withResources.get())
     *     {
     *         task.run();
     *     }
     * </code>
     *
     * @param withResources the resources to create and hold while executing {@code task}
     * @param task the task to execute
     */
    void execute(WithResources withResources, Runnable task);

    /**
     * Invoke {@code task}, returning a future representing this computation.
     * The invoking thread will first instantiate the resources provided before
     * invoking {@code task}, so that thread state may be modified and cleaned up.
     *
     * The invoking thread will execute something semantically equivlent to:
     *
     * <code>
     *     try (Closeable close = withResources.get())
     *     {
     *         return task.call();
     *     }
     * </code>
     *
     * @param withResources the resources to create and hold while executing {@code task}
     * @param task the task to execute
     */
    <T> Future<T> submit(WithResources withResources, Callable<T> task);

    /**
     * Invoke {@code task}, returning a future yielding {@code null} if successful,
     * or the abnormal termination of {@code task} otherwise.
     *
     * The invoking thread will first instantiate the resources provided before
     * invoking {@code task}, so that thread state may be modified and cleaned up
     *
     * <code>
     *     try (Closeable close = withResources.get())
     *     {
     *         task.run();
     *         return null;
     *     }
     * </code>
     *
     * @param withResources the resources to create and hold while executing {@code task}
     * @param task the task to execute
     */
    Future<?> submit(WithResources withResources, Runnable task);

    /**
     * Invoke {@code task}, returning a future yielding {@code result} if successful,
     * or the abnormal termination of {@code task} otherwise.
     *
     * The invoking thread will first instantiate the resources provided before
     * invoking {@code task}, so that thread state may be modified and cleaned up.
     *
     * The invoking thread will execute something semantically equivlent to:
     *
     * <code>
     *     try (Closeable close = withResources.get())
     *     {
     *         task.run();
     *         return result;
     *     }
     * </code>
     *
     * @param withResources the resources to create and hold while executing {@code task}
     * @param task the task to execute
     * @param result the result if successful
     */
    <T> Future<T> submit(WithResources withResources, Runnable task, T result);

    /**
     * @return true iff the caller is a worker thread actively serving this executor
     */
    boolean inExecutor();

    default <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }
    default <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }
    default <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
        throw new UnsupportedOperationException();
    }
    default <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        throw new UnsupportedOperationException();
    }
}
