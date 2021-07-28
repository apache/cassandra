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

import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;

import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * This class inherits Executor best practices from {@link ThreadPoolExecutorBase}
 * and {@link ThreadPoolExecutorBuilder}. Most Cassandra executors should use or extend this.
 *
 * This class' addition is to abstract the semantics of task encapsulation to handle
 * exceptions and {@link ExecutorLocals}. See {@link TaskFactory} for more detail.
 */
public class ThreadPoolExecutorPlus extends ThreadPoolExecutorBase implements ExecutorPlus
{
    final TaskFactory taskFactory;

    ThreadPoolExecutorPlus(ThreadPoolExecutorBuilder<? extends ThreadPoolExecutorPlus> builder)
    {
        this(builder, TaskFactory.standard());
    }

    ThreadPoolExecutorPlus(ThreadPoolExecutorBuilder<? extends ThreadPoolExecutorPlus> builder, TaskFactory taskFactory)
    {
        super(builder);
        this.taskFactory = taskFactory;
    }

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
    public int getMaxTasksQueued()
    {
        return getQueue().size();
    }
}
