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

import org.apache.cassandra.concurrent.DebuggableTask.RunnableDebuggableTask;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.RunnableFuture;

import static org.apache.cassandra.concurrent.FutureTask.callable;
import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * A simple mechanism to impose our desired semantics on the execution of a task without requiring a specialised
 * executor service. We wrap tasks in a suitable {@link FutureTask} or encapsulating {@link Runnable}.
 *
 * The encapsulations handle any exceptions in our standard way, as well as ensuring {@link ExecutorLocals} are
 * propagated in the case of {@link #localAware()}
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface TaskFactory
{
    Runnable toExecute(Runnable runnable);
    <T> RunnableFuture<T> toSubmit(Runnable runnable);
    <T> RunnableFuture<T> toSubmit(Runnable runnable, T result);
    <T> RunnableFuture<T> toSubmit(Callable<T> callable);

    Runnable toExecute(WithResources withResources, Runnable runnable);
    <T> RunnableFuture<T> toSubmit(WithResources withResources, Runnable runnable);
    <T> RunnableFuture<T> toSubmit(WithResources withResources, Runnable runnable, T result);
    <T> RunnableFuture<T> toSubmit(WithResources withResources, Callable<T> callable);

    static TaskFactory standard() { return Standard.INSTANCE; }
    static TaskFactory localAware() { return LocalAware.INSTANCE; }

    public class Standard implements TaskFactory
    {
        static final Standard INSTANCE = new Standard();
        protected Standard() {}

        @Override
        public Runnable toExecute(Runnable runnable)
        {
            return ExecutionFailure.suppressing(runnable);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(Runnable runnable)
        {
            return newTask(callable(runnable));
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(Runnable runnable, T result)
        {
            return newTask(callable(runnable, result));
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(Callable<T> callable)
        {
            return newTask(callable);
        }

        @Override
        public Runnable toExecute(WithResources withResources, Runnable runnable)
        {
            return ExecutionFailure.suppressing(withResources, runnable);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(WithResources withResources, Runnable runnable)
        {
            return withResources.isNoOp() ? newTask(callable(runnable))
                                          : newTask(withResources, callable(runnable));
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(WithResources withResources, Runnable runnable, T result)
        {
            return withResources.isNoOp() ? newTask(callable(runnable, result))
                                          : newTask(withResources, callable(runnable, result));
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(WithResources withResources, Callable<T> callable)
        {
            return withResources.isNoOp() ? newTask(callable)
                                          : newTask(withResources, callable);
        }

        protected <T> RunnableFuture<T> newTask(Callable<T> call)
        {
            return new FutureTask<>(call);
        }

        protected <T> RunnableFuture<T> newTask(WithResources withResources, Callable<T> call)
        {
            return new FutureTaskWithResources<>(withResources, call);
        }
    }

    public class LocalAware extends Standard
    {
        static final LocalAware INSTANCE = new LocalAware();

        protected LocalAware() {}

        @Override
        public Runnable toExecute(Runnable runnable)
        {
            if (runnable instanceof RunnableDebuggableTask)
                return ExecutionFailure.suppressingDebuggable(ExecutorLocals.propagate(), (RunnableDebuggableTask) runnable);

            // no reason to propagate exception when it is inaccessible to caller
            return ExecutionFailure.suppressing(ExecutorLocals.propagate(), runnable);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(Runnable runnable)
        {
            return super.toSubmit(ExecutorLocals.propagate(), runnable);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(Runnable runnable, T result)
        {
            return super.toSubmit(ExecutorLocals.propagate(), runnable, result);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(Callable<T> callable)
        {
            return super.toSubmit(ExecutorLocals.propagate(), callable);
        }

        @Override
        public Runnable toExecute(WithResources withResources, Runnable runnable)
        {
            return ExecutionFailure.suppressing(withLocals(withResources), runnable);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(WithResources withResources, Runnable runnable)
        {
            return super.toSubmit(withLocals(withResources), runnable);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(WithResources withResources, Runnable runnable, T result)
        {
            return super.toSubmit(withLocals(withResources), runnable, result);
        }

        @Override
        public <T> RunnableFuture<T> toSubmit(WithResources withResources, Callable<T> callable)
        {
            return super.toSubmit(withLocals(withResources), callable);
        }

        private static WithResources withLocals(WithResources withResources)
        {
            return withResources instanceof ExecutorLocals ? withResources : ExecutorLocals.propagate().and(withResources);
        }
    }

}
