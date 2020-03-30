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

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.ActionPlan;
import org.apache.cassandra.simulator.Actions;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedFutureTaskExecution;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedThreadStart;
import org.apache.cassandra.simulator.systems.InterceptingExecutor.InterceptedScheduledFutureTask;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.NotScheduledFuture;
import org.apache.cassandra.utils.concurrent.RunnableFuture;

import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_DAEMON;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_TASK;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.SCHEDULED_TIMEOUT;
import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.TASK;

public class SimulatedExecution implements InterceptorOfExecution
{
    static class NoExecutorMarker implements InterceptingExecutor
    {
        static final NoExecutorMarker INFINITE_LOOP = new NoExecutorMarker();
        static final NoExecutorMarker THREAD = new NoExecutorMarker();

        @Override public void addPending(Object task) { throw new UnsupportedOperationException(); }
        @Override public void cancelPending(Object task) { throw new UnsupportedOperationException(); }
        @Override public void submitUnmanaged(Runnable task) { throw new UnsupportedOperationException(); }
        @Override public void submitAndAwaitPause(Runnable task, InterceptorOfConsequences interceptor) { throw new UnsupportedOperationException(); }

        @Override public OrderOn orderAppliesAfterScheduling() { throw new UnsupportedOperationException(); }
        @Override public int concurrency() { return Integer.MAX_VALUE; }
    }

    /**
     * Used to handle executions submitted outside of the simulation
     * Once "closed" only simulated executions are permitted (until a new NoIntercept instance is assigned).
     */
    static class NoIntercept implements InterceptExecution
    {
        private final AtomicInteger unmanaged = new AtomicInteger();
        private volatile Condition forbidden;

        public  <V, T extends RunnableFuture<V>> T addTask(T task, InterceptingExecutor executor)
        {
            synchronized (this)
            {
                if (forbidden != null)
                    throw new InterceptingExecutor.ForbiddenExecutionException();
                unmanaged.incrementAndGet();
            }

            class Run implements Runnable
            {
                public void run()
                {
                    try
                    {
                        task.run();
                    }
                    finally
                    {
                        if (unmanaged.decrementAndGet() == 0 && forbidden != null)
                            forbidden.signal();
                    }
                }
            }

            executor.submitUnmanaged(new Run());
            return task;
        }

        public <T> ScheduledFuture<T> schedule(SimulatedAction.Kind kind, long delayNanos, long deadlineNanos, Callable<T> runnable, InterceptingExecutor executor)
        {
            return new NotScheduledFuture<>();
        }

        public Thread start(SimulatedAction.Kind kind, Function<Runnable, InterceptibleThread> factory, Runnable run)
        {
            Thread thread = factory.apply(run);
            thread.start();
            return thread;
        }

        NoIntercept forbidExecution()
        {
            forbidden = new NotInterceptedSyncCondition();
            if (0 == unmanaged.get())
                forbidden.signal();
            return this;
        }

        void awaitTermination()
        {
            forbidden.awaitUninterruptibly();
        }
    }

    static class Intercept implements InterceptExecution
    {
        final InterceptorOfConsequences intercept;

        Intercept(InterceptorOfConsequences intercept)
        {
            this.intercept = intercept;
        }

        public <V, T extends RunnableFuture<V>> T addTask(T task, InterceptingExecutor executor)
        {
            InterceptedFutureTaskExecution<?> intercepted = new InterceptedFutureTaskExecution<>(TASK, executor, task);
            intercept.interceptExecution(intercepted, executor);
            return task;
        }

        public <V> ScheduledFuture<V> schedule(SimulatedAction.Kind kind, long delayNanos, long deadlineNanos, Callable<V> call, InterceptingExecutor executor)
        {
            assert kind == SCHEDULED_TASK || kind == SCHEDULED_TIMEOUT || kind == SCHEDULED_DAEMON;
            InterceptedScheduledFutureTask<V> task = new InterceptedScheduledFutureTask<>(delayNanos, call);
            InterceptedFutureTaskExecution<?> intercepted = new InterceptedFutureTaskExecution<>(kind, executor, task, deadlineNanos);
            task.onCancel(intercepted::cancel);
            intercept.interceptExecution(intercepted, executor.orderAppliesAfterScheduling());
            return task;
        }

        public Thread start(SimulatedAction.Kind kind, Function<Runnable, InterceptibleThread> factory, Runnable run)
        {
            InterceptedThreadStart intercepted = new InterceptedThreadStart(factory, run, kind);
            intercept.interceptExecution(intercepted, kind == SimulatedAction.Kind.INFINITE_LOOP ? NoExecutorMarker.INFINITE_LOOP : NoExecutorMarker.THREAD);
            return intercepted.thread;
        }
    }

    private NoIntercept noIntercept = new NoIntercept();

    public SimulatedExecution()
    {
    }

    public InterceptingExecutorFactory factory(InterceptorOfGlobalMethods interceptorOfGlobalMethods, ClassLoader classLoader, ThreadGroup threadGroup)
    {
        return new InterceptingExecutorFactory(this, interceptorOfGlobalMethods, classLoader, threadGroup);
    }

    public InterceptExecution intercept()
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread))
            return noIntercept;

        InterceptibleThread interceptibleThread = (InterceptibleThread) thread;
        if (!interceptibleThread.isIntercepting())
            return noIntercept;

        return new SimulatedExecution.Intercept(interceptibleThread);
    }

    public ActionPlan plan()
    {
        return ActionPlan.setUpTearDown(start(), stop());
    }

    private ActionList start()
    {
        return ActionList.of(Actions.of("Start Simulating Execution", () -> {
            noIntercept.forbidExecution().awaitTermination();
            return ActionList.empty();
        }));
    }

    private ActionList stop()
    {
        return ActionList.of(Actions.of("Stop Simulating Execution", () -> {
            noIntercept = new NoIntercept();
            return ActionList.empty();
        }));
    }

    public void forceStop()
    {
        noIntercept = new NoIntercept();
    }

    public static <T> Callable<T> callable(Runnable run, T result)
    {
        return new Callable<T>()
        {
            public T call()
            {
                run.run();
                return result;
            }

            public String toString()
            {
                return result == null ? run.toString() : (run + " returning " + result);
            }
        };
    }
}
