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

import java.util.function.Function;

import com.google.common.base.Preconditions;

import org.apache.cassandra.simulator.systems.NotifyThreadPaused.AwaitPaused;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.RunnableFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

// An asynchronous task we can intercept the execution of
@Shared(scope = SIMULATION)
public interface InterceptedExecution
{
    SimulatedAction.Kind kind();
    long deadlineNanos();
    void invokeAndAwaitPause(InterceptorOfConsequences interceptor);
    void cancel();
    void onCancel(Runnable onCancel);

    /**
     * Abstract implementation we only need to implement Runnable for, which we will invoke
     * on the given executor. The runnable should perform any necessary transfer to the target
     * class loader itself.
     */
    abstract class InterceptedTaskExecution implements InterceptedExecution, Runnable
    {
        public final InterceptingExecutor executor;
        boolean submittedOrCancelled;
        Runnable onCancel;

        public InterceptedTaskExecution(InterceptingExecutor executor)
        {
            Preconditions.checkNotNull(executor);
            this.executor = executor;
            executor.addPending(this);
        }

        @Override
        public SimulatedAction.Kind kind()
        {
            return SimulatedAction.Kind.TASK;
        }

        @Override
        public long deadlineNanos()
        {
            return -1L;
        }

        public void invokeAndAwaitPause(InterceptorOfConsequences interceptor)
        {
            Preconditions.checkState(!submittedOrCancelled);
            executor.submitAndAwaitPause(this, interceptor);
            submittedOrCancelled = true;
        }

        @Override
        public void cancel()
        {
            if (!submittedOrCancelled)
            {
                executor.cancelPending(this);
                submittedOrCancelled = true;
                if (onCancel != null)
                {
                    onCancel.run();
                    onCancel = null;
                }
            }
        }

        @Override
        public void onCancel(Runnable onCancel)
        {
            assert this.onCancel == null || onCancel == null;
            this.onCancel = onCancel;
        }
    }

    /**
     * Simple implementation we only need to supply a Runnable to, which we will invoke
     * on the given executor. The runnable should perform any necessary transfer to the target
     * class loader itself.
     */
    class InterceptedRunnableExecution extends InterceptedTaskExecution
    {
        final Runnable run;
        public InterceptedRunnableExecution(InterceptingExecutor executor, Runnable run)
        {
            super(executor);
            this.run = run;
        }

        public void run()
        {
            run.run();
        }

        public String toString()
        {
            return run + " with " + executor;
        }
    }

    class InterceptedFutureTaskExecution<T> implements InterceptedExecution
    {
        private final SimulatedAction.Kind kind;
        private final InterceptingExecutor executor;
        private final long deadlineNanos;
        private RunnableFuture<T> run;
        private Runnable onCancel;

        public InterceptedFutureTaskExecution(SimulatedAction.Kind kind, InterceptingExecutor executor, RunnableFuture<T> run)
        {
            this(kind, executor, run, -1L);
        }

        public InterceptedFutureTaskExecution(SimulatedAction.Kind kind, InterceptingExecutor executor, RunnableFuture<T> run, long deadlineNanos)
        {
            Preconditions.checkArgument(deadlineNanos >= -1);
            this.kind = kind;
            this.executor = executor;
            this.run = run;
            this.deadlineNanos = deadlineNanos;
            executor.addPending(run);
        }

        public String toString()
        {
            return (run != null ? run.toString() : "(done)") + " with " + executor;
        }

        @Override
        public SimulatedAction.Kind kind()
        {
            return kind;
        }

        @Override
        public long deadlineNanos()
        {
            return deadlineNanos;
        }

        @Override
        public void invokeAndAwaitPause(InterceptorOfConsequences interceptor)
        {
            Preconditions.checkNotNull(run);
            executor.submitAndAwaitPause(run, interceptor);
            run = null;
        }

        @Override
        public void cancel()
        {
            if (run != null)
            {
                executor.cancelPending(run);
                run = null;
                if (onCancel != null)
                {
                    onCancel.run();
                    onCancel = null;
                }
            }
        }

        @Override
        public void onCancel(Runnable onCancel)
        {
            assert this.onCancel == null || onCancel == null;
            this.onCancel = onCancel;
        }
    }

    public class InterceptedThreadStart implements Runnable, InterceptedExecution
    {
        final SimulatedAction.Kind kind;
        final InterceptibleThread thread;
        Runnable run;
        Runnable onCancel;

        public InterceptedThreadStart(Function<Runnable, InterceptibleThread> factory, Runnable run, SimulatedAction.Kind kind)
        {
            this.thread = factory.apply(this);
            this.kind = kind;
            this.run = run;
        }

        public void run()
        {
            try
            {
                run.run();
            }
            catch (UncheckedInterruptedException ignore)
            {
                // thrown on abnormal shutdown; don't want to pollute the log
            }
            catch (Throwable t)
            {
                thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
            }
            finally
            {
                thread.onTermination();
                thread.interceptTermination(true);
            }
        }

        public String toString()
        {
            return (run != null ? run.toString() : "(done)") + " with " + thread;
        }

        @Override
        public SimulatedAction.Kind kind()
        {
            return kind;
        }

        @Override
        public long deadlineNanos()
        {
            return -1L;
        }

        public void invokeAndAwaitPause(InterceptorOfConsequences interceptor)
        {
            AwaitPaused done = new AwaitPaused();
            synchronized (done)
            {
                thread.beforeInvocation(interceptor, done);
                thread.start();
                done.awaitPause();
            }
        }

        @Override
        public void cancel()
        {
            if (run != null)
            {
                run = null;
                if (onCancel != null)
                {
                    onCancel.run();
                    onCancel = null;
                }
            }
        }

        @Override
        public void onCancel(Runnable onCancel)
        {
            assert this.onCancel == null || onCancel == null;
            this.onCancel = onCancel;
        }
    }
}
