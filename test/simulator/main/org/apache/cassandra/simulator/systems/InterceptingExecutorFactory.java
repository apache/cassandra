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

import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ExecutorBuilder;
import org.apache.cassandra.concurrent.ExecutorBuilderFactory;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.concurrent.LocalAwareSequentialExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.SyncFutureTask;
import org.apache.cassandra.concurrent.TaskFactory;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableBiFunction;
import org.apache.cassandra.distributed.impl.IsolatedExecutor;
import org.apache.cassandra.simulator.systems.InterceptingExecutor.InterceptingTaskFactory;
import org.apache.cassandra.simulator.systems.InterceptingExecutor.InterceptingLocalAwareSequentialExecutor;
import org.apache.cassandra.simulator.systems.InterceptingExecutor.InterceptingPooledExecutor;
import org.apache.cassandra.simulator.systems.InterceptingExecutor.InterceptingPooledLocalAwareExecutor;
import org.apache.cassandra.simulator.systems.InterceptingExecutor.InterceptingSequentialExecutor;
import org.apache.cassandra.simulator.systems.InterceptorOfExecution.InterceptExecution;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.RunnableFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class InterceptingExecutorFactory implements ExecutorFactory
{
    static class StandardSyncTaskFactory extends TaskFactory.Standard implements InterceptingTaskFactory, Serializable
    {
        @Override
        public <T> RunnableFuture<T> newTask(Callable<T> call)
        {
            return new SyncFutureTask<>(call);
        }

        @Override
        protected <T> RunnableFuture<T> newTask(WithResources withResources, Callable<T> call)
        {
            return new SyncFutureTask<>(withResources, call);
        }
    }

    static class LocalAwareSyncTaskFactory extends TaskFactory.LocalAware implements InterceptingTaskFactory, Serializable
    {
        @Override
        public <T> RunnableFuture<T> newTask(Callable<T> call)
        {
            return new SyncFutureTask<>(call);
        }

        @Override
        protected <T> RunnableFuture<T> newTask(WithResources withResources, Callable<T> call)
        {
            return new SyncFutureTask<>(withResources, call);
        }
    }

    abstract static class AbstractExecutorBuilder<E extends ExecutorService> implements ExecutorBuilder<E>
    {
        ThreadGroup threadGroup;
        UncaughtExceptionHandler uncaughtExceptionHandler;

        @Override
        public ExecutorBuilder<E> withKeepAlive(long keepAlive, TimeUnit keepAliveUnits)
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withKeepAlive()
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withThreadPriority(int threadPriority)
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withThreadGroup(ThreadGroup threadGroup)
        {
            this.threadGroup = threadGroup;
            return this;
        }

        @Override
        public ExecutorBuilder<E> withQueueLimit(int queueLimit)
        {
            // should implement (not pressing)
            return this;
        }

        @Override
        public ExecutorBuilder<E> withRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler)
        {
            // we don't currently ever reject execution, but we should perhaps consider implementing it
            return this;
        }

        @Override
        public ExecutorBuilder<E> withUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler)
        {
            this.uncaughtExceptionHandler = uncaughtExceptionHandler;
            return this;
        }
    }

    class SimpleExecutorBuilder<E extends ExecutorService> extends AbstractExecutorBuilder<E>
    {
        ThreadGroup threadGroup;

        final String name;
        final SerializableBiFunction<InterceptorOfExecution, ThreadFactory, E> factory;

        SimpleExecutorBuilder(String name, SerializableBiFunction<InterceptorOfExecution, ThreadFactory, E> factory)
        {
            this.factory = factory;
            this.name = name;
        }

        @Override
        public E build()
        {
            return transferToInstance.apply(factory).apply(simulatedExecution, factory(name, null, threadGroup, uncaughtExceptionHandler));
        }
    }

    final SimulatedExecution simulatedExecution;
    final ClassLoader classLoader;
    final ThreadGroup threadGroup;
    final IIsolatedExecutor.DynamicFunction<Serializable> transferToInstance;
    volatile boolean isClosed;

    InterceptingExecutorFactory(SimulatedExecution simulatedExecution, ClassLoader classLoader, ThreadGroup threadGroup)
    {
        this.simulatedExecution = simulatedExecution;
        this.classLoader = classLoader;
        this.threadGroup = threadGroup;
        this.transferToInstance = IsolatedExecutor.transferTo(classLoader);
    }

    public InterceptibleThreadFactory factory(String name)
    {
        return factory(name, null);
    }

    InterceptibleThreadFactory factory(String name, Object extraInfo)
    {
        return factory(name, extraInfo, threadGroup);
    }

    InterceptibleThreadFactory factory(String name, Object extraInfo, ThreadGroup threadGroup)
    {
        return factory(name, extraInfo, threadGroup, null);
    }

    InterceptibleThreadFactory factory(String name, Object extraInfo, ThreadGroup threadGroup, UncaughtExceptionHandler uncaughtExceptionHandler)
    {
        if (uncaughtExceptionHandler == null)
            uncaughtExceptionHandler = transferToInstance.apply((IIsolatedExecutor.SerializableCallable<UncaughtExceptionHandler>)() -> (thread, throwable) -> {
                if (!(throwable instanceof UncheckedInterruptedException))
                    JVMStabilityInspector.uncaughtException(thread, throwable);
            }).call();

        if (threadGroup == null) threadGroup = this.threadGroup;
        else if (!this.threadGroup.parentOf(threadGroup)) throw new IllegalArgumentException();
        return new InterceptibleThreadFactory(name, Thread.NORM_PRIORITY, classLoader, uncaughtExceptionHandler, threadGroup, this, extraInfo);
    }

    @Override
    public ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus> withJmx(String jmxPath)
    {
        return this;
    }

    @Override
    public ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus> withJmxInternal()
    {
        return this;
    }

    @Override
    public ExecutorBuilder<? extends SequentialExecutorPlus> configureSequential(String name)
    {
        return new SimpleExecutorBuilder<>(name, (interceptSupplier, threadFactory) -> new InterceptingSequentialExecutor(interceptSupplier, threadFactory, new StandardSyncTaskFactory()));
    }

    @Override
    public ExecutorBuilder<? extends ExecutorPlus> configurePooled(String name, int threads)
    {
        return new SimpleExecutorBuilder<>(name, (interceptSupplier, threadFactory) -> new InterceptingPooledExecutor(interceptSupplier, threads, threadFactory, new StandardSyncTaskFactory()));
    }

    public SequentialExecutorPlus sequential(String name)
    {
        return configureSequential(name).build();
    }

    @Override
    public LocalAwareSubFactory localAware()
    {
        return new LocalAwareSubFactory()
        {
            @Override
            public LocalAwareSubFactoryWithJMX withJmx(String jmxPath)
            {
                return new LocalAwareSubFactoryWithJMX()
                {
                    @Override
                    public LocalAwareExecutorPlus shared(String name, int threads, ExecutorPlus.MaximumPoolSizeListener onSetMaxSize)
                    {
                        return pooled(name, threads);
                    }

                    @Override
                    public ExecutorBuilder<? extends LocalAwareSequentialExecutorPlus> configureSequential(String name)
                    {
                        return new SimpleExecutorBuilder<>(name, (interceptSupplier, threadFactory) -> new InterceptingLocalAwareSequentialExecutor(interceptSupplier, threadFactory, new LocalAwareSyncTaskFactory()));
                    }

                    @Override
                    public ExecutorBuilder<? extends LocalAwareExecutorPlus> configurePooled(String name, int threads)
                    {
                        return new SimpleExecutorBuilder<>(name, (interceptSupplier, threadFactory) -> new InterceptingPooledLocalAwareExecutor(interceptSupplier, threads, threadFactory, new LocalAwareSyncTaskFactory()));
                    }
                };
            }

            @Override
            public ExecutorBuilder<? extends LocalAwareSequentialExecutorPlus> configureSequential(String name)
            {
                return new SimpleExecutorBuilder<>(name, (interceptSupplier, threadFactory) -> new InterceptingLocalAwareSequentialExecutor(interceptSupplier, threadFactory, new LocalAwareSyncTaskFactory()));
            }

            @Override
            public ExecutorBuilder<? extends LocalAwareExecutorPlus> configurePooled(String name, int threads)
            {
                return new SimpleExecutorBuilder<>(name, (interceptSupplier, threadFactory) -> new InterceptingPooledLocalAwareExecutor(interceptSupplier, threads, threadFactory, new LocalAwareSyncTaskFactory()));
            }
        };
    }

    @Override
    public ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name, int priority)
    {
        return transferToInstance.apply((SerializableBiFunction<InterceptorOfExecution, ThreadFactory, ScheduledExecutorPlus>) (interceptSupplier, threadFactory) -> new InterceptingSequentialExecutor(interceptSupplier, threadFactory, new StandardSyncTaskFactory())).apply(simulatedExecution, factory(name));
    }

    @Override
    public ExecutorPlus pooled(String name, int threads)
    {
        if (threads == 1)
            return configureSequential(name).build();
        return configurePooled(name, threads).build();
    }

    public Thread startThread(String name, Runnable run)
    {
        return simulatedExecution.intercept().start(SimulatedAction.Kind.THREAD, factory(name)::newThread, run);
    }

    @VisibleForTesting
    public InterceptedExecution.InterceptedThreadStart startParked(String name, Runnable run)
    {
        return new InterceptedExecution.InterceptedThreadStart(factory(name)::newThread,
                                                               run,
                                                               SimulatedAction.Kind.THREAD);
    }

    public Interruptible infiniteLoop(String name, Interruptible.Task task, boolean simulatorSafe)
    {
        InterceptExecution interceptor = !simulatorSafe ? simulatedExecution.noIntercept() : simulatedExecution.intercept();
        return new InfiniteLoopExecutor((n, r) -> interceptor.start(SimulatedAction.Kind.INFINITE_LOOP, factory(n, task)::newThread, r), name, task);
    }

    @Override
    public ThreadGroup newThreadGroup(String name)
    {
        return null;
    }

    public void close()
    {
        isClosed = true;
        forEach(threadGroup, thread -> {
            thread.trapInterrupts(false);
            thread.interrupt();
        });
        threadGroup.interrupt();
    }

    public void interrupt()
    {
        threadGroup.interrupt();
    }

    private static void forEach(ThreadGroup threadGroup, Consumer<InterceptibleThread> consumer)
    {
        Thread[] threads;
        ThreadGroup[] groups;
        synchronized (threadGroup)
        {
            threads = new Thread[threadGroup.activeCount()];
            threadGroup.enumerate(threads, false);
            groups = new ThreadGroup[threadGroup.activeGroupCount()];
            threadGroup.enumerate(groups, false);
        }
        for (Thread thread : threads) consumer.accept((InterceptibleThread) thread);
        for (ThreadGroup group : groups) forEach(group, consumer);
    }
}
