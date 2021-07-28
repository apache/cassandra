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

import java.util.function.Consumer;

import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Shared;

import static java.lang.Thread.*;
import static org.apache.cassandra.concurrent.NamedThreadFactory.createThread;
import static org.apache.cassandra.concurrent.NamedThreadFactory.setupThread;
import static org.apache.cassandra.concurrent.ThreadPoolExecutorBuilder.pooledJmx;
import static org.apache.cassandra.concurrent.ThreadPoolExecutorBuilder.sequentialJmx;
import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Entry point for configuring and creating new executors.
 *
 * Supports quick and easy construction of default-configured executors via
 * <li>{@link #sequential(String)}
 * <li>{@link #pooled(String, int)}
 * <li>{@link #scheduled(String)}
 * <li>{@link #scheduled(boolean, String)}
 * <li>{@link #scheduled(boolean, String, int)}
 *
 * Supports custom configuration of executors via
 * <li>{@link #configureSequential(String)}
 * <li>{@link #configurePooled(String, int)}
 *
 * Supports any of the above with added JMX registration via sub-factories
 * <li>{@link #withJmx(String)}
 * <li>{@link #withJmxInternal()}
 *
 * Supports any of the above with the resultant executor propagating {@link ExecutorLocals} via sub-factory
 * <li>{@link #localAware()}
 *
 * Supports shared executors via sub-factory {@code localAware().withJMX()}
 * using {@link LocalAwareSubFactoryWithJMX#shared(String, int, ExecutorPlus.MaximumPoolSizeListener)}
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface ExecutorFactory extends ExecutorBuilderFactory.Jmxable<ExecutorPlus, SequentialExecutorPlus>
{
    public interface LocalAwareSubFactoryWithJMX extends ExecutorBuilderFactory<LocalAwareExecutorPlus, LocalAwareSequentialExecutorPlus>
    {
        LocalAwareExecutorPlus shared(String name, int threads, ExecutorPlus.MaximumPoolSizeListener onSetMaxSize);
    }

    public interface LocalAwareSubFactory extends ExecutorBuilderFactory<LocalAwareExecutorPlus, LocalAwareSequentialExecutorPlus>
    {
        LocalAwareSubFactoryWithJMX withJmx(String jmxPath);
        default LocalAwareSubFactoryWithJMX withJmxInternal() { return withJmx("internal"); }
    }

    /**
     * @return a factory that configures executors that propagate {@link ExecutorLocals} to the executing thread
     */
    LocalAwareSubFactory localAware();

    /**
     * @param name the name of the executor, the executor's thread group, and of any worker threads
     * @return a default-configured {@link ScheduledExecutorPlus}
     */
    default ScheduledExecutorPlus scheduled(String name) { return scheduled(true, name, NORM_PRIORITY); }

    /**
     * @param executeOnShutdown if false, waiting tasks will be cancelled on shutdown
     * @param name the name of the executor, the executor's thread group, and of any worker threads
     * @return a {@link ScheduledExecutorPlus} with normal thread priority
     */
    default ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name) { return scheduled(executeOnShutdown, name, NORM_PRIORITY); }

    /**
     * @param executeOnShutdown if false, waiting tasks will be cancelled on shutdown
     * @param name the name of the executor, the executor's thread group, and of any worker threads
     * @param priority the thread priority of workers
     * @return a {@link ScheduledExecutorPlus}
     */
    ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name, int priority);

    /**
     * Create and start a new thread to execute {@code runnable}
     * @param name the name of the thread
     * @param runnable the task to execute
     * @return the new thread
     */
    Thread startThread(String name, Runnable runnable);

    /**
     * Create and start a new InfiniteLoopExecutor to repeatedly invoke {@code runnable}.
     * On shutdown, the executing thread will be interrupted; to support clean shutdown
     * {@code runnable} should propagate {@link InterruptedException}
     *
     * @param name the name of the thread used to invoke the task repeatedly
     * @param task the task to execute repeatedly
     * @return the new thread
     */
    Interruptible infiniteLoop(String name, Interruptible.Task task, boolean simulatorSafe);

    /**
     * Create and start a new InfiniteLoopExecutor to repeatedly invoke {@code runnable}.
     * On shutdown, the executing thread will be interrupted; to support clean shutdown
     * {@code runnable} should propagate {@link InterruptedException}
     *
     * @param name the name of the thread used to invoke the task repeatedly
     * @param task the task to execute repeatedly
     * @param interruptHandler perform specific processing of interrupts of the task execution thread
     * @return the new thread
     */
    Interruptible infiniteLoop(String name, Interruptible.Task task, boolean simulatorSafe, Consumer<Thread> interruptHandler);

    /**
     * Create and start a new InfiniteLoopExecutor to repeatedly invoke {@code runnable}.
     * On shutdown, the executing thread will be interrupted; to support clean shutdown
     * {@code runnable} should propagate {@link InterruptedException}
     *
     * @param name the name of the thread used to invoke the task repeatedly
     * @param task the task to execute repeatedly
     * @return the new thread
     */
    default Interruptible infiniteLoop(String name, Interruptible.SimpleTask task, boolean simulatorSafe)
    {
        return infiniteLoop(name, Interruptible.Task.from(task), simulatorSafe);
    }

    /**
     * Create a new thread group for use with builders - this thread group will be situated within
     * this factory's parent thread group, and may be supplied to multiple executor builders.
     */
    ThreadGroup newThreadGroup(String name);

    public static final class Global
    {
        // deliberately not volatile to ensure zero overhead outside of testing;
        // depend on other memory visibility primitives to ensure visibility
        private static ExecutorFactory FACTORY = new ExecutorFactory.Default(Global.class.getClassLoader(), null, JVMStabilityInspector::uncaughtException);
        public static ExecutorFactory executorFactory()
        {
            return FACTORY;
        }

        public static void unsafeSet(ExecutorFactory executorFactory)
        {
            FACTORY = executorFactory;
        }
    }

    public static final class Default extends NamedThreadFactory.MetaFactory implements ExecutorFactory
    {
        public Default(ClassLoader contextClassLoader, ThreadGroup threadGroup, UncaughtExceptionHandler uncaughtExceptionHandler)
        {
            super(contextClassLoader, threadGroup, uncaughtExceptionHandler);
        }

        public LocalAwareSubFactory localAware()
        {
            return new LocalAwareSubFactory()
            {
                public ExecutorBuilder<? extends LocalAwareSequentialExecutorPlus> configureSequential(String name)
                {
                    return ThreadPoolExecutorBuilder.sequential(LocalAwareSingleThreadExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name);
                }

                public ExecutorBuilder<LocalAwareThreadPoolExecutorPlus> configurePooled(String name, int threads)
                {
                    return ThreadPoolExecutorBuilder.pooled(LocalAwareThreadPoolExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, threads);
                }

                public LocalAwareSubFactoryWithJMX withJmx(String jmxPath)
                {
                    return new LocalAwareSubFactoryWithJMX()
                    {
                        public ExecutorBuilder<LocalAwareSingleThreadExecutorPlus> configureSequential(String name)
                        {
                            return sequentialJmx(LocalAwareSingleThreadExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, jmxPath);
                        }

                        public ExecutorBuilder<LocalAwareThreadPoolExecutorPlus> configurePooled(String name, int threads)
                        {
                            return pooledJmx(LocalAwareThreadPoolExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, threads, jmxPath);
                        }

                        public LocalAwareExecutorPlus shared(String name, int threads, ExecutorPlus.MaximumPoolSizeListener onSetMaxSize)
                        {
                            return SharedExecutorPool.SHARED.newExecutor(threads, onSetMaxSize, jmxPath, name);
                        }
                    };
                }
            };
        }

        @Override
        public ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus> withJmx(String jmxPath)
        {
            return new ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus>()
            {
                @Override
                public ExecutorBuilder<? extends SequentialExecutorPlus> configureSequential(String name)
                {
                    return ThreadPoolExecutorBuilder.sequentialJmx(SingleThreadExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, jmxPath);
                }

                @Override
                public ExecutorBuilder<? extends ExecutorPlus> configurePooled(String name, int threads)
                {
                    return ThreadPoolExecutorBuilder.pooledJmx(ThreadPoolExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, threads, jmxPath);
                }
            };
        }

        public ExecutorBuilder<SingleThreadExecutorPlus> configureSequential(String name)
        {
            return ThreadPoolExecutorBuilder.sequential(SingleThreadExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name);
        }

        public ExecutorBuilder<ThreadPoolExecutorPlus> configurePooled(String name, int threads)
        {
            return ThreadPoolExecutorBuilder.pooled(ThreadPoolExecutorPlus::new, contextClassLoader, threadGroup, uncaughtExceptionHandler, name, threads);
        }

        public ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name, int priority)
        {
            ScheduledThreadPoolExecutorPlus executor = new ScheduledThreadPoolExecutorPlus(newThreadFactory(name, priority));
            if (!executeOnShutdown)
                executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            return executor;
        }

        public Thread startThread(String name, Runnable runnable)
        {
            Thread thread = setupThread(createThread(threadGroup, runnable, name, true), Thread.NORM_PRIORITY, contextClassLoader, uncaughtExceptionHandler);
            thread.start();
            return thread;
        }

        public Interruptible infiniteLoop(String name, Interruptible.Task task, boolean simulatorSafe)
        {
            return new InfiniteLoopExecutor(this, name, task);
        }

        @Override
        public Interruptible infiniteLoop(String name, Interruptible.Task task, boolean simulatorSafe, Consumer<Thread> interruptHandler)
        {
            return new InfiniteLoopExecutor(this, name, task, interruptHandler);
        }

        @Override
        public ThreadGroup newThreadGroup(String name)
        {
            return threadGroup == null ? null : new ThreadGroup(threadGroup, name);
        }
    }
}