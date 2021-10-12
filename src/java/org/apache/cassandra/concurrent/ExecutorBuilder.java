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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Configure an executor before creating it.
 * See {@link ThreadPoolExecutorBuilder}
 */
@Shared(scope = SIMULATION)
public interface ExecutorBuilder<E extends ExecutorService>
{
    /**
     * Threads for the executor built by this factory will timeout (terminate) after the specified period.
     */
    ExecutorBuilder<E> withKeepAlive(long keepAlive, TimeUnit keepAliveUnits);

    /**
     * Core threads for the executor built by this factory will never timeout (default for single threaded builders).
     * Note that there is ordinarily no difference between core and non-core threads; only when the queue limit is zero
     * do we create non-core threads.
     */
    ExecutorBuilder<E> withKeepAlive();

    /**
     * Specify the priority of threads that service the executor built by this factory (default to {@link Thread#NORM_PRIORITY})
     */
    ExecutorBuilder<E> withThreadPriority(int threadPriority);

    /**
     * Threads for the executor built by this factory will all be (transitively) members of {@code threadGroup},
     * but may directly reside in a child thread group.
     */
    ExecutorBuilder<E> withThreadGroup(ThreadGroup threadGroup);

    /**
     * Use the system default thread group for the threads we create.
     * This is used only for testing, so that we do not hold onto a transitive global reference to all threads.
     */
    @VisibleForTesting
    ExecutorBuilder<E> withDefaultThreadGroup();

    /**
     * The executor built by this factory will limit the number of queued tasks; default is unlimited.
     * Once the queue limit is reached and all threads are executing tasks will be rejected
     * (see {@link #withRejectedExecutionHandler(RejectedExecutionHandler)})
     */
    ExecutorBuilder<E> withQueueLimit(int queueLimit);

    /**
     * Set the {@link RejectedExecutionHandler} for the executor built by this factory.
     * By default this is executor-specific, either:
     * <li> {@link ThreadPoolExecutorBase#blockingExecutionHandler}
     * <li> {@link ScheduledThreadPoolExecutorPlus#rejectedExecutionHandler}
     * <li> and maybe wrapped by {@link ThreadPoolExecutorJMXAdapter#rejectedExecutionHandler}
     */
    ExecutorBuilder<E> withRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler);

    /**
     * Set the {@link UncaughtExceptionHandler} for threads that service executors built by this factory.
     * By default {@link JVMStabilityInspector#uncaughtException(Thread, Throwable)}
     */
    ExecutorBuilder<E> withUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler);

    /**
     * Build the configured executor
     */
    E build();
}
