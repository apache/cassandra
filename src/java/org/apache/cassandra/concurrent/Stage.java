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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ExecutorUtils;

import org.apache.cassandra.utils.FBUtilities;

import static java.util.stream.Collectors.toMap;

public enum Stage
{
    READ              (false, "ReadStage",             "request",  DatabaseDescriptor::getConcurrentReaders,        DatabaseDescriptor::setConcurrentReaders,        Stage::multiThreadedLowSignalStage),
    MUTATION          (true,  "MutationStage",         "request",  DatabaseDescriptor::getConcurrentWriters,        DatabaseDescriptor::setConcurrentWriters,        Stage::multiThreadedLowSignalStage),
    COUNTER_MUTATION  (true,  "CounterMutationStage",  "request",  DatabaseDescriptor::getConcurrentCounterWriters, DatabaseDescriptor::setConcurrentCounterWriters, Stage::multiThreadedLowSignalStage),
    VIEW_MUTATION     (true,  "ViewMutationStage",     "request",  DatabaseDescriptor::getConcurrentViewWriters,    DatabaseDescriptor::setConcurrentViewWriters,    Stage::multiThreadedLowSignalStage),
    GOSSIP            (true,  "GossipStage",           "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    REQUEST_RESPONSE  (false, "RequestResponseStage",  "request",  FBUtilities::getAvailableProcessors,             null,                                            Stage::multiThreadedLowSignalStage),
    ANTI_ENTROPY      (false, "AntiEntropyStage",      "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    MIGRATION         (false, "MigrationStage",        "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    MISC              (false, "MiscStage",             "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    TRACING           (false, "TracingStage",          "internal", () -> 1,                                         null,                                            Stage::tracingExecutor),
    INTERNAL_RESPONSE (false, "InternalResponseStage", "internal", FBUtilities::getAvailableProcessors,             null,                                            Stage::multiThreadedStage),
    IMMEDIATE         (false, "ImmediateStage",        "internal", () -> 0,                                         null,                                            Stage::immediateExecutor);

    public static final long KEEP_ALIVE_SECONDS = 60; // seconds to keep "extra" threads alive for when idle
    public final String jmxName;
    /** Set true if this executor should be gracefully shutdown before stopping
     * the commitlog allocator. Tasks on executors that issue mutations may
     * block indefinitely waiting for a new commitlog segment, preventing a
     * clean drain/shutdown.
     */
    public final boolean shutdownBeforeCommitlog;
    private final Supplier<LocalAwareExecutorService> initialiser;
    private volatile LocalAwareExecutorService executor = null;

    Stage(Boolean shutdownBeforeCommitlog, String jmxName, String jmxType, IntSupplier numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize, ExecutorServiceInitialiser initialiser)
    {
        this.shutdownBeforeCommitlog = shutdownBeforeCommitlog;
        this.jmxName = jmxName;
        this.initialiser = () -> initialiser.init(jmxName,jmxType, numThreads.getAsInt(), onSetMaximumPoolSize);
    }

    private static String normalizeName(String stageName)
    {
        // Handle discrepancy between JMX names and actual pool names
        String upperStageName = stageName.toUpperCase();
        if (upperStageName.endsWith("STAGE"))
        {
            upperStageName = upperStageName.substring(0, stageName.length() - 5);
        }
        return upperStageName;
    }

    private static final Map<String,Stage> nameMap = Arrays.stream(values())
                                                           .collect(toMap(s -> Stage.normalizeName(s.jmxName),
                                                                          s -> s));

    public static Stage fromPoolName(String stageName)
    {
        String upperStageName = normalizeName(stageName);

        Stage result = nameMap.get(upperStageName);
        if (result != null)
            return result;

        try
        {
            return valueOf(upperStageName);
        }
        catch (IllegalArgumentException e)
        {
            switch(upperStageName) // Handle discrepancy between configuration file and stage names
            {
                case "CONCURRENT_READS":
                    return READ;
                case "CONCURRENT_WRITERS":
                    return MUTATION;
                case "CONCURRENT_COUNTER_WRITES":
                    return COUNTER_MUTATION;
                case "CONCURRENT_MATERIALIZED_VIEW_WRITES":
                    return VIEW_MUTATION;
                default:
                    throw new IllegalStateException("Must be one of " + Arrays.stream(values())
                                                                              .map(Enum::toString)
                                                                              .collect(Collectors.joining(",")));
            }
        }
    }

    // Convenience functions to execute on this stage
    public void execute(Runnable command) { executor().execute(command); }
    public void execute(Runnable command, ExecutorLocals locals) { executor().execute(command, locals); }
    public void maybeExecuteImmediately(Runnable command) { executor().maybeExecuteImmediately(command); }
    public <T> Future<T> submit(Callable<T> task) { return executor().submit(task); }
    public Future<?> submit(Runnable task) { return executor().submit(task); }
    public <T> Future<T> submit(Runnable task, T result) { return executor().submit(task, result); }

    public LocalAwareExecutorService executor()
    {
        if (executor == null)
        {
            synchronized (this)
            {
                if (executor == null)
                {
                    executor = initialiser.get();
                }
            }
        }
        return executor;
    }

    private static List<ExecutorService> executors()
    {
        return Stream.of(Stage.values())
                     .map(Stage::executor)
                     .collect(Collectors.toList());
    }

    private static List<ExecutorService> mutatingExecutors()
    {
        return Stream.of(Stage.values())
                     .filter(stage -> stage.shutdownBeforeCommitlog)
                     .map(Stage::executor)
                     .collect(Collectors.toList());
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        ExecutorUtils.shutdownNow(executors());
    }

    public static void shutdownAndAwaitMutatingExecutors(boolean interrupt, long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> executors = mutatingExecutors();
        ExecutorUtils.shutdown(interrupt, executors);
        ExecutorUtils.awaitTermination(timeout, units, executors);
    }

    public static boolean areMutationExecutorsTerminated()
    {
        return mutatingExecutors().stream().allMatch(ExecutorService::isTerminated);
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> executors = executors();
        ExecutorUtils.shutdownNow(executors);
        ExecutorUtils.awaitTermination(timeout, units, executors);
    }

    static LocalAwareExecutorService tracingExecutor(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        RejectedExecutionHandler reh = (r, executor) -> MessagingService.instance().metrics.recordSelfDroppedMessage(Verb._TRACE);
        return new TracingExecutor(1,
                                   1,
                                   KEEP_ALIVE_SECONDS,
                                   TimeUnit.SECONDS,
                                   new ArrayBlockingQueue<>(1000),
                                   new NamedThreadFactory(jmxName),
                                   reh);
    }

    static LocalAwareExecutorService multiThreadedStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEP_ALIVE_SECONDS,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<>(),
                                                new NamedThreadFactory(jmxName),
                                                jmxType);
    }

    static LocalAwareExecutorService multiThreadedLowSignalStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return SharedExecutorPool.SHARED.newExecutor(numThreads, onSetMaximumPoolSize, jmxType, jmxName);
    }

    static LocalAwareExecutorService singleThreadedStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return new JMXEnabledSingleThreadExecutor(jmxName, jmxType);
    }

    static LocalAwareExecutorService immediateExecutor(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return ImmediateExecutor.INSTANCE;
    }

    @FunctionalInterface
    public interface ExecutorServiceInitialiser
    {
        public LocalAwareExecutorService init(String jmxName, String jmxType, int numThreads, LocalAwareExecutorService.MaximumPoolSizeListener onSetMaximumPoolSize);
    }

    /**
     * Returns core thread pool size
     */
    public int getCorePoolSize()
    {
        return executor().getCorePoolSize();
    }

    /**
     * Allows user to resize core thread pool size
     */
    public void setCorePoolSize(int newCorePoolSize)
    {
        executor().setCorePoolSize(newCorePoolSize);
    }

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumPoolSize()
    {
        return executor().getMaximumPoolSize();
    }

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumPoolSize(int newMaximumPoolSize)
    {
        executor().setMaximumPoolSize(newMaximumPoolSize);
    }

    /**
     * The executor used for tracing.
     */
    private static class TracingExecutor extends ThreadPoolExecutor implements LocalAwareExecutorService
    {
        TracingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler)
        {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        }

        public void execute(Runnable command, ExecutorLocals locals)
        {
            assert locals == null;
            super.execute(command);
        }

        public void maybeExecuteImmediately(Runnable command)
        {
            execute(command);
        }

        @Override
        public int getActiveTaskCount()
        {
            return getActiveCount();
        }

        @Override
        public int getPendingTaskCount()
        {
            return getQueue().size();
        }
    }
}
