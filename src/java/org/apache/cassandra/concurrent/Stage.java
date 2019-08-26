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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ExecutorUtils;

import static java.util.stream.Collectors.toMap;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentCounterWriters;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentReaders;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentViewWriters;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentWriters;
import static org.apache.cassandra.utils.ExecutorUtils.awaitTermination;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

public enum Stage
{
    READ              ("ReadStage",             "request",  getConcurrentReaders(),        DatabaseDescriptor::setConcurrentReaders,        Stage::multiThreadedLowSignalStage),
    MUTATION          ("MutationStage",         "request",  getConcurrentWriters(),        DatabaseDescriptor::setConcurrentWriters,        Stage::multiThreadedLowSignalStage),
    COUNTER_MUTATION  ("CounterMutationStage",  "request",  getConcurrentCounterWriters(), DatabaseDescriptor::setConcurrentCounterWriters, Stage::multiThreadedLowSignalStage),
    VIEW_MUTATION     ("ViewMutationStage",     "request",  getConcurrentViewWriters(),    DatabaseDescriptor::setConcurrentViewWriters,    Stage::multiThreadedLowSignalStage),
    GOSSIP            ("GossipStage",           "internal", 1,                             null,                                            Stage::singleThreadedStage),
    REQUEST_RESPONSE  ("RequestResponseStage",  "request",  getAvailableProcessors(),      null,                                            Stage::multiThreadedLowSignalStage),
    ANTI_ENTROPY      ("AntiEntropyStage",      "internal", 1,                             null,                                            Stage::multiThreadedStage),
    MIGRATION         ("MigrationStage",        "internal", 1,                             null,                                            Stage::multiThreadedStage),
    MISC              ("MiscStage",             "internal", 1,                             null,                                            Stage::multiThreadedStage), //TODO: ANTI_ENTROPY, MIGRATION & MISC can use Stage::singleThreadedStage
    TRACING           ("TracingStage",          "internal", 1,                             null,                                            Stage::tracingExecutor),
    INTERNAL_RESPONSE ("InternalResponseStage", "internal", getAvailableProcessors(),      null,                                            Stage::multiThreadedStage),
    IMMEDIATE         ("ImmediateStage",        "internal", 1,                             null,                                            Stage::immediateExecutor);

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle
    public final String jmxName;
    public final LocalAwareExecutorService executor;

    Stage(String jmxName, String jmxType, int numThreads, Consumer<Integer> setNumThreads, ExecutorServiceInitialiser initialiser)
    {
        this.jmxName = jmxName;
        this.executor = initialiser.init(jmxName, jmxType, numThreads, setNumThreads);
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

    private static Map<String,Stage> nameMap = Arrays.stream(values())
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

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownAllNow()
    {
        for (Stage stage : Stage.values())
        {
            stage.executor.shutdownNow();
        }
    }

    @VisibleForTesting
    public static void shutdownAllAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        List<LocalAwareExecutorService> executorServices = Stream.of(Stage.values())
                                                                 .map(stage -> stage.executor)
                                                                 .collect(Collectors.toList());

        executorServices.forEach(ExecutorUtils::shutdown);
        awaitTermination(timeout, units, executorServices);
    }

    static LocalAwareExecutorService tracingExecutor(String jmxName, String jmxType, int numThreads, Consumer<Integer> setNumThreads)
    {
        RejectedExecutionHandler reh = (r, executor) -> MessagingService.instance().metrics.recordSelfDroppedMessage(Verb._TRACE);
        return new TracingExecutor(1,
                                   1,
                                   KEEPALIVE,
                                   TimeUnit.SECONDS,
                                   new ArrayBlockingQueue<>(1000),
                                   new NamedThreadFactory(jmxName),
                                   reh);
    }

    static LocalAwareExecutorService multiThreadedStage(String jmxName, String jmxType, int numThreads, Consumer<Integer> setNumThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<>(),
                                                new NamedThreadFactory(jmxName),
                                                jmxType);
    }

    static LocalAwareExecutorService multiThreadedLowSignalStage(String jmxName, String jmxType, int numThreads, Consumer<Integer> setNumThreads)
    {
        return SharedExecutorPool.SHARED.newExecutor(numThreads, setNumThreads, Integer.MAX_VALUE, jmxType, jmxName);
    }

    static LocalAwareExecutorService singleThreadedStage(String jmxName, String jmxType, int numThreads, Consumer<Integer> setNumThreads)
    {
        return new JMXEnabledSingleThreadExecutor(jmxName, jmxType);
    }

    static LocalAwareExecutorService immediateExecutor(String jmxName, String jmxType, int numThreads, Consumer<Integer> setNumThreads)
    {
        return ImmediateExecutor.INSTANCE;
    }

    @FunctionalInterface
    public interface ExecutorServiceInitialiser
    {
        public LocalAwareExecutorService init(String jmxName, String jmxType, int numThreads, Consumer<Integer> setNumThreads);
    }

    /**
     * Returns core thread pool size
     */
    public int getCorePoolSize()
    {
        return executor.getCorePoolSize();
    }

    /**
     * Allows user to resize core thread pool size
     */
    public void setCorePoolSize(int newCorePoolSize)
    {
        executor.setCorePoolSize(newCorePoolSize);
    }

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumPoolSize()
    {
        return executor.getMaximumPoolSize();
    }

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumPoolSize(int newMaxWorkers)
    {
        executor.setMaximumPoolSize(newMaxWorkers);
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
