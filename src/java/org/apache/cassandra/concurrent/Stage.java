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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ExecutorUtils;

import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentCounterWriters;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentReaders;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentViewWriters;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentWriters;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

public enum Stage
{

    READ              ("ReadStage",             "request",  getConcurrentReaders(),        Stage::multiThreadedLowSignalStage),
    MUTATION          ("MutationStage",         "request",  getConcurrentWriters(),        Stage::multiThreadedLowSignalStage),
    COUNTER_MUTATION  ("CounterMutationStage",  "request",  getConcurrentCounterWriters(), Stage::multiThreadedLowSignalStage),
    VIEW_MUTATION     ("ViewMutationStage",     "request",  getConcurrentViewWriters(),    Stage::multiThreadedLowSignalStage),
    GOSSIP            ("GossipStage",           "internal", 1,                             Stage::singleThreadedStage),
    REQUEST_RESPONSE  ("RequestResponseStage",  "request",  getAvailableProcessors(),      Stage::multiThreadedLowSignalStage),
    ANTI_ENTROPY      ("AntiEntropyStage",      "internal", 1,                             Stage::multiThreadedStage),
    MIGRATION         ("MigrationStage",        "internal", 1,                             Stage::multiThreadedStage),
    MISC              ("MiscStage",             "internal", 1,                             Stage::multiThreadedStage), //TODO: ANTI_ENTROPY, MIGRATION & MISC can use Stage::singleThreadedStage
    TRACING           ("TracingStage",          "internal", 1,                             Stage::tracingExecutor),
    INTERNAL_RESPONSE ("InternalResponseStage", "internal", getAvailableProcessors(),      Stage::multiThreadedStage),
    IMMEDIATE         ("ImmediateStage",        "internal", 0,                             Stage::immediateExecutor);

    public static final long KEEP_ALIVE_SECONDS = 60; // seconds to keep "extra" threads alive for when idle
    public final LocalAwareExecutorService executor;


    Stage(String jmxName, String jmxType, int numThreads, ExecutorServiceInitialiser initialiser)
    {
        this.executor = initialiser.init(jmxName, jmxType, numThreads);
    }

    private static List<ExecutorService> executors()
    {
        return Stream.of(Stage.values())
                     .map(stage -> stage.executor)
                     .collect(Collectors.toList());
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        ExecutorUtils.shutdownNow(executors());
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> executors = executors();
        ExecutorUtils.shutdownNow(executors);
        ExecutorUtils.awaitTermination(timeout, units, executors);
    }

    static LocalAwareExecutorService tracingExecutor(String jmxName, String jmxType, int numThreads)
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

    static LocalAwareExecutorService multiThreadedStage(String jmxName, String jmxType, int numThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEP_ALIVE_SECONDS,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<>(),
                                                new NamedThreadFactory(jmxName),
                                                jmxType);
    }

    static LocalAwareExecutorService multiThreadedLowSignalStage(String jmxName, String jmxType, int numThreads)
    {
        return SharedExecutorPool.SHARED.newExecutor(numThreads, Integer.MAX_VALUE, jmxType, jmxName);
    }

    static LocalAwareExecutorService singleThreadedStage(String jmxName, String jmxType, int numThreads)
    {
        return new JMXEnabledSingleThreadExecutor(jmxName, jmxType);
    }

    static LocalAwareExecutorService immediateExecutor(String jmxName, String jmxType, int numThreads)
    {
        return ImmediateExecutor.INSTANCE;
    }

    @FunctionalInterface
    public interface ExecutorServiceInitialiser
    {
        public LocalAwareExecutorService init(String jmxName, String jmxType, int numThreads);
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
