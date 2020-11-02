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

import java.util.EnumMap;
import java.util.concurrent.*;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.DatabaseDescriptor.*;


/**
 * This class manages executor services for Messages recieved: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager
{
    private static final Logger logger = LoggerFactory.getLogger(StageManager.class);

    private static final EnumMap<Stage, LocalAwareExecutorService> stages = new EnumMap<Stage, LocalAwareExecutorService>(Stage.class);

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    static
    {
        stages.put(Stage.MUTATION, multiThreadedLowSignalStage(Stage.MUTATION, getConcurrentWriters()));
        stages.put(Stage.COUNTER_MUTATION, multiThreadedLowSignalStage(Stage.COUNTER_MUTATION, getConcurrentCounterWriters()));
        stages.put(Stage.VIEW_MUTATION, multiThreadedLowSignalStage(Stage.VIEW_MUTATION, getConcurrentViewWriters()));
        stages.put(Stage.READ, multiThreadedLowSignalStage(Stage.READ, getConcurrentReaders()));
        stages.put(Stage.REQUEST_RESPONSE, multiThreadedLowSignalStage(Stage.REQUEST_RESPONSE, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.INTERNAL_RESPONSE, multiThreadedStage(Stage.INTERNAL_RESPONSE, FBUtilities.getAvailableProcessors()));
        // the rest are all single-threaded
        stages.put(Stage.GOSSIP, new JMXEnabledThreadPoolExecutor(Stage.GOSSIP));
        stages.put(Stage.ANTI_ENTROPY, new JMXEnabledThreadPoolExecutor(Stage.ANTI_ENTROPY));
        stages.put(Stage.MIGRATION, new JMXEnabledThreadPoolExecutor(Stage.MIGRATION));
        stages.put(Stage.MISC, new JMXEnabledThreadPoolExecutor(Stage.MISC));
        stages.put(Stage.READ_REPAIR, multiThreadedStage(Stage.READ_REPAIR, FBUtilities.getAvailableProcessors()));
        stages.put(Stage.TRACING, tracingExecutor());
    }

    private static ExecuteOnlyExecutor tracingExecutor()
    {
        RejectedExecutionHandler reh = new RejectedExecutionHandler()
        {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
            {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb._TRACE);
            }
        };
        return new ExecuteOnlyExecutor(1,
                                       1,
                                       KEEPALIVE,
                                       TimeUnit.SECONDS,
                                       new ArrayBlockingQueue<Runnable>(1000),
                                       new NamedThreadFactory(Stage.TRACING.getJmxName()),
                                       reh);
    }

    private static JMXEnabledThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(),
                                                new NamedThreadFactory(stage.getJmxName()),
                                                stage.getJmxType());
    }

    private static LocalAwareExecutorService multiThreadedLowSignalStage(Stage stage, int numThreads)
    {
        return SharedExecutorPool.SHARED.newExecutor(numThreads, stage.getJmxType(), stage.getJmxName());
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stage name of the stage to be retrieved.
     */
    public static LocalAwareExecutorService getStage(Stage stage)
    {
        return stages.get(stage);
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        for (Stage stage : Stage.values())
        {
            StageManager.stages.get(stage).shutdownNow();
        }
    }

    public final static Runnable NO_OP_TASK = new Runnable()
    {
        public void run()
        {

        }
    };

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, StageManager.stages.values());
    }

    /**
     * A TPE that disallows submit so that we don't need to worry about unwrapping exceptions on the
     * tracing stage.  See CASSANDRA-1123 for background. We allow submitting NO_OP tasks, to allow
     * a final wait on pending trace events since typically the tracing executor is single-threaded, see
     * CASSANDRA-11465.
     */
    private static class ExecuteOnlyExecutor extends ThreadPoolExecutor implements LocalAwareExecutorService
    {
        public ExecuteOnlyExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler)
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
        public Future<?> submit(Runnable task)
        {
            if (task.equals(NO_OP_TASK))
            {
                assert getMaximumPoolSize() == 1 : "Cannot wait for pending tasks if running more than 1 thread";
                return super.submit(task);
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            throw new UnsupportedOperationException();
        }
    }
}
