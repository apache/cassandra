/**
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentReaders;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentWriters;


/**
 * This class manages executor services for Messages recieved: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager
{
    private static Map<String, ThreadPoolExecutor> stages = new HashMap<String, ThreadPoolExecutor>();

    public final static String READ_STAGE = "ROW-READ-STAGE";
    public final static String MUTATION_STAGE = "ROW-MUTATION-STAGE";
    public final static String STREAM_STAGE = "STREAM-STAGE";
    public final static String GOSSIP_STAGE = "GS";
    public static final String RESPONSE_STAGE = "RESPONSE-STAGE";
    public final static String AE_SERVICE_STAGE = "AE-SERVICE-STAGE";
    private static final String LOADBALANCE_STAGE = "LOAD-BALANCER-STAGE";
    public static final String MIGRATION_STAGE = "MIGRATION-STAGE";

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    static
    {
        stages.put(MUTATION_STAGE, multiThreadedConfigurableStage(MUTATION_STAGE, getConcurrentWriters()));
        stages.put(READ_STAGE, multiThreadedConfigurableStage(READ_STAGE, getConcurrentReaders()));        
        stages.put(RESPONSE_STAGE, multiThreadedStage(RESPONSE_STAGE, Math.max(2, Runtime.getRuntime().availableProcessors())));
        // the rest are all single-threaded
        stages.put(STREAM_STAGE, new JMXEnabledThreadPoolExecutor(STREAM_STAGE));
        stages.put(GOSSIP_STAGE, new JMXEnabledThreadPoolExecutor("GMFD"));
        stages.put(AE_SERVICE_STAGE, new JMXEnabledThreadPoolExecutor(AE_SERVICE_STAGE));
        stages.put(LOADBALANCE_STAGE, new JMXEnabledThreadPoolExecutor(LOADBALANCE_STAGE));
        stages.put(MIGRATION_STAGE, new JMXEnabledThreadPoolExecutor(MIGRATION_STAGE));
    }

    private static ThreadPoolExecutor multiThreadedStage(String name, int numThreads)
    {
        // avoid running afoul of requirement in DebuggableThreadPoolExecutor that single-threaded executors
        // must have unbounded queues
        assert numThreads > 1 : "multi-threaded stages must have at least 2 threads";

        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(DatabaseDescriptor.getStageQueueSize()),
                                                new NamedThreadFactory(name));
    }
    
    private static ThreadPoolExecutor multiThreadedConfigurableStage(String name, int numThreads)
    {
        assert numThreads > 1 : "multi-threaded stages must have at least 2 threads";
        
        return new JMXConfigurableThreadPoolExecutor(numThreads,
                                                     numThreads,
                                                     KEEPALIVE,
                                                     TimeUnit.SECONDS,
                                                     new LinkedBlockingQueue<Runnable>(DatabaseDescriptor.getStageQueueSize()),
                                                     new NamedThreadFactory(name));
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stageName name of the stage to be retrieved.
    */
    public static ThreadPoolExecutor getStage(String stageName)
    {
        return stages.get(stageName);
    }
    
    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        Set<String> stages = StageManager.stages.keySet();
        for (String stage : stages)
        {
            StageManager.stages.get(stage).shutdownNow();
        }
    }
}
