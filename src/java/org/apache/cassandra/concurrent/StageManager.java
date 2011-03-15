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

import java.util.EnumMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.config.DatabaseDescriptor.*;


/**
 * This class manages executor services for Messages recieved: each Message requests
 * running on a specific "stage" for concurrency control; hence the Map approach,
 * even though stages (executors) are not created dynamically.
 */
public class StageManager
{
    private static EnumMap<Stage, ThreadPoolExecutor> stages = new EnumMap<Stage, ThreadPoolExecutor>(Stage.class);

    public static final long KEEPALIVE = 60; // seconds to keep "extra" threads alive for when idle

    static
    {
        stages.put(Stage.MUTATION, multiThreadedConfigurableStage(Stage.MUTATION, getConcurrentWriters()));
        stages.put(Stage.READ, multiThreadedConfigurableStage(Stage.READ, getConcurrentReaders()));        
        stages.put(Stage.REQUEST_RESPONSE, multiThreadedStage(Stage.REQUEST_RESPONSE, Runtime.getRuntime().availableProcessors()));
        stages.put(Stage.INTERNAL_RESPONSE, multiThreadedStage(Stage.INTERNAL_RESPONSE, Runtime.getRuntime().availableProcessors()));
        stages.put(Stage.REPLICATE_ON_WRITE, multiThreadedConfigurableStage(Stage.REPLICATE_ON_WRITE, getConcurrentReplicators()));
        // the rest are all single-threaded
        stages.put(Stage.STREAM, new JMXEnabledThreadPoolExecutor(Stage.STREAM));
        stages.put(Stage.GOSSIP, new JMXEnabledThreadPoolExecutor(Stage.GOSSIP));
        stages.put(Stage.ANTI_ENTROPY, new JMXEnabledThreadPoolExecutor(Stage.ANTI_ENTROPY));
        stages.put(Stage.MIGRATION, new JMXEnabledThreadPoolExecutor(Stage.MIGRATION));
        stages.put(Stage.MISC, new JMXEnabledThreadPoolExecutor(Stage.MISC));
        stages.put(Stage.READ_REPAIR, multiThreadedStage(Stage.READ_REPAIR, Runtime.getRuntime().availableProcessors()));
    }

    private static ThreadPoolExecutor multiThreadedStage(Stage stage, int numThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                KEEPALIVE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(),
                                                new NamedThreadFactory(stage.getJmxName()),
                                                stage.getJmxType());
    }
    
    private static ThreadPoolExecutor multiThreadedConfigurableStage(Stage stage, int numThreads)
    {
        return new JMXConfigurableThreadPoolExecutor(numThreads,
                                                     KEEPALIVE,
                                                     TimeUnit.SECONDS,
                                                     new LinkedBlockingQueue<Runnable>(),
                                                     new NamedThreadFactory(stage.getJmxName()),
                                                     stage.getJmxType());
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stage name of the stage to be retrieved.
    */
    public static ThreadPoolExecutor getStage(Stage stage)
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
}
