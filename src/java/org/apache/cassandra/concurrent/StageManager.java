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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentWriters;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentReaders;


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

    static
    {
        stages.put(MUTATION_STAGE, multiThreadedStage(MUTATION_STAGE, getConcurrentWriters()));
        stages.put(READ_STAGE, multiThreadedStage(READ_STAGE, getConcurrentReaders()));
        stages.put(RESPONSE_STAGE, multiThreadedStage("RESPONSE-STAGE", MessagingService.MESSAGE_DESERIALIZE_THREADS));
        // the rest are all single-threaded
        stages.put(STREAM_STAGE, new JMXEnabledThreadPoolExecutor(STREAM_STAGE));
        stages.put(GOSSIP_STAGE, new JMXEnabledThreadPoolExecutor("GMFD"));
        stages.put(AE_SERVICE_STAGE, new JMXEnabledThreadPoolExecutor(AE_SERVICE_STAGE));
        stages.put(LOADBALANCE_STAGE, new JMXEnabledThreadPoolExecutor(LOADBALANCE_STAGE));
    }

    private static ThreadPoolExecutor multiThreadedStage(String name, int numThreads)
    {
        return new JMXEnabledThreadPoolExecutor(numThreads,
                                                numThreads,
                                                Integer.MAX_VALUE,
                                                TimeUnit.SECONDS,
                                                new LinkedBlockingQueue<Runnable>(),
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
    public static void shutdown()
    {
        Set<String> stages = StageManager.stages.keySet();
        for (String stage : stages)
        {
            StageManager.stages.get(stage).shutdown();
        }
    }
}
