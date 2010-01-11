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
    private static Map<String, IStage> stageQueues = new HashMap<String, IStage>();

    public final static String READ_STAGE = "ROW-READ-STAGE";
    public final static String MUTATION_STAGE = "ROW-MUTATION-STAGE";
    public final static String STREAM_STAGE = "STREAM-STAGE";
    public final static String GOSSIP_STAGE = "GS";
    public static final String RESPONSE_STAGE = "RESPONSE-STAGE";
    public final static String AE_SERVICE_STAGE = "AE-SERVICE-STAGE";
    private static final String LOADBALANCE_STAGE = "LOAD-BALANCER-STAGE";

    static
    {
        stageQueues.put(MUTATION_STAGE, new MultiThreadedStage(MUTATION_STAGE, getConcurrentWriters()));
        stageQueues.put(READ_STAGE, new MultiThreadedStage(READ_STAGE, getConcurrentReaders()));
        stageQueues.put(STREAM_STAGE, new SingleThreadedStage(STREAM_STAGE));
        stageQueues.put(GOSSIP_STAGE, new SingleThreadedStage("GMFD"));
        stageQueues.put(RESPONSE_STAGE, new MultiThreadedStage("RESPONSE-STAGE", MessagingService.MESSAGE_DESERIALIZE_THREADS));
        stageQueues.put(AE_SERVICE_STAGE, new SingleThreadedStage(AE_SERVICE_STAGE));
        stageQueues.put(LOADBALANCE_STAGE, new SingleThreadedStage(LOADBALANCE_STAGE));
    }

    /**
     * Retrieve a stage from the StageManager
     * @param stageName name of the stage to be retrieved.
    */
    public static IStage getStage(String stageName)
    {
        return stageQueues.get(stageName);
    }
    
    /**
     * This method shuts down all registered stages.
     */
    public static void shutdown()
    {
        Set<String> stages = stageQueues.keySet();
        for ( String stage : stages )
        {
            IStage registeredStage = stageQueues.get(stage);
            registeredStage.shutdown();
        }
    }
}
