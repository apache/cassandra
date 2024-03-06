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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.SimpleSeedProvider;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class SplitBrainTest extends TestBaseImpl
{
    @Test
    public void testSplitBrainStartup() throws IOException, TimeoutException
    {
        // partition the cluster in 2 parts on startup, node1, node2 in one, node3, node4 in the other
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                    .set("seed_provider", new IInstanceConfig.ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                                                                                                 Collections.singletonMap("seeds", "127.0.0.1,127.0.0.3")))
                                                                    .set("discovery_timeout", "1s"))
                                        .createWithoutStarting())
        {
            cluster.filters().allVerbs().from(1,2).to(3,4).drop();
            cluster.filters().allVerbs().from(3,4).to(1,2).drop();
            List<Thread> startupThreads = new ArrayList<>(4);
            for (int i = 0; i < 4; i++)
            {
                int threadNr = i + 1;
                startupThreads.add(new Thread(() -> cluster.get(threadNr).startup()));
            }
            startupThreads.forEach(Thread::start);
            startupThreads.forEach(SplitBrainTest::join);
            cluster.filters().reset();
            cluster.coordinator(1).execute(withKeyspace("create keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':1}"), ConsistencyLevel.ALL);

            cluster.get(3).logs().watchFor("Cluster Metadata Identifier mismatch");
        }
    }

    private static void join(Thread t)
    {
        try
        {
            t.join();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
