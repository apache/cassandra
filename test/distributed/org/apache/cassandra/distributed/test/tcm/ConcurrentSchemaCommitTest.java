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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Concurrent DDL can exhaust its retry budget when local proposals race for the same epoch.
 * Unlike DistributedLogTest.testConcurrentCommit, these tests require every submitted statement to succeed.
 */
public class ConcurrentSchemaCommitTest extends TestBaseImpl
{
    @Test
    public void concurrentSchemaChangesOnOneCMSMember() throws Exception
    {
        concurrentSchemaChanges(1, 4, 4);
    }

    @Test
    public void concurrentSchemaChangesOnThreeCMSMembers() throws Exception
    {
        concurrentSchemaChanges(3, 4, 4);
    }

    @Test
    @Ignore("Manual stress workload; deadline expiry depends on available CPU and I/O")
    public void concurrentSchemaChangesStress() throws Exception
    {
        concurrentSchemaChanges(1, 48, 18);
    }

    private void concurrentSchemaChanges(int nodes, int workers, int tables) throws Exception
    {
        try (Cluster cluster = Cluster.build(nodes)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set("request_timeout", "60s"))
                                      .start())
        {
            if (1 < nodes)
                cluster.get(1).nodetoolResult("cms", "reconfigure", Integer.toString(nodes)).asserts().success();
            // Create the keyspace first so missing-schema failures cannot cascade into the workload.
            cluster.schemaChange("CREATE KEYSPACE concurrent_ddl WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 1}");
            ExecutorService executor = Executors.newFixedThreadPool(workers);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            try
            {
                for (int w = 0; w < workers; ++w)
                {
                    int worker = w;
                    futures.add(executor.submit(() -> {
                        start.await();
                        for (int t = 0; t < tables; ++t)
                            cluster.coordinator(1 + worker % nodes).execute("CREATE TABLE concurrent_ddl.t_" + worker + '_' + t +
                                                                           " (k int PRIMARY KEY, v text)", ConsistencyLevel.ONE);
                        return null;
                    }));
                }
                start.countDown();
                // Propagate worker failures; counting only successful commits would hide the regression.
                for (Future<?> future : futures)
                    future.get(5, TimeUnit.MINUTES);
                cluster.schemaChange("CREATE TABLE concurrent_ddl.barrier (k int PRIMARY KEY)");
                for (int node = 1; node <= nodes; ++node)
                    assertEquals(workers * tables + 1,
                                 cluster.coordinator(node).execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                                                                   ConsistencyLevel.ONE, "concurrent_ddl").length);
            }
            finally
            {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }
}
