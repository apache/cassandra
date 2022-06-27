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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class QueriesTableTest extends TestBaseImpl
{
    public static final int ITERATIONS = 256;

    @Test
    public void shouldExposeReadsAndWrites() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            ExecutorService executor = Executors.newFixedThreadPool(16);
            
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int primary key, v int)");

            AtomicInteger reads = new AtomicInteger(0);
            AtomicInteger writes = new AtomicInteger(0);
            AtomicInteger paxos = new AtomicInteger(0);
            
            for (int i = 0; i < ITERATIONS; i++)
            {
                int k = i;
                executor.execute(() -> cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (" + k + ", 0)", ConsistencyLevel.ALL));
                executor.execute(() -> cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 10 WHERE k = " + (k - 1) + " IF v = 0", ConsistencyLevel.ALL));
                executor.execute(() -> cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = " + (k - 1), ConsistencyLevel.ALL));

                executor.execute(() ->
                {
                    SimpleQueryResult result = cluster.get(1).executeInternalWithResult("SELECT * FROM system_views.queries");
                    
                    while (result.hasNext())
                    {
                        Row row = result.next();
                        String threadId = row.get("thread_id").toString();
                        String task = row.get("task").toString();

                        if (threadId.contains("Read") && task.contains("SELECT"))
                            reads.incrementAndGet();
                        else if (threadId.contains("Mutation") && task.contains("Mutation"))
                            writes.incrementAndGet();
                        else if (threadId.contains("Mutation") && task.contains("Paxos"))
                            paxos.incrementAndGet();
                    }
                });
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
            
            // We should see at least one read, write, and conditional update in the "queries" table.
            assertThat(reads.get()).isGreaterThan(0).isLessThanOrEqualTo(ITERATIONS);
            assertThat(writes.get()).isGreaterThan(0).isLessThanOrEqualTo(ITERATIONS);
            assertThat(paxos.get()).isGreaterThan(0).isLessThanOrEqualTo(ITERATIONS);
        }
    }
}
