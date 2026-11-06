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

package org.apache.cassandra.distributed.test.log;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DropKeyspaceTest extends TestBaseImpl
{
    @Test
    public void testDropKS() throws IOException, InterruptedException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .start()))
        {
            createTables(cluster);
            AtomicBoolean stop = new AtomicBoolean();
            Thread t = new Thread(() -> {
                while (!stop.get())
                {
                    try
                    {
                        cluster.coordinator(ThreadLocalRandom.current().nextInt(3) + 1).execute("insert into abc.tbl"+(ThreadLocalRandom.current().nextInt(4) + 1)+" (id) values (?)", ConsistencyLevel.ALL, ThreadLocalRandom.current().nextInt(1000));
                    }
                    catch (IllegalArgumentException iae)
                    {
                        assertTrue(iae.getMessage().contains("Unknown CF"));
                    }
                    catch (Exception e)
                    {
                        Throwable cause = e.getCause();

                        if (!e.getClass().getSimpleName().equals("KeyspaceNotDefinedException") &&
                            !cause.getClass().getSimpleName().equals("CoordinatorBehindException") &&
                            !cause.getClass().getSimpleName().equals("UnknownTableException"))
                        {
                            fail();
                        }
                    }
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                }
            });
            t.start();
            for (int i = 0; i < 5; i++)
            {
                long epoch = cluster.get(1).callOnInstance(() ->  ClusterMetadata.current().epoch.getEpoch());
                cluster.schemaChange("drop keyspace abc");
                createTables(cluster);
                // make sure we make progress
                assertTrue(epoch < cluster.get(1).callOnInstance(() ->  ClusterMetadata.current().epoch.getEpoch()));
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            stop.set(true);
            t.join();
        }
    }

    private static void createTables(Cluster cluster)
    {
        cluster.schemaChange("create keyspace abc with replication={'class': 'NetworkTopologyStrategy', 'datacenter0': 3}");
        cluster.schemaChange("create table abc.tbl1 (id int primary key)");
        cluster.schemaChange("create table abc.tbl2 (id int primary key)");
        cluster.schemaChange("create table abc.tbl3 (id int primary key)");
        cluster.schemaChange("create table abc.tbl4 (id int primary key)");
    }
}
