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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

public class SecondaryIndexTest extends TestBaseImpl
{
    private static final int NUM_NODES = 3;
    private static final int REPLICATION_FACTOR = 1;
    private static final String CREATE_TABLE = "CREATE TABLE %s(k int, v int, PRIMARY KEY (k))";
    private static final String CREATE_INDEX = "CREATE INDEX v_index ON %s(v)";

    private static final AtomicInteger seq = new AtomicInteger();
    private static String tableName;
    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NUM_NODES).start(), REPLICATION_FACTOR);
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        // create the table
        tableName = String.format("%s.t_%d", KEYSPACE, seq.getAndIncrement());
        cluster.schemaChange(String.format(CREATE_TABLE, tableName));
        cluster.schemaChange(String.format(CREATE_INDEX, tableName));
    }

    @After
    public void after()
    {
        cluster.schemaChange(String.format("DROP TABLE %s", tableName));
    }

    @Test
    public void test_only_coordinator_chooses_index_for_query() throws InterruptedException, UnknownHostException
    {
        for (int i = 0 ; i < 99 ; ++i)
            cluster.coordinator(1).execute(String.format("INSERT INTO %s (k, v) VALUES (?, ?)", tableName), ConsistencyLevel.ALL, i, i/3);
        cluster.forEach(i -> i.flush(KEYSPACE));

        for (int i = 0 ; i < 33 ; ++i)
        {
            UUID trace = UUID.randomUUID();
            Object[][] result = cluster.coordinator(1).executeWithTracing(trace, String.format("SELECT * FROM %s WHERE v = ?", tableName), ConsistencyLevel.ALL, i);
            Assert.assertEquals(3, result.length);
            Thread.sleep(100L);
            Object[][] traces = cluster.coordinator(1).execute(String.format("SELECT source, activity FROM system_traces.events WHERE session_id = ?", tableName), ConsistencyLevel.ALL, trace);
            List<InetAddress> scanning = Arrays.stream(traces)
                                               .filter(t -> t[1].toString().matches("Index mean cardinalities are v_index:[0-9]+. Scanning with v_index."))
                                               .map(t -> (InetAddress) t[0])
                                               .distinct().collect(Collectors.toList());

            List<InetAddress> executing = Arrays.stream(traces)
                                                .filter(t -> t[1].toString().equals("Executing read on " + tableName + " using index v_index"))
                                                .map(t -> (InetAddress) t[0])
                                                .distinct().collect(Collectors.toList());

            Assert.assertEquals(Collections.singletonList(cluster.get(1).broadcastAddress().getAddress()), scanning);
            Assert.assertEquals(3, executing.size());
        }
    }
}
