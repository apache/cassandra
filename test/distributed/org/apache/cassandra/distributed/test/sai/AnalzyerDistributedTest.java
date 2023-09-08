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

package org.apache.cassandra.distributed.test.sai;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.cql3.CQLTester.getRandom;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class AnalzyerDistributedTest extends TestBaseImpl
{
    @Rule
    public SAITester.FailureWatcher failureRule = new SAITester.FailureWatcher();

    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %%s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String CREATE_TABLE = "CREATE TABLE %%s (pk int PRIMARY KEY, not_analyzed int, val text)";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}";
    private static final int NUM_REPLICAS = 3;
    private static final int RF = 2;

    private static final AtomicInteger seq = new AtomicInteger();
    private static String table;

    private static Cluster cluster;

    private static int dimensionCount;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = Cluster.build(NUM_REPLICAS)
                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                         .start();

        cluster.schemaChange(withKeyspace(String.format(CREATE_KEYSPACE, RF)));
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void before()
    {
        table = "table_" + seq.getAndIncrement();
        dimensionCount = getRandom().nextIntBetween(100, 2048);
    }

    @After
    public void after()
    {
        cluster.schemaChange(formatQuery("DROP TABLE IF EXISTS %s"));
    }

    @Test
    public void testAnalyzerSearch()
    {
        cluster.schemaChange(formatQuery(String.format(CREATE_TABLE, dimensionCount)));
        cluster.schemaChange(formatQuery(String.format(CREATE_INDEX, "val")));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        var iterations = 15000;
        for (int i = 0; i < iterations; i++)
        {
            var x = i % 100;
            if (i % 100 == 0)
            {
                execute(String.format(
                "INSERT INTO %s (pk, not_analyzed, val) VALUES (%s, %s, '%s')",
                KEYSPACE + '.' + table, i, x, "this will be tokenized"));
            }
            else if (i % 2 == 0)
            {
                execute(String.format(
                "INSERT INTO %s (pk, not_analyzed, val) VALUES (%s, %s, '%s')",
                KEYSPACE + '.' + table, i, x, "this is different"));
            }
            else
            {
                execute(String.format(
                "INSERT INTO %s (pk, not_analyzed, val) VALUES (%s, %s, '%s')",
                KEYSPACE + '.' + table, i, x, "basic test"));
            }
        }
        // We match the first inserted statement here, and that one is just written 1/100 times
        var result = execute("SELECT * FROM %s WHERE val : 'tokenized'");
        assertThat(result).hasSize(iterations / 100);
        // We match the first and second inserted statements here, and those account for 1/2 the inserts
        result = execute("SELECT * FROM %s WHERE val : 'this'");
        assertThat(result).hasSize(iterations / 2);
        // We match the last write here, and that accounts for the other 1/2 of the inserts
        result = execute("SELECT * FROM %s WHERE val : 'test'");
        assertThat(result).hasSize(iterations / 2);
    }

    private static Object[][] execute(String query)
    {
        return execute(query, ConsistencyLevel.QUORUM);
    }

    private static Object[][] execute(String query, ConsistencyLevel consistencyLevel)
    {
        return cluster.coordinator(1).execute(formatQuery(query), consistencyLevel);
    }

    private static String formatQuery(String query)
    {
        return String.format(query, KEYSPACE + '.' + table);
    }
}
