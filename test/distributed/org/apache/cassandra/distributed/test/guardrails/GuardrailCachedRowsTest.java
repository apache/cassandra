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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.exceptions.OverloadedException;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Guardrails#cachedRows}.
 * A brief version of {@link org.apache.cassandra.distributed.test.ReplicaFilteringProtectionTest}.
 */
public class GuardrailCachedRowsTest extends GuardrailTester
{
    private static Cluster cluster;

    private static final int CACHED_ROWS_WARN_THRESHOLD = 5;
    private static final int CACHED_ROWS_FAIL_THRESHOLD = 11;
    private static final int REPLICAS = 2;
    private static final int PARTITIONS = 3;
    private static final int ROWS_PER_PARTITION = 6;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(REPLICAS)
                              .withConfig(c -> c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP))
                              .withConfig(c -> c.set("replica_filtering_protection.cached_rows_warn_threshold", CACHED_ROWS_WARN_THRESHOLD)
                                                .set("replica_filtering_protection.cached_rows_fail_threshold", CACHED_ROWS_FAIL_THRESHOLD))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @Test
    public void testCachedRowsGuardrail() throws Throwable
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS cached_rows_guardrail WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '"+ REPLICAS +"'}");
        schemaChange("CREATE TABLE IF NOT EXISTS cached_rows_guardrail.users (id int, name text, age int, PRIMARY KEY (id, name))");

        for (int i = 0; i < PARTITIONS; i++)
            for (int j = 0; j < ROWS_PER_PARTITION; j++)
                execute("INSERT INTO cached_rows_guardrail.users (id, name, age) VALUES (?, ?, ?)", i, "name_" + j, 20);

        // Noraml case:
        updateAllRowsOnlyOnOneNode(1, 2);
        SimpleQueryResult oldResult = cluster.coordinator(1)
                                             .executeWithResult("SELECT * FROM cached_rows_guardrail.users WHERE age = 200 ALLOW FILTERING",
                                              ConsistencyLevel.ALL);
        assertEquals(2, oldResult.toObjectArrays().length);
        Assert.assertTrue(oldResult.warnings().isEmpty());

        // Warn case:
        updateAllRowsOnlyOnOneNode(2, 3);
        oldResult = cluster.coordinator(1).executeWithResult("SELECT * FROM cached_rows_guardrail.users WHERE age = 200 ALLOW FILTERING",
                                              ConsistencyLevel.ALL);
        assertEquals(6, oldResult.toObjectArrays().length);
        Assert.assertFalse(oldResult.warnings().isEmpty());

        // Fail case:
        try
        {
            updateAllRowsOnlyOnOneNode(2, 6);
            cluster.coordinator(1).executeWithResult("SELECT * FROM cached_rows_guardrail.users WHERE age = 200 ALLOW FILTERING",
                                                                 ConsistencyLevel.ALL);
            Assert.fail("should meet the cached_rows_fail_threshold and throw exception");
        }
        catch (RuntimeException e)
        {
            assertEquals(OverloadedException.class.getName(), e.getClass().getName());
            Assert.assertTrue(e.getMessage().contains("cached_rows_fail_threshold"));
        }
    }

    private void execute(String query, Object... args)
    {
        cluster.coordinator(1).execute(format(query), ConsistencyLevel.ALL, args);
    }

    private void updateAllRowsOnlyOnOneNode(int partitions, int rowsPerPartitions)
    {
        for (int i = 0; i < partitions; i++)
            for (int j = 0; j < rowsPerPartitions; j++)
                cluster.get(1).executeInternal("update cached_rows_guardrail.users SET age = 200 WHERE id = ? and name = ?", i, "name_" + j);
    }
}
