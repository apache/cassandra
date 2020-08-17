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

package org.apache.cassandra.distributed.upgrade;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

public class CompactStorage3to4UpgradeTest extends UpgradeTestBase
{

    public static final String TABLE_NAME = "cs_tbl";
    public static final String CREATE_TABLE_C1_R1 = String.format(
         "CREATE TABLE %s.%s (key int, c1 int, v int, PRIMARY KEY (key, c1)) WITH COMPACT STORAGE",
         KEYSPACE, TABLE_NAME);
    public static final String CREATE_TABLE_C1_ONLY = String.format(
         "CREATE TABLE %s.%s (key int, c1 int, PRIMARY KEY (key, c1)) WITH COMPACT STORAGE",
         KEYSPACE, TABLE_NAME);
    public static final String CREATE_TABLE_R_ONLY = String.format(
    "CREATE TABLE %s.%s (key int, c1 int, c2 int, PRIMARY KEY (key)) WITH COMPACT STORAGE",
    KEYSPACE, TABLE_NAME);

    public static final String INSERT_C1_R1 = String.format(
         "INSERT INTO %s.%s (key, c1, v) VALUES (?, ?, ?)",
         KEYSPACE, TABLE_NAME);

    @Test
    public void ignoreDenseCompoundTablesWithValueColumn() throws Throwable
    {
        System.setProperty("cassandra.auto_drop_compact_storage", "true");
        final int partitions = 10;
        final int rowsPerPartition = 10;

        DropCompactTestHelper helper = new DropCompactTestHelper();
        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .setup(cluster -> {
            cluster.schemaChange(CREATE_TABLE_C1_R1);

            ICoordinator coordinator = cluster.coordinator(1);
            for (int i = 1; i <= partitions; i++)
                for (int j = 1; j <= rowsPerPartition; j++)
                    coordinator.execute(INSERT_C1_R1, ConsistencyLevel.ALL, i, j, i + j);


            runQueries(coordinator, helper, new String[]{
                String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE_NAME),

                String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                              KEYSPACE, TABLE_NAME, partitions - 3, rowsPerPartition - 2),

                String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                              KEYSPACE, TABLE_NAME, partitions - 1, rowsPerPartition - 5),

                String.format("SELECT * FROM %s.%s WHERE key = %d and c1 > %d",
                              KEYSPACE, TABLE_NAME, partitions - 8, rowsPerPartition - 3),
            });
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            validateResults(helper, cluster, 1);
            validateResults(helper, cluster, 2);

            String flagQuery = String.format("SELECT flags FROM system_schema.tables WHERE keyspace_name='%s' and table_name='%s'", KEYSPACE, TABLE_NAME);
            Object[][] results = cluster.get(node).executeInternal(flagQuery);
            if (results.length != 1)
                Assert.fail("failed to find table flags with query: " + flagQuery);

            Set<String> flags = (Set) results[0][0];
            Assert.assertTrue("missing compound flag", flags.contains("compound"));
            Assert.assertFalse("found dense flag", flags.contains("dense"));
        })
        .run();
    }

    @Test
    public void failOnCompactClusteredTablesWithValueOutColumn() throws Throwable
    {
        try
        {
            new TestCase()
            .nodes(2)
            .upgrade(Versions.Major.v30, Versions.Major.v4)
            .setup(cluster -> cluster.schemaChange(CREATE_TABLE_C1_ONLY))
            .runAfterNodeUpgrade((cluster, node) -> {
                Assert.fail("should never run because we don't expect the node to start");
            })
            .run();
        } catch (RuntimeException e)
        {
            validateError(e);
        }
    }

    @Test
    public void failOnCompactTablesWithNoClustering() throws Throwable
    {
        try
        {
            new TestCase()
            .nodes(2)
            .upgrade(Versions.Major.v30, Versions.Major.v4)
            .setup(cluster -> cluster.schemaChange(CREATE_TABLE_R_ONLY))
            .runAfterNodeUpgrade((cluster, node) -> {
                Assert.fail("should never run because we don't expect the node to start");
            })
            .run();
        } catch (RuntimeException e)
        {
            validateError(e);
        }
    }


    public void validateResults(DropCompactTestHelper helper, UpgradeableCluster cluster, int node)
    {
        validateResults(helper, cluster, node, ConsistencyLevel.ALL);
    }

    public void validateResults(DropCompactTestHelper helper, UpgradeableCluster cluster, int node, ConsistencyLevel cl)
    {
        for (Map.Entry<String, Object[][]> entry : helper.queriesAndResults().entrySet())
        {
            Object[][] postUpgradeResult = cluster.coordinator(node).execute(entry.getKey(), cl);
            assertRows(postUpgradeResult, entry.getValue());
        }

    }

    private void runQueries(ICoordinator coordinator, DropCompactTestHelper helper, String[] queries)
    {
        for (String query : queries)
            helper.addResult(query, coordinator.execute(query, ConsistencyLevel.ALL));
    }

    private void validateError(Throwable t)
    {
        Throwable cause = t.getCause();
        if (cause instanceof StartupException)
        {
            Assert.assertTrue("Message was: " + cause.getMessage(),
                              cause.getMessage().contains(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE", KEYSPACE, TABLE_NAME)));
        }

    }

    public static class DropCompactTestHelper
    {
        final private Map<String, Object[][]> preUpgradeResults = new HashMap<>();

        public void addResult(String query, Object[][] results)
        {
            preUpgradeResults.put(query, results);
        }

        public Map<String, Object[][]> queriesAndResults()
        {
            return preUpgradeResults;
        }
    }

}
