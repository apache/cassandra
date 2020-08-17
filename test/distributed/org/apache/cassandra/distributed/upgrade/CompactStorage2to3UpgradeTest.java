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

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.Versions;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;

public class CompactStorage2to3UpgradeTest extends UpgradeTestBase
{
    @Test
    public void multiColumn() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            assert cluster.size() == 3;
            int rf = cluster.size() - 1;
            assert rf == 2;
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + (cluster.size() - 1) + "};");
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, v1 int, v2 text, PRIMARY KEY (pk)) WITH COMPACT STORAGE");
            ICoordinator coordinator = cluster.coordinator(1);
            // these shouldn't be replicated by the 3rd node
            coordinator.execute("INSERT INTO ks.tbl (pk, v1, v2) VALUES (3, 3, '3')", ConsistencyLevel.ALL);
            coordinator.execute("INSERT INTO ks.tbl (pk, v1, v2) VALUES (9, 9, '9')", ConsistencyLevel.ALL);
            for (int i = 0; i < cluster.size(); i++)
            {
                int nodeNum = i + 1;
                System.out.println(String.format("****** node %s: %s", nodeNum, cluster.get(nodeNum).config()));
            }
        })
        .runAfterNodeUpgrade(((cluster, node) -> {
            if (node != 2)
                return;

            Object[][] rows = cluster.coordinator(3).execute("SELECT * FROM ks.tbl LIMIT 2", ConsistencyLevel.ALL);
            Object[][] expected = {
            row(9, 9, "9"),
            row(3, 3, "3")
            };
            assertRows(rows, expected);
        })).run();
    }

    @Test
    public void singleColumn() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            assert cluster.size() == 3;
            int rf = cluster.size() - 1;
            assert rf == 2;
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + (cluster.size() - 1) + "};");
            cluster.schemaChange("CREATE TABLE ks.tbl (pk int, v int, PRIMARY KEY (pk)) WITH COMPACT STORAGE");
            ICoordinator coordinator = cluster.coordinator(1);
            // these shouldn't be replicated by the 3rd node
            coordinator.execute("INSERT INTO ks.tbl (pk, v) VALUES (3, 3)", ConsistencyLevel.ALL);
            coordinator.execute("INSERT INTO ks.tbl (pk, v) VALUES (9, 9)", ConsistencyLevel.ALL);
            for (int i = 0; i < cluster.size(); i++)
            {
                int nodeNum = i + 1;
                System.out.println(String.format("****** node %s: %s", nodeNum, cluster.get(nodeNum).config()));
            }
        })
        .runAfterNodeUpgrade(((cluster, node) -> {

            if (node < 2)
                return;

            Object[][] rows = cluster.coordinator(3).execute("SELECT * FROM ks.tbl LIMIT 2", ConsistencyLevel.ALL);
            Object[][] expected = {
            row(9, 9),
            row(3, 3)
            };
            assertRows(rows, expected);
        })).run();
    }

    @Test
    public void testDropCompactWithClusteringAndValueColumn() throws Throwable
    {
        final String table = "clustering_and_value";
        final int partitions = 10;
        final int rowsPerPartition = 10;

        final ResultsRecorder recorder = new ResultsRecorder();
        new TestCase()
                .nodes(2)
                .upgrade(Versions.Major.v22, Versions.Major.v30)
                .setup(cluster -> {
                    cluster.schemaChange(String.format(
                            "CREATE TABLE %s.%s (key int, c1 int, c2 int, c3 int, PRIMARY KEY (key, c1, c2)) WITH COMPACT STORAGE",
                            KEYSPACE, table));
                    ICoordinator coordinator = cluster.coordinator(1);


                    for (int i = 1; i <= partitions; i++)
                    {
                        for (int j = 1; j <= rowsPerPartition; j++)
                        {
                            coordinator.execute(String.format("INSERT INTO %s.%s (key, c1, c2, c3) VALUES (%d, %d, 1, 1)",
                                    KEYSPACE, table, i, j), ConsistencyLevel.ALL);
                            coordinator.execute(String.format("INSERT INTO %s.%s (key, c1, c2, c3) VALUES (%d, %d, 2, 1)",
                                                              KEYSPACE, table, i, j), ConsistencyLevel.ALL);
                        }
                    }

                    runQueries(cluster.coordinator(1), recorder, new String[] {
                            String.format("SELECT * FROM %s.%s", KEYSPACE, table),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                                    KEYSPACE, table, partitions - 3, rowsPerPartition - 2),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                                    KEYSPACE, table, partitions - 1, rowsPerPartition - 5),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d and c2 = %d",
                                    KEYSPACE, table, partitions - 1, rowsPerPartition - 5, 1),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d and c2 = %d",
                                    KEYSPACE, table, partitions - 4, rowsPerPartition - 9, 1),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d and c2 > %d",
                                          KEYSPACE, table, partitions - 4, rowsPerPartition - 9, 1),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d and c2 > %d",
                                          KEYSPACE, table, partitions - 4, rowsPerPartition - 9, 2),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 > %d",
                                    KEYSPACE, table, partitions - 8, rowsPerPartition - 3),

                    });
                }).runAfterClusterUpgrade(cluster ->
                {
                    for (int i = 1; i <= cluster.size(); i++)
                    {
                        NodeToolResult result = cluster.get(1).nodetoolResult("upgradesstables");
                        assertEquals("upgrade sstables failed for node " + i, 0, result.getRc());
                    }

                    // make sure the results are the same after upgrade and upgrade sstables but before dropping compact storage
                    recorder.validateResults(cluster, 1);
                    recorder.validateResults(cluster, 2);

                    // make sure the results are the same after dropping compact storage on only the first node
                    IMessageFilters.Filter filter = cluster.verbs().allVerbs().to(2).drop();
                    cluster.schemaChange(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE", KEYSPACE, table), 1);

                    recorder.validateResults(cluster, 1, ConsistencyLevel.ONE);

                    filter.off();
                    recorder.validateResults(cluster, 1);
                    recorder.validateResults(cluster, 2);

                    // make sure the results continue to be the same after dropping compact storage on the second node
                    cluster.schemaChange(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE", KEYSPACE, table), 2);
                    recorder.validateResults(cluster, 1);
                    recorder.validateResults(cluster, 2);
                })
                .run();
    }

    @Test
    public void testDropCompactWithClusteringAndValueColumnWithDeletesAndWrites() throws Throwable
    {
        final String table = "clustering_and_value_with_deletes";
        final int partitions = 10;
        final int rowsPerPartition = 10;
        final int additionalParititons = 5;

        new TestCase()
                .nodes(2)
                .upgrade(Versions.Major.v22, Versions.Major.v30)
                .setup(cluster -> {
                    cluster.schemaChange(String.format(
                            "CREATE TABLE %s.%s (key int, c1 int, c2 int, c3 int, PRIMARY KEY (key, c1, c2)) WITH COMPACT STORAGE",
                            KEYSPACE, table));
                    ICoordinator coordinator = cluster.coordinator(1);

                    for (int i = 1; i <= partitions; i++)
                    {
                        for (int j = 1; j <= rowsPerPartition; j++)
                        {
                            coordinator.execute(String.format("INSERT INTO %s.%s (key, c1, c2, c3) VALUES (%d, %d, 1, 1)",
                                    KEYSPACE, table, i, j), ConsistencyLevel.ALL);
                            coordinator.execute(String.format("INSERT INTO %s.%s (key, c1, c2, c3) VALUES (%d, %d, 2, 2)",
                                                              KEYSPACE, table, i, j), ConsistencyLevel.ALL);
                            coordinator.execute(String.format("INSERT INTO %s.%s (key, c1, c2, c3) VALUES (%d, %d, 3, 3)",
                                                              KEYSPACE, table, i, j), ConsistencyLevel.ALL);
                        }
                    }

                })
                .runAfterClusterUpgrade(cluster -> {
                    for (int i = 1; i <= cluster.size(); i++)
                    {
                        NodeToolResult result = cluster.get(1).nodetoolResult("upgradesstables");
                        assertEquals("upgrade sstables failed for node " + i, 0, result.getRc());
                    }

                    // drop compact storage on only one node before performing writes
                    IMessageFilters.Filter filter = cluster.verbs().allVerbs().to(2).drop();
                    cluster.schemaChange(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE", KEYSPACE, table), 1);
                    filter.off();

                    // add new partitions and delete some of the old ones
                    ICoordinator coordinator = cluster.coordinator(1);
                    for (int i = 0; i < additionalParititons; i++)
                    {
                        for (int j = 1; j <= rowsPerPartition; j++)
                        {
                            coordinator.execute(String.format("INSERT INTO %s.%s (key, c1, c2, c3) VALUES (%d, %d, 1, 1)",
                                    KEYSPACE, table, i, j), ConsistencyLevel.ALL);
                        }
                    }

                    coordinator.execute(String.format("DELETE FROM %s.%s WHERE key = %d and c1 = %d",
                            KEYSPACE, table, 0, 3), ConsistencyLevel.ALL);

                    coordinator.execute(String.format("DELETE FROM %s.%s WHERE key = %d",
                            KEYSPACE, table, 1), ConsistencyLevel.ALL);

                    coordinator.execute(String.format("DELETE FROM %s.%s WHERE key = %d and c1 = %d and c2 = %d",
                            KEYSPACE, table, 7, 2, 2), ConsistencyLevel.ALL);

                    coordinator.execute(String.format("DELETE FROM %s.%s WHERE key = %d and c1 = %d and c2 = %d",
                            KEYSPACE, table, 7, 6, 1), ConsistencyLevel.ALL);

                    coordinator.execute(String.format("DELETE FROM %s.%s WHERE key = %d and c1 = %d and c2 = %d",
                                                      KEYSPACE, table, 4, 1, 1), ConsistencyLevel.ALL);

                    coordinator.execute(String.format("DELETE c3 FROM %s.%s WHERE key = %d and c1 = %d and c2 = %d",
                            KEYSPACE, table, 8, 1, 3), ConsistencyLevel.ALL);

                    coordinator.execute(String.format("DELETE FROM %s.%s WHERE key = %d and c1 = %d and c2 > 1",
                                                      KEYSPACE, table, 6, 2, 4), ConsistencyLevel.ALL);

                    ResultsRecorder recorder = new ResultsRecorder();
                    runQueries(coordinator, recorder, new String[] {
                            String.format("SELECT * FROM %s.%s", KEYSPACE, table),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                                    KEYSPACE, table, partitions - 3, rowsPerPartition - 2),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                                    KEYSPACE, table, partitions - 1, rowsPerPartition - 5),


                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 > %d",
                                    KEYSPACE, table, partitions - 8, rowsPerPartition - 3),

                            String.format("SELECT * FROM %s.%s WHERE key = %d",
                                    KEYSPACE, table, 7),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                                    KEYSPACE, table, 7, 2),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 = %d",
                                    KEYSPACE, table, 8, 1),

                            String.format("SELECT c1, c2 FROM %s.%s WHERE key = %d and c1 = %d",
                                    KEYSPACE, table, 8, 1),

                            String.format("SELECT c1, c2 FROM %s.%s WHERE key = %d and c1 = %d",
                                          KEYSPACE, table, 8, 1),

                            String.format("SELECT c1, c2 FROM %s.%s WHERE key = %d and c1 = %d",
                                          KEYSPACE, table, 4, 1),

                            String.format("SELECT c1, c2 FROM %s.%s WHERE key = %d",
                                          KEYSPACE, table, 6),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 > %d",
                                    KEYSPACE, table, 0, 1),

                            String.format("SELECT * FROM %s.%s WHERE key = %d",
                                    KEYSPACE, table, partitions - (additionalParititons - 2)),

                            String.format("SELECT * FROM %s.%s WHERE key = %d and c1 > %d",
                                    KEYSPACE, table, partitions - (additionalParititons - 3), 4)

                    });

                    // drop compact storage on remaining node and check result
                    cluster.schemaChange(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE", KEYSPACE, table), 2);
                    recorder.validateResults(cluster, 1);
                    recorder.validateResults(cluster, 2);
                }).run();
    }


    private void runQueries(ICoordinator coordinator, ResultsRecorder helper, String[] queries)
    {
        for (String query : queries)
            helper.addResult(query, coordinator.execute(query, ConsistencyLevel.ALL));
    }

    public static class ResultsRecorder
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

        public void validateResults(UpgradeableCluster cluster, int node)
        {
            validateResults(cluster, node, ConsistencyLevel.ALL);
        }

        public void validateResults(UpgradeableCluster cluster, int node, ConsistencyLevel cl)
        {
            for (Map.Entry<String, Object[][]> entry : queriesAndResults().entrySet())
            {
                Object[][] postUpgradeResult = cluster.coordinator(node).execute(entry.getKey(), cl);
                assertRows(postUpgradeResult, entry.getValue());
            }

        }
    }

}