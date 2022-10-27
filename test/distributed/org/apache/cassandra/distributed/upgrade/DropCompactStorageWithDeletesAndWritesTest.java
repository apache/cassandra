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

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IMessageFilters;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;


public class DropCompactStorageWithDeletesAndWritesTest extends DropCompactStorageTester
{
    @Test
    public void testDropCompactWithClusteringAndValueColumnWithDeletesAndWrites() throws Throwable
    {
        final String table = "clustering_and_value_with_deletes";
        final int partitions = 10;
        final int rowsPerPartition = 10;
        final int additionalParititons = 5;

        new TestCase()
        .nodes(2)
        .upgradesFrom(v22)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL).set("enable_drop_compact_storage", true))
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
            cluster.forEach(n -> n.nodetoolResult("upgradesstables", KEYSPACE).asserts().success());
            Thread.sleep(1000);

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
                                              KEYSPACE, table, 6, 2), ConsistencyLevel.ALL);

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
}
