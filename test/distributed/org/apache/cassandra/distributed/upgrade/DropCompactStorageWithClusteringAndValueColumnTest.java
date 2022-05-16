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
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;


public class DropCompactStorageWithClusteringAndValueColumnTest extends DropCompactStorageTester
{
    @Test
    public void testDropCompactWithClusteringAndValueColumn() throws Throwable
    {
        final String table = "clustering_and_value";
        final int partitions = 10;
        final int rowsPerPartition = 10;

        final ResultsRecorder recorder = new ResultsRecorder();
        new TestCase()
        .nodes(2)
        .upgradesFrom(v22)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
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

            runQueries(cluster.coordinator(1), recorder, new String[]{
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
        }).runBeforeNodeRestart((cluster, node) -> {
            cluster.get(node).config().set("enable_drop_compact_storage", true);
        }).runAfterClusterUpgrade(cluster -> {
            for (int i = 1; i <= cluster.size(); i++)
            {
                NodeToolResult result = cluster.get(i).nodetoolResult("upgradesstables");
                assertEquals("upgrade sstables failed for node " + i, 0, result.getRc());
            }
            Thread.sleep(1000);

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
}
