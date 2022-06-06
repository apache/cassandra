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

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Test read repair after partial deletions when the cluster nodes are on different versions.
 *
 * This test and {@link MixedModeReadRepairWriteTest} are separated to avoid OOM errors in CI (see CASSANDRA-16237).
 */
public class MixedModeReadRepairDeleteTest extends UpgradeTestBase
{
    /**
     * Test that queries repair rows that exist in both replicas but have been deleted only in one replica.
     * The row deletion can be either in the upgraded or in the not-upgraded node.
     */
    @Test
    public void mixedModeReadRepairDeleteRow() throws Throwable
    {
        // rows for columns (k, c, v, s)
        Object[] row1 = row(0, 1, 10, 8);
        Object[] row2 = row(0, 2, 20, 8);

        allUpgrades(2, 1)
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, s int static, PRIMARY KEY (k, c))"));
        })
        .runBeforeClusterUpgrade(cluster -> {
            // insert the rows in all the nodes
            String insert = withKeyspace("INSERT INTO %s.t (k, c, v, s) VALUES (?, ?, ?, ?)");
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, row1);
            cluster.coordinator(2).execute(insert, ConsistencyLevel.ALL, row2);
        })
        .runAfterClusterUpgrade(cluster -> {

            // internally delete one row per replica
            String delete = withKeyspace("DELETE FROM %s.t WHERE k=? AND c=?");
            cluster.get(1).executeInternal(delete, 0, 1);
            cluster.get(2).executeInternal(delete, 0, 2);

            // query to trigger read repair
            String query = withKeyspace("SELECT k, c, v, s FROM %s.t");
            assertRows(cluster.get(1).executeInternal(query), row2);
            assertRows(cluster.get(2).executeInternal(query), row1);
            Object[] emptyPartition = row(0, null, null, 8);
            assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL), emptyPartition);
            assertRows(cluster.get(1).executeInternal(query), emptyPartition);
            assertRows(cluster.get(2).executeInternal(query), emptyPartition);
        })
        .run();
    }

    /**
     * Test that queries repair partitions that exist in both replicas but have been deleted only in one replica.
     * The partition deletion can be either in the upgraded or in the not-upgraded node.
     */
    @Test
    public void mixedModeReadRepairDeletePartition() throws Throwable
    {
        // partitions for columns (k, c, v, s)
        Object[][] partition1 = new Object[][]{ row(1, 1, 11, 10), row(1, 2, 12, 10) };
        Object[][] partition2 = new Object[][]{ row(2, 1, 21, 20), row(2, 2, 22, 20) };

        allUpgrades(2, 1)
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, s int static, PRIMARY KEY (k, c))"));
        })
        .runBeforeClusterUpgrade(cluster -> {
            // insert half partition in each node
            String insert = withKeyspace("INSERT INTO %s.t (k, c, v, s) VALUES (?, ?, ?, ?)");
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, partition1[0]);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, partition1[1]);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, partition2[0]);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, partition2[1]);
        })
        .runAfterClusterUpgrade(cluster -> {

            // internally delete one partition per replica
            String delete = withKeyspace("DELETE FROM %s.t WHERE k=?");
            cluster.get(1).executeInternal(delete, 1);
            cluster.get(2).executeInternal(delete, 2);

            // query to trigger read repair
            String query = withKeyspace("SELECT k, c, v, s FROM %s.t");
            assertRows(cluster.get(1).executeInternal(query), partition2);
            assertRows(cluster.get(2).executeInternal(query), partition1);
            assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL));
            assertRows(cluster.get(1).executeInternal(query));
            assertRows(cluster.get(2).executeInternal(query));
        })
        .run();
    }
}
