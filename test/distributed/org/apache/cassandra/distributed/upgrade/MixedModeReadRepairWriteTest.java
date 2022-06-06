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
 * Test read repair after partial writes when the cluster nodes are on different versions.
 *
 * This test and {@link MixedModeReadRepairDeleteTest} are separated to avoid OOM errors in CI (see CASSANDRA-16237).
 */
public class MixedModeReadRepairWriteTest extends UpgradeTestBase
{
    /**
     * Test that queries repair rows that have been inserted in one replica only.
     * The insertion can be either in the upgraded or in the not-upgraded node.
     */
    @Test
    public void mixedModeReadRepairInsert() throws Throwable
    {
        // rows for columns (k, c, v)
        Object[] row1 = row(0, 1, 10);
        Object[] row2 = row(0, 2, 20);

        allUpgrades(2, 1)
        .setup(c -> c.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c))")))
        .runBeforeClusterUpgrade(cluster -> cluster.coordinator(1).execute(withKeyspace("TRUNCATE %s.t"), ConsistencyLevel.ALL))
        .runAfterClusterUpgrade(cluster -> {

            // insert rows internally in each node
            String insert = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
            cluster.get(1).executeInternal(insert, row1);
            cluster.get(2).executeInternal(insert, row2);

            // query to trigger read repair
            String query = withKeyspace("SELECT * FROM %s.t");
            assertRows(cluster.get(1).executeInternal(query), row1);
            assertRows(cluster.get(2).executeInternal(query), row2);
            assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL), row1, row2);
            assertRows(cluster.get(1).executeInternal(query), row1, row2);
            assertRows(cluster.get(2).executeInternal(query), row1, row2);
        })
        .run();
    }

    /**
     * Test that queries repair rows that exist in both replicas but have been updated only in one replica.
     * The update can be either in the upgraded or in the not-upgraded node.
     */
    @Test
    public void mixedModeReadRepairUpdate() throws Throwable
    {
        // rows for columns (k, c, v)
        Object[] row1 = row(0, 1, 10);
        Object[] row2 = row(0, 2, 20);

        allUpgrades(2, 1)
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c))"));
        })
        .runBeforeClusterUpgrade(cluster -> {
            // insert the initial version of the rows in all the nodes
            String insert = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, row1);
            cluster.coordinator(2).execute(insert, ConsistencyLevel.ALL, row2);
        })
        .runAfterClusterUpgrade(cluster -> {

            // internally update one row per replica
            String update = withKeyspace("UPDATE %s.t SET v=? WHERE k=? AND c=?");
            cluster.get(1).executeInternal(update, 11, 0, 1);
            cluster.get(2).executeInternal(update, 22, 0, 2);

            // query to trigger read repair
            String query = withKeyspace("SELECT * FROM %s.t");
            assertRows(cluster.get(1).executeInternal(query), row(0, 1, 11), row2);
            assertRows(cluster.get(2).executeInternal(query), row1, row(0, 2, 22));
            assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL), row(0, 1, 11), row(0, 2, 22));
            assertRows(cluster.get(1).executeInternal(query), row(0, 1, 11), row(0, 2, 22));
            assertRows(cluster.get(2).executeInternal(query), row(0, 1, 11), row(0, 2, 22));
        })
        .run();
    }
}
