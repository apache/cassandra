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
import org.apache.cassandra.distributed.shared.Versions;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Test read repair when the cluster nodes are in different versions
 */
public class MixedModeReadRepairTest extends UpgradeTestBase
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

        allSingleNodeUpgrades()
        .setup(c -> c.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c))")))
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

        allSingleNodeUpgrades()
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c))"));

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

        allSingleNodeUpgrades()
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, s int static, PRIMARY KEY (k, c))"));

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

        allSingleNodeUpgrades()
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, s int static, PRIMARY KEY (k, c))"));

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

    private TestCase allSingleNodeUpgrades()
    {
        return new TestCase().nodes(2)
                             .upgrade(Versions.Major.v22, Versions.Major.v30)
                             .upgrade(Versions.Major.v22, Versions.Major.v3X)
                             .upgrade(Versions.Major.v30, Versions.Major.v3X)
                             .upgrade(Versions.Major.v30, Versions.Major.v4)
                             .upgrade(Versions.Major.v3X, Versions.Major.v4)
                             .nodesToUpgrade(1);
    }

    /**
     * Tests {@code COMPACT STORAGE} behaviour with mixed replica versions.
     * <p>
     * See CASSANDRA-15363 for further details.
     */
    @Test
    public void mixedModeReadRepairCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .upgrade(Versions.Major.v22, Versions.Major.v3X)
        .upgrade(Versions.Major.v30, Versions.Major.v3X)
        .setup((cluster) -> cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl" +
                                                              " (pk ascii, b boolean, v blob, PRIMARY KEY (pk))" +
                                                              " WITH COMPACT STORAGE")))
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node != 1)
                return;
            // now node1 is 3.0/3.x and node2 is 2.2
            // make sure 2.2 side does not get the mutation
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl WHERE pk = ?"), "something");
            // trigger a read repair
            cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"),
                                           ConsistencyLevel.ALL,
                                           "something");
            cluster.get(2).flush(KEYSPACE);
        })
        .runAfterClusterUpgrade((cluster) -> cluster.get(2).forceCompact(KEYSPACE, "tbl"))
        .run();
    }
}