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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.distributed.shared.AssertUtils.*;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class SimpleReadWriteTest extends SharedClusterTestBase
{
    @Test
    public void coordinatorReadTest() throws Throwable
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
        cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
        cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");

        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                  ConsistencyLevel.ALL,
                                                  1),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3));
    }

    @Test
    public void largeMessageTest() throws Throwable
    {
        int largeMessageThreshold = 1024 * 64;
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < largeMessageThreshold; i++)
            builder.append('a');
        String s = builder.toString();
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
                                       ConsistencyLevel.ALL,
                                       s);
        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                  ConsistencyLevel.ALL,
                                                  1),
                   row(1, 1, s));
    }

    @Test
    public void coordinatorWriteTest() throws Throwable
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.QUORUM);

        for (int i = 0; i < 3; i++)
        {
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }

        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.QUORUM),
                   row(1, 1, 1));
    }

    @Test
    public void readRepairTest() throws Throwable
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
        cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

        assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.ALL), // ensure node3 in preflist
                   row(1, 1, 1));

        // Verify that data got repaired to the third node
        assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                   row(1, 1, 1));
    }

    /**
     * If a node receives a mutation for a column it's not aware of, it should fail, since it can't write the data.
     */
    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

        cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

        // Introduce schema disagreement
        cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

        Exception thrown = null;
        try
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)",
                                           ConsistencyLevel.ALL);
        }
        catch (RuntimeException e)
        {
            thrown = e;
        }

        Assert.assertTrue(thrown.getMessage().contains("Exception occurred on node"));
        Assert.assertTrue(thrown.getCause().getCause().getCause().getMessage().contains("Unknown column v2"));
    }



    /**
     * If a node isn't aware of a column, but receives a mutation without that column, the write should succeed
     */
    @Test
    public void writeWithInconsequentialSchemaDisagreement() throws Throwable
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))");

        cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");
        cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (1, 1, 1)");

        // Introduce schema disagreement
        cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int", 1);

        // this write shouldn't cause any problems because it doesn't write to the new column
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1) VALUES (2, 2, 2)",
                                       ConsistencyLevel.ALL);
    }

    @Test
    public void simplePagedReadsTest() throws Throwable
    {

        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        int size = 100;
        Object[][] results = new Object[size][];
        for (int i = 0; i < size; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                           ConsistencyLevel.QUORUM,
                                           i, i);
            results[i] = new Object[]{ 1, i, i };
        }

        // Make sure paged read returns same results with different page sizes
        for (int pageSize : new int[]{ 1, 2, 3, 5, 10, 20, 50 })
        {
            assertRows(cluster.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                ConsistencyLevel.QUORUM,
                                                                pageSize),
                       results);
        }
    }

    @Test
    public void pagingWithRepairTest() throws Throwable
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        int size = 100;
        Object[][] results = new Object[size][];
        for (int i = 0; i < size; i++)
        {
            // Make sure that data lands on different nodes and not coordinator
            cluster.get(i % 2 == 0 ? 2 : 3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                            i, i);

            results[i] = new Object[]{ 1, i, i };
        }

        // Make sure paged read returns same results with different page sizes
        for (int pageSize : new int[]{ 1, 2, 3, 5, 10, 20, 50 })
        {
            assertRows(cluster.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                ConsistencyLevel.ALL,
                                                                pageSize),
                       results);
        }

        assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl"),
                   results);
    }

    @Test
    public void pagingTests() throws Throwable
    {
        try (ICluster singleNode = init(builder().withNodes(1).withSubnet(1).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            singleNode.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                   ConsistencyLevel.QUORUM,
                                                   i, j, i + i);
                    singleNode.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                      ConsistencyLevel.QUORUM,
                                                      i, j, i + i);
                }
            }

            int[] pageSizes = new int[]{ 1, 2, 3, 5, 10, 20, 50 };
            String[] statements = new String[]{ "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 LIMIT 3",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5 LIMIT 2",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 LIMIT 2",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC LIMIT 3",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC LIMIT 2",
                                                "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC LIMIT 2",
                                                "SELECT DISTINCT pk FROM " + KEYSPACE + ".tbl LIMIT 3",
                                                "SELECT DISTINCT pk FROM " + KEYSPACE + ".tbl WHERE pk IN (3,5,8,10)",
                                                "SELECT DISTINCT pk FROM " + KEYSPACE + ".tbl WHERE pk IN (3,5,8,10) LIMIT 2"
            };
            for (String statement : statements)
            {
                for (int pageSize : pageSizes)
                {
                    assertRows(cluster.coordinator(1)
                                      .executeWithPaging(statement,
                                                         ConsistencyLevel.QUORUM, pageSize),
                               singleNode.coordinator(1)
                                         .executeWithPaging(statement,
                                                            ConsistencyLevel.QUORUM, Integer.MAX_VALUE));
                }
            }
        }
    }

    @Test
    public void metricsCountQueriesTest() throws Throwable
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        for (int i = 0; i < 100; i++)
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?,?,?)", ConsistencyLevel.ALL, i, i, i);

        long readCount1 = readCount((IInvokableInstance) cluster.get(1));
        long readCount2 = readCount((IInvokableInstance) cluster.get(2));
        for (int i = 0; i < 100; i++)
            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ? and ck = ?", ConsistencyLevel.ALL, i, i);

        readCount1 = readCount((IInvokableInstance) cluster.get(1)) - readCount1;
        readCount2 = readCount((IInvokableInstance) cluster.get(2)) - readCount2;
        assertEquals(readCount1, readCount2);
        assertEquals(100, readCount1);
    }


    @Test
    public void skippedSSTableWithPartitionDeletionTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 2");
            cluster.get(1).flush(KEYSPACE);
            // expect a single sstable, where minTimestamp equals the timestamp of the partition delete
            cluster.get(1).runOnInstance(() -> {
                Set<SSTableReader> sstables = Keyspace.open(KEYSPACE)
                                                      .getColumnFamilyStore("tbl")
                                                      .getLiveSSTables();
                assertEquals(1, sstables.size());
                assertEquals(1, sstables.iterator().next().getMinTimestamp());
            });

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");


            Object[][] rows = cluster.coordinator(1)
                                     .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                                              ConsistencyLevel.ALL);
            assertEquals(0, rows.length);
        }
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 1");
            cluster.get(1).flush(KEYSPACE);
            // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
            // insert a row which is not shadowed by the partition delete and flush to a second sstable. Importantly,
            // this sstable's minTimestamp is > than the maxTimestamp of the first sstable. This would cause the first
            // sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
            // know what data and/or tombstones are present on other nodes
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2");
            cluster.get(1).flush(KEYSPACE);

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");

            Object[][] rows = cluster.coordinator(1)
                                     .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                                              ConsistencyLevel.ALL);
            // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
            // node 1 (0, 6, 6) was not.
            assertRows(rows, new Object[] {0, 6 ,6});
        }
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode2() throws Throwable
    {
        // don't not add skipped sstables back just because the partition delete ts is < the local min ts

        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))");
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 0");
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 3");
            cluster.get(1).flush(KEYSPACE);
            // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
            // insert a row which is not shadowed by the partition delete and flush to a second sstable. The first sstable
            // has a maxTimestamp > than the min timestamp of all sstables, so it is a candidate for reinclusion to the
            // merge. Hoever, the second sstable's minTimestamp is > than the partition delete. This would  cause the
            // first sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
            // know what data and/or tombstones are present on other nodes
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2");
            cluster.get(1).flush(KEYSPACE);

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0");

            Object[][] rows = cluster.coordinator(1)
                                     .execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=0 AND ck > 5",
                                              ConsistencyLevel.ALL);
            // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
            // node 1 (0, 6, 6) was not.
            assertRows(rows, new Object[] {0, 6 ,6});
        }
    }

    private long readCount(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readLatency.latency.getCount());
    }

    private static Object[][] rows(Object[]...rows)
    {
        Object[][] r = new Object[rows.length][];
        System.arraycopy(rows, 0, r, 0, rows.length);
        return r;
    }

}
