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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.IInvokableInstance;

import static org.junit.Assert.assertEquals;

public class DistributedReadWritePathTest extends DistributedTestBase
{
    @Test
    public void coordinatorReadTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
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
    }

    @Test
    public void coordinatorWriteTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
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
    }

    @Test
    public void readRepairTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
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
    }

    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
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
                                              ConsistencyLevel.QUORUM);
            }
            catch (RuntimeException e)
            {
                thrown = e;
            }

            Assert.assertTrue(thrown.getMessage().contains("Exception occurred on node"));
            Assert.assertTrue(thrown.getCause().getCause().getCause().getMessage().contains("Unknown column v2 during deserialization"));
        }
    }

    @Test
    public void readWithSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
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
                assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                         ConsistencyLevel.ALL),
                           row(1, 1, 1, null));
            }
            catch (Exception e)
            {
                thrown = e;
            }
            Assert.assertTrue(thrown.getMessage().contains("Exception occurred on node"));
            Assert.assertTrue(thrown.getCause().getCause().getCause().getMessage().contains("Unknown column v2 during deserialization"));
        }
    }
    @Test
    public void simplePagedReadsTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            int size = 100;
            Object[][] results = new Object[size][];
            for (int i = 0; i < size; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                               ConsistencyLevel.QUORUM,
                                               i, i);
                results[i] = new Object[] { 1, i, i};
            }

            // First, make sure that non-paged reads are able to fetch the results
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.QUORUM),
                       results);

            // Make sure paged read returns same results with different page sizes
            for (int pageSize : new int[] { 1, 2, 3, 5, 10, 20, 50})
            {
                assertRows(cluster.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                    ConsistencyLevel.QUORUM,
                                                                    pageSize),
                           results);
            }
        }
    }

    @Test
    public void pagingWithRepairTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            int size = 100;
            Object[][] results = new Object[size][];
            for (int i = 0; i < size; i++)
            {
                // Make sure that data lands on different nodes and not coordinator
                cluster.get(i % 2 == 0 ? 2 : 3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)",
                                                                i, i);

                results[i] = new Object[] { 1, i, i};
            }

            // Make sure paged read returns same results with different page sizes
            for (int pageSize : new int[] { 1, 2, 3, 5, 10, 20, 50})
            {
                assertRows(cluster.coordinator(1).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl",
                                                                    ConsistencyLevel.ALL,
                                                                    pageSize),
                           results);
            }

            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl"),
                       results);
        }
    }

    @Test
    public void pagingTests() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3));
             Cluster singleNode = init(Cluster.create(1)))
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

            int[] pageSizes = new int[] { 1, 2, 3, 5, 10, 20, 50};
            String[] statements = new String [] {"SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 LIMIT 3",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5 LIMIT 2",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 LIMIT 2",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC LIMIT 3",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC LIMIT 2",
                                                 "SELECT * FROM " + KEYSPACE  + ".tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC LIMIT 2",
                                                 "SELECT DISTINCT pk FROM " + KEYSPACE  + ".tbl LIMIT 3",
                                                 "SELECT DISTINCT pk FROM " + KEYSPACE  + ".tbl WHERE pk IN (3,5,8,10)",
                                                 "SELECT DISTINCT pk FROM " + KEYSPACE  + ".tbl WHERE pk IN (3,5,8,10) LIMIT 2"
            };
            for (String statement : statements)
            {
                for (int pageSize : pageSizes)
                {
                    assertRows(cluster.coordinator(1)
                                      .executeWithPaging(statement,
                                                         ConsistencyLevel.QUORUM,  pageSize),
                               singleNode.coordinator(1)
                                         .executeWithPaging(statement,
                                                            ConsistencyLevel.QUORUM,  Integer.MAX_VALUE));
                }
            }

        }
    }

    @Test
    public void metricsCountQueriesTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (pk, ck, v) VALUES (?,?,?)", ConsistencyLevel.ALL, i, i, i);

            long readCount1 = readCount(cluster.get(1));
            long readCount2 = readCount(cluster.get(2));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl WHERE pk = ? and ck = ?", ConsistencyLevel.ALL, i, i);

            readCount1 = readCount(cluster.get(1)) - readCount1;
            readCount2 = readCount(cluster.get(2)) - readCount2;
            assertEquals(readCount1, readCount2);
            assertEquals(100, readCount1);
        }
    }

    private long readCount(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readLatency.latency.getCount());
    }
}
