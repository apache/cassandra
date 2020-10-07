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

import java.io.IOException;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;

import static org.apache.cassandra.distributed.shared.AssertUtils.*;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class SimpleReadWritePathTest extends TestBaseImpl
{
    private static final TestBaseImpl impl = new TestBaseImpl();
    private static ICluster cluster;

    @BeforeClass
    public static void before() throws IOException
    {
        cluster = init(impl.builder().withNodes(3).start());
    }

    @AfterClass
    public static void after() throws Exception
    {
        cluster.close();
    }

    @After
    public void afterEach()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(cluster);
    }

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
}
