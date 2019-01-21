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

package org.apache.cassandra.distributed;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;

public class DistributedReadWritePathTest extends DistributedTestBase
{
    @Test
    public void coordinatorRead() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
            cluster.get(3).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                     ConsistencyLevel.ALL,
                                                     1),
                       row(1, 1, 1),
                       row(1, 2, 2),
                       row(1, 3, 3));
        }
    }

    @Test
    public void coordinatorWrite() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                          ConsistencyLevel.QUORUM);

            for (int i = 0; i < 3; i++)
            {
                assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                           row(1, 1, 1));
            }

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.QUORUM),
                       row(1, 1, 1));
        }
    }

    @Test
    public void readRepairTest() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                     ConsistencyLevel.QUORUM),
                       row(1, 1, 1));

            // Verify that data got repaired to the third node
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                       row(1, 1, 1));
        }
    }

    @Test
    public void writeWithSchemaDisagreement() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
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
                cluster.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) VALUES (2, 2, 2, 2)",
                                              ConsistencyLevel.QUORUM);
            }
            catch (RuntimeException e)
            {
                thrown = e;
            }

            Assert.assertTrue(thrown.getMessage().contains("Exception occurred on the node"));
            Assert.assertTrue(thrown.getCause().getMessage().contains("Unknown column v2 during deserialization"));
        }
    }

    @Test
    public void readWithSchemaDisagreement() throws Throwable
    {
        try (TestCluster cluster = createCluster(3))
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
                assertRows(cluster.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                         ConsistencyLevel.ALL),
                           row(1, 1, 1, null));
            }
            catch (Exception e)
            {
                thrown = e;
            }
            Assert.assertTrue(thrown.getMessage().contains("Exception occurred on the node"));
            Assert.assertTrue(thrown.getCause().getMessage().contains("Unknown column v2 during deserialization"));
        }
    }
}
