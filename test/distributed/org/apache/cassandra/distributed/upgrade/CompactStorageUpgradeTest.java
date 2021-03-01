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

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.Versions;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;

public class CompactStorageUpgradeTest extends UpgradeTestBase
{
    @Test
    public void compactStorageColumnDeleteTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
        })
        .runAfterNodeUpgrade((cluster, i) -> {
            for (int coord = 1; coord <= 2; coord++)
            {
                int v1 = coord * 10;
                int v2 = coord * 10;

                cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, v1, v1, v1);
                cluster.coordinator(coord).execute("DELETE v FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?", ConsistencyLevel.ALL, v1, v1);
                assertRows(cluster.coordinator(coord).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                          ConsistencyLevel.ALL,
                                                          v1));

                cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, v2, v2, v2);
                assertRows(cluster.coordinator(coord).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                              ConsistencyLevel.ALL,
                                                              v2),
                           row(v2, v2, v2));
            }
        }).run();
    }

    @Test
    public void compactStoragePagingTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
            for (int i = 1; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, 1, i, i);
        })
        .runAfterNodeUpgrade((cluster, i) -> {
            for (int coord = 1; coord <= 2; coord++)
            {
                Iterator<Object[]> iter = cluster.coordinator(coord).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL, 2);
                for (int j = 1; j < 10; j++)
                {
                    Assert.assertTrue(iter.hasNext());
                    Assert.assertArrayEquals(new Object[]{ 1, j, j }, iter.next());
                }
                Assert.assertFalse(iter.hasNext());
            }
        }).run();
    }

    @Test
    public void compactStorageImplicitNullInClusteringTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck1 int, ck2 int, v int, PRIMARY KEY (pk, ck1, ck2)) WITH COMPACT STORAGE");
        })
        .runAfterClusterUpgrade((cluster) -> {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck1, v) VALUES (2, 2, 2)", ConsistencyLevel.ALL);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                      ConsistencyLevel.ALL,
                                                      2),
                       row(2, 2, null, 2));
        }).run();
    }

    @Test
    public void compactStorageHiddenColumnTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
        })
        .runAfterNodeUpgrade((cluster, node) -> {

            for (int coord = 1; coord <= 2; coord++)
            {
                int v1 = coord * 10;
                int v2 = coord * 10;

                cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck) VALUES (?, ?)", ConsistencyLevel.ALL, v1, v1);
                cluster.coordinator(coord).execute("DELETE FROM " + KEYSPACE + ".tbl WHERE pk = ? AND ck = ?", ConsistencyLevel.ALL, v1, v1);
                assertRows(cluster.coordinator(coord).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                          ConsistencyLevel.ALL,
                                                          v1));

                cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck) VALUES (?, ?)", ConsistencyLevel.ALL, v2, v2);
                assertRows(cluster.coordinator(coord).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                          ConsistencyLevel.ALL,
                                                          v2),
                           row(v2, v2));
            }
        }).run();
    }

    @Test
    public void compactStorageUpgradeTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1, 2)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck) VALUES (1,1)", ConsistencyLevel.ALL);
        })
        .runAfterClusterUpgrade((cluster) -> {
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl DROP COMPACT STORAGE");
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                      ConsistencyLevel.ALL),
                       row(1, 1, null));
        }).run();
    }
}
