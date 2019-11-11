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

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.impl.Versions;
import org.apache.cassandra.distributed.test.DistributedTestBase;

import static junit.framework.Assert.assertEquals;

public class UpgradeTest extends UpgradeTestBase
{

    @Test
    public void upgradeTest() throws Throwable
    {
        new TestCase()
            .upgrade(Versions.Major.v22, Versions.Major.v30, Versions.Major.v3X)
            .setup((cluster) -> {
                cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

                cluster.get(1).executeInternal("INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
                cluster.get(2).executeInternal("INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, 2)");
                cluster.get(3).executeInternal("INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, 3)");
            })
            .runAfterClusterUpgrade((cluster) -> {
                DistributedTestBase.assertRows(cluster.coordinator(1).execute("SELECT * FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                                                              ConsistencyLevel.ALL,
                                                                              1),
                                               DistributedTestBase.row(1, 1, 1),
                                               DistributedTestBase.row(1, 2, 2),
                                               DistributedTestBase.row(1, 3, 3));
            }).run();
    }

    @Test
    public void mixedModePagingTest() throws Throwable
    {
        new TestCase()
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .nodes(2)
        .nodesToUpgrade(2)
        .setup((cluster) -> {
            cluster.schemaChange("ALTER KEYSPACE " + DistributedTestBase.KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) with compact storage");
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < 200; j++)
                    cluster.coordinator(2).execute("INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, 1)", ConsistencyLevel.ALL, i, j);
            cluster.forEach((i) -> i.flush(DistributedTestBase.KEYSPACE));
            for (int i = 0; i < 100; i++)
                for (int j = 10; j < 30; j++)
                    cluster.coordinator(2).execute("DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl where pk=? and ck=?", ConsistencyLevel.ALL, i, j);
            cluster.forEach((i) -> i.flush(DistributedTestBase.KEYSPACE));
        })
        .runAfterClusterUpgrade((cluster) -> {
            for (int i = 0; i < 100; i++)
            {
                for (int pageSize = 10; pageSize < 100; pageSize++)
                {
                    Iterator<Object[]> res = cluster.coordinator(1).executeWithPaging("SELECT * FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                                                                      ConsistencyLevel.ALL,
                                                                                      pageSize, i);
                    assertEquals(180, Iterators.size(res));
                }
            }
        }).run();
    }

}
