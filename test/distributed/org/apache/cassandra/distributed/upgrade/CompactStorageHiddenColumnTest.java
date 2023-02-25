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

public class CompactStorageHiddenColumnTest extends UpgradeTestBase
{
    @Test
    public void testHiddenColumnWithCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgradesToCurrentFrom(v40)
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
}
