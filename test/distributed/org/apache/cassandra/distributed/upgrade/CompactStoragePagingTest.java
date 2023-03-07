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

public class CompactStoragePagingTest extends UpgradeTestBase
{
    @Test
    public void testPagingWithCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgradesToCurrentFrom(v40)
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
}
