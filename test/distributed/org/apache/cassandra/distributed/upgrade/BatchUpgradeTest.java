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

import org.apache.cassandra.Util;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.junit.Assert.assertEquals;

public class BatchUpgradeTest extends UpgradeTestBase
{
    @Test
    public void batchTest() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgradesToCurrentFrom(v40).setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".users (" +
                                 "userid uuid PRIMARY KEY," +
                                 "firstname ascii," +
                                 "lastname ascii," +
                                 "age int) WITH COMPACT STORAGE");
        }).runAfterNodeUpgrade((cluster, node) -> {
            cluster.coordinator(2).execute("BEGIN BATCH\n" +
                                           "    UPDATE " + KEYSPACE + ".users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479\n" +
                                           "    DELETE firstname, lastname FROM " + KEYSPACE + ".users WHERE userid = 550e8400-e29b-41d4-a716-446655440000\n" +
                                           "APPLY BATCH", ConsistencyLevel.ALL);
        }).runAfterClusterUpgrade((cluster) -> {
            Util.spinAssertEquals(0, () -> cluster.get(1).executeInternal("select * from system.batches").length, 10);
            Util.spinAssertEquals(0, () -> cluster.get(2).executeInternal("select * from system.batches").length, 10);
            assertEquals(0, cluster.get(1).logs().grep("ClassCastException").getResult().size());
            assertEquals(0, cluster.get(2).logs().grep("ClassCastException").getResult().size());
        })
        .run();
    }
}
