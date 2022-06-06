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

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class DropCompactStorageTest extends UpgradeTestBase
{
    @Test
    public void testDropCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1, 2)
        .upgradesToCurrentFrom(v30)
        .withConfig(config -> config.with(GOSSIP, NETWORK).set("enable_drop_compact_storage", true))
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
