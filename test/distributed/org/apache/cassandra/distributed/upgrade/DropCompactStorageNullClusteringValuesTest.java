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

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

public class DropCompactStorageNullClusteringValuesTest extends UpgradeTestBase
{
    public static final String TABLE_NAME = "cs_tbl";

    @Test
    public void testNullClusteringValues() throws Throwable
    {
        new TestCase().nodes(1)
                      .upgradesToCurrentFrom(v30)
                      .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL).set("enable_drop_compact_storage", true))
                      .setup(cluster -> {
                          String create = "CREATE TABLE %s.%s(k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) " +
                                          "WITH compaction = { 'class':'LeveledCompactionStrategy', 'enabled':'false'} AND COMPACT STORAGE";
                          cluster.schemaChange(String.format(create, KEYSPACE, TABLE_NAME));

                          String insert = "INSERT INTO %s.%s(k, c1, v) values (?, ?, ?)";
                          cluster.get(1).executeInternal(String.format(insert, KEYSPACE, TABLE_NAME), 1, 1, 1);
                          cluster.get(1).flush(KEYSPACE);

                          cluster.get(1).executeInternal(String.format(insert, KEYSPACE, TABLE_NAME), 2, 2, 2);
                          cluster.get(1).flush(KEYSPACE);

                          cluster.schemaChange(String.format("ALTER TABLE %s.%s DROP COMPACT STORAGE", KEYSPACE, TABLE_NAME));
                      })
                      .runAfterNodeUpgrade((cluster, node) -> {
                          cluster.get(1).forceCompact(KEYSPACE, TABLE_NAME);
                          Object[][] actual = cluster.get(1).executeInternal(String.format("SELECT * FROM %s.%s", KEYSPACE, TABLE_NAME));
                          assertRows(actual, new Object[] {1, 1, null, 1}, new Object[] {2, 2, null, 2});
                      })
                      .run();
    }
}
