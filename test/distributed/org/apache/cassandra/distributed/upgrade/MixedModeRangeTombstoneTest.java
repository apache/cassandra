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
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.Versions;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests related to the handle of range tombstones during 2.x to 3.x upgrades.
 */
public class MixedModeRangeTombstoneTest extends UpgradeTestBase
{
    /**
     * Tests the interaction of range tombstones covering multiple rows and collection tombsones within the covered
     * rows.
     *
     * <p>This test reproduces the issue of CASSANDRA-15805.
     */
    @Test
    public void multiRowsRangeTombstoneAndCollectionTombstoneInteractionTest() throws Throwable {
        String tableName = DistributedTestBase.KEYSPACE + ".t";
        String schema = "CREATE TABLE " + tableName + " (" +
                        "  k int," +
                        "  c1 text," +
                        "  c2 text," +
                        "  a text," +
                        "  b set<text>," +
                        "  c text," +
                        "  PRIMARY KEY((k), c1, c2)" +
                        " )";


        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup(cluster -> {
            cluster.schemaChange(schema);
            cluster.coordinator(1).execute(format("DELETE FROM %s USING TIMESTAMP 1 WHERE k = 0 AND c1 = 'A'", tableName), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(format("INSERT INTO %s(k, c1, c2, a, b, c) VALUES (0, 'A', 'X', 'foo', {'whatever'}, 'bar') USING TIMESTAMP 2", tableName), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(format("DELETE b FROM %s USING TIMESTAMP 3 WHERE k = 0 AND c1 = 'A' and c2 = 'X'", tableName), ConsistencyLevel.ALL);
            cluster.get(1).flush(DistributedTestBase.KEYSPACE);
            cluster.get(2).flush(DistributedTestBase.KEYSPACE);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            assertRows(cluster.coordinator(node).execute(format("SELECT * FROM %s", tableName), ConsistencyLevel.ALL),
                       row(0, "A", "X", "foo", null, "bar"));
        })
        .run();
    }
}
