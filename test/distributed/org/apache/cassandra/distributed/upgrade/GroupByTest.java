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

public class GroupByTest extends UpgradeTestBase
{
    @Test
    public void testReads() throws Throwable
    {
        // CASSANDRA-16582: group-by across mixed version cluster would fail with ArrayIndexOutOfBoundException
        new UpgradeTestBase.TestCase()
        .nodes(2)
        .upgradesToCurrentFrom(v3X)
        .nodesToUpgrade(1)
        .withConfig(config -> config.with(GOSSIP, NETWORK))
        .setup(cluster -> {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (a int, b int, c int, v int, primary key (a, b, c))"));
            String insert = withKeyspace("INSERT INTO %s.t (a, b, c, v) VALUES (?, ?, ?, ?)");
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, 1, 1, 1, 3);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, 1, 2, 1, 6);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, 1, 2, 2, 12);
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, 1, 3, 2, 12);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            String query = withKeyspace("SELECT a, b, count(c) FROM %s.t GROUP BY a,b");
            Object[][] expectedResult = {
            row(1, 1, 1L),
            row(1, 2, 2L),
            row(1, 3, 1L)
            };
            assertRows(cluster.coordinator(1).execute(query, ConsistencyLevel.ALL), expectedResult);
            assertRows(cluster.coordinator(2).execute(query, ConsistencyLevel.ALL), expectedResult);
        })
        .run();
    }
}
