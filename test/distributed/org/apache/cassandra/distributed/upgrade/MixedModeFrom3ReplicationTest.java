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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class MixedModeFrom3ReplicationTest extends UpgradeTestBase
{
    @Test
    public void testSimpleStrategy() throws Throwable
    {
        String insert = "INSERT INTO test_simple.names (key, name) VALUES (?, ?)";
        String select = "SELECT * FROM test_simple.names WHERE key = ?";

        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2)
        .upgradesToCurrentFrom(v30)
        .setup(cluster -> {
            cluster.schemaChange("CREATE KEYSPACE test_simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
            cluster.schemaChange("CREATE TABLE test_simple.names (key int PRIMARY KEY, name text)");
        })
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            List<Long> initialTokens = new ArrayList<>(cluster.size() + 1);
            initialTokens.add(null); // The first valid token is at 1 to avoid offset math below.

            for (int i = 1; i <= cluster.size(); i++)
                initialTokens.add(Long.valueOf(cluster.get(i).config().get("initial_token").toString()));

            List<Long> validTokens = initialTokens.subList(1, cluster.size() + 1);

            // Exercise all the coordinators...
            for (int i = 1; i <= cluster.size(); i++)
            {
                // ...and sample enough keys that we cover the ring.
                for (int j = 0; j < 10; j++)
                {
                    int key = j + (i * 10);
                    Object[] row = row(key, "Nero");
                    Long token = tokenFrom(key);

                    cluster.coordinator(i).execute(insert, ConsistencyLevel.ALL, row);

                    int node = primaryReplica(validTokens, token);
                    assertRows(cluster.get(node).executeInternal(select, key), row);

                    node = nextNode(node, cluster.size());
                    assertRows(cluster.get(node).executeInternal(select, key), row);

                    // At RF=2, this node should not have received the write.
                    node = nextNode(node, cluster.size());
                    assertRows(cluster.get(node).executeInternal(select, key));
                }
            }
        })
        .run();
    }
}
