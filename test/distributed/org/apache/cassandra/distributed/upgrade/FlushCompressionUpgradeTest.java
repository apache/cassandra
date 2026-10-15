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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.Versions;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * ALTER TABLE ... WITH flush_compression is rejected until every node supports metadata version V11,
 * other ALTER TABLE statements are accepted in mixed mode.
 */
public class FlushCompressionUpgradeTest extends UpgradeTestBase
{
    private static final String TABLE = KEYSPACE + ".tbl";

    @Test
    public void alterDuringUpgrade() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2)
        .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .singleUpgradeToCurrentFrom(v60)
        .setup(cluster -> cluster.schemaChange("CREATE TABLE " + TABLE + " (pk int PRIMARY KEY, v int)"))
        .runAfterClusterUpgrade(cluster -> {
            for (int node = 1; node <= 2; node++)
            {
                int coordinator = node;
                assertThatThrownBy(() -> alter(cluster, coordinator, "flush_compression = 'none'"))
                .hasMessageContaining("Transformation rejected");
            }

            alter(cluster, 1, "comment = 'mixed'");
            alter(cluster, 3, "gc_grace_seconds = 1000");
            awaitSchema(cluster, "tbl", "comment, gc_grace_seconds", row("mixed", 1000));

            // plain DDL from an upgraded coordinator must still be applied by the 6.0 node
            cluster.coordinator(1).execute("CREATE TABLE " + KEYSPACE + ".t2 (pk int PRIMARY KEY, v int)", ConsistencyLevel.ALL);
            awaitQueryable(cluster, "t2");
            cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + ".t2 (pk, v) VALUES (1, 1)", ConsistencyLevel.ALL);

            IUpgradeableInstance node3 = cluster.get(3);
            node3.shutdown().get();
            node3.setVersion(Versions.find().getLatest(CURRENT));
            node3.startup();

            alter(cluster, 1, "flush_compression = 'none'");
            awaitSchema(cluster, "tbl", "comment, gc_grace_seconds, flush_compression", row("mixed", 1000, "none"));

            cluster.coordinator(3).execute("INSERT INTO " + TABLE + " (pk, v) VALUES (1, 1)", ConsistencyLevel.ALL);
            cluster.get(3).flush(KEYSPACE);
            assertRows(cluster.coordinator(1).execute("SELECT pk, v FROM " + TABLE + " WHERE pk = 1", ConsistencyLevel.ALL),
                       row(1, 1));
        })
        .run();
    }

    private static void alter(UpgradeableCluster cluster, int node, String option)
    {
        cluster.coordinator(node).execute("ALTER TABLE " + TABLE + " WITH " + option, ConsistencyLevel.ALL);
    }

    /** system_schema is written before the epoch is enacted, so wait until every node serves queries for the table. */
    private static void awaitQueryable(UpgradeableCluster cluster, String table)
    {
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            for (int node = 1; node <= 3; node++)
            {
                try
                {
                    cluster.get(node).executeInternal("SELECT * FROM " + KEYSPACE + '.' + table + " LIMIT 1");
                }
                catch (Exception e)
                {
                    return false;
                }
            }
            return true;
        });
    }

    /** Committed schema reaches non-coordinator nodes asynchronously, so poll every node. */
    private static void awaitSchema(UpgradeableCluster cluster, String table, String columns, Object[] expected)
    {
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            for (int node = 1; node <= 3; node++)
                assertRows(cluster.get(node).executeInternal("SELECT " + columns + " FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                                                             KEYSPACE, table),
                           expected);
        });
    }
}
