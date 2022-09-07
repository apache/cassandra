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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.Cluster.build;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.awaitility.Awaitility.await;

public class AllowAutoSnapshotTest extends TestBaseImpl
{
    @Test
    public void testAllowAutoSnapshotOnAutoSnapshotEnabled() throws Exception
    {
        try (Cluster c = getCluster(true))
        {
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table (a text primary key, b int) WITH allow_auto_snapshot = true"));
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table;"));

            checkSnapshots(c, true, "test_table", "dropped");

            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table2 (a text primary key, b int) WITH allow_auto_snapshot = false"));
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table2;"));

            checkSnapshots(c, false, "test_table2", "dropped");
        }
    }

    @Test
    public void testAllowAutoSnapshotOnAutoSnapshotDisabled() throws Exception
    {
        try (Cluster c = getCluster(false))
        {
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table (a text primary key, b int) WITH allow_auto_snapshot = true"));
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table;"));

            checkSnapshots(c, false, "test_table", "dropped");

            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table2 (a text primary key, b int) WITH allow_auto_snapshot = false"));
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table2;"));

            checkSnapshots(c, false, "test_table2", "dropped");
        }
    }

    @Test
    public void testDisableAndEnableAllowAutoSnapshot() throws Exception
    {
        try (Cluster c = getCluster(true))
        {
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table (a text primary key, b int) WITH allow_auto_snapshot = true"));

            c.schemaChange(withKeyspace("ALTER TABLE %s.test_table WITH allow_auto_snapshot = false"));
            c.schemaChange(withKeyspace("ALTER TABLE %s.test_table WITH allow_auto_snapshot = true"));

            c.schemaChange(withKeyspace("DROP TABLE %s.test_table;"));

            checkSnapshots(c, true, "test_table", "dropped");
        }
    }

    @Test
    public void testTruncateAllowAutoSnapshot() throws Exception
    {
        try (Cluster c = getCluster(true))
        {
            // allow_auto_snapshot = true
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table (a text primary key, b int) WITH allow_auto_snapshot = true"));

            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1);"), ALL);

            c.schemaChange(withKeyspace("TRUNCATE TABLE %s.test_table;"));

            checkSnapshots(c, true, "test_table", "truncated");

            // allow_auto_snapshot = false
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table2 (a text primary key, b int) WITH allow_auto_snapshot = false"));

            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table2 (a, b) VALUES ('a', 1);"), ALL);

            c.schemaChange(withKeyspace("TRUNCATE TABLE %s.test_table2;"));

            checkSnapshots(c, false, "test_table2", "truncated");
        }
    }

    @Test
    public void testMaterializedViewAllowAutoSnapshot() throws Exception
    {
        try (Cluster c = getCluster(true))
        {
            // materialized view allow_auto_snapshot = false
            c.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k, c1, c2))"));
            c.schemaChange(withKeyspace("CREATE MATERIALIZED VIEW %s.mv1 AS SELECT * FROM t WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL PRIMARY KEY (c1, k, c2) WITH allow_auto_snapshot = false"));

            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, c1, c2, v1, v2) VALUES (1, 2, 3, 4, 5);"), ALL);

            c.schemaChange(withKeyspace("DROP MATERIALIZED VIEW %s.mv1;"));

            checkSnapshots(c, false, "mv1", "dropped");

            // materialized view allow_auto_snapshot = true
            c.schemaChange(withKeyspace("CREATE MATERIALIZED VIEW %s.mv1 AS SELECT * FROM t WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL PRIMARY KEY (c1, k, c2) WITH allow_auto_snapshot = true"));

            c.schemaChange(withKeyspace("DROP MATERIALIZED VIEW %s.mv1;"));
            checkSnapshots(c, true, "mv1", "dropped");
        }
    }

    private Cluster getCluster(boolean autoSnapshotEnabled) throws IOException
    {
        return init(build(2).withConfig(c -> c.with(GOSSIP)
                                              .set("auto_snapshot", autoSnapshotEnabled)
                                              .set("materialized_views_enabled", true)).start());
    }

    private void checkSnapshots(Cluster cluster, boolean shouldContain, String table, String snapshotPrefix)
    {
        for (int i = 1; i < cluster.size() + 1; i++)
        {
            final int node = i; // has to be effectively final for the usage in "until" method
            await().until(() -> cluster.get(node).appliesOnInstance((IIsolatedExecutor.SerializableTriFunction<Boolean, String, String, Boolean>) (shouldContainSnapshot, tableName, prefix) -> {
                                                                     Stream<String> stream = StorageService.instance.getSnapshotDetails(Collections.emptyMap()).keySet().stream();
                                                                     Predicate<String> predicate = tag -> tag.startsWith(prefix + '-') && tag.endsWith('-' + tableName);
                                                                     return shouldContainSnapshot ? stream.anyMatch(predicate) : stream.noneMatch(predicate);
            }).apply(shouldContain, table, snapshotPrefix));
        }
    }
}
