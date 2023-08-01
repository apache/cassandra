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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.db.ColumnFamilyStore.SNAPSHOT_DROP_PREFIX;
import static org.apache.cassandra.db.ColumnFamilyStore.SNAPSHOT_TRUNCATE_PREFIX;
import static org.apache.cassandra.distributed.Cluster.build;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.awaitility.Awaitility.await;

public class AutoSnapshotTtlTest extends TestBaseImpl
{
    public static final Integer SNAPSHOT_CLEANUP_PERIOD_SECONDS = 1;
    public static final Integer FIVE_SECONDS = 5;
    private static WithProperties properties = new WithProperties();

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS, 0);
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS, SNAPSHOT_CLEANUP_PERIOD_SECONDS);
        properties.set(CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS, FIVE_SECONDS);
    }

    @AfterClass
    public static void after()
    {
        properties.close();
    }

    /**
     * Check that when auto_snapshot_ttl=5s, snapshots created from TRUNCATE are expired after 10s
     */
    @Test
    public void testAutoSnapshotTTlOnTruncate() throws IOException
    {
        try (Cluster cluster = init(build().withNodes(1)
                                      .withConfig(c -> c.with(Feature.GOSSIP)
                                                        .set("auto_snapshot_ttl", String.format("%ds", FIVE_SECONDS)))
                                      .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));

            populate(cluster);

            // Truncate Table
            cluster.schemaChange(withKeyspace("TRUNCATE %s.tbl;"));

            // Check snapshot is listed after table is truncated
            instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains(SNAPSHOT_TRUNCATE_PREFIX);

            // Check snapshot is removed after 10s
            await().timeout(10, SECONDS)
                   .pollInterval(1, SECONDS)
                   .until(() -> !instance.nodetoolResult("listsnapshots").getStdout().contains(SNAPSHOT_DROP_PREFIX));
        }
    }

    /**
     * Check that when auto_snapshot_ttl=5s, snapshots created from TRUNCATE are expired after 10s
     */
    @Test
    public void testAutoSnapshotTTlOnDrop() throws IOException
    {
        try (Cluster cluster = init(build().withNodes(1)
                                      .withConfig(c -> c.with(Feature.GOSSIP)
                                                                  .set("auto_snapshot_ttl", String.format("%ds", FIVE_SECONDS)))
                                      .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));

            populate(cluster);

            // Drop Table
            cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl;"));

            // Check snapshot is listed after table is dropped
            instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains(SNAPSHOT_DROP_PREFIX);

            // Check snapshot is removed after 10s
            await().timeout(10, SECONDS)
                   .pollInterval(1, SECONDS)
                   .until(() -> !instance.nodetoolResult("listsnapshots").getStdout().contains(SNAPSHOT_DROP_PREFIX));
        }
    }

    /**
     * Check that when auto_snapshot_ttl=60s, snapshots created from DROP TABLE are expired after a node restart
     */
    @Test
    public void testAutoSnapshotTTlOnDropAfterRestart() throws IOException
    {
        int ONE_MINUTE = 60; // longer TTL to allow snapshot to survive node restart
        try (Cluster cluster = init(build().withNodes(1)
                                           .withConfig(c -> c.with(Feature.GOSSIP)
                                                             .set("auto_snapshot_ttl", String.format("%ds", ONE_MINUTE)))
                                           .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));

            populate(cluster);

            // Drop Table
            cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl;"));

            // Restart node
            stopUnchecked(instance);
            instance.startup();

            // Check snapshot is listed after restart
            instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains(SNAPSHOT_DROP_PREFIX);

            // Check snapshot is removed after at most auto_snapshot_ttl + 1s
            await().timeout(ONE_MINUTE + 1, SECONDS)
                   .pollInterval(1, SECONDS)
                   .until(() -> !instance.nodetoolResult("listsnapshots").getStdout().contains(SNAPSHOT_DROP_PREFIX));
        }
    }

    /**
     * Check that when auto_snapshot_ttl is unset, snapshots created from DROP or TRUNCATE do not expire
     */
    @Test
    public void testAutoSnapshotTtlDisabled() throws IOException, InterruptedException
    {
        try (Cluster cluster = init(build().withNodes(1)
                                      .withConfig(c -> c.with(Feature.GOSSIP))
                                      .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (key int, value text, PRIMARY KEY (key))"));

            populate(cluster);

            // Truncate Table
            cluster.schemaChange(withKeyspace("TRUNCATE %s.tbl;"));

            // Drop Table
            cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl;"));

            // Check snapshots are created after table is truncated and dropped
            instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains(SNAPSHOT_TRUNCATE_PREFIX);
            instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains(SNAPSHOT_DROP_PREFIX);

            // Check snapshot are *NOT* expired after 10s
            Thread.sleep(2 * FIVE_SECONDS * 1000L);
            instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains(ColumnFamilyStore.SNAPSHOT_TRUNCATE_PREFIX);
            instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains(ColumnFamilyStore.SNAPSHOT_DROP_PREFIX);
        }
    }

    protected static void populate(Cluster cluster)
    {
        for (int i = 0; i < 100; i++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (key, value) VALUES (?, 'txt')"), ConsistencyLevel.ONE, i);
    }
}
