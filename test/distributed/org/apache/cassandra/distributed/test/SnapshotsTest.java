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
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;

public class SnapshotsTest extends TestBaseImpl
{
    public static final Integer SNAPSHOT_CLEANUP_PERIOD_SECONDS = 1;
    public static final Integer FIVE_SECONDS = 5;
    private static WithProperties properties = new WithProperties();
    private static Cluster cluster;

    @BeforeClass
    public static void before() throws IOException
    {
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS, 0);
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS, SNAPSHOT_CLEANUP_PERIOD_SECONDS);
        properties.set(CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS, FIVE_SECONDS);
        cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.GOSSIP)).start());
    }

    @AfterClass
    public static void after()
    {
        properties.close();
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testSnapshotsCleanupByTTL() throws Exception
    {
        cluster.get(1).nodetoolResult("snapshot", "--ttl", String.format("%ds", FIVE_SECONDS),
                                      "-t", "basic").asserts().success();
        cluster.get(1).nodetoolResult("listsnapshots").asserts().success().stdoutContains("basic");

        Thread.sleep(2 * FIVE_SECONDS * 1000L);
        cluster.get(1).nodetoolResult("listsnapshots").asserts().success().stdoutNotContains("basic");
    }

    @Test
    public void testSnapshotCleanupAfterRestart() throws Exception
    {
        int TWENTY_SECONDS = 20; // longer TTL to allow snapshot to survive node restart
        IInvokableInstance instance = cluster.get(1);

        // Create snapshot and check exists
        instance.nodetoolResult("snapshot", "--ttl", String.format("%ds", TWENTY_SECONDS),
                                "-t", "basic").asserts().success();
        instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains("basic");

        // Restart node
        stopUnchecked(instance);
        instance.startup();

        // Check snapshot still exists after restart
        instance.nodetoolResult("listsnapshots").asserts().success().stdoutContains("basic");

        // Sleep for 2*TTL and then check snapshot is gone
        Thread.sleep(TWENTY_SECONDS * 1000L);
        cluster.get(1).nodetoolResult("listsnapshots").asserts().success().stdoutNotContains("basic");
    }

    @Test
    public void testSnapshotInvalidArgument() throws Exception
    {
        IInvokableInstance instance = cluster.get(1);

        instance.nodetoolResult("snapshot", "--ttl", String.format("%ds", 1),
                                "-t", "basic").asserts().failure().stdoutContains(String.format("ttl for snapshot must be at least %d seconds", FIVE_SECONDS));

        instance.nodetoolResult("snapshot", "--ttl", "invalid-ttl").asserts().failure();
    }

    @Test
    public void testListingSnapshotsWithoutTTL()
    {
        // take snapshot without ttl
        cluster.get(1).nodetoolResult("snapshot", "-t", "snapshot_without_ttl").asserts().success();

        // take snapshot with ttl
        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      String.format("%ds", 1000),
                                      "-t", "snapshot_with_ttl").asserts().success();

        // list snaphots without TTL
        NodeToolResult.Asserts withoutTTLResult = cluster.get(1).nodetoolResult("listsnapshots", "-nt").asserts().success();
        withoutTTLResult.stdoutContains("snapshot_without_ttl");
        withoutTTLResult.stdoutNotContains("snapshot_with_ttl");

        // list all snapshots
        NodeToolResult.Asserts allSnapshotsResult = cluster.get(1).nodetoolResult("listsnapshots").asserts().success();
        allSnapshotsResult.stdoutContains("snapshot_without_ttl");
        allSnapshotsResult.stdoutContains("snapshot_with_ttl");
    }

    @Test
    public void testManualSnapshotCleanup() throws Exception
    {
        // take snapshots with ttl
        NodeToolResult.Asserts listSnapshotsResult;
        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      String.format("%ds", FIVE_SECONDS),
                                      "-t", "first").asserts().success();

        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      String.format("%ds", FIVE_SECONDS),
                                      "-t", "second").asserts().success();

        listSnapshotsResult = cluster.get(1).nodetoolResult("listsnapshots").asserts().success();
        listSnapshotsResult.stdoutContains("first");
        listSnapshotsResult.stdoutContains("second");

        cluster.get(1).nodetoolResult("clearsnapshot", "-t", "first").asserts().success();

        listSnapshotsResult = cluster.get(1).nodetoolResult("listsnapshots").asserts().success();
        listSnapshotsResult.stdoutNotContains("first");
        listSnapshotsResult.stdoutContains("second");

        Thread.sleep(2 * FIVE_SECONDS * 1000L);

        listSnapshotsResult = cluster.get(1).nodetoolResult("listsnapshots").asserts().success();
        listSnapshotsResult.stdoutNotContains("first");
        listSnapshotsResult.stdoutNotContains("second");
    }

    @Test
    public void testSecondaryIndexCleanup() throws Exception
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS default WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
        cluster.schemaChange("CREATE TABLE default.tbl (key int, value text, PRIMARY KEY (key))");
        cluster.schemaChange("CREATE INDEX value_idx ON default.tbl (value)");

        populate(cluster);

        cluster.get(1).nodetoolResult("snapshot", "--ttl",
                                      String.format("%ds", FIVE_SECONDS),
                                      "-t", "first",
                                      "-kt", "default.tbl").asserts().success();

        NodeToolResult.Asserts listSnapshotsResult;

        listSnapshotsResult = cluster.get(1).nodetoolResult("listsnapshots").asserts().success();
        listSnapshotsResult.stdoutContains("first");

        Thread.sleep(FIVE_SECONDS * 2 * 1000);

        listSnapshotsResult = cluster.get(1).nodetoolResult("listsnapshots").asserts().success();
        listSnapshotsResult.stdoutNotContains("first");
    }

    @Test
    public void testSameTimestampOnEachTableOfSnaphot()
    {
        cluster.get(1).nodetoolResult("snapshot", "-t", "sametimestamp").asserts().success();
        NodeToolResult result = cluster.get(1).nodetoolResult("listsnapshots");

        long distinctTimestamps = Arrays.stream(result.getStdout().split("\n"))
                                   .filter(line -> line.startsWith("sametimestamp"))
                                   .map(line -> line.replaceAll(" +", " ").split(" ")[7])
                                   .distinct()
                                   .count();

        // assert all dates are same so there is just one value accross all individual tables
        Assert.assertEquals(1, distinctTimestamps);
    }

    private void populate(Cluster cluster)
    {
        for (int i = 0; i < 100; i++)
            cluster.coordinator(1).execute("INSERT INTO default.tbl (key, value) VALUES (?, 'txt')", ConsistencyLevel.ONE, i);
    }
}
