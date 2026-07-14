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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.assertj.core.api.Assertions;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public abstract class TrackedRepairTransferTestBase extends TrackedTransferTestBase
{
    public void testFullRepairSinglePlan(String keyspace, Cluster cluster, ByteBuffer key, int syncs, boolean optimized, String... repairCommandAndArgs) throws IOException
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
        cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(keyspace) + " (pk BLOB PRIMARY KEY, v INT)");

        IInvokableInstance coordinator = cluster.get(1);
        coordinator.executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 1)", key);
        coordinator.flush(keyspace);
        coordinator.executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 1)", key);
        coordinator.flush(keyspace);

        // Write should only be present on instance 1
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", key);
            if (ClusterUtils.instanceId(instance) == 1)
                assertRows(rows, row(key, 1));
            else
                assertRows(rows); // empty
        });

        long mark = coordinator.logs().mark();
        NodeToolResult result = coordinator.nodetoolResult(repairCommandAndArgs);
        result.asserts().success();
        List<String> logs = coordinator.logs().grep(mark, "Created " + syncs + (optimized ? " optimised" : "") + " sync tasks based on 3 merkle tree responses").getResult();
        Assertions.assertThat(logs).isNotEmpty();

        // Write visible on all instances after repair
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", key);
            assertRows(rows, row(key, 1));
        });

        // Make sure all instances can successfully coordinate
        cluster.forEach(instance -> {
            Object[][] rows = instance.coordinator().execute("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", ALL, key);
            assertRows(rows, row(key, 1));
        });
    }

    public void testFullRepairRemoteSender(String keyspace, Cluster cluster, int syncs, boolean optimized, String... repairCommandAndArgs) throws IOException
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
        cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(keyspace) + " (pk BLOB PRIMARY KEY, v INT)");

        IInvokableInstance coordinator = cluster.get(1);
        cluster.get(2).executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 2)", KEY_201);

        // Second key should only be present on instance 2
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_201);
            if (ClusterUtils.instanceId(instance) == 2)
                assertRows(rows, row(KEY_201, 2));
            else
                assertRows(rows); // empty
        });

        long mark = coordinator.logs().mark();
        NodeToolResult result = coordinator.nodetoolResult(repairCommandAndArgs);
        result.asserts().success();
        List<String> logs = coordinator.logs().grep(mark, "Created " + syncs + (optimized ? " optimised" : "") + " sync tasks based on 3 merkle tree responses").getResult();
        Assertions.assertThat(logs).isNotEmpty();

        // Write visible on all instances after repair
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_201);
            assertRows(rows, row(KEY_201, 2));
        });

        // Make sure all instances can successfully coordinate
        cluster.forEach(instance -> {
            Object[][] rows = instance.coordinator().execute("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", ALL, KEY_201);
            assertRows(rows, row(KEY_201, 2));
        });
    }

    public void testFullRepairDuplicateSender(String keyspace, Cluster cluster, ByteBuffer key, int syncs, boolean optimized, String... repairCommandAndArgs) throws IOException
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
        cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(keyspace) + " (pk BLOB PRIMARY KEY, v INT)");

        IInvokableInstance coordinator = cluster.get(1);
        coordinator.executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 1)", key);
        cluster.get(2).executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 1)", key);

        // Write should be missing on node 3
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", key);
            if (ClusterUtils.instanceId(instance) != 3)
                assertRows(rows, row(key, 1));
            else
                assertRows(rows); // empty
        });

        long mark = coordinator.logs().mark();
        coordinator.nodetoolResult(repairCommandAndArgs).asserts().success();

        List<String> logs = coordinator.logs().grep(mark, "Created " + syncs + (optimized ? " optimised" : "") + " sync tasks based on 3 merkle tree responses").getResult();
        Assertions.assertThat(logs).isNotEmpty();

        // Write visible on all instances after repair
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", key);
            assertRows(rows, row(key, 1));
        });

        // Make sure all instances can successfully coordinate
        cluster.forEach(instance -> {
            Object[][] rows = instance.coordinator().execute("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", ALL, key);
            assertRows(rows, row(key, 1));
        });
    }

    public void testFullRepairMultiSender(String keyspace, Cluster cluster, int syncs, boolean optimized, String... repairCommandAndArgs) throws IOException
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
        cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(keyspace) + " (pk BLOB PRIMARY KEY, v INT)");

        IInvokableInstance coordinator = cluster.get(1);
        coordinator.executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 1)", KEY_200);
        cluster.get(2).executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 2)", KEY_201);

        // First key should only be present on instance 1
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_200);
            if (ClusterUtils.instanceId(instance) == 1)
                assertRows(rows, row(KEY_200, 1));
            else
                assertRows(rows); // empty
        });

        // Second key should only be present on instance 2
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_201);
            if (ClusterUtils.instanceId(instance) == 2)
                assertRows(rows, row(KEY_201, 2));
            else
                assertRows(rows); // empty
        });

        long mark = coordinator.logs().mark();
        NodeToolResult result = coordinator.nodetoolResult(repairCommandAndArgs);
        result.asserts().success();
        List<String> logs = coordinator.logs().grep(mark, "Created " + syncs + (optimized ? " optimised" : "") + " sync tasks based on 3 merkle tree responses").getResult();
        Assertions.assertThat(logs).isNotEmpty();

        // Writes visible on all instances after repair
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_200);
            assertRows(rows, row(KEY_200, 1));
            rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_201);
            assertRows(rows, row(KEY_201, 2));
        });

        // Make sure all instances can successfully coordinate
        cluster.forEach(instance -> {
            Object[][] rows = instance.coordinator().execute("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", ALL, KEY_200);
            assertRows(rows, row(KEY_200, 1));
            rows = instance.coordinator().execute("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", ALL, KEY_201);
            assertRows(rows, row(KEY_201, 2));
        });
    }

    public void testFullRepairMultiSenderSameToken(String keyspace, Cluster cluster, int syncs, boolean optimized, String... repairCommandAndArgs) throws IOException
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
        cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(keyspace) + " (pk BLOB PRIMARY KEY, v INT)");

        IInvokableInstance coordinator = cluster.get(1);
        coordinator.executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 1) USING TIMESTAMP 1", KEY_200);
        cluster.get(2).executeInternal("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, 2) USING TIMESTAMP 2", KEY_200);

        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_200);
            if (ClusterUtils.instanceId(instance) == 1)
                assertRows(rows, row(KEY_200, 1));
            else if (ClusterUtils.instanceId(instance) == 2)
                assertRows(rows, row(KEY_200, 2));
            else
                assertRows(rows); // empty
        });

        long mark = coordinator.logs().mark();
        NodeToolResult result = coordinator.nodetoolResult(repairCommandAndArgs);
        result.asserts().success();
        List<String> logs = coordinator.logs().grep(mark, "Created " + syncs + (optimized ? " optimised" : "") + " sync tasks based on 3 merkle tree responses").getResult();
        Assertions.assertThat(logs).isNotEmpty();

        // Writes visible on all instances after repair
        cluster.forEach(instance -> {
            Object[][] rows = instance.executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", KEY_200);
            assertRows(rows, row(KEY_200, 2));
        });

        // Make sure all instances can successfully coordinate
        cluster.forEach(instance -> {
            Object[][] rows = instance.coordinator().execute("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", ALL, KEY_200);
            assertRows(rows, row(KEY_200, 2));
        });
    }
}
