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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;

import static org.junit.Assert.assertEquals;

/**
 * Distributed tests for unlogged batches with mutation tracking.
 * Tests the new capability to run batches against tracked keyspaces.
 */
public class TrackedBatchTest extends TestBaseImpl
{
    private static final String TRACKED_KS = "tracked_ks";
    private static final String UNTRACKED_KS = "untracked_ks";

    @Test
    public void testMultipleTrackedMutations() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {
            // Create tracked keyspace
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE " + TRACKED_KS + " WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));
            cluster.schemaChange("CREATE TABLE " + TRACKED_KS + ".tbl (k int primary key, v int);");

            // Verify keyspace is tracked
            String keyspaceName = TRACKED_KS;
            cluster.get(1).runOnInstance(() -> {
                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            // Execute unlogged batch with multiple mutations to tracked keyspace
            String batchCql = "BEGIN UNLOGGED BATCH\n" +
                              "  INSERT INTO " + TRACKED_KS + ".tbl (k, v) VALUES (1, 100);\n" +
                              "  INSERT INTO " + TRACKED_KS + ".tbl (k, v) VALUES (2, 200);\n" +
                              "  INSERT INTO " + TRACKED_KS + ".tbl (k, v) VALUES (3, 300);\n" +
                              "APPLY BATCH";

            cluster.coordinator(1).execute(batchCql, ConsistencyLevel.QUORUM);

            // Verify all mutations succeeded
            Object[][] result = cluster.coordinator(1).execute("SELECT * FROM " + TRACKED_KS + ".tbl", ConsistencyLevel.QUORUM);
            assertEquals(3, result.length);

            // Verify data on all nodes (at RF=3, all nodes should have the data)
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance node = cluster.get(i);
                Object[][] nodeResult = node.executeInternal("SELECT * FROM " + TRACKED_KS + ".tbl");
                assertEquals(3, nodeResult.length);
            }
        }
    }

    @Test
    public void testMixedTrackedUntracked() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {
            // Create tracked keyspace
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE " + TRACKED_KS + " WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));
            cluster.schemaChange("CREATE TABLE " + TRACKED_KS + ".tbl (k int primary key, v int);");

            // Create untracked keyspace
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE " + UNTRACKED_KS + " WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3};"));
            cluster.schemaChange("CREATE TABLE " + UNTRACKED_KS + ".tbl (k int primary key, v int);");

            // Verify keyspace types
            String trackedKsName = TRACKED_KS;
            String untrackedKsName = UNTRACKED_KS;
            cluster.get(1).runOnInstance(() -> {
                KeyspaceMetadata tracked = Schema.instance.getKeyspaceMetadata(trackedKsName);
                assertEquals(ReplicationType.tracked, tracked.params.replicationType);

                KeyspaceMetadata untracked = Schema.instance.getKeyspaceMetadata(untrackedKsName);
                assertEquals(ReplicationType.untracked, untracked.params.replicationType);
            });

            // Execute mixed batch
            String batchCql = "BEGIN UNLOGGED BATCH\n" +
                              "  INSERT INTO " + TRACKED_KS + ".tbl (k, v) VALUES (1, 100);\n" +
                              "  INSERT INTO " + UNTRACKED_KS + ".tbl (k, v) VALUES (2, 200);\n" +
                              "  INSERT INTO " + TRACKED_KS + ".tbl (k, v) VALUES (3, 300);\n" +
                              "  INSERT INTO " + UNTRACKED_KS + ".tbl (k, v) VALUES (4, 400);\n" +
                              "APPLY BATCH";

            cluster.coordinator(1).execute(batchCql, ConsistencyLevel.QUORUM);

            // Verify tracked keyspace mutations
            Object[][] trackedResult = cluster.coordinator(1).execute("SELECT * FROM " + TRACKED_KS + ".tbl", ConsistencyLevel.QUORUM);
            assertEquals(2, trackedResult.length);

            // Verify untracked keyspace mutations
            Object[][] untrackedResult = cluster.coordinator(1).execute("SELECT * FROM " + UNTRACKED_KS + ".tbl", ConsistencyLevel.QUORUM);
            assertEquals(2, untrackedResult.length);

            // Verify data on all nodes
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance node = cluster.get(i);

                Object[][] trackedNodeResult = node.executeInternal("SELECT * FROM " + TRACKED_KS + ".tbl");
                assertEquals(2, trackedNodeResult.length);

                Object[][] untrackedNodeResult = node.executeInternal("SELECT * FROM " + UNTRACKED_KS + ".tbl");
                assertEquals(2, untrackedNodeResult.length);
            }
        }
    }
}
