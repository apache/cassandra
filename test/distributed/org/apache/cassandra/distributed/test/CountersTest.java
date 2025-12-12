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
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.metrics.ClientMetrics;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class CountersTest extends TestBaseImpl
{
    @Test
    public void testUpdateCounter() throws Throwable
    {
        testUpdateCounter(false);
    }

    @Test
    public void testUpdateCounterWithDroppedCompactStorage() throws Throwable
    {
        testUpdateCounter(true);
    }

    private static void testUpdateCounter(boolean droppedCompactStorage) throws Throwable
    {
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL).set("drop_compact_storage_enabled", true)).start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

            String createTable = "CREATE TABLE k.t ( k int, c int, total counter, PRIMARY KEY (k, c))";
            if (droppedCompactStorage)
            {
                cluster.schemaChange(createTable + " WITH COMPACT STORAGE");
                cluster.schemaChange("ALTER TABLE k.t DROP COMPACT STORAGE");
            }
            else
            {
                cluster.schemaChange(createTable);
            }

            ConsistencyLevel cl = ConsistencyLevel.ONE;
            String select = "SELECT total FROM k.t WHERE k = 1 AND c = ?";

            for (int i = 1; i <= cluster.size(); i++)
            {
                ICoordinator coordinator = cluster.coordinator(i);

                coordinator.execute("UPDATE k.t SET total = total + 1 WHERE k = 1 AND c = ?", cl, i);
                assertRows(coordinator.execute(select, cl, i), row(1L));

                coordinator.execute("UPDATE k.t SET total = total - 4 WHERE k = 1 AND c = ?", cl, i);
                assertRows(coordinator.execute(select, cl, i), row(-3L));
            }
        }
    }

    /**
     * Helper to capture counter write metrics from both nodes before and after a write.
     */
    private static class MetricsSnapshot
    {
        long coordinatorToLeaderNode1, coordinatorToReplicasNode1, leaderToReplicasNode1;
        long coordinatorToLeaderNode2, coordinatorToReplicasNode2, leaderToReplicasNode2;

        MetricsSnapshot(Cluster cluster) throws Throwable
        {
            coordinatorToLeaderNode1 = cluster.get(1).callOnInstance(() -> 
                ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderAttempts.getCount());
            coordinatorToReplicasNode1 = cluster.get(1).callOnInstance(() -> 
                ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasAttempts.getCount());
            leaderToReplicasNode1 = cluster.get(1).callOnInstance(() -> 
                ClientMetrics.instance.counterWriteLeaderWaitForReplicasAttempts.getCount());

            coordinatorToLeaderNode2 = cluster.get(2).callOnInstance(() -> 
                ClientMetrics.instance.counterWriteCoordinatorWaitForLeaderAttempts.getCount());
            coordinatorToReplicasNode2 = cluster.get(2).callOnInstance(() -> 
                ClientMetrics.instance.counterWriteCoordinatorWaitForReplicasAttempts.getCount());
            leaderToReplicasNode2 = cluster.get(2).callOnInstance(() -> 
                ClientMetrics.instance.counterWriteLeaderWaitForReplicasAttempts.getCount());
        }

        void assertMetricsUnchanged(MetricsSnapshot before, String context)
        {
            assert this.coordinatorToLeaderNode1 == before.coordinatorToLeaderNode1 : 
                context + " - coordinatorToLeaderNode1 should NOT change";
            assert this.coordinatorToReplicasNode1 == before.coordinatorToReplicasNode1 : 
                context + " - coordinatorToReplicasNode1 should NOT change";
            assert this.leaderToReplicasNode1 == before.leaderToReplicasNode1 : 
                context + " - leaderToReplicasNode1 should NOT change";
            assert this.coordinatorToLeaderNode2 == before.coordinatorToLeaderNode2 : 
                context + " - coordinatorToLeaderNode2 should NOT change";
            assert this.coordinatorToReplicasNode2 == before.coordinatorToReplicasNode2 : 
                context + " - coordinatorToReplicasNode2 should NOT change";
            assert this.leaderToReplicasNode2 == before.leaderToReplicasNode2 : 
                context + " - leaderToReplicasNode2 should NOT change";
        }

        void assertMetricsChanged(MetricsSnapshot before, int node1CoordToLeaderChange, int node1CoordToReplicasChange, int node1LeaderToReplicasChange, 
                                 int node2CoordToLeaderChange, int node2CoordToReplicasChange, int node2LeaderToReplicasChange)
        {
            assert this.coordinatorToLeaderNode1 == before.coordinatorToLeaderNode1 + node1CoordToLeaderChange : 
                "Node1 coordinatorToLeader mismatch (expected +" + node1CoordToLeaderChange + ")";
            assert this.coordinatorToReplicasNode1 == before.coordinatorToReplicasNode1 + node1CoordToReplicasChange : 
                "Node1 coordinatorToReplicas mismatch (expected +" + node1CoordToReplicasChange + ")";
            assert this.leaderToReplicasNode1 == before.leaderToReplicasNode1 + node1LeaderToReplicasChange : 
                "Node1 leaderToReplicas mismatch (expected +" + node1LeaderToReplicasChange + ")";
            assert this.coordinatorToLeaderNode2 == before.coordinatorToLeaderNode2 + node2CoordToLeaderChange : 
                "Node2 coordinatorToLeader mismatch (expected +" + node2CoordToLeaderChange + ")";
            assert this.coordinatorToReplicasNode2 == before.coordinatorToReplicasNode2 + node2CoordToReplicasChange : 
                "Node2 coordinatorToReplicas mismatch (expected +" + node2CoordToReplicasChange + ")";
            assert this.leaderToReplicasNode2 == before.leaderToReplicasNode2 + node2LeaderToReplicasChange : 
                "Node2 leaderToReplicas mismatch (expected +" + node2LeaderToReplicasChange + ")";
        }
    }

    /**
     * Test counter write path when coordinator is a replica, with tracking DISABLED.
     * 
     * TEST SETUP: 2-node cluster with RF=1
     * - Only node 1 is a replica for the key
     * - Coordinator is node 1, which would trigger COORDINATOR_WAIT_FOR_REPLICAS path, but metrics should NOT be marked.
     */
    @Test
    public void testCounterWriteCoordinatorToReplicasPathWithTrackingDisabled() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)
                                                                   .set("track_counter_write_metrics", false)).start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE k.t (k int PRIMARY KEY, c counter)");

            ConsistencyLevel cl = ConsistencyLevel.ONE;

            MetricsSnapshot before = new MetricsSnapshot(cluster);
            cluster.coordinator(1).execute("UPDATE k.t SET c = c + 1 WHERE k = " + 1, cl);
            MetricsSnapshot after = new MetricsSnapshot(cluster);

            // All metrics should be unchanged when tracking is disabled
            after.assertMetricsUnchanged(before, "Tracking disabled - all metrics should remain 0");
        }
    }

    /**
     * Test counter write path when coordinator is a replica, with Attempts metrics verification.
     * 
     * TEST SETUP: 2-node cluster with RF=1
     * - Only node 1 is a replica for the key
     * - Coordinator is node 1, which triggers COORDINATOR_WAIT_FOR_REPLICAS path.
     */
    @Test
    public void testCounterWriteCoordinatorToReplicasPathWithTrackingEnabled() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)
                                                                   .set("track_counter_write_metrics", true)).start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE k.t (k int PRIMARY KEY, c counter)");

            ConsistencyLevel cl = ConsistencyLevel.ONE;

            MetricsSnapshot before = new MetricsSnapshot(cluster);
            cluster.coordinator(1).execute("UPDATE k.t SET c = c + 1 WHERE k = " + 1, cl);
            MetricsSnapshot after = new MetricsSnapshot(cluster);

            // Node1: coordinator is a replica -> COORDINATOR_WAIT_FOR_REPLICAS (+1)
            // Node2: not involved (no change)
            after.assertMetricsChanged(before, 0, 1, 0, 0, 0, 0);
        }
    }

    /**
     * Test counter write path when coordinator is not a replica, with tracking DISABLED.
     * 
     * TEST SETUP: 2-node cluster with RF=1
     * - Only node 1 is a replica for the key
     * - Node 2 (coordinator) is non-replica and would trigger COORDINATOR_WAIT_FOR_LEADER path, 
     *   but metrics should NOT be marked when tracking is disabled.
     */
    @Test
    public void testCounterWriteCoordinatorToLeaderPathWithTrackingDisabled() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)
                                                        .set("track_counter_write_metrics", false))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE k.t (k INT PRIMARY KEY, c COUNTER)");

            ConsistencyLevel cl = ConsistencyLevel.ONE;

            MetricsSnapshot before = new MetricsSnapshot(cluster);
            cluster.coordinator(2).execute("UPDATE k.t SET c = c + 1 WHERE k = " + 1, cl);
            MetricsSnapshot after = new MetricsSnapshot(cluster);

            // All metrics should be unchanged when tracking is disabled
            after.assertMetricsUnchanged(before, "Tracking disabled - all metrics should remain 0");
        }
    }

    /**
     * Test counter write path when coordinator is not a replica, with Attempts metrics verification.
     * 
     * TEST SETUP: 2-node cluster with RF=1
     * - Only node 1 is a replica for the key
     * - Node 2 (coordinator) is non-replica and will trigger COORDINATOR_WAIT_FOR_LEADER path. 
     *   Node 1 (leader replica) will trigger LEADER_WAIT_FOR_REPLICAS path.
     */
    @Test
    public void testCounterWriteCoordinatorToLeaderPathWithTrackingEnabled() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)
                                                                   .set("track_counter_write_metrics", true)).start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE k.t (k int PRIMARY KEY, c counter)");

            ConsistencyLevel cl = ConsistencyLevel.ONE;

            MetricsSnapshot before = new MetricsSnapshot(cluster);
            cluster.coordinator(2).execute("UPDATE k.t SET c = c + 1 WHERE k = " + 1, cl);
            MetricsSnapshot after = new MetricsSnapshot(cluster);

            // Node2: coordinator is non-replica -> COORDINATOR_WAIT_FOR_LEADER (+1)
            // Node1: leader replica -> LEADER_WAIT_FOR_REPLICAS (+1)
            after.assertMetricsChanged(before, 0, 0, 1, 1, 0, 0);
        }
    }
}
