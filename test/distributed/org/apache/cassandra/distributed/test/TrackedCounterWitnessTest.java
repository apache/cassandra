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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Ported distributed tests from Python counter_test.py covering mutation tracking
 * with witness keyspaces (RF='3/1').
 */
public class TrackedCounterWitnessTest extends TestBaseImpl
{
    private static final String TABLE = "counters";
    private static final String QUALIFIED_TABLE = KEYSPACE + '.' + TABLE;

    private static Cluster createWitnessCluster() throws IOException
    {
        Cluster cluster = Cluster.build(3)
                                 .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                       .with(Feature.GOSSIP)
                                                       .with(Feature.NATIVE_PROTOCOL)
                                                       .set("transient_replication_enabled", "true")
                                                       .set("dynamic_snitch", false)
                                                       .set("node_proximity", WitnessAlwaysReadsFullReplicaTest.TransientFirstProximity.class.getName()))
                                 .start();

        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} AND replication_type='tracked'");
        cluster.schemaChange("CREATE TABLE " + QUALIFIED_TABLE + " (pk int PRIMARY KEY, c counter)");
        return cluster;
    }

    /**
     * Ported from Python counter_test.py:test_simple_increment
     *
     * Tests multiple counter partitions incremented by rotating coordinators across all
     * cluster nodes (including full replicas and the witness).
     */
    @Test
    public void testRotatingCoordinatorIncrements() throws Throwable
    {
        try (Cluster cluster = createWitnessCluster())
        {
            int numCounters = 10;
            int numIncrements = 30;

            for (int i = 0; i < numIncrements; i++)
            {
                for (int pk = 0; pk < numCounters; pk++)
                {
                    // Rotate coordinator node across nodes 1, 2, and 3
                    int coordinatorNode = ((i + pk) % 3) + 1;
                    cluster.coordinator(coordinatorNode).execute(
                        "UPDATE " + QUALIFIED_TABLE + " SET c = c + 1 WHERE pk = ?",
                        ConsistencyLevel.QUORUM, pk);
                }
            }

            // Verify all counters have the expected count from all coordinators
            for (int pk = 0; pk < numCounters; pk++)
            {
                for (int node = 1; node <= 3; node++)
                {
                    assertRows(cluster.coordinator(node).execute(
                        "SELECT c FROM " + QUALIFIED_TABLE + " WHERE pk = ?",
                        ConsistencyLevel.QUORUM, pk),
                        row((long) numIncrements));
                }
            }
        }
    }

    /**
     * Ported from Python counter_test.py:test_counter_node_down
     *
     * With RF='3/1' (2 full replicas + 1 witness), if 1 full replica goes down,
     * QUORUM (2 nodes) is still satisfied by 1 surviving full replica + 1 witness.
     * Counter writes must succeed when coordinated either by the full replica or by the witness.
     */
    @Test
    public void testCounterWritesWithOneFullReplicaDown() throws Throwable
    {
        try (Cluster cluster = createWitnessCluster())
        {
            int pk = 42;

            List<Integer> fullReplicas = fullReplicaNodesFor(cluster, pk);
            int fullReplicaToStop = fullReplicas.get(0);

            // Shut down 1 full replica
            cluster.get(fullReplicaToStop).shutdown().get();

            // Find the surviving nodes (1 full replica and 1 witness)
            List<Integer> survivingNodes = new ArrayList<>();
            for (int i = 1; i <= cluster.size(); i++)
            {
                if (i != fullReplicaToStop)
                    survivingNodes.add(i);
            }

            int survivingNode1 = survivingNodes.get(0);
            int survivingNode2 = survivingNodes.get(1);

            // Perform 20 increments alternating between the two surviving nodes
            for (int i = 0; i < 20; i++)
            {
                int coordinator = (i % 2 == 0) ? survivingNode1 : survivingNode2;
                cluster.coordinator(coordinator).execute(
                    "UPDATE " + QUALIFIED_TABLE + " SET c = c + 1 WHERE pk = ?",
                    ConsistencyLevel.QUORUM, pk);
            }

            // Verify count is 20
            assertRows(cluster.coordinator(survivingNode1).execute(
                "SELECT c FROM " + QUALIFIED_TABLE + " WHERE pk = ?",
                ConsistencyLevel.QUORUM, pk),
                row(20L));
        }
    }

    /**
     * Ported from Python counter_test.py:test_upgrade / rolling restart
     *
     * Tests that bouncing nodes (restarting witness, restarting full replicas)
     * does not cause counter state corruption or loss.
     */
    @Test
    public void testRollingRestart() throws Throwable
    {
        try (Cluster cluster = createWitnessCluster())
        {
            int pk = 99;

            // 1. Initial 10 increments
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("UPDATE " + QUALIFIED_TABLE + " SET c = c + 1 WHERE pk = ?", ConsistencyLevel.QUORUM, pk);

            assertRows(cluster.coordinator(1).execute("SELECT c FROM " + QUALIFIED_TABLE + " WHERE pk = ?", ConsistencyLevel.QUORUM, pk), row(10L));

            // 2. Restart node 3 and do 10 more increments
            cluster.get(3).shutdown().get();
            cluster.get(3).startup();

            for (int i = 0; i < 10; i++)
                cluster.coordinator(2).execute("UPDATE " + QUALIFIED_TABLE + " SET c = c + 1 WHERE pk = ?", ConsistencyLevel.QUORUM, pk);

            assertRows(cluster.coordinator(2).execute("SELECT c FROM " + QUALIFIED_TABLE + " WHERE pk = ?", ConsistencyLevel.QUORUM, pk), row(20L));

            // 3. Restart node 1 - forces counter cache reload from disk
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();

            for (int i = 0; i < 10; i++)
                cluster.coordinator(3).execute("UPDATE " + QUALIFIED_TABLE + " SET c = c + 1 WHERE pk = ?", ConsistencyLevel.QUORUM, pk);

            assertRows(cluster.coordinator(3).execute("SELECT c FROM " + QUALIFIED_TABLE + " WHERE pk = ?", ConsistencyLevel.QUORUM, pk), row(30L));
        }
    }

    /**
     * Ported from Python counter_test.py:test_counter_consistency
     *
     * Stresses randomized increments and decrements (+2, +10, -1, -5) across multiple
     * keys and rotating coordinators (including the witness coordinator), asserting that the
     * final CRDT merge produces the exact expected count for each partition.
     */
    @Test
    public void testCounterConsistencyRandomDeltas() throws Throwable
    {
        try (Cluster cluster = createWitnessCluster())
        {
            int numCounters = 5;
            long[] expectedCounts = new long[numCounters];
            long[] deltas = new long[]{ 2L, 10L, -1L, -5L };
            Random rand = new Random(42);

            int numOperations = 100;
            for (int i = 0; i < numOperations; i++)
            {
                int pk = rand.nextInt(numCounters);
                long delta = deltas[rand.nextInt(deltas.length)];
                expectedCounts[pk] += delta;

                int coordinator = (i % 3) + 1;
                cluster.coordinator(coordinator).execute(
                    "UPDATE " + QUALIFIED_TABLE + " SET c = c + ? WHERE pk = ?",
                    ConsistencyLevel.QUORUM, delta, pk);
            }

            // Verify all counters have the exact expected sum from all nodes
            for (int pk = 0; pk < numCounters; pk++)
            {
                for (int node = 1; node <= 3; node++)
                {
                    assertRows(cluster.coordinator(node).execute(
                        "SELECT c FROM " + QUALIFIED_TABLE + " WHERE pk = ?",
                        ConsistencyLevel.QUORUM, pk),
                        row(expectedCounts[pk]));
                }
            }
        }
    }

    /**
     * Ported from Python counter_test.py:test_multi_counter_update
     *
     * Tests multiple counter columns within the same table and clustering keys,
     * ensuring independent counter shards are properly maintained and replicated.
     */
    @Test
    public void testMultiCounterColumns() throws Throwable
    {
        try (Cluster cluster = createWitnessCluster())
        {
            String multiTable = KEYSPACE + ".multi_counters";
            cluster.schemaChange("CREATE TABLE " + multiTable + " (pk int, ck int, c1 counter, c2 counter, PRIMARY KEY (pk, ck))");

            int pk = 1;
            int numRows = 5;

            // Increment c1 and c2 across multiple clustering keys rotating through coordinators
            for (int ck = 0; ck < numRows; ck++)
            {
                int coordinator = (ck % 3) + 1;
                long delta1 = (ck + 1) * 2L;
                long delta2 = (ck + 1) * 5L;

                cluster.coordinator(coordinator).execute(
                    "UPDATE " + multiTable + " SET c1 = c1 + ?, c2 = c2 + ? WHERE pk = ? AND ck = ?",
                    ConsistencyLevel.QUORUM, delta1, delta2, pk, ck);
            }

            // Verify all clustering rows from all coordinators
            for (int ck = 0; ck < numRows; ck++)
            {
                long expectedC1 = (ck + 1) * 2L;
                long expectedC2 = (ck + 1) * 5L;

                for (int node = 1; node <= 3; node++)
                {
                    assertRows(cluster.coordinator(node).execute(
                        "SELECT c1, c2 FROM " + multiTable + " WHERE pk = ? AND ck = ?",
                        ConsistencyLevel.QUORUM, pk, ck),
                        row(expectedC1, expectedC2));
                }
            }
        }
    }

    private static List<Integer> fullReplicaNodesFor(Cluster cluster, int pk)
    {
        String endpoints = cluster.get(1).callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            Token token = Schema.instance.getTableMetadata(KEYSPACE, TABLE)
                                         .partitioner.getToken(ByteBufferUtil.bytes(pk));
            StringBuilder sb = new StringBuilder();
            metadata.placements.get(Schema.instance.getKeyspaceMetadata(KEYSPACE).params.replication)
                    .reads.forToken(token).get()
                    .stream()
                    .filter(Replica::isFull)
                    .forEach(r -> sb.append(sb.length() == 0 ? "" : ",").append(r.endpoint().getHostAddress(false)));
            return sb.toString();
        });

        List<Integer> nodes = new ArrayList<>();
        for (String endpoint : endpoints.split(","))
            nodes.add(Integer.parseInt(endpoint.substring(endpoint.lastIndexOf('.') + 1)));
        return nodes;
    }
}
