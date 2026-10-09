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
package org.apache.cassandra.distributed.test.replication;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.MutationTrackingSyncCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Distributed tests for MutationTrackingSyncCoordinator.
 *
 * Tests that the sync coordinator correctly waits for offset convergence
 * across all nodes in a cluster.
 */
public class MutationTrackingSyncCoordinatorTest extends TestBaseImpl
{
    private static final String KS_NAME = "sync_test_ks";
    private static final String TBL_NAME = "sync_test_tbl";

    private void createTrackedKeyspace(Cluster cluster, String keyspaceSuffix)
    {
        String ksName = KS_NAME + keyspaceSuffix;
        cluster.schemaChange("CREATE KEYSPACE " + ksName + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                             "AND replication_type='tracked'");
        cluster.schemaChange("CREATE TABLE " + ksName + '.' + TBL_NAME + " (k int PRIMARY KEY, v int)");
    }

    private String tableName(String suffix)
    {
        return KS_NAME + suffix + '.' + TBL_NAME;
    }

    private static Range<Token> fullTokenRange()
    {
        return new Range<>(
            new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
            new Murmur3Partitioner.LongToken(Long.MAX_VALUE)
        );
    }

    @Test
    public void testSyncCoordinatorCompletesWhenNoShards() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            createTrackedKeyspace(cluster, "");

            // Create a sync coordinator for a range that has no data
            // It should complete immediately since there are no offsets to sync
            Boolean completed = cluster.get(1).callOnInstance(() -> {
                Range<Token> range = fullTokenRange();
                RepairJobDesc desc = new RepairJobDesc(TimeUUID.Generator.nextTimeUUID(),
                                                       TimeUUID.Generator.nextTimeUUID(),
                                                       KS_NAME, "", java.util.List.of(range));
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(
                    SharedContext.Global.instance, desc, null, ClusterMetadata.current());
                coordinator.start();

                try
                {
                    coordinator.awaitCompletion();
                    return true;
                }
                catch (Exception e)
                {
                    return false;
                }
            });

            assertTrue("Sync coordinator should complete when there are no pending offsets", completed);
        }
    }

    @Test
    public void testSyncCoordinatorWaitsForAllReplicasMutations() throws Throwable
    {
        // Enable repair retries with a short request timeout so that the dropped MT_SYNC_RSP
        // from node 1 causes a quick timeout and retry rather than a 2-minute wait and failure.
        // After the message filter is reset, the retried MT_SYNC_REQ will get a response.
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.set("request_timeout", "1000ms")
                                                                    .set("repair.retries.max_attempts", 10)
                                                                    .set("repair.retries.base_sleep_time", "100ms")
                                                                    .set("repair.retries.max_sleep_time", "500ms"))
                                        .start())
        {
            createTrackedKeyspace(cluster, "3");

            // Block all messages FROM node 1 to prevent write replication
            // and also to drop MT_SYNC_RSP from node 1 back to the coordinator.
            // This ensures that write only succeeds locally on node 1 and the
            // sync coordinator can't get node 1's sync response.
            cluster.filters().allVerbs().from(1).drop();

            cluster.coordinator(1).execute(
                "INSERT INTO " + tableName("3") + " (k, v) VALUES (1, 1)",
                ConsistencyLevel.ONE
            );

            // Start MutationTrackingSyncCoordinator on node 2 in a separate thread.
            // It should wait for offsets to sync since node 1's sync response is being dropped.
            // The coordinator sends MT_SYNC_REQ to nodes 1 and 3. Node 3's response comes back
            // but node 1's response is dropped. The coordinator stays blocked because
            // pendingSyncResponses still contains node 1. After the filter is reset, the
            // retried MT_SYNC_REQ will succeed and the coordinator can proceed.
            CompletableFuture<Boolean> coordinatorFuture = CompletableFuture.supplyAsync(() -> cluster.get(2).callOnInstance(() -> {
                Range<Token> range = fullTokenRange();
                RepairJobDesc desc = new RepairJobDesc(TimeUUID.Generator.nextTimeUUID(),
                                                       TimeUUID.Generator.nextTimeUUID(),
                                                       KS_NAME + '3', "", java.util.List.of(range));
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(
                    SharedContext.Global.instance, desc, null, ClusterMetadata.current());
                coordinator.start();

                try
                {
                    coordinator.awaitCompletion();
                    return true;
                }
                catch (Exception e)
                {
                    return false;
                }
            }));

            // Wait until node 1 has the data
            Awaitility.await()
                      .atMost(Duration.ofSeconds(5))
                      .pollInterval(Duration.ofMillis(100))
                      .untilAsserted(() -> {
                          Object[][] results = cluster.get(1).executeInternal(
                          "SELECT k, v FROM " + tableName("3") + " WHERE k = 1");
                          assertEquals("Node 1 should have the data", 1, results.length);
                      });

            // Verify other nodes shouldn't have the data yet since we have blocked messages
            for (int i = 2; i <= 3; i++)
            {
                Object[][] results = cluster.get(i).executeInternal(
                    "SELECT k, v FROM " + tableName("3") + " WHERE k = 1"
                );
                assertEquals("Node " + i + " should not have data yet", 0, results.length);
            }

            // Verify coordinator stays blocked for at least 2 seconds while node 1's
            // sync response is being dropped. The coordinator can't complete because
            // pendingSyncResponses still contains node 1.
            Awaitility.await()
                      .during(Duration.ofSeconds(2))
                      .atMost(Duration.ofSeconds(3))
                      .until(() -> !coordinatorFuture.isDone());

            // Reset filter so that retried MT_SYNC_REQ to node 1 can get a response,
            // and offset broadcasts from node 1 can reach other nodes.
            cluster.filters().reset();

            // Force offset broadcasts on all nodes to drive reconciliation.
            // After the sync response from node 1 establishes targets, the coordinator
            // needs to see that all replicas have caught up via offset broadcasts.
            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            // Wait for coordinator to complete
            Awaitility.await()
                      .atMost(Duration.ofSeconds(30))
                      .pollInterval(Duration.ofMillis(200))
                      .until(coordinatorFuture::isDone);

            assertTrue("Coordinator should complete successfully", coordinatorFuture.get());

            // Verify data propagated to all replicas
            for (int i = 1; i <= 3; i++)
            {
                final int nodeId = i;
                Awaitility.await()
                          .atMost(Duration.ofSeconds(10))
                          .pollInterval(Duration.ofMillis(100))
                          .untilAsserted(() -> {
                              Object[][] results = cluster.get(nodeId).executeInternal(
                              "SELECT k, v FROM " + tableName("3") + " WHERE k = 1");
                              assertEquals("Node " + nodeId + " should have the data", 1, results.length);
                              assertEquals(1, results[0][0]);
                              assertEquals(1, results[0][1]);
                          });
            }
        }
    }

    @Test
    public void testSyncCoordinatorCancel() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            createTrackedKeyspace(cluster, "4");

            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute(
                    "INSERT INTO " + tableName("4") + " (k, v) VALUES (?, ?)",
                    ConsistencyLevel.ONE, i, i);
            }

            // Drop sync responses so the coordinator never receives them
            cluster.filters().verbs(Verb.MT_SYNC_RSP.id).drop();

            // Start coordinator - it will be stuck waiting for sync responses
            Boolean wasCancelled = cluster.get(1).callOnInstance(() -> {
                Range<Token> range = fullTokenRange();
                RepairJobDesc desc = new RepairJobDesc(TimeUUID.Generator.nextTimeUUID(),
                                                       TimeUUID.Generator.nextTimeUUID(),
                                                       KS_NAME + '4', "", java.util.List.of(range));
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(
                    SharedContext.Global.instance, desc, null, ClusterMetadata.current());
                coordinator.start();

                try
                {
                    Thread.sleep(100);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }

                coordinator.cancel(); // Cancel it

                // Verify it was cancelled
                try
                {
                    coordinator.awaitCompletion();
                    return false; // Should have thrown
                }
                catch (Exception e)
                {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    return cause.getMessage() != null && cause.getMessage().contains("cancelled");
                }
            });
            assertTrue("Sync coordinator should be cancelled", wasCancelled);
        }
    }
}
