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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.MutationTrackingSyncCoordinator;
import org.awaitility.Awaitility;

import static org.junit.Assert.*;

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

    private void pauseOffsetBroadcasts(Cluster cluster, boolean pause)
    {
        for (int i = 1; i <= cluster.size(); i++)
            cluster.get(i).runOnInstance(() -> MutationTrackingService.instance.pauseOffsetBroadcast(pause));
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
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME, fullTokenRange());
                coordinator.start();

                try
                {
                    return coordinator.awaitCompletion(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
            });

            assertTrue("Sync coordinator should complete when there are no pending offsets", completed);
        }
    }

    @Test
    public void testSyncCoordinatorWaitsForAllReplicasMutations() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            createTrackedKeyspace(cluster, "3");

            // Block all messages FROM node 1 to prevent write replication
            // This ensures that write only succeeds locally on node 1
            cluster.filters().allVerbs().from(1).drop();

            cluster.coordinator(1).execute(
                "INSERT INTO " + tableName("3") + " (k, v) VALUES (1, 1)",
                ConsistencyLevel.ONE
            );

            // Start MutationTrackingSyncCoordinator on node 2 in a separate thread
            // It should wait for offsets to sync since node 1's data hasn't propagated yet
            long syncStartTime = System.currentTimeMillis();
            CompletableFuture<Boolean> coordinatorFuture = CompletableFuture.supplyAsync(() -> cluster.get(2).callOnInstance(() -> {
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME + '3', fullTokenRange());
                coordinator.start();

                try
                {
                    return coordinator.awaitCompletion(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
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

            // Verify coordinator stays blocked for at least 2 seconds
            Awaitility.await()
                      .during(Duration.ofSeconds(2))
                      .atMost(Duration.ofSeconds(3))
                      .until(() -> !coordinatorFuture.isDone());

            cluster.filters().reset();

            for (int i = 1; i <= 3; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance.broadcastOffsetsForTesting());

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

            // Verify the sync respected the minimum broadcast wait time (MIN_BROADCAST_WAIT_MS = 300ms)
            long syncDuration = System.currentTimeMillis() - syncStartTime;
            assertTrue("Sync should wait at least MIN_BROADCAST_WAIT_MS (300ms). Actual: " + syncDuration + "ms",
                       syncDuration >= 300);
        }
    }

    @Test
    public void testSyncCoordinatorCancel() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            createTrackedKeyspace(cluster, "4");

            // Pause offset broadcasts on all nodes to prevent sync from completing
            pauseOffsetBroadcasts(cluster, true);

            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute(
                    "INSERT INTO " + tableName("4") + " (k, v) VALUES (?, ?)",
                    ConsistencyLevel.ONE, i, i);
            }

            // Start coordinator - it will be stuck waiting for offsets
            Boolean wasCancelled = cluster.get(1).callOnInstance(() -> {
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME + '4', fullTokenRange());
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
                    coordinator.awaitCompletion(1, TimeUnit.SECONDS);
                    return false; // Should have thrown
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
                catch (RuntimeException e)
                {
                    return e.getMessage() != null && e.getMessage().contains("cancelled");
                }
            });
            assertTrue("Sync coordinator should be cancelled", wasCancelled);
        }
    }

    @Test
    public void testSyncCoordinatorTimesOutOnUnresponsiveParticipant() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            createTrackedKeyspace(cluster, "5");

            cluster.coordinator(1).execute(
                "INSERT INTO " + tableName("5") + " (k, v) VALUES (1, 1)",
                ConsistencyLevel.ALL
            );

            // Broadcast from all nodes first so they're in sync
            for (int i = 1; i <= cluster.size(); i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance.broadcastOffsetsForTesting());

            // Block all messages FROM node 3 permanently - it will never report
            cluster.filters().allVerbs().from(3).drop();

            long syncStartTime = System.currentTimeMillis();

            // Start sync coordinator on node 1 - it should time out waiting for node 3
            Boolean completed = cluster.get(1).callOnInstance(() -> {
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(
                    KS_NAME + '5', fullTokenRange());
                coordinator.start();

                try
                {
                    // Wait longer than PARTICIPANT_TIMEOUT_MS (10s) + buffer
                    return coordinator.awaitCompletion(20, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
            });

            long syncDuration = System.currentTimeMillis() - syncStartTime;

            assertTrue("Sync coordinator should complete after timeout", completed);
            // Should have taken at least PARTICIPANT_TIMEOUT_MS (10s)
            assertTrue("Sync should have timed out waiting for participant. Actual: " + syncDuration + "ms",
                       syncDuration >= 10000);
        }
    }
}
