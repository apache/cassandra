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
    public void testSyncCoordinatorCompletesAfterDataSync() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(6).start())
        {
            createTrackedKeyspace(cluster, "2");

            // Insert some data to create mutations
            for (int i = 0; i < 10000; i++)
            {
                cluster.coordinator(1).execute(
                    "INSERT INTO " + tableName("2") + " (k, v) VALUES (?, ?)",
                    ConsistencyLevel.ALL, i, i
                );
            }

            Thread.sleep(500); // Wait for offset broadcasts to propagate

            // Create a sync coordinator - should complete since all data is synced (CL.ALL)
            Boolean completed = cluster.get(1).callOnInstance(() -> {
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME + '2', fullTokenRange());
                coordinator.start();

                try
                {
                    // Give it enough time for broadcasts to arrive
                    return coordinator.awaitCompletion(15, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
            });

            assertTrue("Sync coordinator should complete after data is fully replicated", completed);
        }
    }

    @Test
    public void testSyncCoordinatorWaitsForAllReplicasMutations() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(6).start())
        {
            createTrackedKeyspace(cluster, "3");

            // Pause broadcasts so nodes don't share offsets yet
            pauseOffsetBroadcasts(cluster, true);

            // Write from different nodes with CL.ONE - each node has different mutations
            // Different coordinators create mutations that only their local replica group knows about initially
            cluster.coordinator(1).execute("INSERT INTO " + tableName("3") + " (k, v) VALUES (1, 1)", ConsistencyLevel.ONE);
            cluster.coordinator(2).execute("INSERT INTO " + tableName("3") + " (k, v) VALUES (2, 2)", ConsistencyLevel.ONE);
            cluster.coordinator(3).execute("INSERT INTO " + tableName("3") + " (k, v) VALUES (3, 3)", ConsistencyLevel.ONE);

            // Resume broadcasts so nodes can share their offsets
            pauseOffsetBroadcasts(cluster, false);

            // Trigger broadcasts to share offsets between nodes
            for (int i = 1; i <= 6; i++)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance.broadcastOffsetsForTesting());

            Thread.sleep(500); // Wait for broadcasts to propagate

            Boolean completed = cluster.get(4).callOnInstance(() -> {
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME + "3", fullTokenRange());
                coordinator.start();

                try
                {
                    return coordinator.awaitCompletion(30, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
            });

            assertTrue("Sync should complete after all mutations from all nodes are reconciled", completed);
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
                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME + "4", fullTokenRange());
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
}
