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

    @Test
    public void testSyncCoordinatorCompletesWhenNoShards() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            // Create a tracked keyspace
            cluster.schemaChange("CREATE KEYSPACE " + KS_NAME + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                 "AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE " + KS_NAME + '.' + TBL_NAME + " (k int PRIMARY KEY, v int)");

            // Create a sync coordinator for a range that has no data
            // It should complete immediately since there are no offsets to sync
            Boolean completed = cluster.get(1).callOnInstance(() -> {
                Range<Token> fullRange = new Range<>(
                    new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
                    new Murmur3Partitioner.LongToken(Long.MAX_VALUE)
                );

                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME, fullRange);
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
            // Create a tracked keyspace
            cluster.schemaChange("CREATE KEYSPACE " + KS_NAME + "2 WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                 "AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE " + KS_NAME + "2.tbl (k int PRIMARY KEY, v int)");

            // Insert some data to create mutations
            for (int i = 0; i < 10000; i++)
            {
                cluster.coordinator(1).execute(
                    "INSERT INTO " + KS_NAME + "2.tbl (k, v) VALUES (?, ?)",
                    ConsistencyLevel.ALL, i, i
                );
            }

            Thread.sleep(500); // Wait for offset broadcasts to propagate

            // Create a sync coordinator - should complete since all data is synced (CL.ALL)
            Boolean completed = cluster.get(1).callOnInstance(() -> {
                Range<Token> fullRange = new Range<>(
                    new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
                    new Murmur3Partitioner.LongToken(Long.MAX_VALUE)
                );

                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(KS_NAME + '2', fullRange);
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
    public void testSyncCoordinatorCancel() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            // Create a tracked keyspace with data so there are shards to sync
            cluster.schemaChange("CREATE KEYSPACE cancel_test_ks WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                 "AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE cancel_test_ks.tbl (k int PRIMARY KEY, v int)");

            // Pause offset broadcasts on all nodes to prevent sync from completing
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance.pauseOffsetBroadcast(true));
            }

            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute(
                    "INSERT INTO cancel_test_ks.tbl (k, v) VALUES (?, ?)",
                    ConsistencyLevel.ONE, i, i);
            }

            // Start coordinator - it will be stuck waiting for offsets
            Boolean wasCancelled = cluster.get(1).callOnInstance(() -> {
                Range<Token> fullRange = new Range<>(
                    new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
                    new Murmur3Partitioner.LongToken(Long.MAX_VALUE)
                );

                MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator("cancel_test_ks", fullRange);
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
                    return e.getMessage() != null  && e.getMessage().contains("cancelled");
                }
            });
            assertTrue("Sync coordinator should be cancelled", wasCancelled);
        }
    }
}
