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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.metrics.MutationTrackingMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MutationTrackingMetricsTest extends TestBaseImpl
{
    private static final String CREATE_KEYSPACE =
            "CREATE KEYSPACE %s WITH replication = " +
                    "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                    "AND replication_type = 'tracked'";

    private static final String CREATE_TABLE =
            "CREATE TABLE %s.tbl (pk int PRIMARY KEY, val text)";

    @Test(timeout = 60000)
    @SuppressWarnings("Convert2MethodRef")
    public void testWriteTimeOffsetsDiscoveredMetric() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                .withConfig(cfg -> cfg.with(Feature.NETWORK)
                        .with(Feature.GOSSIP))
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Get initial write-time discovery counts on all nodes
            long initialNode1Count = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount());
            long initialNode2Count = cluster.get(2).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount());
            long initialNode3Count = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount());

            // Perform writes with QUORUM - each write goes to at least 2 replicas
            int numWrites = 10;
            for (int i = 0; i < numWrites; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                        ConsistencyLevel.QUORUM, i, "test" + i);
            }

            // Wait for all nodes to discover offsets at write time
            // With RF=3, each node should discover offsets and total should be at least numWrites * 3
            Awaitility.await()
                      .atMost(Duration.ofSeconds(5))
                      .pollInterval(Duration.ofMillis(100))
                      .until(() -> {
                          long node1Delta = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount()) - initialNode1Count;
                          long node2Delta = cluster.get(2).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount()) - initialNode2Count;
                          long node3Delta = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount()) - initialNode3Count;
                          long totalDiscovered = node1Delta + node2Delta + node3Delta;

                          return node1Delta > 0 && node2Delta > 0 && node3Delta > 0 && totalDiscovered >= (long) numWrites * 3;
                      });

            // Verify final counts
            long afterNode1Count = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount());
            long afterNode2Count = cluster.get(2).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount());
            long afterNode3Count = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().writeTimeOffsetsDiscovered.getCount());

            long node1Delta = afterNode1Count - initialNode1Count;
            long node2Delta = afterNode2Count - initialNode2Count;
            long node3Delta = afterNode3Count - initialNode3Count;

            assertThat(node1Delta)
                    .as("Node 1 should have discovered offsets at write time")
                    .isGreaterThan(0L);

            assertThat(node2Delta)
                    .as("Node 2 should have discovered offsets at write time")
                    .isGreaterThan(0L);

            assertThat(node3Delta)
                    .as("Node 3 should have discovered offsets at write time")
                    .isGreaterThan(0L);

            long totalDiscovered = node1Delta + node2Delta + node3Delta;
            assertThat(totalDiscovered)
                    .as("Total write-time discoveries across all nodes should be at least %d (RF=3)", numWrites * 3)
                    .isGreaterThanOrEqualTo((long) numWrites * 3);
        }
    }

    @Test(timeout = 60000)
    @SuppressWarnings("Convert2MethodRef")
    public void testBroadcastOffsetsDiscoveredMetric() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                .withConfig(cfg -> cfg.with(Feature.NETWORK)
                        .with(Feature.GOSSIP))
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Record initial broadcast metrics on receiving node 3 since we are next going to block this node to from receiving mutations
            long initialNode3Count = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().broadcastOffsetsDiscovered.getCount());

            // Block node 3 from receiving mutation writes (but allow broadcast messages)
            cluster.filters().verbs(Verb.MUTATION_REQ.id).to(3).drop();

            // Write data - nodes 1 and 2 will get it, node 3 won't
            int numWrites = 5;
            for (int i = 0; i < numWrites; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                        ConsistencyLevel.QUORUM, i, "test" + i);
            }

            // Verify node 3 missed the writes
            Object[][] node3Before = cluster.coordinator(3).execute(
                    withKeyspace("SELECT * FROM %s.tbl"), ConsistencyLevel.ONE);
            assertThat(node3Before.length)
                .as("Node 3 should have no data (was blocked)")
                .isEqualTo(0);

            // Broadcast offsets from node 1 to other nodes
            // This tells node 3 about mutations it's missing
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            // Wait for broadcasts to propagate to node 3
            long[] previousCount = {0};
            Awaitility.await()
                    .atMost(Duration.ofSeconds(5))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> {
                        long currentCount = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().broadcastOffsetsDiscovered.getCount());
                        boolean hasDiscoveredOffsets = currentCount > initialNode3Count;
                        boolean isStable = hasDiscoveredOffsets && currentCount == previousCount[0];
                        previousCount[0] = currentCount;
                        return isStable;
                    });

            // Get the count after first broadcast
            long afterFirstBroadcast = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().broadcastOffsetsDiscovered.getCount());

            // Broadcast the same offsets again (duplicate) - should NOT increment metric
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            // Wait for duplicate broadcast to propagate, then verify metric stayed the same
            // We poll to ensure the broadcast had time to arrive, then check it didn't increment
            Awaitility.await()
                      .pollDelay(Duration.ofMillis(200))
                      .atMost(Duration.ofSeconds(2))
                      .pollInterval(Duration.ofMillis(100))
                      .until(() -> {
                          long count = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().broadcastOffsetsDiscovered.getCount());
                          return count == afterFirstBroadcast; // Should remain at the same value (duplicate doesn't increment)
                      });

            // Clear filter to allow reconciliation
            cluster.filters().reset();

            // Read from node 3 to trigger reconciliation using broadcast offsets
            // Node 3 knows it's missing data (from broadcast offsets) and will request it
            // Poll for reconciliation to complete
            Awaitility.await()
                      .atMost(Duration.ofSeconds(10))
                      .pollInterval(Duration.ofMillis(200))
                      .until(() -> {
                          Object[][] result = cluster.coordinator(3).execute(
                                  withKeyspace("SELECT * FROM %s.tbl"),
                                  ConsistencyLevel.QUORUM);
                          return result.length == numWrites;
                      });

            // Verify all rows data is present after reconciliation
            Object[][] result = cluster.coordinator(3).execute(
                    withKeyspace("SELECT * FROM %s.tbl"),
                    ConsistencyLevel.QUORUM);
            assertThat(result.length)
                .as("Should return all rows after reconciliation")
                .isEqualTo(numWrites);

            // Check metrics after reconciliation - if reconciliation worked, broadcasts happened
            long afterNode3Count = cluster.get(3).callOnInstance(() -> MutationTrackingMetrics.instance().broadcastOffsetsDiscovered.getCount());
            long node3Delta = afterNode3Count - initialNode3Count;

            // Node 3 was blocked before and now must have applied broadcast offsets
            assertThat(node3Delta)
                .as("Node 3 should have applied broadcast offsets")
                .isGreaterThan(0L);
        }
    }

    @Test(timeout = 60000)
    @SuppressWarnings("Convert2MethodRef")
    public void testReadSummarySizeMetric() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                .withConfig(cfg -> cfg.with(Feature.NETWORK)
                        .with(Feature.GOSSIP))
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Get initial metric value from coordinator node
            long initialSize = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().readSummarySize.getCount());

            // Insert test data
            int numWrites = 10;
            for (int i = 0; i < numWrites; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                        ConsistencyLevel.QUORUM, i, "test" + i);
            }

            // Execute read operations (metric should increment once per read request)
            int numReads = 10;
            for (int i = 0; i < numReads; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"),
                        ConsistencyLevel.QUORUM, i);
            }

            // Verify metric incremented by at least twice the number of reads as
            // each read creates TWO summaries: initial (before read) + secondary (after read)
            // This is to detect concurrent writes during read execution for proper reconciliation
            long afterSize = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().readSummarySize.getCount());

            long delta = afterSize - initialSize;
            assertThat(delta)
                .as("Should have at least twice of %d summaries", numReads)
                .isGreaterThanOrEqualTo(2L * numReads);
        }
    }

    @Test(timeout = 60000)
    @SuppressWarnings("Convert2MethodRef")
    public void testUnreconciledMutationCountMetric() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                .withConfig(cfg -> cfg.with(Feature.NETWORK)
                        .with(Feature.GOSSIP))
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Get initial unreconciled count (should be 0)
            long initialCount = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().unreconciledMutationCount.getValue());
            assertThat(initialCount)
                .as("Initial unreconciled count should be 0")
                .isEqualTo(0L);

            // Block node 3 from receiving messages from node 1
            cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(3).drop();

            // Write with QUORUM (only nodes 1 and 2 will receive writes, node 3 won't)
            int numWrites = 10;
            for (int i = 0; i < numWrites; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                        ConsistencyLevel.QUORUM, i, "test" + i);
            }

            // Node 1 should now have unreconciled mutations (since node 3 didn't get them)
            long afterWrites = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().unreconciledMutationCount.getValue());
            assertThat(afterWrites)
                .as("Expected %d unreconciled mutations (node 3 blocked)", numWrites)
                .isEqualTo((long) numWrites);

            // Clear filters to allow reconciliation
            cluster.filters().reset();

            // Perform reads to trigger reconciliation
            for (int i = 0; i < numWrites; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"),
                        ConsistencyLevel.QUORUM, i);
            }

            // Wait for reconciliation to complete
            Awaitility.await()
                      .atMost(Duration.ofSeconds(5))
                      .pollInterval(Duration.ofMillis(100))
                      .until(() -> {
                          long count = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().unreconciledMutationCount.getValue());
                          return count == 0;
                      });

            // Verify reconciliation actually happened
            long afterReconcile = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().unreconciledMutationCount.getValue());
            assertThat(afterReconcile)
                .as("Unreconciled count should be 0 after reconciliation")
                .isEqualTo(0L);
        }
    }

    @Test(timeout = 60000)
    @SuppressWarnings("Convert2MethodRef")
    public void testJournalDiskSpaceUsedMetric() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                .withConfig(cfg -> cfg.with(Feature.NETWORK)
                        .set("commitlog_segment_size", "1MiB")) // Create a smaller size segment
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Get initial disk space - would be 2 * 1024 * 1024 as 2 segements are allocated by default
            long initialSpace = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().journalDiskSpaceUsed.getValue());

            // Write enough data to fill 1MiB segment and force new segment creation
            int numWrites = 200;
            for (int i = 0; i < numWrites; i++)
            {
                cluster.coordinator(1).execute(
                        withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                        ConsistencyLevel.ONE, i, "test-" + i);

                // Close segment every 20 writes to create multiple segments
                if (i % 20 == 0 && i > 0)
                    cluster.get(1).runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());
            }

            // Verify disk space increased
            long afterWrites = cluster.get(1).callOnInstance(() -> MutationTrackingMetrics.instance().journalDiskSpaceUsed.getValue());

            assertThat(afterWrites)
                .as("Disk space should increase after writes: before=%d", initialSpace)
                .isGreaterThan(initialSpace);
        }
    }

    /**
     * The local read latency must not include the time spent waiting for missing mutations from other replicas.
     */
    @Test(timeout = 60000)
    public void testLocalReadLatencyMetric() throws Throwable
    {
        long pullDelayMillis = 2000;
        try (Cluster cluster = Cluster.build(3)
                .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                      .set("read_request_timeout", "30000ms"))
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            // Node 1 misses the write, so the read has to pull the mutation from another replica
            cluster.filters().verbs(Verb.MUTATION_REQ.id).from(2).to(1).drop();
            cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                           ConsistencyLevel.QUORUM, 1, "test1");
            cluster.filters().reset();

            AtomicInteger pulls = new AtomicInteger();
            cluster.filters().verbs(Verb.MT_PULL_MUTATIONS_REQ.id).from(1).outbound().messagesMatching((from, to, message) -> {
                pulls.incrementAndGet();
                Uninterruptibles.sleepUninterruptibly(pullDelayMillis, TimeUnit.MILLISECONDS);
                return false;
            }).drop();

            long start = System.nanoTime();
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"),
                                           ConsistencyLevel.QUORUM, 1);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertThat(pulls.get()).as("The read must wait for a delayed pull for this test to be meaningful").isGreaterThan(0);
            assertThat(elapsedMillis).isGreaterThanOrEqualTo(pullDelayMillis);

            long count = cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readLatency.latency.getCount());
            long maxMicros = cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readLatency.latency.getSnapshot().getMax());
            assertThat(count)
                .as("Local read latency metric should count tracked reads")
                .isEqualTo(1);
            assertThat(maxMicros)
                .as("Local read latency should not include time spent waiting for other replicas")
                .isLessThan(TimeUnit.MILLISECONDS.toMicros(pullDelayMillis) / 2);
        }
    }

    @Test(timeout = 60000)
    public void testCoordinatorWriteTimeoutMetric() throws Throwable
    {
        long timeoutMillis = 1000;
        try (Cluster cluster = Cluster.build(3)
                .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                      .set("write_request_timeout", timeoutMillis + "ms"))
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            long initialTimeouts = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.timeouts.getCount());
            long initialLatencyCount = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.latency.getCount());

            // Only the coordinator's own replica can acknowledge the write
            cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(2, 3).drop();

            assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                                                    ConsistencyLevel.QUORUM, 1, "test1"))
                .hasMessageContaining("timed out");

            long timeouts = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.timeouts.getCount());
            long latencyCount = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.latency.getCount());
            long maxMicros = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.latency.getSnapshot().getMax());
            assertThat(timeouts)
                .as("Timed out tracked write should be counted")
                .isEqualTo(initialTimeouts + 1);
            assertThat(latencyCount)
                .as("Timed out tracked write should record latency")
                .isEqualTo(initialLatencyCount + 1);
            assertThat(maxMicros)
                .as("Recorded latency should cover the wait for the timeout")
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toMicros(timeoutMillis) / 2);
        }
    }

    @Test(timeout = 60000)
    public void testCoordinatorWriteLatencyMetric() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                .withConfig(cfg -> cfg.with(Feature.NETWORK))
                .start())
        {
            cluster.schemaChange(withKeyspace(CREATE_KEYSPACE));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));

            long initialCount = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.latency.getCount());
            long initialTableCount = cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.coordinatorWriteLatency.getCount());

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                           ConsistencyLevel.ONE, 1, "test1");

            long afterCount = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.latency.getCount());
            long afterTableCount = cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.coordinatorWriteLatency.getCount());
            assertThat(afterCount)
                .as("Coordinator write latency metric should count tracked writes")
                .isEqualTo(initialCount + 1);
            assertThat(afterTableCount)
                .as("Per-table coordinator write latency metric should count tracked writes")
                .isEqualTo(initialTableCount + 1);
        }
    }

    @Test(timeout = 60000)
    public void testWriteUnavailableMetric() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
                .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 2} " +
                                              "AND replication_type = 'tracked'"));
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE));
            cluster.get(2).shutdown().get();
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                cluster.get(1).callOnInstance(() -> org.apache.cassandra.gms.Gossiper.instance.getLiveMembers().size()) == 1);

            long initial = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.unavailables.getCount());

            assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, val) VALUES (?, ?)"),
                                                                    ConsistencyLevel.ALL, 1, "test1"))
                .hasMessageContaining("Cannot achieve consistency level");

            long after = cluster.get(1).callOnInstance(() ->
                ClientRequestsMetricsHolder.writeMetrics.unavailables.getCount());
            assertThat(after)
                .as("Unavailable write metric should count tracked writes")
                .isEqualTo(initial + 1);
        }
    }
}
