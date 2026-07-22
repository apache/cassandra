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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Bootstrap of a tracked keyspace (single-token, no vnodes). Exercises {@code BootstrapAndJoin.bootstrap()}
 * including the (happy-path) sealing of shards obsoleted by the join, and asserts the new node joins the ring.
 * <ul>
 *   <li>{@link #bootstrapSealsObsoletedShards()} — 3 nodes -> 4, RF=3 (RF == old cluster size: one seal group)</li>
 *   <li>{@link #bootstrapSealsObsoletedShards_largerRing()} — 5 nodes -> 6, RF=3 (acquired ranges span
 *       several pre-join replica sets: multiple seal groups)</li>
 * </ul>
 * Uses evenly-distributed tokens across the full Murmur3 ring (single-token, no vnodes; node N -> the N-th token in
 * increasing order); the joining node is assigned the top token slot, so its acquired range is interior (no MIN
 * wraparound).
 */
public class TrackedBootstrapTest extends TestBaseImpl
{
    /**
     * Keyspace / table used across all tests.  Each test method uses its own {@link Cluster}
     * instance, so re-using the same names is fine.
     */
    private static final String KEYSPACE = "tracked_bootstrap_ks";
    private static final String TABLE    = "tbl";

    @Test
    public void bootstrapSealsObsoletedShards() throws Throwable
    {
        bootstrapAndSeal(3, 3);
    }

    @Test
    public void bootstrapSealsObsoletedShards_largerRing() throws Throwable
    {
        bootstrapAndSeal(5, 3);
    }

    /**
     * Start {@code initialNodes} (single token each, evenly-distributed across the Murmur3 ring), create an
     * RF={@code rf} tracked keyspace, then bootstrap one more node. The joining node is assigned the top token slot
     * of the expanded ring, so its acquired range is interior - no MIN wraparound to reason about.
     */
    private void bootstrapAndSeal(int initialNodes, int rf) throws Throwable
    {
        int expandedNodes = initialNodes + 1;
        // Real Murmur3 tokens for the full expanded ring; the joining node (index expandedNodes) gets the top
        // token slot, whose primary range is interior (does not wrap MIN).
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(expandedNodes, 1);
        try (Cluster cluster = builder().withNodes(initialNodes)
                                        .withTokenSupplier(tokenSupplier)
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodes, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    .set("num_tokens", 1))
                                        .start())
        {
            // RF tracked keyspace (mutation tracking is enabled by default in jvm-dtest)
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy','replication_factor':%d} AND replication_type='tracked'",
                                        KEYSPACE, rf));
            cluster.schemaChange(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v int)", KEYSPACE, TABLE));

            // Wait until every initial node sees the whole ring as UP + NORMAL before issuing writes, so the
            // tracked-write forwarding paths see all replicas alive rather than racing startup/gossip.
            for (int n = 1; n <= initialNodes; n++)
                ClusterUtils.awaitRingHealthy(cluster.get(n));

            // Pre-bootstrap writes: exercise the regular and Paxos (LWT) tracked-write chokepoints.
            // (Counters are intentionally omitted: with RF < N the tracked counter-leader forward path
            // -- ForwardedWrite.forwardCounterMutationInternal -> findCounterLeaderReplica -- fails to
            // resolve a remote leader; tracked-counter forwarding is a separate, pre-existing issue.)
            int rows = 1000;
            // Mirror the write operations (in order) to derive the expected final state for read-back.
            Map<Integer, Integer> expected = new ConcurrentHashMap<>();
            for (int i = 0; i < rows; i++)
            {
                // regular tracked write -> TrackedWriteRequest.perform (regular branch)
                cluster.coordinator(1).execute(format("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", KEYSPACE, TABLE),
                                               ConsistencyLevel.QUORUM, i, i);
                expected.put(i, i);
                // LWT -> Paxos tracked commit path (StorageProxy.commitPaxosTracked)
                cluster.coordinator(1).execute(format("INSERT INTO %s.%s (pk, v) VALUES (?, ?) IF NOT EXISTS", KEYSPACE, TABLE),
                                               ConsistencyLevel.QUORUM, 100 + i, i);
                expected.putIfAbsent(100 + i, i);
            }

            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance newNode = cluster.bootstrap(config);

            // Keep client writes running in the background for the whole duration of the bootstrap, through the
            // existing (always-alive) coordinators. The joining node must obtain these writes -- already-acked ones
            // by streaming from the seeded replicas, in-flight ones via the write set it joins while BOOTSTRAPPING --
            // and the obsoleted shards must still reconcile and seal. Only acked keys are recorded, using a key range
            // disjoint from the baseline writes so the read-back is exact.
            int concurrentBase = 1_000_000;
            AtomicBoolean done = new AtomicBoolean(false);
            AtomicInteger nextKey = new AtomicInteger(concurrentBase);
            AtomicReference<Throwable> bgError = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                while (!done.get())
                {
                    int k = nextKey.getAndIncrement();
                    // Writes can race the in-flight topology change and hit a retryable "ring has changed" error;
                    // retry the (idempotent) insert until it is acknowledged, as a real client would.
                    for (int attempt = 1; ; attempt++)
                    {
                        try
                        {
                            cluster.coordinator((k % 2) + 1)
                                   .execute(format("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", KEYSPACE, TABLE),
                                            ConsistencyLevel.QUORUM, k, k);
                            expected.put(k, k);
                            break;
                        }
                        catch (Throwable t)
                        {
                            if (done.get())
                                return; // shutting down; drop this in-flight write
                            if (attempt >= 100)
                            {
                                bgError.set(t);
                                return;
                            }
                            LockSupport.parkNanos(20_000_000L); // 20ms backoff, then retry the same key
                        }
                    }
                }
            }, "background-writer");
            writer.start();

            newNode.startup(cluster);

            ClusterUtils.awaitRingJoin(cluster.get(1), newNode);
            ClusterUtils.awaitRingJoin(newNode, cluster.get(1));

            // Stop background writes and surface any failure.
            done.set(true);
            writer.join();
            if (bgError.get() != null)
                throw new AssertionError("Background writes failed during bootstrap", bgError.get());
            assertTrue("expected concurrent writes to run during the bootstrap", nextKey.get() > concurrentBase);

            // Read the full dataset back at QUORUM, both through an existing replica and through the freshly
            // bootstrapped node, exercising the tracked read path across the post-seal topology.
            assertAllRows(cluster.coordinator(1), expected);
            assertAllRows(newNode.coordinator(), expected);
        }
    }

    /** Read the whole table at QUORUM through {@code coordinator} and assert it matches {@code expected}. */
    private static void assertAllRows(ICoordinator coordinator, Map<Integer, Integer> expected)
    {
        Object[][] result = coordinator.execute(format("SELECT pk, v FROM %s.%s", KEYSPACE, TABLE),
                                                ConsistencyLevel.QUORUM);
        Map<Integer, Integer> actual = new HashMap<>(result.length);
        for (Object[] row : result)
            actual.put((Integer) row[0], (Integer) row[1]);
        assertEquals("unexpected row count", expected.size(), actual.size());
        assertEquals("row contents differ", expected, actual);
    }
}
