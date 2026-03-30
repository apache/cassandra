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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Shard;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Decommission of a node in a tracked keyspace (single-token, no vnodes), exercising the leave-side shard sealing
 * in {@link org.apache.cassandra.replication.SealingCoordinator}:
 * <ul>
 *   <li>{@code collectAndSealDecommissionObsoletedShards} (pre-streaming, from {@code UnbootstrapStreams}) seals the pre-leave
 *       shards over the handed-off ranges, and</li>
 *   <li>{@code collectAndSealFinishLeaveShards} (post-FINISH_LEAVE, from {@code UnbootstrapAndLeave}) seals the
 *       intermediate over-replicated shards created during START_LEAVE plus any shard obsoleted only by the
 *       range merge.</li>
 * </ul>
 * Uses evenly-distributed tokens across the full Murmur3 ring (single-token, no vnodes; node N -> the N-th token in
 * increasing order) so each node owns a real, balanced fraction of the token space.
 */
public class TrackedUnbootstrapTest extends TestBaseImpl
{
    private static final String KEYSPACE = "tracked_decommission_ks";
    private static final String TABLE    = "tbl";

    private static final int NODES               = 7;
    private static final int RF                  = 3;
    private static final int DECOMMISSION_TARGET = 4; // a node in the middle of the ring

    @Test
    public void decommissionSealsObsoletedShards() throws Throwable
    {
        // Human-readable single tokens (node N -> token N*100). The leaving node is interior (the ring wrap is
        // owned by node 1 at token 100), so its ranges and the FINISH_LEAVE merge are all interior - no MIN wraparound.
        TokenSupplier tokenSupplier = i -> java.util.Collections.singleton(String.valueOf(i * 100L));

        try (Cluster cluster = builder().withNodes(NODES)
                                        .withTokenSupplier(tokenSupplier)
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(NODES, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    .set("num_tokens", 1))
                                        .start())
        {
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy','replication_factor':%d} AND replication_type='tracked'",
                                        KEYSPACE, RF));
            cluster.schemaChange(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v int)", KEYSPACE, TABLE));

            for (int n = 1; n <= NODES; n++)
                ClusterUtils.awaitRingHealthy(cluster.get(n));

            int rows = 1000;
            Map<Integer, Integer> expected = new ConcurrentHashMap<>();
            for (int i = 0; i < rows; i++)
            {
                cluster.coordinator(1).execute(format("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", KEYSPACE, TABLE),
                                               ConsistencyLevel.QUORUM, i, i);
                expected.put(i, i);
            }

            // Keep client writes running in the background for the whole duration of the decommission, through
            // surviving coordinators (nodes 1 and 2; the leaving node is node DECOMMISSION_TARGET). The obsoleted
            // shards must still reconcile and seal despite the in-flight writes and the departing participant. Only
            // acked keys are recorded, continuing past the baseline range so the read-back is exact.
            AtomicBoolean done = new AtomicBoolean(false);
            AtomicInteger nextKey = new AtomicInteger(rows);
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
                    LockSupport.parkNanos(5_000_000L); // ~5ms think-time: exercise concurrency without saturating the test cluster
                }
            }, "background-writer");
            writer.start();

            cluster.get(DECOMMISSION_TARGET).nodetoolResult("decommission", "--force").asserts().success();

            // Stop background writes and surface any failure.
            done.set(true);
            writer.join();
            if (bgError.get() != null)
                throw new AssertionError("Background writes failed during decommission", bgError.get());
            assertTrue("expected concurrent writes to run during the decommission", nextKey.get() > rows);

            // Correctness invariant after decommission: no surviving node may keep an *unsealed* shard that still
            // lists the departed node as a participant. Such a shard could never finish background reconciliation
            // (it would wait forever on the departed participant), so the leave-side sealing must have reconciled
            // and sealed all of them (the intermediate START_LEAVE shards). Shards obsoleted only by the range merge
            // keep only live participants and are allowed to remain unsealed (they reconcile on their own).
            for (int n = 1; n <= NODES; n++)
            {
                if (n == DECOMMISSION_TARGET)
                    continue;
                cluster.get(n).runOnInstance(() -> {
                    // Only JOINED nodes count as members. The decommissioned node lingers in the directory in state
                    // LEFT (still in peers, removed only at a later unregister), so peerIds() would still include it
                    // and rob this check of teeth; filter to JOINED so an unsealed shard still listing the departed
                    // node is actually flagged.
                    ClusterMetadata cm = ClusterMetadata.current();
                    Set<Integer> members = new HashSet<>();
                    for (NodeId id : cm.directory.peerIds())
                        if (cm.directory.peerState(id) == NodeState.JOINED)
                            members.add(id.id());
                    for (Shard shard : MutationTrackingService.instance().getShards())
                    {
                        if (!shard.keyspace.equals(KEYSPACE) || shard.isSealed())
                            continue;
                        for (int participant : shard.participants.asSet())
                            if (!members.contains(participant))
                                throw new AssertionError(format("Unsealed shard %s@%d references departed node %d (participants %s)",
                                                                shard.range, shard.sinceEpoch, participant, shard.participants));
                    }
                });
            }

            // Data is fully preserved across the decommission, read back at QUORUM through a surviving node.
            assertAllRows(cluster.coordinator(1), expected);
        }
    }

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
