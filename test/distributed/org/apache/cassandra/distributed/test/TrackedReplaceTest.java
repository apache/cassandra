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
import org.apache.cassandra.distributed.api.IInvokableInstance;
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
 * Host replacement of a (dead) node in a tracked keyspace (single-token, no vnodes), exercising the replace-side
 * shard sealing in {@link org.apache.cassandra.replication.SealingCoordinator}.
 * <p>
 * A replacement reuses the dead node's token, so the ring layout is unchanged: only the replica membership of the
 * dead node's ranges shifts from the dead node to the (new-NodeId) replacement. The shards over those ranges that
 * still list the dead node as a participant are obsoleted by the replacement and must be sealed, mirroring the
 * join- and leave-side sealing covered by {@link TrackedBootstrapTest} and {@link TrackedUnbootstrapTest}.
 * <p>
 * Only same-cluster, different-address replacement is supported (same-address replacement is rejected at startup
 * when mutation tracking is enabled).
 * <p>
 * Uses human-readable single tokens (node N -> token N*100: 100, 200, ...) so the ring and every shard range is
 * trivial to read in the logs. The replaced node (REPLACE_TARGET) is interior (the ring wrap is owned by node 1 at
 * token 100), so the obsoleted shards are all interior - no MIN wraparound to reason about.
 */
public class TrackedReplaceTest extends TestBaseImpl
{
    private static final String KEYSPACE = "tracked_replacement_ks";
    private static final String TABLE    = "tbl";

    private static final int NODES           = 7;
    private static final int RF              = 3;
    private static final int REPLACE_TARGET  = 4; // a node in the middle of the ring

    @Test
    public void replacementSealsObsoletedShards() throws Throwable
    {
        // Human-readable single tokens (node N -> token N*100). The replaced node is interior, so its obsoleted
        // shards are interior ranges (no MIN wraparound).
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

            // Baseline data written before the replacement begins.
            int baseline = 1000;
            Map<Integer, Integer> expected = new ConcurrentHashMap<>();
            for (int i = 0; i < baseline; i++)
            {
                cluster.coordinator(1).execute(format("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", KEYSPACE, TABLE),
                                               ConsistencyLevel.QUORUM, i, i);
                expected.put(i, i);
            }

            IInvokableInstance victim = cluster.get(REPLACE_TARGET);
            Set<String> victimTokens = getNodeTokens(victim);
            ClusterUtils.stopUnchecked(victim);

            // Keep client writes running in the background for the whole duration of the replacement (victim down ->
            // streaming -> finished), through always-alive surviving coordinators (nodes 1 and 2; the victim is node
            // REPLACE_TARGET). The replacement must obtain these writes -- already-acked ones by streaming from the
            // survivors, in-flight ones via the over-replicated write set it joins while BOOT_REPLACING -- and the
            // obsoleted shards must still reconcile and seal despite the dead participant. Only keys the coordinator
            // acknowledged are recorded as expected, so the post-replacement read-back is exact.
            AtomicBoolean done = new AtomicBoolean(false);
            AtomicInteger nextKey = new AtomicInteger(baseline);
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

            // Replace the dead node (different-address, token reused from the victim's config).
            IInvokableInstance replacement = ClusterUtils.replaceHostAndStart(cluster, victim);
            ClusterUtils.awaitRingJoin(cluster.get(1), replacement);
            ClusterUtils.awaitRingJoin(replacement, cluster.get(1));

            // Stop background writes and surface any failure.
            done.set(true);
            writer.join();
            if (bgError.get() != null)
                throw new AssertionError("Background writes failed during replacement", bgError.get());
            assertTrue("expected concurrent writes to run during the replacement", nextKey.get() > baseline);

            // The replacement reuses the dead node's tokens.
            assertEquals("replacement should own the replaced node's tokens", victimTokens, getNodeTokens(replacement));

            assertNoUnsealedShardReferencesDepartedNode(cluster);

            // All baseline + concurrent writes are preserved, read back at QUORUM through a surviving node and
            // through the freshly started replacement node.
            assertAllRows(cluster.coordinator(1), expected);
            assertAllRows(replacement.coordinator(), expected);
        }
    }

    /**
     * Correctness invariant after replacement: no surviving node may keep an *unsealed* shard that still lists a
     * departed (non-member) node as a participant. The replaced node is removed from the directory at FINISH_REPLACE,
     * so its id is no longer a member; any pre-replace shard over its ranges is obsoleted and could never finish
     * background reconciliation (it would wait forever on the departed participant), so the replace-side sealing must
     * have reconciled and sealed all of them.
     */
    private static void assertNoUnsealedShardReferencesDepartedNode(Cluster cluster)
    {
        for (int n = 1; n <= NODES; n++)
        {
            if (n == REPLACE_TARGET)
                continue; // the dead node's slot; the replacement is a separate instance
            cluster.get(n).runOnInstance(() -> {
                // Only JOINED nodes count as members. The replaced node may linger in the directory briefly;
                // filter to JOINED so an unsealed shard still listing a departed node is actually flagged.
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
    }

    private static Set<String> getNodeTokens(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            Set<String> tokens = new HashSet<>();
            metadata.tokenMap.tokens(metadata.myNodeId()).forEach(t -> tokens.add(t.toString()));
            return tokens;
        });
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
