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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/*
 * Sealing {@link Shard}s once a topology change obsoletes them.
 *
 * Happy Path Sequence:
 * 1. Bring every participant up to (at least) this seal's epoch and fence allocation, so that everyone
 *    stops writing to the old shards; then wait until all in-progress mutations have been done.
 * 2. Same for tracked transfers
 * 3. Wait until all participating replicas have reconciled the union of their witnessed ids
 * 4. Mark the shard as fully sealed
 * 5. Stop including the shard in mutation summaries for read requests.
 *
 * Unhappy Path Sequence (not yet implemented):
 * 1. Wait until everyone who's up stops writing to the old shards and all in-progress mutations have been done
 * 2. Same for tracked transfers
 * 3. Wait until all *live* participating replicas have reconciled the union of their witnessed ids
 * 4. Mark the shard as partially sealed, label it with the set of participants that were able to seal
 * 5. When a down node comes up, have it talk to other nodes and learn about sealing, and what mutations/logs
 *    they have exchanged. Diff with what we have - in system tables and in the journal (though system table
 *    plus local replay should give us an up-to-date metadata state for the shard)
 */
public final class SealingCoordinator
{
    /*
     * BOOTSTRAP sequence illustrated. Stages in which write placements
     * remain unchanged, and only read placements change, are skipped
     * here as irrelevant to sharding / sealing.
     *
     * There is a tracked keyspace with RF=3.
     * A node n35 with token=35 is added in the middle of the ring.
     *
     * 1. Initial state at e0:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | ACTIVE
     *
     * 2. PREPARE_JOIN at e1:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | ACTIVE
     *    + (30,35] | {n40,n50,n60}     | e1 | ACTIVE
     *    + (35,40] | {n40,n50,n60}     | e1 | ACTIVE
     *
     * 3. START_JOIN at e2:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | ACTIVE
     *    + (10,20] | {n20,n30,n35,n40} | e2 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | ACTIVE
     *    + (20,30] | {n30,n35,n40,n50} | e2 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | ACTIVE
     *      (30,35] | {n40,n50,n60}     | e1 | ACTIVE
     *    + (30,35] | {n35,n40,n50,n60} | e2 | ACTIVE
     *      (35,40] | {n40,n50,n60}     | e1 | ACTIVE
     *
     * 4. MID_JOIN at e3 [*]:
     *
     *    S (10,20] | {n20,n30,n40}     | e0 | SEALED
     *      (10,20] | {n20,n30,n35,n40} | e2 | ACTIVE
     *    S (20,30] | {n30,n40,n50}     | e0 | SEALED
     *      (20,30] | {n30,n35,n40,n50} | e2 | ACTIVE
     *    S (30,40] | {n40,n50,n60}     | e0 | SEALED
     *    S (30,35] | {n40,n50,n60}     | e1 | SEALED
     *      (30,35] | {n35,n40,n50,n60} | e2 | ACTIVE
     *      (35,40] | {n40,n50,n60}     | e1 | ACTIVE
     *
     * 5. FINISH_JOIN at e4:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | SEALED
     *      (10,20] | {n20,n30,n35,n40} | e2 | ACTIVE
     *    + (10,20] | {n20,n30,n35}     | e4 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | SEALED
     *      (20,30] | {n30,n35,n40,n50} | e2 | ACTIVE
     *    + (20,30] | {n30,n35,n40}     | e4 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | SEALED
     *      (30,35] | {n40,n50,n60}     | e1 | SEALED
     *      (30,35] | {n35,n40,n50,n60} | e2 | ACTIVE
     *    + (30,35] | {n35,n40,n50}     | e4 | ACTIVE
     *      (35,40] | {n40,n50,n60}     | e1 | ACTIVE
     *
     * 6. UNLOCK_SEQUENCE:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | SEALED
     *    S (10,20] | {n20,n30,n35,n40} | e2 | SEALED
     *      (10,20] | {n20,n30,n35}     | e4 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | SEALED
     *    S (20,30] | {n30,n35,n40,n50} | e2 | SEALED
     *      (20,30] | {n30,n35,n40}     | e4 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | SEALED
     *      (30,35] | {n40,n50,n60}     | e1 | SEALED
     *    S (30,35] | {n35,n40,n50,n60} | e2 | SEALED
     *      (30,35] | {n35,n40,n50}     | e4 | ACTIVE
     *      (35,40] | {n40,n50,n60}     | e1 | ACTIVE
     *
     * [*] At this stage, after sealing the shards, we stream the sstables.
     * All the offsets belonging to these sealed shards get stripped from
     * stats component of streamed sstables. Otherwise, the new node
     * won't be able to compact them or mark them as reconciled/repaired -
     * it is rightfully unaware of the sealed shards.
     */

    /**
     * Seal the shards obsoleted by bootstrap before the new node starts streaming.
     */
    public static void sealShardsAtMidJoin(ClusterMetadata metadata, MovementMap movements)
    {
        seal(discoverShardsAtMidJoinOrReplace(metadata, movements));
    }

    /**
     * Seal the intermediate, over-replicated shards created during START_JOIN and obsoleted by FINISH_JOIN.
     */
    public static void sealShardsAtFinishJoin(ClusterMetadata metadata, long finishEpoch, PlacementDeltas finishDelta)
    {
        seal(discoverShardsAtFinishJoinOrReplace(metadata, finishEpoch, finishDelta, null));
    }

    /*
     * REPLACE sequence illustrated. Stages in which write placements
     * remain unchanged, and only read placements change, are skipped
     * here as irrelevant to sharding / sealing.
     *
     * There is a tracked keyspace with RF=3.
     * Node n40 is dead, and is being replaced with $n40.
     *
     * 1. Initial state at e0:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | ACTIVE
     *
     * 2. START_REPLACE at e1:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | ACTIVE
     *    + (10,20] | {n20,n30,n40,$n40}| e1 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | ACTIVE
     *    + (20,30] | {n30,n40,$n40,n50}| e1 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | ACTIVE
     *    + (30,40] | {n40,$n40,n50,n60}| e1 | ACTIVE
     *
     * 3. MID_REPLACE at e2 [*]:
     *
     *    S (10,20] | {n20,n30,n40}     | e0 | SEALED
     *      (10,20] | {n20,n30,n40,$n40}| e1 | ACTIVE
     *    S (20,30] | {n30,n40,n50}     | e0 | SEALED
     *      (20,30] | {n30,n40,$n40,n50}| e1 | ACTIVE
     *    S (30,40] | {n40,n50,n60}     | e0 | SEALED
     *      (30,40] | {n40,$n40,n50,n60}| e1 | ACTIVE
     *
     * 4. FINISH_REPLACE at e3:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | SEALED
     *      (10,20] | {n20,n30,n40,$n40}| e1 | ACTIVE
     *    + (10,20] | {n20,n30,$n40}    | e3 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | SEALED
     *      (20,30] | {n30,n40,$n40,n50}| e1 | ACTIVE
     *    + (20,30] | {n30,$n40,n50}    | e3 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | SEALED
     *      (30,40] | {n40,$n40,n50,n60}| e1 | ACTIVE
     *    + (30,40] | {$n40,n50,n60}    | e3 | ACTIVE
     *
     * 5. UNLOCK_SEQUENCE:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | SEALED
     *    S (10,20] | {n20,n30,n40,$n40}| e1 | SEALED
     *      (10,20] | {n20,n30,$n40}    | e3 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | SEALED
     *    S (20,30] | {n30,n40,$n40,n50}| e1 | SEALED
     *      (20,30] | {n30,$n40,n50}    | e3 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | SEALED
     *    S (30,40] | {n40,$n40,n50,n60}| e1 | SEALED
     *      (30,40] | {$n40,n50,n60}    | e3 | ACTIVE
     *
     * [*] At this stage, after sealing the shards, we stream the sstables.
     * Non-repaired (not fully reconciled) SSTables must be split into
     * fully reconciled part and non-reconciled part. When streaming the
     * former, we will strip all the log offsets from their metadata.
     * Otherwise, the new node won't be able to compact them or mark them
     * as reconciled/repaired - it is rightfully unaware of the sealed shards.
     * If we don't split them into two parts, then we risk being unable to
     * rebuild and filter any of the streamed SSTables on the receiving node.
     */

    /**
     * Seal the shards obsoleted by host replacement before the replacement node starts streaming.
     * The {@code deadNode} node cannot participate in the sealing.
     */
    public static void sealShardsAtMidReplace(ClusterMetadata metadata, MovementMap movements, @Nonnull NodeId deadNode)
    {
        seal(discoverShardsAtMidJoinOrReplace(metadata, movements), deadNode);
    }

    /**
     * Seal the intermediate, over-replicated shards created during START_REPLACE and obsoleted by FINISH_REPLACE.
     * The {@code deadNode} node cannot participate in the sealing.
     */
    public static void sealShardsAtFinishReplace(ClusterMetadata metadata, long finishEpoch, PlacementDeltas finishDelta, @Nonnull NodeId deadNode)
    {
        seal(discoverShardsAtFinishJoinOrReplace(metadata, finishEpoch, finishDelta, deadNode), deadNode);
    }

    /**
     * Discover all shards that need to be sealed at MID_JOIN or MID_REPLACE.
     */
    private static Set<ShardMetadata> discoverShardsAtMidJoinOrReplace(ClusterMetadata metadata, MovementMap movements)
    {
        List<Future<Set<ShardMetadata>>> futures = new ArrayList<>();

        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
        {
            if (!ksm.params.replicationType.isTracked())
                continue;

            EndpointsByReplica acquired = movements.get(ksm.params.replication);
            if (acquired.isEmpty()) // when the keyspace is not replicated to this DC
                continue;

            DataPlacement placement = metadata.placements.get(ksm.params.replication);

            for (Map.Entry<Replica, EndpointsForRange> entry : acquired.entrySet())
            {
                Range<Token> range = entry.getKey().range();
                Set<InetAddressAndPort> endpoints = entry.getValue().endpoints();
                // cutoff: the epoch this node was added to the write placement for the range;
                // shards over the range with sinceEpoch < cutoff are obsoleted by this bootstrap or replace.
                long beforeEpoch = placement.writes.forRange(range).lastModified().getEpoch();
                futures.add(FetchShards.fetch(ksm.name, beforeEpoch, range, endpoints));
            }
        }

        Set<ShardMetadata> shards = new HashSet<>();
        try
        {
            for (Set<ShardMetadata> fetched : FutureCombiner.allOf(futures).get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS))
                shards.addAll(fetched);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Failed to fetch shards to seal", e);
        }
        return shards;
    }

    /**
     * Discover all shards that need to be sealed at FINISH_JOIN or FINISH_REPLACE.
     */
    private static Set<ShardMetadata> discoverShardsAtFinishJoinOrReplace(
        ClusterMetadata metadata, long finishEpoch, PlacementDeltas finishDelta, @Nullable NodeId deadNode)
    {
        Set<ShardMetadata> shards = new HashSet<>();
        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
        {
            if (!ksm.params.replicationType.isTracked())
                continue;

            DataPlacement placement = metadata.placements.get(ksm.params.replication);
            finishDelta.get(ksm.params.replication).writes.removals.flattenValues().forEach(removed -> {
                Set<Integer> nodeIDs = new HashSet<>();
                for (InetAddressAndPort ep : placement.writes.forRange(removed.range()).endpoints())
                    nodeIDs.add(metadata.directory.peerId(ep).id());
                nodeIDs.add(deadNode != null ? deadNode.id() : metadata.directory.peerId(removed.endpoint()).id());
                Participants participants = new Participants(nodeIDs);

                MutationTrackingService.instance().forEachShardInKeyspace(ksm.name, shard -> {
                    if (shard.sinceEpoch < finishEpoch
                        && shard.range.equals(removed.range())
                        && shard.participants.equals(participants)
                        && !shard.isSealed())
                        shards.add(new ShardMetadata(shard.keyspace, shard.sinceEpoch, shard.range, shard.participants));
                });
            });
        }
        return shards;
    }

    /*
     * UNBOOTSTRAP and REMOVENODE sequences illustrated (same sequence as far as sharding is concerned).
     * Stages in which write placements remain unchanged, and only read placements change, are skipped
     * here as irrelevant to sharding / sealing.
     *
     * There is a tracked keyspace with RF=3.
     * A node n40 with token=40 is removed in the middle of the ring.
     *
     * 1. Initial state at e0:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | ACTIVE
     *      (40,50] | {n50,n60,n70}     | e0 | ACTIVE
     *
     * 2. START_LEAVE at e1:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | ACTIVE
     *    + (10,20] | {n20,n30,n40,n50} | e1 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | ACTIVE
     *    + (20,30] | {n30,n40,n50,n60} | e1 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | ACTIVE
     *    + (30,40] | {n40,n50,n60,n70} | e1 | ACTIVE
     *      (40,50] | {n50,n60,n70}     | e0 | ACTIVE
     *
     * 3. MID_LEAVE at e2 [*]:
     *
     *    S (10,20] | {n20,n30,n40}     | e0 | SEALED
     *      (10,20] | {n20,n30,n40,n50} | e1 | ACTIVE
     *    S (20,30] | {n30,n40,n50}     | e0 | SEALED
     *      (20,30] | {n30,n40,n50,n60} | e1 | ACTIVE
     *    S (30,40] | {n40,n50,n60}     | e0 | SEALED
     *      (30,40] | {n40,n50,n60,n70} | e1 | ACTIVE
     *      (40,50] | {n50,n60,n70}     | e0 | ACTIVE
     *
     * 4. FINISH_LEAVE at e3:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | SEALED
     *      (10,20] | {n20,n30,n40,n50} | e1 | ACTIVE
     *    + (10,20] | {n20,n30,n50}     | e3 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | SEALED
     *      (20,30] | {n30,n40,n50,n60} | e1 | ACTIVE
     *    + (20,30] | {n30,n50,n60}     | e3 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | SEALED
     *      (30,40] | {n40,n50,n60,n70} | e1 | ACTIVE
     *      (40,50] | {n50,n60,n70}     | e0 | ACTIVE
     *    + (30,50] | {n50,n60,n70}     | e3 | ACTIVE
     *
     * 5. UNLOCK_SEQUENCE:
     *
     *      (10,20] | {n20,n30,n40}     | e0 | SEALED
     *    S (10,20] | {n20,n30,n40,n50} | e1 | SEALED
     *      (10,20] | {n20,n30,n50}     | e3 | ACTIVE
     *      (20,30] | {n30,n40,n50}     | e0 | SEALED
     *    S (20,30] | {n30,n40,n50,n60} | e1 | SEALED
     *      (20,30] | {n30,n50,n60}     | e3 | ACTIVE
     *      (30,40] | {n40,n50,n60}     | e0 | SEALED
     *    S (30,40] | {n40,n50,n60,n70} | e1 | SEALED
     *    S (40,50] | {n50,n60,n70}     | e0 | SEALED
     *      (30,50] | {n50,n60,n70}     | e3 | ACTIVE
     *
     * [*] At this stage, after sealing the shards, we stream the sstables.
     * Non-repaired (not fully reconciled) SSTables must be split into
     * fully reconciled part and non-reconciled part. When streaming the
     * former, we will strip all the log offsets from their metadata.
     * Otherwise, the new node won't be able to compact them or mark them
     * as reconciled/repaired - it is rightfully unaware of the sealed shards.
     * If we don't split them into two parts, then we risk being unable to
     * rebuild and filter any of the streamed SSTables on the receiving node.
     */

    /**
     * Seal the shards obsoleted by START_LEAVE before streaming, for both UNBOOTSTRAP and REMOVENODE.
     */
    public static void sealShardsAtMidLeave(ClusterMetadata metadata, PlacementDeltas startDelta, NodeId leavingOrDeparted, LeaveStreams.Kind kind)
    {
        InetAddressAndPort leavingOrDepartedEndpoint = metadata.directory.endpoint(leavingOrDeparted);

        List<Future<Set<ShardMetadata>>> futures = new ArrayList<>();
        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
        {
            if (!ksm.params.replicationType.isTracked())
                continue;

            DataPlacement placement = metadata.placements.get(ksm.params.replication);
            startDelta.get(ksm.params.replication).writes.additions.flattenValues().forEach(added -> {
                Range<Token> range = added.range();
                long beforeEpoch = placement.writes.forRange(range).lastModified().getEpoch();
                Set<InetAddressAndPort> survivors = new HashSet<>(placement.writes.forRange(range).endpoints());
                survivors.remove(leavingOrDepartedEndpoint);
                futures.add(FetchShards.fetch(ksm.name, beforeEpoch, range, survivors));
            });
        }

        Set<ShardMetadata> shards = new HashSet<>();
        try
        {
            for (Set<ShardMetadata> fetched : FutureCombiner.allOf(futures).get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS))
                for (ShardMetadata shard : fetched)
                    if (shard.participants.contains(leavingOrDeparted.id()))
                        shards.add(shard);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Failed to fetch shards to seal for leave", e);
        }

        seal(shards, kind == LeaveStreams.Kind.UNBOOTSTRAP ? null : leavingOrDeparted);
    }

    /**
     * Seal the shards obsoleted by FINISH_LEAVE, for both UNBOOTSTRAP and REMOVENODE: the departed node's
     * over-replicated generations created during START_LEAVE, plus the merge-half folded into its range.
     */
    public static void sealShardsAtFinishLeave(ClusterMetadata metadata, long finishEpoch, PlacementDeltas finishDelta, NodeId leavingOrDeparted, LeaveStreams.Kind kind)
    {
        InetAddressAndPort leavingOrDepartedEndpoint = metadata.directory.endpoint(leavingOrDeparted);

        List<Future<Set<ShardMetadata>>> futures = new ArrayList<>();
        for (KeyspaceMetadata ksm : metadata.schema.getKeyspaces())
        {
            if (!ksm.params.replicationType.isTracked())
                continue;

            DataPlacement placement = metadata.placements.get(ksm.params.replication);
            RangesAtEndpoint removals = finishDelta.get(ksm.params.replication).writes.removals.get(leavingOrDepartedEndpoint);
            for (Replica removedReplica : removals)
            {
                // FINISH_LEAVE merges the leavingOrDeparted node's (pre-merge) range with an adjacent one. Query the *merged*
                // range (not the pre-merge range) so discovery covers both the leavingOrDeparted node's own obsoleted
                // generations and the merge-half - the adjacent pre-merge range folded into the merged range, whose
                // shard never listed the leavingOrDeparted node. matchRange() yields the containing merged group + its
                // current (survivor) write endpoints.
                VersionedEndpoints.ForRange group = placement.writes.matchRange(removedReplica.range());
                Range<Token> mergedRange = group.range();
                Set<InetAddressAndPort> survivors = new HashSet<>(group.endpoints());
                survivors.remove(leavingOrDepartedEndpoint);
                futures.add(FetchShards.fetch(ksm.name, finishEpoch, mergedRange, survivors));
            }
        }

        Set<ShardMetadata> shards = new HashSet<>();
        try
        {
            for (Set<ShardMetadata> fetched : FutureCombiner.allOf(futures).get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS))
                shards.addAll(fetched);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Failed to fetch shards to seal for leave finish", e);
        }

        seal(shards, kind == LeaveStreams.Kind.UNBOOTSTRAP ? null : leavingOrDeparted);
    }

    /*
     * Sealing logic.
     */

    private static void seal(Set<ShardMetadata> shards)
    {
        seal(shards, null);
    }

    private static void seal(Set<ShardMetadata> shards, @Nullable NodeId withoutNode)
    {
        initiate(shards, withoutNode);  // ACTIVE -> SEALING for each Shard
        drain(shards, withoutNode);     // drain in-flight local writes
        reconcile(shards, withoutNode); // wait for logs to reconcile
        complete(shards, withoutNode);  // SEALING-> SEALED for each Shard + journal flush
    }

    private static void initiate(Set<ShardMetadata> shards, @Nullable NodeId withoutNode)
    {
        List<Future<Void>> futures = new ArrayList<>(shards.size());
        for (ShardMetadata shard : shards)
            futures.add(initiate(shard, withoutNode));
        try
        {
            FutureCombiner.allOf(futures).get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Failed to initiate sealing", e);
        }
    }

    /**
     * Fence allocation on every participant of the obsoleted shard (ACTIVE -> SEALING) by marking the shard
     * SEALING on each.
     * TODO (expected): this assumes every participant has already enacted the topology change that obsoleted the shard (so
     *   it no longer allocates new ids on it). The ProgressBarrier gating bootstrap()/MID_JOIN only waits for
     *   EACH_QUORUM of the affected replicas, NOT all live replicas, so a live-but-lagging participant could still
     *   be on the old shard. Bringing each participant up to the seal epoch must be done per-replica (not
     *   per-shard) and will be added later.
     */
    private static AsyncPromise<Void> initiate(ShardMetadata shard, @Nullable NodeId withoutNode)
    {
        return InitSealing.initiate(shard.keyspace, shard.sinceEpoch, shard.range, toEndpoints(shard.participants, withoutNode));
    }

    private static void drain(Set<ShardMetadata> shards, @Nullable NodeId withoutNode)
    {
        long deadlineNanos = nanoTime() + DatabaseDescriptor.getRpcTimeout(NANOSECONDS);
        Set<ShardMetadata> pending = new HashSet<>(shards);
        while (true)
        {
            // non-blocking poll: each participant reports its drain status immediately; retry until all drained
            pending.removeIf(shard -> drain(shard, withoutNode));
            if (pending.isEmpty())
                return;
            if (nanoTime() >= deadlineNanos)
                throw new RuntimeException("Timed out draining shards; still pending: " + pending);
            LockSupport.parkNanos(SECONDS.toNanos(1));
        }
    }

    /**
     * Poll every participant of the SEALING shard once for drain status; true iff all report that no mutation
     * id allocated before the SEALING fence is still applying locally.
     */
    private static boolean drain(ShardMetadata shard, @Nullable NodeId withoutNode)
    {
        try
        {
            return Drain.poll(shard.keyspace, shard.sinceEpoch, shard.range, toEndpoints(shard.participants, withoutNode))
                        .get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(format("Failed to poll drain for shard %s", shard), e);
        }
    }

    /**
     * For each of the shards, capture the witnessed union then poll each participant until it has
     * caught up and witnessed the entire offset union itself.
     */
    private static void reconcile(Set<ShardMetadata> shards, @Nullable NodeId withoutNode)
    {
        for (ShardMetadata shard : shards)
            reconcile(shard, withoutNode);
    }

    private static void reconcile(ShardMetadata shard, @Nullable NodeId withoutNode)
    {
        // capture shard's witnessed offsets once
        Log2OffsetsMap.Mutable offsets = captureWitnessedOffsets(shard, withoutNode);
        // poll every participant until they've each witnessed the union of offsets
        for (InetAddressAndPort endpoint : toEndpoints(shard.participants, withoutNode))
            pollUntilWitnesses(shard, offsets, endpoint);
    }

    private static Log2OffsetsMap.Mutable captureWitnessedOffsets(ShardMetadata shard, @Nullable NodeId withoutNode)
    {
        List<InetAddressAndPort> endpoints = toEndpoints(shard.participants, withoutNode);
        try
        {
            return ReconcileCapture.capture(shard.keyspace, shard.sinceEpoch, shard.range, endpoints)
                                   .get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(format("Failed to capture witnessed offsets for shard %s", shard), e);
        }
    }

    private static void pollUntilWitnesses(ShardMetadata shard, Log2OffsetsMap.Mutable offsets, InetAddressAndPort endpoint)
    {
        // TODO (expected): use a longer timeout
        long deadlineNanos = nanoTime() + DatabaseDescriptor.getRpcTimeout(NANOSECONDS);

        while (true)
        {
            if (nanoTime() >= deadlineNanos)
                throw new RuntimeException("Timed out reconciling shard: " + shard);
            try
            {
                boolean witnessed =
                    ReconcilePoll.poll(shard.keyspace, shard.sinceEpoch, shard.range, offsets, endpoint)
                                 .get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS);
                if (witnessed)
                    return;
            }
            catch (InterruptedException | ExecutionException | TimeoutException e)
            {
                throw new RuntimeException(format("Failed to poll reconcile for shard %s", shard), e);
            }

            LockSupport.parkNanos(SECONDS.toNanos(1));
        }
    }

    private static void complete(Set<ShardMetadata> shards, @Nullable NodeId withoutNode)
    {
        List<Future<Void>> futures = new ArrayList<>(shards.size());
        for (ShardMetadata shard : shards)
            futures.add(complete(shard, withoutNode));

        try
        {
            FutureCombiner.allOf(futures).get(DatabaseDescriptor.getRpcTimeout(MILLISECONDS), MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException("Failed to complete sealing", e);
        }
    }

    /**
     * Promote every participant of the (drained and reconciled) shard from SEALING to SEALED.
     */
    private static AsyncPromise<Void> complete(ShardMetadata shard, @Nullable NodeId withoutNode)
    {
        return CompleteSealing.complete(shard.keyspace, shard.sinceEpoch, shard.range, toEndpoints(shard.participants, withoutNode));
    }

    /**
     * A bootstrapping node knows nothing about the shards that currently exist,
     * so it must collect the list of shards from the existing replicas before
     * it can proceed with the rest of the sealing steps.
     */
    public static final class FetchShards
    {
        public static final class Request
        {
            final String keyspace;
            final long beforeEpoch;
            final Range<Token> range;

            Request(String keyspace, long beforeEpoch, Range<Token> range)
            {
                this.keyspace = keyspace;
                this.beforeEpoch = beforeEpoch;
                this.range = range;
            }
        }

        public static final class Response
        {
            final Set<ShardMetadata> shards;

            Response(Set<ShardMetadata> shards)
            {
                this.shards = shards;
            }
        }

        /**
         * Query all replicas in a group in parallel for the shards over {@code ranges} obsoleted by an
         * in-flight topology change (those with {@code sinceEpoch < beforeEpoch}). Resembles
         * {@link ShardMetadataRequest#queryPeers}, but waits for every (live) replica to respond and
         * merges their deduplicated shards rather than taking the first response.
         */
        public static AsyncPromise<Set<ShardMetadata>> fetch(
            String keyspace, long beforeEpoch, Range<Token> range, Set<InetAddressAndPort> endpoints)
        {
            Preconditions.checkArgument(!endpoints.isEmpty());
            AsyncPromise<Set<ShardMetadata>> promise = new AsyncPromise<>();
            Set<ShardMetadata> merged = ConcurrentHashMap.newKeySet();

            RequestCallback<Response> callback = new RequestCallback<>()
            {
                private final AtomicInteger remaining = new AtomicInteger(endpoints.size());

                @Override
                public void onResponse(Message<Response> msg)
                {
                    merged.addAll(msg.payload.shards);
                    if (remaining.decrementAndGet() == 0)
                        promise.trySuccess(merged);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    // happy path assumes all replicas are up; fail the whole fetch if any can't respond
                    promise.tryFailure(new RuntimeException(format("Failed to fetch shards to seal from %s for keyspace %s: %s", from, keyspace, failure)));
                }

                @Override
                public boolean invokeOnFailure()
                {
                    return true;
                }
            };

            Message<Request> message = Message.out(Verb.MT_FETCH_SHARDS_REQ, new Request(keyspace, beforeEpoch, range));
            for (InetAddressAndPort peer : endpoints)
                MessagingService.instance().sendWithCallback(message, peer, callback);

            return promise;
        }

        public static final IVerbHandler<Request> verbHandler = message ->
        {
            MutationTrackingService.ensureEnabled();
            Request request = message.payload;
            Set<ShardMetadata> shards = new HashSet<>();
            MutationTrackingService.instance().forEachShardInKeyspace(request.keyspace, shard -> {
                if (shard.sinceEpoch < request.beforeEpoch
                    && (request.range.contains(shard.range) || shard.range.contains(request.range))
                    && !shard.isSealed())
                    shards.add(new ShardMetadata(shard.keyspace, shard.sinceEpoch, shard.range, shard.participants));
            });
            MessagingService.instance().send(message.responseWith(new Response(shards)), message.from());
        };

        public static final VersionedSerializer<Request> requestSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Request r, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUTF(r.keyspace);
                out.writeLong(r.beforeEpoch);
                AbstractBounds.tokenSerializer.serialize(r.range, out, version.messagingVersion());
            }

            @Override
            public Request deserialize(DataInputPlus in, Version version) throws IOException
            {
                String keyspace = in.readUTF();
                long beforeEpoch = in.readLong();
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
                return new Request(keyspace, beforeEpoch, range);
            }

            @Override
            public long serializedSize(Request r, Version version)
            {
                long size = TypeSizes.sizeof(r.keyspace);
                size += TypeSizes.sizeof(r.beforeEpoch);
                size += AbstractBounds.tokenSerializer.serializedSize(r.range, version.messagingVersion());
                return size;
            }
        };

        public static final VersionedSerializer<Response> responseSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Response r, DataOutputPlus out, Version version) throws IOException
            {
                CollectionSerializers.serializeCollection(r.shards, out, version, ShardMetadata.serializer);
            }

            @Override
            public Response deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new Response(CollectionSerializers.deserializeSet(in, version, ShardMetadata.serializer));
            }

            @Override
            public long serializedSize(Response r, Version version)
            {
                return CollectionSerializers.serializedCollectionSize(r.shards, version, ShardMetadata.serializer);
            }
        };
    }

    public static final class InitSealing
    {
        public static final class Request
        {
            final String keyspace;
            final long sinceEpoch;
            final Range<Token> range;

            Request(String keyspace, long sinceEpoch, Range<Token> range)
            {
                this.keyspace = keyspace;
                this.sinceEpoch = sinceEpoch;
                this.range = range;
            }
        }

        // cannot use NoPayload, because its serializer cannot be wrapped inside mtEmbedded()
        public static final class Response
        {
            private static final Response instance = new Response();
        }

        /**
         * Tell every participant of an obsoleted shard to fence id allocation (mark it SEALING)
         */
        public static AsyncPromise<Void> initiate(
            String keyspace, long sinceEpoch, Range<Token> range, List<InetAddressAndPort> endpoints)
        {
            AsyncPromise<Void> promise = new AsyncPromise<>();

            RequestCallback<Response> callback = new RequestCallback<>()
            {
                private final AtomicInteger remaining = new AtomicInteger(endpoints.size());

                @Override
                public void onResponse(Message<Response> msg)
                {
                    if (remaining.decrementAndGet() == 0)
                        promise.trySuccess(null);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    promise.tryFailure(new RuntimeException(format("Failed to initiate sealing on %s for keyspace %s: %s", from, keyspace, failure)));
                }

                @Override
                public boolean invokeOnFailure()
                {
                    return true;
                }
            };

            Message<Request> message = Message.out(Verb.MT_INIT_SEALING_REQ, new Request(keyspace, sinceEpoch, range));
            for (InetAddressAndPort peer : endpoints)
                MessagingService.instance().sendWithCallback(message, peer, callback);

            return promise;
        }

        public static final IVerbHandler<Request> verbHandler = message -> {
            MutationTrackingService.ensureEnabled();
            Request request = message.payload;
            MutationTrackingService.instance().markShardSealing(request.keyspace, request.sinceEpoch, request.range);
            MessagingService.instance().send(message.responseWith(Response.instance), message.from());
        };

        public static final VersionedSerializer<Request> requestSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Request r, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUTF(r.keyspace);
                out.writeLong(r.sinceEpoch);
                AbstractBounds.tokenSerializer.serialize(r.range, out, version.messagingVersion());
            }

            @Override
            public Request deserialize(DataInputPlus in, Version version) throws IOException
            {
                String keyspace = in.readUTF();
                long sinceEpoch = in.readLong();
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
                return new Request(keyspace, sinceEpoch, range);
            }

            @Override
            public long serializedSize(Request r, Version version)
            {
                long size = TypeSizes.sizeof(r.keyspace);
                size += TypeSizes.sizeof(r.sinceEpoch);
                size += AbstractBounds.tokenSerializer.serializedSize(r.range, version.messagingVersion());
                return size;
            }
        };

        public static final UnversionedSerializer<Response> responseSerializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Response r, DataOutputPlus out)
            {
            }

            @Override
            public Response deserialize(DataInputPlus in)
            {
                return Response.instance;
            }

            @Override
            public long serializedSize(Response r)
            {
                return 0;
            }
        };
    }

    public static final class Drain
    {
        public static final class Request
        {
            final String keyspace;
            final long sinceEpoch;
            final Range<Token> range;

            Request(String keyspace, long sinceEpoch, Range<Token> range)
            {
                this.keyspace = keyspace;
                this.sinceEpoch = sinceEpoch;
                this.range = range;
            }
        }

        public static final class Response
        {
            final boolean drained;

            Response(boolean drained)
            {
                this.drained = drained;
            }
        }

        /**
         * Poll every participant of a SEALING shard for whether it has drained its in-flight local
         * writes. Succeeds with {@code true} iff every participant reports drained.
         */
        public static AsyncPromise<Boolean> poll(
            String keyspace, long sinceEpoch, Range<Token> range, List<InetAddressAndPort> endpoints)
        {
            AsyncPromise<Boolean> promise = new AsyncPromise<>();

            RequestCallback<Response> callback = new RequestCallback<>()
            {
                private final AtomicInteger remaining = new AtomicInteger(endpoints.size());
                private volatile boolean allDrained = true;

                @Override
                public void onResponse(Message<Response> msg)
                {
                    if (!msg.payload.drained)
                        allDrained = false;
                    if (remaining.decrementAndGet() == 0)
                        promise.trySuccess(allDrained);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    promise.tryFailure(new RuntimeException(format("Failed to poll drain on %s for keyspace %s: %s", from, keyspace, failure)));
                }

                @Override
                public boolean invokeOnFailure()
                {
                    return true;
                }
            };

            Message<Request> message = Message.out(Verb.MT_DRAIN_REQ, new Request(keyspace, sinceEpoch, range));
            for (InetAddressAndPort peer : endpoints)
                MessagingService.instance().sendWithCallback(message, peer, callback);

            return promise;
        }

        public static final IVerbHandler<Request> verbHandler = message -> {
            MutationTrackingService.ensureEnabled();
            Request request = message.payload;
            boolean drained = MutationTrackingService.instance().isShardDrained(request.keyspace, request.sinceEpoch, request.range);
            MessagingService.instance().send(message.responseWith(new Response(drained)), message.from());
        };

        public static final VersionedSerializer<Request> requestSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Request r, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUTF(r.keyspace);
                out.writeLong(r.sinceEpoch);
                AbstractBounds.tokenSerializer.serialize(r.range, out, version.messagingVersion());
            }

            @Override
            public Request deserialize(DataInputPlus in, Version version) throws IOException
            {
                String keyspace = in.readUTF();
                long sinceEpoch = in.readLong();
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
                return new Request(keyspace, sinceEpoch, range);
            }

            @Override
            public long serializedSize(Request r, Version version)
            {
                long size = TypeSizes.sizeof(r.keyspace);
                size += TypeSizes.sizeof(r.sinceEpoch);
                size += AbstractBounds.tokenSerializer.serializedSize(r.range, version.messagingVersion());
                return size;
            }
        };

        public static final UnversionedSerializer<Response> responseSerializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Response r, DataOutputPlus out) throws IOException
            {
                out.writeBoolean(r.drained);
            }

            @Override
            public Response deserialize(DataInputPlus in) throws IOException
            {
                return new Response(in.readBoolean());
            }

            @Override
            public long serializedSize(Response r)
            {
                return TypeSizes.sizeof(r.drained);
            }
        };
    }

    public static final class ReconcileCapture
    {
        public static final class Request
        {
            final String keyspace;
            final long sinceEpoch;
            final Range<Token> range;

            Request(String keyspace, long sinceEpoch, Range<Token> range)
            {
                this.keyspace = keyspace;
                this.sinceEpoch = sinceEpoch;
                this.range = range;
            }
        }

        public static final class Response
        {
            final Log2OffsetsMap<?> witnessed;

            Response(Log2OffsetsMap<?> witnessed)
            {
                this.witnessed = witnessed;
            }
        }

        public static AsyncPromise<Log2OffsetsMap.Mutable> capture(
            String keyspace, long sinceEpoch, Range<Token> range, List<InetAddressAndPort> endpoints)
        {
            AsyncPromise<Log2OffsetsMap.Mutable> promise = new AsyncPromise<>();
            Log2OffsetsMap.Mutable union = new Log2OffsetsMap.Mutable();

            RequestCallback<Response> callback = new RequestCallback<>()
            {
                private final AtomicInteger remaining = new AtomicInteger(endpoints.size());

                @Override
                public void onResponse(Message<Response> msg)
                {
                    synchronized (union)
                    {
                        union.addAll(msg.payload.witnessed);
                    }
                    if (remaining.decrementAndGet() == 0)
                        promise.trySuccess(union);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    promise.tryFailure(new RuntimeException(format("Failed to capture witnessed offsets on %s for keyspace %s: %s", from, keyspace, failure)));
                }

                @Override
                public boolean invokeOnFailure()
                {
                    return true;
                }
            };

            Message<Request> message = Message.out(Verb.MT_RECONCILE_CAPTURE_REQ, new Request(keyspace, sinceEpoch, range));
            for (InetAddressAndPort peer : endpoints)
                MessagingService.instance().sendWithCallback(message, peer, callback);

            return promise;
        }

        public static final IVerbHandler<Request> verbHandler = message -> {
            MutationTrackingService.ensureEnabled();
            Request request = message.payload;
            Log2OffsetsMap<?> witnessed =
                MutationTrackingService.instance()
                                       .collectLocallyWitnessedOffsets(request.keyspace, request.sinceEpoch, request.range);
            MessagingService.instance().send(message.responseWith(new Response(witnessed)), message.from());
        };

        public static final VersionedSerializer<Request> requestSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Request r, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUTF(r.keyspace);
                out.writeLong(r.sinceEpoch);
                AbstractBounds.tokenSerializer.serialize(r.range, out, version.messagingVersion());
            }

            @Override
            public Request deserialize(DataInputPlus in, Version version) throws IOException
            {
                String keyspace = in.readUTF();
                long sinceEpoch = in.readLong();
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
                return new Request(keyspace, sinceEpoch, range);
            }

            @Override
            public long serializedSize(Request r, Version version)
            {
                long size = TypeSizes.sizeof(r.keyspace);
                size += TypeSizes.sizeof(r.sinceEpoch);
                size += AbstractBounds.tokenSerializer.serializedSize(r.range, version.messagingVersion());
                return size;
            }
        };

        public static final UnversionedSerializer<Response> responseSerializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Response r, DataOutputPlus out) throws IOException
            {
                Log2OffsetsMap.serializer.serialize(r.witnessed, out);
            }

            @Override
            public Response deserialize(DataInputPlus in) throws IOException
            {
                return new Response(Log2OffsetsMap.serializer.deserialize(in));
            }

            @Override
            public long serializedSize(Response r)
            {
                return Log2OffsetsMap.serializer.serializedSize(r.witnessed);
            }
        };
    }

    public static final class ReconcilePoll
    {
        public static final class Request
        {
            final String keyspace;
            final long sinceEpoch;
            final Range<Token> range;
            final Log2OffsetsMap<?> offsets;

            Request(String keyspace, long sinceEpoch, Range<Token> range, Log2OffsetsMap<?> offsets)
            {
                this.keyspace = keyspace;
                this.sinceEpoch = sinceEpoch;
                this.range = range;
                this.offsets = offsets;
            }
        }

        public static final class Response
        {
            final boolean witnessed;

            Response(boolean witnessed)
            {
                this.witnessed = witnessed;
            }
        }

        public static AsyncPromise<Boolean> poll(
            String keyspace, long sinceEpoch, Range<Token> range, Log2OffsetsMap.Mutable offsets, InetAddressAndPort endpoint)
        {
            AsyncPromise<Boolean> promise = new AsyncPromise<>();

            RequestCallback<Response> callback = new RequestCallback<>()
            {
                @Override
                public void onResponse(Message<Response> msg)
                {
                    promise.trySuccess(msg.payload.witnessed);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    promise.tryFailure(new RuntimeException(format("Failed to reconcile poll %s for keyspace %s: %s", from, keyspace, failure)));
                }

                @Override
                public boolean invokeOnFailure()
                {
                    return true;
                }
            };

            MessagingService.instance()
                            .sendWithCallback(Message.out(Verb.MT_RECONCILE_POLL_REQ, new Request(keyspace, sinceEpoch, range, offsets)),
                                              endpoint, callback);

            return promise;
        }

        public static final IVerbHandler<Request> verbHandler = message -> {
            MutationTrackingService.ensureEnabled();
            Request request = message.payload;
            boolean witnessed =
                MutationTrackingService.instance().hasWitnessed(request.keyspace, request.sinceEpoch, request.range, request.offsets);
            MessagingService.instance().send(message.responseWith(new Response(witnessed)), message.from());
        };

        public static final VersionedSerializer<Request> requestSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Request r, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUTF(r.keyspace);
                out.writeLong(r.sinceEpoch);
                AbstractBounds.tokenSerializer.serialize(r.range, out, version.messagingVersion());
                Log2OffsetsMap.serializer.serialize(r.offsets, out);
            }

            @Override
            public Request deserialize(DataInputPlus in, Version version) throws IOException
            {
                String keyspace = in.readUTF();
                long sinceEpoch = in.readLong();
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
                Log2OffsetsMap.Immutable offsets = Log2OffsetsMap.serializer.deserialize(in);
                return new Request(keyspace, sinceEpoch, range, offsets);
            }

            @Override
            public long serializedSize(Request r, Version version)
            {
                long size = TypeSizes.sizeof(r.keyspace);
                size += TypeSizes.sizeof(r.sinceEpoch);
                size += AbstractBounds.tokenSerializer.serializedSize(r.range, version.messagingVersion());
                size += Log2OffsetsMap.serializer.serializedSize(r.offsets);
                return size;
            }
        };

        public static final UnversionedSerializer<Response> responseSerializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Response r, DataOutputPlus out) throws IOException
            {
                out.writeBoolean(r.witnessed);
            }

            @Override
            public Response deserialize(DataInputPlus in) throws IOException
            {
                return new Response(in.readBoolean());
            }

            @Override
            public long serializedSize(Response r)
            {
                return TypeSizes.sizeof(r.witnessed);
            }
        };
    }

    public static final class CompleteSealing
    {
        public static final class Request
        {
            final String keyspace;
            final long sinceEpoch;
            final Range<Token> range;

            Request(String keyspace, long sinceEpoch, Range<Token> range)
            {
                this.keyspace = keyspace;
                this.sinceEpoch = sinceEpoch;
                this.range = range;
            }
        }

        // cannot use NoPayload, because its serializer cannot be wrapped inside mtEmbedded()
        public static final class Response
        {
            private static final Response instance = new Response();
        }

        /**
         * Tell every participant of a sealing shard to promote it from SEALING to SEALED.
         */
        public static AsyncPromise<Void> complete(
            String keyspace, long sinceEpoch, Range<Token> range, List<InetAddressAndPort> endpoints)
        {
            AsyncPromise<Void> promise = new AsyncPromise<>();

            RequestCallback<Response> callback = new RequestCallback<>()
            {
                private final AtomicInteger remaining = new AtomicInteger(endpoints.size());

                @Override
                public void onResponse(Message<Response> msg)
                {
                    if (remaining.decrementAndGet() == 0)
                        promise.trySuccess(null);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    promise.tryFailure(new RuntimeException(format("Failed to complete sealing on %s for keyspace %s: %s", from, keyspace, failure)));
                }

                @Override
                public boolean invokeOnFailure()
                {
                    return true;
                }
            };

            Message<Request> message = Message.out(Verb.MT_COMPLETE_SEALING_REQ, new Request(keyspace, sinceEpoch, range));
            for (InetAddressAndPort peer : endpoints)
                MessagingService.instance().sendWithCallback(message, peer, callback);

            return promise;
        }

        public static final IVerbHandler<Request> verbHandler = message -> {
            MutationTrackingService.ensureEnabled();
            Request request = message.payload;

            // flush every table in the keyspace so a just-sealed shard's mutations are all made durable as SSTables
            List<Future<?>> flushes = new ArrayList<>();
            for (ColumnFamilyStore cfs : Keyspace.open(request.keyspace).getColumnFamilyStores())
                flushes.add(cfs.forceFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED));
            FBUtilities.waitOnFutures(flushes);

            MutationTrackingService.instance().markShardSealed(request.keyspace, request.sinceEpoch, request.range);
            MessagingService.instance().send(message.responseWith(Response.instance), message.from());
        };

        public static final VersionedSerializer<Request> requestSerializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(Request r, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUTF(r.keyspace);
                out.writeLong(r.sinceEpoch);
                AbstractBounds.tokenSerializer.serialize(r.range, out, version.messagingVersion());
            }

            @Override
            public Request deserialize(DataInputPlus in, Version version) throws IOException
            {
                String keyspace = in.readUTF();
                long sinceEpoch = in.readLong();
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
                return new Request(keyspace, sinceEpoch, range);
            }

            @Override
            public long serializedSize(Request r, Version version)
            {
                long size = TypeSizes.sizeof(r.keyspace);
                size += TypeSizes.sizeof(r.sinceEpoch);
                size += AbstractBounds.tokenSerializer.serializedSize(r.range, version.messagingVersion());
                return size;
            }
        };

        public static final UnversionedSerializer<Response> responseSerializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Response r, DataOutputPlus out)
            {
            }

            @Override
            public Response deserialize(DataInputPlus in)
            {
                return Response.instance;
            }

            @Override
            public long serializedSize(Response r)
            {
                return 0;
            }
        };
    }

    private static List<InetAddressAndPort> toEndpoints(Participants participants, @Nullable NodeId withoutNode)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        List<InetAddressAndPort> endpoints = new ArrayList<>(participants.size());
        for (int i = 0, size = participants.size(); i < size; i++)
        {
            NodeId nodeId = new NodeId(participants.get(i));
            if (!nodeId.equals(withoutNode))
                endpoints.add(metadata.directory.endpoint(nodeId));
        }
        return endpoints;
    }
}
