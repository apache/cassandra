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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.messages.MutationTrackingSyncRequest;
import org.apache.cassandra.repair.messages.MutationTrackingSyncResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static com.google.common.base.Preconditions.checkState;

public class MutationTrackingSyncCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingSyncCoordinator.class);

    private final SharedContext ctx;
    private final RepairJobDesc desc;
    private final String keyspace;
    private final Range<Token> range;
    private final Set<InetAddressAndPort> participants;
    private final AsyncPromise<Void> completionFuture = new AsyncPromise<>();

    // Per-shard state: tracks what each node has reported for that shard.
    private ImmutableMap<Range<Token>, ShardSyncState> shardStates;

    // Host IDs of participants for scoped offset collection/completion.
    // Null means all shard participants (no filtering).
    private final Set<Integer> liveHostIds;

    private final AtomicBoolean started = new AtomicBoolean(false);

    // Remote participants we are waiting for sync responses from. Completion is
    // not possible until all responses have been received, since remote nodes may
    // report targets that the local node doesn't know about yet.
    private final Set<InetAddressAndPort> pendingSyncResponses = ConcurrentHashMap.newKeySet();

    /**
     * @param ctx shared context
     * @param desc repair job descriptor
     * @param participants the set of remote endpoints that should participate in this sync,
     *                     as determined by the repair options (force, specific hosts).
     *                     Only these endpoints will receive sync requests. If null,
     *                     all remote replicas for overlapping shards will participate.
     * @param metadata the snapshotted cluster metadata used to resolve endpoint-to-host-ID mappings
     */
    public MutationTrackingSyncCoordinator(SharedContext ctx, RepairJobDesc desc, Set<InetAddressAndPort> participants, ClusterMetadata metadata)
    {
        this.ctx = ctx;
        this.desc = desc;
        this.keyspace = desc.keyspace;
        this.range = Iterables.getOnlyElement(desc.ranges);
        this.participants = participants;

        // Convert participant endpoints to host IDs for scoped completion checks.
        // If participants is null (no filtering), all shard participants are live.
        if (participants != null)
        {
            ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
            for (InetAddressAndPort ep : participants)
            {
                builder.add(metadata.directory.peerId(ep).id());
            }
            // Always include the local node
            builder.add(metadata.directory.peerId(ctx.broadcastAddressAndPort()).id());
            liveHostIds = builder.build();
        }
        else
        {
            liveHostIds = null;
        }
    }

    public void start()
    {
        if (!started.compareAndSet(false, true))
            throw new IllegalStateException("Sync coordinator already started");

        List<Shard> overlappingShards = new ArrayList<>();
        MutationTrackingService.instance().forEachShardInKeyspace(keyspace, shard -> {
            if (shard.range.intersects(range))
                overlappingShards.add(shard);
        });

        checkState(!overlappingShards.isEmpty(), "No intersecting shards found for keyspace {} range {}", keyspace, range);

        ImmutableMap.Builder<Range<Token>, ShardSyncState> builder = ImmutableMap.builder();
        for (Shard shard : overlappingShards)
        {
            ShardSyncState state = new ShardSyncState(shard, liveHostIds);
            builder.put(shard.range, state);
        }
        shardStates = builder.build();

        // Register to receive offset updates
        MutationTrackingService.instance().registerSyncCoordinator(this);

        // Capture local targets
        captureTargets();

        logger.info("Sync coordinator started for keyspace {} range {}, tracking {} shards",
                   keyspace, range, overlappingShards.size());

        // Send sync requests to all remote participants
        sendSyncRequests();

        // Check if already complete (e.g. single node, no targets)
        checkIfReadyToComplete();
    }

    private void complete()
    {
        if (completionFuture.trySuccess(null))
            MutationTrackingService.instance().unregisterSyncCoordinator(this);
    }

    private void sendSyncRequests()
    {
        MutationTrackingSyncRequest request = new MutationTrackingSyncRequest(desc, liveHostIds);
        // Collect remote replicas, filtering to only allowed participants if specified.
        // This respects --force (which excludes dead nodes) and --hosts (which
        // restricts to specific nodes).
        Set<InetAddressAndPort> remoteParticipants = ConcurrentHashMap.newKeySet();
        for (ShardSyncState state : shardStates.values())
            remoteParticipants.addAll(state.shard.remoteReplicas());

        if (participants != null)
            remoteParticipants.retainAll(participants);

        pendingSyncResponses.addAll(remoteParticipants);

        for (InetAddressAndPort participant : remoteParticipants)
        {
            logger.debug("Sending mutation tracking sync request to {} for {}", participant, desc);

            RepairMessage.sendMessageWithRetries(ctx,
                                                 RepairMessage.notDone(completionFuture),
                                                 request,
                                                 Verb.MT_SYNC_REQ,
                                                 participant,
                                                 new RequestCallback<MutationTrackingSyncResponse>()
                                                 {
                                                     @Override
                                                     public void onResponse(Message<MutationTrackingSyncResponse> msg)
                                                     {
                                                         onSyncResponse(msg.from(), msg.payload);
                                                     }

                                                     @Override
                                                     public void onFailure(InetAddressAndPort from, RequestFailure failure)
                                                     {
                                                         fail(new RuntimeException(
                                                             String.format("Mutation tracking sync failed: participant %s returned failure %s", from, failure.reason)));
                                                     }

                                                     @Override
                                                     public boolean invokeOnFailure()
                                                     {
                                                         return true;
                                                     }
                                                 });
        }
    }

    private void captureTargets()
    {
        checkState(!completionFuture.isDone());
        checkForTopologyChange();

        for (ShardSyncState state : shardStates.values())
        {
            state.captureTargets();
        }
    }

    /**
     * Checks if any of the shards we're tracking have changed due to topology updates.
     * If a change is detected, fails the repair via {@link #fail(Throwable)}.
     */
    private void checkForTopologyChange()
    {
        for (ShardSyncState state : shardStates.values())
        {
            Shard currentShard = getCurrentShard(state.shard.range);
            if (currentShard != state.shard)
            {
                fail(new RuntimeException("Repair failed: topology changed during sync"));
                return;
            }
        }
    }

    private Shard getCurrentShard(Range<Token> shardRange)
    {
        Shard[] result = new Shard[1];
        MutationTrackingService.instance().forEachShardInKeyspace(keyspace, shard -> {
            if (shard.range.equals(shardRange))
                result[0] = shard;
        });
        return result[0];
    }

    private void fail(Throwable cause)
    {
        if (completionFuture.tryFailure(cause))
        {
            MutationTrackingService.instance().unregisterSyncCoordinator(this);
            logger.warn("Sync coordinator for keyspace {} range {} failed: {}",
                        keyspace, range, cause.getMessage());
        }
    }

    /**
     * Check if all targets are reconciled across all shards.
     */
    private void checkIfReadyToComplete()
    {
        if (completionFuture.isDone())
            return;
        checkForTopologyChange();

        if (checkIfComplete())
        {
            logger.info("Sync coordinator completed for keyspace {} range {}", keyspace, range);
            complete();
        }
    }

    private boolean checkIfComplete()
    {
        if (completionFuture.isDone())
            return true;

        if (!pendingSyncResponses.isEmpty())
            return false;

        for (ShardSyncState state : shardStates.values())
        {
            if (!state.isComplete())
                return false;
        }
        return true;
    }

    /**
     * Called when offset updates are received from a participant.
     */
    public void onOffsetsReceived()
    {
        if (completionFuture.isDone())
            return;

        checkIfReadyToComplete();
    }

    /**
     * Called when a sync response is received from a participant in response to a
     * MutationTrackingSyncRequest. Updates the shard targets with the offsets from the
     * response, establishing a happens-before relationship with the repair start.
     *
     * @param from the participant that sent the response
     * @param response the sync response from a participant
     */
    public void onSyncResponse(InetAddressAndPort from, MutationTrackingSyncResponse response)
    {
        if (completionFuture.isDone())
            return;

        // Deduplicate: retries of MT_SYNC_REQ can produce multiple responses from the
        // same participant. Only process the first one.
        if (!pendingSyncResponses.remove(from))
            return;

        // Update shard targets with the offsets received from the participant
        for (Map.Entry<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> entry : response.offsetsByShard.entrySet())
        {
            Range<Token> shardRange = entry.getKey();
            ShardSyncState state = shardStates.get(shardRange);
            if (state != null)
            {
                state.targets.putAll(entry.getValue());
            }
        }

        logger.trace("Sync coordinator received sync response from {}", from);

        checkIfReadyToComplete();
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public Range<Token> getRange()
    {
        return range;
    }

    /**
     * Return the union of captured targets across all shards this coordinator managed, as a flat map keyed by 
     * globally unique {@link CoordinatorLogId}. Intended for consumers that need the offset IR waited for every
     * live replica to reach.
     * <p>
     * Callers should only invoke this after the completion future resolves successfully. Targets are captured before 
     * sync requests dispatch (see {@link #captureTargets}), but the caller usually wants the post-IR state.
     */
    public Map<CoordinatorLogId, Offsets.Immutable> getCapturedTargets()
    {
        Map<CoordinatorLogId, Offsets.Immutable> merged = new HashMap<>();
        for (ShardSyncState state : shardStates.values())
            merged.putAll(state.targets);
        return merged;
    }

    public void awaitCompletion() throws Exception
    {
        completionFuture.get();
    }

    public void cancel()
    {
        if (completionFuture.tryFailure(new RuntimeException("Sync cancelled")))
            MutationTrackingService.instance().unregisterSyncCoordinator(this);
    }

    public void timeout()
    {
        if (completionFuture.tryFailure(new TimeoutException("Mutation tracking sync timed out")))
            MutationTrackingService.instance().unregisterSyncCoordinator(this);
    }

    /**
     * Tracks sync state for a single shard.
     * Completion is scoped to only the live participant host IDs when provided,
     * so that dead/excluded nodes don't block sync completion.
     */
    private static class ShardSyncState
    {
        private final Shard shard;

        // If non-null, only these host IDs are considered for union/intersection.
        // If null, all shard participants are used (equivalent to no filtering).
        private final Set<Integer> liveHostIds;

        // Target offsets: LogId -> the offsets we're waiting for live nodes to have
        private final Map<CoordinatorLogId, Offsets.Immutable> targets = new ConcurrentHashMap<>();

        ShardSyncState(Shard shard, Set<Integer> liveHostIds)
        {
            this.shard = shard;
            this.liveHostIds = liveHostIds;
        }

        void captureTargets()
        {
            Map<CoordinatorLogId, Offsets.Immutable> unionOffsets = shard.collectUnionOfWitnessedOffsetsPerLog(liveHostIds);
            targets.putAll(unionOffsets);
        }

        boolean isComplete()
        {
            Map<CoordinatorLogId, Offsets.Immutable> currentReconciled = shard.collectReconciledOffsetsPerLog(liveHostIds);

            for (Map.Entry<CoordinatorLogId, Offsets.Immutable> entry : targets.entrySet())
            {
                CoordinatorLogId logId = entry.getKey();
                Offsets.Immutable target = entry.getValue();

                Offsets.Immutable reconciled = currentReconciled.get(logId);
                if (reconciled == null)
                    return false;

                if (!containsAll(reconciled, target))
                    return false;
            }
            return true;
        }

        private boolean containsAll(Offsets reconciled, Offsets target)
        {
            for (ShortMutationId id : target)
            {
                if (!reconciled.contains(id.offset()))
                    return false;
            }
            return true;
        }
    }
}
