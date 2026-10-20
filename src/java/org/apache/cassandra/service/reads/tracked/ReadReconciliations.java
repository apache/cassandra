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
package org.apache.cassandra.service.reads.tracked;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.ActiveLogReconciler;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.ExpiredStatePurger;
import org.apache.cassandra.replication.IncomingMutations;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Node2OffsetsMap;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.replication.PullMutationsRequest;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.locator.SystemReplicas.FULL_RANGE;

/**
 * Coordinates read reconciliation across replicas for tracked reads.
 * <p>
 * Protocol flow:
 * <p>
 * On the initial coordinator (replica or non-replica):
 *  0. Send summary requests to all summary nodes in parallel.
 * <p>
 * On the data node:
 *  1. Compute the local summary and send it to all summary nodes.
 *  2. For primary logs in the summary, initiate priority delivery of mutations
 *     present locally but absent from any peer.
 *  3. Collect remote summaries and request missing mutations.
 *  4. Collect requested mutations from peers.
 *  5. Track pairwise synchronization between nodes with local summaries and
 *     per-peer synchronization acknowledgments
 *  6. As soon as a mutually synchronized subset of nodes satisfies the quorum,
 *     invoke the read completion callback with augmenting offsets.
 *  7. When all contacted nodes deliver summaries and local pulls complete,
 *     remove the coordinator state.
 * <p>
 * On each summary node:
 *  1. Compute the local summary and send it to all peers.
 *  2. For primary logs in the summary, initiate priority delivery of mutations
 *     present locally but absent from any peer.
 *  3. Collect remote summaries and request missing mutations.
 *  4. Collect requested mutations from peers.
 *  5. When all missing mutations for a peer arrive, send a per-peer ack to the
 *     data node.
 *  6. When all summaries arrive, local pulls complete, and all required ACKs are
 *     sent, remove the coordinator state.
 */
public class ReadReconciliations implements ExpiredStatePurger.Expireable
{
    private static final int LOCAL_NODE = ClusterMetadata.current().myNodeId().id();

    public static final ReadReconciliations instance = new ReadReconciliations();

    public ReadReconciliations()
    {
        ExpiredStatePurger.instance.register(this);
    }

    public interface QuorumChecker
    {
        QuorumChecker NONE = endpoints -> false;

        boolean isSufficient(Endpoints<?> candidateEndpoints);

        static QuorumChecker simple(int requiredCount)
        {
            return endpoints -> endpoints.size() >= requiredCount;
        }

        static QuorumChecker forRead(ReplicaPlan.AbstractForRead<?, ?> plan)
        {
            Endpoints<?> planReplicas = plan.consistencyLevel().isDatacenterLocal()
                                        ? plan.liveAndDown().filter(InOurDc.replicas())
                                        : plan.liveAndDown();
            return endpoints -> ReplicaPlans.isSufficientLiveReplicasForRead(
                ClusterMetadata.current().locator,
                plan.replicationStrategy(),
                plan.consistencyLevel(),
                planReplicas.filter(replica -> endpoints.contains(replica.endpoint()))
            );
        }
    }

    // We are reusing the read id just for the ease of it, but it doesn't really have to match
    // read's id. Could be another instance of it, or a UUID or any other cluster-unique identifier.
    private final NonBlockingHashMap<TrackedRead.Id, Coordinator> coordinators = new NonBlockingHashMap<>();

    /**
     * Main entry point for summary nodes - if a coordinator's request arrives before any peer responses.
     * Executed in READ stage. Calculate the summary, process it locally and send it to peers so that they
     * can fill their own gaps.
     */
    public void handleSummaryRequest(TrackedRead.SummaryRequest request)
    {
        Coordinator reconcile = getOrCreateCoordinator(request.readId, request.dataNode, request.summaryNodes);
        if (reconcile.acceptLocalSummary(request.command.createMutationSummary(false)))
            coordinators.remove(request.readId, reconcile);
    }

    /**
     * The way for the data node to provide its final (seconary) summary.
     */
    public void acceptLocalSummary(TrackedRead.Id id, MutationSummary summary, int[] summaryNodes, QuorumChecker quorumChecker)
    {
        Coordinator reconcile = getOrCreateCoordinator(id, LOCAL_NODE, summaryNodes, quorumChecker);
        if (reconcile.acceptLocalSummary(summary, quorumChecker))
            coordinators.remove(id, reconcile);
    }

    /**
     * Can arrive from a peer node before a summary request
     */
    public void acceptRemoteSummary(InetAddressAndPort from, TrackedSummaryResponse summary, boolean supportsPerPeerAck)
    {
        Coordinator reconcile = getOrCreateCoordinator(summary.readId, summary.dataNode, summary.summaryNodes);
        if (reconcile.acceptRemoteSummary(summary.summary, node(from), supportsPerPeerAck))
            coordinators.remove(summary.readId, reconcile);
    }

    public void acceptSyncAck(InetAddressAndPort from, TrackedRead.Id id, Integer peerNode)
    {
        Coordinator reconcile = coordinators.get(id);
        if (reconcile != null && reconcile.acceptSyncAck(node(from), peerNode)) // could be already timed out / expired
            coordinators.remove(id, reconcile);
    }

    public void acceptMutation(TrackedRead.Id id, ShortMutationId mutationId)
    {
        logger.debug("Accepted mutation {} {}", id, mutationId);
        Coordinator reconcile = coordinators.get(id);
        if (reconcile != null && reconcile.acceptMutation(mutationId)) // could be already timed out / expired
            coordinators.remove(id, reconcile);
    }

    private Coordinator getOrCreateCoordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes)
    {
        return getOrCreateCoordinator(id, dataNode, summaryNodes, QuorumChecker.NONE);
    }

    private Coordinator getOrCreateCoordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes, int requiredSummaries)
    {
        return getOrCreateCoordinator(id, dataNode, summaryNodes, QuorumChecker.simple(requiredSummaries + 1));
    }

    private Coordinator getOrCreateCoordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes, QuorumChecker quorumChecker)
    {
        Coordinator reconcile = coordinators.get(id);
        if (reconcile != null) return reconcile;
        return coordinators.computeIfAbsent(id, readId -> new Coordinator(id, dataNode, summaryNodes, quorumChecker));
    }

    @VisibleForTesting
    Coordinator getCoordinator(TrackedRead.Id id)
    {
        return coordinators.get(id);
    }

    @Override
    public int expire(long nanoTime)
    {
        int n = 0;
        for (Map.Entry<TrackedRead.Id, Coordinator> entry : coordinators.entrySet())
        {
            TrackedRead.Id id = entry.getKey();
            Coordinator coordinator = entry.getValue();

            if (coordinator.isPurgeable(nanoTime) && coordinators.remove(id, coordinator))
                n++;
        }
        return n;
    }

    static final class Coordinator
    {
        static final class PeerState
        {
            final int nodeId;
            final Replica replica;
            MutationSummary summary;
            boolean supportsPerPeerAck;
            final Log2OffsetsMap.Mutable pendingLocalMutations = new Log2OffsetsMap.Mutable();
            boolean ackSent = false;
            final IntHashSet syncedPeers = new IntHashSet();
            boolean legacyAckReceived = false;

            PeerState(int nodeId)
            {
                this.nodeId = nodeId;
                this.replica = Replica.fullReplica(host(nodeId), FULL_RANGE);
            }
        }

        private final TrackedRead.Id id;
        private final int dataNode;
        private final int[] summaryNodes;
        private final Replica dataNodeReplica;
        private QuorumChecker quorumChecker;
        private final long expiresAtNanos;

        private MutationSummary localSummary;
        private final Int2ObjectHashMap<PeerState> remotePeers = new Int2ObjectHashMap<>();
        private final Log2OffsetsMap.Mutable completedMutations = new Log2OffsetsMap.Mutable();
        private final Log2OffsetsMap.Mutable requested = new Log2OffsetsMap.Mutable();
        private boolean readCompleted = false;
        private boolean coordinatorCompleted = false;

        Coordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes)
        {
            this(id, dataNode, summaryNodes, QuorumChecker.NONE);
        }

        Coordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes, int requiredSummaries)
        {
            this(id, dataNode, summaryNodes, QuorumChecker.simple(requiredSummaries + 1));
        }

        Coordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes, QuorumChecker quorumChecker)
        {
            this.id = id;
            this.dataNode = dataNode;
            this.summaryNodes = summaryNodes;
            this.dataNodeReplica = Replica.fullReplica(host(dataNode), FULL_RANGE);
            this.quorumChecker = quorumChecker != null ? quorumChecker : QuorumChecker.NONE;
            this.expiresAtNanos =
                Clock.Global.nanoTime() + Math.max(DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS),
                                                   DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.NANOSECONDS));
            if (dataNode != LOCAL_NODE)
                remotePeers.put(dataNode, new PeerState(dataNode));
            for (int node : summaryNodes)
                if (node != LOCAL_NODE)
                    remotePeers.put(node, new PeerState(node));
        }

        PeerState getPeerState(int nodeId)
        {
            return remotePeers.get(nodeId);
        }

        @VisibleForTesting
        void setLocalSummaryForTesting(MutationSummary summary)
        {
            this.localSummary = summary;
            updatePairwiseSyncForLocalSummary();
        }

        @VisibleForTesting
        void setQuorumCheckerForTesting(QuorumChecker quorumChecker)
        {
            this.quorumChecker = quorumChecker;
        }

        @VisibleForTesting
        boolean isReadCompleted()
        {
            return readCompleted;
        }

        @VisibleForTesting
        boolean isCoordinatorCompleted()
        {
            return coordinatorCompleted;
        }

        /**
         * For all the logs in the summary that are owned by us, preemptively prioritise delivery of
         * any mutations that are absent from other participating nodes according to our primary coordinator
         * log knowledge. This is an optimisation, and it can be omitted without affecting correctness.
         */
        synchronized boolean acceptLocalSummary(MutationSummary summary)
        {
            return acceptLocalSummary(summary, null);
        }

        synchronized boolean acceptLocalSummary(MutationSummary summary, @Nullable QuorumChecker quorumChecker)
        {
            if (quorumChecker != null)
                this.quorumChecker = quorumChecker;
            this.localSummary = summary;
            updatePairwiseSyncForLocalSummary();
            IntArrayList remoteNodes = new IntArrayList(summaryNodes.length, Integer.MIN_VALUE);
            if (dataNode != LOCAL_NODE)
                remoteNodes.addInt(dataNode);
            for (int node : summaryNodes)
                if (node != LOCAL_NODE)
                    remoteNodes.addInt(node);

            if (!remoteNodes.isEmpty())
            {
                // send the summary to all peers, so that they can initiate reconciling their gaps ASAP
                TrackedSummaryResponse response = new TrackedSummaryResponse(id, summary, dataNode, summaryNodes);
                Message<TrackedSummaryResponse> message = Message.builder(Verb.MT_SUMMARY_RSP, response)
                                                                 .withFlag(MessageFlag.MT_PER_PEER_RECONCILE_ACK)
                                                                 .withTracingParams()
                                                                 .build();
                for (int i = 0; i < remoteNodes.size(); i++)
                {
                    int node = remoteNodes.getInt(i);
                    MutationTrackingService.instance().validateSummaryForParticipant(summary, node);
                    MessagingService.instance().send(message, host(node));
                }
            }

            Node2OffsetsMap missingOffsets = new Node2OffsetsMap();
            summary.forEachUnreconciled(ourOffsets -> {
                CoordinatorLogId logId = ourOffsets.logId();
                if (logId.hostId() != LOCAL_NODE)
                    return;

                MutationTrackingService.instance().collectRemotelyMissingMutations(ourOffsets, remoteNodes, missingOffsets);
                // we don't listen to or block on delivery of these mutations, intentionally
                missingOffsets.forEach(ReadReconciliations::push);
                missingOffsets.clear();
            });

            return maybeComplete();
        }

        /**
         * Calculate the diff with the remote summary to see which mutations from the summary we lack,
         * and pull them - from the primary coordinator, if it's alive, or the remote replica, if it's not.
         */
        synchronized boolean acceptRemoteSummary(MutationSummary summary, int remoteNode, boolean supportsPerPeerAck)
        {
            PeerState peerState = remotePeers.computeIfAbsent(remoteNode, PeerState::new);
            peerState.summary = summary;
            peerState.supportsPerPeerAck = supportsPerPeerAck;
            updatePairwiseSync(peerState);

            Log2OffsetsMap.Mutable locallyMissing = new Log2OffsetsMap.Mutable();
            MutationTrackingService.instance().collectLocallyMissingMutations(summary, locallyMissing);

            // Record pending local mutations before network pull deduplication, excluding mutations
            // that were already received during this read.
            peerState.pendingLocalMutations.addAll(locallyMissing);
            peerState.pendingLocalMutations.removeAll(completedMutations);

            // don't request what's already been requested for other remote summaries
            // TODO (consider): rely entirely on IncomingMutations deduplication instead
            Log2OffsetsMap.Mutable toRequest = new Log2OffsetsMap.Mutable();
            toRequest.addAll(locallyMissing);
            toRequest.removeAll(requested);
            requested.addAll(toRequest);

            // TODO (expected, low priority): handle timeouts here to stop earlier
            IncomingMutations.Callback callback =
                (mutationId) -> ReadReconciliations.instance.acceptMutation(id, mutationId);
            for (Offsets offsets : toRequest.offsets())
                pull(remoteNode, offsets, callback);

            // Check if any mutations arrived locally while registering callbacks
            if (!peerState.pendingLocalMutations.isEmpty())
            {
                Log2OffsetsMap.Mutable stillMissing = new Log2OffsetsMap.Mutable();
                MutationTrackingService.instance().collectLocallyMissingMutations(summary, stillMissing);
                Log2OffsetsMap.Mutable nowWitnessed = new Log2OffsetsMap.Mutable();
                nowWitnessed.addAll(peerState.pendingLocalMutations);
                nowWitnessed.removeAll(stillMissing);
                completedMutations.addAll(nowWitnessed);
                peerState.pendingLocalMutations.removeAll(nowWitnessed);
            }

            return maybeComplete();
        }

        synchronized boolean acceptSyncAck(int syncId, Integer peerNode)
        {
            PeerState sender = remotePeers.get(syncId);
            if (sender == null)
                throw new IllegalStateException(String.format("Received reconcile ack from unknown peer %d for read %s (expected peers: %s)",
                                                              syncId, id, remotePeers.keySet()));

            if (peerNode != null)
                sender.syncedPeers.add((int) peerNode);
            else
                sender.legacyAckReceived = true;

            return maybeComplete();
        }

        synchronized boolean acceptMutation(ShortMutationId mutationId)
        {
            completedMutations.add(mutationId);
            boolean anyPeerBecameEmpty = false;
            for (PeerState peer : remotePeers.values())
            {
                boolean wasEmpty = peer.pendingLocalMutations.isEmpty();
                peer.pendingLocalMutations.remove(mutationId);
                if (!wasEmpty && peer.pendingLocalMutations.isEmpty())
                    anyPeerBecameEmpty = true;
            }

            if (!anyPeerBecameEmpty)
                return false;

            return maybeComplete();
        }

        private boolean isDataNode()
        {
            return LOCAL_NODE == dataNode;
        }

        private boolean allSummariesReceived()
        {
            if (localSummary == null)
                return false;
            for (PeerState peer : remotePeers.values())
            {
                if (peer.summary == null)
                    return false;
            }
            return true;
        }

        private boolean allPendingLocalMutationsEmpty()
        {
            for (PeerState peer : remotePeers.values())
            {
                if (!peer.pendingLocalMutations.isEmpty())
                    return false;
            }
            return true;
        }

        private boolean dataNodeSupportsPerPeerAck()
        {
            PeerState dataNodePeer = remotePeers.get(dataNode);
            return dataNodePeer != null && dataNodePeer.summary != null && dataNodePeer.supportsPerPeerAck;
        }

        void maybeSendPerPeerAcksOnSummaryNode()
        {
            if (isDataNode() || localSummary == null || !dataNodeSupportsPerPeerAck())
                return;

            for (PeerState peer : remotePeers.values())
            {
                if (peer.ackSent || peer.summary == null)
                    continue;

                // If missing offsets existed, send ack once pending local mutations for this peer are empty
                if (peer.pendingLocalMutations.isEmpty())
                {
                    peer.ackSent = true;
                    Message<ReadReconcileAck> ackMsg =
                        Message.builder(Verb.MT_READ_RECONCILE_ACK, new ReadReconcileAck(id))
                               .withParam(ParamType.MT_RECONCILE_PEER, peer.nodeId)
                               .withTracingParams()
                               .build();
                    MessagingService.instance().send(ackMsg, host(dataNode));
                }
            }
        }

        private boolean allAcksSentOrNotNeededOnSummaryNode()
        {
            if (isDataNode())
                return true;

            if (dataNodeSupportsPerPeerAck())
            {
                for (PeerState peer : remotePeers.values())
                {
                    if (!peer.ackSent)
                        return false;
                }
            } // else Legacy mode: single ack is sent in complete()
            return true;
        }

        private void updatePairwiseSync(PeerState peer)
        {
            if (peer.summary == null)
                return;

            updatePairwiseSyncWithLocal(peer);

            for (PeerState other : remotePeers.values())
            {
                if (other.nodeId == peer.nodeId || other.summary == null)
                    continue;

                if (!MutationSummary.hasMissingUnreconciled(other.summary, peer.summary))
                    peer.syncedPeers.add(other.nodeId);

                if (!MutationSummary.hasMissingUnreconciled(peer.summary, other.summary))
                    other.syncedPeers.add(peer.nodeId);
            }
        }

        private void updatePairwiseSyncWithLocal(PeerState peer)
        {
            if (localSummary == null || peer.summary == null)
                return;

            if (!MutationSummary.hasMissingUnreconciled(localSummary, peer.summary))
                peer.syncedPeers.add(LOCAL_NODE);

            if (!isDataNode() && !MutationSummary.hasMissingUnreconciled(peer.summary, localSummary))
                peer.ackSent = true;
        }

        private void updatePairwiseSyncForLocalSummary()
        {
            if (localSummary == null)
                return;

            for (PeerState peer : remotePeers.values())
                updatePairwiseSyncWithLocal(peer);
        }

        private boolean isSynchronizedWith(PeerState peer, int peerNodeId, MutationSummary peerSummary)
        {
            if (peer.summary == null || peerSummary == null)
                return false;

            return peer.legacyAckReceived || peer.syncedPeers.contains(peerNodeId);
        }

        private boolean isMutuallySynchronizedWith(PeerState candidate, List<PeerState> current)
        {
            for (PeerState peer : current)
            {
                if (!isSynchronizedWith(candidate, peer.nodeId, peer.summary) ||
                    !isSynchronizedWith(peer, candidate.nodeId, candidate.summary))
                    return false;
            }
            return true;
        }

        private EndpointsForRange toEndpoints(List<PeerState> quorumSummaryNodes)
        {
            EndpointsForRange.Builder builder = EndpointsForRange.builder(FULL_RANGE, quorumSummaryNodes.size() + 1);
            builder.add(dataNodeReplica);
            for (PeerState peer : quorumSummaryNodes)
                builder.add(peer.replica);
            return builder.build();
        }

        private boolean isQuorumSatisfied(List<PeerState> candidateQuorum)
        {
            if (quorumChecker == null)
                return false;

            return quorumChecker.isSufficient(toEndpoints(candidateQuorum));
        }

        /**
         * Searches for a mutually synchronized subset of summary nodes that satisfies the quorum checker.
         */
        private List<PeerState> findSatisfiedQuorum()
        {
            if (localSummary == null)
                return null;

            // The data node alone may satisfy the quorum (e.g. standard CL.ONE), even when summary nodes were contacted
            if (isQuorumSatisfied(Collections.emptyList()))
                return Collections.emptyList();

            // Find all locally ready summary nodes (summary received, dataNode pulls done, and synchronized with dataNode)
            List<PeerState> readyNodes = new ArrayList<>();
            for (int nodeId : summaryNodes)
            {
                if (nodeId == LOCAL_NODE)
                    continue;

                PeerState peer = remotePeers.get(nodeId);
                if (peer != null && peer.summary != null && peer.pendingLocalMutations.isEmpty()
                    && isSynchronizedWith(peer, dataNode, localSummary))
                {
                    readyNodes.add(peer);
                }
            }

            if (readyNodes.isEmpty())
                return null;

            // Search for smallest subset of readyNodes (size 1..readyNodes.size()) that satisfies the quorum
            for (int k = 1; k <= readyNodes.size(); k++)
            {
                List<PeerState> subset = findMutualQuorumSubset(readyNodes, k, 0, new ArrayList<>());
                if (subset != null)
                    return subset;
            }

            return null;
        }

        private List<PeerState> findMutualQuorumSubset(List<PeerState> candidates, int k, int start, List<PeerState> current)
        {
            if (current.size() == k)
                return isQuorumSatisfied(current) ? new ArrayList<>(current) : null;

            for (int i = start; i < candidates.size(); i++)
            {
                if (current.size() + (candidates.size() - i) < k)
                    break;

                PeerState candidate = candidates.get(i);
                if (!isMutuallySynchronizedWith(candidate, current))
                    continue;

                current.add(candidate);
                List<PeerState> result = findMutualQuorumSubset(candidates, k, i + 1, current);
                if (result != null)
                    return result;
                current.remove(current.size() - 1);
            }

            return null;
        }

        private boolean maybeComplete()
        {
            if (coordinatorCompleted)
                return false;

            if (!isDataNode())
            {
                maybeSendPerPeerAcksOnSummaryNode();
                if (allSummariesReceived() && allPendingLocalMutationsEmpty() && allAcksSentOrNotNeededOnSummaryNode())
                {
                    coordinatorCompleted = true;
                    return complete();
                }
                return false;
            }

            // On dataNode: eager read completion
            if (!readCompleted)
            {
                List<PeerState> quorum = findSatisfiedQuorum();
                if (quorum != null)
                {
                    readCompleted = true;
                    MutationTrackingService.instance().localReads().acknowledgeReconcile(id, augmentingOffsets(quorum));
                }
            }

            // Clean up Coordinator once all contacted nodes have delivered summaries and pulls are done
            if (readCompleted && allSummariesReceived() && allPendingLocalMutationsEmpty())
            {
                coordinatorCompleted = true;
                return true;
            }

            return false;
        }

        private Log2OffsetsMap.Mutable augmentingOffsets(List<PeerState> quorum)
        {
            Log2OffsetsMap.Mutable offsets = new Log2OffsetsMap.Mutable();

            for (PeerState peer : quorum)
            {
                if (peer.summary != null)
                    offsets.addAll(peer.summary);
            }

            if (localSummary != null)
                offsets.removeAll(localSummary);

            return offsets;
        }

        private boolean complete()
        {
            if (!dataNodeSupportsPerPeerAck())
            {
                // Legacy mode fallback: send a single ack without peer param
                MessagingService.instance().send(Message.out(Verb.MT_READ_RECONCILE_ACK, new ReadReconcileAck(id)), host(dataNode));
            }
            return true;
        }

        boolean isPurgeable(long nanoTime)
        {
            return nanoTime - expiresAtNanos > 0;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ReadReconciliations.class);

    /**
     * @param node node id of the remote replica from which we got the summary
     * @param offsets offsets that we need to pull - from the coordinator, if alive, or from the
     *                node we got the summary from (by definition it also has the mutation)
     */
    private static void pull(int node, Offsets offsets, IncomingMutations.Callback callback)
    {
        InetAddressAndPort logCoordinator = host(offsets.logId().hostId());
        InetAddressAndPort pullFrom = FailureDetector.instance.isAlive(logCoordinator)
                                    ? logCoordinator
                                    : host(node);

        Offsets.Mutable toPull = new Offsets.Mutable(offsets.logId());
        for (ShortMutationId id : offsets)
            if (MutationTrackingService.instance().registerMutationCallback(id, callback))
                toPull.add(id.offset());

        if (!toPull.isEmpty())
        {
            PullMutationsRequest pull = new PullMutationsRequest(Offsets.Immutable.copy(toPull), ActiveLogReconciler.Priority.HIGH);
            logger.debug("Pulling {} from {}", pull, pullFrom);
            MessagingService.instance().send(Message.out(Verb.MT_PULL_MUTATIONS_REQ, pull), pullFrom);
        }
    }

    private static void push(int node, Offsets offsets)
    {
        // TODO implement pre-emptive push with read priority
    }

    private static InetAddressAndPort host(int node)
    {
        return ClusterMetadata.current().directory.endpoint(new NodeId(node));
    }

    private static int node(InetAddressAndPort host)
    {
        return ClusterMetadata.current().directory.peerId(host).id();
    }
}
