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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.agrona.collections.IntArrayList;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
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
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Accumulator;

/**
 * On the initial coordinator (replica or not):
 *  0. Issue summary requests to all summary nodes, in parallel
 * <p>
 * On the data node / coordinator:
 *  1. Compute the secondary summary, send to all other nodes for reconcile
 *  2. For primary logs in the summary, initiate priority delivery of mutations
 *     that are present in our summary but absent from any summary node
 *  3. Collect summary responses from all summary nodes, request missing mutations
 *     whenever we get a summary from any of the nodes
 *  4. Collect the requested mutations from other nodes
 *  5. Collect sync-done acks from every summary node
 *  6. When all the summaries, all missing mutations, and all sync-done acks have
 *     been received, invoke the reconcile-complete callback
 *  7. Clean up state after ourselves (states map)
 * <p>
 * On each summary node:
 *  1. Compute the secondary summary, send to all other nodes for reconcile
 *  2. For primary logs in the summary, initiate priority delivery of mutations
 *     that are present in our summary but absent from any summary or data node
 *  3. Collect summary responses from all summary nodes, request missing mutations
 *     whenever we get a summary from any of the nodes
 *  4. Collect the requested mutations from other nodes
 *  5. When all the summaries and all missing mutations have been received,
 *     respond to the data node with a sync-done message
 *  6. Clean up state after ourselves (states) map
 */
public class ReadReconciliations implements ExpiredStatePurger.Expireable
{
    private static final int LOCAL_NODE = ClusterMetadata.current().myNodeId().id();

    public static final ReadReconciliations instance = new ReadReconciliations();

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
            coordinators.remove(request.readId);
    }

    /**
     * The way for the data node to provide its final (seconary) summary.
     */
    public void acceptLocalSummary(TrackedRead.Id id, MutationSummary summary, int[] summaryNodes)
    {
        Coordinator reconcile = getOrCreateCoordinator(id, LOCAL_NODE, summaryNodes);
        if (reconcile.acceptLocalSummary(summary))
            coordinators.remove(id);
    }

    /**
     * Can arrive from a peer node before a summary request
     */
    public void acceptRemoteSummary(InetAddressAndPort from, TrackedSummaryResponse summary)
    {
        Coordinator reconcile = getOrCreateCoordinator(summary.readId, summary.dataNode, summary.summaryNodes);
        if (reconcile.acceptRemoteSummary(summary.summary, node(from)))
            coordinators.remove(summary.readId);
    }

    public void acceptSyncAck(InetAddressAndPort from, TrackedRead.Id id)
    {
        Coordinator reconcile = coordinators.get(id);
        if (reconcile != null && reconcile.acceptSyncAck(node(from))) // could be already timed out / expired
            coordinators.remove(id);
    }

    public void acceptMutation(TrackedRead.Id id, ShortMutationId mutationId)
    {
        logger.debug("Accepted mutation {} {}", id, mutationId);
        Coordinator reconcile = coordinators.get(id);
        if (reconcile != null && reconcile.acceptMutation(mutationId)) // could be already timed out / expired
            coordinators.remove(id);
    }

    private Coordinator getOrCreateCoordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes)
    {
        Coordinator reconcile = coordinators.get(id);
        if (reconcile != null) return reconcile;
        return coordinators.computeIfAbsent(id, readId -> new Coordinator(id, dataNode, summaryNodes));
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

    static class Coordinator
    {
        private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

        // FIXME: this will probably break per-DC consistency semantica of SatelliteReplicationStrategy
        //  once read speculation is implemented
        private static final AtomicLongFieldUpdater<Coordinator> remainingUpdater =
            AtomicLongFieldUpdater.newUpdater(Coordinator.class, "remaining");
        private volatile long remaining; // three values packed into one atomic long

        private final TrackedRead.Id id;
        private final int dataNode;
        private final int[] summaryNodes;
        private final long expiresAtNanos;

        private final Accumulator<Pair<Integer, MutationSummary>> summaries;

        private final Set<ShortMutationId> requested = new NonBlockingHashSet<>();

        Coordinator(TrackedRead.Id id, int dataNode, int[] summaryNodes)
        {
            this.id = id;
            this.dataNode = dataNode;
            this.summaryNodes = summaryNodes;
            this.expiresAtNanos =
                Clock.Global.nanoTime() + Math.max(DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS),
                                                   DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.NANOSECONDS));
            int remainingSummaries = 1 + summaryNodes.length;
            int remainingMutations = 0;
            int remainingSyncAcks = dataNode == LOCAL_NODE ? summaryNodes.length : 0;
            remaining = remaining(remainingMutations, remainingSummaries, remainingSyncAcks);

            summaries = new Accumulator<>(remainingSummaries);
        }

        /**
         * Confirm that we're only counting responses from nodes initially chosen by the read coordinator
         * This is to prevent the implementation of tracked read speculation (doesn't exist yet) from breaking
         * the per-dc consistency semantics of SatelliteReplicationStrategy because of the simple count completion
         * mechanics this class uses for tracking received summarys / syncAcks
         */
        private void checkNodeIsExpected(int check)
        {
            if (check == dataNode)
                return;

            for (int node : summaryNodes)
                if (check == node)
                    return;

            throw new IllegalStateException("Not expecting response from node " + check);
        }

        /**
         * For all the logs in the summary that are owned by us, preemptively prioritise delivery of
         * any mutations that are absent from other participating nodes according to our primary coordinator
         * log knowledge. This is an optimisation, and it can be omitted without affecting correctness.
         */
        boolean acceptLocalSummary(MutationSummary summary)
        {
            checkNodeIsExpected(LOCAL_NODE);
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
                Message<TrackedSummaryResponse> message = Message.out(Verb.TRACKED_SUMMARY_RSP, response);
                for (int node : remoteNodes)
                    MessagingService.instance().send(message, host(node));
            }

            Iterator<Offsets> iter = summary.onlyUnreconciled();
            Node2OffsetsMap missingOffsets = new Node2OffsetsMap();
            while (iter.hasNext())
            {
                Offsets ourOffsets = iter.next();
                CoordinatorLogId logId = ourOffsets.logId();
                if (logId.hostId() != LOCAL_NODE)
                    continue;

                MutationTrackingService.instance().collectRemotelyMissingMutations(ourOffsets, remoteNodes, missingOffsets);
                // we don't listen to or block on delivery of these mutations, intentionally
                missingOffsets.forEach(ReadReconciliations::push);
                missingOffsets.clear();
            }

            summaries.add(Pair.create(LOCAL_NODE, summary));
            return updateRemainingAndMaybeComplete(0, -1, 0);
        }

        /**
         * Calculate the diff with the remote summary to see which mutations from the summary we lack,
         * and pull them - from the primary coordinator, if it's alive, or the remote replica, if it's not.
         */
        boolean acceptRemoteSummary(MutationSummary summary, int remoteNode)
        {
            checkNodeIsExpected(remoteNode);
            Log2OffsetsMap.Mutable missingMutations = new Log2OffsetsMap.Mutable();
            MutationTrackingService.instance().collectLocallyMissingMutations(summary, missingMutations);

            // don't request what's already been requested for other remote summaries
            // TODO (consider): rely entirely on IncomingMutations deduplication instead
            missingMutations.removeAll(requested);
            missingMutations.forEach(requested::add);
            int missingCount = missingMutations.idCount();

            // TODO (expected, low priority): handle timeouts here to stop earlier
            IncomingMutations.Callback callback =
                (mutationId) -> ReadReconciliations.instance.acceptMutation(id, mutationId);
            for (Offsets offsets : missingMutations.offsets())
                pull(remoteNode, offsets, callback);

            summaries.add(Pair.create(remoteNode, summary));
            return updateRemainingAndMaybeComplete(missingCount, -1, 0);
        }

        boolean acceptSyncAck(int node)
        {
            checkNodeIsExpected(node);
            return updateRemainingAndMaybeComplete(0, 0, -1);
        }

        boolean acceptMutation(ShortMutationId ignoredMutationId)
        {
            return updateRemainingAndMaybeComplete(-1, 0, 0);
        }

        /**
         * Remote summaries minus data node summary offsets
         * <p>
         * This calculation combines BOTH reconciled and unreconciled mutations reported by other nodes, and
         * then subtracts mutations reported locally for correctness
         * <p>
         * If we subtracted reconciled ids from the unreconciled ids, we could violate read monotonicity in this scenario:
         * 1. Read starts locally and doesn't see mutation M.
         * 2. During reconciliation, mutation M arrives and is marked reconciled, other replicas report mutation M as reconciled
         * 3. If we filtered out reconciled mutations, this read wouldn't augment with M
         * 4. A concurrent read could see M in its initial data
         * 5. This read returns without M
         * <p>
         * Instead, we include all mutations and rely on token range filtering during actual mutation
         * retrieval (in PartialTrackedRead.augment()) to ensure we only augment with mutations
         * relevant to this read's range/key
         */
        private Log2OffsetsMap.Mutable augmentingOffsets()
        {
            Log2OffsetsMap.Mutable offsets = new Log2OffsetsMap.Mutable();

            // add up all remote summaries
            for (int i = 0, size = summaries.size(); i < size; i++)
            {
                Pair<Integer, MutationSummary> summary = summaries.get(i);
                if (summary.left != LOCAL_NODE)
                    offsets.addAll(summary.right);
            }

            // subtract the local summary
            for (int i = 0, size = summaries.size(); i < size; i++)
            {
                Pair<Integer, MutationSummary> summary = summaries.get(i);
                if (summary.left == LOCAL_NODE)
                {
                    offsets.removeAll(summary.right);
                    break;
                }
            }

            return offsets;
        }

        private boolean isDataNode()
        {
            return LOCAL_NODE == dataNode;
        }

        /*
         * Logic to deal with remaining mutations/summaries/syncAcks counters atomically.
         */

        private static int remainingMutations(long remaining)
        {
            return (int) ((remaining >>> 32) & 0xFFFFFFFFL);
        }

        private static int remainingSummaries(long remaining)
        {
            return (int) ((remaining >>> 16) & 0xFFFFL);
        }

        private static int remainingSyncAcks(long remaining)
        {
            return (int) (remaining & 0xFFFFL);
        }

        // 32 bits for mutations, 16 bits for summaries, 16 bits for syncAcks
        private static long remaining(int mutations, int summaries, int syncAcks)
        {
            return ((long) mutations << 32) | (((long) summaries << 16) | syncAcks);
        }

        private long updateRemaining(int mutationsDelta, int summariesDelta, int syncAcksDelta)
        {
            long prev, next;
            do
            {
                prev = remaining;
                int mutations = remainingMutations(prev) + mutationsDelta;
                int summaries = remainingSummaries(prev) + summariesDelta;
                int syncAcks = remainingSyncAcks(prev) + syncAcksDelta;
                logger.trace("[Read {}] Still waiting for {} mutations, {} summaries, {} syncAcks", id, mutations, summaries, syncAcks);
                next = remaining(mutations, summaries, syncAcks);
            } while (!remainingUpdater.compareAndSet(this, prev, next));
            return next;
        }

        protected boolean updateRemainingAndMaybeComplete(int mutationsDelta, int summariesDelta, int syncAcksDelta)
        {
            return updateRemaining(mutationsDelta, summariesDelta, syncAcksDelta) == 0 && complete();
        }

        protected boolean complete()
        {
            if (isDataNode())
                MutationTrackingService.instance().localReads().acknowledgeReconcile(id, augmentingOffsets());
            else
                MessagingService.instance().send(Message.out(Verb.READ_RECONCILE_ACK, new ReadReconcileAck(id)), host(dataNode));

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
            PullMutationsRequest pull = new PullMutationsRequest(Offsets.Immutable.copy(toPull));
            logger.debug("Pulling {} from {}", pull, pullFrom);
            MessagingService.instance().send(Message.out(Verb.PULL_MUTATIONS_REQ, pull), pullFrom);
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
