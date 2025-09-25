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

package org.apache.cassandra.service.paxos;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntHashSet;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.service.paxos.Paxos.Participants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyMap;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_COMMIT_REMOTE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.service.StorageProxy.shouldHint;
import static org.apache.cassandra.service.StorageProxy.submitHint;
import static org.apache.cassandra.service.paxos.Commit.Agreed;
import static org.apache.cassandra.utils.concurrent.ConditionAsConsumer.newConditionAsConsumer;

// Does not support EACH_QUORUM, as no such thing as EACH_SERIAL
public class PaxosCommit<OnDone extends Consumer<? super PaxosCommit.Status>> extends PaxosRequestCallback<NoPayload>
{
    public static final RequestHandler requestHandler = new RequestHandler();
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommit.class);

    private static volatile boolean ENABLE_DC_LOCAL_COMMIT = CassandraRelevantProperties.ENABLE_DC_LOCAL_COMMIT.getBoolean();

    public static boolean getEnableDcLocalCommit()
    {
        return ENABLE_DC_LOCAL_COMMIT;
    }

    public static void setEnableDcLocalCommit(boolean enableDcLocalCommit)
    {
        ENABLE_DC_LOCAL_COMMIT = enableDcLocalCommit;
    }

    /**
     * Represents the current status of a commit action: it is a status rather than a result,
     * as the result may be unknown without sufficient responses (though in most cases it is final status).
     */
    static class Status
    {
        private final Paxos.MaybeFailure maybeFailure;

        Status(Paxos.MaybeFailure maybeFailure)
        {
            this.maybeFailure = maybeFailure;
        }

        boolean isSuccess() { return maybeFailure == null; }
        Paxos.MaybeFailure maybeFailure() { return maybeFailure; }

        public String toString() { return maybeFailure == null ? "Success" : maybeFailure.toString(); }
    }

    private static final Status success = new Status(null);

    private static final AtomicLongFieldUpdater<PaxosCommit> responsesUpdater = AtomicLongFieldUpdater.newUpdater(PaxosCommit.class, "responses");

    final Agreed commit;
    final boolean allowHints;
    final ConsistencyLevel consistencyForConsensus;
    final ConsistencyLevel consistencyForCommit;

    final EndpointsForToken replicas;
    final int required;
    final OnDone onDone;

    @Nullable
    final IntHashSet remoteReplicas;

    /**
     * packs two 32-bit integers;
     * bit 00-31: accepts
     * bit 32-63: failures/timeouts
     * 
     * {@link #accepts} 
     * {@link #failures}
     */
    private volatile long responses;

    public PaxosCommit(Agreed commit, boolean allowHints, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, EndpointsForToken replicas, int required, OnDone onDone)
    {
        // Check if this is a tracked keyspace
        boolean isTracked = commit.metadata().replicationType().isTracked();

        Agreed commitToUse = commit;
        IntHashSet remoteReplicas = null;
        if (isTracked)
        {
            // Precondition: for tracked keyspaces, the local node must be a replica
            // so it can generate an ID
            InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();
            checkState(replicas.endpoints().contains(localEndpoint),
                       "For tracked keyspaces, the coordinator must be a replica. Local endpoint %s not in replicas %s",
                       localEndpoint, replicas.endpoints());

            // Generate mutation ID if the commit doesn't already have one
            // (commits loaded from system.paxos may already have the saved mutation ID)
            if (commit.mutation.id().isNone())
            {
                Token token = commit.partitionKey().getToken();
                MutationId mutationId = MutationTrackingService.instance().nextMutationId(commit.metadata().keyspace, token);
                Mutation mutationWithId = commit.makeMutation(mutationId);
                commitToUse = new Commit.Agreed(commit.ballot, mutationWithId);
            }

            // Collect remote replicas for tracking service
            remoteReplicas = new IntHashSet();
            ClusterMetadata metadata = ClusterMetadata.current();
            for (int i = 0; i < replicas.size(); i++)
            {
                Replica replica = replicas.get(i);
                if (!replica.isSelf())
                    remoteReplicas.add(metadata.directory.peerId(replica.endpoint()).id());
            }
        }

        this.commit = commitToUse;
        this.allowHints = allowHints;
        this.consistencyForConsensus = consistencyForConsensus;
        this.consistencyForCommit = consistencyForCommit;
        this.replicas = replicas;
        this.onDone = onDone;
        this.required = required;
        this.remoteReplicas = remoteReplicas;

        if (required == 0)
            onDone.accept(status());
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static Paxos.Async<Status> commit(Agreed commit, EndpointsForToken all, EndpointsForToken allLive, EndpointsForToken allDown, int required, boolean isUrgent, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints)
    {
        // Check if this is a tracked keyspace requiring forwarding to a replica coordinator
        if (isTrackedKeyspaceRequiringForwarding(commit, all))
        {
            // For async version, create a wrapper that handles forwarding
            Status[] statusHolder = new Status[1];
            ConditionAsConsumer<Status> condition = newConditionAsConsumer();
            Consumer<Status> statusCapture = status -> {
                statusHolder[0] = status;
                condition.accept(status);
            };
            forwardPaxos2Commit(commit, all, allLive, allDown, required, isUrgent, consistencyForConsensus, consistencyForCommit, statusCapture);
            
            return new Paxos.Async<Status>()
            {
                @Override
                public Status awaitUntil(long deadline)
                {
                    try
                    {
                        condition.awaitUntil(deadline);
                        return statusHolder[0] != null ? statusHolder[0] : new Status(new Paxos.MaybeFailure(true, all.size(), required, 0, emptyMap()));
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return new Status(new Paxos.MaybeFailure(true, all.size(), required, 0, emptyMap()));
                    }
                }
            };
        }

        // to avoid unnecessary object allocations we extend PaxosPropose to implements Paxos.Async
        class Async extends PaxosCommit<ConditionAsConsumer<Status>> implements Paxos.Async<Status>
        {
            private Async(Agreed commit, boolean allowHints, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, EndpointsForToken all, int required)
            {
                super(commit, allowHints, consistencyForConsensus, consistencyForCommit, all, required, newConditionAsConsumer());
            }

            public Status awaitUntil(long deadline)
            {
                try
                {
                    onDone.awaitUntil(deadline);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return new Status(new Paxos.MaybeFailure(true, replicas.size(), required, 0, emptyMap()));
                }

                return status();
            }
        }

        Async async = new Async(commit, allowHints, consistencyForConsensus, consistencyForCommit, all, required);
        async.start(allLive, allDown, isUrgent, false);
        return async;
    }

    /**
     * Submit the proposal for commit with all replicas, and wait synchronously until at most {@code deadline} for the result
     */
    static <T extends Consumer<Status>> T commit(Agreed commit, EndpointsForToken all, EndpointsForToken allLive, EndpointsForToken allDown, int required, boolean isUrgent, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints, T onDone)
    {
        // Check if this is a tracked keyspace requiring forwarding to a replica coordinator
        if (isTrackedKeyspaceRequiringForwarding(commit, all))
        {
            forwardPaxos2Commit(commit, all, allLive, allDown, required, isUrgent, consistencyForConsensus, consistencyForCommit, onDone);
            return onDone;
        }

        new PaxosCommit<>(commit, allowHints, consistencyForConsensus, consistencyForCommit, all, required, onDone)
                .start(allLive, allDown, isUrgent, true);
        return onDone;
    }

    static Paxos.Async<Status> commit(Agreed commit, Participants participants, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints)
    {
        return commit(commit, participants.all, participants.allLive, participants.allDown, 
                     participants.requiredFor(consistencyForCommit), participants.isUrgent(),
                     consistencyForConsensus, consistencyForCommit, allowHints);
    }

    static <T extends Consumer<Status>> T commit(Agreed commit, Participants participants, ConsistencyLevel consistencyForConsensus, ConsistencyLevel consistencyForCommit, /** @deprecated See CASSANDRA-17164 */ @Deprecated(since = "4.1") boolean allowHints, T onDone)
    {
        return commit(commit, participants.all, participants.allLive, participants.allDown, 
                     participants.requiredFor(consistencyForCommit), participants.isUrgent(),
                     consistencyForConsensus, consistencyForCommit, allowHints, onDone);
    }

    /**
     * Send commit messages to peers (or self)
     */
    void start(EndpointsForToken allLive, EndpointsForToken allDown, boolean isUrgent, boolean async)
    {
        Message<Agreed> commitMessage = Message.out(PAXOS_COMMIT_REQ, commit, isUrgent);

        Message<Mutation> mutationMessage = null;
        if (ENABLE_DC_LOCAL_COMMIT && consistencyForConsensus.isDatacenterLocal())
            mutationMessage = Message.out(PAXOS2_COMMIT_REMOTE_REQ, commit.makeMutation(), isUrgent);

        // For tracked keyspaces, the local commit MUST execute synchronously BEFORE sending to remote replicas.
        // This ensures the mutation is written to the journal before any failure callback can trigger
        // reconciliation via ActiveLogReconciler. Without this ordering, a fast remote failure could schedule
        // reconciliation for a mutation that hasn't been journaled yet, causing NullPointerException.
        boolean isTrackedKeyspace = remoteReplicas != null;
        boolean localExecutedSynchronously = false;
        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();

        if (isTrackedKeyspace)
        {
            // For tracked keyspaces, we MUST execute locally synchronously, regardless of USE_SELF_EXECUTION setting.
            // This is critical because retries are scheduled on the local ActiveLogReconciler and look up mutations
            // in the local MutationJournal. If we don't execute synchronously first, a fast remote failure could
            // trigger a retry before the mutation that hasn't been journaled yet, causing NullPointerException.
            //
            // We check BOTH allLive AND allDown because the local endpoint might be incorrectly considered DOWN
            // (e.g., in simulation or during network partition recovery). If local is in allDown, we STILL need
            // to execute locally to write to the journal, but we'll also skip calling onFailure for self below.
            boolean localIsReplica = false;
            for (int i = 0, mi = allLive.size(); i < mi; ++i)
            {
                if (allLive.endpoint(i).equals(localEndpoint))
                {
                    localIsReplica = true;
                    break;
                }
            }
            if (!localIsReplica)
            {
                for (int i = 0, mi = allDown.size(); i < mi; ++i)
                {
                    if (allDown.endpoint(i).equals(localEndpoint))
                    {
                        localIsReplica = true;
                        break;
                    }
                }
            }
            if (localIsReplica)
            {
                executeOnSelf();
                localExecutedSynchronously = true;
            }
        }

        // Now send to remote replicas (and record local execution for non-tracked keyspaces)
        boolean executeOnSelf = false;
        for (int i = 0, mi = allLive.size(); i < mi ; ++i)
        {
            InetAddressAndPort endpoint = allLive.endpoint(i);
            // Skip self if we already executed synchronously for tracked keyspace.
            // Use direct comparison instead of shouldExecuteOnSelf to avoid dependence on USE_SELF_EXECUTION.
            if (localExecutedSynchronously && endpoint.equals(localEndpoint))
                continue;
            executeOnSelf |= isSelfOrSend(commitMessage, mutationMessage, endpoint);
        }

        for (int i = 0, mi = allDown.size(); i < mi ; ++i)
        {
            InetAddressAndPort endpoint = allDown.endpoint(i);
            // Skip self if we already executed synchronously for tracked keyspace.
            // We can't "retry" to self via network anyway, and we've already written to the journal.
            if (localExecutedSynchronously && endpoint.equals(localEndpoint))
                continue;
            onFailure(endpoint, RequestFailure.NODE_DOWN);
        }

        // Tracked if remoteReplicas != null, register write request with tracking service for tracked keyspaces
        if (remoteReplicas != null)
        {
            checkState(!remoteReplicas.isEmpty());
            MutationTrackingService.instance().sentWriteRequest(commit.makeMutation(), remoteReplicas);
        }

        if (executeOnSelf)
        {
            ExecutorPlus executor = PAXOS_COMMIT_REQ.stage.executor();
            if (async) executor.execute(this::executeOnSelf);
            else executor.maybeExecuteImmediately(this::executeOnSelf);
        }
    }

    /**
     * If isLocal return true; otherwise if the destination is alive send our message, and if not mark the callback with failure
     */
    private boolean isSelfOrSend(Message<Agreed> commitMessage, Message<Mutation> mutationMessage, InetAddressAndPort destination)
    {
        if (shouldExecuteOnSelf(destination))
            return true;

        // don't send commits to remote dcs for local_serial operations
        if (mutationMessage != null && !isInLocalDc(destination))
            MessagingService.instance().sendWithCallback(mutationMessage, destination, this);
        else
            MessagingService.instance().sendWithCallback(commitMessage, destination, this);

        return false;
    }

    private static boolean isInLocalDc(InetAddressAndPort destination)
    {
        Locator locator = DatabaseDescriptor.getLocator();
        return locator.local().sameDatacenter(locator.location(destination));
    }

    private boolean isTracked()
    {
        return !commit.mutation.id().equals(MutationId.none());
    }

    /**
     * Record a failure or timeout, and maybe submit a hint to {@code from}
     */
    @Override
    public void onFailure(InetAddressAndPort from, RequestFailure reason)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} {} from {}", commit, reason, from);

        // Track failed response for tracked keyspaces
        if (isTracked())
            MutationTrackingService.instance().retryFailedWrite(commit.mutation.id(), from, reason);

        response(false, from);
        Replica replica = replicas.lookup(from);

        if (allowHints && shouldHint(replica))
            submitHint(commit.makeMutation(), replica, null);
    }

    /**
     * Record a success response
     */
    public void onResponse(Message<NoPayload> response)
    {
        logger.trace("{} Success from {}", commit, response.from());

        // Track successful response for tracked keyspaces 
        // (Local mutations are witnessed from Keyspace.applyInternalTracked)
        if (isTracked())
            MutationTrackingService.instance().receivedWriteResponse(commit.mutation.id(), response.from());

        response(true, response.from());
    }

    /**
     * Execute locally and record response
     */
    public void executeOnSelf()
    {
        if (isTracked())
        {
            // For tracked keyspaces, local execution MUST succeed and write to journal.
            // Use direct execution instead of executeOnSelf to ensure we detect failures.
            NoPayload response = RequestHandler.execute(commit);
            if (response == null)
            {
                throw new IllegalStateException(String.format(
                    "Local execution failed for tracked mutation %s. " +
                    "isInRangeAndShouldProcess returned false but this node is the coordinator for a tracked keyspace. " +
                    "partitionKey=%s, table=%s, localEndpoint=%s",
                    commit.mutation.id(), commit.partitionKey(), commit.metadata().keyspace + "." + commit.metadata().name,
                    FBUtilities.getBroadcastAddressAndPort()));
            }
            onResponse(response, FBUtilities.getBroadcastAddressAndPort());
        }
        else
        {
            executeOnSelf(commit, RequestHandler::execute);
        }
    }

    @Override
    public void onResponse(NoPayload response, InetAddressAndPort from)
    {
        // Track successful response for tracked keyspaces
        if (isTracked())
        {
            if (response != null)
                MutationTrackingService.instance().receivedWriteResponse(commit.mutation.id(), from);
            else
                MutationTrackingService.instance().retryFailedWrite(commit.mutation.id(), from, RequestFailure.UNKNOWN);
        }

        response(response != null, from);
    }

    /**
     * Record a failure or success response if {@code from} contributes to our consistency.
     * If we have reached a final outcome of the commit, run {@code onDone}.
     */
    private void response(boolean success, InetAddressAndPort from)
    {
        if (consistencyForCommit.isDatacenterLocal() && !InOurDc.endpoints().test(from))
            return;

        long responses = responsesUpdater.addAndGet(this, success ? 0x1L : 0x100000000L);
        // next two clauses mutually exclusive to ensure we only invoke onDone once, when either failed or succeeded
        if (accepts(responses) == required) // if we have received _precisely_ the required accepts, we have succeeded
            onDone.accept(status());
        else if (replicas.size() - failures(responses) == required - 1) // if we are _unable_ to receive the required accepts, we have failed
            onDone.accept(status());
    }

    /**
     * @return the Status as of now, which may be final or may indicate we have not received sufficient responses
     */
    Status status()
    {
        long responses = this.responses;
        if (isSuccessful(responses))
            return success;

        return new Status(new Paxos.MaybeFailure(replicas.size(), required, accepts(responses), failureReasonsAsMap()));
    }

    private boolean isSuccessful(long responses)
    {
        return accepts(responses) >= required;
    }

    private static int accepts(long responses)
    {
        return (int) (responses & 0xffffffffL);
    }

    private static int failures(long responses)
    {
        return (int) (responses >>> 32);
    }

    public static class RequestHandler implements IVerbHandler<Agreed>
    {
        @Override
        public void doVerb(Message<Agreed> message)
        {
            NoPayload response = execute(message.payload);
            // NOTE: for correctness, this must be our last action, so that we cannot throw an error and send both a response and a failure response
            if (response == null)
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
            else
                MessagingService.instance().respond(response, message);
        }

        private static NoPayload execute(Agreed agreed)
        {
            if (!Paxos.isInRangeAndShouldProcess(agreed.partitionKey(), agreed.metadata(), false))
                return null;

            PaxosState.commitDirect(agreed);
            Tracing.trace("Enqueuing acknowledge to {}", agreed.ballot);
            return NoPayload.noPayload;
        }
    }

    /**
     * Checks if this commit needs to be forwarded to a replica coordinator for tracked keyspace support.
     */
    private static boolean isTrackedKeyspaceRequiringForwarding(Agreed commit, EndpointsForToken all)
    {
        if (!commit.metadata().replicationType().isTracked())
            return false;
            
        // Check if current coordinator is not a replica
        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();
        boolean isLocalReplica = all.endpoints().contains(localEndpoint);
        return !isLocalReplica;
    }

    /**
     * Forwards a Paxos V2 commit operation to a replica coordinator for tracked keyspaces.
     */
    private static <T extends Consumer<Status>> void forwardPaxos2Commit(Agreed commit,
                                                                         EndpointsForToken all,
                                                                         EndpointsForToken allLive,
                                                                         EndpointsForToken allDown,
                                                                         int required,
                                                                         boolean isUrgent,
                                                                         ConsistencyLevel consistencyForConsensus,
                                                                         ConsistencyLevel consistencyForCommit,
                                                                         T onDone)
    {
        InetAddressAndPort localEndpoint = FBUtilities.getBroadcastAddressAndPort();

        // Filter out local endpoint and sort by proximity to find best replica to forward to
        EndpointsForToken liveReplicasExcludingSelf = allLive.filter(r -> !r.endpoint().equals(localEndpoint));

        if (liveReplicasExcludingSelf.isEmpty())
        {
            // No live replica available to forward to
            logger.debug("No live replicas available to forward Paxos V2 commit for {}", commit.partitionKey());
            Tracing.trace("No live replicas available to forward Paxos V2 commit");
            onDone.accept(new Status(new Paxos.MaybeFailure(true, 1, 1, 0, emptyMap())));
            return;
        }

        // Sort by proximity and select the best coordinator
        EndpointsForToken sortedReplicas = DatabaseDescriptor.getNodeProximity().sortedByProximity(localEndpoint, liveReplicasExcludingSelf);
        InetAddressAndPort replicaCoordinator = sortedReplicas.get(0).endpoint();

        logger.debug("Forwarding Paxos V2 commit for {} to replica coordinator {}", commit.partitionKey(), replicaCoordinator);
        Tracing.trace("Forwarding Paxos V2 commit to replica coordinator {}", replicaCoordinator);

        // Create forward request with extracted participant data
        Paxos2CommitForwardRequest forwardRequest = new Paxos2CommitForwardRequest(commit, consistencyForConsensus, consistencyForCommit,
                                                                                   all, allLive, allDown,
                                                                                   required, isUrgent);
        Message<Paxos2CommitForwardRequest> message = Message.out(Verb.PAXOS2_COMMIT_FORWARD_REQ, forwardRequest);

        // Create callback to handle forwarding response
        RequestCallback<NoPayload> callback = new RequestCallback<NoPayload>()
        {
            @Override
            public void onResponse(Message<NoPayload> response)
            {
                Tracing.trace("Forwarded Paxos V2 commit completed successfully");
                onDone.accept(success);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.debug("Forwarded Paxos V2 commit to {} failed: {}", from, failure);
                Tracing.trace("Forwarded Paxos V2 commit to {} failed: {}", from, failure);
                // Populate the failure map with the actual failure reason; contacted=1, required=1 for forwarded request
                onDone.accept(new Status(new Paxos.MaybeFailure(true, 1, 1, 0,
                                                                java.util.Collections.singletonMap(from, failure.reason))));
            }
        };

        try
        {
            MessagingService.instance().sendWithCallback(message, replicaCoordinator, callback);
        }
        catch (Exception e)
        {
            logger.debug("Failed to send forwarded Paxos V2 commit to {}: {}", replicaCoordinator, e.getMessage());
            Tracing.trace("Failed to send forwarded Paxos V2 commit: {}", e.getMessage());
            onDone.accept(new Status(new Paxos.MaybeFailure(true, 1, 1, 0,
                                                            java.util.Collections.singletonMap(replicaCoordinator, UNKNOWN))));
        }
    }

}
