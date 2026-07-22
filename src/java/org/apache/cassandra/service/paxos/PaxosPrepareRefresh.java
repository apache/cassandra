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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.exceptions.RequestFailureReason.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REFRESH_REQ;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.PaxosRequestCallback.shouldExecuteOnSelf;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

/**
 * Nodes that have promised in response to our prepare, may be missing the latestCommit, meaning we cannot be sure the
 * prior round has been committed to the necessary quorum of participants, so that it will be visible to future quorums.
 *
 * To resolve this problem, we submit the latest commit we have seen, and wait for confirmation before continuing
 * (verifying that we are still promised in the process).
 */
public class PaxosPrepareRefresh implements RequestCallbackWithFailure<PaxosPrepareRefresh.Response>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosPrepareRefresh.class);

    public static final RequestHandler requestHandler = new RequestHandler();
    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final ResponseSerializer responseSerializer = new ResponseSerializer();

    interface Callbacks
    {
        void onRefreshFailure(InetAddressAndPort from, RequestFailure reason);
        void onRefreshSuccess(Ballot isSupersededBy, InetAddressAndPort from);
    }

    private volatile Message<Request> send;
    private final Callbacks callbacks;
    private final Paxos.Participants participants;
    private final boolean isUrgent;
    private boolean selfCallbackDelivered;

    public PaxosPrepareRefresh(Ballot prepared, Paxos.Participants participants, Committed latestCommitted, Callbacks callbacks)
    {
        this.callbacks = callbacks;
        this.participants = participants;
        this.isUrgent = participants.isUrgent();
        this.send = Message.out(PAXOS2_PREPARE_REFRESH_REQ, new Request(prepared, latestCommitted), isUrgent);
    }

    /*
     * onRefreshSuccess can be called from this method synchronously due to local application at this node
     * so the caller must be aware that in PaxosPrepare completion may already have been signaled.
     */
    void refresh(List<InetAddressAndPort> refresh)
    {
        selfCallbackDelivered = false;
        Committed commit = send.payload.missingCommit;
        boolean tracked = MigrationRouter.shouldUseTrackedForWrites(commit.metadata().keyspace,
                                                                    commit.metadata().id,
                                                                    commit.partitionKey().getToken());

        if (tracked && commit.mutation.id().isNone())
        {
            Replica localReplica = participants.all.byEndpoint().get(getBroadcastAddressAndPort());
            if (localReplica == null)
            {
                forwardRefresh(refresh);
                return;
            }
            if (!generateMutationIdAndPersistLocally(commit, refresh))
                return;
        }
        else if (!tracked && !commit.mutation.id().isNone())
        {
            logger.warn("Stripping mutation ID {} from PaxosPrepareRefresh for {}.{} partition {} - keyspace migrated to untracked",
                        commit.mutation.id(), commit.metadata().keyspace, commit.metadata().name, commit.partitionKey());
            Tracing.trace("Stripping mutation ID {} from PaxosPrepareRefresh for {}.{} partition {} - keyspace migrated to untracked",
                          commit.mutation.id(), commit.metadata().keyspace, commit.metadata().name, commit.partitionKey());
            updateSendMessage(commit.withMutationId(MutationId.none()));
        }

        dispatchRefresh(refresh, tracked);
    }

    /**
     * Generates a mutation ID as the local replica, updates the send message, executes locally
     * to persist to the journal (for ID ownership), and reports self's callback if self is a target.
     *
     * @return true if local write succeeded, false if it failed (all target callbacks reported as failure)
     */
    private boolean generateMutationIdAndPersistLocally(Committed commit, List<InetAddressAndPort> refresh)
    {
        String keyspaceName = commit.metadata().keyspace;
        MutationId mutationId = MutationTrackingService.instance().nextMutationId(keyspaceName, commit.partitionKey().getToken());
        updateSendMessage(commit.withMutationId(mutationId));
        selfCallbackDelivered = true;

        Response localResponse = null;
        try
        {
            localResponse = RequestHandler.execute(this.send.payload, getBroadcastAddressAndPort());
            if (localResponse == null)
                logger.warn("Local execution failed for tracked mutation {}", mutationId);
        }
        catch (Exception e)
        {
            logger.warn("Exception writing tracked mutation {} locally", mutationId, e);
        }
        finally
        {
            MutationTrackingService.instance().completeLocalWrite(mutationId);
        }

        if (localResponse == null)
        {
            for (InetAddressAndPort target : refresh)
                callbacks.onRefreshFailure(target, RequestFailure.UNKNOWN);
            return false;
        }

        for (int i = 0, size = refresh.size(); i < size; ++i)
        {
            if (shouldExecuteOnSelf(refresh.get(i)))
            {
                callbacks.onRefreshSuccess(localResponse.isSupersededBy, getBroadcastAddressAndPort());
                break;
            }
        }
        return true;
    }

    private void updateSendMessage(Committed commit)
    {
        this.send = Message.out(PAXOS2_PREPARE_REFRESH_REQ, new Request(send.payload.promised, commit), isUrgent);
    }

    /**
     * Dispatches refresh to all targets. For tracked keyspaces, self is executed synchronously
     * before remotes (unless already handled during ID generation). For untracked keyspaces,
     * self is scheduled for async execution after remotes.
     */
    private void dispatchRefresh(List<InetAddressAndPort> refresh, boolean tracked)
    {
        if (tracked && !selfCallbackDelivered)
            executeSelfSynchronously(refresh);

        boolean selfInList = false;
        for (int i = 0, size = refresh.size(); i < size; ++i)
        {
            InetAddressAndPort destination = refresh.get(i);

            if (shouldExecuteOnSelf(destination))
            {
                selfInList = true;
                continue;
            }

            if (logger.isTraceEnabled())
                logger.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit, Ballot.toString(send.payload.promised, "Promise"), destination);

            if (Tracing.isTracing())
                Tracing.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit.ballot, send.payload.promised, destination);

            MessagingService.instance().sendWithCallback(send, destination, this);
        }

        if (!tracked && selfInList)
            PAXOS2_PREPARE_REFRESH_REQ.stage.execute(this::executeOnSelf);
    }

    private void executeSelfSynchronously(List<InetAddressAndPort> refresh)
    {
        for (int i = 0, size = refresh.size(); i < size; ++i)
        {
            if (shouldExecuteOnSelf(refresh.get(i)))
            {
                executeOnSelf();
                break;
            }
        }
    }

    /**
     * Forward the refresh operation to a replica coordinator.
     * The replica will generate (or find) the mutation ID and send the refresh to all target nodes.
     */
    private void forwardRefresh(List<InetAddressAndPort> refreshTargets)
    {
        InetAddressAndPort localEndpoint = getBroadcastAddressAndPort();
        EndpointsForToken liveExcludingSelf = participants.allLive.filter(replica -> !replica.endpoint().equals(localEndpoint));
        InetAddressAndPort targetReplica = liveExcludingSelf.isEmpty()
                                         ? null
                                         : DatabaseDescriptor.getNodeProximity().sortedByProximity(localEndpoint, liveExcludingSelf).get(0).endpoint();

        if (targetReplica == null)
        {
            logger.error("No live replica available to forward PaxosPrepareRefresh for {}.{} partition {}",
                         send.payload.missingCommit.metadata().keyspace, send.payload.missingCommit.metadata().name,
                         send.payload.missingCommit.partitionKey());
            for (InetAddressAndPort target : refreshTargets)
                callbacks.onRefreshFailure(target, RequestFailure.UNKNOWN);
            return;
        }

        logger.debug("Forwarding PaxosPrepareRefresh to replica {} for mutation ID generation", targetReplica);
        Tracing.trace("Forwarding PaxosPrepareRefresh to replica {}", targetReplica);

        ImmutableList<InetAddressAndPort> immutableRefreshTargets = ImmutableList.copyOf(refreshTargets);
        PrepareRefreshForwardRequest forwardRequest = new PrepareRefreshForwardRequest(send.payload.promised,
                                                                                       send.payload.missingCommit,
                                                                                       immutableRefreshTargets,
                                                                                       isUrgent);

        Message<PrepareRefreshForwardRequest> message = Message.out(Verb.PAXOS_PREPARE_REFRESH_FORWARD_REQ,
                                                                    forwardRequest, isUrgent);

        MessagingService.instance().sendWithCallback(message, targetReplica,
                                                     new ForwardCallback(immutableRefreshTargets));
    }

    /**
     * Callback for forwarded refresh operations.
     * Receives multiple onResponse() calls (non-final + final) as the forward handler
     * streams per-target results back incrementally.
     * Caches the mutation ID from the first response so subsequent refresh() calls
     * can dispatch directly without forwarding.
     */
    private class ForwardCallback implements RequestCallbackWithFailure<PrepareRefreshForwardResponse>
    {
        private final List<InetAddressAndPort> refreshTargets;
        private final boolean[] reported;

        ForwardCallback(List<InetAddressAndPort> refreshTargets)
        {
            this.refreshTargets = refreshTargets;
            this.reported = new boolean[refreshTargets.size()];
        }

        @Override
        public synchronized void onResponse(Message<PrepareRefreshForwardResponse> message)
        {
            PrepareRefreshForwardResponse response = message.payload;

            Message<Request> currentSend = send;
            if (!response.mutationId.isNone() && currentSend.payload.missingCommit.mutation.id().isNone())
                updateSendMessage(currentSend.payload.missingCommit.withMutationId(response.mutationId));

            if (response.targetIndex != null && !reported[response.targetIndex])
            {
                reported[response.targetIndex] = true;
                InetAddressAndPort target = refreshTargets.get(response.targetIndex);
                if (PrepareRefreshForwardHandler.FAILED_SENTINEL.equals(response.supersededBy))
                    callbacks.onRefreshFailure(target, RequestFailure.UNKNOWN);
                else
                    callbacks.onRefreshSuccess(response.supersededBy, target);
            }
        }

        @Override
        public synchronized void onFailure(InetAddressAndPort from, RequestFailure reason)
        {
            for (int i = 0; i < refreshTargets.size(); i++)
            {
                if (!reported[i])
                {
                    reported[i] = true;
                    callbacks.onRefreshFailure(refreshTargets.get(i), reason);
                }
            }
        }
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailure reason)
    {
        callbacks.onRefreshFailure(from, reason);
    }

    @Override
    public void onResponse(Message<Response> message)
    {
        ClusterMetadataService.instance().fetchLogFromPeerOrCMS(ClusterMetadata.current(), message.from(), message.epoch());
        onResponse(message.payload, message.from());
    }

    private void executeOnSelf()
    {
        Response response;
        try
        {
            response = RequestHandler.execute(send.payload, getBroadcastAddressAndPort());
            if (response == null)
            {
                onFailure(getBroadcastAddressAndPort(), RequestFailure.UNKNOWN);
                return;
            }
        }
        catch (RetryOnDifferentSystemException e)
        {
            onFailure(getBroadcastAddressAndPort(), RequestFailure.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM);
            return;
        }
        catch (Exception ex)
        {
            RequestFailure reason = RequestFailure.UNKNOWN;
            if (ex instanceof WriteTimeoutException) reason = RequestFailure.TIMEOUT;
            else logger.error("Failed to apply paxos refresh-prepare locally", ex);

            onFailure(getBroadcastAddressAndPort(), reason);
            return;
        }
        onResponse(response, getBroadcastAddressAndPort());
    }

    private void onResponse(Response response, InetAddressAndPort from)
    {
        callbacks.onRefreshSuccess(response.isSupersededBy, from);
    }

    static class Request
    {
        final Ballot promised;
        final Committed missingCommit;

        Request(Ballot promised, Committed missingCommit)
        {
            this.promised = promised;
            this.missingCommit = missingCommit;
        }
    }

    static class Response
    {
        final Ballot isSupersededBy;
        Response(Ballot isSupersededBy)
        {
            this.isSupersededBy = isSupersededBy;
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(Message<Request> message)
        {
            ClusterMetadata metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(ClusterMetadata.current(),
                                                                                              message.from(),
                                                                                              message.epoch());

            Committed commit = message.payload.missingCommit;
            MigrationRouter.checkPaxosCommitMigration(metadata, message, message.from(),
                                                      commit.metadata().keyspace, commit.metadata().id,
                                                      commit.partitionKey().getToken(),
                                                      !commit.mutation.id().isNone());

            try
            {
                Response response = execute(message.payload, message.from());
                if (response == null)
                    MessagingService.instance().respondWithFailure(RequestFailureReason.UNKNOWN, message);
                else
                    MessagingService.instance().respond(response, message);
            }
            catch (RetryOnDifferentSystemException e)
            {
                MessagingService.instance().respondWithFailure(RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM, message);
            }
        }

        public static Response execute(Request request, InetAddressAndPort from)
        {
            Agreed commit = request.missingCommit;

            if (!Paxos.isInRangeAndShouldProcess(commit.partitionKey(), commit.metadata(), false))
                return null;

            try (PaxosState state = PaxosState.get(commit))
            {
                state.commit(commit);
                Ballot latest = state.current(request.promised).latestWitnessedOrLowBound();
                if (isAfter(latest, request.promised))
                {
                    Tracing.trace("Promise {} rescinded; latest is now {}", request.promised, latest);
                    return new Response(latest);
                }
                else
                {
                    Tracing.trace("Promise confirmed for ballot {}", request.promised);
                    return new Response(null);
                }
            }
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            request.promised.serialize(out);
            Committed.serializer.serialize(request.missingCommit, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Ballot promise = Ballot.deserialize(in);
            Committed missingCommit = Committed.serializer.deserialize(in, version);
            return new Request(promise, missingCommit);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Ballot.sizeInBytes()
                   + Committed.serializer.serializedSize(request.missingCommit, version);
        }
    }

    public static class ResponseSerializer implements IVersionedSerializer<Response>
    {
        public void serialize(Response response, DataOutputPlus out, int version) throws IOException
        {
            serializeNullable(response.isSupersededBy, out, version, Ballot.Serializer.instance);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            Ballot isSupersededBy = deserializeNullable(in, version, Ballot.Serializer.instance);
            return new Response(isSupersededBy);
        }

        public long serializedSize(Response response, int version)
        {
            return serializedNullableSize(response.isSupersededBy, version, Ballot.Serializer.instance);
        }
    }

}
