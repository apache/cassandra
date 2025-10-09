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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
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

    private Message<Request> send;
    private final Callbacks callbacks;
    private final Paxos.Participants participants;
    private final boolean isUrgent;

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
        // Check if forwarding is needed for tracked keyspaces
        Committed commit = send.payload.missingCommit;

        if (commit.metadata().replicationType().isTracked() && commit.mutation.id().isNone())
        {
            // Check if we can generate mutation ID locally (are we a replica?)
            Replica localReplica = participants.all.byEndpoint().get(getBroadcastAddressAndPort());

            if (localReplica != null)
            {
                // We ARE a replica - generate mutation ID and update the commit
                String keyspaceName = commit.metadata().keyspace;
                MutationId mutationId = MutationTrackingService.instance.nextMutationId(keyspaceName, commit.partitionKey().getToken());
                Mutation mutationWithId = commit.makeMutation(mutationId);
                Committed commitWithId = new Commit.Committed(commit.ballot, mutationWithId);

                // Update the message payload with the new commit
                this.send = Message.out(PAXOS2_PREPARE_REFRESH_REQ,
                                        new Request(send.payload.promised, commitWithId),
                                        isUrgent);

                // For tracked keyspaces, we MUST ALWAYS write to the local journal since we generated the mutation ID.
                // This is required for retry purposes: if a remote target fails, the ActiveLogReconciler will try
                // to look up the mutation in the local journal. The node that generated the mutation ID is the "owner"
                // and must have the mutation available for retries.
                //
                // We do this BEFORE the main refresh loop to ensure the mutation is in the journal before any
                // failure callback can trigger reconciliation.
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

                // If self is in the refresh list, report the local execution result to callbacks
                // We need to do this because the main loop will skip self for tracked keyspaces
                for (int i = 0, size = refresh.size(); i < size; ++i)
                {
                    if (shouldExecuteOnSelf(refresh.get(i)))
                    {
                        if (localResponse != null)
                            callbacks.onRefreshSuccess(localResponse.isSupersededBy, getBroadcastAddressAndPort());
                        else
                            callbacks.onRefreshFailure(getBroadcastAddressAndPort(), RequestFailure.UNKNOWN);
                        break;
                    }
                }
            }
            else
            {
                // We're NOT a replica - forward to a replica
                forwardRefresh(refresh);
                return;
            }
        }

        // For tracked keyspaces where we generated the ID above, we already wrote locally.
        // For tracked keyspaces where the ID was already present, we still need to ensure local execution.
        boolean isTracked = !send.payload.missingCommit.mutation.id().isNone();

        // If we just generated the ID above, we already wrote locally - check by examining the original commit
        boolean alreadyWroteLocally = commit.metadata().replicationType().isTracked()
                                      && commit.mutation.id().isNone()
                                      && participants.all.byEndpoint().get(getBroadcastAddressAndPort()) != null;
        boolean localExecutedSync = alreadyWroteLocally;

        // For tracked keyspaces where we DIDN'T generate the ID (it was already present), we still need to
        // execute locally BEFORE sending to remotes if self is in the refresh list.
        if (isTracked && !alreadyWroteLocally)
        {
            for (int i = 0, size = refresh.size(); i < size; ++i)
            {
                if (shouldExecuteOnSelf(refresh.get(i)))
                {
                    executeOnSelf();  // SYNCHRONOUS - journal write completes here
                    localExecutedSync = true;
                    break;
                }
            }
        }

        // Now send to remote nodes (and record local execution for non-tracked keyspaces)
        boolean executeOnSelf = false;
        for (int i = 0, size = refresh.size(); i < size ; ++i)
        {
            InetAddressAndPort destination = refresh.get(i);

            if (logger.isTraceEnabled())
                logger.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit, Ballot.toString(send.payload.promised, "Promise"), destination);

            if (Tracing.isTracing())
                Tracing.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit.ballot, send.payload.promised, destination);

            if (shouldExecuteOnSelf(destination))
            {
                // For tracked keyspaces, skip self - already executed synchronously above
                if (!localExecutedSync)
                    executeOnSelf = true;
            }
            else
            {
                MessagingService.instance().sendWithCallback(send, destination, this);
            }
        }

        // Async local execution only for non-tracked keyspaces
        if (executeOnSelf)
            PAXOS2_PREPARE_REFRESH_REQ.stage.execute(this::executeOnSelf);
    }

    /**
     * Forward the refresh operation to a replica coordinator.
     * The replica will generate the mutation ID and send to all refresh nodes.
     */
    private void forwardRefresh(List<InetAddressAndPort> refreshTargets)
    {
        // Find a live replica to forward to (that's not us)
        InetAddressAndPort targetReplica = null;
        for (Replica replica : participants.all)
        {
            if (!replica.endpoint().equals(getBroadcastAddressAndPort()) &&
                FailureDetector.instance.isAlive(replica.endpoint()))
            {
                targetReplica = replica.endpoint();
                break;
            }
        }

        if (targetReplica == null)
        {
            logger.error("No live replica available to forward PaxosPrepareRefresh for {}",
                         send.payload.missingCommit.partitionKey());
            // Report failure for all refresh targets
            for (InetAddressAndPort target : refreshTargets)
                callbacks.onRefreshFailure(target, RequestFailure.UNKNOWN);
            return;
        }

        logger.debug("Forwarding PaxosPrepareRefresh to replica {} for mutation ID generation", targetReplica);
        Tracing.trace("Forwarding PaxosPrepareRefresh to replica {}", targetReplica);

        // Create forward request with refresh targets
        PrepareRefreshForwardRequest forwardRequest = new PrepareRefreshForwardRequest(
            send.payload.promised,
            send.payload.missingCommit,
            refreshTargets,
            isUrgent
        );

        Message<PrepareRefreshForwardRequest> message = Message.out(
            Verb.PAXOS_PREPARE_REFRESH_FORWARD_REQ, forwardRequest, isUrgent);

        // Send and handle response
        MessagingService.instance().sendWithCallback(message, targetReplica,
            new ForwardCallback(refreshTargets));
    }

    /**
     * Callback for forwarded refresh operations.
     * Translates forward response to individual refresh callbacks.
     */
    private class ForwardCallback implements RequestCallbackWithFailure<PrepareRefreshForwardResponse>
    {
        private final List<InetAddressAndPort> refreshTargets;

        ForwardCallback(List<InetAddressAndPort> refreshTargets)
        {
            this.refreshTargets = refreshTargets;
        }

        @Override
        public void onResponse(Message<PrepareRefreshForwardResponse> message)
        {
            PrepareRefreshForwardResponse response = message.payload;
            // Report results for each target
            for (int i = 0; i < refreshTargets.size(); i++)
            {
                InetAddressAndPort target = refreshTargets.get(i);
                Ballot supersededBy = response.supersededBy.get(i);
                callbacks.onRefreshSuccess(supersededBy, target);
            }
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure reason)
        {
            // Report failure for all targets
            for (InetAddressAndPort target : refreshTargets)
                callbacks.onRefreshFailure(target, reason);
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
                return;
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
            ClusterMetadataService.instance().fetchLogFromPeerOrCMS(ClusterMetadata.current(), message.from(), message.epoch());
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
