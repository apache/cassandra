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

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.exceptions.RequestFailureReason.TIMEOUT;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REFRESH_REQ;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.PaxosRequestCallback.shouldExecuteOnSelf;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

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
        void onRefreshFailure(InetAddressAndPort from, RequestFailureReason reason);
        void onRefreshSuccess(Ballot isSupersededBy, InetAddressAndPort from);
    }

    private final Message<Request> send;
    private final Callbacks callbacks;

    public PaxosPrepareRefresh(Ballot prepared, Paxos.Participants participants, Committed latestCommitted, Callbacks callbacks)
    {
        this.callbacks = callbacks;
        this.send = Message.out(PAXOS2_PREPARE_REFRESH_REQ, new Request(prepared, latestCommitted));
    }

    void refresh(List<InetAddressAndPort> refresh)
    {
        boolean executeOnSelf = false;
        for (int i = 0, size = refresh.size(); i < size ; ++i)
        {
            InetAddressAndPort destination = refresh.get(i);

            if (logger.isTraceEnabled())
                logger.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit, Ballot.toString(send.payload.promised, "Promise"), destination);

            if (Tracing.isTracing())
                Tracing.trace("Refresh {} and Confirm {} to {}", send.payload.missingCommit.ballot, send.payload.promised, destination);

            if (shouldExecuteOnSelf(destination))
                executeOnSelf = true;
            else
                MessagingService.instance().sendWithCallback(send, destination, this);
        }

        if (executeOnSelf)
            PAXOS2_PREPARE_REFRESH_REQ.stage.execute(this::executeOnSelf);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        callbacks.onRefreshFailure(from, reason);
    }

    @Override
    public void onResponse(Message<Response> message)
    {
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
        catch (Exception ex)
        {
            RequestFailureReason reason = UNKNOWN;
            if (ex instanceof WriteTimeoutException) reason = TIMEOUT;
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

    private static class Request
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
            Response response = execute(message.payload, message.from());
            if (response == null)
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
            else
                MessagingService.instance().respond(response, message);
        }

        public static Response execute(Request request, InetAddressAndPort from)
        {
            Agreed commit = request.missingCommit;

            if (!Paxos.isInRangeAndShouldProcess(from, commit.update.partitionKey(), commit.update.metadata(), false))
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
            serializeNullable(Ballot.Serializer.instance, response.isSupersededBy, out, version);
        }

        public Response deserialize(DataInputPlus in, int version) throws IOException
        {
            Ballot isSupersededBy = deserializeNullable(Ballot.Serializer.instance, in, version);
            return new Response(isSupersededBy);
        }

        public long serializedSize(Response response, int version)
        {
            return serializedSizeNullable(Ballot.Serializer.instance, response.isSupersededBy, version);
        }
    }

}
