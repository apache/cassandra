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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

/**
 * Request sent to peers to resolve the shard metadata (epoch, range, participants)
 * for an unknown {@link CoordinatorLogId}, to resolve the ambiguity of what shard
 * the coordinator log should be placed under.
 * Sent to all peers in parallel; the first non-null response is sufficient.
 */
public final class ShardMetadataRequest
{
    private static final Logger logger = LoggerFactory.getLogger(ShardMetadataRequest.class);

    final CoordinatorLogId logId;

    public ShardMetadataRequest(CoordinatorLogId logId)
    {
        this.logId = logId;
    }

    /**
     * Query all provided peers in parallel for the shard metadata of a coordinator log.
     * Waits for the first peer to reply with a known result.
     */
    public static AsyncPromise<ShardMetadata> queryPeers(CoordinatorLogId logId, Set<InetAddressAndPort> peers)
    {
        if (peers.isEmpty())
            throw new IllegalArgumentException("Empty peers set to query");

        Set<InetAddressAndPort> livePeers = new HashSet<>(peers);
        for (InetAddressAndPort peer : peers)
            if (!FailureDetector.instance.isAlive(peer))
                livePeers.remove(peer);

        if (livePeers.isEmpty())
            throw new RuntimeException("No peers known or alive to retrieve shard metadata from");

        AsyncPromise<ShardMetadata> promise = new AsyncPromise<>();
        RequestCallback<ShardMetadataResponse> callback = new RequestCallback<>()
        {
            // Decremented unconditionally on every response or failure; the promise is completed either
            // early with the first non-null metadata, or with null once every peer has responded.
            private final AtomicInteger remaining = new AtomicInteger(livePeers.size());

            @Override
            public void onResponse(Message<ShardMetadataResponse> msg)
            {
                if (msg.payload.metadata != null)
                    promise.trySuccess(msg.payload.metadata);
                if (remaining.decrementAndGet() == 0)
                    promise.trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                if (remaining.decrementAndGet() == 0)
                    promise.trySuccess(null);
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        };

        Message<ShardMetadataRequest> message = Message.out(Verb.MT_SHARD_METADATA_REQ, new ShardMetadataRequest(logId));
        for (InetAddressAndPort peer : livePeers)
            MessagingService.instance().sendWithCallback(message, peer, callback);

        return promise;
    }

    public static final IVerbHandler<ShardMetadataRequest> verbHandler = message ->
    {
        MutationTrackingService.ensureEnabled();

        ShardMetadataRequest request = message.payload;
        logger.trace("Received shard metadata request from {} for log {}", message.from(), request.logId);

        ShardMetadata metadata = MutationTrackingService.instance().getShardMetadata(request.logId);
        Message<ShardMetadataResponse> response = message.responseWith(new ShardMetadataResponse(metadata));
        MessagingService.instance().send(response, message.from());
    };

    public static final UnversionedSerializer<ShardMetadataRequest> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(ShardMetadataRequest request, DataOutputPlus out) throws IOException
        {
            CoordinatorLogId.serializer.serialize(request.logId, out);
        }

        @Override
        public ShardMetadataRequest deserialize(DataInputPlus in) throws IOException
        {
            return new ShardMetadataRequest(CoordinatorLogId.serializer.deserialize(in));
        }

        @Override
        public long serializedSize(ShardMetadataRequest request)
        {
            return CoordinatorLogId.serializer.serializedSize(request.logId);
        }
    };
}
