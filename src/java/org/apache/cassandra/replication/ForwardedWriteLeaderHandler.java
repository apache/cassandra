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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

// Leader just needs to acknowledge propagation for its own log, not for client consistency level
// See org.apache.cassandra.service.TrackedWriteResponseHandler.onResponse, this class should probably merge with that one
public class ForwardedWriteLeaderHandler implements RequestCallback<NoPayload>
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedWriteLeaderHandler.class);

    private final String keyspace;
    private final Token token;
    private final MutationId id;
    private final ForwardedWriteRequest.DirectAcknowledgementInfo ackTo;
    private final Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();

    public ForwardedWriteLeaderHandler(String keyspace, Token token, MutationId id, ForwardedWriteRequest.DirectAcknowledgementInfo ackTo)
    {
        this.keyspace = keyspace;
        this.token = token;
        this.id = id;
        this.ackTo = ackTo;
    }

    @Override
    public void onResponse(Message<NoPayload> msg)
    {
        // Local mutations are witnessed from Keyspace.applyInternalTracked
        if (msg != null)
            MutationTrackingService.instance.witnessedRemoteMutation(keyspace, token, id, msg.from());

        // Local write needs to be ack'd to coordinator
        if (msg == null && ackTo != null)
        {
            Message<NoPayload> message = Message.builder(Verb.MUTATION_RSP, NoPayload.noPayload)
                                                .from(FBUtilities.getBroadcastAddressAndPort())
                                                .withId(ackTo.id)
                                                .build();
            MessagingService.instance().send(message, ackTo.coordinator);
        }
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        logger.error("Got failure from {} reason {}", from, failureReason);
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    public Dispatcher.RequestTime getRequestTime()
    {
        return requestTime;
    }
}
