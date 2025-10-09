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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;

/**
 * Handler for forwarded Paxos V1 commit requests.
 * Executes the commit operation on behalf of the original coordinator,
 * ensuring that MutationId generation happens on a replica coordinator.
 *
 * TODO (expected): more comprehensive testing
 */
public class PaxosCommitForwardHandler implements IVerbHandler<PaxosCommitForwardRequest>
{
    public static final PaxosCommitForwardHandler instance = new PaxosCommitForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommitForwardHandler.class);

    @Override
    public void doVerb(Message<PaxosCommitForwardRequest> message)
    {
        // PaxosV1 when doing commit picks whatever the current replicas are to send the commits to
        // so make sure we at least match what they would have picked
        ClusterMetadataService.instance().fetchLogFromPeerOrCMS(message.from(), message.header.epoch);
        PaxosCommitForwardRequest request = message.payload;

        Tracing.trace("Executing forwarded Paxos commit for {}", request.proposal.partitionKey());

        try
        {
            String ksName = request.proposal.metadata().keyspace;
            Keyspace keyspace = Keyspace.openIfExists(ksName);
            if (keyspace == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward paxos commit for non-existent keyspace {}", ksName);
                return;
            }

            if (!keyspace.getMetadata().params.replicationType.isTracked())
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Asked to perform forwarded paxos commit, but keyspace {} is not tracked", ksName);
                return;
            }

            // Call commitPaxosTracked which handles mutation ID generation, sending to all replicas,
            // and tracking. The respondAfterSend flag determines if we wait for application.
            StorageProxy.commitPaxosTracked(keyspace, request.proposal, request.consistencyLevel,
                Dispatcher.RequestTime.forImmediateExecution(), request.respondAfterSend);

            MessagingService.instance().respond(NoPayload.noPayload, message);
        }
        catch (Exception e)
        {
            MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
            logger.error("Failed to execute forwarded paxos commit for {}", request.proposal, e);
        }
    }
}
