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
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;

/**
 * Handler for forwarded Paxos V1 commit requests.
 * Routes the commit to the tracked or untracked path based on the current
 * migration state of the keyspace for the affected partition.
 *
 * TODO (expected): more comprehensive testing
 * TODO: should loop on CoordinatorBehindException rather than propagating failure to the forwarding coordinator
 */
public class PaxosCommitForwardHandler implements IVerbHandler<PaxosCommitForwardRequest>
{
    public static final PaxosCommitForwardHandler instance = new PaxosCommitForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommitForwardHandler.class);

    @Override
    public void doVerb(Message<PaxosCommitForwardRequest> message)
    {
        PaxosCommitForwardRequest request = message.payload;
        Commit proposal = request.proposal;

        Tracing.trace("Executing forwarded Paxos commit for {}", proposal.partitionKey());

        try
        {
            String ksName = proposal.metadata().keyspace;
            Keyspace keyspace = Keyspace.openIfExists(ksName);
            if (keyspace == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward paxos commit for non-existent keyspace {}.{} partition {}",
                             ksName, proposal.metadata().name, proposal.partitionKey());
                return;
            }

            ClusterMetadata metadata = ClusterMetadata.current();
            boolean shouldBeTracked = MigrationRouter.shouldUseTrackedForWrites(metadata,
                                                                                ksName,
                                                                                proposal.metadata().id,
                                                                                proposal.partitionKey().getToken());

            if (!shouldBeTracked && message.epoch().isAfter(metadata.epoch))
            {
                metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
                shouldBeTracked = MigrationRouter.shouldUseTrackedForWrites(metadata,
                                                                            ksName,
                                                                            proposal.metadata().id,
                                                                            proposal.partitionKey().getToken());
            }

            if (shouldBeTracked)
            {
                StorageProxy.commitPaxosTracked(keyspace, proposal, request.consistencyLevel,
                                                Dispatcher.RequestTime.forImmediateExecution(), request.respondAfterSend);
            }
            else
            {
                // respondAfterSend is not propagated here — commitPaxosUntracked always blocks.
                // During migration races this adds latency but doesn't affect correctness.
                Commit reconciled = proposal;
                if (!proposal.mutation.id().isNone())
                {
                    logger.warn("Stripping mutation ID {} from forwarded PaxosCommit for {}.{} partition {} - keyspace is untracked at handler",
                                proposal.mutation.id(), ksName, proposal.metadata().name, proposal.partitionKey());
                    Tracing.trace("Stripping mutation ID {} from forwarded PaxosCommit for {}.{} partition {} - keyspace is untracked at handler",
                                  proposal.mutation.id(), ksName, proposal.metadata().name, proposal.partitionKey());
                    reconciled = proposal.withMutationId(MutationId.none());
                }
                StorageProxy.commitPaxosUntracked(keyspace, reconciled, request.consistencyLevel,
                                                  true, Dispatcher.RequestTime.forImmediateExecution());
            }

            MessagingService.instance().respond(NoPayload.noPayload, message);
        }
        catch (Exception e)
        {
            MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
            logger.error("Failed to execute forwarded paxos commit for {}", request.proposal, e);
        }
    }
}
