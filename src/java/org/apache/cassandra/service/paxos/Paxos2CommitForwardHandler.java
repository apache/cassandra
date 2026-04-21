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

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static org.apache.cassandra.utils.concurrent.ConditionAsConsumer.newConditionAsConsumer;

/**
 * Handler for forwarded Paxos V2 commit requests.
 * Delegates to PaxosCommit.commit() which handles mutation ID generation
 * in its constructor.
 *
 * TODO (expected): more comprehensive testing
 * TODO: should loop on CoordinatorBehindException rather than propagating failure to the forwarding coordinator
 */
public class Paxos2CommitForwardHandler implements IVerbHandler<Paxos2CommitForwardRequest>
{
    public static final Paxos2CommitForwardHandler instance = new Paxos2CommitForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(Paxos2CommitForwardHandler.class);

    @Override
    public void doVerb(Message<Paxos2CommitForwardRequest> message)
    {
        Paxos2CommitForwardRequest request = message.payload;
        Commit.Agreed commit = request.commit;

        Tracing.trace("Executing forwarded Paxos V2 commit for {}.{} partition {}",
                      commit.metadata().keyspace, commit.metadata().name, commit.partitionKey());

        try
        {
            String ksName = commit.metadata().keyspace;
            ClusterMetadata metadata = ClusterMetadata.current();
            boolean shouldBeTracked = MigrationRouter.shouldUseTrackedForWrites(metadata,
                                                                                ksName,
                                                                                commit.metadata().id,
                                                                                commit.partitionKey().getToken());

            if (!shouldBeTracked && message.epoch().isAfter(metadata.epoch))
            {
                metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, message.from(), message.epoch());
                // shouldBeTracked isn't used after this, but is kept up to date just in case
                shouldBeTracked = MigrationRouter.shouldUseTrackedForWrites(metadata,
                                                                            ksName,
                                                                            commit.metadata().id,
                                                                            commit.partitionKey().getToken());
            }

            if (metadata.schema.getKeyspaces().getNullable(ksName) == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward paxos commit for non-existent keyspace {}.{} partition {}",
                             ksName, commit.metadata().name, commit.partitionKey());
                Tracing.trace("Failed to forward paxos commit for non-existent keyspace {}.{} partition {}",
                              ksName, commit.metadata().name, commit.partitionKey());
                return;
            }

            // Execute the commit operation - PaxosCommit constructor handles mutation ID generation
            ConditionAsConsumer<PaxosCommit.Status> onDone = newConditionAsConsumer();
            PaxosCommit.Status[] statusHolder = new PaxosCommit.Status[1];

            Consumer<PaxosCommit.Status> statusCapture = status -> {
                statusHolder[0] = status;
                onDone.accept(status);
            };

            // Delegate to PaxosCommit.commit() - it will generate mutation ID in constructor
            PaxosCommit.commit(request.commit,
                               request.all,
                               request.allLive,
                               request.allDown,
                               request.required,
                               request.isUrgent,
                               request.consistencyForConsensus,
                               request.consistencyForCommit,
                               false, // allowHints
                               statusCapture);

            // Wait for completion
            try
            {
                onDone.awaitUntil(message.expiresAtNanos());
                PaxosCommit.Status status = statusHolder[0];

                if (status != null && status.isSuccess())
                {
                    MessagingService.instance().respond(NoPayload.noPayload, message);
                }
                else
                {
                    RequestFailureReason reason = RequestFailureReason.UNKNOWN;
                    if (status != null && status.maybeFailure() != null)
                    {
                        for (RequestFailureReason r : status.maybeFailure().failures.values())
                        {
                            if (r == RequestFailureReason.COORDINATOR_BEHIND)
                            {
                                reason = RequestFailureReason.COORDINATOR_BEHIND;
                                break;
                            }
                        }
                    }
                    MessagingService.instance().respondWithFailure(reason, message);
                    logger.error("Forwarded Paxos V2 commit failed with status: {}", status);
                    Tracing.trace("Forwarded Paxos V2 commit failed with status: {}", status);
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
                logger.error("Forwarded Paxos V2 commit interrupted", e);
                Tracing.trace("Forwarded Paxos V2 commit interrupted");
            }
        }
        catch (Exception e)
        {
            MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
            logger.error("Failed to execute forwarded Paxos V2 commit for {}", request.commit, e);
        }
    }
}
