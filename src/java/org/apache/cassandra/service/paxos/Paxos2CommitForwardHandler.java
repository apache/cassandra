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
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.ConditionAsConsumer;

import static org.apache.cassandra.utils.concurrent.ConditionAsConsumer.newConditionAsConsumer;

/**
 * Handler for forwarded Paxos V2 commit requests.
 * Executes the commit operation on behalf of the original coordinator,
 * ensuring that MutationId generation happens on a replica coordinator.
 *
 * The PaxosCommit constructor handles mutation ID generation, so this handler
 * simply delegates to PaxosCommit.commit() with the original commit.
 *
 * TODO (expected): more comprehensive testing
 */
public class Paxos2CommitForwardHandler implements IVerbHandler<Paxos2CommitForwardRequest>
{
    public static final Paxos2CommitForwardHandler instance = new Paxos2CommitForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(Paxos2CommitForwardHandler.class);

    @Override
    public void doVerb(Message<Paxos2CommitForwardRequest> message)
    {
        // Ensure we have up-to-date cluster metadata before executing the forwarded commit
        ClusterMetadataService.instance().fetchLogFromPeerOrCMS(message.from(), message.header.epoch);
        Paxos2CommitForwardRequest request = message.payload;

        Tracing.trace("Executing forwarded Paxos V2 commit for {}", request.commit.partitionKey());

        try
        {
            String ksName = request.commit.metadata().keyspace;
            KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(ksName);
            if (ksMetadata == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward paxos commit for non-existent keyspace {}", ksName);
                return;
            }

            if (!ksMetadata.params.replicationType.isTracked())
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Asked to perform forwarded paxos commit, but keyspace {} is not tracked", ksName);
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
                    MessagingService.instance().respondWithFailure(RequestFailureReason.UNKNOWN, message);
                    logger.error("Forwarded Paxos V2 commit failed with status: {}", status);
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
                logger.error("Forwarded Paxos V2 commit interrupted", e);
            }
        }
        catch (Exception e)
        {
            MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
            logger.error("Failed to execute forwarded Paxos V2 commit for {}", request.commit, e);
        }
    }
}
