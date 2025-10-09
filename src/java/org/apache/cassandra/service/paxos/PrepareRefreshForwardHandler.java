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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REFRESH_REQ;
import static org.apache.cassandra.service.paxos.PaxosRequestCallback.shouldExecuteOnSelf;

/**
 * Handler for forwarded PaxosPrepareRefresh requests.
 * Generates a mutation ID and sends the refresh to all target nodes.
 *
 * This handler is invoked when a non-replica coordinator forwards the refresh
 * to a full replica that can generate the mutation ID.
 *
 * TODO (expected): more comprehensive testing
 */
public class PrepareRefreshForwardHandler implements IVerbHandler<PrepareRefreshForwardRequest>
{
    public static final PrepareRefreshForwardHandler instance = new PrepareRefreshForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(PrepareRefreshForwardHandler.class);

    @Override
    public void doVerb(Message<PrepareRefreshForwardRequest> message)
    {
        ClusterMetadataService.instance().fetchLogFromPeerOrCMS(message.from(), message.header.epoch);
        PrepareRefreshForwardRequest request = message.payload;

        Tracing.trace("Executing forwarded PaxosPrepareRefresh for {}", request.commit.partitionKey());

        try
        {
            String ksName = request.commit.metadata().keyspace;
            KeyspaceMetadata ksMetadata = Schema.instance.getKeyspaceMetadata(ksName);
            if (ksMetadata == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward paxos prepare refresh for non-existent keyspace {}", ksName);
                return;
            }

            if (!ksMetadata.params.replicationType.isTracked())
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Asked to perform forwarded prepare refresh, but keyspace {} is not tracked", ksName);
                return;
            }

            Token token = request.commit.partitionKey().getToken();
            MutationId mutationId = MutationTrackingService.instance.nextMutationId(ksName, token);

            Mutation mutationWithId = request.commit.makeMutation(mutationId);
            Committed commitWithId = new Commit.Committed(request.commit.ballot, mutationWithId);

            // Now send the refresh to all targets and collect responses
            List<InetAddressAndPort> targets = request.refreshTargets;
            List<Ballot> supersededBy = Collections.synchronizedList(new ArrayList<>(Collections.nCopies(targets.size(), null)));
            CountDownLatch latch = CountDownLatch.newCountDownLatch(targets.size());

            Message<PaxosPrepareRefresh.Request> refreshMsg = Message.out(
                PAXOS2_PREPARE_REFRESH_REQ,
                new PaxosPrepareRefresh.Request(request.promised, commitWithId),
                request.isUrgent
            );

            // For tracked keyspaces, we MUST ALWAYS write to the local journal since we generated the mutation ID.
            // This is required for retry purposes: if a remote target fails, the ActiveLogReconciler will try
            // to look up the mutation in the local journal. The node that generated the mutation ID is the "owner"
            // and must have the mutation available for retries.
            //
            // This is different from checking if self is in targets - even if we're not in targets,
            // we're still the ID generator and need the mutation locally.
            try
            {
                PaxosPrepareRefresh.RequestHandler.execute(
                    new PaxosPrepareRefresh.Request(request.promised, commitWithId), FBUtilities.getBroadcastAddressAndPort());
                // Note: we don't use the response since this node may not be in targets
            }
            catch (Exception e)
            {
                // Log but continue - we still need to send to targets
                logger.warn("Failed to execute local commit for tracked keyspace mutation {}", mutationId, e);
            }

            // Now send to remote targets
            for (int i = 0; i < targets.size(); i++)
            {
                final int targetIndex = i;
                InetAddressAndPort target = targets.get(i);

                // Check if self is in targets for response tracking (separate from the local write above)
                // We need to decrement the latch for the local target since we already executed above
                if (shouldExecuteOnSelf(target))
                {
                    latch.decrement();
                    continue;
                }

                RequestCallbackWithFailure<PaxosPrepareRefresh.Response> callback = new RequestCallbackWithFailure<>()
                {
                    @Override
                    public void onResponse(Message<PaxosPrepareRefresh.Response> response)
                    {
                        supersededBy.set(targetIndex, response.payload.isSupersededBy);
                        latch.decrement();
                    }

                    @Override
                    public void onFailure(InetAddressAndPort from, RequestFailure reason)
                    {
                        // Leave null to indicate we didn't get a definitive answer
                        latch.decrement();
                    }
                };

                MessagingService.instance().sendWithCallback(refreshMsg, target, callback);
            }

            // Wait for all responses with timeout
            long timeoutNanos = message.expiresAtNanos() - Clock.Global.nanoTime();
            boolean completed = latch.await(Math.max(0, timeoutNanos), TimeUnit.NANOSECONDS);

            if (!completed)
                logger.warn("Forwarded PaxosPrepareRefresh timed out waiting for responses");

            // Send aggregated response back to original coordinator
            MessagingService.instance().respond(new PrepareRefreshForwardResponse(supersededBy), message);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
            logger.error("Forwarded PaxosPrepareRefresh interrupted", e);
        }
        catch (Exception e)
        {
            MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
            logger.error("Failed to execute forwarded PaxosPrepareRefresh for {}", request.commit, e);
        }
    }
}
