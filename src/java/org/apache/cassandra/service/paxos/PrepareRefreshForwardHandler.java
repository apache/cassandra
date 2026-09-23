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

import java.util.List;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.service.paxos.Commit.Committed;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REFRESH_REQ;
import static org.apache.cassandra.service.paxos.PaxosRequestCallback.shouldExecuteOnSelf;

/**
 * Handler for forwarded PaxosPrepareRefresh requests.
 * Generates a mutation ID and sends the refresh to all target nodes,
 * streaming per-target results back to the coordinator incrementally
 * using the NOT_FINAL message flag pattern.
 */
public class PrepareRefreshForwardHandler implements IVerbHandler<PrepareRefreshForwardRequest>
{
    public static final PrepareRefreshForwardHandler instance = new PrepareRefreshForwardHandler();
    private static final Logger logger = LoggerFactory.getLogger(PrepareRefreshForwardHandler.class);
    static final Ballot FAILED_SENTINEL = Ballot.none();

    @Override
    public void doVerb(Message<PrepareRefreshForwardRequest> message)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        PrepareRefreshForwardRequest request = message.payload;
        Tracing.trace("Executing forwarded PaxosPrepareRefresh for {}", request.commit.partitionKey());

        try
        {
            String ksName = request.commit.metadata().keyspace;
            Token token = request.commit.partitionKey().getToken();

            if (metadata.schema.getKeyspaces().getNullable(ksName) == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA, message);
                logger.error("Failed to forward prepare refresh for non-existent keyspace {}.{} partition {}",
                             ksName, request.commit.metadata().name, request.commit.partitionKey());
                return;
            }

            MigrationRouter.checkPaxosCommitMigration(metadata, message, message.from(),
                                                      ksName, request.commit.metadata().id, token, true);

            MutationId mutationId = MutationTrackingService.instance().nextMutationId(ksName, token);

            Mutation mutationWithId = request.commit.makeMutation(mutationId);
            Committed commitWithId = new Commit.Committed(request.commit.ballot, mutationWithId);

            // For tracked keyspaces, we MUST ALWAYS write to the local journal since we generated the mutation ID.
            // This is required for retry purposes: if a remote target fails, the ActiveLogReconciler will try
            // to look up the mutation in the local journal. The node that generated the mutation ID is the "owner"
            // and must have the mutation available for retries.
            PaxosPrepareRefresh.Response localResponse = null;
            try
            {
                localResponse = PaxosPrepareRefresh.RequestHandler.execute(new PaxosPrepareRefresh.Request(request.promised, commitWithId),
                                                                           FBUtilities.getBroadcastAddressAndPort());
            }
            catch (Exception e)
            {
                logger.warn("Failed to execute local commit for tracked keyspace mutation {}", mutationId, e);
            }
            finally
            {
                MutationTrackingService.instance().completeLocalWrite(mutationId);
            }

            if (localResponse == null)
            {
                MessagingService.instance().respondWithFailure(RequestFailureReason.UNKNOWN, message);
                logger.error("Aborting forwarded PaxosPrepareRefresh: local journal write failed for mutation {}", mutationId);
                return;
            }

            List<InetAddressAndPort> targets = request.refreshTargets;
            int[] remaining = new int[] { targets.size() };

            Message<PaxosPrepareRefresh.Request> refreshMsg = Message.out(PAXOS2_PREPARE_REFRESH_REQ,
                                                                          new PaxosPrepareRefresh.Request(request.promised, commitWithId),
                                                                          request.isUrgent);

            for (int i = 0; i < targets.size(); i++)
            {
                final int targetIndex = i;
                InetAddressAndPort target = targets.get(i);

                if (shouldExecuteOnSelf(target))
                {
                    respond(message, mutationId, targetIndex, localResponse.isSupersededBy, remaining);
                    continue;
                }

                RequestCallbackWithFailure<PaxosPrepareRefresh.Response> callback = new RequestCallbackWithFailure<>()
                {
                    @Override
                    public void onResponse(Message<PaxosPrepareRefresh.Response> response)
                    {
                        respond(message, mutationId, targetIndex, response.payload.isSupersededBy, remaining);
                    }

                    @Override
                    public void onFailure(InetAddressAndPort from, RequestFailure reason)
                    {
                        respond(message, mutationId, targetIndex, FAILED_SENTINEL, remaining);
                    }
                };

                MessagingService.instance().sendWithCallback(refreshMsg, target, callback);
            }
        }
        catch (Exception e)
        {
            MessagingService.instance().respondWithFailure(RequestFailure.forException(e), message);
            logger.error("Failed to execute forwarded PaxosPrepareRefresh for {}", request.commit, e);
        }
    }

    private void respond(Message<?> request, MutationId mutationId, int targetIndex,
                         @Nullable Ballot supersededBy, int[] remaining)
    {
        PrepareRefreshForwardResponse payload = new PrepareRefreshForwardResponse(mutationId, targetIndex, supersededBy);
        Message<PrepareRefreshForwardResponse> response = request.responseWith(payload);
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (remaining)
        {
            boolean isFinal = --remaining[0] == 0;
            if (!isFinal)
                response = response.withFlag(MessageFlag.NOT_FINAL);
            MessagingService.instance().send(response, request.respondTo());
        }
    }
}
