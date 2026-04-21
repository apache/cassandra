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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmbeddableSinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.KeyMigrationState;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.PaxosPrepare.Rejected;
import org.apache.cassandra.service.paxos.PaxosPrepare.Response;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.service.reads.tracked.TrackedRead.Id;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher.RequestTime;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static com.google.common.util.concurrent.Futures.getUnchecked;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_COMMIT_AND_PREPARE_REQ;
import static org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.getKeyMigrationState;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
import static org.apache.cassandra.service.paxos.PaxosPrepare.start;

public class PaxosCommitAndPrepare
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosCommitAndPrepare.class);

    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final RequestHandler requestHandler = new RequestHandler();

    static PaxosPrepare commitAndPrepare(Agreed commit, Paxos.Participants participants, SinglePartitionReadCommand readCommand, boolean isWrite, boolean acceptEarlyReadSuccess)
    {
        Ballot ballot = newBallot(commit.ballot, participants.consistencyForConsensus);

        Tracing.trace("Committing {}; Preparing {}", commit.ballot, ballot);
        /*
         * For simplicity with tracked keyspaces do the commit as a regular commit synchronously and then separately do a regular prepare.
         * CommitAndPrepare goes down the prepare path with a message containing the commit along with the prepare
         * which means this node is the coordinator and would need to either re-use the original commit mutation id
         * (which wasn't saved in the system table) or generate a new one which it might not be able to do without forwarding.
         *
         * All these things are tractable to do better, but for now doing something simple and correct.
         */
        boolean shouldBeTracked = MigrationRouter.shouldUseTrackedForWrites(commit.metadata().keyspace,
                                                                            commit.metadata().id,
                                                                            commit.partitionKey().getToken());

        // Reconcile the mutation's ID with the current migration state.
        // The commit may have been saved to system.paxos under a different replication type.
        if (!shouldBeTracked && !commit.mutation.id().isNone())
        {
            logger.warn("Stripping mutation ID {} from PaxosCommitAndPrepare for {}.{} partition {} - keyspace migrated to untracked",
                        commit.mutation.id(), commit.metadata().keyspace, commit.metadata().name, commit.partitionKey());
            Tracing.trace("Stripping mutation ID {} from PaxosCommitAndPrepare for {}.{} partition {} - keyspace migrated to untracked",
                          commit.mutation.id(), commit.metadata().keyspace, commit.metadata().name, commit.partitionKey());
            commit = commit.withMutationId(MutationId.none());
        }

        if (shouldBeTracked)
        {
            /*
             * Consistency for consensus is tricky to pick here. The goal of sending this commit is to unblock the prepare
             * on nodes that are missing the commit. CommitAndPrepare is an outcome that occurs when prepare/propose already failed
             * because enough nodes were missing a commmit so we need to try again. To keep things highly available we
             * use the same consistency as consensus so that when we go to do the prepare there are enough nodes
             * we know have the commit that this can succeed.
             */
            PaxosCommit.commit(commit, participants, participants.consistencyForConsensus, participants.consistencyForConsensus, isWrite);
            return PaxosPrepare.prepareWithBallot(ballot, participants, readCommand, isWrite, acceptEarlyReadSuccess);
        }
        else
        {
            Request request = new Request(commit, ballot, participants.electorate, readCommand, isWrite, true);
            PaxosPrepare prepare = new PaxosPrepare(participants, request, acceptEarlyReadSuccess, null);
            Message<Request> message = Message.out(PAXOS2_COMMIT_AND_PREPARE_REQ, request, participants.isUrgent());

            start(prepare, participants, message, RequestHandler::execute);
            return prepare;
        }
    }

    private static class Request extends PaxosPrepare.AbstractRequest<Request>
    {
        final Agreed commit;

        Request(Agreed commit, Ballot ballot, Paxos.Electorate electorate, EmbeddableSinglePartitionReadCommand read, boolean isWrite, boolean isForRecovery)
        {
            super(ballot, electorate, read, isWrite, isForRecovery);
            this.commit = commit;
        }

        private Request(Agreed commit, Ballot ballot, Paxos.Electorate electorate, DecoratedKey partitionKey, TableMetadata table, boolean isWrite, boolean isForRecovery)
        {
            super(ballot, electorate, partitionKey, table, isWrite, isForRecovery);
            this.commit = commit;
        }

        Request withoutRead()
        {
            return new Request(commit, ballot, electorate, partitionKey, table, isForWrite, isForRecovery);
        }

        @Override
        public Request asTrackedDataRequest(Id id, ConsistencyLevel consistencyLevel, int dataNode, int[] summaryNodes)
        {
            return new Request(commit, ballot, electorate, new TrackedRead.DataRequest(id, (SinglePartitionReadCommand)read, dataNode, summaryNodes, consistencyLevel), isForWrite, isForRecovery);
        }

        @Override
        public Request asTrackedSummaryRequest(Id id, int dataNode, int[] summaryNodes)
        {
            return new Request(commit, ballot, electorate, new TrackedRead.SummaryRequest(id, (SinglePartitionReadCommand)read, dataNode, summaryNodes), isForWrite, isForRecovery);
        }

        public String toString()
        {
            return commit.toString("CommitAndPrepare(") + ", " + Ballot.toString(ballot) + ')';
        }
    }

    public static class RequestSerializer extends PaxosPrepare.AbstractRequestSerializer<Request, Agreed>
    {
        Request construct(Agreed param, Ballot ballot, Paxos.Electorate electorate, EmbeddableSinglePartitionReadCommand read, boolean isWrite, boolean isForRecovery)
        {
            return new Request(param, ballot, electorate, read, isWrite, isForRecovery);
        }

        Request construct(Agreed param, Ballot ballot, Paxos.Electorate electorate, DecoratedKey partitionKey, TableMetadata table, boolean isWrite, boolean isForRecovery)
        {
            return new Request(param, ballot, electorate, partitionKey, table, isWrite, isForRecovery);
        }

        @Override
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            Agreed.serializer.serialize(request.commit, out, version);
            super.serialize(request, out, version);
        }

        @Override
        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            Agreed committed = Agreed.serializer.deserialize(in, version);
            return deserialize(committed, in, version);
        }

        @Override
        public long serializedSize(Request request, int version)
        {
            return Agreed.serializer.serializedSize(request.commit, version)
                    + super.serializedSize(request, version);
        }
    }

    public static class RequestHandler implements IVerbHandler<Request>
    {
        @Override
        public void doVerb(Message<Request> message)
        {
            ClusterMetadata metadata = ClusterMetadata.current();

            Agreed commit = message.payload.commit;
            boolean coordinatorSaysTracked = !commit.mutation.id().isNone();
            metadata = MigrationRouter.checkPaxosCommitMigration(metadata, message, message.from(),
                                                                 commit.metadata().keyspace, commit.metadata().id,
                                                                 commit.partitionKey().getToken(),
                                                                 coordinatorSaysTracked);

            if (message.payload.read != null)
                MigrationRouter.checkPaxosPrepareReadMigration(metadata, message, message.from(), message.payload.read);

            Future<Response> response = execute(message.payload, new RequestTime(message.createdAtNanos()));
            if (response == null)
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
            else
                // TODO unwrap error for error handling in the verb
                MessagingService.instance().respond(getUnchecked(response), message);
        }

        private static Future<PaxosPrepare.Response> execute(Request request, RequestTime requestTime)
        {
            Agreed commit = request.commit;
            if (!Paxos.isInRangeAndShouldProcess(commit.partitionKey(), commit.metadata(), request.read != null))
                return null;

            // This can be done outside the lock
            ClusterMetadata cm = ClusterMetadata.current();
            KeyMigrationState keyMigrationState = getKeyMigrationState(cm, commit.metadata().id, commit.partitionKey());
            // Make sure the operation is safe and there is no Accord state that needs application
            // Also need to know max HLC in order to accept this ballot
            long maxHLC = keyMigrationState.maybePerformAccordToPaxosKeyMigration(true);
            if (maxHLC >= commit.ballot.unixMicros())
                return ImmediateFuture.success(new Rejected(Ballot.atUnixMicrosWithLsb(maxHLC + 1, 0, commit.ballot.flag())));

            try (PaxosState state = PaxosState.get(commit))
            {
                state.commit(commit);
                return PaxosPrepare.RequestHandler.execute(requestTime, request, state, cm);
            }
        }
    }
}
