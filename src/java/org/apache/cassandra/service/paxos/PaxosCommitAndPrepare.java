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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.Verb.PAXOS2_COMMIT_AND_PREPARE_REQ;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
import static org.apache.cassandra.service.paxos.PaxosPrepare.start;

public class PaxosCommitAndPrepare
{
    public static final RequestSerializer requestSerializer = new RequestSerializer();
    public static final RequestHandler requestHandler = new RequestHandler();

    static PaxosPrepare commitAndPrepare(Agreed commit, Paxos.Participants participants, SinglePartitionReadCommand readCommand, boolean isWrite, boolean acceptEarlyReadSuccess)
    {
        Ballot ballot = newBallot(commit.ballot, participants.consistencyForConsensus);
        Request request = new Request(commit, ballot, participants.electorate, readCommand, isWrite);
        PaxosPrepare prepare = new PaxosPrepare(participants, request, acceptEarlyReadSuccess, null);

        Tracing.trace("Committing {}; Preparing {}", commit.ballot, ballot);
        Message<Request> message = Message.out(PAXOS2_COMMIT_AND_PREPARE_REQ, request);
//                .permitsArtificialDelay(participants.consistencyForConsensus);
        start(prepare, participants, message, RequestHandler::execute);
        return prepare;
    }

    private static class Request extends PaxosPrepare.AbstractRequest<Request>
    {
        final Agreed commit;

        Request(Agreed commit, Ballot ballot, Paxos.Electorate electorate, SinglePartitionReadCommand read, boolean isWrite)
        {
            super(ballot, electorate, read, isWrite);
            this.commit = commit;
        }

        private Request(Agreed commit, Ballot ballot, Paxos.Electorate electorate, DecoratedKey partitionKey, TableMetadata table, boolean isWrite)
        {
            super(ballot, electorate, partitionKey, table, isWrite);
            this.commit = commit;
        }

        Request withoutRead()
        {
            return new Request(commit, ballot, electorate, partitionKey, table, isForWrite);
        }

        public String toString()
        {
            return commit.toString("CommitAndPrepare(") + ", " + Ballot.toString(ballot) + ')';
        }
    }

    public static class RequestSerializer extends PaxosPrepare.AbstractRequestSerializer<Request, Agreed>
    {
        Request construct(Agreed param, Ballot ballot, Paxos.Electorate electorate, SinglePartitionReadCommand read, boolean isWrite)
        {
            return new Request(param, ballot, electorate, read, isWrite);
        }

        Request construct(Agreed param, Ballot ballot, Paxos.Electorate electorate, DecoratedKey partitionKey, TableMetadata table, boolean isWrite)
        {
            return new Request(param, ballot, electorate, partitionKey, table, isWrite);
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
            PaxosPrepare.Response response = execute(message.payload, message.from());
            if (response == null)
                MessagingService.instance().respondWithFailure(UNKNOWN, message);
            else
                MessagingService.instance().respond(response, message);
        }

        private static PaxosPrepare.Response execute(Request request, InetAddressAndPort from)
        {
            Agreed commit = request.commit;
            if (!Paxos.isInRangeAndShouldProcess(from, commit.update.partitionKey(), commit.update.metadata(), request.read != null))
                return null;

            try (PaxosState state = PaxosState.get(commit))
            {
                state.commit(commit);
                return PaxosPrepare.RequestHandler.execute(request, state);
            }
        }
    }
}
