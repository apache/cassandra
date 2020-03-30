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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_START_PREPARE_REQ;
import static org.apache.cassandra.service.paxos.Paxos.newBallot;
import static org.apache.cassandra.service.paxos.PaxosState.ballotTracker;

/**
 * Determines the highest ballot we should attempt to repair
 */
public class PaxosStartPrepareCleanup extends AsyncFuture<PaxosCleanupHistory> implements RequestCallbackWithFailure<PaxosCleanupHistory>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosStartPrepareCleanup.class);

    public static final RequestSerializer serializer = new RequestSerializer();

    private final TableId table;

    private final Set<InetAddressAndPort> waitingResponse;
    private Ballot maxBallot = null;
    private PaxosRepairHistory history = null;

    PaxosStartPrepareCleanup(TableId table, Collection<InetAddressAndPort> endpoints)
    {
        this.table = table;
        this.waitingResponse = new HashSet<>(endpoints);
    }

    /**
     * We run paxos repair as part of topology changes, so we include the local endpoint state in the paxos repair
     * prepare message to prevent racing with gossip dissemination and guarantee that every repair participant is aware
     * of the pending ring change during repair.
     */
    public static PaxosStartPrepareCleanup prepare(TableId tableId, Collection<InetAddressAndPort> endpoints, EndpointState localEpState, Collection<Range<Token>> ranges)
    {
        PaxosStartPrepareCleanup callback = new PaxosStartPrepareCleanup(tableId, endpoints);
        synchronized (callback)
        {
            Message<Request> message = Message.out(PAXOS2_CLEANUP_START_PREPARE_REQ, new Request(tableId, localEpState, ranges));
            for (InetAddressAndPort endpoint : endpoints)
                MessagingService.instance().sendWithCallback(message, endpoint, callback);
        }
        return callback;
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        tryFailure(new PaxosCleanupException("Received " + reason + " failure response from " + from));
    }

    public synchronized void onResponse(Message<PaxosCleanupHistory> msg)
    {
        if (isDone())
            return;

        if (!waitingResponse.remove(msg.from()))
            throw new IllegalArgumentException("Received unexpected response from " + msg.from());

        if (Commit.isAfter(msg.payload.highBound, maxBallot))
            maxBallot = msg.payload.highBound;

        history = PaxosRepairHistory.merge(history, msg.payload.history);

        if (waitingResponse.isEmpty())
            trySuccess(new PaxosCleanupHistory(table, maxBallot, history));
    }

    private static void maybeUpdateTopology(InetAddressAndPort endpoint, EndpointState remote)
    {
        EndpointState local = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (local == null || local.isSupersededBy(remote))
        {
            logger.trace("updating endpoint info for {} with {}", endpoint, remote);
            Map<InetAddressAndPort, EndpointState> states = Collections.singletonMap(endpoint, remote);

            Gossiper.runInGossipStageBlocking(() -> {
                Gossiper.instance.notifyFailureDetector(states);
                Gossiper.instance.applyStateLocally(states);
            });
            // TODO: We should also wait for schema pulls/pushes, however this would be quite an involved change to MigrationManager
            //       (which currently drops some migration tasks on the floor).
            //       Note it would be fine for us to fail to complete the migration task and simply treat this response as a failure/timeout.
        }
        // even if we have th latest gossip info, wait until pending range calculations are complete
        PendingRangeCalculatorService.instance.blockUntilFinished();
    }

    public static class Request
    {
        final TableId tableId;
        final EndpointState epState;
        final Collection<Range<Token>> ranges;

        public Request(TableId tableId, EndpointState epState, Collection<Range<Token>> ranges)
        {
            this.tableId = tableId;
            this.epState = epState;
            this.ranges = ranges;
        }
    }

    public static class RequestSerializer implements IVersionedSerializer<Request>
    {
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            request.tableId.serialize(out);
            EndpointState.serializer.serialize(request.epState, out, version);
            out.writeInt(request.ranges.size());
            for (Range<Token> rt : request.ranges)
                AbstractBounds.tokenSerializer.serialize(rt, out, version);
        }

        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            EndpointState epState = EndpointState.serializer.deserialize(in, version);

            int numRanges = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>();
            for (int i = 0; i < numRanges; i++)
            {
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version);
                ranges.add(range);
            }
            return new Request(tableId, epState, ranges);
        }

        public long serializedSize(Request request, int version)
        {
            long size = request.tableId.serializedSize();
            size += EndpointState.serializer.serializedSize(request.epState, version);
            size += TypeSizes.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            return size;
        }
    }

    public static final IVerbHandler<Request> verbHandler = in -> {
        ColumnFamilyStore table = Schema.instance.getColumnFamilyStoreInstance(in.payload.tableId);
        maybeUpdateTopology(in.from(), in.payload.epState);
        Ballot highBound = newBallot(ballotTracker().getHighBound(), ConsistencyLevel.SERIAL);
        PaxosRepairHistory history = table.getPaxosRepairHistoryForRanges(in.payload.ranges);
        Message<PaxosCleanupHistory> out = in.responseWith(new PaxosCleanupHistory(table.metadata.id, highBound, history));
        MessagingService.instance().send(out, in.respondTo());
    };
}
