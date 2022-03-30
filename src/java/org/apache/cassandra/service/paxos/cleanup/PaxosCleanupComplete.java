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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_COMPLETE_REQ;

public class PaxosCleanupComplete extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>, Runnable
{
    private final Set<InetAddressAndPort> waitingResponse;
    final TableId tableId;
    final Collection<Range<Token>> ranges;
    final Ballot lowBound;
    final boolean skippedReplicas;

    PaxosCleanupComplete(Collection<InetAddressAndPort> endpoints, TableId tableId, Collection<Range<Token>> ranges, Ballot lowBound, boolean skippedReplicas)
    {
        this.waitingResponse = new HashSet<>(endpoints);
        this.tableId = tableId;
        this.ranges = ranges;
        this.lowBound = lowBound;
        this.skippedReplicas = skippedReplicas;
    }

    public synchronized void run()
    {
        Request request = !skippedReplicas ? new Request(tableId, lowBound, ranges)
                                           : new Request(tableId, Ballot.none(), Collections.emptyList());
        Message<Request> message = Message.out(PAXOS2_CLEANUP_COMPLETE_REQ, request);
        for (InetAddressAndPort endpoint : waitingResponse)
            MessagingService.instance().sendWithCallback(message, endpoint, this);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        tryFailure(new PaxosCleanupException("Timed out waiting on response from " + from));
    }

    @Override
    public synchronized void onResponse(Message<Void> msg)
    {
        if (isDone())
            return;

        if (!waitingResponse.remove(msg.from()))
            throw new IllegalArgumentException("Received unexpected response from " + msg.from());

        if (waitingResponse.isEmpty())
            trySuccess(null);
    }

    static class Request
    {
        final TableId tableId;
        final Ballot lowBound;
        final Collection<Range<Token>> ranges;

        Request(TableId tableId, Ballot lowBound, Collection<Range<Token>> ranges)
        {
            this.tableId = tableId;
            this.ranges = ranges;
            this.lowBound = lowBound;
        }
    }

    public static final IVersionedSerializer<Request> serializer = new IVersionedSerializer<Request>()
    {
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            request.tableId.serialize(out);
            request.lowBound.serialize(out);
            out.writeInt(request.ranges.size());
            for (Range<Token> rt : request.ranges)
                AbstractBounds.tokenSerializer.serialize(rt, out, version);
        }

        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            Ballot lowBound = Ballot.deserialize(in);
            int numRanges = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>();
            for (int i = 0; i < numRanges; i++)
            {
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, getPartitioner(), version);
                ranges.add(range);
            }
            return new Request(tableId, lowBound, ranges);
        }

        public long serializedSize(Request request, int version)
        {
            long size = request.tableId.serializedSize();
            size += Ballot.sizeInBytes();
            size += TypeSizes.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            return size;
        }
    };

    public static final IVerbHandler<Request> verbHandler = (in) -> {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(in.payload.tableId);
        cfs.onPaxosRepairComplete(in.payload.ranges, in.payload.lowBound);
        MessagingService.instance().respond(noPayload, in);
    };
}
