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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Ballot.Flag;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.PAXOS2_UPDATE_LOW_BALLOT_REQ;

public class PaxosUpdateLowBallot extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>
{
    private final Set<InetAddressAndPort> waitingResponse;
    private final long lowBound;
    private final SharedContext ctx;

    public PaxosUpdateLowBallot(SharedContext ctx, Collection<InetAddressAndPort> endpoints, long maxHlc)
    {
        checkArgument(maxHlc != IAccordService.NO_HLC);
        this.ctx = ctx;
        this.waitingResponse = new HashSet<>(endpoints);
        this.lowBound = maxHlc + 1;
    }

    public synchronized void start()
    {
        Request request = new Request(lowBound);

        Message<Request> message = Message.out(PAXOS2_UPDATE_LOW_BALLOT_REQ, request);

        for (InetAddressAndPort endpoint : waitingResponse)
            ctx.messaging().sendWithCallback(message, endpoint, this);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailure reason)
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

    public static class Request
    {
        final long lowBound;

        Request(long lowBound)
        {
            this.lowBound = lowBound;
        }
    }

    public static final IVersionedSerializer<Request> serializer = new IVersionedSerializer<Request>()
    {
        public void serialize(Request request, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(request.lowBound);
        }

        public Request deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Request(in.readLong());
        }

        public long serializedSize(Request request, int version)
        {
            return TypeSizes.sizeof(request.lowBound);
        }
    };

    public static IVerbHandler<Request> createVerbHandler(SharedContext ctx)
    {
        return (in) -> {
            PaxosState.ballotTracker().updateLowBound(Ballot.atUnixMicrosWithLsb(in.payload.lowBound, 0, Flag.GLOBAL));
            ctx.messaging().respond(noPayload, in);
        };
    }

    public static final IVerbHandler<Request> verbHandler = createVerbHandler(SharedContext.Global.instance);
}
