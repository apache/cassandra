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

import java.util.*;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

public class PaxosFinishPrepareCleanup extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>
{
    private final Set<InetAddressAndPort> waitingResponse;

    PaxosFinishPrepareCleanup(Collection<InetAddressAndPort> endpoints)
    {
        this.waitingResponse = new HashSet<>(endpoints);
    }

    public static PaxosFinishPrepareCleanup finish(SharedContext ctx, Collection<InetAddressAndPort> endpoints, PaxosCleanupHistory result)
    {
        PaxosFinishPrepareCleanup callback = new PaxosFinishPrepareCleanup(endpoints);
        synchronized (callback)
        {
            Message<PaxosCleanupHistory> message = Message.out(Verb.PAXOS2_CLEANUP_FINISH_PREPARE_REQ, result);
            for (InetAddressAndPort endpoint : endpoints)
                ctx.messaging().sendWithCallback(message, endpoint, callback);
        }
        return callback;
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        tryFailure(new PaxosCleanupException(reason + " failure response from " + from));
    }

    public synchronized void onResponse(Message<Void> msg)
    {
        if (isDone())
            return;

        if (!waitingResponse.remove(msg.from()))
            throw new IllegalArgumentException("Received unexpected response from " + msg.from());

        if (waitingResponse.isEmpty())
            trySuccess(null);
    }

    public static IVerbHandler<PaxosCleanupHistory> createVerbHandler(SharedContext ctx)
    {
        return ctx.paxosRepairState()::addCleanupHistory;
    }

    public static final IVerbHandler<PaxosCleanupHistory> verbHandler = createVerbHandler(SharedContext.Global.instance);
}
