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
package org.apache.cassandra.service;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;

public class TrackedWriteResponseHandler extends AbstractWriteResponseHandler<NoPayload>
{
    private final AbstractWriteResponseHandler<NoPayload> wrapped;

    private final String keyspace;
    private final Token token;
    private final MutationId mutationId;

    private TrackedWriteResponseHandler(
        AbstractWriteResponseHandler<NoPayload> wrapped, String keyspace, Token token, MutationId mutationId)
    {
        super(wrapped.replicaPlan, wrapped.callback, wrapped.writeType, null, wrapped.getRequestTime());
        this.wrapped = wrapped;
        this.keyspace = keyspace;
        this.token = token;
        this.mutationId = mutationId;
    }

    public static TrackedWriteResponseHandler wrap(
        AbstractWriteResponseHandler<NoPayload> handler, String keyspace, Token token, MutationId mutationId)
    {
        return new TrackedWriteResponseHandler(handler, keyspace, token, mutationId);
    }

    @Override
    public void onResponse(Message<NoPayload> msg)
    {
        // Local mutations are witnessed from Keyspace.applyInternalTracked
        if (msg != null)
            MutationTrackingService.instance.witnessedRemoteMutation(keyspace, token, mutationId, msg.from());
        wrapped.onResponse(msg);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        wrapped.onFailure(from, failureReason);
    }

    @Override
    public boolean trackLatencyForSnitch()
    {
        return wrapped.trackLatencyForSnitch();
    }

    @Override
    protected int ackCount()
    {
        return wrapped.ackCount();
    }

    @Override
    public boolean invokeOnFailure()
    {
        return wrapped.invokeOnFailure();
    }

    @Override
    public void get() throws WriteTimeoutException, WriteFailureException
    {
        wrapped.get();
    }

    @Override
    protected int blockFor()
    {
        return wrapped.blockFor();
    }

    @Override
    protected int candidateReplicaCount()
    {
        return wrapped.candidateReplicaCount();
    }

    @Override
    protected boolean waitingFor(InetAddressAndPort from)
    {
        return wrapped.waitingFor(from);
    }

    @Override
    protected void signal()
    {
        wrapped.signal();
    }
}
