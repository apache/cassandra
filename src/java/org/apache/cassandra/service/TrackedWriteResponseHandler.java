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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;

public class TrackedWriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedWriteResponseHandler.class);

    private final AbstractWriteResponseHandler<T> wrapped;

    private final MutationId mutationId;

    private TrackedWriteResponseHandler(AbstractWriteResponseHandler<T> wrapped, MutationId mutationId)
    {
        super(wrapped.replicaPlan, wrapped.callback, wrapped.writeType, null, wrapped.getRequestTime());
        this.wrapped = wrapped;
        this.mutationId = mutationId;
    }

    public static <T> TrackedWriteResponseHandler<T> wrap(AbstractWriteResponseHandler<T> handler, MutationId mutationId)
    {
        return new TrackedWriteResponseHandler<>(handler, mutationId);
    }

    @Override
    public void onResponse(Message<T> msg)
    {
        // Local mutations are witnessed from Keyspace.applyInternalTracked
        if (msg != null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Received write response for mutation {} from {}", mutationId, msg.from());
            MutationTrackingService.instance().receivedWriteResponse(mutationId, msg.from());
        }
        wrapped.onResponse(msg);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailure failure)
    {
        if (logger.isTraceEnabled())
            logger.trace("Write failed for mutation {} from {}: {}", mutationId, from, failure);
        MutationTrackingService.instance().retryFailedWrite(mutationId, from, failure);
        wrapped.onFailure(from, failure);
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
