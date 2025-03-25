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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.transport.Dispatcher;

// rm?
public class ForwardedWriteResponseHandler extends AbstractWriteResponseHandler<NoPayload>
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedWriteResponseHandler.class);

    private final AbstractWriteResponseHandler<NoPayload> delegate;

    private ForwardedWriteResponseHandler(AbstractWriteResponseHandler<NoPayload> delegate)
    {
        super(delegate.replicaPlan, delegate.callback, delegate.writeType, () -> null, delegate.getRequestTime());
        this.delegate = delegate;
    }

    public static ForwardedWriteResponseHandler wrap(AbstractWriteResponseHandler<NoPayload> handler)
    {
        return new ForwardedWriteResponseHandler(handler);
    }

    @Override
    public void get() throws WriteTimeoutException, WriteFailureException
    {
        delegate.get();
    }

    @Override
    protected int blockFor()
    {
        return delegate.blockFor();
    }

    @Override
    protected int candidateReplicaCount()
    {
        return delegate.candidateReplicaCount();
    }

    @Override
    public ConsistencyLevel consistencyLevel()
    {
        return delegate.consistencyLevel();
    }

    @Override
    protected boolean waitingFor(InetAddressAndPort from)
    {
        return delegate.waitingFor(from);
    }

    @Override
    public Dispatcher.RequestTime getRequestTime()
    {
        return delegate.getRequestTime();
    }

    @Override
    protected void signal()
    {
        delegate.signal();
    }

    @Override
    public void onResponse(Message<NoPayload> msg)
    {
        logger.debug("Got direct response from replica {}", msg);
        delegate.onResponse(msg);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        delegate.onFailure(from, failureReason);
    }

    @Override
    public boolean invokeOnFailure()
    {
        return delegate.invokeOnFailure();
    }

    @Override
    public void maybeTryAdditionalReplicas(IMutation mutation, StorageProxy.WritePerformer writePerformer, String localDC)
    {
        delegate.maybeTryAdditionalReplicas(mutation, writePerformer, localDC);
    }

    @Override
    public boolean trackLatencyForSnitch()
    {
        return true;
    }

    @Override
    protected int ackCount()
    {
        return delegate.ackCount();
    }
}
