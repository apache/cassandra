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

package org.apache.cassandra.service.accord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.Timeout;
import accord.local.AgentExecutor;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.SafeCallback;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;

class AccordCallback<T extends Reply> extends SafeCallback<T> implements RequestCallback<T>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCallback.class);
    private final AccordEndpointMapper endpointMapper;

    public AccordCallback(AgentExecutor executor, Callback<T> callback, AccordEndpointMapper endpointMapper)
    {
        super(executor, callback);
        this.endpointMapper = endpointMapper;
    }

    @Override
    public void onResponse(Message<T> msg)
    {
        logger.trace("Received response {} from {}", msg.payload, msg.from());
        success(endpointMapper.mappedId(msg.from()), msg.payload);
    }

    private static Throwable convertFailureMessage(RequestFailure failure)
    {
        return failure.reason == RequestFailureReason.TIMEOUT ?
               new Timeout(null, null) :
               new RuntimeException(failure.failure);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailure failure)
    {
        logger.trace("Received failure {} from {} for {}", failure, from, this);
        // TODO (now): we should distinguish timeout failures with some placeholder Exception
        failure(endpointMapper.mappedId(from), convertFailureMessage(failure));
    }

    @Override
    public boolean trackLatencyForSnitch()
    {
        return true;
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }
}
