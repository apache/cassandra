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

package org.apache.cassandra.service.accord.interop;

import accord.coordinate.Timeout;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.utils.Invariants;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;

import static accord.messages.ReadData.CommitOrReadNack.Insufficient;

public abstract class AccordInteropReadCallback<T> implements Callback<ReadReply>
{
    private final Node.Id id;
    private final InetAddressAndPort endpoint;
    private final Message<?> message;
    private final RequestCallback<T> wrapped;
    private final AccordInteropExecution interopExecution;

    public AccordInteropReadCallback(Node.Id id, InetAddressAndPort endpoint, Message<?> message, RequestCallback<T> wrapped, AccordInteropExecution interopExecution)
    {
        this.id = id;
        this.message = message;
        this.endpoint = endpoint;
        this.wrapped = wrapped;
        this.interopExecution = interopExecution;
    }

    abstract T convertResponse(ReadOk ok);

    @Override
    public void onSuccess(Node.Id from, ReadReply reply)
    {
        Invariants.requireArgument(from.equals(id));
        if (reply.isOk())
        {
            ReadOk readOk = (ReadOk)reply;
            interopExecution.maybeUpdateUniqueHlc(readOk.uniqueHlc);
            wrapped.onResponse(message.responseWith(convertResponse(readOk)).withFrom(endpoint));
        }
        else if (reply == Insufficient)
        {
            // Might still send a response if we send a maximal commit. Accord would tryAlternative and send
            // both the commit and an additional repair, but Cassandra doesn't have tryAlternative unless we add
            // it and instead opts to trigger additional repair messages based on time.
            interopExecution.sendMaximalCommit(id);
        }
        else if (reply != ReadData.CommitOrReadNack.Waiting)
        {
            wrapped.onFailure(endpoint, RequestFailure.UNKNOWN);
        }
    }

    @Override
    public void onFailure(Node.Id from, Throwable failure)
    {
        RequestFailure requestFailure;
        // Convert from Accord's timeout exception to our failure reason because timeout is something
        // that is useful for metrics and can be handled differently
        if (failure instanceof Timeout)
            requestFailure = new RequestFailure(RequestFailureReason.TIMEOUT, failure);
        else
            requestFailure = new RequestFailure(RequestFailureReason.UNKNOWN, failure);
        wrapped.onFailure(endpoint, requestFailure);
    }

    @Override
    public boolean onCallbackFailure(Node.Id from, Throwable failure)
    {
        wrapped.onFailure(endpoint, RequestFailure.UNKNOWN);
        return true;
    }
}
