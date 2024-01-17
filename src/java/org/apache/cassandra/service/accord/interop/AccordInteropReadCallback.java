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

import javax.annotation.Nonnull;

import accord.local.Node;
import accord.messages.Callback;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.utils.Invariants;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;

import static accord.messages.ReadData.CommitOrReadNack.Insufficient;

public abstract class AccordInteropReadCallback<T> implements Callback<ReadReply>
{
    interface MaximalCommitSender
    {
        void sendMaximalCommit(@Nonnull Node.Id to);
    }

    private final Node.Id id;
    private final InetAddressAndPort endpoint;
    private final Message<?> message;
    private final RequestCallback<T> wrapped;
    private final MaximalCommitSender maximalCommitSender;

    public AccordInteropReadCallback(Node.Id id, InetAddressAndPort endpoint, Message<?> message, RequestCallback<T> wrapped, MaximalCommitSender maximalCommitSender)
    {
        this.id = id;
        this.message = message;
        this.endpoint = endpoint;
        this.wrapped = wrapped;
        this.maximalCommitSender = maximalCommitSender;
    }

    abstract T convertResponse(ReadOk ok);

    public void onSuccess(Node.Id from, ReadReply reply)
    {
        Invariants.checkArgument(from.equals(id));
        if (reply.isOk())
        {
            wrapped.onResponse(message.responseWith(convertResponse((ReadOk) reply)).withFrom(endpoint));
        }
        else if (reply == Insufficient)
        {
            // Might still send a response if we send a maximal commit. Accord would tryAlternative and send
            // both the commit and an additional repair, but Cassandra doesn't have tryAlternative unless we add
            // it and instead opts to trigger additional repair messages based on time.
            maximalCommitSender.sendMaximalCommit(id);
        }
        else
        {
            wrapped.onFailure(endpoint, RequestFailure.UNKNOWN);
        }
    }

    public void onFailure(Node.Id from, Throwable failure)
    {
        wrapped.onFailure(endpoint, RequestFailure.UNKNOWN);
    }

    public void onCallbackFailure(Node.Id from, Throwable failure)
    {
        wrapped.onFailure(endpoint, RequestFailure.UNKNOWN);
    }
}
