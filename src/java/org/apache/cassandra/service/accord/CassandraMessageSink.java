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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import accord.api.MessageSink;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.PreAccept;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.service.accord.EndpointMapping.getEndpoint;

public class CassandraMessageSink implements MessageSink
{
    private static final ImmutableMap<Class<? extends Request>, Verb> REQUEST_VERBS;
    private static final ImmutableMap<Class<? extends Reply>, Verb> RESPONSE_VERBS;
    static {
        // TODO: make an accord level Message verb/type we can map
        REQUEST_VERBS = ImmutableMap.<Class<? extends Request>, Verb>builder()
                            .put(PreAccept.class, Verb.ACCORD_PREACCEPT_REQ)
                            .build();
        RESPONSE_VERBS = ImmutableMap.<Class<? extends Reply>, Verb>builder()
                            .put(PreAccept.PreAcceptOk.class, Verb.ACCORD_PREACCEPT_RSP)
                            .build();
    }

    private static class AccordCallback<T extends Reply> implements RequestCallback<T>
    {
        private final Callback<T> callback;

        public AccordCallback(Callback<T> callback)
        {
            this.callback = callback;
        }

        @Override
        public void onResponse(Message<T> msg)
        {
            callback.onSuccess(EndpointMapping.endpointToId(msg.from()), msg.payload);
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            callback.onFailure(EndpointMapping.endpointToId(from), new RuntimeException(failureReason.toString()));
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        Verb verb = REQUEST_VERBS.get(request.getClass());
        Preconditions.checkArgument(verb != null);
        Message<Request> message = Message.out(verb, request);
        MessagingService.instance().send(message, getEndpoint(to));
    }

    @Override
    public void send(Node.Id to, Request request, Callback callback)
    {
        Verb verb = REQUEST_VERBS.get(request.getClass());
        Preconditions.checkArgument(verb != null);
        Message<Request> message = Message.out(verb, request);

        MessagingService.instance().sendWithCallback(message, getEndpoint(to), new AccordCallback<>((Callback<Reply>) callback));
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        Message<?> replyTo = (Message<?>) replyContext;
        MessagingService.instance().send(replyTo.responseWith(reply), getEndpoint(replyingToNode));
    }
}
