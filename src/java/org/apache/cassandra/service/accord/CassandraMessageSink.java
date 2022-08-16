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

import java.util.EnumMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import accord.api.MessageSink;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.service.accord.EndpointMapping.getEndpoint;

public class CassandraMessageSink implements MessageSink
{
    private static final Map<MessageType, Verb> VERB_MAPPING = new EnumMap<>(MessageType.class);
    static {
        VERB_MAPPING.put(MessageType.PREACCEPT_REQ, Verb.ACCORD_PREACCEPT_REQ);
        VERB_MAPPING.put(MessageType.PREACCEPT_RSP, Verb.ACCORD_PREACCEPT_RSP);
        VERB_MAPPING.put(MessageType.ACCEPT_REQ, Verb.ACCORD_ACCEPT_REQ);
        VERB_MAPPING.put(MessageType.ACCEPT_RSP, Verb.ACCORD_ACCEPT_RSP);
        VERB_MAPPING.put(MessageType.COMMIT_REQ, Verb.ACCORD_COMMIT_REQ);
        VERB_MAPPING.put(MessageType.APPLY_REQ, Verb.ACCORD_APPLY_REQ);
        VERB_MAPPING.put(MessageType.READ_REQ, Verb.ACCORD_READ_REQ);
        VERB_MAPPING.put(MessageType.READ_RSP, Verb.ACCORD_READ_RSP);
        VERB_MAPPING.put(MessageType.RECOVER_REQ, Verb.ACCORD_RECOVER_REQ);
        VERB_MAPPING.put(MessageType.RECOVER_RSP, Verb.ACCORD_RECOVER_RSP);
        VERB_MAPPING.put(MessageType.WAIT_ON_COMMIT_REQ, Verb.ACCORD_WAIT_COMMIT_REQ);
        VERB_MAPPING.put(MessageType.WAIT_ON_COMMIT_RSP, Verb.ACCORD_WAIT_COMMIT_RSP);
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        Verb verb = VERB_MAPPING.get(request.type());
        Preconditions.checkArgument(verb != null);
        Message<Request> message = Message.out(verb, request);
        MessagingService.instance().send(message, getEndpoint(to));
    }

    @Override
    public void send(Node.Id to, Request request, Callback callback)
    {
        Verb verb = VERB_MAPPING.get(request.type());
        Preconditions.checkArgument(verb != null);
        Message<Request> message = Message.out(verb, request);
        MessagingService.instance().sendWithCallback(message, getEndpoint(to), new AccordCallback<>((Callback<Reply>) callback));
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        Message<?> replyTo = (Message<?>) replyContext;
        Message<?> replyMsg = replyTo.responseWith(reply);
        Preconditions.checkArgument(replyMsg.verb() == VERB_MAPPING.get(reply.type()));
        MessagingService.instance().send(replyMsg, getEndpoint(replyingToNode));
    }
}
