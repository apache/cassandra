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
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.MessageSink;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.service.accord.EndpointMapping.getEndpoint;

public class AccordMessageSink implements MessageSink
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMessageSink.class);

    private static class VerbMapping
    {
        private static final VerbMapping instance = new VerbMapping();

        private final Map<MessageType, Verb> mapping = new EnumMap<>(MessageType.class);

        private VerbMapping()
        {
            mapping.put(MessageType.PREACCEPT_REQ,          Verb.ACCORD_PREACCEPT_REQ);
            mapping.put(MessageType.PREACCEPT_RSP,          Verb.ACCORD_PREACCEPT_RSP);
            mapping.put(MessageType.ACCEPT_REQ,             Verb.ACCORD_ACCEPT_REQ);
            mapping.put(MessageType.ACCEPT_RSP,             Verb.ACCORD_ACCEPT_RSP);
            mapping.put(MessageType.ACCEPT_INVALIDATE_REQ,  Verb.ACCORD_ACCEPT_INVALIDATE_REQ);
            mapping.put(MessageType.COMMIT_REQ,             Verb.ACCORD_COMMIT_REQ);
            mapping.put(MessageType.COMMIT_INVALIDATE,      Verb.ACCORD_COMMIT_INVALIDATE_REQ);
            mapping.put(MessageType.APPLY_REQ,              Verb.ACCORD_APPLY_REQ);
            mapping.put(MessageType.APPLY_RSP,              Verb.ACCORD_APPLY_RSP);
            mapping.put(MessageType.READ_REQ,               Verb.ACCORD_READ_REQ);
            mapping.put(MessageType.READ_RSP,               Verb.ACCORD_READ_RSP);
            mapping.put(MessageType.BEGIN_RECOVER_REQ,      Verb.ACCORD_RECOVER_REQ);
            mapping.put(MessageType.BEGIN_RECOVER_RSP,      Verb.ACCORD_RECOVER_RSP);
            mapping.put(MessageType.BEGIN_INVALIDATE_REQ,   Verb.ACCORD_BEGIN_INVALIDATE_REQ);
            mapping.put(MessageType.BEGIN_INVALIDATE_RSP,   Verb.ACCORD_BEGIN_INVALIDATE_RSP);
            mapping.put(MessageType.WAIT_ON_COMMIT_REQ,     Verb.ACCORD_WAIT_COMMIT_REQ);
            mapping.put(MessageType.WAIT_ON_COMMIT_RSP,     Verb.ACCORD_WAIT_COMMIT_RSP);
            mapping.put(MessageType.INFORM_TXNID_REQ,       Verb.ACCORD_INFORM_OF_TXNID_REQ);
            mapping.put(MessageType.INFORM_HOME_DURABLE_REQ,Verb.ACCORD_INFORM_HOME_DURABLE_REQ);
            mapping.put(MessageType.INFORM_DURABLE_REQ,     Verb.ACCORD_INFORM_DURABLE_REQ);
            mapping.put(MessageType.CHECK_STATUS_REQ,       Verb.ACCORD_CHECK_STATUS_REQ);
            mapping.put(MessageType.CHECK_STATUS_RSP,       Verb.ACCORD_CHECK_STATUS_RSP);
            mapping.put(MessageType.GET_DEPS_REQ,           Verb.ACCORD_GET_DEPS_REQ);
            mapping.put(MessageType.GET_DEPS_RSP,           Verb.ACCORD_GET_DEPS_RSP);
            mapping.put(MessageType.SIMPLE_RSP,             Verb.ACCORD_SIMPLE_RSP);

            for (MessageType type : MessageType.values())
            {
                if (!mapping.containsKey(type))
                    throw new AssertionError("Missing mapping for Accord MessageType " + type);
            }
        }
    }

    private static Verb getVerb(MessageType type)
    {
        return VerbMapping.instance.mapping.get(type);
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        Verb verb = getVerb(request.type());
        Objects.requireNonNull(verb, "verb");
        Message<Request> message = Message.out(verb, request);
        InetAddressAndPort endpoint = getEndpoint(to);
        logger.debug("Sending {} {} to {}", verb, message.payload, endpoint);
        MessagingService.instance().send(message, endpoint);
    }

    @Override
    public void send(Node.Id to, Request request, Callback callback)
    {
        Verb verb = getVerb(request.type());
        Preconditions.checkArgument(verb != null);
        Message<Request> message = Message.out(verb, request);
        InetAddressAndPort endpoint = getEndpoint(to);
        logger.debug("Sending {} {} to {}", verb, message.payload, endpoint);
        MessagingService.instance().sendWithCallback(message, endpoint, new AccordCallback<>((Callback<Reply>) callback));
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        Message<?> replyTo = (Message<?>) replyContext;
        Message<?> replyMsg = replyTo.responseWith(reply);
        Preconditions.checkArgument(replyMsg.verb() == getVerb(reply.type()));
        InetAddressAndPort endpoint = getEndpoint(replyingToNode);
        logger.debug("Replying {} {} to {}", replyMsg.verb(), replyMsg.payload, endpoint);
        MessagingService.instance().send(replyMsg, endpoint);
    }
}
