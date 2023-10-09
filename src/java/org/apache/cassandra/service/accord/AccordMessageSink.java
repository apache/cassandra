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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.MessageSink;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;

import static accord.messages.MessageType.Kind.REMOTE;

public class AccordMessageSink implements MessageSink
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMessageSink.class);

    public static final class AccordMessageType extends MessageType
    {
        public static final MessageType INTEROP_READ_REQ                = amt(REMOTE, false);
        public static final MessageType INTEROP_READ_RSP                = amt(REMOTE, false);
        public static final MessageType INTEROP_READ_REPAIR_REQ         = amt(REMOTE, false);
        public static final MessageType INTEROP_READ_REPAIR_RSP         = amt(REMOTE, false);
        public static final MessageType INTEROP_COMMIT_MINIMAL_REQ      = amt(REMOTE, true );
        public static final MessageType INTEROP_COMMIT_MAXIMAL_REQ      = amt(REMOTE, true );
        public static final MessageType INTEROP_APPLY_MINIMAL_REQ       = amt(REMOTE, true );
        public static final MessageType INTEROP_APPLY_MAXIMAL_REQ       = amt(REMOTE, true );


        public static final List<MessageType> values;

        static
        {
            ImmutableList.Builder<MessageType> builder = ImmutableList.builder();
            for (Field f : AccordMessageType.class.getDeclaredFields())
            {
                if (f.getType().equals(AccordMessageType.class) && Modifier.isStatic(f.getModifiers()))
                {
                    try
                    {
                        builder.add((MessageType) f.get(null));
                    }
                    catch (IllegalAccessException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
            values = builder.build();
        }

        private static MessageType amt(MessageType.Kind kind, boolean hasSideEffects)
        {
            return new AccordMessageType(kind, hasSideEffects);
        }

        private AccordMessageType(MessageType.Kind kind, boolean hasSideEffects)
        {
            super(kind, hasSideEffects);
        }
    }

    private static class VerbMapping
    {
        private static final VerbMapping instance = new VerbMapping();

        private final Map<MessageType, Verb> mapping;
        private final Map<Verb, Set<Verb>> overrideReplyVerbs = ImmutableMap.<Verb, Set<Verb>>builder()
                                                                            // read takes Result | Nack
                                                                            .put(Verb.ACCORD_FETCH_DATA_REQ, EnumSet.of(Verb.ACCORD_FETCH_DATA_RSP, Verb.ACCORD_READ_RSP /* nack */))
                                                                            .put(Verb.ACCORD_INTEROP_COMMIT_REQ, EnumSet.of(Verb.ACCORD_INTEROP_READ_RSP, Verb.ACCORD_READ_RSP))
                                                                            .put(Verb.ACCORD_INTEROP_READ_REPAIR_REQ, EnumSet.of(Verb.ACCORD_INTEROP_READ_REPAIR_RSP, Verb.ACCORD_READ_RSP))
                                                                            .build();

        private VerbMapping()
        {
            ImmutableMap.Builder<MessageType, Verb> builder = ImmutableMap.builder();
            builder.put(MessageType.SIMPLE_RSP,                               Verb.ACCORD_SIMPLE_RSP);
            builder.put(MessageType.PRE_ACCEPT_REQ,                           Verb.ACCORD_PRE_ACCEPT_REQ);
            builder.put(MessageType.PRE_ACCEPT_RSP,                           Verb.ACCORD_PRE_ACCEPT_RSP);
            builder.put(MessageType.ACCEPT_REQ,                               Verb.ACCORD_ACCEPT_REQ);
            builder.put(MessageType.ACCEPT_RSP,                               Verb.ACCORD_ACCEPT_RSP);
            builder.put(MessageType.ACCEPT_INVALIDATE_REQ,                    Verb.ACCORD_ACCEPT_INVALIDATE_REQ);
            builder.put(MessageType.GET_DEPS_REQ,                             Verb.ACCORD_GET_DEPS_REQ);
            builder.put(MessageType.GET_DEPS_RSP,                             Verb.ACCORD_GET_DEPS_RSP);
            builder.put(MessageType.COMMIT_MINIMAL_REQ,                       Verb.ACCORD_COMMIT_REQ);
            builder.put(MessageType.COMMIT_MAXIMAL_REQ,                       Verb.ACCORD_COMMIT_REQ);
            builder.put(MessageType.COMMIT_INVALIDATE_REQ,                    Verb.ACCORD_COMMIT_INVALIDATE_REQ);
            builder.put(MessageType.APPLY_MINIMAL_REQ,                        Verb.ACCORD_APPLY_REQ);
            builder.put(MessageType.APPLY_MAXIMAL_REQ,                        Verb.ACCORD_APPLY_REQ);
            builder.put(MessageType.APPLY_RSP,                                Verb.ACCORD_APPLY_RSP);
            builder.put(MessageType.READ_REQ,                                 Verb.ACCORD_READ_REQ);
            builder.put(MessageType.READ_RSP,                                 Verb.ACCORD_READ_RSP);
            builder.put(MessageType.BEGIN_RECOVER_REQ,                        Verb.ACCORD_BEGIN_RECOVER_REQ);
            builder.put(MessageType.BEGIN_RECOVER_RSP,                        Verb.ACCORD_BEGIN_RECOVER_RSP);
            builder.put(MessageType.BEGIN_INVALIDATE_REQ,                     Verb.ACCORD_BEGIN_INVALIDATE_REQ);
            builder.put(MessageType.BEGIN_INVALIDATE_RSP,                     Verb.ACCORD_BEGIN_INVALIDATE_RSP);
            builder.put(MessageType.WAIT_ON_COMMIT_REQ,                       Verb.ACCORD_WAIT_ON_COMMIT_REQ);
            builder.put(MessageType.WAIT_ON_COMMIT_RSP,                       Verb.ACCORD_WAIT_ON_COMMIT_RSP);
            builder.put(MessageType.WAIT_UNTIL_APPLIED_REQ,                   Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ);
            builder.put(MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ,        Verb.ACCORD_APPLY_AND_WAIT_UNTIL_APPLIED_REQ);
            builder.put(MessageType.INFORM_OF_TXN_REQ,                        Verb.ACCORD_INFORM_OF_TXN_REQ);
            builder.put(MessageType.INFORM_DURABLE_REQ,                       Verb.ACCORD_INFORM_DURABLE_REQ);
            builder.put(MessageType.INFORM_HOME_DURABLE_REQ,                  Verb.ACCORD_INFORM_HOME_DURABLE_REQ);
            builder.put(MessageType.CHECK_STATUS_REQ,                         Verb.ACCORD_CHECK_STATUS_REQ);
            builder.put(MessageType.CHECK_STATUS_RSP,                         Verb.ACCORD_CHECK_STATUS_RSP);
            builder.put(MessageType.FETCH_DATA_REQ,                           Verb.ACCORD_FETCH_DATA_REQ);
            builder.put(MessageType.FETCH_DATA_RSP,                           Verb.ACCORD_FETCH_DATA_RSP);
            builder.put(MessageType.SET_SHARD_DURABLE_REQ,                    Verb.ACCORD_SET_SHARD_DURABLE_REQ);
            builder.put(MessageType.SET_GLOBALLY_DURABLE_REQ,                 Verb.ACCORD_SET_GLOBALLY_DURABLE_REQ);
            builder.put(MessageType.QUERY_DURABLE_BEFORE_REQ,                 Verb.ACCORD_QUERY_DURABLE_BEFORE_REQ);
            builder.put(MessageType.QUERY_DURABLE_BEFORE_RSP,                 Verb.ACCORD_QUERY_DURABLE_BEFORE_RSP);
            builder.put(AccordMessageType.INTEROP_READ_REQ,                   Verb.ACCORD_INTEROP_READ_REQ);
            builder.put(AccordMessageType.INTEROP_READ_RSP,                   Verb.ACCORD_INTEROP_READ_RSP);
            builder.put(AccordMessageType.INTEROP_READ_REPAIR_REQ,            Verb.ACCORD_INTEROP_READ_REPAIR_REQ);
            builder.put(AccordMessageType.INTEROP_READ_REPAIR_RSP,            Verb.ACCORD_INTEROP_READ_REPAIR_RSP);
            builder.put(AccordMessageType.INTEROP_COMMIT_MINIMAL_REQ,         Verb.ACCORD_INTEROP_COMMIT_REQ);
            builder.put(AccordMessageType.INTEROP_COMMIT_MAXIMAL_REQ,         Verb.ACCORD_INTEROP_COMMIT_REQ);
            builder.put(AccordMessageType.INTEROP_APPLY_MINIMAL_REQ,          Verb.ACCORD_APPLY_REQ);
            builder.put(AccordMessageType.INTEROP_APPLY_MAXIMAL_REQ,          Verb.ACCORD_APPLY_REQ);
            mapping = builder.build();

            for (MessageType type : Iterables.concat(AccordMessageType.values, MessageType.values))
            {
                // Any request can receive a generic failure response
                if (type == MessageType.FAILURE_RSP)
                    continue;

                if (mapping.containsKey(type))
                {
                    if (type.isLocal()) throw new AssertionError("Extraneous mapping for LOCAL Accord MessageType " + type);
                }
                else
                {
                    if (type.isRemote()) throw new AssertionError("Missing mapping for REMOTE Accord MessageType " + type);
                }
            }
        }
    }

    private static Verb getVerb(MessageType type)
    {
        return VerbMapping.instance.mapping.get(type);
    }

    private static Verb getVerb(Request request)
    {
        MessageType type = request.type();
        if (type != null)
            return getVerb(request.type());

        return null;
    }

    private final Agent agent;
    private final MessageDelivery messaging;
    private final AccordEndpointMapper endpointMapper;

    public AccordMessageSink(Agent agent, MessageDelivery messaging, AccordEndpointMapper endpointMapper)
    {
        this.agent = agent;
        this.messaging = messaging;
        this.endpointMapper = endpointMapper;
    }

    public AccordMessageSink(Agent agent, AccordConfigurationService endpointMapper)
    {
        this(agent, MessagingService.instance(), endpointMapper);
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        Verb verb = getVerb(request);
        Preconditions.checkNotNull(verb, "Verb is null for type %s", request.type());
        Message<Request> message = Message.out(verb, request);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(to);
        logger.debug("Sending {} {} to {}", verb, message.payload, endpoint);
        messaging.send(message, endpoint);
    }

    @Override
    public void send(Node.Id to, Request request, AgentExecutor executor, Callback callback)
    {
        Verb verb = getVerb(request);
        Preconditions.checkNotNull(verb, "Verb is null for type %s", request.type());
        Message<Request> message = Message.out(verb, request);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(to);
        logger.debug("Sending {} {} to {}", verb, message.payload, endpoint);
        messaging.sendWithCallback(message, endpoint, new AccordCallback<>(executor, (Callback<Reply>) callback, endpointMapper));
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        Message<?> replyTo = (Message<?>) replyContext;
        Message<?> replyMsg = replyTo.responseWith(reply);
        if (!reply.isFinal())
            replyMsg = replyMsg.withFlag(MessageFlag.NOT_FINAL);
        checkReplyType(reply, replyTo);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(replyingToNode);
        logger.debug("Replying {} {} to {}", replyMsg.verb(), replyMsg.payload, endpoint);
        messaging.send(replyMsg, endpoint);
    }

    @Override
    public void replyWithUnknownFailure(Node.Id replyingToNode, ReplyContext replyContext, Throwable failure)
    {
        Message<?> replyTo = (Message<?>) replyContext;
        Message<?> replyMsg = replyTo.failureResponse(RequestFailureReason.UNKNOWN, failure);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(replyingToNode);
        logger.debug("Replying with failure {} {} to {}", replyMsg.verb(), replyMsg.payload, endpoint);
        messaging.send(replyMsg, endpoint);
    }

    private static void checkReplyType(Reply reply, Message<?> replyTo)
    {
        Verb verb = getVerb(reply.type());
        Preconditions.checkNotNull(verb, "Verb is null for type %s", reply.type());
        Set<Verb> allowedVerbs = expectedReplyTypes(replyTo.verb());
        Preconditions.checkArgument(allowedVerbs.contains(verb), "Expected reply message with verbs %s but got %s; reply type was %s, request verb was %s", allowedVerbs, verb, reply.type(), replyTo.verb());
    }

    private static Set<Verb> expectedReplyTypes(Verb verb)
    {
        Set<Verb> extra = VerbMapping.instance.overrideReplyVerbs.get(verb);
        if (extra != null) return extra;
        Verb v = verb.responseVerb;
        return v == null ? Collections.emptySet() : Collections.singleton(v);
    }
}
