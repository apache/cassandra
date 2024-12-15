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
import accord.impl.RequestCallbacks;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.primitives.TxnId;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory;
import org.apache.cassandra.utils.Clock;

import static accord.messages.MessageType.Kind.REMOTE;
import static accord.primitives.Routable.Domain.Range;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class AccordMessageSink implements MessageSink
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMessageSink.class);

    public static final class AccordMessageType extends MessageType
    {
        public static final AccordMessageType INTEROP_READ_REQ           = remote("INTEROP_READ_REQ",           false);
        public static final AccordMessageType INTEROP_READ_RSP           = remote("INTEROP_READ_RSP",           false);
        public static final AccordMessageType INTEROP_READ_REPAIR_REQ    = remote("INTEROP_READ_REPAIR_REQ",    false);
        public static final AccordMessageType INTEROP_READ_REPAIR_RSP    = remote("INTEROP_READ_REPAIR_RSP",    false);
        public static final AccordMessageType INTEROP_COMMIT_MINIMAL_REQ = remote("INTEROP_COMMIT_MINIMAL_REQ", true );
        public static final AccordMessageType INTEROP_COMMIT_MAXIMAL_REQ = remote("INTEROP_COMMIT_MAXIMAL_REQ", true );
        public static final AccordMessageType INTEROP_APPLY_MINIMAL_REQ  = remote("INTEROP_APPLY_MINIMAL_REQ",  true );
        public static final AccordMessageType INTEROP_APPLY_MAXIMAL_REQ  = remote("INTEROP_APPLY_MAXIMAL_REQ",  true );

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

        protected static AccordMessageType remote(String name, boolean hasSideEffects)
        {
            return new AccordMessageType(name, REMOTE, hasSideEffects);
        }

        private AccordMessageType(String name, MessageType.Kind kind, boolean hasSideEffects)
        {
            super(name, kind, hasSideEffects);
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
            builder.put(MessageType.CALCULATE_DEPS_REQ,                       Verb.ACCORD_CALCULATE_DEPS_REQ);
            builder.put(MessageType.CALCULATE_DEPS_RSP,                       Verb.ACCORD_CALCULATE_DEPS_RSP);
            builder.put(MessageType.GET_LATEST_DEPS_REQ,                      Verb.ACCORD_GET_LATEST_DEPS_REQ);
            builder.put(MessageType.GET_LATEST_DEPS_RSP,                      Verb.ACCORD_GET_LATEST_DEPS_RSP);
            builder.put(MessageType.GET_EPHEMERAL_READ_DEPS_REQ,              Verb.ACCORD_GET_EPHMRL_READ_DEPS_REQ);
            builder.put(MessageType.GET_EPHEMERAL_READ_DEPS_RSP,              Verb.ACCORD_GET_EPHMRL_READ_DEPS_RSP);
            builder.put(MessageType.GET_MAX_CONFLICT_REQ,                     Verb.ACCORD_GET_MAX_CONFLICT_REQ);
            builder.put(MessageType.GET_MAX_CONFLICT_RSP,                     Verb.ACCORD_GET_MAX_CONFLICT_RSP);
            builder.put(MessageType.COMMIT_SLOW_PATH_REQ,                     Verb.ACCORD_COMMIT_REQ);
            builder.put(MessageType.COMMIT_MAXIMAL_REQ,                       Verb.ACCORD_COMMIT_REQ);
            builder.put(MessageType.STABLE_FAST_PATH_REQ,                     Verb.ACCORD_COMMIT_REQ);
            builder.put(MessageType.STABLE_SLOW_PATH_REQ,                     Verb.ACCORD_COMMIT_REQ);
            builder.put(MessageType.STABLE_MAXIMAL_REQ,                       Verb.ACCORD_COMMIT_REQ);
            builder.put(MessageType.COMMIT_INVALIDATE_REQ,                    Verb.ACCORD_COMMIT_INVALIDATE_REQ);
            builder.put(MessageType.APPLY_MINIMAL_REQ,                        Verb.ACCORD_APPLY_REQ);
            builder.put(MessageType.APPLY_MAXIMAL_REQ,                        Verb.ACCORD_APPLY_REQ);
            builder.put(MessageType.APPLY_RSP,                                Verb.ACCORD_APPLY_RSP);
            builder.put(MessageType.READ_REQ,                                 Verb.ACCORD_READ_REQ);
            builder.put(MessageType.READ_EPHEMERAL_REQ,                       Verb.ACCORD_READ_REQ);
            builder.put(MessageType.READ_RSP,                                 Verb.ACCORD_READ_RSP);
            builder.put(MessageType.BEGIN_RECOVER_REQ,                        Verb.ACCORD_BEGIN_RECOVER_REQ);
            builder.put(MessageType.BEGIN_RECOVER_RSP,                        Verb.ACCORD_BEGIN_RECOVER_RSP);
            builder.put(MessageType.BEGIN_INVALIDATE_REQ,                     Verb.ACCORD_BEGIN_INVALIDATE_REQ);
            builder.put(MessageType.BEGIN_INVALIDATE_RSP,                     Verb.ACCORD_BEGIN_INVALIDATE_RSP);
            builder.put(MessageType.AWAIT_REQ,                                Verb.ACCORD_AWAIT_REQ);
            builder.put(MessageType.AWAIT_RSP,                                Verb.ACCORD_AWAIT_RSP);
            builder.put(MessageType.ASYNC_AWAIT_COMPLETE_REQ,                 Verb.ACCORD_AWAIT_ASYNC_RSP_REQ);
            builder.put(MessageType.WAIT_UNTIL_APPLIED_REQ,                   Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ);
            builder.put(MessageType.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ,        Verb.ACCORD_APPLY_AND_WAIT_REQ);
            builder.put(MessageType.INFORM_DURABLE_REQ,                       Verb.ACCORD_INFORM_DURABLE_REQ);
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
            builder.put(AccordMessageType.INTEROP_APPLY_MINIMAL_REQ,          Verb.ACCORD_INTEROP_APPLY_REQ);
            builder.put(AccordMessageType.INTEROP_APPLY_MAXIMAL_REQ,          Verb.ACCORD_INTEROP_APPLY_REQ);
            mapping = builder.build();

            for (MessageType type : Iterables.concat(AccordMessageType.values, MessageType.values))
            {
                // Any request can receive a generic failure response
                if (type == MessageType.FAILURE_RSP || type.isLocal())
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
            return getVerb(type);

        return null;
    }

    private final Agent agent;
    private final MessageDelivery messaging;
    private final AccordEndpointMapper endpointMapper;
    private final RequestCallbacks callbacks;
    // TODO (required): make hot property
    private TimeoutStrategy slowPreaccept, slowRead;

    public AccordMessageSink(Agent agent, MessageDelivery messaging, AccordEndpointMapper endpointMapper, RequestCallbacks callbacks)
    {
        AccordSpec config = DatabaseDescriptor.getAccord();
        if (config != null)
        {
            // TODO (expected): introduce better metrics, esp. for preaccept, but also to disambiguate DC latencies
            slowPreaccept = new TimeoutStrategy(config.slowPreAccept, LatencySourceFactory.of(ClientRequestsMetricsHolder.accordReadMetrics));
            slowRead = new TimeoutStrategy(config.slowRead, LatencySourceFactory.of(ClientRequestsMetricsHolder.accordReadMetrics));
        }
        this.agent = agent;
        this.messaging = messaging;
        this.endpointMapper = endpointMapper;
        this.callbacks = callbacks;
    }

    public AccordMessageSink(Agent agent, AccordConfigurationService endpointMapper, RequestCallbacks callbacks)
    {
        this(agent, MessagingService.instance(), endpointMapper, callbacks);
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        Verb verb = getVerb(request);
        Preconditions.checkNotNull(verb, "Verb is null for type %s", request.type());
        Message<Request> message = Message.out(verb, request);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(to);
        logger.trace("Sending {} {} to {}", verb, message.payload, endpoint);
        messaging.send(message, endpoint);
    }

    private static boolean isRangeBarrier(Request request)
    {
        TxnId txnId = null;
        if (request instanceof TxnRequest)
        {
            TxnRequest<?> txnRequest = (TxnRequest<?>) request;
            txnId = txnRequest.txnId;
        }
        else if (request instanceof ReadData)
        {
            ReadData epochRequest = (ReadData)request;
            txnId = epochRequest.txnId;
        }

        return txnId != null  && txnId.isSyncPoint() && txnId.is(Range);
    }

    // TODO (expected): permit bulk send to save esp. on callback registration (and combine records)
    @Override
    public void send(Node.Id to, Request request, AgentExecutor executor, Callback callback)
    {
        Verb verb = getVerb(request);
        Preconditions.checkNotNull(verb, "Verb is null for type %s", request.type());

        long nowNanos = Clock.Global.nanoTime();
        long delayedAtNanos = Long.MAX_VALUE;
        long expiresAtNanos;
        if (isRangeBarrier(request))
            expiresAtNanos = nowNanos + DatabaseDescriptor.getAccordRangeSyncPointTimeoutNanos();
        else
            expiresAtNanos = nowNanos + verb.expiresAfterNanos();

        switch (verb)
        {
            case ACCORD_COMMIT_REQ:
                if (((Commit)request).readData == null)
                    break;

            case ACCORD_READ_REQ:
                if (slowRead == null || isRangeBarrier(request))
                    break;

            case ACCORD_CHECK_STATUS_REQ:
                delayedAtNanos = nowNanos + slowRead.computeWait(1, NANOSECONDS);
                break;

            case ACCORD_PRE_ACCEPT_REQ:
                if (slowPreaccept == null || isRangeBarrier(request))
                    break;
                delayedAtNanos = nowNanos + slowPreaccept.computeWait(1, NANOSECONDS);
        }

        Message<Request> message = Message.out(verb, request, expiresAtNanos);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(to);
        logger.trace("Sending {} {} to {}", verb, message.payload, endpoint);
        callbacks.registerAt(message.id(), executor, callback, to, nowNanos, delayedAtNanos, expiresAtNanos, NANOSECONDS);
        messaging.send(message, endpoint);
    }

    @Override
    public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
    {
        ResponseContext respondTo = (ResponseContext) replyContext;
        Message<?> responseMsg = Message.responseWith(reply, respondTo);
        if (!reply.isFinal())
            responseMsg = responseMsg.withFlag(MessageFlag.NOT_FINAL);
        checkReplyType(reply, respondTo);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(replyingToNode);
        logger.trace("Replying {} {} to {}", responseMsg.verb(), responseMsg.payload, endpoint);
        messaging.send(responseMsg, endpoint);
    }

    @Override
    public void replyWithUnknownFailure(Node.Id replyingToNode, ReplyContext replyContext, Throwable failure)
    {
        ResponseContext respondTo = (ResponseContext) replyContext;
        Message<?> responseMsg = Message.failureResponse(RequestFailureReason.UNKNOWN, failure, respondTo);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpoint(replyingToNode);
        logger.trace("Replying with failure {} {} to {}", responseMsg.verb(), responseMsg.payload, endpoint);
        messaging.send(responseMsg, endpoint);
    }

    private static void checkReplyType(Reply reply, ResponseContext respondTo)
    {
        Verb verb = getVerb(reply.type());
        Preconditions.checkNotNull(verb, "Verb is null for type %s", reply.type());
        Set<Verb> allowedVerbs = expectedReplyTypes(respondTo.verb());
        Preconditions.checkArgument(allowedVerbs.contains(verb), "Expected reply message with verbs %s but got %s; reply type was %s, request verb was %s", allowedVerbs, verb, reply.type(), respondTo.verb());
    }

    private static Set<Verb> expectedReplyTypes(Verb verb)
    {
        Set<Verb> extra = VerbMapping.instance.overrideReplyVerbs.get(verb);
        if (extra != null) return extra;
        Verb v = verb.responseVerb;
        return v == null ? Collections.emptySet() : Collections.singleton(v);
    }
}
