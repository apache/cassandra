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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import accord.api.AsyncExecutor;
import accord.api.MessageSink;
import accord.api.Tracing;
import accord.impl.RequestCallbacks;
import accord.local.MapReduceCommandStores;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.service.accord.topology.AccordEndpointMapper;

import static accord.messages.MessageType.StandardMessage.ACCEPT_REQ;
import static accord.messages.MessageType.StandardMessage.ACCEPT_RSP;
import static accord.messages.MessageType.StandardMessage.APPLY_REQ;
import static accord.messages.MessageType.StandardMessage.APPLY_RSP;
import static accord.messages.MessageType.StandardMessage.APPLY_THEN_WAIT_UNTIL_APPLIED_REQ;
import static accord.messages.MessageType.StandardMessage.ASYNC_AWAIT_COMPLETE_REQ;
import static accord.messages.MessageType.StandardMessage.AWAIT_REQ;
import static accord.messages.MessageType.StandardMessage.AWAIT_RSP;
import static accord.messages.MessageType.StandardMessage.BEGIN_INVALIDATE_REQ;
import static accord.messages.MessageType.StandardMessage.BEGIN_INVALIDATE_RSP;
import static accord.messages.MessageType.StandardMessage.BEGIN_RECOVER_REQ;
import static accord.messages.MessageType.StandardMessage.BEGIN_RECOVER_RSP;
import static accord.messages.MessageType.StandardMessage.CHECK_STATUS_REQ;
import static accord.messages.MessageType.StandardMessage.CHECK_STATUS_RSP;
import static accord.messages.MessageType.StandardMessage.COMMIT_INVALIDATE_REQ;
import static accord.messages.MessageType.StandardMessage.COMMIT_REQ;
import static accord.messages.MessageType.StandardMessage.FAILURE_RSP;
import static accord.messages.MessageType.StandardMessage.FETCH_DATA_REQ;
import static accord.messages.MessageType.StandardMessage.FETCH_DATA_RSP;
import static accord.messages.MessageType.StandardMessage.GET_DURABLE_BEFORE_REQ;
import static accord.messages.MessageType.StandardMessage.GET_DURABLE_BEFORE_RSP;
import static accord.messages.MessageType.StandardMessage.GET_EPHEMERAL_READ_DEPS_REQ;
import static accord.messages.MessageType.StandardMessage.GET_EPHEMERAL_READ_DEPS_RSP;
import static accord.messages.MessageType.StandardMessage.GET_LATEST_DEPS_REQ;
import static accord.messages.MessageType.StandardMessage.GET_LATEST_DEPS_RSP;
import static accord.messages.MessageType.StandardMessage.GET_MAX_CONFLICT_REQ;
import static accord.messages.MessageType.StandardMessage.GET_MAX_CONFLICT_RSP;
import static accord.messages.MessageType.StandardMessage.INFORM_DURABLE_REQ;
import static accord.messages.MessageType.StandardMessage.NOT_ACCEPT_REQ;
import static accord.messages.MessageType.StandardMessage.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.StandardMessage.PRE_ACCEPT_RSP;
import static accord.messages.MessageType.StandardMessage.READ_EPHEMERAL_REQ;
import static accord.messages.MessageType.StandardMessage.READ_REQ;
import static accord.messages.MessageType.StandardMessage.READ_RSP;
import static accord.messages.MessageType.StandardMessage.RECOVER_AWAIT_REQ;
import static accord.messages.MessageType.StandardMessage.RECOVER_AWAIT_RSP;
import static accord.messages.MessageType.StandardMessage.REMOTE_SUCCESS_REQ;
import static accord.messages.MessageType.StandardMessage.SET_GLOBALLY_DURABLE_REQ;
import static accord.messages.MessageType.StandardMessage.SET_SHARD_DURABLE_REQ;
import static accord.messages.MessageType.StandardMessage.SIMPLE_RSP;
import static accord.messages.MessageType.StandardMessage.STABLE_THEN_READ_REQ;
import static accord.messages.MessageType.StandardMessage.StandardMessage;
import static accord.messages.MessageType.StandardMessage.WAIT_UNTIL_APPLIED_REQ;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.expire;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.slowPreaccept;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.slowRead;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordMessageSink implements MessageSink
{
    public enum AccordMessageType implements MessageType
    {
        INTEROP_READ_REQ(Verb.ACCORD_INTEROP_READ_REQ),
        INTEROP_STABLE_THEN_READ_REQ(Verb.ACCORD_INTEROP_STABLE_THEN_READ_REQ),
        INTEROP_READ_RSP(Verb.ACCORD_INTEROP_READ_RSP),
        INTEROP_READ_REPAIR_REQ(Verb.ACCORD_INTEROP_READ_REPAIR_REQ),
        INTEROP_READ_REPAIR_RSP(Verb.ACCORD_INTEROP_READ_REPAIR_RSP),
        INTEROP_APPLY_REQ(Verb.ACCORD_INTEROP_APPLY_REQ);
        final Verb verb;

        AccordMessageType(Verb verb)
        {
            this.verb = verb;
        }
    }

    private static class VerbMapping
    {
        private static final Map<Verb, Set<Verb>> overrideReplyVerbs = ImmutableMap.<Verb, Set<Verb>>builder()
                                                                                   // read takes Result | Nack
                                                                                   .put(Verb.ACCORD_FETCH_DATA_REQ, EnumSet.of(Verb.ACCORD_FETCH_DATA_RSP, Verb.ACCORD_READ_RSP /* nack */))
                                                                                   .put(Verb.ACCORD_INTEROP_STABLE_THEN_READ_REQ, EnumSet.of(Verb.ACCORD_INTEROP_READ_RSP, Verb.ACCORD_READ_RSP))
                                                                                   .put(Verb.ACCORD_INTEROP_READ_REPAIR_REQ, EnumSet.of(Verb.ACCORD_INTEROP_READ_REPAIR_RSP, Verb.ACCORD_READ_RSP))
                                                                                   .build();

        static
        {
            ImmutableMap.Builder<StandardMessage, Verb> builder = ImmutableMap.builder();
            builder.put(SIMPLE_RSP,                               Verb.ACCORD_SIMPLE_RSP);
            builder.put(PRE_ACCEPT_REQ,                           Verb.ACCORD_PRE_ACCEPT_REQ);
            builder.put(PRE_ACCEPT_RSP,                           Verb.ACCORD_PRE_ACCEPT_RSP);
            builder.put(ACCEPT_REQ,                               Verb.ACCORD_ACCEPT_REQ);
            builder.put(ACCEPT_RSP,                               Verb.ACCORD_ACCEPT_RSP);
            builder.put(NOT_ACCEPT_REQ,                           Verb.ACCORD_NOT_ACCEPT_REQ);
            builder.put(RECOVER_AWAIT_REQ,                        Verb.ACCORD_RECOVER_AWAIT_REQ);
            builder.put(RECOVER_AWAIT_RSP,                        Verb.ACCORD_RECOVER_AWAIT_RSP);
            builder.put(GET_LATEST_DEPS_REQ,                      Verb.ACCORD_GET_LATEST_DEPS_REQ);
            builder.put(GET_LATEST_DEPS_RSP,                      Verb.ACCORD_GET_LATEST_DEPS_RSP);
            builder.put(GET_EPHEMERAL_READ_DEPS_REQ,              Verb.ACCORD_GET_EPHMRL_READ_DEPS_REQ);
            builder.put(GET_EPHEMERAL_READ_DEPS_RSP,              Verb.ACCORD_GET_EPHMRL_READ_DEPS_RSP);
            builder.put(GET_MAX_CONFLICT_REQ,                     Verb.ACCORD_GET_MAX_CONFLICT_REQ);
            builder.put(GET_MAX_CONFLICT_RSP,                     Verb.ACCORD_GET_MAX_CONFLICT_RSP);
            builder.put(COMMIT_REQ,                               Verb.ACCORD_COMMIT_REQ);
            builder.put(COMMIT_INVALIDATE_REQ,                    Verb.ACCORD_COMMIT_INVALIDATE_REQ);
            builder.put(APPLY_REQ,                                Verb.ACCORD_APPLY_REQ);
            builder.put(APPLY_RSP,                                Verb.ACCORD_APPLY_RSP);
            builder.put(READ_REQ,                                 Verb.ACCORD_READ_REQ);
            builder.put(STABLE_THEN_READ_REQ,                     Verb.ACCORD_STABLE_THEN_READ_REQ);
            builder.put(READ_EPHEMERAL_REQ,                       Verb.ACCORD_READ_REQ);
            builder.put(READ_RSP,                                 Verb.ACCORD_READ_RSP);
            builder.put(BEGIN_RECOVER_REQ,                        Verb.ACCORD_BEGIN_RECOVER_REQ);
            builder.put(BEGIN_RECOVER_RSP,                        Verb.ACCORD_BEGIN_RECOVER_RSP);
            builder.put(BEGIN_INVALIDATE_REQ,                     Verb.ACCORD_BEGIN_INVALIDATE_REQ);
            builder.put(BEGIN_INVALIDATE_RSP,                     Verb.ACCORD_BEGIN_INVALIDATE_RSP);
            builder.put(AWAIT_REQ,                                Verb.ACCORD_AWAIT_REQ);
            builder.put(AWAIT_RSP,                                Verb.ACCORD_AWAIT_RSP);
            builder.put(ASYNC_AWAIT_COMPLETE_REQ,                 Verb.ACCORD_AWAIT_ASYNC_RSP_REQ);
            builder.put(WAIT_UNTIL_APPLIED_REQ,                   Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ);
            builder.put(APPLY_THEN_WAIT_UNTIL_APPLIED_REQ,        Verb.ACCORD_APPLY_AND_WAIT_REQ);
            builder.put(INFORM_DURABLE_REQ,                       Verb.ACCORD_INFORM_DURABLE_REQ);
            builder.put(CHECK_STATUS_REQ,                         Verb.ACCORD_CHECK_STATUS_REQ);
            builder.put(CHECK_STATUS_RSP,                         Verb.ACCORD_CHECK_STATUS_RSP);
            builder.put(FETCH_DATA_REQ,                           Verb.ACCORD_FETCH_DATA_REQ);
            builder.put(FETCH_DATA_RSP,                           Verb.ACCORD_FETCH_DATA_RSP);
            builder.put(SET_SHARD_DURABLE_REQ,                    Verb.ACCORD_SET_SHARD_DURABLE_REQ);
            builder.put(SET_GLOBALLY_DURABLE_REQ,                 Verb.ACCORD_SET_GLOBALLY_DURABLE_REQ);
            builder.put(GET_DURABLE_BEFORE_REQ,                   Verb.ACCORD_GET_DURABLE_BEFORE_REQ);
            builder.put(GET_DURABLE_BEFORE_RSP,                   Verb.ACCORD_GET_DURABLE_BEFORE_RSP);
            builder.put(REMOTE_SUCCESS_REQ,                       Verb.ACCORD_REMOTE_SUCCESS_REQ);
            builder.put(FAILURE_RSP,                              Verb.FAILURE_RSP);
            Map<StandardMessage, Verb> mapping = builder.build();
            StandardMessage.initialise(mapping);
        }

        private static Verb getVerb(MessageType type)
        {
            if (type.getClass() == StandardMessage.class)
                return (Verb) ((StandardMessage) type).mapToImplementation();
            return ((AccordMessageType)type).verb;
        }

        private static Verb getVerb(Request request)
        {
            MessageType type = request.type();
            if (type != null)
                return getVerb(type);
            return null;
        }
    }

    private final MessageDelivery messaging;
    private final AccordEndpointMapper endpointMapper;
    private final RequestCallbacks callbacks;

    public AccordMessageSink(MessageDelivery messaging, AccordEndpointMapper endpointMapper, RequestCallbacks callbacks)
    {
        this.messaging = messaging;
        this.endpointMapper = endpointMapper;
        this.callbacks = callbacks;
    }

    public AccordMessageSink(AccordEndpointMapper endpointMapper, RequestCallbacks callbacks)
    {
        this(MessagingService.instance(), endpointMapper, callbacks);
    }

    @Override
    public void send(Node.Id to, Request request)
    {
        Verb verb = VerbMapping.getVerb(request);
        Preconditions.checkNotNull(verb, "Verb is null for type %s", request.type());

        Message<Request> message = Message.out(verb, request);
        InetAddressAndPort endpoint = endpointMapper.mappedEndpointOrNull(to, message);
        if (endpoint == null)
            return;

        messaging.send(message, endpoint);
    }

    // TODO (expected): permit bulk send to save esp. on callback registration (and combine records)
    @Override
    public Cancellable send(Node.Id to, Request request, int attempt, AsyncExecutor executor, Callback callback)
    {
        Verb verb = VerbMapping.getVerb(request);
        Preconditions.checkNotNull(verb, "Verb is null for type %s", request.type());

        long nowNanos = nanoTime();
        TxnId txnId = request.primaryTxnId();
        long slowAtNanos = Long.MAX_VALUE;
        long expiresAtNanos = nowNanos + expire(txnId, verb).computeWait(attempt, NANOSECONDS);

        switch (verb)
        {
            case ACCORD_READ_REQ:
            case ACCORD_STABLE_THEN_READ_REQ:
            case ACCORD_CHECK_STATUS_REQ:
            {
                TimeoutStrategy slow = slowRead(txnId);
                if (slow != null)
                    slowAtNanos = nowNanos + slow.computeWait(attempt, NANOSECONDS);
                break;
            }

            case ACCORD_PRE_ACCEPT_REQ:
            {
                TimeoutStrategy slow = slowPreaccept(txnId);
                if (slow != null)
                    slowAtNanos = nowNanos + slow.computeWait(attempt, NANOSECONDS);
                break;
            }

            case ACCORD_WAIT_UNTIL_APPLIED_REQ:
            {
                TimeoutStrategy slow = slowPreaccept(txnId);
                if (slow != null)
                    slowAtNanos = nowNanos + slow.computeWait(attempt, NANOSECONDS);
                break;
            }
        }

        Message<Request> message = Message.out(verb, request, expiresAtNanos);
        if (request instanceof MapReduceCommandStores<?, ?>)
        {
            Tracing tracing =  ((MapReduceCommandStores<?, ?>) request).tracing();
            if (tracing != null) tracing = tracing.send();
            if (tracing != null) message = message.withParam(ParamType.ACCORD_TRACING, tracing);
        }

        InetAddressAndPort endpoint = endpointMapper.mappedEndpointOrNull(to, message);
        if (endpoint == null)
        {
            executor.execute(() -> callback.onFailure(to, null));
            return null;
        }

        Cancellable cancellable = callbacks.registerAt(message.id(), executor, callback, to, nowNanos, slowAtNanos, expiresAtNanos, NANOSECONDS);
        messaging.send(message, endpoint);
        return cancellable;
    }

    public void reply(Node.Id replyingTo, ReplyContext replyContext, Reply reply, Throwable failure)
    {
        ResponseContext respondTo = (ResponseContext) replyContext;
        Message<?> message;
        if (failure != null) message = Message.failureResponse(RequestFailureReason.UNKNOWN, failure, respondTo);
        else
        {
            message = Message.responseWith(reply, respondTo);
            if (Invariants.isParanoid()) checkReplyType(reply, respondTo);
        }
        Object tracing = respondTo.params().get(ParamType.ACCORD_TRACING);
        if (tracing != null)
        {
            tracing = ((Tracing)tracing).send();
            if (tracing != null)
                message = message.withParam(ParamType.ACCORD_TRACING, tracing);
        }

        InetAddressAndPort endpoint = endpointMapper.mappedEndpointOrNull(replyingTo, message);
        if (endpoint == null)
            return;

        messaging.send(message, endpoint);
    }

    private static void checkReplyType(Reply reply, ResponseContext respondTo)
    {
        Verb verb = VerbMapping.getVerb(reply.type());
        Preconditions.checkNotNull(verb, "Verb is null for type %s", reply.type());
        Set<Verb> allowedVerbs = expectedReplyTypes(respondTo.verb());
        Preconditions.checkArgument(allowedVerbs.contains(verb), "Expected reply message with verbs %s but got %s; reply type was %s, request verb was %s", allowedVerbs, verb, reply.type(), respondTo.verb());
    }

    private static Set<Verb> expectedReplyTypes(Verb verb)
    {
        Set<Verb> extra = VerbMapping.overrideReplyVerbs.get(verb);
        if (extra != null) return extra;
        Verb v = verb.responseVerb;
        return v == null ? Collections.emptySet() : Collections.singleton(v);
    }
}
