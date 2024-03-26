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

package org.apache.cassandra.net;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;

public interface MessageDelivery
{
    Logger logger = LoggerFactory.getLogger(MessageDelivery.class);
    static <REQ, RSP> Collection<Pair<InetAddressAndPort, RSP>> fanoutAndWait(MessageDelivery messaging, Set<InetAddressAndPort> sendTo, Verb verb, REQ payload)
    {
        return fanoutAndWait(messaging, sendTo, verb, payload, DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }
    static <REQ, RSP> Collection<Pair<InetAddressAndPort, RSP>> fanoutAndWait(MessageDelivery messaging, Set<InetAddressAndPort> sendTo, Verb verb, REQ payload, long timeout, TimeUnit timeUnit)
    {
        Accumulator<Pair<InetAddressAndPort, RSP>> responses = new Accumulator<>(sendTo.size());
        CountDownLatch cdl = CountDownLatch.newCountDownLatch(sendTo.size());
        RequestCallback<RSP> callback = new RequestCallbackWithFailure<>()
        {
            @Override
            public void onResponse(Message<RSP> msg)
            {
                logger.info("Received a {} response from {}: {}", msg.verb(), msg.from(), msg.payload);
                responses.add(Pair.create(msg.from(), msg.payload));
                cdl.decrement();
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
            {
                logger.info("Received failure in response to {} from {}: {}", verb, from, reason);
                cdl.decrement();
            }
        };

        sendTo.forEach((ep) -> {
            logger.info("Election for metadata migration sending {} ({}) to {}", verb, payload.toString(), ep);
            messaging.sendWithCallback(Message.out(verb, payload), ep, callback);
        });
        cdl.awaitUninterruptibly(timeout, timeUnit);
        return responses.snapshot();
    }

    public <REQ> void send(Message<REQ> message, InetAddressAndPort to);
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb);
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection);
    public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to);
    public <V> void respond(V response, Message<?> message);
    public default void respondWithFailure(RequestFailureReason reason, Message<?> message)
    {
        send(Message.failureResponse(message.id(), message.expiresAtNanos(), reason), message.respondTo());
    }
}
