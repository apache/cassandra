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

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Sends a response for an incoming message with a matching {@link Matcher}.
 * The actual behavior by any instance of this class can be inspected by
 * interacting with the returned {@link MockMessagingSpy}.
 */
public class MatcherResponse
{
    private final Matcher<?> matcher;
    private final Set<Integer> sendResponses = new HashSet<>();
    private final MockMessagingSpy spy = new MockMessagingSpy();
    private final AtomicInteger limitCounter = new AtomicInteger(Integer.MAX_VALUE);
    private IMessageSink sink;

    MatcherResponse(Matcher<?> matcher)
    {
        this.matcher = matcher;
    }

    /**
     * Do not create any responses for intercepted outbound messages.
     */
    public MockMessagingSpy dontReply()
    {
        return respond((MessageIn<?>)null);
    }

    /**
     * Respond with provided message in reply to each intercepted outbound message.
     * @param message   the message to use as mock reply from the cluster
     */
    public MockMessagingSpy respond(MessageIn<?> message)
    {
        return respondN(message, Integer.MAX_VALUE);
    }

    /**
     * Respond a limited number of times with the provided message in reply to each intercepted outbound message.
     * @param response  the message to use as mock reply from the cluster
     * @param limit     number of times to respond with message
     */
    public MockMessagingSpy respondN(final MessageIn<?> response, int limit)
    {
        return respondN((in, to) -> response, limit);
    }

    /**
     * Respond with the message created by the provided function that will be called with each intercepted outbound message.
     * @param fnResponse    function to call for creating reply based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respond(BiFunction<MessageOut<T>, InetAddress, MessageIn<S>> fnResponse)
    {
        return respondN(fnResponse, Integer.MAX_VALUE);
    }

    /**
     * Respond with message wrapping the payload object created by provided function called for each intercepted outbound message.
     * The target address from the intercepted message will automatically be used as the created message's sender address.
     * @param fnResponse    function to call for creating payload object based on intercepted message and target address
     * @param verb          verb to use for reply message
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(Function<MessageOut<T>, S> fnResponse, MessagingService.Verb verb)
    {
        return respondNWithPayloadForEachReceiver(fnResponse, verb, Integer.MAX_VALUE);
    }

    /**
     * Respond a limited number of times with message wrapping the payload object created by provided function called for
     * each intercepted outbound message. The target address from the intercepted message will automatically be used as the
     * created message's sender address.
     * @param fnResponse    function to call for creating payload object based on intercepted message and target address
     * @param verb          verb to use for reply message
     */
    public <T, S> MockMessagingSpy respondNWithPayloadForEachReceiver(Function<MessageOut<T>, S> fnResponse, MessagingService.Verb verb, int limit)
    {
        return respondN((MessageOut<T> msg, InetAddress to) -> {
                    S payload = fnResponse.apply(msg);
                    if (payload == null)
                        return null;
                    else
                        return MessageIn.create(to, payload, Collections.emptyMap(), verb, MessagingService.current_version);
                },
                limit);
    }

    /**
     * Responds to each intercepted outbound message by creating a response message wrapping the next element consumed
     * from the provided queue. No reply will be send when the queue has been exhausted.
     * @param cannedResponses   prepared payload messages to use for responses
     * @param verb              verb to use for reply message
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(Queue<S> cannedResponses, MessagingService.Verb verb)
    {
        return respondWithPayloadForEachReceiver((MessageOut<T> msg) -> cannedResponses.poll(), verb);
    }

    /**
     * Responds to each intercepted outbound message by creating a response message wrapping the next element consumed
     * from the provided queue. This method will block until queue elements are available.
     * @param cannedResponses   prepared payload messages to use for responses
     * @param verb              verb to use for reply message
     */
    public <T, S> MockMessagingSpy respondWithPayloadForEachReceiver(BlockingQueue<S> cannedResponses, MessagingService.Verb verb)
    {
        return respondWithPayloadForEachReceiver((MessageOut<T> msg) -> {
            try
            {
                return cannedResponses.take();
            }
            catch (InterruptedException e)
            {
                return null;
            }
        }, verb);
    }

    /**
     * Respond a limited number of times with the message created by the provided function that will be called with
     * each intercepted outbound message.
     * @param fnResponse    function to call for creating reply based on intercepted message and target address
     */
    public <T, S> MockMessagingSpy respondN(BiFunction<MessageOut<T>, InetAddress, MessageIn<S>> fnResponse, int limit)
    {
        limitCounter.set(limit);

        assert sink == null: "destroy() must be called first to register new response";

        sink = new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                // prevent outgoing message from being send in case matcher indicates a match
                // and instead send the mocked response
                if (matcher.matches(message, to))
                {
                    spy.matchingMessage(message);

                    if (limitCounter.decrementAndGet() < 0)
                        return false;

                    synchronized (sendResponses)
                    {
                        // I'm not sure about retry semantics regarding message/ID relationships, but I assume
                        // sending a message multiple times using the same ID shouldn't happen..
                        assert !sendResponses.contains(id) : "ID re-use for outgoing message";
                        sendResponses.add(id);
                    }

                    // create response asynchronously to match request/response communication execution behavior
                    new Thread(() ->
                    {
                        MessageIn<?> response = fnResponse.apply(message, to);
                        if (response != null)
                        {
                            CallbackInfo cb = MessagingService.instance().getRegisteredCallback(id);
                            if (cb != null)
                                cb.callback.response(response);
                            else
                                MessagingService.instance().receive(response, id);
                            spy.matchingResponse(response);
                        }
                    }).start();

                    return false;
                }
                return true;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return true;
            }
        };
        MessagingService.instance().addMessageSink(sink);

        return spy;
    }

    /**
     * Stops currently registered response from being send.
     */
    public void destroy()
    {
        MessagingService.instance().removeMessageSink(sink);
    }
}
