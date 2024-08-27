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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Backoff;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.net.MessageFlag.CALL_BACK_ON_FAILURE;

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

    public default <REQ, RSP> Future<Message<RSP>> sendWithRetries(Backoff backoff, RetryScheduler retryThreads,
                                                                   Verb verb, REQ request,
                                                                   Iterator<InetAddressAndPort> candidates,
                                                                   RetryPredicate shouldRetry,
                                                                   RetryErrorMessage errorMessage)
    {
        Promise<Message<RSP>> promise = new AsyncPromise<>();
        this.<REQ, RSP>sendWithRetries(backoff, retryThreads, verb, request, candidates,
                                       (attempt, success, failure) -> {
                                           if (failure != null) promise.tryFailure(failure);
                                           else promise.trySuccess(success);
                                       },
                                       shouldRetry, errorMessage);
        return promise;
    }

    public default <REQ, RSP> void sendWithRetries(Backoff backoff, RetryScheduler retryThreads,
                                                   Verb verb, REQ request,
                                                   Iterator<InetAddressAndPort> candidates,
                                                   OnResult<RSP> onResult,
                                                   RetryPredicate shouldRetry,
                                                   RetryErrorMessage errorMessage)
    {
        sendWithRetries(this, backoff, retryThreads, verb, request, candidates, onResult, shouldRetry, errorMessage, 0);
    }
    public <V> void respond(V response, Message<?> message);
    public default void respondWithFailure(RequestFailureReason reason, Message<?> message)
    {
        send(Message.failureResponse(message.id(), message.expiresAtNanos(), reason), message.respondTo());
    }

    interface OnResult<T>
    {
        void result(int attempt, @Nullable Message<T> success, @Nullable Throwable failure);
    }

    interface RetryPredicate
    {
        boolean test(int attempt, InetAddressAndPort from, RequestFailureReason failure);
    }

    interface RetryErrorMessage
    {
        String apply(int attempt, ResponseFailureReason retryFailure, @Nullable InetAddressAndPort from, @Nullable RequestFailureReason reason);
    }

    private static <REQ, RSP> void sendWithRetries(MessageDelivery messaging,
                                                   Backoff backoff, RetryScheduler retryThreads,
                                                   Verb verb, REQ request,
                                                   Iterator<InetAddressAndPort> candidates,
                                                   OnResult<RSP> onResult,
                                                   RetryPredicate shouldRetry,
                                                   RetryErrorMessage errorMessage,
                                                   int attempt)
    {
        if (Thread.currentThread().isInterrupted())
        {
            onResult.result(attempt, null, new InterruptedException(errorMessage.apply(attempt, ResponseFailureReason.Interrupted, null, null)));
            return;
        }
        if (!candidates.hasNext())
        {
            onResult.result(attempt, null, new NoMoreCandidatesException(errorMessage.apply(attempt, ResponseFailureReason.NoMoreCandidates, null, null)));
            return;
        }
        class Request implements RequestCallbackWithFailure<RSP>
        {
            @Override
            public void onResponse(Message<RSP> msg)
            {
                onResult.result(attempt, msg, null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failure)
            {
                if (!backoff.mayRetry(attempt))
                {
                    onResult.result(attempt, null, new MaxRetriesException(attempt, errorMessage.apply(attempt, ResponseFailureReason.MaxRetries, from, failure)));
                    return;
                }
                if (!shouldRetry.test(attempt, from, failure))
                {
                    onResult.result(attempt, null, new FailedResponseException(from, failure, errorMessage.apply(attempt, ResponseFailureReason.Rejected, from, failure)));
                    return;
                }
                try
                {
                    retryThreads.schedule(() -> sendWithRetries(messaging, backoff, retryThreads, verb, request, candidates, onResult, shouldRetry, errorMessage, attempt + 1),
                                          backoff.computeWaitTime(attempt), backoff.unit());
                }
                catch (Throwable t)
                {
                    onResult.result(attempt, null, new FailedScheduleException(errorMessage.apply(attempt, ResponseFailureReason.FailedSchedule, from, failure), t));
                }
            }
        }
        messaging.sendWithCallback(Message.outWithFlag(verb, request, CALL_BACK_ON_FAILURE), candidates.next(), new Request());
    }

    enum ResponseFailureReason { MaxRetries, Rejected, NoMoreCandidates, Interrupted, FailedSchedule }

    interface RetryScheduler
    {
        void schedule(Runnable command, long delay, TimeUnit unit);
    }

    enum ImmediateRetryScheduler implements RetryScheduler
    {
        instance;

        @Override
        public void schedule(Runnable command, long delay, TimeUnit unit)
        {
            command.run();
        }
    }

    class NoMoreCandidatesException extends IllegalStateException
    {
        public NoMoreCandidatesException(String s)
        {
            super(s);
        }
    }

    class FailedResponseException extends IllegalStateException
    {
        public final InetAddressAndPort from;
        public final RequestFailureReason failure;

        public FailedResponseException(InetAddressAndPort from, RequestFailureReason failure, String message)
        {
            super(message);
            this.from = from;
            this.failure = failure;
        }
    }

    class MaxRetriesException extends IllegalStateException
    {
        public final int attempts;
        public MaxRetriesException(int attempts, String message)
        {
            super(message);
            this.attempts = attempts;
        }
    }

    class FailedScheduleException extends IllegalStateException
    {
        public FailedScheduleException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
