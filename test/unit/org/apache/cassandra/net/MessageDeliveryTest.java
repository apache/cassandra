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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import accord.utils.RandomSource;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery.FailedResponseException;
import org.apache.cassandra.net.MessageDelivery.MaxRetriesException;
import org.apache.cassandra.net.SimulatedMessageDelivery.Action;
import org.apache.cassandra.net.SimulatedMessageDelivery.SimulatedMessageReceiver;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.utils.Backoff;
import org.mockito.Mockito;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

public class MessageDeliveryTest
{
    private static final InetAddressAndPort ID1 = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
    private static final MessageDelivery.RetryErrorMessage RETRY_ERROR_MESSAGE = (i1, i2, i3, i4) -> null;
    private static final MessageDelivery.RetryPredicate ALWAYS_RETRY = (i1, i2, i3) -> true;
    private static final MessageDelivery.RetryPredicate ALWAYS_REJECT = (i1, i2, i3) -> false;

    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
    }

    @Test
    public void sendWithRetryFailsAfterMaxAttempts()
    {
        qt().check(rs -> {
            List<Throwable> failures = new ArrayList<>();
            SimulatedExecutorFactory factory = new SimulatedExecutorFactory(rs.fork(), failures::add);
            ScheduledExecutorPlus scheduler = factory.scheduled("ignored");
            MessageDelivery messaging = simulatedMessages(rs, scheduler, failures, (i1, i2, i3) -> Action.DROP);

            int expectedRetries = 3;
            Backoff backoff = new Backoff.ExponentialBackoff(expectedRetries, 200, 1000, rs.fork()::nextDouble);

            Future<Message<Void>> result = messaging.sendWithRetries(backoff,
                                                                     scheduler::schedule,
                                                                     Verb.ECHO_REQ, NoPayload.noPayload,
                                                                     Iterators.cycle(ID1),
                                                                     ALWAYS_RETRY,
                                                                     RETRY_ERROR_MESSAGE);
            assertThat(result).isNotDone();
            factory.processAll();
            assertThat(result).isDone();

            assertThat(getMaxRetriesException(result).attempts).isEqualTo(expectedRetries);
        });
    }

    @Test
    public void sendWithRetryFirstAttempt()
    {
        qt().check(rs -> {
            List<Throwable> failures = new ArrayList<>();
            SimulatedExecutorFactory factory = new SimulatedExecutorFactory(rs.fork(), failures::add);
            ScheduledExecutorPlus scheduler = factory.scheduled("ignored");
            MessageDelivery messaging = simulatedMessages(rs, scheduler, failures, (i1, i2, i3) -> Action.DELIVER);

            Backoff backoff = Mockito.mock(Backoff.class);

            Future<Message<Void>> result = messaging.sendWithRetries(backoff,
                                                                     scheduler::schedule,
                                                                     Verb.ECHO_REQ, NoPayload.noPayload,
                                                                     Iterators.cycle(ID1),
                                                                     ALWAYS_RETRY,
                                                                     RETRY_ERROR_MESSAGE);
            assertThat(result).isNotDone();
            factory.processAll();
            assertThat(result).isDone();
            assertThat(result.get().header.verb).isEqualTo(Verb.ECHO_RSP);
            Mockito.verify(backoff, Mockito.never()).mayRetry(Mockito.anyInt());
            Mockito.verify(backoff, Mockito.never()).computeWaitTime(Mockito.anyInt());
            Mockito.verify(backoff, Mockito.never()).unit();
        });
    }

    @Test
    public void sendWithRetry()
    {
        qt().check(rs -> {
            List<Throwable> failures = new ArrayList<>();
            SimulatedExecutorFactory factory = new SimulatedExecutorFactory(rs.fork(), failures::add);
            ScheduledExecutorPlus scheduler = factory.scheduled("ignored");

            int maxAttempts = 3;
            int expectedAttempts = 1;
            AtomicInteger attempts = new AtomicInteger(0);
            MessageDelivery messaging = simulatedMessages(rs, scheduler, failures, (i1, i2, i3) -> attempts.incrementAndGet() >= (expectedAttempts + 1) ? Action.DELIVER : Action.DROP);

            Backoff backoff = Mockito.spy(new Backoff.ExponentialBackoff(maxAttempts, 200, 1000, rs.fork()::nextDouble));

            Future<Message<Void>> result = messaging.sendWithRetries(backoff,
                                                                     scheduler::schedule,
                                                                     Verb.ECHO_REQ, NoPayload.noPayload,
                                                                     Iterators.cycle(ID1),
                                                                     ALWAYS_RETRY,
                                                                     RETRY_ERROR_MESSAGE);
            assertThat(result).isNotDone();
            factory.processAll();
            assertThat(result).isDone();
            assertThat(result.get().header.verb).isEqualTo(Verb.ECHO_RSP);
            Mockito.verify(backoff, Mockito.times(expectedAttempts)).mayRetry(Mockito.anyInt());
            Mockito.verify(backoff, Mockito.times(expectedAttempts)).computeWaitTime(Mockito.anyInt());
            Mockito.verify(backoff, Mockito.times(expectedAttempts)).unit();
        });
    }

    @Test
    public void sendWithRetryDontAllowRetry()
    {
        qt().check(rs -> {
            List<Throwable> failures = new ArrayList<>();
            SimulatedExecutorFactory factory = new SimulatedExecutorFactory(rs.fork(), failures::add);
            ScheduledExecutorPlus scheduler = factory.scheduled("ignored");

            MessageDelivery messaging = simulatedMessages(rs, scheduler, failures, (i1, i2, i3) -> Action.DROP);

            Backoff backoff = Mockito.spy(new Backoff.ExponentialBackoff(3, 200, 1000, rs.fork()::nextDouble));

            Future<Message<Void>> result = messaging.sendWithRetries(backoff,
                                                                     scheduler::schedule,
                                                                     Verb.ECHO_REQ, NoPayload.noPayload,
                                                                     Iterators.cycle(ID1),
                                                                     ALWAYS_REJECT,
                                                                     RETRY_ERROR_MESSAGE);
            assertThat(result).isNotDone();
            factory.processAll();
            assertThat(result).isDone();
            FailedResponseException e = getFailedResponseException(result);
            assertThat(e.from).isEqualTo(ID1);
            assertThat(e.failure).isEqualTo(RequestFailureReason.TIMEOUT);
            Mockito.verify(backoff, Mockito.times(1)).mayRetry(Mockito.anyInt());
            Mockito.verify(backoff, Mockito.never()).computeWaitTime(Mockito.anyInt());
            Mockito.verify(backoff, Mockito.never()).unit();
        });
    }

    private static MessageDelivery simulatedMessages(RandomSource rs, ScheduledExecutorPlus scheduler, List<Throwable> failures, SimulatedMessageDelivery.ActionSupplier actionSupplier)
    {
        Map<InetAddressAndPort, SimulatedMessageReceiver> receivers = new HashMap<>();
        SimulatedMessageDelivery messaging = new SimulatedMessageDelivery(ID1,
                                                                          actionSupplier,
                                                                          SimulatedMessageDelivery.randomDelay(rs),
                                                                          (to, message) -> scheduler.execute(() -> receivers.get(to).recieve(message)),
                                                                          (i1, i2, i3) -> {},
                                                                          scheduler::schedule,
                                                                          failures::add);
        receivers.put(ID1, messaging.receiver(m -> messaging.respond(NoPayload.noPayload, m)));
        return messaging;
    }

    private static FailedResponseException getFailedResponseException(Future<Message<Void>> result) throws InterruptedException
    {
        FailedResponseException ex;
        try
        {
            result.get();
            Assert.fail("Should have failed");
            throw new AssertionError("Not Reachable");
        }
        catch (ExecutionException e)
        {
            ex = (FailedResponseException) e.getCause();
        }
        return ex;
    }

    private static MaxRetriesException getMaxRetriesException(Future<Message<Void>> result) throws InterruptedException
    {
        MaxRetriesException ex;
        try
        {
            result.get();
            Assert.fail("Should have failed");
            throw new AssertionError("Not Reachable");
        }
        catch (ExecutionException e)
        {
            ex = (MaxRetriesException) e.getCause();
        }
        return ex;
    }
}