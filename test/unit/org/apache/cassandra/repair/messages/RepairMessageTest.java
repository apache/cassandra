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

package org.apache.cassandra.repair.messages;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.IGossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.utils.Backoff;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.apache.cassandra.repair.messages.RepairMessage.always;
import static org.apache.cassandra.repair.messages.RepairMessage.sendMessageWithRetries;
import static org.apache.cassandra.test.asserts.ExtendedAssertions.assertThat;

public class RepairMessageTest
{
    private static final TimeUUID SESSION = new TimeUUID(0, 0);
    private static final InetAddressAndPort ADDRESS = FBUtilities.getBroadcastAddressAndPort();
    private static final Answer REJECT_ALL = ignore -> {
        throw new UnsupportedOperationException();
    };
    private static final int[] attempts = {1, 2, 10};
    // Tests may use verb / message pairs that do not make sense... that is due to the fact that the message sending logic does not validate this and delegates such validation to messaging, which is mocked within the class...
    // By using messages with simpler state it makes the test easier to read, even though the verb -> message mapping is incorrect.
    private static final Verb VERB = Verb.PREPARE_MSG;
    private static final RepairMessage PAYLOAD = new CleanupMessage(SESSION);
    public static final int[] NO_RETRY_ATTEMPTS = { 0 };

    static
    {
        DatabaseDescriptor.clientInitialization();
        RepairMetrics.init();
    }

    @Before
    public void before()
    {
        RepairMetrics.unsafeReset();
    }

    @Test
    public void noRetries()
    {
        test(NO_RETRY_ATTEMPTS, (ignore, callback) -> {
            callback.onResponse(Message.out(VERB, PAYLOAD));
            assertNoRetries();
        });
    }

    @Test
    public void noRetriesRequestFailed()
    {
        test(NO_RETRY_ATTEMPTS, ((ignore, callback) -> {
            callback.onFailure(ADDRESS, RequestFailureReason.UNKNOWN);
            assertNoRetries();
        }));
    }

    @Test
    public void retryWithSuccess()
    {
        test((maxAttempts, callback) -> {
            callback.onResponse(Message.out(VERB, PAYLOAD));
            assertMetrics(maxAttempts, false, false);
        });
    }

    @Test
    public void retryWithTimeout()
    {
        test((maxAttempts, callback) -> {
            callback.onFailure(ADDRESS, RequestFailureReason.TIMEOUT);
            assertMetrics(maxAttempts, true, false);
        });
    }

    @Test
    public void retryWithFailure()
    {
        test((maxAttempts, callback) -> {
            callback.onFailure(ADDRESS, RequestFailureReason.UNKNOWN);
            assertMetrics(maxAttempts, false, true);
        });
    }

    private void assertNoRetries()
    {
        assertMetrics(0, false, false);
    }

    private void assertMetrics(long attempts, boolean timeout, boolean failure)
    {
        if (attempts == 0)
        {
            assertThat(RepairMetrics.retries).isEmpty();
            assertThat(RepairMetrics.retriesByVerb.get(VERB)).isEmpty();
            assertThat(RepairMetrics.retryTimeout).isEmpty();
            assertThat(RepairMetrics.retryTimeoutByVerb.get(VERB)).isEmpty();
            assertThat(RepairMetrics.retryFailure).isEmpty();
            assertThat(RepairMetrics.retryFailureByVerb.get(VERB)).isEmpty();
        }
        else
        {
            assertThat(RepairMetrics.retries).hasCount(1).hasMax(attempts);
            assertThat(RepairMetrics.retriesByVerb.get(VERB)).hasCount(1).hasMax(attempts);
            assertThat(RepairMetrics.retryTimeout).hasCount(timeout ? 1 : 0);
            assertThat(RepairMetrics.retryTimeoutByVerb.get(VERB)).hasCount(timeout ? 1 : 0);
            assertThat(RepairMetrics.retryFailure).hasCount(failure ? 1 : 0);
            assertThat(RepairMetrics.retryFailureByVerb.get(VERB)).hasCount(failure ? 1 : 0);
        }
    }

    private static Backoff backoff(int maxAttempts)
    {
        return new Backoff.ExponentialBackoff(maxAttempts, 100, 1000, () -> .5);
    }

    private static SharedContext ctx()
    {
        SharedContext ctx = Mockito.mock(SharedContext.class, REJECT_ALL);
        MessageDelivery messaging = Mockito.mock(MessageDelivery.class, REJECT_ALL);
        // allow the single method under test
        Mockito.doNothing().when(messaging).sendWithCallback(Mockito.any(), Mockito.any(), Mockito.any());
        IGossiper gossiper = Mockito.mock(IGossiper.class, REJECT_ALL);
        Mockito.doReturn(RepairMessage.SUPPORTS_RETRY).when(gossiper).getReleaseVersion(Mockito.any());
        ScheduledExecutorPlus executor = Mockito.mock(ScheduledExecutorPlus.class, REJECT_ALL);
        Mockito.doAnswer(invocationOnMock -> {
            Runnable fn = invocationOnMock.getArgument(0);
            fn.run();
            return null;
        }).when(executor).schedule(Mockito.<Runnable>any(), Mockito.anyLong(), Mockito.any());

        Mockito.doReturn(messaging).when(ctx).messaging();
        Mockito.doReturn(gossiper).when(ctx).gossiper();
        Mockito.doReturn(executor).when(ctx).optionalTasks();
        return ctx;
    }

    private static <T extends RepairMessage> RequestCallback<T> callback(MessageDelivery messaging)
    {
        ArgumentCaptor<Message<?>> messageCapture = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<InetAddressAndPort> endpointCapture = ArgumentCaptor.forClass(InetAddressAndPort.class);
        ArgumentCaptor<RequestCallback<T>> callbackCapture = ArgumentCaptor.forClass(RequestCallback.class);

        Mockito.verify(messaging).sendWithCallback(messageCapture.capture(), endpointCapture.capture(), callbackCapture.capture());
        Mockito.clearInvocations(messaging);

        Assertions.assertThat(endpointCapture.getValue()).isEqualTo(ADDRESS);

        return callbackCapture.getValue();
    }

    private interface TestCase
    {
        void test(int maxAttempts, RequestCallback<RepairMessage> callback);
    }

    private void test(TestCase fn)
    {
        test(attempts, fn);
    }

    private void test(int[] attempts, TestCase fn)
    {
        SharedContext ctx = ctx();
        MessageDelivery messaging = ctx.messaging();

        for (int maxAttempts : attempts)
        {
            before();

            sendMessageWithRetries(ctx, backoff(maxAttempts), always(), PAYLOAD, VERB, ADDRESS, RepairMessage.NOOP_CALLBACK, 0);
            for (int i = 0; i < maxAttempts; i++)
                callback(messaging).onFailure(ADDRESS, RequestFailureReason.TIMEOUT);
            fn.test(maxAttempts, callback(messaging));
            Mockito.verifyNoInteractions(messaging);
        }
    }
}