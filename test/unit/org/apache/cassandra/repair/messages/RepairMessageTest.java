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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.apache.cassandra.repair.messages.RepairMessage.always;
import static org.apache.cassandra.repair.messages.RepairMessage.sendMessageWithRetries;
import static org.apache.cassandra.test.asserts.ExtendedAssertions.assertThat;

// Tests may use verb / message pairs that do not make sense... that is due to the fact that the message sending logic does not validate this and delegates such validation to messaging, which is mocked within the class...
// By using messages with simpler state it makes the test easier to read, even though the verb -> message mapping is incorrect.
public class RepairMessageTest
{
    private static final TimeUUID SESSION = new TimeUUID(0, 0);
    private static final InetAddressAndPort ADDRESS = FBUtilities.getBroadcastAddressAndPort();

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
        SharedContext ctx = ctx();
        MessageDelivery messaging = ctx.messaging();

        sendMessageWithRetries(ctx, backoff(1), always(), new CleanupMessage(SESSION), Verb.PREPARE_MSG, ADDRESS, RepairMessage.NO_OP_CALLBACK, 0);
        callback(messaging).onResponse(Message.out(Verb.PREPARE_MSG, new CleanupMessage(SESSION)));
        Mockito.verifyNoInteractions(messaging);

        assertThat(RepairMetrics.retries).isEmpty();
        assertThat(RepairMetrics.retriesByVerb.get(Verb.PREPARE_MSG)).isEmpty();
        assertThat(RepairMetrics.retryTimeout).isEmpty();
        assertThat(RepairMetrics.retryTimeoutByVerb.get(Verb.PREPARE_MSG)).isEmpty();
    }

    @Test
    public void noRetriesRequestFailed()
    {
        SharedContext ctx = ctx();
        MessageDelivery messaging = ctx.messaging();

        sendMessageWithRetries(ctx, backoff(1), always(), new CleanupMessage(SESSION), Verb.PREPARE_MSG, ADDRESS, RepairMessage.NO_OP_CALLBACK, 0);
        callback(messaging).onFailure(ADDRESS, RequestFailureReason.UNKNOWN);
        Mockito.verifyNoInteractions(messaging);

        assertThat(RepairMetrics.retries).isEmpty();
        assertThat(RepairMetrics.retriesByVerb.get(Verb.PREPARE_MSG)).isEmpty();
        assertThat(RepairMetrics.retryTimeout).isEmpty();
        assertThat(RepairMetrics.retryTimeoutByVerb.get(Verb.PREPARE_MSG)).isEmpty();
    }

    @Test
    public void retryWithSuccess()
    {
        SharedContext ctx = ctx();
        MessageDelivery messaging = ctx.messaging();

        sendMessageWithRetries(ctx, backoff(1), always(), new CleanupMessage(SESSION), Verb.PREPARE_MSG, ADDRESS, RepairMessage.NO_OP_CALLBACK, 0);
        callback(messaging).onFailure(ADDRESS, RequestFailureReason.TIMEOUT);
        callback(messaging).onResponse(Message.out(Verb.PREPARE_MSG, new CleanupMessage(SESSION)));
        Mockito.verifyNoInteractions(messaging);

        assertThat(RepairMetrics.retries).hasCount(1);
        assertThat(RepairMetrics.retriesByVerb.get(Verb.PREPARE_MSG)).hasCount(1);
        assertThat(RepairMetrics.retryTimeout).isEmpty();
        assertThat(RepairMetrics.retryTimeoutByVerb.get(Verb.PREPARE_MSG)).isEmpty();
    }

    @Test
    public void retryWithTimeout()
    {
        SharedContext ctx = ctx();
        MessageDelivery messaging = ctx.messaging();

        sendMessageWithRetries(ctx, backoff(1), always(), new CleanupMessage(SESSION), Verb.PREPARE_MSG, ADDRESS, RepairMessage.NO_OP_CALLBACK, 0);
        callback(messaging).onFailure(ADDRESS, RequestFailureReason.TIMEOUT);
        callback(messaging).onFailure(ADDRESS, RequestFailureReason.TIMEOUT);
        Mockito.verifyNoInteractions(messaging);

        assertThat(RepairMetrics.retries).hasCount(1);
        assertThat(RepairMetrics.retriesByVerb.get(Verb.PREPARE_MSG)).hasCount(1);
        assertThat(RepairMetrics.retryTimeout).hasCount(1);
        assertThat(RepairMetrics.retryTimeoutByVerb.get(Verb.PREPARE_MSG)).hasCount(1);
    }

    @Test
    public void retryWithFailure()
    {
        SharedContext ctx = ctx();
        MessageDelivery messaging = ctx.messaging();

        sendMessageWithRetries(ctx, backoff(1), always(), new CleanupMessage(SESSION), Verb.PREPARE_MSG, ADDRESS, RepairMessage.NO_OP_CALLBACK, 0);
        callback(messaging).onFailure(ADDRESS, RequestFailureReason.TIMEOUT);
        callback(messaging).onFailure(ADDRESS, RequestFailureReason.UNKNOWN);
        Mockito.verifyNoInteractions(messaging);

        assertThat(RepairMetrics.retries).hasCount(1);
        assertThat(RepairMetrics.retriesByVerb.get(Verb.PREPARE_MSG)).hasCount(1);
        assertThat(RepairMetrics.retryTimeout).isEmpty();
        assertThat(RepairMetrics.retryTimeoutByVerb.get(Verb.PREPARE_MSG)).isEmpty();
    }

    private static Backoff backoff(int maxAttempts)
    {
        return new Backoff.ExponentialBackoff(maxAttempts, 100, 1000, () -> .5);
    }

    private static SharedContext ctx()
    {
        SharedContext ctx = Mockito.mock(SharedContext.class);
        MessageDelivery messaging = Mockito.mock(MessageDelivery.class);
        IGossiper gossiper = Mockito.mock(IGossiper.class);
        Mockito.when(gossiper.getReleaseVersion(Mockito.any())).thenReturn(RepairMessage.SUPPORTS_RETRY);
        ScheduledExecutorPlus executor = Mockito.mock(ScheduledExecutorPlus.class);
        Mockito.when(executor.schedule(Mockito.<Runnable>any(), Mockito.anyLong(), Mockito.any())).thenAnswer(invocationOnMock -> {
            Runnable fn = invocationOnMock.getArgument(0);
            fn.run();
            return null;
        });

        Mockito.when(ctx.messaging()).thenReturn(messaging);
        Mockito.when(ctx.gossiper()).thenReturn(gossiper);
        Mockito.when(ctx.optionalTasks()).thenReturn(executor);
        return ctx;
    }

    private static <T extends RepairMessage> RequestCallback<T> callback(MessageDelivery messaging)
    {
        ArgumentCaptor<Message<?>> messageCapture = ArgumentCaptor.forClass(Message.class);
        ArgumentCaptor<InetAddressAndPort> endpointCapture = ArgumentCaptor.forClass(InetAddressAndPort.class);
        ArgumentCaptor<RequestCallback<T>> callbackCapture = ArgumentCaptor.forClass(RequestCallback.class);

        Mockito.verify(messaging).sendWithCallback(messageCapture.capture(), endpointCapture.capture(), callbackCapture.capture());
        Mockito.clearInvocations(messaging);

        return callbackCapture.getValue();
    }
}