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

package org.apache.cassandra.transport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.assertj.core.api.Assertions;

public class MessageSizeLimitTest extends NativeProtocolLimitsTestBase
{
    private static final int MAX_CQL_MESSAGE_SIZE = FrameEncoder.Payload.MAX_SIZE * 3;
    private static final int TOO_BIG_MESSAGE_SIZE = MAX_CQL_MESSAGE_SIZE * 2;
    private static final int NORMAL_MESSAGE_SIZE = MAX_CQL_MESSAGE_SIZE - 500;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setNativeTransportReceiveQueueCapacityInBytes(1);
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(MAX_CQL_MESSAGE_SIZE);
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(MAX_CQL_MESSAGE_SIZE);
        DatabaseDescriptor.setNativeTransportMaxMessageSizeInBytes(MAX_CQL_MESSAGE_SIZE);
        requireNetwork();
    }

    @Before
    public void setLimits()
    {
        ClientResourceLimits.setGlobalLimit(MAX_CQL_MESSAGE_SIZE);
        ClientResourceLimits.setEndpointLimit(MAX_CQL_MESSAGE_SIZE);
    }

    @Test
    public void sendMessageWithSizeMoreThanMaxMessageSize()
    {
        runClientLogic((client) ->
               {
                   QueryMessage tooBigQueryMessage = queryMessage(TOO_BIG_MESSAGE_SIZE);
                   Assertions.assertThatThrownBy(() -> client.execute(tooBigQueryMessage))
                             .hasCauseInstanceOf(InvalidRequestException.class);
                   // InvalidRequestException: CQL Message of size 524362 bytes exceeds allowed maximum of 262144 bytes

                   // we send one more message to check that the server continues to process new messages in the opened connection
                   QueryMessage queryMessage = queryMessage(NORMAL_MESSAGE_SIZE);
                   client.execute(queryMessage);
               }
        );
    }

    @Test(timeout = 30_000)
    public void checkThatThereIsNoStarvationForMultiFrameMessages() throws InterruptedException
    {
        runClientLogic((client) -> {}, true); // to create table
        AtomicInteger completedSuccessfully = new AtomicInteger(0);
        int threadsCount = 2;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadsCount; i++)
        {
            threads.add(new Thread(() -> runClientLogic((client) -> {
                    sendMessages(client, 100, NORMAL_MESSAGE_SIZE);
                    completedSuccessfully.incrementAndGet();
                }, false))
            );
        }
        for (Thread thread : threads)
            thread.start();

        for (Thread thread : threads)
            thread.join();

        Assert.assertEquals("not all messages were sent successfully by all threads",
                            threadsCount, completedSuccessfully.get());
    }

    private void sendMessages(SimpleClient client, int messagesCount, int messageSize)
    {
        for (int i = 0; i < messagesCount; i++)
        {
            QueryMessage queryMessage1 = queryMessage(messageSize);
            client.execute(queryMessage1);
        }
    }

    @Test
    public void sendMessageWithSizeBelowLimit()
    {
        runClientLogic((client) ->
               {
                   QueryMessage queryMessage = queryMessage(NORMAL_MESSAGE_SIZE);
                   client.execute(queryMessage);

                   // run one more time, to validate that the connection is still alive
                   queryMessage = queryMessage(NORMAL_MESSAGE_SIZE);
                   client.execute(queryMessage);
               }
        );
    }
}
