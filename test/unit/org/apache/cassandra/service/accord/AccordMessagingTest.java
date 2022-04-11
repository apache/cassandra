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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.messages.Callback;
import accord.messages.ReadData;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.net.Verb;

public class AccordMessagingTest
{
    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void nonFinalReplyTest() throws Exception
    {
        MessagingService messagingService = MessagingService.instance();
        AtomicLong msgId = new AtomicLong(-1);
        messagingService.outboundSink.add((msg, to) -> {
            if (!msgId.compareAndSet(-1, msg.id()))
                throw new AssertionError("Unexpected previous message id: " + msgId.get());
            return false;
        });

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.2");

        AtomicInteger responses = new AtomicInteger(0);
        Callback<ReadData.ReadReply> callback = new Callback<>()
        {
            @Override
            public void onSuccess(Node.Id from, ReadData.ReadReply response)
            {
                Assert.assertEquals(EndpointMapping.endpointToId(peer), from);
                responses.incrementAndGet();
            }

            @Override
            public void onFailure(Node.Id from, Throwable throwable)
            {
                throw new AssertionError("Unexpected failure");
            }
        };

        ReadData req = new ReadData(null, null, null, null, null);
        AccordService.instance.messageSink().send(EndpointMapping.getId(peer), req, callback);
        Assert.assertNotEquals(-1, msgId.get());
        Assert.assertTrue(messagingService.callbacks.contains(msgId.get(), peer));

        ReadData.ReadWaiting readWaiting = new ReadData.ReadWaiting(null, null, null, null, null);
        Message<?> waitingResponse = Message.builder(Verb.ACCORD_READ_RSP, readWaiting).from(peer).withId(msgId.get()).build();

        ResponseVerbHandler.instance.doVerb(waitingResponse);
        Assert.assertEquals(1, responses.get());
        Assert.assertTrue(messagingService.callbacks.contains(msgId.get(), peer));

        ReadData.ReadOk readOk = new ReadData.ReadOk(null);
        Message<?> okResponse = Message.builder(Verb.ACCORD_READ_RSP, readOk).from(peer).withId(msgId.get()).build();
        ResponseVerbHandler.instance.doVerb(okResponse);
        Assert.assertEquals(2, responses.get());
        Assert.assertFalse(messagingService.callbacks.contains(msgId.get(), peer));
    }
}
