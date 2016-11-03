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

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.EchoMessage;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MockMessagingService.all;
import static org.apache.cassandra.net.MockMessagingService.to;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MockMessagingServiceTest
{
    @BeforeClass
    public static void initCluster() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
    }

    @Before
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Test
    public void testRequestResponse() throws InterruptedException, ExecutionException
    {
        // echo message that we like to mock as incoming reply for outgoing echo message
        MessageIn<EchoMessage> echoMessageIn = MessageIn.create(FBUtilities.getBroadcastAddress(),
                EchoMessage.instance,
                Collections.emptyMap(),
                MessagingService.Verb.ECHO,
                MessagingService.current_version);
        MockMessagingSpy spy = MockMessagingService
                .when(
                        all(
                                to(FBUtilities.getBroadcastAddress()),
                                verb(MessagingService.Verb.ECHO)
                        )
                )
                .respond(echoMessageIn);

        MessageOut<EchoMessage> echoMessageOut = new MessageOut<>(MessagingService.Verb.ECHO, EchoMessage.instance, EchoMessage.serializer);
        MessagingService.instance().sendRR(echoMessageOut, FBUtilities.getBroadcastAddress(), new IAsyncCallback()
        {
            public void response(MessageIn msg)
            {
                assertEquals(MessagingService.Verb.ECHO, msg.verb);
                assertEquals(echoMessageIn.payload, msg.payload);
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        });

        // we must have intercepted the outgoing message at this point
        MessageOut<?> msg = spy.captureMessageOut().get();
        assertEquals(1, spy.messagesIntercepted);
        assertTrue(msg == echoMessageOut);

        // and return a mocked response
        assertEquals(1, spy.mockedMessageResponses);
    }
}
