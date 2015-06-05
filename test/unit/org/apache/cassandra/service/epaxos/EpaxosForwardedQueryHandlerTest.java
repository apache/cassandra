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

package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

public class EpaxosForwardedQueryHandlerTest extends AbstractEpaxosTest
{
    @Test
    public void successCase() throws UnknownHostException
    {
        final AtomicReference<Pair<MessageOut, InetAddress>> response = new AtomicReference<>();
        final AtomicReference<SettableFuture> futureRef = new AtomicReference<>();
        MockCallbackService service = new MockCallbackService(3, 0) {
            @Override
            protected void sendOneWay(MessageOut message, InetAddress to)
            {
                throw new AssertionError();
            }

            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                response.set(Pair.create(message, to));
            }

            @Override
            protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
            {
                throw new AssertionError();
            }

            @Override
            SettableFuture setFuture(Instance instance)
            {
                SettableFuture future = super.setFuture(instance);
                futureRef.set(future);
                return future;
            }
        };

        ForwardedQueryVerbHandler handler = new ForwardedQueryVerbHandler(service);
        Assert.assertEquals(0, service.preaccepts.size());
        Assert.assertNull(response.get());

        SerializedRequest request = getSerializedCQLRequest(0, 1);

        InetAddress from = InetAddress.getByName("127.0.0.1");

        MessageIn<SerializedRequest> msg = MessageIn.create(from,
                                                            request,
                                                            Collections.<String, byte[]>emptyMap(),
                                                            MessagingService.Verb.EPAXOS_FORWARD_QUERY,
                                                            0);

        handler.doVerb(msg, 5);

        Assert.assertEquals(1, service.preaccepts.size());
        Assert.assertNull(response.get());

        futureRef.get().set(6);

        Assert.assertEquals(1, service.preaccepts.size());
        Assert.assertNotNull(response.get());

        Pair<MessageOut, InetAddress> reply = response.get();
        Assert.assertEquals(from, reply.right);
        Assert.assertEquals(6, ((SerializedRequest.Result)reply.left.payload).getValue());
    }
}
