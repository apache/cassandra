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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicate;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageOut;

public class EpaxosForwardedQueryTest extends AbstractEpaxosTest
{
    @Test
    public void forwardQueryIsCalledForNonLocalQueries() throws Exception
    {
        final AtomicInteger forwardCalls = new AtomicInteger();

        MockCallbackService service = new MockCallbackService(4, 0) {
            @Override
            protected ParticipantInfo getParticipants(Token token, UUID cfId, Scope scope)
            {
                return new ParticipantInfo(localReplicas, remoteEndpoints, scope.cl);
            }

            @Override
            protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
            {
                ((ForwardedQueryCallback) cb).getFuture().set(20);
                return super.sendRR(message, to, cb);
            }

            @Override
            protected void sendReply(MessageOut message, int id, InetAddress to)
            {
                throw new AssertionError();
            }

            @Override
            protected void sendOneWay(MessageOut message, InetAddress to)
            {
                throw new AssertionError();
            }

            @Override
            protected Predicate<InetAddress> livePredicate()
            {
                return new Predicate<InetAddress>()
                {
                    @Override
                    public boolean apply(InetAddress address)
                    {
                        return !address.equals(localReplicas.get(0));
                    }
                };
            }

            @Override
            public Object forwardQuery(SerializedRequest query, ParticipantInfo pi) throws UnavailableException, WriteTimeoutException
            {
                forwardCalls.incrementAndGet();
                return super.forwardQuery(query, pi);
            }
        };

        Assert.assertEquals(0, forwardCalls.get());
        Assert.assertEquals(0, service.sentMessages.size());

        SerializedRequest query = getSerializedCQLRequest(0, 1);
        int result = service.query(query);

        Assert.assertEquals(20, result);
        Assert.assertEquals(1, forwardCalls.get());
        Assert.assertEquals(1, service.sentMessages.size());

        MockMessengerService.SentMessage sent = service.sentMessages.get(0);
        Assert.assertTrue(sent.cb instanceof ForwardedQueryCallback);
        Assert.assertEquals(service.localReplicas.get(1), sent.to);  // localReplicas[0] was reported dead
        Assert.assertEquals(query, sent.message.payload);
    }
}
