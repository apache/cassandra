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

package org.apache.cassandra.repair.consistent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.utils.concurrent.Future;

class MockMessaging implements MessageDelivery
{
    Map<InetAddressAndPort, List<RepairMessage>> sentMessages = new HashMap<>();
    Map<InetAddressAndPort, Integer> acks = new HashMap<>();
    Map<InetAddressAndPort, Integer> failures = new HashMap<>();

    @Override
    public <REQ> void send(Message<REQ> message, InetAddressAndPort destination)
    {
        if (message.verb() == Verb.REPAIR_RSP && message.payload instanceof NoPayload)
        {
            acks.compute(destination, (ignore, accum) -> accum == null ? 1 : accum + 1);
            return;
        }
        if (message.verb() == Verb.FAILURE_RSP)
        {
            failures.compute(destination, (ignore, accum) -> accum ==  null ? 1 : accum + 1);
            return;
        }
        if (!(message.payload instanceof RepairMessage))
            throw new AssertionError("Unexpected message: " + message);

        if (!sentMessages.containsKey(destination))
        {
            sentMessages.put(destination, new ArrayList<>());
        }
        sentMessages.get(destination).add((RepairMessage) message.payload);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
    {
        send(message, to);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
    {
        send(message, to);
    }

    @Override
    public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> void respond(V response, Message<?> message)
    {
        throw new UnsupportedOperationException();
    }
}
