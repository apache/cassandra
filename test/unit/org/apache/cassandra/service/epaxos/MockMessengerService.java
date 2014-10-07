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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Predicate;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MockMessengerService extends MockMultiDcService
{
    public final InetAddress endpoint;
    public final List<InetAddress> localReplicas;
    public final List<InetAddress> localEndpoints;
    public final List<InetAddress> remoteEndpoints;

    public MockMessengerService(int numLocal, int numRemote)
    {
        numLocal = Math.max(1, numLocal);
        numRemote = Math.max(0, numRemote);

        try
        {
            endpoint = InetAddress.getByAddress(ByteBufferUtil.bytes(1).array());
            localReplicas = new ArrayList<>(numLocal - 1);
            localEndpoints = new ArrayList<>(numLocal);
            localEndpoints.add(endpoint);
            for (int i=1; i<numLocal; i++)
            {
                InetAddress replicaEndpoint = InetAddress.getByAddress(ByteBufferUtil.bytes(i + 1).array());
                localReplicas.add(replicaEndpoint);
                localEndpoints.add(replicaEndpoint);
            }

            remoteEndpoints = new ArrayList<>(numRemote);
            for (int i=0; i<numRemote; i++)
            {
                remoteEndpoints.add(InetAddress.getByAddress(ByteBufferUtil.bytes(i + 1 + numLocal).array()));
            }
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError();
        }
    }

    @Override
    protected TokenStateManager createTokenStateManager(Scope scope)
    {
        return new MockTokenStateManager(scope);
    }

    @Override
    protected InetAddress getEndpoint()
    {
        return endpoint;
    }

    @Override
    protected ParticipantInfo getParticipants(Instance instance)
    {
        return new ParticipantInfo(localEndpoints, remoteEndpoints, instance.getConsistencyLevel());
    }

    public Predicate<InetAddress> livenessPredicate = new Predicate<InetAddress>()
    {
        @Override
        public boolean apply(InetAddress inetAddress)
        {
            return true;
        }
    };

    @Override
    protected Predicate<InetAddress> livePredicate()
    {
        return livenessPredicate;
    }

    protected void sendReply(MessageOut message, int id, InetAddress to)
    {
        throw new UnsupportedOperationException();
    }

    public static class SentMessage
    {
        public final MessageOut message;
        public final InetAddress to;
        public final IAsyncCallback cb;  // will be null for one way messages

        public SentMessage(MessageOut message, InetAddress to, IAsyncCallback cb)
        {
            this.message = message;
            this.to = to;
            this.cb = cb;
        }
    }

    List<SentMessage> sentMessages = new LinkedList<>();

    @Override
    protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        sentMessages.add(new SentMessage(message, to, cb));
        return -1;
    }

    @Override
    protected void sendOneWay(MessageOut message, InetAddress to)
    {
        sentMessages.add(new SentMessage(message, to, null));
    }

    @Override
    protected void scheduleTokenStateMaintenanceTask()
    {
        // no-op
    }
}
