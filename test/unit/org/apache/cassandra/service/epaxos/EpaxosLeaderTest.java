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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Tests the commit/accept methods on EpaxosState called by leaders
 */
public class EpaxosLeaderTest extends AbstractEpaxosTest
{
    private static final List<InetAddress> LOCAL;
    private static final List<InetAddress> REMOTE;

    static
    {
        try
        {
            LOCAL = Lists.newArrayList(InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.3"));
            REMOTE = Lists.newArrayList(InetAddress.getByName("126.0.0.2"), InetAddress.getByName("126.0.0.3"));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static class Service extends MockMultiDcService
    {
        List<Pair<InetAddress, MessageOut>> replies = new LinkedList<>();
        @Override
        protected void sendReply(MessageOut message, int id, InetAddress to)
        {
            replies.add(Pair.create(to, message));
        }

        List<Pair<InetAddress, MessageOut>> oneWay = new LinkedList<>();
        @Override
        protected void sendOneWay(MessageOut message, InetAddress to)
        {
            oneWay.add(Pair.create(to, message));
        }

        List<Pair<InetAddress, MessageOut>> rrs = new LinkedList<>();
        @Override
        protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
        {
            rrs.add(Pair.create(to, message));
            return 0;
        }

        @Override
        protected ParticipantInfo getParticipants(Token token, UUID cfId, Scope scope)
        {
            return new ParticipantInfo(LOCAL, REMOTE, scope.cl);
        }

        @Override
        protected boolean isAlive(InetAddress endpoint)
        {
            return true;
        }

        @Override
        protected Predicate<InetAddress> livePredicate()
        {
            return Predicates.alwaysTrue();
        }
    }

    @Test
    public void localSerialAccept() throws Exception
    {
        int ballot = 10;
        Service service = new Service();

        QueryInstance missing = service.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.LOCAL_SERIAL));
        missing.preaccept(Collections.<UUID>emptySet());
        service.saveInstance(missing);

        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.LOCAL_SERIAL));
        instance.preaccept(Sets.newHashSet(missing.getId()));
        instance.updateBallot(ballot);
        service.saveInstance(instance);

        Assert.assertTrue(service.replies.isEmpty());
        Assert.assertTrue(service.oneWay.isEmpty());
        Assert.assertTrue(service.rrs.isEmpty());

        Set<InetAddress> expectedEndpoints = Sets.newHashSet(LOCAL);
        Set<UUID> newDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), missing.getId());
        Map<InetAddress, Set<UUID>> missingIds = new HashMap<>();
        missingIds.put(LOCAL.get(0), Sets.newHashSet(missing.getId()));

        AcceptDecision decision = new AcceptDecision(true, newDeps, false, null, missingIds);
        service.accept(instance.getId(), decision, null);

        Assert.assertTrue(service.replies.isEmpty());
        Assert.assertTrue(service.oneWay.isEmpty());
        Assert.assertEquals(LOCAL.size(), service.rrs.size());

        for (Pair<InetAddress, MessageOut> message: service.rrs)
        {
            Assert.assertTrue(expectedEndpoints.remove(message.left));
            AcceptRequest request = (AcceptRequest) message.right.payload;
            Instance sent = request.instance;
            Assert.assertEquals(Instance.State.ACCEPTED, sent.getState());
            Assert.assertEquals(newDeps, sent.getDependencies());
            Assert.assertEquals(ballot + 1, sent.getBallot());

            if (message.left.equals(LOCAL.get(0)))
            {
                List<Instance> missingList = request.missingInstances;
                Assert.assertEquals(1, missingList.size());
                Instance sentMissing = missingList.get(0);
                Assert.assertEquals(missing.getId(), sentMissing.getId());
            }
            else
            {
                Assert.assertTrue(request.missingInstances.isEmpty());
            }

            Assert.assertFalse(REMOTE.contains(message.left));
        }
    }

    @Test
    public void localSerialCommit() throws Exception
    {
        int ballot = 10;
        Service service = new Service();
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0, ConsistencyLevel.LOCAL_SERIAL));
        instance.preaccept(Collections.<UUID>emptySet());
        instance.updateBallot(ballot);
        service.saveInstance(instance);

        Assert.assertTrue(service.replies.isEmpty());
        Assert.assertTrue(service.oneWay.isEmpty());
        Assert.assertTrue(service.rrs.isEmpty());

        Set<InetAddress> expectedEndpoints = Sets.newHashSet(LOCAL);
        Set<UUID> newDeps = Sets.newHashSet(UUIDGen.getTimeUUID());

        service.commit(instance.getId(), newDeps);

        Assert.assertTrue(service.replies.isEmpty());
        Assert.assertEquals(LOCAL.size(), service.oneWay.size());
        Assert.assertTrue(service.rrs.isEmpty());

        for (Pair<InetAddress, MessageOut> message: service.oneWay)
        {
            Assert.assertTrue(expectedEndpoints.remove(message.left));
            MessageEnvelope<Instance> envelope = (MessageEnvelope<Instance>) message.right.payload;
            Instance sent = envelope.contents;
            Assert.assertEquals(Instance.State.COMMITTED, sent.getState());
            Assert.assertEquals(newDeps, sent.getDependencies());
            Assert.assertEquals(ballot + 1, sent.getBallot());
            Assert.assertFalse(REMOTE.contains(message.left));
        }
    }
}
