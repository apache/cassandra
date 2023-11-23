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

package org.apache.cassandra.gms;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.tcm.compatibility.GossipHelper;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.gms.ApplicationState.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NewGossiperTest
{
    static Set<InetAddressAndPort> fakePeers = new HashSet<>();
    static
    {
        for (int i = 1; i < 21; i++) // we require 10% of nodes to respond, so for two responses we need a cluster with 20 nodes
            fakePeers.add(InetAddressAndPort.getByNameUnchecked("127.0.0." + i));
    }

    @Test
    public void mergeTest() throws InterruptedException, ExecutionException
    {
        ServerTestUtils.prepareServerNoRegister();

        Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < 5000; i++)
        {
            NewGossiper.ShadowRoundHandler srh = new NewGossiper.ShadowRoundHandler(fakePeers, new NoOpMessageDelivery());
            Future<Map<InetAddressAndPort, EndpointState>> states = srh.doShadowRound();
            Map<InetAddressAndPort, EndpointState> firstResp = buildEpstates(fakePeers, r);
            srh.onAck(firstResp);
            Map<InetAddressAndPort, EndpointState> secondResp = buildEpstates(fakePeers, r);
            srh.onAck(secondResp);
            Map<InetAddressAndPort, EndpointState> result = states.get();
            verifyResult(result, firstResp, secondResp);
        }
    }

    private static void verifyResult(Map<InetAddressAndPort, EndpointState> result,
                                     Map<InetAddressAndPort, EndpointState> firstResp,
                                     Map<InetAddressAndPort, EndpointState> secondResp)
    {
        assertEquals(Sets.union(firstResp.keySet(), secondResp.keySet()), result.keySet());
        assertTrue(GossipHelper.isValidForClusterMetadata(result));

        for (InetAddressAndPort ep : Sets.union(firstResp.keySet(), secondResp.keySet()))
        {
            EndpointState first = firstResp.get(ep);
            EndpointState second = secondResp.get(ep);
            assertTrue(first != null || second != null);
            if (first == null)
                assertEquals(second, result.get(ep));
            else if (second == null)
                assertEquals(first, result.get(ep));
            else if (first.getHeartBeatState().getGeneration() > second.getHeartBeatState().getGeneration())
                assertEquals(first, result.get(ep));
            else if (first.getHeartBeatState().getGeneration() < second.getHeartBeatState().getGeneration())
                assertEquals(second, result.get(ep));
            else // equal generations
            {
                if (first.isSupersededBy(second))
                    assertEquals(second, result.get(ep));
                else if (second.isSupersededBy(first))
                    assertEquals(first, result.get(ep));
                else
                    assertEquals(Gossiper.getMaxEndpointStateVersion(first), Gossiper.getMaxEndpointStateVersion(second));
            }
        }
    }

    private static Map<InetAddressAndPort, EndpointState> buildEpstates(Set<InetAddressAndPort> fakePeers, Random r)
    {
        Map<InetAddressAndPort, EndpointState> epstates = new HashMap<>();

        VersionedValue.VersionedValueFactory vvf = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance, () -> r.nextInt(500));
        for (InetAddressAndPort endpoint : fakePeers)
        {
            if (r.nextDouble() < 0.001)
                continue;
            EndpointState epstate = new EndpointState(new HeartBeatState(r.nextInt(50), r.nextInt(100)));
            epstate.addApplicationState(TOKENS, vvf.tokens(Collections.singleton(new Murmur3Partitioner.LongToken(r.nextInt(5000)))));
            epstate.addApplicationState(HOST_ID, vvf.hostId(UUID.randomUUID()));
            epstate.addApplicationState(DC, vvf.datacenter("dc" + r.nextInt(5)));
            epstate.addApplicationState(RACK, vvf.rack("rack" + r.nextInt(5)));
            epstate.addApplicationState(RELEASE_VERSION, vvf.releaseVersion("5.0."+r.nextInt()));
            epstates.put(endpoint, epstate);
        }
        return epstates;
    }

    private static class NoOpMessageDelivery implements MessageDelivery
    {
        public <REQ> void send(Message<REQ> message, InetAddressAndPort to) {}
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb) {}
        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection) {}
        public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to) {return null;}
        public <V> void respond(V response, Message<?> message) {}
    }

    @Test
    public void testIncompleteState()
    {
        Map<InetAddressAndPort, EndpointState> epstates = buildEpstates(Collections.singleton(fakePeers.iterator().next()), new Random());
        assertTrue(GossipHelper.isValidForClusterMetadata(epstates));
        Map<InetAddressAndPort, EndpointState> brokenEpstates = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : epstates.entrySet())
        {
            EndpointState epstate = new EndpointState(entry.getValue().getHeartBeatState());
            for (Map.Entry<ApplicationState, VersionedValue> vals : entry.getValue().states())
            {
                if (vals.getKey() != TOKENS)
                    epstate.addApplicationState(vals.getKey(), vals.getValue());
            }
        }
        assertFalse(GossipHelper.isValidForClusterMetadata(brokenEpstates)); // does not contain TOKEN anymore
    }
}
