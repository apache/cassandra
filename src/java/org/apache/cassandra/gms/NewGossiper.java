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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.config.DatabaseDescriptor.getClusterName;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitionerName;
import static org.apache.cassandra.net.Verb.GOSSIP_DIGEST_SYN;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public class NewGossiper
{
    private static final Logger logger = LoggerFactory.getLogger(NewGossiper.class);
    public static final NewGossiper instance = new NewGossiper();

    private volatile ShadowRoundHandler handler;

    public Map<InetAddressAndPort, EndpointState> doShadowRound()
    {
        Set<InetAddressAndPort> peers = new HashSet<>(SystemKeyspace.loadHostIds().keySet());
        if (peers.isEmpty())
            peers.addAll(DatabaseDescriptor.getSeeds());

        ShadowRoundHandler shadowRoundHandler = new ShadowRoundHandler(peers);
        handler = shadowRoundHandler;

        try
        {
            return shadowRoundHandler.doShadowRound().get(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }


    public boolean isInShadowRound()
    {
        ShadowRoundHandler srh = handler;
        return srh != null && !srh.isDone();
    }

    void onAck( Map<InetAddressAndPort, EndpointState> epStateMap)
    {
        ShadowRoundHandler srh = handler;
        if (srh != null && !srh.isDone())
            srh.onAck(epStateMap);
    }

    public static class ShadowRoundHandler
    {
        private volatile boolean isDone = false;
        private final Set<InetAddressAndPort> peers;
        private final Accumulator<Map<InetAddressAndPort, EndpointState>> responses;
        private final int requiredResponses;
        private final Promise<Map<InetAddressAndPort, EndpointState>> promise = new AsyncPromise<>();

        public ShadowRoundHandler(Set<InetAddressAndPort> peers)
        {
            this.peers = peers;
            requiredResponses = Math.max(peers.size() / 10, 1);
            responses = new Accumulator<>(requiredResponses);
        }

        public boolean isDone()
        {
            return isDone;
        }

        public Promise<Map<InetAddressAndPort, EndpointState>> doShadowRound()
        {
            // send a completely empty syn
            GossipDigestSyn digestSynMessage = new GossipDigestSyn(getClusterName(), getPartitionerName(), new ArrayList<>());
            Message<GossipDigestSyn> message = Message.out(GOSSIP_DIGEST_SYN, digestSynMessage);

            logger.info("Sending shadow round GOSSIP DIGEST SYN to known peers {}", peers);
            for (InetAddressAndPort peer : peers)
            {
                if (!peer.equals(getBroadcastAddressAndPort()))
                    MessagingService.instance().send(message, peer);
            }
            return promise;
        }

        public void onAck(Map<InetAddressAndPort, EndpointState> epStateMap)
        {
            if (!isDone)
            {
                if (!epStateMap.isEmpty())
                    responses.add(epStateMap);

                logger.debug("Received {} responses. {} required.", responses.size(), requiredResponses);
                if (responses.size() >= requiredResponses)
                {
                    isDone = true;
                    promise.setSuccess(merge(responses.snapshot()));
                }
            }
        }

        private Map<InetAddressAndPort, EndpointState> merge(Collection<Map<InetAddressAndPort, EndpointState>> snapshot)
        {
            return snapshot.iterator().next(); // todo; actually merge/verify the responses
        }
    }
}
