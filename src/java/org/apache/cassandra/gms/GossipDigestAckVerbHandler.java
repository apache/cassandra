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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class GossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAckVerbHandler.class);

    public void doVerb(MessageIn<GossipDigestAck> message, int id)
    {
        InetAddress from = message.from;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestAckMessage from {}", from);
        if (!Gossiper.instance.isEnabled() && !Gossiper.instance.isInShadowRound())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
            return;
        }

        GossipDigestAck gDigestAckMessage = message.payload;
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
        logger.trace("Received ack with {} digests and {} states", gDigestList.size(), epStateMap.size());

        if (Gossiper.instance.isInShadowRound())
        {
            if (logger.isDebugEnabled())
                logger.debug("Received an ack from {}, which may trigger exit from shadow round", from);

            // if the ack is completely empty, then we can infer that the respondent is also in a shadow round
            Gossiper.instance.maybeFinishShadowRound(from, gDigestList.isEmpty() && epStateMap.isEmpty(), epStateMap);
            return; // don't bother doing anything else, we have what we came for
        }

        if (epStateMap.size() > 0)
        {
            // Ignore any GossipDigestAck messages that we handle before a regular GossipDigestSyn has been send.
            // This will prevent Acks from leaking over from the shadow round that are not actual part of
            // the regular gossip conversation.
            if ((System.nanoTime() - Gossiper.instance.firstSynSendAt) < 0 || Gossiper.instance.firstSynSendAt == 0)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Ignoring unrequested GossipDigestAck from {}", from);
                return;
            }

            /* Notify the Failure Detector */
            Gossiper.instance.notifyFailureDetector(epStateMap);
            Gossiper.instance.applyStateLocally(epStateMap);
        }

        /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        for (GossipDigest gDigest : gDigestList)
        {
            InetAddress addr = gDigest.getEndpoint();
            EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
            if (localEpStatePtr != null)
                deltaEpStateMap.put(addr, localEpStatePtr);
        }

        MessageOut<GossipDigestAck2> gDigestAck2Message = new MessageOut<GossipDigestAck2>(MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                                                                                           new GossipDigestAck2(deltaEpStateMap),
                                                                                           GossipDigestAck2.serializer);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        MessagingService.instance().sendOneWay(gDigestAck2Message, from);
    }
}
