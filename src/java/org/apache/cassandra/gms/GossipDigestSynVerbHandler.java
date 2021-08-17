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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.net.Verb.GOSSIP_DIGEST_ACK;

public class GossipDigestSynVerbHandler extends GossipVerbHandler<GossipDigestSyn>
{
    public static final GossipDigestSynVerbHandler instance = new GossipDigestSynVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(GossipDigestSynVerbHandler.class);

    public void doVerb(Message<GossipDigestSyn> message)
    {
        InetAddressAndPort from = message.from();
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestSynMessage from {}", from);
        if (!Gossiper.instance.isEnabled() && !Gossiper.instance.isInShadowRound())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
            return;
        }

        GossipDigestSyn gDigestMessage = message.payload;
        /* If the message is from a different cluster throw it away. */
        if (!gDigestMessage.clusterId.equals(DatabaseDescriptor.getClusterName()))
        {
            logger.warn("ClusterName mismatch from {} {}!={}", from, gDigestMessage.clusterId, DatabaseDescriptor.getClusterName());
            return;
        }

        if (gDigestMessage.partioner != null && !gDigestMessage.partioner.equals(DatabaseDescriptor.getPartitionerName()))
        {
            logger.warn("Partitioner mismatch from {} {}!={}", from, gDigestMessage.partioner, DatabaseDescriptor.getPartitionerName());
            return;
        }

        List<GossipDigest> gDigestList = gDigestMessage.getGossipDigests();

        // if the syn comes from a peer performing a shadow round and this node is
        // also currently in a shadow round, send back a minimal ack. This node must
        // be in the sender's seed list and doing this allows the sender to
        // differentiate between seeds from which it is partitioned and those which
        // are in their shadow round
        if (!Gossiper.instance.isEnabled() && Gossiper.instance.isInShadowRound())
        {
            // a genuine syn (as opposed to one from a node currently
            // doing a shadow round) will always contain > 0 digests
            if (gDigestList.size() > 0)
            {
                logger.debug("Ignoring non-empty GossipDigestSynMessage because currently in gossip shadow round");
                return;
            }

            logger.debug("Received a shadow round syn from {}. Gossip is disabled but " +
                         "currently also in shadow round, responding with a minimal ack", from);
            MessagingService.instance()
                            .send(Message.out(GOSSIP_DIGEST_ACK, new GossipDigestAck(Collections.emptyList(), Collections.emptyMap())),
                                  from);
            return;
        }

        if (logger.isTraceEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest gDigest : gDigestList)
            {
                sb.append(gDigest);
                sb.append(" ");
            }
            logger.trace("Gossip syn digests are : {}", sb);
        }

        Message<GossipDigestAck> gDigestAckMessage = gDigestList.isEmpty() ?
                                                     createShadowReply() :
                                                     createNormalReply(gDigestList);

        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAckMessage to {}", from);
        MessagingService.instance().send(gDigestAckMessage, from);

        super.doVerb(message);
    }

    private static Message<GossipDigestAck> createNormalReply(List<GossipDigest> gDigestList)
    {
        List<GossipDigest> deltaGossipDigestList = new ArrayList<>();
        Map<InetAddressAndPort, EndpointState> deltaEpStateMap = new HashMap<>();
        Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);
        logger.trace("sending {} digests and {} deltas", deltaGossipDigestList.size(), deltaEpStateMap.size());

        return Message.out(GOSSIP_DIGEST_ACK, new GossipDigestAck(deltaGossipDigestList, deltaEpStateMap));
    }

    private static Message<GossipDigestAck> createShadowReply()
    {
        Map<InetAddressAndPort, EndpointState> stateMap = Gossiper.instance.examineShadowState();
        logger.trace("sending 0 digests and {} deltas", stateMap.size());
        return Message.out(GOSSIP_DIGEST_ACK, new GossipDigestAck(Collections.emptyList(), stateMap));
    }
}
