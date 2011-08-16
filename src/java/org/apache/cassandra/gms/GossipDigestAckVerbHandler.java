package org.apache.cassandra.gms;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class GossipDigestAckVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(GossipDigestAckVerbHandler.class);

    public void doVerb(Message message, String id)
    {
        InetAddress from = message.getFrom();
        if (logger_.isTraceEnabled())
            logger_.trace("Received a GossipDigestAckMessage from {}", from);
        if (!Gossiper.instance.isEnabled())
        {
            if (logger_.isTraceEnabled())
                logger_.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
            return;
        }

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new FastByteArrayInputStream(bytes) );

        try
        {
            GossipDigestAckMessage gDigestAckMessage = GossipDigestAckMessage.serializer().deserialize(dis, message.getVersion());
            List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
            Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();

            if ( epStateMap.size() > 0 )
            {
                /* Notify the Failure Detector */
                Gossiper.instance.notifyFailureDetector(epStateMap);
                Gossiper.instance.applyStateLocally(epStateMap);
            }

            /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
            Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
            for( GossipDigest gDigest : gDigestList )
            {
                InetAddress addr = gDigest.getEndpoint();
                EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
                if ( localEpStatePtr != null )
                    deltaEpStateMap.put(addr, localEpStatePtr);
            }

            GossipDigestAck2Message gDigestAck2 = new GossipDigestAck2Message(deltaEpStateMap);
            Message gDigestAck2Message = Gossiper.instance.makeGossipDigestAck2Message(gDigestAck2, message.getVersion());
            if (logger_.isTraceEnabled())
                logger_.trace("Sending a GossipDigestAck2Message to {}", from);
            MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        }
        catch ( IOException e )
        {
            throw new RuntimeException(e);
        }
    }
}
