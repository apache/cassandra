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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;

public class GossipDigestAck2VerbHandler extends GossipVerbHandler<GossipDigestAck2>
{
    public static final GossipDigestAck2VerbHandler instance = new GossipDigestAck2VerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAck2VerbHandler.class);

    public void doVerb(Message<GossipDigestAck2> message)
    {
        if (logger.isTraceEnabled())
        {
            InetAddressAndPort from = message.from();
            logger.trace("Received a GossipDigestAck2Message from {}", from);
        }
        if (!Gossiper.instance.isEnabled())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestAck2Message because gossip is disabled");
            return;
        }
        Map<InetAddressAndPort, EndpointState> remoteEpStateMap = message.payload.epStateMap;
        /* Notify the Failure Detector */
        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
        Gossiper.instance.applyStateLocally(remoteEpStateMap);

        super.doVerb(message);
    }
}
