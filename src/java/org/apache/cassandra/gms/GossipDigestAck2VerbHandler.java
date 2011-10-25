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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class GossipDigestAck2VerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger(GossipDigestAck2VerbHandler.class);

    public void doVerb(Message message, String id)
    {
        if (logger_.isTraceEnabled())
        {
            InetAddress from = message.getFrom();
            logger_.trace("Received a GossipDigestAck2Message from {}", from);
        }

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new FastByteArrayInputStream(bytes) );
        GossipDigestAck2Message gDigestAck2Message;
        try
        {
            gDigestAck2Message = GossipDigestAck2Message.serializer().deserialize(dis, message.getVersion());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        Map<InetAddress, EndpointState> remoteEpStateMap = gDigestAck2Message.getEndpointStateMap();
        /* Notify the Failure Detector */
        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
        Gossiper.instance.applyStateLocally(remoteEpStateMap);
    }
}
