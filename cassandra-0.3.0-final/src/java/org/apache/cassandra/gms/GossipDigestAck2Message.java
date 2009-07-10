/**
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.*;


/**
 * This message gets sent out as a result of the receipt of a GossipDigestAckMessage. This the 
 * last stage of the 3 way messaging of the Gossip protocol.
 *  
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class GossipDigestAck2Message
{
    private static  ICompactSerializer<GossipDigestAck2Message> serializer_;
    static
    {
        serializer_ = new GossipDigestAck2MessageSerializer();
    }
    
    Map<EndPoint, EndPointState> epStateMap_ = new HashMap<EndPoint, EndPointState>();

    public static ICompactSerializer<GossipDigestAck2Message> serializer()
    {
        return serializer_;
    }
    
    GossipDigestAck2Message(Map<EndPoint, EndPointState> epStateMap)
    {
        epStateMap_ = epStateMap;
    }
        
    Map<EndPoint, EndPointState> getEndPointStateMap()
    {
         return epStateMap_;
    }
}

class GossipDigestAck2MessageSerializer implements ICompactSerializer<GossipDigestAck2Message>
{
    public void serialize(GossipDigestAck2Message gDigestAck2Message, DataOutputStream dos) throws IOException
    {
        /* Use the EndPointState */
        EndPointStatesSerializationHelper.serialize(gDigestAck2Message.epStateMap_, dos);
    }

    public GossipDigestAck2Message deserialize(DataInputStream dis) throws IOException
    {
        Map<EndPoint, EndPointState> epStateMap = EndPointStatesSerializationHelper.deserialize(dis);
        return new GossipDigestAck2Message(epStateMap);        
    }
}

