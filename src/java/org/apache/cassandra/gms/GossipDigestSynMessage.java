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
import java.util.Map.Entry;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.CompactEndPointSerializationHelper;
import java.net.InetAddress;
import org.apache.log4j.Logger;


/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a 
 * round. 
 */

class GossipDigestSynMessage
{
    private static ICompactSerializer<GossipDigestSynMessage> serializer_;
    static
    {
        serializer_ = new GossipDigestSynMessageSerializer();
    }
    
    String clusterId_;
    List<GossipDigest> gDigests_ = new ArrayList<GossipDigest>();

    public static ICompactSerializer<GossipDigestSynMessage> serializer()
    {
        return serializer_;
    }
 
    public GossipDigestSynMessage(String clusterId, List<GossipDigest> gDigests)
    {      
        clusterId_ = clusterId;
        gDigests_ = gDigests;
    }
    
    List<GossipDigest> getGossipDigests()
    {
        return gDigests_;
    }
}

class GossipDigestSerializationHelper
{
    private static Logger logger_ = Logger.getLogger(GossipDigestSerializationHelper.class);
    
    static boolean serialize(List<GossipDigest> gDigestList, DataOutputStream dos) throws IOException
    {
        boolean bVal = true;
        int size = gDigestList.size();                        
        dos.writeInt(size);
        
        int estimate = 0;            
        for ( GossipDigest gDigest : gDigestList )
        {
            if ( Gossiper.MAX_GOSSIP_PACKET_SIZE - dos.size() < estimate )
            {
                logger_.info("@@@@ Breaking out to respect the MTU size in GD @@@@");
                bVal = false;
                break;
            }
            int pre = dos.size();               
            GossipDigest.serializer().serialize( gDigest, dos );
            int post = dos.size();
            estimate = post - pre;
        }
        return bVal;
    }

    static List<GossipDigest> deserialize(DataInputStream dis) throws IOException
    {
        int size = dis.readInt();            
        List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
        
        for ( int i = 0; i < size; ++i )
        {
            if ( dis.available() == 0 )
            {
                logger_.info("Remaining bytes zero. Stopping deserialization of GossipDigests.");
                break;
            }
                            
            GossipDigest gDigest = GossipDigest.serializer().deserialize(dis);                
            gDigests.add( gDigest );                
        }        
        return gDigests;
    }
}

class EndPointStatesSerializationHelper
{
    private static final Logger logger_ = Logger.getLogger(EndPointStatesSerializationHelper.class);

    static boolean serialize(Map<InetAddress, EndPointState> epStateMap, DataOutputStream dos) throws IOException
    {
        boolean bVal = true;
        int estimate = 0;                
        int size = epStateMap.size();
        dos.writeInt(size);

        for (Entry<InetAddress, EndPointState> entry : epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            if ( Gossiper.MAX_GOSSIP_PACKET_SIZE - dos.size() < estimate )
            {
                logger_.info("@@@@ Breaking out to respect the MTU size in EPS. Estimate is " + estimate + " @@@@");
                bVal = false;
                break;
            }
    
            int pre = dos.size();
            CompactEndPointSerializationHelper.serialize(ep, dos);
            EndPointState.serializer().serialize(entry.getValue(), dos);
            int post = dos.size();
            estimate = post - pre;
        }
        return bVal;
    }

    static Map<InetAddress, EndPointState> deserialize(DataInputStream dis) throws IOException
    {
        int size = dis.readInt();            
        Map<InetAddress, EndPointState> epStateMap = new HashMap<InetAddress, EndPointState>();
        
        for ( int i = 0; i < size; ++i )
        {
            if ( dis.available() == 0 )
            {
                logger_.info("Remaining bytes zero. Stopping deserialization in EndPointState.");
                break;
            }
            // int length = dis.readInt();            
            InetAddress ep = CompactEndPointSerializationHelper.deserialize(dis);
            EndPointState epState = EndPointState.serializer().deserialize(dis);            
            epStateMap.put(ep, epState);
        }        
        return epStateMap;
    }
}

class GossipDigestSynMessageSerializer implements ICompactSerializer<GossipDigestSynMessage>
{   
    public void serialize(GossipDigestSynMessage gDigestSynMessage, DataOutputStream dos) throws IOException
    {    
        dos.writeUTF(gDigestSynMessage.clusterId_);
        GossipDigestSerializationHelper.serialize(gDigestSynMessage.gDigests_, dos);
    }

    public GossipDigestSynMessage deserialize(DataInputStream dis) throws IOException
    {
        String clusterId = dis.readUTF();
        List<GossipDigest> gDigests = GossipDigestSerializationHelper.deserialize(dis);
        return new GossipDigestSynMessage(clusterId, gDigests);
    }

}

